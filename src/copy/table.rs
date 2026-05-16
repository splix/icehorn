//! Per-table copy orchestrator.
//!
//! `TableCopy` owns the source/destination stores and prefixes for a
//! single Iceberg table and coordinates *load → plan → discover →
//! execute → finalize* across whichever scope was requested. Each
//! phase is its own focused method so a reader can scan top-down; the
//! supporting machinery lives in sibling modules:
//!
//!   * [`super::progress`]  — counters that bridge to the TUI reporter
//!   * [`super::plan`]      — discovery + planning + skip decisions
//!   * [`super::transfer`]  — per-file copy orchestration + sync log
//!   * [`super::paths`]     — small path-string helpers
//!
//! Per-table sync logs live at `<dst-table>/icehorn/sync/`, isolated
//! from any other table sharing the same namespace.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::cli::Scope;
use crate::iceberg::manifest::{self, ManifestTree, WalkEvent};
use crate::iceberg::metadata::{MetadataFile, find_latest, read_json};
use crate::iceberg::model::Metadata;
use crate::sync_log;
use crate::ui::Reporter;

use super::file;
use super::paths::{relative_under, strip_url_prefix};
use super::plan::{
    CopyPlan, build_plan, list_dst_metadata_filenames, list_existing, load_sync_log, pick_metadata,
};
use super::progress::{ScanProgress, TableProgress};
use super::transfer::{
    FileTaskContext, finalize_sync, process_listed, process_relative, render_metadata_json,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct TableStats {
    pub copied: u64,
    pub skipped: u64,
    /// Per-file I/O errors that the next run is expected to retry. No
    /// sync-log entry is written for these, so the destination scan
    /// will treat them as "missing" again next time.
    pub failed: u64,
}

/// Configuration for one table within a namespace copy.
pub struct TableCopy {
    pub src_store: Arc<dyn ObjectStore>,
    pub dst_store: Arc<dyn ObjectStore>,
    /// Bucket-relative prefix of the table on the source side.
    pub src_prefix: String,
    /// Bucket-relative prefix of the table on the destination side.
    pub dst_prefix: String,
    /// Full `s3://…` URL for the table on the source side. Used to
    /// rewrite absolute paths embedded inside metadata files.
    pub src_url: String,
    pub dst_url: String,
    pub scope: Scope,
    pub parallelism: usize,
    /// Sink for progress events. Plain mode passes a no-op reporter.
    pub reporter: Reporter,
    /// Stable key for this table in reporter events; the table UUID.
    pub table_key: String,
    /// Human label for the namespace, shown in the TUI header.
    pub namespace_label: String,
    /// Shared across all tables in the namespace copy. Held only during
    /// the file-copy phase (not during discovery), so a small table can
    /// finish its scan and start copying while large tables are still
    /// scanning. `--tables.parallel` sets the permit count.
    pub copy_gate: Arc<Semaphore>,
}

impl TableCopy {
    pub async fn run(self) -> Result<TableStats> {
        self.reporter
            .table_started(&self.table_key, &self.namespace_label, &self.table_key);
        let result = match self.scope {
            Scope::All => self.run_all().await,
            Scope::Latest => self.run_filtered(None).await,
            Scope::Version(v) => self.run_filtered(Some(v)).await,
        };
        self.reporter.table_finished(&self.table_key);
        result
    }

    /// Wait for a slot in the shared copy phase. We surface a status
    /// while we're queued so the user understands why a freshly
    /// scanned table is sitting idle. The fast path skips the
    /// "waiting" status entirely so it doesn't flicker on screen when
    /// a permit is already free.
    async fn acquire_copy_permit(&self) -> Result<SemaphorePermit<'_>> {
        if let Ok(p) = self.copy_gate.try_acquire() {
            return Ok(p);
        }
        self.reporter
            .table_status(&self.table_key, "waiting for copy slot");
        self.copy_gate
            .acquire()
            .await
            .context("copy gate semaphore closed")
    }

    /// `--scope all`: walk the entire source prefix and copy every
    /// object (with appropriate per-type rewriting). Mirrors the
    /// original behavior of the `copy` command.
    async fn run_all(&self) -> Result<TableStats> {
        let scanned = self.scan_destination_only().await?;

        let _permit = self.acquire_copy_permit().await?;
        self.reporter.table_status(&self.table_key, "copying");

        let (sync_tx, sync_writer) =
            sync_log::start_writer(Arc::clone(&self.dst_store), self.dst_prefix.clone());
        let progress = TableProgress::new(self.reporter.clone(), self.table_key.clone());

        let stream_result = self
            .stream_listing_copy(&scanned, &sync_tx, &progress)
            .await;

        finalize_with_stats(stream_result, sync_tx, sync_writer, &progress).await
    }

    /// `--scope latest` / `--scope <N>`: read the chosen metadata.json,
    /// walk the current snapshot, and copy only the files reachable
    /// from it. The destination metadata.json is rewritten to keep
    /// only snapshots whose manifest-list either lives at the
    /// destination already (from a previous run) or is being copied
    /// now — so repeated incremental runs grow the destination history
    /// without orphaning files.
    ///
    /// If the source `metadata/` is missing or unreadable, the table is
    /// soft-skipped with a warning and zero stats — the namespace copy
    /// keeps going for the rest.
    async fn run_filtered(&self, version: Option<u32>) -> Result<TableStats> {
        let Some(loaded) = self.load_metadata(version).await? else {
            return Ok(TableStats::default());
        };
        let plan = build_plan(
            &self.src_url,
            &loaded.meta,
            &loaded.chosen,
            &loaded.dst_existing_meta,
        )
        .await?;
        let scanned = self.discover_filtered(&plan).await?;
        let prepared = self.prepare_filtered_copy(loaded, plan, scanned.tree)?;

        let (sync_tx, sync_writer) =
            sync_log::start_writer(Arc::clone(&self.dst_store), self.dst_prefix.clone());
        let progress = TableProgress::new(self.reporter.clone(), self.table_key.clone());
        // +1 for the rewritten metadata.json we'll write at the end —
        // it's part of the copy from the user's perspective.
        progress.set_total(prepared.to_copy.len() as u64 + 1);

        let _permit = self.acquire_copy_permit().await?;
        tracing::debug!(
            table = %self.src_prefix,
            to_copy = %prepared.to_copy.len(),
            "start copying"
        );
        self.reporter.table_status(&self.table_key, "copying");

        let stream_result = self
            .stream_planned_copy(&prepared, &scanned.dst, &sync_tx, &progress)
            .await;
        let after_meta = self
            .write_rewritten_metadata(stream_result, &prepared, &progress, &sync_tx)
            .await;

        finalize_with_stats(after_meta, sync_tx, sync_writer, &progress).await
    }

    // ---------- discovery phases (also emit reporter status) ----------

    async fn scan_destination_only(&self) -> Result<DestinationScan> {
        self.reporter
            .table_status(&self.table_key, "scanning destination");
        let scan = ScanProgress::new(self.reporter.clone(), self.table_key.clone());
        let scan_for_dst = Arc::clone(&scan);
        let on_dst = move |n: u64| scan_for_dst.record_dst_objects(n);
        let existing_fut = list_existing(&*self.dst_store, &self.dst_prefix, Some(&on_dst));
        let prev_sync_fut = async {
            Ok::<_, anyhow::Error>(load_sync_log(&*self.dst_store, &self.dst_prefix).await)
        };
        let (existing, prev_sync) = tokio::try_join!(existing_fut, prev_sync_fut)
            .context("scanning destination")?;
        Ok(DestinationScan { existing, prev_sync })
    }

    async fn discover_filtered(&self, plan: &CopyPlan) -> Result<FilteredScan> {
        let manifest_list_abs: Vec<ObjPath> = plan
            .kept_manifest_lists
            .iter()
            .map(|ml| ObjPath::from(format!("{}/{}", self.src_prefix, ml)))
            .collect();
        self.reporter.table_status(
            &self.table_key,
            "walking manifest tree and scanning destination",
        );
        let scan = ScanProgress::new(self.reporter.clone(), self.table_key.clone());
        let scan_for_walk = Arc::clone(&scan);
        let on_walk = move |ev: WalkEvent| match ev {
            WalkEvent::ManifestList { done, total } => {
                scan_for_walk.record_manifest_list(done, total)
            }
            WalkEvent::Manifest { done, total } => scan_for_walk.record_manifest(done, total),
        };
        let scan_for_dst = Arc::clone(&scan);
        let on_dst = move |n: u64| scan_for_dst.record_dst_objects(n);

        let walk_fut = manifest::walk_many(
            Arc::clone(&self.src_store),
            manifest_list_abs,
            self.parallelism,
            Some(&on_walk),
        );
        let existing_fut = list_existing(&*self.dst_store, &self.dst_prefix, Some(&on_dst));
        let prev_sync_fut = async {
            Ok::<_, anyhow::Error>(load_sync_log(&*self.dst_store, &self.dst_prefix).await)
        };
        let (tree, existing, prev_sync) = tokio::try_join!(walk_fut, existing_fut, prev_sync_fut)
            .context("walking source and scanning destination")?;
        tracing::info!(
            table = %self.src_prefix,
            unique_manifests = tree.manifest_files.len(),
            content_files = tree.content_files.len(),
            dst_objects = existing.len(),
            "manifest tree walked, destination scanned"
        );
        Ok(FilteredScan {
            tree,
            dst: DestinationScan { existing, prev_sync },
        })
    }

    /// Find the latest metadata.json, pick the version we actually
    /// want, and load it. Returns `None` (after logging a warning) only
    /// when the source `metadata/` directory has no usable
    /// `<NNNNN>-<uuid>.metadata.json` — that's a legitimate empty-table
    /// case. IO errors against `metadata/` propagate as `Err` so the
    /// caller can mark this table as failed and let the namespace
    /// continue with siblings (next run retries this table).
    async fn load_metadata(&self, version: Option<u32>) -> Result<Option<LoadedMetadata>> {
        self.reporter
            .table_status(&self.table_key, "loading metadata");
        let metadata_prefix = ObjPath::from(format!("{}/metadata", self.src_prefix));
        let latest = find_latest(&*self.src_store, &metadata_prefix)
            .await
            .with_context(|| format!("listing metadata/ for {}", self.src_prefix))?;
        let latest = match latest {
            Some(latest) => latest,
            None => {
                tracing::warn!(table = %self.src_prefix, "no metadata.json found — skipping");
                return Ok(None);
            }
        };
        let chosen = pick_metadata(&*self.src_store, &metadata_prefix, version, &latest).await?;
        tracing::info!(
            table = %self.src_prefix,
            version = chosen.version,
            metadata = %chosen.path.filename().unwrap_or(""),
            "loading metadata"
        );

        let raw = read_json(&*self.src_store, &chosen.path).await?;
        let meta: Metadata = serde_json::from_slice(&raw)
            .with_context(|| format!("parsing metadata.json {}", chosen.path))?;
        let dst_existing_meta =
            list_dst_metadata_filenames(&*self.dst_store, &self.dst_prefix).await?;
        Ok(Some(LoadedMetadata {
            chosen,
            raw_size: raw.len(),
            meta,
            dst_existing_meta,
        }))
    }

    /// Filter the in-memory `Metadata` to match the kept set, build the
    /// final to-copy list, and re-serialize the rewritten
    /// `metadata.json`. All sync, no I/O — runs after both discovery
    /// futures have joined.
    fn prepare_filtered_copy(
        &self,
        loaded: LoadedMetadata,
        plan: CopyPlan,
        tree: ManifestTree,
    ) -> Result<PreparedCopy> {
        let LoadedMetadata {
            chosen,
            raw_size,
            mut meta,
            dst_existing_meta,
        } = loaded;

        let mut content_files: HashSet<String> = HashSet::new();
        for mf in &tree.manifest_files {
            content_files.insert(strip_url_prefix(&self.src_url, mf).to_string());
        }
        for cf in &tree.content_files {
            content_files.insert(strip_url_prefix(&self.src_url, cf).to_string());
        }

        filter_metadata_in_place(&mut meta, &plan, &dst_existing_meta, &chosen, &self.src_url);
        let rewritten_metadata_bytes =
            render_metadata_json(&meta, &chosen, &self.src_url, &self.dst_url)?;
        let metadata_rel = relative_under(&chosen.path, &self.src_prefix).to_string();

        let to_copy = build_to_copy_list(&plan, content_files, &meta, &self.src_url);

        Ok(PreparedCopy {
            to_copy,
            metadata_rel,
            rewritten_metadata_bytes,
            raw_metadata_size: raw_size,
        })
    }

    // ---------- file-copy streaming loops ----------

    async fn stream_listing_copy(
        &self,
        scanned: &DestinationScan,
        sync_tx: &tokio::sync::mpsc::Sender<sync_log::Msg>,
        progress: &Arc<TableProgress>,
    ) -> Result<()> {
        let ctx = self.build_task_context(scanned, sync_tx, progress);
        let src_prefix_obj = ObjPath::from(self.src_prefix.as_str());
        self.src_store
            .list(Some(&src_prefix_obj))
            .map(|r| r.context("failed to list source objects"))
            .try_for_each_concurrent(self.parallelism, move |obj| {
                let ctx = Arc::clone(&ctx);
                async move { process_listed(&ctx, obj).await }
            })
            .await
    }

    async fn stream_planned_copy(
        &self,
        prepared: &PreparedCopy,
        scanned: &DestinationScan,
        sync_tx: &tokio::sync::mpsc::Sender<sync_log::Msg>,
        progress: &Arc<TableProgress>,
    ) -> Result<()> {
        let ctx = self.build_task_context(scanned, sync_tx, progress);
        let to_copy = prepared.to_copy.clone();
        futures::stream::iter(to_copy.into_iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(self.parallelism, move |relative| {
                let ctx = Arc::clone(&ctx);
                async move { process_relative(&ctx, relative).await }
            })
            .await
    }

    /// Write the rewritten `metadata.json` only when content copies
    /// succeeded — otherwise we'd point at a half-populated tree.
    async fn write_rewritten_metadata(
        &self,
        stream_result: Result<()>,
        prepared: &PreparedCopy,
        progress: &Arc<TableProgress>,
        sync_tx: &tokio::sync::mpsc::Sender<sync_log::Msg>,
    ) -> Result<()> {
        stream_result?;
        let dst_meta_path = ObjPath::from(format!("{}/{}", self.dst_prefix, prepared.metadata_rel));
        file::put_bytes(
            &*self.dst_store,
            &dst_meta_path,
            prepared.rewritten_metadata_bytes.clone(),
        )
        .await
        .context("writing rewritten metadata.json")?;
        progress.record_copied();

        let _ = sync_tx
            .send(sync_log::Msg::Entry(sync_log::Entry {
                relative_path: prepared.metadata_rel.clone(),
                source_size: prepared.raw_metadata_size as i64,
                source_etag: None,
                file_type: file::FileType::Json.as_str().to_string(),
                rewritten: true,
            }))
            .await;
        Ok(())
    }

    fn build_task_context(
        &self,
        scanned: &DestinationScan,
        sync_tx: &tokio::sync::mpsc::Sender<sync_log::Msg>,
        progress: &Arc<TableProgress>,
    ) -> Arc<FileTaskContext> {
        Arc::new(FileTaskContext {
            src_store: Arc::clone(&self.src_store),
            dst_store: Arc::clone(&self.dst_store),
            existing: Arc::clone(&scanned.existing),
            prev_sync: Arc::clone(&scanned.prev_sync),
            progress: Arc::clone(progress),
            reporter: self.reporter.clone(),
            table_key: self.table_key.clone(),
            sync_tx: sync_tx.clone(),
            src_prefix: self.src_prefix.clone(),
            dst_prefix: self.dst_prefix.clone(),
            src_url: self.src_url.clone(),
            dst_url: self.dst_url.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// Phase outputs (just data — no behavior)
// ---------------------------------------------------------------------------

struct DestinationScan {
    existing: Arc<HashMap<String, u64>>,
    prev_sync: Arc<HashMap<String, sync_log::Entry>>,
}

struct FilteredScan {
    tree: ManifestTree,
    dst: DestinationScan,
}

struct LoadedMetadata {
    chosen: MetadataFile,
    raw_size: usize,
    meta: Metadata,
    dst_existing_meta: HashSet<String>,
}

struct PreparedCopy {
    to_copy: Vec<String>,
    metadata_rel: String,
    rewritten_metadata_bytes: Bytes,
    raw_metadata_size: usize,
}

// ---------------------------------------------------------------------------
// Pure helpers reused by the orchestrator.
// ---------------------------------------------------------------------------

/// Drop snapshots / log entries / statistics whose backing files we
/// won't have at the destination after this run. The rest of the
/// metadata structure (schemas, partition specs, properties, …) is
/// preserved verbatim by serde's `flatten`.
fn filter_metadata_in_place(
    meta: &mut Metadata,
    plan: &CopyPlan,
    dst_existing_meta: &HashSet<String>,
    chosen: &MetadataFile,
    src_url: &str,
) {
    meta.snapshots.retain(|s| {
        s.manifest_list.as_deref().is_some_and(|ml| {
            let rel = strip_url_prefix(src_url, ml);
            plan.kept_manifest_lists.contains(rel)
        })
    });
    let kept_snapshot_ids: HashSet<i64> = meta.snapshots.iter().map(|s| s.snapshot_id).collect();
    meta.snapshot_log
        .retain(|e| kept_snapshot_ids.contains(&e.snapshot_id));
    meta.metadata_log.retain(|e| {
        let filename = file_basename(&e.metadata_file);
        dst_existing_meta.contains(filename) || filename == chosen.path.filename().unwrap_or("")
    });
    meta.statistics
        .retain(|s| kept_snapshot_ids.contains(&s.snapshot_id));
    meta.partition_statistics
        .retain(|s| kept_snapshot_ids.contains(&s.snapshot_id));
}

fn build_to_copy_list(
    plan: &CopyPlan,
    content_files: HashSet<String>,
    meta: &Metadata,
    src_url: &str,
) -> Vec<String> {
    let mut to_copy: Vec<String> = Vec::new();
    to_copy.extend(plan.kept_manifest_lists.iter().cloned());
    to_copy.extend(content_files);
    for s in &meta.statistics {
        to_copy.push(strip_url_prefix(src_url, &s.statistics_path).to_string());
    }
    for s in &meta.partition_statistics {
        to_copy.push(strip_url_prefix(src_url, &s.statistics_path).to_string());
    }
    // Dedup — the same data file can appear in multiple manifests.
    let mut seen = HashSet::new();
    to_copy.into_iter().filter(|p| seen.insert(p.clone())).collect()
}

fn file_basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// Drain the sync log and produce final stats. Shared between the two
/// scope variants since they both end the same way.
async fn finalize_with_stats(
    final_result: Result<()>,
    sync_tx: tokio::sync::mpsc::Sender<sync_log::Msg>,
    sync_writer: sync_log::WriterHandle,
    progress: &TableProgress,
) -> Result<TableStats> {
    let copy_error = final_result.err();
    finalize_sync(sync_tx, sync_writer, copy_error.is_none()).await?;
    if let Some(e) = copy_error {
        return Err(e);
    }
    Ok(TableStats {
        copied: progress.copied_count(),
        skipped: progress.skipped_count(),
        failed: progress.failed_count(),
    })
}
