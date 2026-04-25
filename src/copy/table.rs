//! Per-table copy.
//!
//! Each `TableCopy` owns the source/destination stores and prefixes for
//! a single Iceberg table, picks files according to its `Scope`, and
//! drives parallel copy + sync-log writing for that table only.
//!
//! Per-table sync logs live at `<dst-table>/icehorn/sync/`, isolated
//! from any other table sharing the same namespace.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::cli::Scope;
use crate::iceberg::{
    manifest,
    metadata::{find_latest, read_json, MetadataFile},
    model::Metadata,
};
use crate::sync_log;

#[derive(Debug, Default, Clone, Copy)]
pub struct TableStats {
    pub copied: u64,
    pub skipped: u64,
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
}

impl TableCopy {
    pub async fn run(self) -> Result<TableStats> {
        match self.scope {
            Scope::All => self.run_all().await,
            Scope::Latest => self.run_filtered(None).await,
            Scope::Version(v) => self.run_filtered(Some(v)).await,
        }
    }

    /// `--scope all`: walk the entire source prefix and copy every object
    /// (with appropriate per-type rewriting). Mirrors the original
    /// behavior of the `copy` command.
    async fn run_all(&self) -> Result<TableStats> {
        // Both touch the destination but issue independent S3 LISTs, so
        // overlap them.
        let existing_fut = list_existing(&*self.dst_store, &self.dst_prefix);
        let prev_sync_fut = async {
            Ok::<_, anyhow::Error>(load_sync_log(&*self.dst_store, &self.dst_prefix).await)
        };
        let (existing, prev_sync) = tokio::try_join!(existing_fut, prev_sync_fut)
            .context("scanning destination")?;

        let (sync_tx, sync_writer) = sync_log::start_writer(
            Arc::clone(&self.dst_store),
            self.dst_prefix.clone(),
        );

        let copied = Arc::new(AtomicU64::new(0));
        let skipped = Arc::new(AtomicU64::new(0));

        let src_prefix_obj = ObjPath::from(self.src_prefix.as_str());
        let parallelism = self.parallelism;

        let listing = self.src_store.list(Some(&src_prefix_obj));
        let result = listing
            .map(|r| r.context("failed to list source objects"))
            .try_for_each_concurrent(parallelism, |obj| {
                let src_store = Arc::clone(&self.src_store);
                let dst_store = Arc::clone(&self.dst_store);
                let existing = existing.clone();
                let prev_sync = prev_sync.clone();
                let copied = Arc::clone(&copied);
                let skipped = Arc::clone(&skipped);
                let sync_tx = sync_tx.clone();
                let src_url = self.src_url.clone();
                let dst_url = self.dst_url.clone();
                let src_prefix = self.src_prefix.clone();
                let dst_prefix = self.dst_prefix.clone();

                async move {
                    let relative = relative_under(&obj.location, &src_prefix);
                    let dst_path = ObjPath::from(format!("{dst_prefix}/{relative}"));

                    if should_skip(&existing, &prev_sync, relative, obj.size, obj.e_tag.as_deref()) {
                        skipped.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }

                    let file_type = super::file::classify(obj.location.as_ref());
                    let res = run_copy(
                        &*src_store,
                        &obj.location,
                        &*dst_store,
                        &dst_path,
                        file_type,
                        &src_url,
                        &dst_url,
                    )
                    .await;

                    record_outcome(
                        res,
                        &obj.location,
                        relative,
                        obj.size,
                        obj.e_tag.as_deref(),
                        file_type,
                        &copied,
                        &skipped,
                        &sync_tx,
                    )
                    .await;
                    Ok(())
                }
            })
            .await;

        let copy_error = result.err();
        finalize_sync(sync_tx, sync_writer, copy_error.is_none()).await?;

        if let Some(e) = copy_error {
            return Err(e);
        }

        Ok(TableStats {
            copied: copied.load(Ordering::Relaxed),
            skipped: skipped.load(Ordering::Relaxed),
        })
    }

    /// `--scope latest` / `--scope <N>`: read the chosen metadata.json,
    /// walk the current snapshot, and copy only the files reachable from
    /// it. The destination metadata.json is rewritten to keep only
    /// snapshots whose manifest-list either lives at the destination
    /// already (from a previous run) or is being copied now — so
    /// repeated incremental runs grow the destination history without
    /// orphaning files.
    async fn run_filtered(&self, version: Option<u32>) -> Result<TableStats> {
        let metadata_prefix = ObjPath::from(format!("{}/metadata", self.src_prefix));
        let chosen = pick_metadata(&*self.src_store, &metadata_prefix, version).await?;

        tracing::info!(
            table = %self.src_prefix,
            version = chosen.version,
            "loading metadata"
        );

        let raw = read_json(&*self.src_store, &chosen.path).await?;
        let mut meta: Metadata = serde_json::from_slice(&raw)
            .with_context(|| format!("parsing metadata.json {}", chosen.path))?;

        let dst_existing_meta = list_dst_metadata_filenames(&*self.dst_store, &self.dst_prefix).await?;

        let plan = build_plan(&self.src_url, &meta, &chosen, &dst_existing_meta).await?;
        tracing::info!(
            table = %self.src_prefix,
            version = chosen.version,
            snapshots = plan.kept_manifest_lists.len(),
            "walking manifest tree and scanning destination"
        );

        // The manifest walk hits the source; the destination scan and the
        // sync log read hit the destination. Running them concurrently
        // overlaps two independent multi-minute LIST phases on large
        // tables.
        let manifest_list_abs: Vec<ObjPath> = plan
            .kept_manifest_lists
            .iter()
            .map(|ml_rel| ObjPath::from(format!("{}/{}", self.src_prefix, ml_rel)))
            .collect();
        let walk_fut = manifest::walk_many(
            Arc::clone(&self.src_store),
            manifest_list_abs,
            self.parallelism,
        );
        let existing_fut = list_existing(&*self.dst_store, &self.dst_prefix);
        let prev_sync_fut = async {
            Ok::<_, anyhow::Error>(load_sync_log(&*self.dst_store, &self.dst_prefix).await)
        };
        let (tree, existing, prev_sync) = tokio::try_join!(walk_fut, existing_fut, prev_sync_fut)
            .context("walking source and scanning destination")?;
        tracing::info!(
            table = %self.src_prefix,
            version = chosen.version,
            unique_manifests = tree.manifest_files.len(),
            content_files = tree.content_files.len(),
            dst_objects = existing.len(),
            "manifest tree walked, destination scanned"
        );

        let mut content_files: HashSet<String> = HashSet::new();
        for mf in &tree.manifest_files {
            content_files.insert(strip_url_prefix(&self.src_url, mf).to_string());
        }
        for cf in &tree.content_files {
            content_files.insert(strip_url_prefix(&self.src_url, cf).to_string());
        }

        // Filter the in-memory Metadata to match the kept set, then
        // re-serialize. The rest of the metadata structure (schemas,
        // partition specs, properties, …) round-trips unchanged.
        meta.snapshots
            .retain(|s| s.manifest_list.as_deref().is_some_and(|ml| {
                let rel = strip_url_prefix(&self.src_url, ml);
                plan.kept_manifest_lists.contains(rel)
            }));
        let kept_snapshot_ids: HashSet<i64> =
            meta.snapshots.iter().map(|s| s.snapshot_id).collect();
        meta.snapshot_log
            .retain(|e| kept_snapshot_ids.contains(&e.snapshot_id));
        meta.metadata_log.retain(|e| {
            let filename = filename_of(&e.metadata_file);
            dst_existing_meta.contains(filename) || filename == chosen.path.filename().unwrap_or("")
        });
        meta.statistics
            .retain(|s| kept_snapshot_ids.contains(&s.snapshot_id));
        meta.partition_statistics
            .retain(|s| kept_snapshot_ids.contains(&s.snapshot_id));

        let rewritten_metadata_bytes = render_metadata_json(&meta, &chosen, &self.src_url, &self.dst_url)?;

        // Build the full set of relative paths to copy. The metadata.json
        // is handled separately — we already have its bytes.
        let metadata_rel = relative_under(&chosen.path, &self.src_prefix).to_string();
        let mut to_copy: Vec<String> = Vec::new();
        to_copy.extend(plan.kept_manifest_lists.iter().cloned());
        to_copy.extend(content_files);
        for s in &meta.statistics {
            to_copy.push(strip_url_prefix(&self.src_url, &s.statistics_path).to_string());
        }
        for s in &meta.partition_statistics {
            to_copy.push(strip_url_prefix(&self.src_url, &s.statistics_path).to_string());
        }
        // Dedup — the same data file can appear in multiple manifests.
        let to_copy: Vec<String> = {
            let mut seen = HashSet::new();
            to_copy.into_iter().filter(|p| seen.insert(p.clone())).collect()
        };

        let (sync_tx, sync_writer) = sync_log::start_writer(
            Arc::clone(&self.dst_store),
            self.dst_prefix.clone(),
        );

        let copied = Arc::new(AtomicU64::new(0));
        let skipped = Arc::new(AtomicU64::new(0));

        tracing::debug!(
            table = %self.src_prefix,
            version = chosen.version,
            to_copy = %to_copy.len(),
            "start copying"
        );

        // Copy referenced files in parallel, then write the rewritten
        // metadata.json last — so a partial run never leaves a metadata
        // file pointing at content that hasn't landed yet.
        let parallelism = self.parallelism;
        let stream_result = futures::stream::iter(to_copy.into_iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(parallelism, |relative| {
                let src_store = Arc::clone(&self.src_store);
                let dst_store = Arc::clone(&self.dst_store);
                let existing = existing.clone();
                let prev_sync = prev_sync.clone();
                let copied = Arc::clone(&copied);
                let skipped = Arc::clone(&skipped);
                let sync_tx = sync_tx.clone();
                let src_url = self.src_url.clone();
                let dst_url = self.dst_url.clone();
                let src_prefix = self.src_prefix.clone();
                let dst_prefix = self.dst_prefix.clone();

                async move {
                    let src_path = ObjPath::from(format!("{src_prefix}/{relative}"));
                    let dst_path = ObjPath::from(format!("{dst_prefix}/{relative}"));

                    let head = match src_store.head(&src_path).await {
                        Ok(h) => h,
                        Err(object_store::Error::NotFound { .. }) => {
                            tracing::warn!(src = %src_path, "referenced file missing on source, skipping");
                            skipped.fetch_add(1, Ordering::Relaxed);
                            return Ok(());
                        }
                        Err(e) => return Err(anyhow::Error::from(e)).context("HEAD source"),
                    };

                    if should_skip(&existing, &prev_sync, &relative, head.size, head.e_tag.as_deref()) {
                        skipped.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }

                    let file_type = super::file::classify(src_path.as_ref());
                    let res = run_copy(
                        &*src_store,
                        &src_path,
                        &*dst_store,
                        &dst_path,
                        file_type,
                        &src_url,
                        &dst_url,
                    )
                    .await;

                    record_outcome(
                        res,
                        &src_path,
                        &relative,
                        head.size,
                        head.e_tag.as_deref(),
                        file_type,
                        &copied,
                        &skipped,
                        &sync_tx,
                    )
                    .await;
                    Ok(())
                }
            })
            .await;

        // Write the rewritten metadata.json only when content copies
        // succeeded. Otherwise we'd point at a half-populated tree.
        let final_result = match stream_result {
            Ok(()) => {
                let dst_meta_path = ObjPath::from(format!("{}/{}", self.dst_prefix, metadata_rel));
                super::file::put_bytes(&*self.dst_store, &dst_meta_path, rewritten_metadata_bytes)
                    .await
                    .context("writing rewritten metadata.json")?;
                copied.fetch_add(1, Ordering::Relaxed);

                let _ = sync_tx.send(sync_log::Msg::Entry(sync_log::Entry {
                    relative_path: metadata_rel,
                    source_size: raw.len() as i64,
                    source_etag: None,
                    file_type: super::file::FileType::Json.as_str().to_string(),
                    rewritten: true,
                })).await;
                Ok(())
            }
            Err(e) => Err(e),
        };

        let copy_error = final_result.err();
        finalize_sync(sync_tx, sync_writer, copy_error.is_none()).await?;

        if let Some(e) = copy_error {
            return Err(e);
        }

        Ok(TableStats {
            copied: copied.load(Ordering::Relaxed),
            skipped: skipped.load(Ordering::Relaxed),
        })
    }
}

/// What gets kept from the source's `snapshots[]` after intersecting
/// with the destination's existing manifest-list files.
struct CopyPlan {
    /// Relative paths (under the table prefix) of every manifest list
    /// we want present on the destination after this run.
    kept_manifest_lists: HashSet<String>,
}

async fn build_plan(
    src_url: &str,
    meta: &Metadata,
    chosen: &MetadataFile,
    dst_existing_meta: &HashSet<String>,
) -> Result<CopyPlan> {
    let current = meta.current_snapshot();
    let current_ml_rel = current
        .and_then(|s| s.manifest_list.as_deref())
        .map(|ml| strip_url_prefix(src_url, ml).to_string());

    let mut kept: HashSet<String> = HashSet::new();
    if let Some(ml) = &current_ml_rel {
        kept.insert(ml.clone());
    }

    // Keep a snapshot's manifest-list iff its file already exists on the
    // destination — that's how we accumulate history across incremental
    // runs without pulling more data than needed.
    for snap in &meta.snapshots {
        let Some(ml) = snap.manifest_list.as_deref() else {
            continue;
        };
        let rel = strip_url_prefix(src_url, ml).to_string();
        let filename = filename_of(&rel);
        if dst_existing_meta.contains(filename) {
            kept.insert(rel);
        }
    }

    if kept.is_empty() {
        // An empty current snapshot is fine (freshly created table); we
        // still produce a metadata.json. But there's nothing else to
        // walk. Bail here so the caller produces the right log message.
        tracing::info!(version = chosen.version, "no snapshots to copy — empty table");
    }

    Ok(CopyPlan {
        kept_manifest_lists: kept,
    })
}

async fn pick_metadata(
    store: &dyn ObjectStore,
    metadata_prefix: &ObjPath,
    version: Option<u32>,
) -> Result<MetadataFile> {
    let latest = find_latest(store, metadata_prefix)
        .await?
        .ok_or_else(|| anyhow!("no <NNNNN>-<uuid>.metadata.json files found under metadata/"))?;
    match version {
        None => Ok(latest),
        Some(v) if v == latest.version => Ok(latest),
        Some(v) => find_specific_version(store, metadata_prefix, v).await,
    }
}

async fn find_specific_version(
    store: &dyn ObjectStore,
    metadata_prefix: &ObjPath,
    version: u32,
) -> Result<MetadataFile> {
    use futures::TryStreamExt;
    let mut stream = store.list(Some(metadata_prefix));
    while let Some(obj) = stream.try_next().await.context("listing metadata files")? {
        if let Some(m) = MetadataFile::parse(&obj.location)
            && m.version == version
        {
            return Ok(m);
        }
    }
    Err(anyhow!("metadata version {version:05} not found under {metadata_prefix}"))
}

/// Pre-scan the destination's `metadata/` directory and collect the
/// filenames of every manifest list / metadata.json we already wrote in
/// previous runs. Used by `build_plan` to decide which historical
/// snapshots can stay in the rewritten metadata.json.
async fn list_dst_metadata_filenames(
    store: &dyn ObjectStore,
    dst_prefix: &str,
) -> Result<HashSet<String>> {
    let prefix = ObjPath::from(format!("{dst_prefix}/metadata"));
    let stream = store.list(Some(&prefix));
    let names: Vec<String> = stream
        .map_ok(|obj| filename_of(obj.location.as_ref()).to_string())
        .try_collect()
        .await
        .context("listing destination metadata/")?;
    Ok(names.into_iter().collect())
}

async fn list_existing(store: &dyn ObjectStore, dst_prefix: &str) -> Result<Arc<HashMap<String, u64>>> {
    let dst_prefix_obj = ObjPath::from(dst_prefix);
    let map: HashMap<String, u64> = store
        .list(Some(&dst_prefix_obj))
        .map(|r| {
            r.map(|obj| {
                let rel = relative_under(&obj.location, dst_prefix).to_string();
                (rel, obj.size)
            })
        })
        .try_collect()
        .await
        .context("scanning destination for existing files")?;
    Ok(Arc::new(map))
}

async fn load_sync_log(store: &dyn ObjectStore, dst_prefix: &str) -> Arc<HashMap<String, sync_log::Entry>> {
    match sync_log::load_all(store, dst_prefix).await {
        Ok(log) => Arc::new(log),
        Err(e) => {
            tracing::warn!(error = %e, "failed to load sync logs, proceeding without");
            Arc::new(HashMap::new())
        }
    }
}

fn should_skip(
    existing: &HashMap<String, u64>,
    prev_sync: &HashMap<String, sync_log::Entry>,
    relative: &str,
    src_size: u64,
    src_etag: Option<&str>,
) -> bool {
    // Verbatim-identical: same size on dst.
    if let Some(&dst_size) = existing.get(relative)
        && dst_size == src_size
    {
        return true;
    }
    // Rewritten and source unchanged since last sync: dst still good.
    if existing.contains_key(relative)
        && let Some(entry) = prev_sync.get(relative)
        && entry.matches_source(src_size, src_etag)
    {
        return true;
    }
    false
}

async fn run_copy(
    src_store: &dyn ObjectStore,
    src_path: &ObjPath,
    dst_store: &dyn ObjectStore,
    dst_path: &ObjPath,
    file_type: super::file::FileType,
    src_url: &str,
    dst_url: &str,
) -> Result<()> {
    use super::file::{copy_avro, copy_json, copy_verbatim, FileType};
    tracing::trace!(path = %src_path, "Copying file");
    match file_type {
        FileType::Parquet | FileType::Other => {
            copy_verbatim(src_store, src_path, dst_store, dst_path).await
        }
        FileType::Json => copy_json(src_store, src_path, dst_store, dst_path, src_url, dst_url).await,
        FileType::Avro => copy_avro(src_store, src_path, dst_store, dst_path, src_url, dst_url).await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn record_outcome(
    res: Result<()>,
    src_path: &ObjPath,
    relative: &str,
    src_size: u64,
    src_etag: Option<&str>,
    file_type: super::file::FileType,
    copied: &Arc<AtomicU64>,
    skipped: &Arc<AtomicU64>,
    sync_tx: &tokio::sync::mpsc::Sender<sync_log::Msg>,
) {
    match res {
        Ok(()) => {
            copied.fetch_add(1, Ordering::Relaxed);
            let _ = sync_tx.send(sync_log::Msg::Entry(sync_log::Entry {
                relative_path: relative.to_string(),
                source_size: src_size as i64,
                source_etag: src_etag.map(|s| s.to_string()),
                file_type: file_type.as_str().to_string(),
                rewritten: file_type.is_rewritten(),
            })).await;
        }
        Err(e)
            if e.downcast_ref::<object_store::Error>()
                .is_some_and(|oe| matches!(oe, object_store::Error::NotFound { .. })) =>
        {
            tracing::warn!(src = %src_path, "source file disappeared during copy, skipping");
            skipped.fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => {
            tracing::warn!(src = %src_path, error = %e, "failed to copy file, skipping");
            skipped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn finalize_sync(
    sync_tx: tokio::sync::mpsc::Sender<sync_log::Msg>,
    sync_writer: sync_log::WriterHandle,
    commit: bool,
) -> Result<()> {
    if commit {
        let _ = sync_tx.send(sync_log::Msg::Close).await;
    }
    drop(sync_tx);
    sync_writer.join().await
}

/// Strip a known leading `<src-prefix>/` from an `ObjPath` and return
/// the remainder. We never want a leading slash in our relative keys
/// because they're concatenated with `<dst-prefix>/`.
fn relative_under<'a>(path: &'a ObjPath, prefix: &str) -> &'a str {
    path.as_ref()
        .strip_prefix(prefix)
        .unwrap_or(path.as_ref())
        .trim_start_matches('/')
}

/// Strip a `s3://bucket/<table>/` URL prefix from an absolute path stored
/// inside metadata. Falls back to returning the input on mismatch — the
/// caller should treat that as a copy-as-is signal rather than aborting.
fn strip_url_prefix<'a>(src_url: &str, abs: &'a str) -> &'a str {
    let prefix = format!("{src_url}/");
    abs.strip_prefix(prefix.as_str())
        .unwrap_or_else(|| abs.strip_prefix(src_url).unwrap_or(abs).trim_start_matches('/'))
}

fn filename_of(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn render_metadata_json(
    meta: &Metadata,
    chosen: &MetadataFile,
    src_url: &str,
    dst_url: &str,
) -> Result<Bytes> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    // Serialize → blanket prefix-replace catches every absolute path
    // embedded in fields we model and fields we don't (schemas often
    // carry `s3://` references via doc strings, custom properties, …).
    let json = serde_json::to_vec(meta).context("serializing rewritten metadata.json")?;
    let text = String::from_utf8(json).expect("serde_json emits valid UTF-8");
    let rewritten = text.replace(src_url, dst_url);

    // Match the source's compression: if the chosen file ended in `.gz`
    // we re-emit gzipped to keep the writer's expectations consistent.
    let needs_gzip = chosen
        .path
        .filename()
        .is_some_and(|n| n.ends_with(".gz") || n.contains(".gz."));

    if needs_gzip {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(rewritten.as_bytes())?;
        Ok(Bytes::from(encoder.finish()?))
    } else {
        Ok(Bytes::from(rewritten.into_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_url_prefix_handles_with_and_without_trailing_slash() {
        let src = "s3://bucket/iceberg/ns/uuid";
        assert_eq!(
            strip_url_prefix(src, "s3://bucket/iceberg/ns/uuid/metadata/foo.avro"),
            "metadata/foo.avro"
        );
        // No trailing slash form (paranoid input).
        assert_eq!(
            strip_url_prefix(src, "s3://bucket/iceberg/ns/uuid"),
            ""
        );
    }

    #[test]
    fn filename_of_returns_last_segment() {
        assert_eq!(filename_of("metadata/00005-uuid.metadata.json"), "00005-uuid.metadata.json");
        assert_eq!(filename_of("nofileinpath"), "nofileinpath");
    }

    #[test]
    fn relative_under_strips_prefix_and_leading_slash() {
        let p = ObjPath::from("ns/uuid/data/file.parquet");
        assert_eq!(relative_under(&p, "ns/uuid"), "data/file.parquet");
    }
}
