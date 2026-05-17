//! Discovering what to copy: locate the right metadata, walk the
//! destination, and decide which files can be skipped.
//!
//! All planning lives here so the orchestrator (`table.rs`) can read
//! top-down — load → plan → discover → execute → finalize — without
//! the planning details cluttering the page.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use futures::TryStreamExt;
use futures::stream::{self, StreamExt};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::iceberg::metadata::MetadataFile;
use crate::iceberg::model::Metadata;
use crate::sync_log;

use super::paths::{filename_of, relative_under, strip_url_prefix};

/// Uncertain-residue count below which we probe the destination
/// per-file via HEAD instead of paginating the entire prefix with
/// LIST. "Uncertain" means *not in the sync log* — files we've
/// already recorded copying are trusted as present without a
/// round-trip, since `to_copy` for a long-lived table typically
/// contains tens of thousands of carry-overs from prior snapshots
/// of which only a handful are genuinely new.
///
/// On a long-lived destination (hundreds of thousands of files) the
/// full LIST dominates the table-prep time; for the typical
/// incremental run (a handful of new files since last time), N HEADs
/// in parallel finish in a fraction of a second. The threshold is
/// roughly the page size of an S3 LIST so the request count stays
/// in the same order of magnitude in either branch.
pub(super) const HEAD_PROBE_THRESHOLD: usize = 1000;

/// What gets kept from the source's `snapshots[]` after intersecting
/// with the destination's existing manifest-list files.
pub(super) struct CopyPlan {
    /// Relative paths (under the table prefix) of every manifest list
    /// we want present on the destination after this run.
    pub kept_manifest_lists: HashSet<String>,
}

pub(super) async fn build_plan(
    src_url: &str,
    meta: &Metadata,
    chosen: &MetadataFile,
    dst_existing_meta: &HashSet<String>,
) -> Result<CopyPlan> {
    let mut kept: HashSet<String> = HashSet::new();
    if let Some(snap) = meta.current_snapshot()
        && let Some(ml) = snap.manifest_list.as_deref()
    {
        kept.insert(strip_url_prefix(src_url, ml).to_string());
    }

    // Keep a snapshot's manifest-list iff its file already exists on the
    // destination — that's how we accumulate history across incremental
    // runs without pulling more data than needed.
    for snap in &meta.snapshots {
        let Some(ml) = snap.manifest_list.as_deref() else {
            continue;
        };
        let rel = strip_url_prefix(src_url, ml).to_string();
        if dst_existing_meta.contains(filename_of(&rel)) {
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

/// Pick the metadata.json to copy. `discovered_latest` is reused from
/// the namespace-wide discovery pass to avoid re-listing `metadata/` —
/// the only case that still has to walk S3 is `--scope <N>` where the
/// requested version differs from the latest.
pub(super) async fn pick_metadata(
    store: &dyn ObjectStore,
    metadata_prefix: &ObjPath,
    version: Option<u32>,
    discovered_latest: &MetadataFile,
) -> Result<MetadataFile> {
    match version {
        None => Ok(discovered_latest.clone()),
        Some(v) if v == discovered_latest.version => Ok(discovered_latest.clone()),
        Some(v) => find_specific_version(store, metadata_prefix, v).await,
    }
}

async fn find_specific_version(
    store: &dyn ObjectStore,
    metadata_prefix: &ObjPath,
    version: u32,
) -> Result<MetadataFile> {
    let mut stream = store.list(Some(metadata_prefix));
    while let Some(obj) = stream.try_next().await.context("listing metadata files")? {
        if let Some(m) = MetadataFile::parse(&obj.location)
            && m.version == version
        {
            return Ok(m);
        }
    }
    Err(anyhow!(
        "metadata version {version:05} not found under {metadata_prefix}"
    ))
}

/// Pre-scan the destination's `metadata/` directory and collect the
/// filenames of every manifest list / metadata.json we already wrote in
/// previous runs. Used by [`build_plan`] to decide which historical
/// snapshots can stay in the rewritten metadata.json.
pub(super) async fn list_dst_metadata_filenames(
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

/// Probe the destination one file at a time for a known set of
/// relative paths. Use this instead of [`list_existing`] when the set
/// is small relative to the destination — N parallel HEADs beat a
/// full LIST that paginates through hundreds of thousands of unrelated
/// keys.
///
/// Files that 404 are omitted from the returned map (the file-copy
/// phase then treats them as "not present" and tries to copy them).
/// Other HEAD errors are logged and the file is treated as not
/// present too — `process_relative` will surface the failure loudly
/// enough when the copy itself runs.
pub(super) async fn head_existing(
    store: Arc<dyn ObjectStore>,
    dst_prefix: &str,
    relative_paths: &[String],
    parallelism: usize,
) -> Result<Arc<HashMap<String, u64>>> {
    let dst_prefix = dst_prefix.to_string();
    let parallelism = parallelism.max(1);

    let results: Vec<Option<(String, u64)>> = stream::iter(relative_paths.iter().cloned())
        .map(|rel| {
            let store = Arc::clone(&store);
            let dst_prefix = dst_prefix.clone();
            async move {
                let dst_path = ObjPath::from(format!("{dst_prefix}/{rel}"));
                match store.head(&dst_path).await {
                    Ok(meta) => Some((rel, meta.size)),
                    Err(object_store::Error::NotFound { .. }) => None,
                    Err(e) => {
                        tracing::warn!(
                            path = %dst_path,
                            error = %e,
                            "HEAD failed during dst scan — assuming not present"
                        );
                        None
                    }
                }
            }
        })
        .buffer_unordered(parallelism)
        .collect()
        .await;

    Ok(Arc::new(results.into_iter().flatten().collect()))
}

/// Walk the destination and return `(relative_key, size)` for every
/// existing object. The returned map drives the "already there" branch
/// of [`should_skip`]. The optional progress callback fires once per
/// listed object so the TUI can show the count climbing.
pub(super) async fn list_existing(
    store: &dyn ObjectStore,
    dst_prefix: &str,
    on_progress: Option<&(dyn Fn(u64) + Send + Sync)>,
) -> Result<Arc<HashMap<String, u64>>> {
    let dst_prefix_obj = ObjPath::from(dst_prefix);
    let mut map: HashMap<String, u64> = HashMap::new();
    let mut stream = store.list(Some(&dst_prefix_obj));
    let mut count = 0u64;
    while let Some(obj) = stream
        .try_next()
        .await
        .context("scanning destination for existing files")?
    {
        let rel = relative_under(&obj.location, dst_prefix).to_string();
        map.insert(rel, obj.size);
        count += 1;
        if let Some(cb) = on_progress {
            cb(count);
        }
    }
    Ok(Arc::new(map))
}

/// Read every prior sync log file. A read failure is logged and
/// downgraded to "no log" — sync logs are an optimization, not a
/// requirement, so a corrupted one shouldn't fail the whole copy.
pub(super) async fn load_sync_log(
    store: &dyn ObjectStore,
    dst_prefix: &str,
) -> Arc<HashMap<String, sync_log::Entry>> {
    match sync_log::load_all(store, dst_prefix).await {
        Ok(log) => Arc::new(log),
        Err(e) => {
            tracing::warn!(error = %e, "failed to load sync logs, proceeding without");
            Arc::new(HashMap::new())
        }
    }
}

/// Filter `candidates` down to paths we have no prior sync-log
/// record of copying. The result is what we'd actually need to
/// verify on the destination — the sync-log-known files are trusted
/// as already-present and don't need a HEAD round-trip.
pub(super) fn uncertain_paths(
    candidates: &[String],
    prev_sync: &HashMap<String, sync_log::Entry>,
) -> Vec<String> {
    candidates
        .iter()
        .filter(|p| !prev_sync.contains_key(*p))
        .cloned()
        .collect()
}

/// Add a synthetic `existing` entry for every sync-log path the HEAD
/// pass didn't cover, using the source size we recorded at copy
/// time. This is the trick that lets the HEAD optimisation skip
/// sync-log-known files entirely: when `should_skip` later compares
/// the seeded size against the live `src_size`, an unchanged source
/// matches and skips via the first arm, a changed source falls
/// through to the etag-match arm and gets re-copied.
///
/// Trade-off: a file the user manually deleted from the destination
/// stays "skipped" here. That's the price of avoiding the round-trip
/// — the destination's ground truth is no longer checked for
/// sync-log-known files when we take this path.
pub(super) fn seed_existing_with_sync_log(
    existing: &mut HashMap<String, u64>,
    prev_sync: &HashMap<String, sync_log::Entry>,
) {
    for (rel, entry) in prev_sync {
        existing
            .entry(rel.clone())
            .or_insert(entry.source_size as u64);
    }
}

/// Decide whether a source file is already present at the destination.
///
/// When the sync log has an entry for this path, that's the precise
/// signal — it checks both size *and* etag, so it catches same-size
/// content changes (and lets the HEAD-optimised path seed `existing`
/// with a placeholder size without false-positive skips on etag
/// mismatch). When no sync-log entry exists (first run, lost log,
/// pre-sync-log copies), we fall back to size-only matching against
/// whatever `existing` reports.
pub(super) fn should_skip(
    existing: &HashMap<String, u64>,
    prev_sync: &HashMap<String, sync_log::Entry>,
    relative: &str,
    src_size: u64,
    src_etag: Option<&str>,
) -> bool {
    if let Some(entry) = prev_sync.get(relative) {
        return existing.contains_key(relative) && entry.matches_source(src_size, src_etag);
    }
    if let Some(&dst_size) = existing.get(relative)
        && dst_size == src_size
    {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(rel: &str, source_size: i64, etag: Option<&str>) -> sync_log::Entry {
        sync_log::Entry {
            relative_path: rel.to_string(),
            source_size,
            source_etag: etag.map(String::from),
            file_type: "parquet".to_string(),
            rewritten: false,
        }
    }

    /// The point of this filter: a 100k-file table where 99980 are
    /// carry-overs from prior runs shouldn't push us into the full
    /// LIST path — the residue is what determines the right strategy.
    #[test]
    fn uncertain_paths_keeps_only_files_missing_from_sync_log() {
        let candidates = vec![
            "data/a.parquet".to_string(),
            "data/b.parquet".to_string(),
            "data/c.parquet".to_string(),
        ];
        let mut sync = HashMap::new();
        sync.insert("data/a.parquet".to_string(), entry("data/a.parquet", 10, None));
        sync.insert("data/c.parquet".to_string(), entry("data/c.parquet", 30, None));

        let residue = uncertain_paths(&candidates, &sync);
        assert_eq!(residue, vec!["data/b.parquet".to_string()]);
    }

    #[test]
    fn uncertain_paths_returns_everything_when_sync_log_empty() {
        let candidates = vec!["x".to_string(), "y".to_string()];
        let residue = uncertain_paths(&candidates, &HashMap::new());
        assert_eq!(residue, candidates);
    }

    /// Seeded entries let `should_skip` recognise sync-log-known
    /// files even though we didn't HEAD them: an unchanged source
    /// matches the seeded size and skips via the first arm.
    #[test]
    fn seeded_sync_log_entries_make_should_skip_recognise_unchanged_source() {
        let mut sync = HashMap::new();
        sync.insert("a".to_string(), entry("a", 100, Some("etag-a")));
        let mut existing = HashMap::new();
        seed_existing_with_sync_log(&mut existing, &sync);

        assert!(should_skip(&existing, &sync, "a", 100, Some("etag-a")));
    }

    /// And when the source changed (different size or etag), the
    /// seeded entry must NOT cause a false skip — copy should still
    /// be attempted.
    #[test]
    fn seeded_sync_log_does_not_skip_when_source_changed() {
        let mut sync = HashMap::new();
        sync.insert("a".to_string(), entry("a", 100, Some("etag-old")));
        let mut existing = HashMap::new();
        seed_existing_with_sync_log(&mut existing, &sync);

        // Different size — should not skip.
        assert!(!should_skip(&existing, &sync, "a", 200, Some("etag-old")));
        // Same size, different etag — should not skip either.
        assert!(!should_skip(&existing, &sync, "a", 100, Some("etag-new")));
    }

    /// HEAD results take priority over the seeded sync-log fallback
    /// — they're ground truth, the sync log is a best guess.
    #[test]
    fn seeding_does_not_overwrite_existing_head_results() {
        let mut sync = HashMap::new();
        sync.insert("a".to_string(), entry("a", 100, None));
        let mut existing = HashMap::new();
        // Pretend HEAD returned a different size (e.g. dst was
        // modified externally since the sync log was written).
        existing.insert("a".to_string(), 999);

        seed_existing_with_sync_log(&mut existing, &sync);
        assert_eq!(existing.get("a"), Some(&999));
    }

    /// Sync log presence must take priority over the size-only
    /// match: a file whose source size happens to equal what's on
    /// dst, but whose etag has shifted, must not be skipped. The
    /// old logic (first arm = size match, second arm = sync log)
    /// would false-positive here.
    #[test]
    fn sync_log_check_overrides_coincidental_size_match() {
        let mut sync = HashMap::new();
        sync.insert("a".to_string(), entry("a", 100, Some("etag-old")));
        let mut existing = HashMap::new();
        existing.insert("a".to_string(), 100);

        // src is 100 bytes (matches existing[a]) but etag changed,
        // so the content is different — we must copy.
        assert!(!should_skip(&existing, &sync, "a", 100, Some("etag-new")));
    }

    /// Falling back to size match when no sync-log entry exists keeps
    /// behaviour for pre-sync-log copies and runs whose log was lost.
    #[test]
    fn size_only_fallback_when_no_sync_log_entry() {
        let existing: HashMap<String, u64> = [("a".to_string(), 100)].into_iter().collect();
        let sync: HashMap<String, sync_log::Entry> = HashMap::new();
        assert!(should_skip(&existing, &sync, "a", 100, None));
        assert!(!should_skip(&existing, &sync, "a", 200, None));
    }
}
