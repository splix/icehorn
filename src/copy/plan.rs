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
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;

use crate::iceberg::metadata::{MetadataFile, find_latest};
use crate::iceberg::model::Metadata;
use crate::sync_log;

use super::paths::{filename_of, relative_under, strip_url_prefix};

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

pub(super) async fn pick_metadata(
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

/// Decide whether a source file is already present at the destination.
/// Two routes: same-size verbatim, or rewritten-but-source-unchanged
/// (size+etag match the prior sync log). Anything else is a copy.
pub(super) fn should_skip(
    existing: &HashMap<String, u64>,
    prev_sync: &HashMap<String, sync_log::Entry>,
    relative: &str,
    src_size: u64,
    src_etag: Option<&str>,
) -> bool {
    if let Some(&dst_size) = existing.get(relative)
        && dst_size == src_size
    {
        return true;
    }
    if existing.contains_key(relative)
        && let Some(entry) = prev_sync.get(relative)
        && entry.matches_source(src_size, src_etag)
    {
        return true;
    }
    false
}
