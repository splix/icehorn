//! Walk an Iceberg snapshot's manifest tree and collect referenced file
//! paths.
//!
//! Iceberg embeds absolute paths at every layer (manifest list →
//! manifest files → data/delete files). To copy "only what the current
//! snapshot needs", we read each manifest list and manifest Avro file
//! and pull out the absolute paths in `manifest_path` and
//! `data_file.file_path` fields. See
//! `reference/iceberg-naming-conventions.md` and
//! `reference/iceberg-s3-migration.md` §3.2 / §3.3.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use apache_avro::types::Value;
use apache_avro::Reader;
use futures::stream::{self, StreamExt};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};

/// All files referenced by a manifest list, separated by what they are.
#[derive(Debug, Default)]
pub struct ManifestTree {
    /// Absolute paths of every manifest file linked from the manifest
    /// list (data and delete manifests alike).
    pub manifest_files: Vec<String>,
    /// Absolute paths of every data / delete file recorded inside those
    /// manifests.
    pub content_files: Vec<String>,
}

/// Progress signal emitted during [`walk_many`]. Walking happens in two
/// phases (manifest lists, then deduped manifests); the caller decides
/// how to render each. `total` is `None` for the second phase until the
/// first phase finishes — it isn't known until then.
#[derive(Debug, Clone, Copy)]
pub enum WalkEvent {
    ManifestList { done: usize, total: usize },
    Manifest { done: usize, total: usize },
}

/// Type alias for the optional progress callback. Implementations should
/// be cheap and non-blocking — they're invoked from the I/O loop.
pub type OnProgress<'a> = &'a (dyn Fn(WalkEvent) + Send + Sync);

/// Read a manifest list Avro and return every `manifest_path` value.
///
/// The error chain deliberately omits the path: callers already have
/// it on hand and log it as a structured field, and `object_store`'s
/// own error message embeds the full URL, so adding it here would
/// produce a third copy on one log line.
pub async fn read_manifest_list(
    store: &dyn ObjectStore,
    path: &ObjPath,
) -> Result<Vec<String>> {
    let bytes = store
        .get(path)
        .await
        .context("reading manifest list")?
        .bytes()
        .await?;
    let bytes = bytes.to_vec();

    tokio::task::spawn_blocking(move || extract_manifest_paths(&bytes))
        .await
        .context("manifest list parser panicked")?
}

/// Read a manifest Avro and return every `data_file.file_path` value.
/// See [`read_manifest_list`] for why the path is omitted from the
/// error chain.
pub async fn read_manifest(store: &dyn ObjectStore, path: &ObjPath) -> Result<Vec<String>> {
    let bytes = store
        .get(path)
        .await
        .context("reading manifest")?
        .bytes()
        .await?;
    let bytes = bytes.to_vec();

    tokio::task::spawn_blocking(move || extract_file_paths(&bytes))
        .await
        .context("manifest parser panicked")?
}

/// Walk many manifest lists concurrently and return the deduped union of
/// every file they reference.
///
/// In long-lived Iceberg tables a snapshot's manifest list overlaps
/// almost entirely with its parent's, so the same `*-mN.avro` file is
/// referenced by hundreds or thousands of snapshots. Reading each
/// manifest list and manifest sequentially turns into N×M serial S3
/// GETs and quickly dominates wall clock time on tables with many
/// snapshots. We deduplicate after reading the lists so each unique
/// manifest is only fetched once, then run both phases concurrently
/// (`parallelism` in flight at a time).
pub async fn walk_many(
    store: Arc<dyn ObjectStore>,
    manifest_list_paths: Vec<ObjPath>,
    parallelism: usize,
    on_progress: Option<OnProgress<'_>>,
) -> Result<ManifestTree> {
    let parallelism = parallelism.max(1);

    let manifest_lists_total = manifest_list_paths.len();
    let manifest_lists_done = AtomicUsize::new(0);

    // Per-item errors are downgraded to warnings: a missing/corrupted
    // manifest list (or manifest) on the source shouldn't abort the
    // whole table copy. Its referenced files simply won't make it into
    // the to-copy list this run; the next run will pick them up if the
    // failure was transient.
    let lists_store = Arc::clone(&store);
    let manifest_path_groups: Vec<Vec<String>> = stream::iter(manifest_list_paths)
        .map(move |path| {
            let store = Arc::clone(&lists_store);
            async move {
                match read_manifest_list(&*store, &path).await {
                    Ok(paths) => Some(paths),
                    Err(e) => {
                        tracing::warn!(
                            path = %path,
                            error = %e,
                            "failed to read manifest list — its data files will be skipped this run"
                        );
                        None
                    }
                }
            }
        })
        .buffer_unordered(parallelism)
        .inspect(|_| {
            if let Some(cb) = on_progress {
                let done = manifest_lists_done.fetch_add(1, Ordering::Relaxed) + 1;
                cb(WalkEvent::ManifestList {
                    done,
                    total: manifest_lists_total,
                });
            }
        })
        .filter_map(|maybe| async move { maybe })
        .collect()
        .await;

    let unique: HashSet<String> = manifest_path_groups.into_iter().flatten().collect();
    let manifest_files: Vec<String> = unique.into_iter().collect();

    let manifests_total = manifest_files.len();
    let manifests_done = AtomicUsize::new(0);

    let manifests_store = Arc::clone(&store);
    let content_groups: Vec<Vec<String>> = stream::iter(manifest_files.clone())
        .map(move |abs| {
            let store = Arc::clone(&manifests_store);
            async move {
                let key = strip_s3_prefix(&abs).unwrap_or(abs.as_str());
                let path = ObjPath::from(key);
                match read_manifest(&*store, &path).await {
                    Ok(files) => Some(files),
                    Err(e) => {
                        tracing::warn!(
                            path = %path,
                            error = %e,
                            "failed to read manifest — its data files will be skipped this run"
                        );
                        None
                    }
                }
            }
        })
        .buffer_unordered(parallelism)
        .inspect(|_| {
            if let Some(cb) = on_progress {
                let done = manifests_done.fetch_add(1, Ordering::Relaxed) + 1;
                cb(WalkEvent::Manifest {
                    done,
                    total: manifests_total,
                });
            }
        })
        .filter_map(|maybe| async move { maybe })
        .collect()
        .await;

    let content_files: Vec<String> = content_groups.into_iter().flatten().collect();

    Ok(ManifestTree {
        manifest_files,
        content_files,
    })
}

/// Strip `s3://bucket/` (or `s3a://`) from a path, returning the key.
/// Returns `None` if the input isn't an s3 URL — callers can fall back
/// to using the input as a key directly.
pub fn strip_s3_prefix(url: &str) -> Option<&str> {
    let rest = url.strip_prefix("s3://").or_else(|| url.strip_prefix("s3a://"))?;
    rest.split_once('/').map(|(_, key)| key)
}

fn extract_manifest_paths(bytes: &[u8]) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for record in Reader::new(bytes).context("opening manifest list avro")? {
        let value = record.context("reading manifest list record")?;
        collect_named(&value, "manifest_path", &mut out);
    }
    Ok(out)
}

fn extract_file_paths(bytes: &[u8]) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for record in Reader::new(bytes).context("opening manifest avro")? {
        let value = record.context("reading manifest record")?;
        collect_named(&value, "file_path", &mut out);
    }
    Ok(out)
}

/// Recursively walk an Avro value tree and collect every `String` value
/// whose enclosing record field has the given name.
///
/// Mirrors the structure of `copy::file::rewrite_paths` — the same
/// schema knowledge applied to extraction instead of mutation.
fn collect_named(value: &Value, target_name: &str, out: &mut Vec<String>) {
    match value {
        Value::Record(fields) => {
            for (name, val) in fields {
                if name == target_name {
                    if let Value::String(s) = unwrap_union(val) {
                        out.push(s.clone());
                    }
                } else {
                    collect_named(val, target_name, out);
                }
            }
        }
        Value::Union(_, inner) => collect_named(inner, target_name, out),
        Value::Array(items) => {
            for item in items {
                collect_named(item, target_name, out);
            }
        }
        Value::Map(map) => {
            for v in map.values() {
                collect_named(v, target_name, out);
            }
        }
        _ => {}
    }
}

/// Avro nullable fields show up as `Union`. Peel one layer so callers
/// can match on the inner concrete value.
fn unwrap_union(value: &Value) -> &Value {
    match value {
        Value::Union(_, inner) => inner,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_s3_prefix_handles_s3_and_s3a() {
        assert_eq!(strip_s3_prefix("s3://b/path/to/file"), Some("path/to/file"));
        assert_eq!(strip_s3_prefix("s3a://b/path/to/file"), Some("path/to/file"));
        assert_eq!(strip_s3_prefix("not-an-s3-url"), None);
        assert_eq!(strip_s3_prefix("s3://bucket-only"), None);
    }

    #[test]
    fn collect_named_finds_field_at_any_depth() {
        let v = Value::Record(vec![
            ("data_file".into(), Value::Record(vec![
                ("file_path".into(), Value::String("a.parquet".into())),
                ("file_size".into(), Value::Long(123)),
            ])),
            ("status".into(), Value::Int(1)),
        ]);
        let mut out = Vec::new();
        collect_named(&v, "file_path", &mut out);
        assert_eq!(out, vec!["a.parquet".to_string()]);
    }

    #[test]
    fn collect_named_handles_union_wrapping() {
        // Avro nullable strings appear as Union(idx, String) at runtime.
        let inner = Value::String("b.parquet".into());
        let v = Value::Record(vec![(
            "file_path".into(),
            Value::Union(1, Box::new(inner)),
        )]);
        let mut out = Vec::new();
        collect_named(&v, "file_path", &mut out);
        assert_eq!(out, vec!["b.parquet".to_string()]);
    }
}
