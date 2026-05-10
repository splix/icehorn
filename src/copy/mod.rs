//! `copy` — copy Iceberg tables from one S3 namespace to another.
//!
//! `--from` is expected to point at a *namespace* prefix that contains
//! one or more tables as UUID-named subdirectories (each with a
//! `metadata/` subdir of its own). The orchestrator below discovers the
//! tables, validates each one looks like a real Iceberg table, then
//! spawns a `TableCopy` per table and runs them concurrently.
//!
//! Per-table behavior — including which files are picked for `latest` /
//! `all` / `<version>` scopes, sync-log handling, and parallel file
//! transfer — lives in `table::TableCopy`.

mod file;
mod paths;
mod plan;
mod progress;
mod table;
mod transfer;

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use uuid::Uuid;

use tokio::sync::Semaphore;

use crate::cli::CopyArgs;
use crate::config::S3Config;
use crate::iceberg::metadata::find_latest;
use crate::s3_url::S3Location;
use crate::ui::Reporter;

use table::{TableCopy, TableStats};

pub async fn run(args: CopyArgs, reporter: Reporter) -> Result<()> {
    let src_config = S3Config::from_file(&args.from_config)?;
    let dst_config = S3Config::from_file(&args.to_config)?;

    let src = S3Location::parse(&args.from)?;
    let dst = S3Location::parse(&args.to)?;

    let src_store: Arc<dyn ObjectStore> = Arc::new(src_config.build_store(&src.bucket)?);
    let dst_store: Arc<dyn ObjectStore> = Arc::new(dst_config.build_store(&dst.bucket)?);

    tracing::info!(namespace = %args.from, "discovering tables");
    reporter.global_status("discovering tables");
    let tables = discover_tables(&*src_store, &src.prefix).await?;
    reporter.clear_global_status();
    if tables.is_empty() {
        return Err(anyhow!(
            "no Iceberg tables found under {} — expected UUID-named subdirectories with a metadata/ folder",
            args.from
        ));
    }
    tracing::info!(
        count = tables.len(),
        scope = ?args.scope,
        tables_parallel = args.tables_parallel,
        copy_parallel = args.copy_parallel,
        "starting namespace copy"
    );

    let shutdown = shutdown::Shutdown::new().context("registering shutdown signals")?;

    let namespace_label = src
        .prefix
        .rsplit('/')
        .find(|s| !s.is_empty())
        .unwrap_or(&src.prefix)
        .to_string();

    // `tables.parallel` now caps only the file-copy phase. Discovery
    // (manifest walks, dst scans) runs concurrently for every table —
    // small tables don't have to wait for large tables to finish
    // scanning before they can start copying.
    let copy_gate = Arc::new(Semaphore::new(args.tables_parallel.max(1)));

    let copies = tables.iter().map(|uuid| TableCopy {
        src_store: Arc::clone(&src_store),
        dst_store: Arc::clone(&dst_store),
        src_prefix: format!("{}/{uuid}", src.prefix),
        dst_prefix: format!("{}/{uuid}", dst.prefix),
        src_url: format!("{}/{uuid}", args.from),
        dst_url: format!("{}/{uuid}", args.to),
        scope: args.scope,
        parallelism: args.copy_parallel,
        reporter: reporter.clone(),
        table_key: uuid.clone(),
        namespace_label: namespace_label.clone(),
        copy_gate: Arc::clone(&copy_gate),
    });

    let result = run_concurrent(copies, &shutdown).await?;

    let total_copied: u64 = result.iter().map(|s| s.copied).sum();
    let total_skipped: u64 = result.iter().map(|s| s.skipped).sum();
    tracing::info!(
        tables = tables.len(),
        copied = total_copied,
        skipped = total_skipped,
        "namespace copy complete"
    );
    Ok(())
}

/// Schedule every `TableCopy` to start immediately. Each one self-paces:
/// discovery runs in parallel across the whole namespace, while the
/// file-copy phase is gated by the shared semaphore inside `TableCopy`
/// so we never exceed `--tables.parallel` simultaneous transfers.
///
/// The outer shutdown future short-circuits the whole orchestration on
/// Ctrl+C; each `TableCopy` finalises its own sync log when its future
/// is dropped.
async fn run_concurrent(
    copies: impl IntoIterator<Item = TableCopy>,
    shutdown: &shutdown::Shutdown,
) -> Result<Vec<TableStats>> {
    let mut in_flight: FuturesUnordered<_> = copies.into_iter().map(run_one).collect();
    let mut results = Vec::new();

    loop {
        tokio::select! {
            _ = shutdown.signalled() => {
                tracing::info!("interrupted — abandoning in-flight table copies");
                return Ok(results);
            }
            next = in_flight.next() => {
                match next {
                    Some((prefix, Ok(stats))) => {
                        tracing::info!(table = %prefix, copied = stats.copied, skipped = stats.skipped, "table done");
                        results.push(stats);
                    }
                    Some((prefix, Err(e))) => {
                        return Err(e.context(format!("table {prefix}")));
                    }
                    None => break,
                }
            }
        }
    }

    Ok(results)
}

async fn run_one(copy: TableCopy) -> (String, Result<TableStats>) {
    let prefix = copy.src_prefix.clone();
    tracing::info!(table = %prefix, scope = ?copy.scope, "table copy starting");
    let res = copy.run().await;
    (prefix, res)
}

/// List the namespace one level deep, validate each subdir is a
/// UUID-named table with a `metadata/` containing at least one parseable
/// `<NNNNN>-<uuid>.metadata.json`, and return the table UUIDs in
/// listing order. Each found table is logged with its latest metadata
/// version so a human watching the run can sanity-check the source.
///
/// Non-UUID directories and UUID directories without a usable
/// `metadata/` are skipped with a warning rather than failing the run —
/// real namespaces sometimes carry sibling junk (logs, docs, partial
/// imports), and one bad subdir shouldn't block the rest.
async fn discover_tables(store: &dyn ObjectStore, namespace_prefix: &str) -> Result<Vec<String>> {
    let prefix = ObjPath::from(namespace_prefix);
    let listing = store
        .list_with_delimiter(Some(&prefix))
        .await
        .with_context(|| format!("listing namespace {namespace_prefix}"))?;

    let mut tables = Vec::new();
    for cp in &listing.common_prefixes {
        let leaf = cp.as_ref().rsplit('/').find(|s| !s.is_empty()).unwrap_or("");
        if Uuid::parse_str(leaf).is_err() {
            tracing::debug!(path = %cp, "skipping non-UUID subdir");
            continue;
        }

        let metadata_prefix = ObjPath::from(format!("{namespace_prefix}/{leaf}/metadata"));
        match find_latest(store, &metadata_prefix).await {
            Ok(Some(latest)) => {
                tracing::info!(
                    table = %leaf,
                    version = latest.version,
                    metadata = %latest.path.filename().unwrap_or(""),
                    "found table"
                );
                tables.push(leaf.to_string());
            }
            Ok(None) => {
                tracing::warn!(table = %leaf, "UUID dir has no metadata.json — skipping");
            }
            Err(e) => {
                tracing::warn!(table = %leaf, error = %e, "could not check metadata/ — skipping");
            }
        }
    }

    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};

    /// `discover_tables` should return only UUID-named subdirs that have
    /// a parseable metadata.json. Junk dirs and empty UUID dirs are
    /// silently skipped, since real namespaces accumulate both.
    #[tokio::test]
    async fn discovers_only_valid_tables_under_namespace() {
        let store = InMemory::new();
        let ns = "iceberg/ns-uuid";

        let valid_uuid = "019d3bc6-1e12-79e3-a0a2-caa2f0aec0b7";
        let other_uuid = "019d9daf-e720-7131-ba9a-b771f5c2b2f1";

        // Valid table.
        store
            .put(
                &ObjPath::from(format!(
                    "{ns}/{valid_uuid}/metadata/00001-65f87f03-6d7e-41be-8dce-c813ffe70937.metadata.json"
                )),
                PutPayload::from_static(b"{}"),
            )
            .await
            .unwrap();

        // UUID dir but no metadata file — should be skipped.
        store
            .put(
                &ObjPath::from(format!("{ns}/{other_uuid}/data/file.parquet")),
                PutPayload::from_static(b""),
            )
            .await
            .unwrap();

        // Non-UUID sibling — should be skipped.
        store
            .put(
                &ObjPath::from(format!("{ns}/notes/readme.txt")),
                PutPayload::from_static(b""),
            )
            .await
            .unwrap();

        let tables = discover_tables(&store, ns).await.unwrap();
        assert_eq!(tables, vec![valid_uuid.to_string()]);
    }

    #[tokio::test]
    async fn discovers_tables_returns_empty_when_namespace_has_none() {
        let store = InMemory::new();
        let ns = "iceberg/empty";
        store
            .put(
                &ObjPath::from(format!("{ns}/notes/file.txt")),
                PutPayload::from_static(b""),
            )
            .await
            .unwrap();

        let tables = discover_tables(&store, ns).await.unwrap();
        assert!(tables.is_empty());
    }
}
