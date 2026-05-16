//! `copy` — copy Iceberg tables from one S3 namespace to another.
//!
//! `--from` is expected to point at a *namespace* prefix that contains
//! one or more tables as UUID-named subdirectories (each with a
//! `metadata/` subdir of its own). The orchestrator below lists those
//! UUID subdirs and spawns a `TableCopy` per table; whether each one
//! is a real Iceberg table is decided inside the per-table pipeline,
//! so a slow probe blocks only its own copy rather than the namespace.
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
use object_store::ObjectStore;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::cli::CopyArgs;
use crate::config::S3Config;
use crate::iceberg::layout::list_uuids;
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
    let table_count = tables.len();
    tracing::info!(
        count = table_count,
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

    let copies = tables.into_iter().map(|uuid| TableCopy {
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
    let total_failed: u64 = result.iter().map(|s| s.failed).sum();
    tracing::info!(
        tables = table_count,
        copied = total_copied,
        skipped = total_skipped,
        failed = total_failed,
        "namespace copy complete"
    );
    Ok(())
}

/// Spawn each `TableCopy` as its own tokio task so they're scheduled
/// across the runtime's worker threads — discovery I/O and the work
/// between awaits (manifest parsing, hashmap building) overlap for
/// every table at once, not just S3 round-trips on a single task. The
/// file-copy phase is still gated by the shared semaphore inside
/// `TableCopy` so we never exceed `--tables.parallel` simultaneous
/// transfers.
///
/// The outer shutdown future short-circuits the whole orchestration on
/// Ctrl+C; remaining tasks are aborted, and each `TableCopy` finalises
/// its own sync log when its future is dropped.
async fn run_concurrent(
    copies: impl IntoIterator<Item = TableCopy>,
    shutdown: &shutdown::Shutdown,
) -> Result<Vec<TableStats>> {
    let mut in_flight: JoinSet<(String, Result<TableStats>)> = JoinSet::new();
    for copy in copies {
        in_flight.spawn(run_one(copy));
    }
    let mut results = Vec::new();

    loop {
        tokio::select! {
            _ = shutdown.signalled() => {
                tracing::info!("interrupted — abandoning in-flight table copies");
                in_flight.abort_all();
                return Ok(results);
            }
            next = in_flight.join_next() => {
                match next {
                    Some(Ok((prefix, Ok(stats)))) => {
                        tracing::info!(
                            table = %prefix,
                            copied = stats.copied,
                            skipped = stats.skipped,
                            failed = stats.failed,
                            "table done"
                        );
                        results.push(stats);
                    }
                    Some(Ok((prefix, Err(e)))) => {
                        return Err(e.context(format!("table {prefix}")));
                    }
                    Some(Err(join_err)) if join_err.is_cancelled() => {
                        tracing::debug!("table task cancelled");
                    }
                    Some(Err(join_err)) => {
                        return Err(anyhow!("table task panicked: {join_err}"));
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

/// List the namespace one level deep and return the UUID-named subdirs.
/// One LIST call against S3; whether each UUID is actually a valid
/// Iceberg table is left to the per-table pipeline so a slow probe
/// blocks only its own copy, not the whole namespace.
///
/// Non-UUID directories are skipped with a debug log — real namespaces
/// sometimes carry sibling junk (logs, docs, partial imports), and one
/// bad subdir shouldn't block the rest.
async fn discover_tables(store: &dyn ObjectStore, namespace_prefix: &str) -> Result<Vec<String>> {
    list_uuids(store, namespace_prefix)
        .await
        .with_context(|| format!("listing namespace {namespace_prefix}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjPath;
    use object_store::{ObjectStoreExt, PutPayload};

    /// `discover_tables` returns every UUID-named subdir without
    /// probing its `metadata/`. Whether each candidate is actually a
    /// usable Iceberg table is decided by the per-table pipeline; junk
    /// directories named with a non-UUID leaf are filtered out here.
    #[tokio::test]
    async fn discovers_uuid_named_subdirs_under_namespace() {
        let store = InMemory::new();
        let ns = "iceberg/ns-uuid";

        let with_metadata = "019d3bc6-1e12-79e3-a0a2-caa2f0aec0b7";
        let without_metadata = "019d9daf-e720-7131-ba9a-b771f5c2b2f1";

        store
            .put(
                &ObjPath::from(format!(
                    "{ns}/{with_metadata}/metadata/00001-65f87f03-6d7e-41be-8dce-c813ffe70937.metadata.json"
                )),
                PutPayload::from_static(b"{}"),
            )
            .await
            .unwrap();

        // UUID dir with no metadata file — still returned; the per-table
        // pipeline is responsible for soft-skipping it later.
        store
            .put(
                &ObjPath::from(format!("{ns}/{without_metadata}/data/file.parquet")),
                PutPayload::from_static(b""),
            )
            .await
            .unwrap();

        // Non-UUID sibling — filtered out here.
        store
            .put(
                &ObjPath::from(format!("{ns}/notes/readme.txt")),
                PutPayload::from_static(b""),
            )
            .await
            .unwrap();

        let mut tables = discover_tables(&store, ns).await.unwrap();
        tables.sort();
        assert_eq!(
            tables,
            vec![with_metadata.to_string(), without_metadata.to_string()]
        );
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
