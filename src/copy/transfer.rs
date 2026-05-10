//! Per-file copy orchestration.
//!
//! Three layers:
//!   * [`run_copy_with_progress`] — wraps a single file copy with the
//!     reporter's `FileStarted`/`FileFinished` lifecycle so the TUI can
//!     show byte-level progress for the file.
//!   * `process_relative` / `process_listed` — turn one input (a
//!     planned relative path or an `ObjectMeta` from a streaming LIST)
//!     into the full skip-or-copy decision plus sync-log accounting.
//!   * [`record_outcome`] / [`finalize_sync`] / [`render_metadata_json`]
//!     — small primitives shared by both modes.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use crate::iceberg::metadata::MetadataFile;
use crate::iceberg::model::Metadata;
use crate::sync_log;
use crate::ui::Reporter;

use super::file::{self, FileType, ProgressFn, copy_avro, copy_json, copy_verbatim};
use super::paths::{filename_of, relative_under};
use super::plan::should_skip;
use super::progress::TableProgress;

/// Everything the per-file workers need beyond the input itself. Bundled
/// to keep `try_for_each_concurrent` closures readable and to avoid
/// passing a dozen arguments through every helper.
pub(super) struct FileTaskContext {
    pub src_store: Arc<dyn ObjectStore>,
    pub dst_store: Arc<dyn ObjectStore>,
    pub existing: Arc<HashMap<String, u64>>,
    pub prev_sync: Arc<HashMap<String, sync_log::Entry>>,
    pub progress: Arc<TableProgress>,
    pub reporter: Reporter,
    pub table_key: String,
    pub sync_tx: tokio::sync::mpsc::Sender<sync_log::Msg>,
    pub src_prefix: String,
    pub dst_prefix: String,
    pub src_url: String,
    pub dst_url: String,
}

/// Process one *planned* relative path (from `--scope latest|<N>`):
/// HEAD the source for size + etag, decide skip-or-copy, then record.
pub(super) async fn process_relative(ctx: &FileTaskContext, relative: String) -> Result<()> {
    let src_path = ObjPath::from(format!("{}/{relative}", ctx.src_prefix));
    let dst_path = ObjPath::from(format!("{}/{relative}", ctx.dst_prefix));

    let head = match ctx.src_store.head(&src_path).await {
        Ok(h) => h,
        Err(object_store::Error::NotFound { .. }) => {
            tracing::warn!(src = %src_path, "referenced file missing on source, skipping");
            ctx.progress.record_skipped();
            return Ok(());
        }
        Err(e) => return Err(anyhow::Error::from(e)).context("HEAD source"),
    };

    if should_skip(
        &ctx.existing,
        &ctx.prev_sync,
        &relative,
        head.size,
        head.e_tag.as_deref(),
    ) {
        ctx.progress.record_skipped();
        return Ok(());
    }

    copy_one(ctx, &src_path, &dst_path, &relative, head.size, head.e_tag.as_deref()).await;
    Ok(())
}

/// Process one *listed* source object (from `--scope all`): we already
/// know size + etag from the LIST entry, so no HEAD is needed.
pub(super) async fn process_listed(ctx: &FileTaskContext, obj: ObjectMeta) -> Result<()> {
    // Discovery streams files in one at a time — grow the queue total
    // as we see them so the bar expands during listing.
    ctx.progress.add_total(1);

    let relative = relative_under(&obj.location, &ctx.src_prefix).to_string();
    let dst_path = ObjPath::from(format!("{}/{relative}", ctx.dst_prefix));

    if should_skip(
        &ctx.existing,
        &ctx.prev_sync,
        &relative,
        obj.size,
        obj.e_tag.as_deref(),
    ) {
        ctx.progress.record_skipped();
        return Ok(());
    }

    copy_one(ctx, &obj.location, &dst_path, &relative, obj.size, obj.e_tag.as_deref()).await;
    Ok(())
}

/// Shared tail of [`process_relative`] / [`process_listed`]: pick the
/// right copy primitive, run it, and record the outcome.
async fn copy_one(
    ctx: &FileTaskContext,
    src_path: &ObjPath,
    dst_path: &ObjPath,
    relative: &str,
    src_size: u64,
    src_etag: Option<&str>,
) {
    let file_type = file::classify(src_path.as_ref());
    let res = run_copy_with_progress(
        &*ctx.src_store,
        src_path,
        &*ctx.dst_store,
        dst_path,
        file_type,
        &ctx.src_url,
        &ctx.dst_url,
        &ctx.reporter,
        &ctx.table_key,
        relative,
        src_size,
    )
    .await;
    record_outcome(
        res,
        src_path,
        relative,
        src_size,
        src_etag,
        file_type,
        &ctx.progress,
        &ctx.sync_tx,
    )
    .await;
}

/// Wrap the file copy with the reporter's per-file lifecycle so a
/// progress bar appears, ticks, and disappears for every actively
/// copied file. `FileFinished` must fire even on error so the TUI
/// doesn't strand a bar on screen.
#[allow(clippy::too_many_arguments)]
async fn run_copy_with_progress(
    src_store: &dyn ObjectStore,
    src_path: &ObjPath,
    dst_store: &dyn ObjectStore,
    dst_path: &ObjPath,
    file_type: FileType,
    src_url: &str,
    dst_url: &str,
    reporter: &Reporter,
    table_key: &str,
    relative: &str,
    src_size: u64,
) -> Result<()> {
    let id = reporter.file_started(table_key, filename_of(relative), src_size);
    let result = run_copy(
        src_store, src_path, dst_store, dst_path, file_type, src_url, dst_url, reporter, id,
    )
    .await;
    reporter.file_finished(id);
    result
}

#[allow(clippy::too_many_arguments)]
async fn run_copy(
    src_store: &dyn ObjectStore,
    src_path: &ObjPath,
    dst_store: &dyn ObjectStore,
    dst_path: &ObjPath,
    file_type: FileType,
    src_url: &str,
    dst_url: &str,
    reporter: &Reporter,
    file_id: u64,
) -> Result<()> {
    tracing::trace!(path = %src_path, "Copying file");

    let progress: Option<Box<ProgressFn>> = if reporter.is_active() {
        let reporter = reporter.clone();
        Some(Box::new(move |bytes| reporter.file_progress(file_id, bytes)))
    } else {
        None
    };
    let progress = progress.as_deref();

    match file_type {
        FileType::Parquet | FileType::Other => {
            copy_verbatim(src_store, src_path, dst_store, dst_path, progress).await
        }
        FileType::Json => {
            copy_json(src_store, src_path, dst_store, dst_path, src_url, dst_url, progress).await
        }
        FileType::Avro => {
            copy_avro(src_store, src_path, dst_store, dst_path, src_url, dst_url, progress).await
        }
    }
}

/// On success, increment the copied counter and append a sync-log entry.
/// On error, log + count as skipped — a single bad file shouldn't fail
/// the whole table copy.
#[allow(clippy::too_many_arguments)]
async fn record_outcome(
    res: Result<()>,
    src_path: &ObjPath,
    relative: &str,
    src_size: u64,
    src_etag: Option<&str>,
    file_type: FileType,
    progress: &TableProgress,
    sync_tx: &tokio::sync::mpsc::Sender<sync_log::Msg>,
) {
    match res {
        Ok(()) => {
            progress.record_copied();
            let _ = sync_tx
                .send(sync_log::Msg::Entry(sync_log::Entry {
                    relative_path: relative.to_string(),
                    source_size: src_size as i64,
                    source_etag: src_etag.map(|s| s.to_string()),
                    file_type: file_type.as_str().to_string(),
                    rewritten: file_type.is_rewritten(),
                }))
                .await;
        }
        Err(e)
            if e.downcast_ref::<object_store::Error>()
                .is_some_and(|oe| matches!(oe, object_store::Error::NotFound { .. })) =>
        {
            tracing::warn!(src = %src_path, "source file disappeared during copy, skipping");
            progress.record_skipped();
        }
        Err(e) => {
            tracing::warn!(src = %src_path, error = %e, "failed to copy file, skipping");
            progress.record_skipped();
        }
    }
}

pub(super) async fn finalize_sync(
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

/// Re-serialize the in-memory `Metadata` after we've stripped snapshots
/// the destination won't have. The trailing prefix-replace is a blunt
/// instrument that catches every absolute path in fields we model
/// *and* fields we don't (schemas with `s3://` doc strings, custom
/// properties, …).
pub(super) fn render_metadata_json(
    meta: &Metadata,
    chosen: &MetadataFile,
    src_url: &str,
    dst_url: &str,
) -> Result<Bytes> {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

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
