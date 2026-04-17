use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::cli::CopyArgs;
use crate::config::S3Config;
use crate::s3_url::S3Location;
use crate::sync_log;

pub async fn run(args: CopyArgs) -> Result<()> {
    let src_config = S3Config::from_file(&args.from_config)?;
    let dst_config = S3Config::from_file(&args.to_config)?;

    let src = S3Location::parse(&args.from)?;
    let dst = S3Location::parse(&args.to)?;
    let src_prefix = src.prefix;
    let dst_prefix = dst.prefix;

    let src_store = Arc::new(src_config.build_store(&src.bucket)?);
    let dst_store = Arc::new(dst_config.build_store(&dst.bucket)?);

    // Pre-scan destination: collect relative_path → size for all existing objects
    tracing::info!("scanning destination for existing files…");
    let dst_prefix_obj = ObjPath::from(dst_prefix.as_str());
    let existing: HashMap<String, u64> = dst_store
        .list(Some(&dst_prefix_obj))
        .map(|r| {
            r.map(|obj| {
                let rel = obj
                    .location
                    .as_ref()
                    .strip_prefix(dst_prefix.as_str())
                    .unwrap_or(obj.location.as_ref())
                    .trim_start_matches('/')
                    .to_string();
                (rel, obj.size)
            })
        })
        .try_collect()
        .await
        .context("failed to list destination objects")?;
    tracing::info!("destination has {} existing files", existing.len());

    // Load sync logs so we can skip rewritten metadata that hasn't changed at source
    let prev_sync = match sync_log::load_all(&*dst_store, &dst_prefix).await {
        Ok(log) => log,
        Err(e) => {
            tracing::warn!(error = %e, "failed to load sync logs, proceeding without");
            HashMap::new()
        }
    };
    let prev_sync = Arc::new(prev_sync);

    let shutdown = shutdown::Shutdown::new().context("registering shutdown signals")?;

    // Start the sync log writer — entries flow to it via a channel, and it
    // commits to S3 on Close or on shutdown (independently).
    let (sync_tx, sync_writer) = sync_log::start_writer(
        Arc::clone(&dst_store) as Arc<dyn ObjectStore>,
        dst_prefix.clone(),
    );

    let existing = Arc::new(existing);
    let copied = Arc::new(AtomicU64::new(0));
    let skipped = Arc::new(AtomicU64::new(0));
    let from_url = Arc::<str>::from(args.from.as_str());
    let to_url = Arc::<str>::from(args.to.as_str());
    let dst_prefix = Arc::<str>::from(dst_prefix.as_str());

    let src_prefix_obj = ObjPath::from(src_prefix.as_str());
    let parallelism = args.copy_parallel;

    let copy_all = src_store
        .list(Some(&src_prefix_obj))
        .map(|r| r.context("failed to list source objects"))
        .try_for_each_concurrent(parallelism, |obj| {
            let src_store = Arc::clone(&src_store);
            let dst_store = Arc::clone(&dst_store);
            let existing = Arc::clone(&existing);
            let prev_sync = Arc::clone(&prev_sync);
            let copied = Arc::clone(&copied);
            let skipped = Arc::clone(&skipped);
            let sync_tx = sync_tx.clone();
            let from_url = Arc::clone(&from_url);
            let to_url = Arc::clone(&to_url);
            let dst_prefix = Arc::clone(&dst_prefix);
            let src_prefix = src_prefix.clone();

            async move {
                let src_path = &obj.location;

                let relative = src_path
                    .as_ref()
                    .strip_prefix(src_prefix.as_str())
                    .unwrap_or(src_path.as_ref())
                    .trim_start_matches('/');
                let dst_path = ObjPath::from(format!("{dst_prefix}/{relative}"));

                let file_type = classify(src_path.as_ref());

                // Skip verbatim files that already exist with the same size
                if let Some(&dst_size) = existing.get(relative) {
                    if dst_size == obj.size {
                        skipped.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                }

                // Skip rewritten metadata if the source hasn't changed (sync log)
                // and the destination file still exists
                if file_type.is_rewritten() && existing.contains_key(relative) {
                    if let Some(entry) = prev_sync.get(relative) {
                        if entry.matches_source(obj.size, obj.e_tag.as_deref()) {
                            skipped.fetch_add(1, Ordering::Relaxed);
                            return Ok(());
                        }
                    }
                }

                tracing::debug!(?file_type, src = %src_path, dst = %dst_path, "copying");

                let result = match file_type {
                    FileType::Parquet | FileType::Other => {
                        copy_verbatim(&*src_store, src_path, &*dst_store, &dst_path).await
                    }
                    FileType::Json => {
                        copy_json(
                            &*src_store, src_path, &*dst_store, &dst_path, &from_url, &to_url,
                        )
                        .await
                    }
                    FileType::Avro => {
                        copy_avro(
                            &*src_store, src_path, &*dst_store, &dst_path, &from_url, &to_url,
                        )
                        .await
                    }
                };

                match result {
                    Ok(()) => {
                        copied.fetch_add(1, Ordering::Relaxed);
                        let _ = sync_tx.send(sync_log::Msg::Entry(sync_log::Entry {
                            relative_path: relative.to_string(),
                            source_size: obj.size as i64,
                            source_etag: obj.e_tag.clone(),
                            file_type: file_type.as_str().to_string(),
                            rewritten: file_type.is_rewritten(),
                        })).await;
                    }
                    Err(e) if e.downcast_ref::<object_store::Error>().is_some_and(|oe| matches!(oe, object_store::Error::NotFound { .. })) => {
                        tracing::warn!(src = %src_path, "source file disappeared during copy, skipping");
                        skipped.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        tracing::warn!(src = %src_path, error = %e, "failed to copy file, skipping");
                        skipped.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(())
            }
        });

    // Race the copy against a shutdown signal so we still save progress on Ctrl+C.
    // The sync log writer monitors shutdown independently and will commit on its own.
    let copy_error = tokio::select! {
        result = copy_all => result.err(),
        _ = shutdown.signalled() => {
            tracing::info!("interrupted — waiting for sync log to flush");
            None
        }
    };

    // On normal completion, tell the writer to commit. On error, drop
    // the sender without Close so the writer abandons the file.
    if copy_error.is_none() {
        let _ = sync_tx.send(sync_log::Msg::Close).await;
    }
    drop(sync_tx);
    sync_writer.join().await?;

    let copied = copied.load(Ordering::Relaxed);
    let skipped = skipped.load(Ordering::Relaxed);
    tracing::info!("done — {copied} copied, {skipped} skipped");

    if let Some(e) = copy_error {
        return Err(e);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// File-type classification
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum FileType {
    Parquet,
    Json,
    Avro,
    Other,
}

impl FileType {
    fn as_str(self) -> &'static str {
        match self {
            FileType::Parquet => "parquet",
            FileType::Json => "json",
            FileType::Avro => "avro",
            FileType::Other => "other",
        }
    }

    fn is_rewritten(self) -> bool {
        matches!(self, FileType::Json | FileType::Avro)
    }
}

fn classify(path: &str) -> FileType {
    if path.ends_with(".parquet") {
        FileType::Parquet
    } else if path.ends_with(".json") {
        FileType::Json
    } else if path.ends_with(".avro") {
        FileType::Avro
    } else {
        FileType::Other
    }
}

// ---------------------------------------------------------------------------
// Verbatim copy (parquet, version-hint.text, …)
// ---------------------------------------------------------------------------

async fn copy_verbatim(
    src: &impl ObjectStore,
    src_path: &ObjPath,
    dst: &impl ObjectStore,
    dst_path: &ObjPath,
) -> Result<()> {
    let data = src.get(src_path).await?.bytes().await?;
    dst.put(dst_path, data.into()).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// JSON metadata — simple string replacement of the source prefix
// ---------------------------------------------------------------------------

async fn copy_json(
    src: &impl ObjectStore,
    src_path: &ObjPath,
    dst: &impl ObjectStore,
    dst_path: &ObjPath,
    from_url: &str,
    to_url: &str,
) -> Result<()> {
    use flate2::read::GzDecoder;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::{Read, Write};

    let raw = src.get(src_path).await?.bytes().await?;
    let from_url = from_url.to_string();
    let to_url = to_url.to_string();

    let output = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
        // Detect gzip by magic bytes (1f 8b)
        let is_gzip = raw.len() >= 2 && raw[0] == 0x1f && raw[1] == 0x8b;

        let text = if is_gzip {
            let mut decoder = GzDecoder::new(&raw[..]);
            let mut s = String::new();
            decoder
                .read_to_string(&mut s)
                .context("failed to decompress gzip JSON metadata")?;
            s
        } else {
            String::from_utf8(raw.to_vec()).context("JSON metadata is not valid UTF-8")?
        };

        let rewritten = text.replace(&from_url, &to_url);

        if is_gzip {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(rewritten.as_bytes())?;
            Ok(encoder.finish()?)
        } else {
            Ok(rewritten.into_bytes())
        }
    })
    .await??;

    dst.put(dst_path, Bytes::from(output).into()).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Avro metadata — parse records, rewrite path fields, re-encode
// ---------------------------------------------------------------------------

async fn copy_avro(
    src: &impl ObjectStore,
    src_path: &ObjPath,
    dst: &impl ObjectStore,
    dst_path: &ObjPath,
    from_url: &str,
    to_url: &str,
) -> Result<()> {
    let data = src.get(src_path).await?.bytes().await?;
    let from_url = from_url.to_string();
    let to_url = to_url.to_string();

    let rewritten = tokio::task::spawn_blocking(move || rewrite_avro(&data, &from_url, &to_url))
        .await??;

    dst.put(dst_path, Bytes::from(rewritten).into()).await?;
    Ok(())
}

fn rewrite_avro(data: &[u8], from: &str, to: &str) -> Result<Vec<u8>> {
    use apache_avro::{Reader, Writer};

    let reader = Reader::new(data)?;
    let schema = reader.writer_schema().clone();

    // TODO: preserve original codec (snappy / zstd) from the source file
    let mut writer = Writer::new(&schema, Vec::new());

    for record in reader {
        let mut value = record?;
        rewrite_paths(&mut value, from, to);
        writer.append_value_ref(&value)?;
    }

    let bytes = writer.into_inner()?;
    Ok(bytes)
}

/// Recursively walk an Avro value tree and replace S3 path prefixes in the
/// fields that Iceberg uses to store absolute locations:
///
/// - `manifest_path` (in manifest-list Avro files)
/// - `file_path`     (inside the `data_file` struct in manifest Avro files)
fn rewrite_paths(value: &mut apache_avro::types::Value, from: &str, to: &str) {
    use apache_avro::types::Value;

    match value {
        Value::Record(fields) => {
            for (name, val) in fields.iter_mut() {
                match name.as_str() {
                    "manifest_path" | "file_path" => {
                        if let Value::String(s) = val {
                            if s.contains(from) {
                                *s = s.replace(from, to);
                            }
                        }
                    }
                    _ => rewrite_paths(val, from, to),
                }
            }
        }
        Value::Union(_, inner) => rewrite_paths(inner, from, to),
        Value::Array(items) => {
            for item in items {
                rewrite_paths(item, from, to);
            }
        }
        Value::Map(map) => {
            for val in map.values_mut() {
                rewrite_paths(val, from, to);
            }
        }
        _ => {}
    }
}
