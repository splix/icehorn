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

pub async fn run(args: CopyArgs) -> Result<()> {
    let src_config = S3Config::from_file(&args.from_config)?;
    let dst_config = S3Config::from_file(&args.to_config)?;

    let (src_bucket, src_prefix) = parse_s3_url(&args.from)?;
    let (dst_bucket, dst_prefix) = parse_s3_url(&args.to)?;

    let src_store = Arc::new(src_config.build_store(&src_bucket)?);
    let dst_store = Arc::new(dst_config.build_store(&dst_bucket)?);

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

    let existing = Arc::new(existing);
    let copied = Arc::new(AtomicU64::new(0));
    let skipped = Arc::new(AtomicU64::new(0));
    let from_url = Arc::<str>::from(args.from.as_str());
    let to_url = Arc::<str>::from(args.to.as_str());
    let dst_prefix = Arc::<str>::from(dst_prefix.as_str());

    let src_prefix_obj = ObjPath::from(src_prefix.as_str());
    let parallelism = args.copy_parallel;

    src_store
        .list(Some(&src_prefix_obj))
        .map(|r| r.context("failed to list source objects"))
        .try_for_each_concurrent(parallelism, |obj| {
            let src_store = Arc::clone(&src_store);
            let dst_store = Arc::clone(&dst_store);
            let existing = Arc::clone(&existing);
            let copied = Arc::clone(&copied);
            let skipped = Arc::clone(&skipped);
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

                // Skip files that already exist at the destination with the same size
                if let Some(&dst_size) = existing.get(relative) {
                    if dst_size == obj.size {
                        skipped.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                }

                let file_type = classify(src_path.as_ref());
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
        })
        .await?;

    let copied = copied.load(Ordering::Relaxed);
    let skipped = skipped.load(Ordering::Relaxed);
    tracing::info!("done — {copied} copied, {skipped} skipped");
    Ok(())
}

// ---------------------------------------------------------------------------
// S3 URL helpers
// ---------------------------------------------------------------------------

fn parse_s3_url(url: &str) -> Result<(String, String)> {
    let rest = url
        .strip_prefix("s3://")
        .or_else(|| url.strip_prefix("s3a://"))
        .with_context(|| format!("URL must start with s3:// or s3a://: {url}"))?;
    let (bucket, path) = rest
        .split_once('/')
        .with_context(|| format!("URL must contain a path after bucket: {url}"))?;
    Ok((bucket.to_string(), path.trim_end_matches('/').to_string()))
}

// ---------------------------------------------------------------------------
// File-type classification
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum FileType {
    Parquet,
    Json,
    Avro,
    Other,
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
