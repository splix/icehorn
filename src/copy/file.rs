//! Single-file copy primitives.
//!
//! Each Iceberg file type needs different handling: data files (parquet,
//! puffin, version-hint, …) copy verbatim, while metadata layers (json,
//! avro) need their embedded absolute S3 paths rewritten to point at the
//! destination location. The classifier and three copy functions below
//! split that responsibility — orchestration lives in `table::TableCopy`.
//!
//! See `reference/iceberg-s3-migration.md` §3 for the full inventory of
//! absolute-path fields that drive these rewrites.

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};

/// Per-chunk progress callback used during the source `get` phase. The
/// argument is the cumulative bytes received so far. Implementations
/// should be fast and non-blocking — they're called from the I/O loop.
pub type ProgressFn = dyn Fn(u64) + Send + Sync;

#[derive(Debug, Clone, Copy)]
pub enum FileType {
    Parquet,
    Json,
    Avro,
    Other,
}

impl FileType {
    pub fn as_str(self) -> &'static str {
        match self {
            FileType::Parquet => "parquet",
            FileType::Json => "json",
            FileType::Avro => "avro",
            FileType::Other => "other",
        }
    }

    /// Files that we modify in flight rather than passing through. Used
    /// by the sync log to know whether a destination object can be reused
    /// when its source size/etag haven't changed.
    pub fn is_rewritten(self) -> bool {
        matches!(self, FileType::Json | FileType::Avro)
    }
}

pub fn classify(path: &str) -> FileType {
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

/// Verbatim copy — used for data files (parquet/orc/avro data) and
/// anything else without embedded paths.
pub async fn copy_verbatim(
    src: &dyn ObjectStore,
    src_path: &ObjPath,
    dst: &dyn ObjectStore,
    dst_path: &ObjPath,
    progress: Option<&ProgressFn>,
) -> Result<()> {
    let data = read_with_progress(src, src_path, progress).await?;
    dst.put(dst_path, data.into()).await?;
    Ok(())
}

/// Drain a `get` stream into one `Bytes`, calling `progress` after each
/// chunk with the running total. Used by every copy variant — the only
/// difference is what the caller does with the bytes afterwards.
async fn read_with_progress(
    src: &dyn ObjectStore,
    src_path: &ObjPath,
    progress: Option<&ProgressFn>,
) -> Result<Bytes> {
    let result = src.get(src_path).await?;
    let mut stream = result.into_stream();
    let mut buf = BytesMut::new();
    let mut copied: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        copied += chunk.len() as u64;
        if let Some(cb) = progress {
            cb(copied);
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf.freeze())
}

/// JSON copy with prefix substitution.
///
/// Iceberg metadata.json files use absolute S3 URLs throughout
/// (`location`, `snapshots[].manifest-list`, `metadata-log[].metadata-file`,
/// `statistics[].statistics-path`). A blunt string replace covers all of
/// them without having to model every field. We sniff gzip rather than
/// trusting the suffix — some engines write `.gz.metadata.json`.
pub async fn copy_json(
    src: &dyn ObjectStore,
    src_path: &ObjPath,
    dst: &dyn ObjectStore,
    dst_path: &ObjPath,
    from_url: &str,
    to_url: &str,
    progress: Option<&ProgressFn>,
) -> Result<()> {
    use flate2::read::GzDecoder;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::{Read, Write};

    let raw = read_with_progress(src, src_path, progress).await?;
    let from_url = from_url.to_string();
    let to_url = to_url.to_string();

    let output = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
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

/// Avro copy with path rewriting — used for manifest lists and
/// manifest files. We parse the records, replace the path prefix in the
/// known fields, and re-encode.
pub async fn copy_avro(
    src: &dyn ObjectStore,
    src_path: &ObjPath,
    dst: &dyn ObjectStore,
    dst_path: &ObjPath,
    from_url: &str,
    to_url: &str,
    progress: Option<&ProgressFn>,
) -> Result<()> {
    let data = read_with_progress(src, src_path, progress).await?;
    let from_url = from_url.to_string();
    let to_url = to_url.to_string();

    let rewritten = tokio::task::spawn_blocking(move || rewrite_avro(&data, &from_url, &to_url))
        .await??;

    dst.put(dst_path, Bytes::from(rewritten).into()).await?;
    Ok(())
}

/// Write pre-built bytes verbatim. Used for the rewritten `metadata.json`
/// emitted by `--scope latest` / `--scope <version>`, which is filtered
/// in-process and shouldn't be re-fetched from the source.
pub async fn put_bytes(
    dst: &dyn ObjectStore,
    dst_path: &ObjPath,
    body: Bytes,
) -> Result<()> {
    dst.put(dst_path, body.into()).await?;
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

/// Recursively walk an Avro value tree and replace S3 path prefixes in
/// the fields that Iceberg uses to store absolute locations:
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
                        if let Value::String(s) = val
                            && s.contains(from)
                        {
                            *s = s.replace(from, to);
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
