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

use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use object_store::path::Path as ObjPath;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};

/// Per-chunk progress callback used during the source `get` phase. The
/// argument is the cumulative bytes received so far. Implementations
/// should be fast and non-blocking — they're called from the I/O loop.
pub type ProgressFn = dyn Fn(u64) + Send + Sync;

/// Total GETs we'll issue per file before giving up. Iceberg data files
/// are routinely ~500 MB, so a single connection drop discards a lot
/// of in-flight bytes; `object_store`'s built-in `RetryConfig` only
/// covers the *initial* request, not body-stream interruptions, so
/// we need our own resume loop here.
const MAX_DOWNLOAD_ATTEMPTS: u32 = 16;

/// Delay before the first retry; doubled each subsequent attempt
/// (500 ms, 1 s, 2 s, 4 s) so a flaky upstream gets a chance to
/// recover without us hammering it.
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);

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
///
/// Resumes mid-stream interruptions by re-issuing the GET as a Range
/// request from the byte we last received, so a single TCP reset on a
/// half-finished 500 MB Parquet file doesn't discard the bytes we
/// already pulled. `object_store`'s `RetryConfig` only retries the
/// initial request — once the body stream is live, that layer can't
/// help us, and without resume the file is likely to never finish on
/// a flaky link.
async fn read_with_progress(
    src: &dyn ObjectStore,
    src_path: &ObjPath,
    progress: Option<&ProgressFn>,
) -> Result<Bytes> {
    download_resumable(
        |offset| async move {
            let opts = if offset == 0 {
                GetOptions::default()
            } else {
                GetOptions {
                    range: Some(GetRange::Offset(offset)),
                    ..Default::default()
                }
            };
            let result = src.get_opts(src_path, opts).await?;
            Ok(result.into_stream())
        },
        progress,
    )
    .await
    .with_context(|| format!("downloading {src_path}"))
}

/// Drive an open-stream callable until the stream completes; on
/// transient stream errors, sleep with backoff and re-invoke the
/// callable with the cumulative byte count so it can resume from
/// there. `NotFound` short-circuits without retry — higher layers
/// already treat "source gone" as a deliberate "retry on next run"
/// outcome rather than a transient failure.
///
/// Kept generic over the stream source so the resume loop can be
/// unit-tested without a real `ObjectStore`.
async fn download_resumable<F, Fut, S>(open: F, progress: Option<&ProgressFn>) -> Result<Bytes>
where
    F: Fn(u64) -> Fut,
    Fut: Future<Output = std::result::Result<S, object_store::Error>>,
    S: Stream<Item = std::result::Result<Bytes, object_store::Error>> + Unpin,
{
    let mut buf = BytesMut::new();
    let mut copied: u64 = 0;
    let mut last_err: Option<object_store::Error> = None;

    for attempt in 1..=MAX_DOWNLOAD_ATTEMPTS {
        if attempt > 1 {
            let delay = backoff(attempt - 1);
            tracing::warn!(
                attempt,
                copied,
                error = ?last_err,
                "resuming download from byte {copied} after {delay:?}"
            );
            tokio::time::sleep(delay).await;
        }

        let stream = match open(copied).await {
            Ok(s) => s,
            Err(e) if matches!(e, object_store::Error::NotFound { .. }) => {
                return Err(anyhow::Error::from(e));
            }
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        };

        match drain_stream(stream, &mut buf, &mut copied, progress).await {
            Ok(()) => return Ok(buf.freeze()),
            Err(e) => last_err = Some(e),
        }
    }

    let err = last_err.expect("loop always records an error before exit");
    Err(anyhow::Error::from(err).context(format!(
        "download failed after {MAX_DOWNLOAD_ATTEMPTS} attempts at byte {copied}"
    )))
}

/// Pull every chunk from one stream into `buf`, ticking `progress`
/// with the running total. Returns the first stream error so the
/// caller can decide whether to resume.
async fn drain_stream<S>(
    mut stream: S,
    buf: &mut BytesMut,
    copied: &mut u64,
    progress: Option<&ProgressFn>,
) -> std::result::Result<(), object_store::Error>
where
    S: Stream<Item = std::result::Result<Bytes, object_store::Error>> + Unpin,
{
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        *copied += chunk.len() as u64;
        if let Some(cb) = progress {
            cb(*copied);
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(())
}

fn backoff(retry_index: u32) -> Duration {
    let exp = retry_index.saturating_sub(1).min(4);
    INITIAL_BACKOFF * (1u32 << exp)
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

#[cfg(test)]
mod tests {
    //! Resume-loop tests. We exercise `download_resumable` directly
    //! with hand-rolled stream factories so the test can stage exact
    //! failure sequences (mid-stream errors, NotFound, total
    //! exhaustion) — much cheaper than scaffolding a failing
    //! `ObjectStore` impl, and it pins the property we actually care
    //! about: that the next attempt is invoked with the byte offset
    //! we already have.

    use super::*;
    use futures::stream;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    type ChunkResult = std::result::Result<Bytes, object_store::Error>;
    type ChunkStream = Pin<Box<dyn Stream<Item = ChunkResult> + Send>>;

    fn boxed(chunks: Vec<ChunkResult>) -> ChunkStream {
        Box::pin(stream::iter(chunks))
    }

    fn transport_err(msg: &str) -> object_store::Error {
        object_store::Error::Generic {
            store: "test",
            source: msg.to_string().into(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn returns_full_bytes_on_first_try() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_c = Arc::clone(&calls);
        let bytes = download_resumable(
            move |offset| {
                let calls = Arc::clone(&calls_c);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(offset, 0);
                    Ok::<_, object_store::Error>(boxed(vec![
                        Ok(Bytes::from_static(b"hello ")),
                        Ok(Bytes::from_static(b"world")),
                    ]))
                }
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(&bytes[..], b"hello world");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// The whole reason this code exists: a mid-stream failure must
    /// resume from the byte we already buffered, not re-download from
    /// zero. The assertion on `offset` is the load-bearing one.
    #[tokio::test(start_paused = true)]
    async fn resumes_from_received_offset_after_stream_error() {
        let attempt = Arc::new(AtomicUsize::new(0));
        let attempt_c = Arc::clone(&attempt);
        let bytes = download_resumable(
            move |offset| {
                let attempt = Arc::clone(&attempt_c);
                async move {
                    let n = attempt.fetch_add(1, Ordering::SeqCst);
                    let chunks = match n {
                        0 => {
                            assert_eq!(offset, 0);
                            vec![Ok(Bytes::from_static(b"hello ")), Err(transport_err("reset"))]
                        }
                        1 => {
                            assert_eq!(offset, 6);
                            vec![Ok(Bytes::from_static(b"world"))]
                        }
                        _ => panic!("unexpected attempt {n}"),
                    };
                    Ok::<_, object_store::Error>(boxed(chunks))
                }
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(&bytes[..], b"hello world");
        assert_eq!(attempt.load(Ordering::SeqCst), 2);
    }

    /// Stream errors that never resolve must terminate after the cap
    /// rather than loop forever — the table copy depends on this to
    /// move on to other files instead of stalling.
    #[tokio::test(start_paused = true)]
    async fn gives_up_after_max_attempts() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_c = Arc::clone(&calls);
        let err = download_resumable(
            move |_offset| {
                let calls = Arc::clone(&calls_c);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, object_store::Error>(boxed(vec![Err(transport_err("reset"))]))
                }
            },
            None,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("download failed"));
        assert_eq!(calls.load(Ordering::SeqCst), MAX_DOWNLOAD_ATTEMPTS as usize);
    }

    /// `NotFound` is a deliberate "file gone" signal, not a transient
    /// failure — retrying it would just spin until the cap. Higher
    /// layers convert it into "skip this file, retry on next run".
    #[tokio::test(start_paused = true)]
    async fn not_found_is_not_retried() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_c = Arc::clone(&calls);
        let err = download_resumable(
            move |_offset| {
                let calls = Arc::clone(&calls_c);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err::<ChunkStream, _>(object_store::Error::NotFound {
                        path: "x".into(),
                        source: "gone".to_string().into(),
                    })
                }
            },
            None,
        )
        .await
        .unwrap_err();
        assert!(
            err.downcast_ref::<object_store::Error>()
                .is_some_and(|e| matches!(e, object_store::Error::NotFound { .. })),
            "expected NotFound, got: {err:?}",
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// Initial-GET errors (no body stream yet) must also be retried —
    /// otherwise transient connect-time failures during the brief
    /// window between attempts would still fail the file.
    #[tokio::test(start_paused = true)]
    async fn retries_when_opening_stream_fails() {
        let attempt = Arc::new(AtomicUsize::new(0));
        let attempt_c = Arc::clone(&attempt);
        let bytes = download_resumable(
            move |offset| {
                let attempt = Arc::clone(&attempt_c);
                async move {
                    let n = attempt.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        assert_eq!(offset, 0);
                        Err(transport_err("connect refused"))
                    } else {
                        Ok(boxed(vec![Ok(Bytes::from_static(b"ok"))]))
                    }
                }
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(&bytes[..], b"ok");
        assert_eq!(attempt.load(Ordering::SeqCst), 2);
    }

    /// Progress callback must reflect cumulative bytes across all
    /// resume attempts — the TUI bar would otherwise jump backwards
    /// when a resume kicked in.
    #[tokio::test(start_paused = true)]
    async fn progress_is_cumulative_across_resumes() {
        let attempt = Arc::new(AtomicUsize::new(0));
        let attempt_c = Arc::clone(&attempt);
        let observed = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
        let observed_c = Arc::clone(&observed);
        let cb: Box<ProgressFn> = Box::new(move |n| observed_c.lock().unwrap().push(n));

        download_resumable(
            move |offset| {
                let attempt = Arc::clone(&attempt_c);
                async move {
                    let n = attempt.fetch_add(1, Ordering::SeqCst);
                    let chunks = match n {
                        0 => {
                            assert_eq!(offset, 0);
                            vec![Ok(Bytes::from_static(b"abc")), Err(transport_err("reset"))]
                        }
                        1 => {
                            assert_eq!(offset, 3);
                            vec![Ok(Bytes::from_static(b"de"))]
                        }
                        _ => panic!("unexpected attempt {n}"),
                    };
                    Ok::<_, object_store::Error>(boxed(chunks))
                }
            },
            Some(&*cb),
        )
        .await
        .unwrap();

        assert_eq!(*observed.lock().unwrap(), vec![3, 5]);
    }
}
