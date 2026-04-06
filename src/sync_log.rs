/// Tracks which files were already processed during previous sync runs,
/// so that unchanged metadata files can be skipped on subsequent copies.
///
/// Each sync run writes a new Avro file under `icehorn/sync/` on the
/// destination S3. Iceberg only uses `data/` and `metadata/` directories,
/// so this location is safe from interference.
///
/// The writer runs as a background tokio task receiving entries through a
/// channel, keeping the Avro serialization off the async runtime via
/// `spawn_blocking`.
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use apache_avro::types::Value;
use apache_avro::{Reader, Schema, Writer as AvroWriter};
use futures::TryStreamExt;
use object_store::buffered::BufWriter;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::io::SyncIoBridge;

/// Sync logs live here, under the table root on the destination bucket.
/// Iceberg only uses `data/` and `metadata/`, so this is safe from interference.
const SYNC_LOG_DIR: &str = "icehorn/sync";

// ---------------------------------------------------------------------------
// Entry — one record in the sync log
// ---------------------------------------------------------------------------

/// Source-side metadata captured when a file is processed, used to detect
/// whether the source has changed since the last sync.
#[derive(Debug, Clone)]
pub struct Entry {
    pub relative_path: String,
    pub source_size: i64,
    pub source_etag: Option<String>,
    pub file_type: String,
    pub rewritten: bool,
}

impl Entry {
    /// Does this entry still describe the same source object?
    pub fn matches_source(&self, size: u64, etag: Option<&str>) -> bool {
        if self.source_size != size as i64 {
            return false;
        }
        match (&self.source_etag, etag) {
            (Some(a), Some(b)) => a == b,
            (None, None) => true,
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Channel messages and writer handle
// ---------------------------------------------------------------------------

/// Messages sent to the background sync log writer.
pub enum Msg {
    /// A file was successfully processed during this sync run.
    Entry(Entry),
    /// Normal completion — finalize and upload the sync log.
    Close,
}

/// Handle to the background writer task. Must be joined to ensure the
/// sync log is flushed to S3 before the process exits.
pub struct WriterHandle {
    task: JoinHandle<Result<()>>,
}

impl WriterHandle {
    /// Wait for the background writer to finish uploading.
    pub async fn join(self) -> Result<()> {
        self.task.await?
    }
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

fn schema() -> Schema {
    Schema::parse_str(
        r#"{
        "type": "record",
        "name": "SyncLogEntry",
        "namespace": "com.pterodator.icehorn",
        "fields": [
            {"name": "relative_path", "type": "string"},
            {"name": "source_size",   "type": "long"},
            {"name": "source_etag",   "type": ["null", "string"], "default": null},
            {"name": "file_type",     "type": "string"},
            {"name": "rewritten",     "type": "boolean"}
        ]
    }"#,
    )
    .expect("sync log schema is valid")
}

// ---------------------------------------------------------------------------
// Load — read all existing sync logs into a lookup table
// ---------------------------------------------------------------------------

/// Read every sync log file from `icehorn/sync/` and merge them into a
/// single map keyed by relative path. Later files overwrite earlier ones,
/// so the result reflects the most recent state.
pub async fn load_all(
    store: &dyn ObjectStore,
    table_prefix: &str,
) -> Result<HashMap<String, Entry>> {
    let log_prefix = ObjPath::from(format!("{table_prefix}/{SYNC_LOG_DIR}"));

    // Collect and sort by name — filenames contain timestamps so this
    // gives chronological order (older entries get overwritten by newer).
    let mut log_files: Vec<_> = store
        .list(Some(&log_prefix))
        .try_collect()
        .await
        .context("listing sync log files")?;
    log_files.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));

    let mut entries = HashMap::new();

    for meta in &log_files {
        if !meta.location.as_ref().ends_with(".avro") {
            continue;
        }
        let data = store
            .get(&meta.location)
            .await
            .with_context(|| format!("reading sync log {}", meta.location))?
            .bytes()
            .await?;

        let reader = Reader::new(&data[..])
            .with_context(|| format!("parsing sync log {}", meta.location))?;

        for record in reader {
            let value: Value = record.context("reading sync log record")?;
            if let Some(entry) = parse_entry(&value) {
                entries.insert(entry.relative_path.clone(), entry);
            }
        }
    }

    if !entries.is_empty() {
        tracing::info!(
            files = log_files.len(),
            entries = entries.len(),
            "loaded sync log"
        );
    }
    Ok(entries)
}

fn parse_entry(value: &Value) -> Option<Entry> {
    let Value::Record(fields) = value else {
        return None;
    };

    let get = |name: &str| fields.iter().find(|(k, _)| k == name).map(|(_, v)| v);

    let relative_path = match get("relative_path")? {
        Value::String(s) => s.clone(),
        _ => return None,
    };
    let source_size = match get("source_size")? {
        Value::Long(n) => *n,
        _ => return None,
    };
    let source_etag = match get("source_etag") {
        Some(Value::Union(_, inner)) => match inner.as_ref() {
            Value::String(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    };
    let file_type = match get("file_type")? {
        Value::String(s) => s.clone(),
        _ => return None,
    };
    let rewritten = match get("rewritten")? {
        Value::Boolean(b) => *b,
        _ => return None,
    };

    Some(Entry {
        relative_path,
        source_size,
        source_etag,
        file_type,
        rewritten,
    })
}

// ---------------------------------------------------------------------------
// Writer — blocking thread that encodes entries straight to S3
// ---------------------------------------------------------------------------

/// Spawn a writer that encodes sync log entries to Avro and streams them
/// to S3 via `BufWriter` + `SyncIoBridge`.  Entries are encoded and
/// flushed immediately — nothing accumulates in memory.
///
/// - Send `Msg::Entry` for each processed file.
/// - Send `Msg::Close` when the copy completes normally.
/// - On shutdown the writer commits independently.
/// - Dropping all senders without `Close` abandons the log (no file created).
pub fn start_writer(
    store: Arc<dyn ObjectStore>,
    table_prefix: String,
) -> (mpsc::Sender<Msg>, WriterHandle) {
    let (tx, rx) = mpsc::channel(256);
    let task = tokio::spawn(writer_run(rx, store, table_prefix));
    (tx, WriterHandle { task })
}

/// Spawns a blocking thread that receives entries from the channel,
/// encodes each one into the Avro writer immediately, and streams bytes
/// to S3 via `BufWriter` + `SyncIoBridge`.
async fn writer_run(
    rx: mpsc::Receiver<Msg>,
    store: Arc<dyn ObjectStore>,
    table_prefix: String,
) -> Result<()> {
    let path = ObjPath::from(format!(
        "{table_prefix}/{SYNC_LOG_DIR}/{}",
        generate_filename()
    ));
    let path_log = path.clone();

    let buf_writer = BufWriter::new(store, path);
    let bridge = SyncIoBridge::new(buf_writer);

    let count = tokio::task::spawn_blocking(move || {
        writer_blocking(rx, bridge)
    })
    .await??;

    if count > 0 {
        tracing::info!(entries = count, path = %path_log, "wrote sync log");
    }
    Ok(())
}

/// Blocking loop: receive → encode → write. Runs on a blocking thread so
/// the sync Avro writer never stalls the async runtime.
fn writer_blocking(
    mut rx: mpsc::Receiver<Msg>,
    bridge: SyncIoBridge<BufWriter>,
) -> Result<u64> {
    let schema = schema();
    let mut avro = AvroWriter::new(&schema, bridge);
    let mut count = 0u64;
    let mut commit = false;

    while let Some(msg) = rx.blocking_recv() {
        match msg {
            Msg::Entry(entry) => {
                append_entry(&mut avro, &entry)?;
                count += 1;
            }
            Msg::Close => {
                commit = true;
                break;
            }
        }
    }

    if commit && count > 0 {
        // Flush last Avro block, then finalize the S3 upload.
        let mut bridge = avro.into_inner().context("finalizing avro")?;
        bridge.shutdown().context("finalizing S3 upload")?;
    }
    // No Close received → BufWriter dropped without shutdown → upload abandoned

    Ok(count)
}

fn append_entry(avro: &mut AvroWriter<SyncIoBridge<BufWriter>>, entry: &Entry) -> Result<()> {
    let etag = match &entry.source_etag {
        Some(s) => Value::Union(1, Box::new(Value::String(s.clone()))),
        None => Value::Union(0, Box::new(Value::Null)),
    };

    let record = Value::Record(vec![
        ("relative_path".into(), Value::String(entry.relative_path.clone())),
        ("source_size".into(), Value::Long(entry.source_size)),
        ("source_etag".into(), etag),
        ("file_type".into(), Value::String(entry.file_type.clone())),
        ("rewritten".into(), Value::Boolean(entry.rewritten)),
    ]);

    avro.append_value_ref(&record)?;
    Ok(())
}

fn generate_filename() -> String {
    let now = jiff::Timestamp::now();
    format!(
        "sync-{}-{:09}.avro",
        now.strftime("%Y%m%d-%H%M%S"),
        now.subsec_nanosecond()
    )
}
