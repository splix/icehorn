//! `show snapshot` — read the latest metadata.json and print the current
//! snapshot plus a few preceding ones.

use std::collections::HashMap;

use anyhow::{Context, Result};
use jiff::Timestamp;
use object_store::path::Path as ObjPath;
use serde::Deserialize;

use super::metadata::{find_latest, read_json};
use crate::cli::TableArgs;
use crate::config::S3Config;
use crate::s3_url::S3Location;

/// How deep we walk the `parent-snapshot-id` chain beyond the current
/// snapshot. Three is enough to understand recent write activity without
/// dumping a full history.
const PREVIOUS_SNAPSHOTS_TO_SHOW: usize = 3;

pub async fn run(args: TableArgs) -> Result<()> {
    let config = S3Config::from_file(&args.config)?;
    let location = S3Location::parse(&args.location)?;
    let store = config.build_store(&location.bucket)?;

    let metadata_prefix = ObjPath::from(format!("{}/metadata", location.prefix));
    let latest = find_latest(&store, &metadata_prefix)
        .await?
        .context("no <NNNNN>-<uuid>.metadata.json files found under metadata/")?;

    let bytes = read_json(&store, &latest.path).await?;
    let meta: Metadata =
        serde_json::from_slice(&bytes).context("failed to parse metadata JSON")?;

    println!("Metadata version: {}", latest.version);
    println!("Format version: {}", meta.format_version);
    if let Some(uuid) = &meta.table_uuid {
        println!("Table UUID: {uuid}");
    }

    let by_id: HashMap<i64, &Snapshot> = meta
        .snapshots
        .iter()
        .map(|s| (s.snapshot_id, s))
        .collect();

    // A freshly created table has no current snapshot — Iceberg encodes that
    // either by omitting `current-snapshot-id` or setting it to `-1`.
    let current_id = match meta.current_snapshot_id {
        None | Some(-1) => {
            println!();
            println!("No current snapshot (empty table).");
            return Ok(());
        }
        Some(id) => id,
    };

    println!();
    println!("Current snapshot:");
    print_snapshot(&by_id, current_id, "  ");

    println!();
    println!("Previous snapshots:");
    let mut parent = by_id.get(&current_id).and_then(|s| s.parent_snapshot_id);
    let mut shown = 0;
    while let Some(id) = parent {
        if shown >= PREVIOUS_SNAPSHOTS_TO_SHOW {
            break;
        }
        print_snapshot(&by_id, id, "  ");
        parent = by_id.get(&id).and_then(|s| s.parent_snapshot_id);
        shown += 1;
    }
    if shown == 0 {
        println!("  (none)");
    }

    Ok(())
}

fn print_snapshot(by_id: &HashMap<i64, &Snapshot>, id: i64, indent: &str) {
    // A snapshot referenced from `current-snapshot-id` or via a parent chain
    // may be missing from `snapshots` if it was expired — surface it rather
    // than silently eliding the entry.
    let Some(snap) = by_id.get(&id) else {
        println!("{indent}ID: {id} (expired / not in metadata)");
        return;
    };

    println!("{indent}ID: {}", snap.snapshot_id);
    println!("{indent}Timestamp: {}", format_timestamp(snap.timestamp_ms));
    if let Some(op) = snap.summary.get("operation") {
        println!("{indent}Operation: {op}");
    }
    for (label, key) in [
        ("Added records", "added-records"),
        ("Deleted records", "deleted-records"),
        ("Added data files", "added-data-files"),
        ("Deleted data files", "deleted-data-files"),
    ] {
        if let Some(value) = snap.summary.get(key) {
            println!("{indent}{label}: {value}");
        }
    }
}

fn format_timestamp(ms: i64) -> String {
    Timestamp::from_millisecond(ms)
        .map(|t| t.to_string())
        .unwrap_or_else(|_| format!("{ms} ms since epoch"))
}

// ---------------------------------------------------------------------------
// metadata.json — minimal subset we actually use.
//
// The schema is large and evolves between format versions, so we only
// deserialize the fields needed here. Unknown keys are ignored by serde.
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct Metadata {
    #[serde(rename = "format-version")]
    format_version: u32,
    #[serde(rename = "table-uuid")]
    table_uuid: Option<String>,
    #[serde(rename = "current-snapshot-id")]
    current_snapshot_id: Option<i64>,
    #[serde(default)]
    snapshots: Vec<Snapshot>,
}

#[derive(Deserialize)]
struct Snapshot {
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
    #[serde(rename = "parent-snapshot-id", default)]
    parent_snapshot_id: Option<i64>,
    #[serde(rename = "timestamp-ms")]
    timestamp_ms: i64,
    #[serde(default)]
    summary: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_representative_metadata() {
        // Trimmed real-world metadata.json with two snapshots.
        let raw = r#"{
            "format-version": 2,
            "table-uuid": "019d9daf-e720-7131-ba9a-b771f5c2b2f1",
            "location": "s3://bucket/tbl",
            "last-updated-ms": 1700000050000,
            "last-column-id": 3,
            "current-snapshot-id": 1000000000000000002,
            "snapshots": [
                {
                    "snapshot-id": 1000000000000000001,
                    "timestamp-ms": 1700000000000,
                    "summary": { "operation": "append", "added-records": "100" },
                    "manifest-list": "s3://bucket/tbl/metadata/snap-1.avro"
                },
                {
                    "snapshot-id": 1000000000000000002,
                    "parent-snapshot-id": 1000000000000000001,
                    "timestamp-ms": 1700000050000,
                    "summary": { "operation": "append", "added-records": "250" },
                    "manifest-list": "s3://bucket/tbl/metadata/snap-2.avro"
                }
            ]
        }"#;

        let meta: Metadata = serde_json::from_str(raw).unwrap();
        assert_eq!(meta.format_version, 2);
        assert_eq!(meta.current_snapshot_id, Some(1000000000000000002));
        assert_eq!(meta.snapshots.len(), 2);
        assert_eq!(meta.snapshots[1].parent_snapshot_id, Some(1000000000000000001));
        assert_eq!(meta.snapshots[0].summary.get("operation").unwrap(), "append");
    }

    #[test]
    fn format_timestamp_is_iso8601_in_utc() {
        assert_eq!(format_timestamp(1700000000000), "2023-11-14T22:13:20Z");
    }
}
