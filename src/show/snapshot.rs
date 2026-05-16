//! `show snapshot` — read the latest metadata.json and print the current
//! snapshot plus a few preceding ones.

use std::collections::HashMap;

use anyhow::{Context, Result};
use jiff::Timestamp;

use crate::cli::TableArgs;
use crate::config::S3Config;
use crate::iceberg::metadata::load_latest;
use crate::iceberg::model::Snapshot;
use crate::s3_url::S3Location;

/// How deep we walk the `parent-snapshot-id` chain beyond the current
/// snapshot. Three is enough to understand recent write activity without
/// dumping a full history.
const PREVIOUS_SNAPSHOTS_TO_SHOW: usize = 3;

pub async fn run(args: TableArgs) -> Result<()> {
    let config = S3Config::from_file(&args.config)?;
    let location = S3Location::parse(&args.location)?;
    let store = config.build_store(&location.bucket)?;

    let loaded = load_latest(&store, &location.prefix)
        .await?
        .context("no <NNNNN>-<uuid>.metadata.json files found under metadata/")?;
    let file = loaded.file;
    let meta = loaded.meta;

    println!("Metadata file    : {}", file.path.filename().unwrap_or("n/a"));
    println!("Metadata version : {}", file.version);
    println!("Metadata id      : {}", file.uuid);
    println!("Format version   : {}", meta.format_version);
    if let Some(uuid) = &meta.table_uuid {
        println!("Table UUID       : {uuid}");
    }

    let by_id: HashMap<i64, &Snapshot> = meta
        .snapshots
        .iter()
        .map(|s| (s.snapshot_id, s))
        .collect();

    let Some(current) = meta.current_snapshot() else {
        println!();
        println!("No current snapshot (empty table).");
        return Ok(());
    };

    println!();
    println!("Current snapshot:");
    print_snapshot(&by_id, current.snapshot_id, "  ");

    println!();
    println!("Previous snapshots:");
    let mut parent = current.parent_snapshot_id;
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

    println!("{indent}ID                : {}", snap.snapshot_id);
    println!("{indent}Timestamp         : {}", format_timestamp(snap.timestamp_ms));
    if let Some(op) = snap.summary.get("operation") {
        println!("{indent}Operation         : {op}");
    }
    for (label, key) in [
        ("Added records     ", "added-records"),
        ("Deleted records   ", "deleted-records"),
        ("Added data files  ", "added-data-files"),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_timestamp_is_iso8601_in_utc() {
        assert_eq!(format_timestamp(1700000000000), "2023-11-14T22:13:20Z");
    }
}
