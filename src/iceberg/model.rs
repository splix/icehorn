//! Minimal serde model for `metadata.json`.
//!
//! The Iceberg metadata schema is large and evolves between format
//! versions. We only deserialize the fields that the `show` and `copy`
//! commands actually read. Unknown keys are ignored by serde, which keeps
//! us robust to spec evolution.
//!
//! See `reference/iceberg-s3-migration.md` §3.1 for the absolute-path
//! fields that matter when relocating a table.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Metadata {
    #[serde(rename = "format-version")]
    pub format_version: u32,

    #[serde(rename = "table-uuid", skip_serializing_if = "Option::is_none")]
    pub table_uuid: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,

    #[serde(rename = "current-snapshot-id", skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshots: Vec<Snapshot>,

    #[serde(rename = "snapshot-log", default, skip_serializing_if = "Vec::is_empty")]
    pub snapshot_log: Vec<SnapshotLogEntry>,

    #[serde(rename = "metadata-log", default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_log: Vec<MetadataLogEntry>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statistics: Vec<StatisticsFile>,

    #[serde(rename = "partition-statistics", default, skip_serializing_if = "Vec::is_empty")]
    pub partition_statistics: Vec<StatisticsFile>,

    /// Everything else — preserved verbatim on round-trip so we don't drop
    /// fields we don't model (schemas, partition specs, sort orders,
    /// properties, refs, …).
    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Snapshot {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,

    #[serde(rename = "parent-snapshot-id", default, skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,

    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,

    #[serde(rename = "manifest-list", default, skip_serializing_if = "Option::is_none")]
    pub manifest_list: Option<String>,

    #[serde(default)]
    pub summary: HashMap<String, String>,

    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SnapshotLogEntry {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MetadataLogEntry {
    #[serde(rename = "metadata-file")]
    pub metadata_file: String,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StatisticsFile {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "statistics-path")]
    pub statistics_path: String,

    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

impl Metadata {
    /// Resolve the snapshot referenced by `current-snapshot-id`. A freshly
    /// created table has no current snapshot; Iceberg encodes that either
    /// by omitting the field or setting it to `-1`.
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        let id = match self.current_snapshot_id {
            None | Some(-1) => return None,
            Some(id) => id,
        };
        self.snapshots.iter().find(|s| s.snapshot_id == id)
    }
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
        assert_eq!(
            meta.current_snapshot().unwrap().snapshot_id,
            1000000000000000002
        );
    }

    #[test]
    fn current_snapshot_handles_empty_table() {
        let raw = r#"{ "format-version": 2 }"#;
        let meta: Metadata = serde_json::from_str(raw).unwrap();
        assert!(meta.current_snapshot().is_none());

        let raw = r#"{ "format-version": 2, "current-snapshot-id": -1 }"#;
        let meta: Metadata = serde_json::from_str(raw).unwrap();
        assert!(meta.current_snapshot().is_none());
    }

    #[test]
    fn round_trips_unknown_fields() {
        let raw = r#"{
            "format-version": 2,
            "schemas": [{"schema-id": 0, "fields": []}],
            "properties": {"write.format.default": "parquet"}
        }"#;
        let meta: Metadata = serde_json::from_str(raw).unwrap();
        let back = serde_json::to_value(&meta).unwrap();
        assert!(back.get("schemas").is_some());
        assert!(back.get("properties").is_some());
    }
}
