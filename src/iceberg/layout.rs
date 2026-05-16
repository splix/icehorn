//! Helpers for navigating the on-S3 directory layout of an Iceberg
//! warehouse: listing one level deep, recognising UUID-named
//! namespace/table directories, and stripping trailing slashes from
//! common-prefix paths. Used by both `show tables` (which classifies
//! location levels for display) and `copy` (which trusts the caller
//! passed a namespace and just enumerates its tables).

use anyhow::{Context, Result};
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use uuid::Uuid;

/// List the immediate subdirectory names below `prefix`, stripped of
/// their trailing `/`. Common-prefix entries with no non-empty trailing
/// segment are silently dropped — they can't be meaningfully named.
pub async fn list_subdirs(
    store: &(impl ObjectStore + ?Sized),
    prefix: &str,
) -> Result<Vec<String>> {
    let listing = store
        .list_with_delimiter(Some(&ObjPath::from(prefix)))
        .await
        .with_context(|| format!("listing {prefix}"))?;
    Ok(listing
        .common_prefixes
        .iter()
        .filter_map(|cp| last_segment(cp.as_ref()).map(String::from))
        .collect())
}

/// Wrap [`list_subdirs`] and keep only entries whose leaf name parses
/// as a UUID — the common case for `copy` and `show tables`, both of
/// which want to enumerate Iceberg namespaces or tables and discard
/// sibling junk (logs, docs, partial imports). Non-UUID entries are
/// logged at debug level so unexpected layouts are still discoverable.
pub async fn list_uuids(
    store: &(impl ObjectStore + ?Sized),
    prefix: &str,
) -> Result<Vec<String>> {
    let subdirs = list_subdirs(store, prefix).await?;
    Ok(subdirs
        .into_iter()
        .filter(|leaf| {
            if is_uuid(leaf) {
                true
            } else {
                tracing::debug!(prefix = %prefix, leaf = %leaf, "skipping non-UUID subdir");
                false
            }
        })
        .collect())
}

/// Last non-empty `/`-separated segment of a path. Returns `None` for
/// empty strings or paths consisting only of slashes — callers decide
/// what default to substitute.
pub fn last_segment(path: &str) -> Option<&str> {
    path.rsplit('/').find(|s| !s.is_empty())
}

/// Iceberg's non-Hadoop layout names every namespace and table by UUID.
pub fn is_uuid(s: &str) -> bool {
    Uuid::parse_str(s).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};

    #[test]
    fn last_segment_strips_trailing_slashes() {
        assert_eq!(last_segment("ns/uuid/"), Some("uuid"));
        assert_eq!(last_segment("ns/uuid"), Some("uuid"));
        assert_eq!(last_segment("just-one"), Some("just-one"));
        assert_eq!(last_segment(""), None);
        assert_eq!(last_segment("///"), None);
    }

    #[test]
    fn is_uuid_accepts_standard_form_and_rejects_others() {
        assert!(is_uuid("019d3bc6-1e12-79e3-a0a2-caa2f0aec0b7"));
        assert!(!is_uuid("notes"));
        assert!(!is_uuid(""));
    }

    #[tokio::test]
    async fn list_subdirs_returns_leaf_names_without_trailing_slash() {
        let store = InMemory::new();
        let prefix = "iceberg/ns";
        for key in [
            "iceberg/ns/019d3bc6-1e12-79e3-a0a2-caa2f0aec0b7/data.parquet",
            "iceberg/ns/019d9daf-e720-7131-ba9a-b771f5c2b2f1/data.parquet",
            "iceberg/ns/notes/readme.txt",
        ] {
            store
                .put(&ObjPath::from(key), PutPayload::from_static(b""))
                .await
                .unwrap();
        }

        let mut subdirs = list_subdirs(&store, prefix).await.unwrap();
        subdirs.sort();
        assert_eq!(
            subdirs,
            vec![
                "019d3bc6-1e12-79e3-a0a2-caa2f0aec0b7".to_string(),
                "019d9daf-e720-7131-ba9a-b771f5c2b2f1".to_string(),
                "notes".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn list_uuids_filters_out_non_uuid_subdirs() {
        let store = InMemory::new();
        let prefix = "iceberg/ns";
        for key in [
            "iceberg/ns/019d3bc6-1e12-79e3-a0a2-caa2f0aec0b7/data.parquet",
            "iceberg/ns/019d9daf-e720-7131-ba9a-b771f5c2b2f1/data.parquet",
            "iceberg/ns/notes/readme.txt",
        ] {
            store
                .put(&ObjPath::from(key), PutPayload::from_static(b""))
                .await
                .unwrap();
        }

        let mut uuids = list_uuids(&store, prefix).await.unwrap();
        uuids.sort();
        assert_eq!(
            uuids,
            vec![
                "019d3bc6-1e12-79e3-a0a2-caa2f0aec0b7".to_string(),
                "019d9daf-e720-7131-ba9a-b771f5c2b2f1".to_string(),
            ]
        );
    }
}
