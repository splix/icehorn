//! `show tables` — list Iceberg tables under an S3 location, printing
//! `<namespace-uuid> | <table-uuid>` rows.
//!
//! The location can be at three levels and we detect which:
//!   * **table**     — has a `metadata/` subdir
//!   * **namespace** — UUID-named subdirs that themselves have `metadata/`
//!   * **root**      — UUID-named subdirs (namespaces) of UUID-named
//!     subdirs (tables)
//!
//! We don't have access to a catalog, so namespaces and tables are
//! identified only by their UUID directory names — which is what real
//! non-Hadoop Iceberg layouts on S3 already look like.

use anyhow::{Context, Result};
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use uuid::Uuid;

use crate::cli::LocationArgs;
use crate::config::S3Config;
use crate::s3_url::S3Location;

/// Placeholder for a UUID we couldn't recover from the path — e.g. the
/// user pointed at a non-UUID-named directory. Surfaced rather than
/// silently elided so the row still lines up with its sibling.
const UNKNOWN: &str = "(unknown)";

pub async fn run(args: LocationArgs) -> Result<()> {
    let config = S3Config::from_file(&args.config)?;
    let location = S3Location::parse(&args.location)?;
    let store = config.build_store(&location.bucket)?;

    let pairs = discover(&store, &location.prefix).await?;
    print_pairs(&pairs);
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct TableEntry {
    namespace: String,
    table: String,
}

/// Walk one or two levels deep to figure out what `location_prefix` is,
/// then return the (namespace, table) UUID pairs underneath. Costs at
/// most one listing for a table location, two for a namespace, and
/// `1 + N` for a root with N namespaces.
async fn discover(
    store: &(impl ObjectStore + ?Sized),
    location_prefix: &str,
) -> Result<Vec<TableEntry>> {
    let subdirs = list_subdirs(store, location_prefix).await?;

    if subdirs.iter().any(|s| s == "metadata") {
        let (namespace, table) = split_ns_and_table(location_prefix);
        return Ok(vec![TableEntry { namespace, table }]);
    }

    let uuid_subdirs: Vec<String> = subdirs.into_iter().filter(|s| is_uuid(s)).collect();
    if uuid_subdirs.is_empty() {
        return Ok(Vec::new());
    }

    // Probe the first UUID child to disambiguate namespace vs root.
    let probe_path = format!("{location_prefix}/{}", uuid_subdirs[0]);
    let probe = list_subdirs(store, &probe_path).await?;
    let probe_is_table = probe.iter().any(|s| s == "metadata");

    if probe_is_table {
        let namespace = last_segment(location_prefix).to_string();
        return Ok(uuid_subdirs
            .into_iter()
            .map(|table| TableEntry {
                namespace: namespace.clone(),
                table,
            })
            .collect());
    }

    let mut entries = Vec::new();
    for namespace in uuid_subdirs {
        let ns_path = format!("{location_prefix}/{namespace}");
        let inner = list_subdirs(store, &ns_path).await?;
        for table in inner.into_iter().filter(|s| is_uuid(s)) {
            entries.push(TableEntry {
                namespace: namespace.clone(),
                table,
            });
        }
    }
    Ok(entries)
}

async fn list_subdirs(
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
        .filter_map(|cp| {
            cp.as_ref()
                .rsplit('/')
                .find(|s| !s.is_empty())
                .map(String::from)
        })
        .collect())
}

fn is_uuid(s: &str) -> bool {
    Uuid::parse_str(s).is_ok()
}

fn last_segment(p: &str) -> &str {
    p.rsplit('/').find(|s| !s.is_empty()).unwrap_or(UNKNOWN)
}

/// Split a `<prefix>/<ns>/<table>` path into its namespace and table
/// segments. Returns `(UNKNOWN, UNKNOWN)` if the path doesn't have at
/// least two segments — we still emit a row so the user sees that
/// something was found, just without identifying UUIDs.
fn split_ns_and_table(p: &str) -> (String, String) {
    let mut segs = p.rsplit('/').filter(|s| !s.is_empty());
    let table = segs.next().unwrap_or(UNKNOWN).to_string();
    let namespace = segs.next().unwrap_or(UNKNOWN).to_string();
    (namespace, table)
}

fn print_pairs(pairs: &[TableEntry]) {
    if pairs.is_empty() {
        println!("No tables found.");
        return;
    }

    let h_ns = "namespace";
    let h_tbl = "table";
    let w_ns = pairs
        .iter()
        .map(|p| p.namespace.len())
        .chain(std::iter::once(h_ns.len()))
        .max()
        .unwrap();
    let w_tbl = pairs
        .iter()
        .map(|p| p.table.len())
        .chain(std::iter::once(h_tbl.len()))
        .max()
        .unwrap();

    println!("{:<w_ns$} | {:<w_tbl$}", h_ns, h_tbl);
    println!("{:-<w_ns$}-+-{:-<w_tbl$}", "", "");
    for p in pairs {
        println!("{:<w_ns$} | {:<w_tbl$}", p.namespace, p.table);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};

    const NS_A: &str = "019d3bc6-1e12-79e3-a0a2-caa2f0aec0b7";
    const NS_B: &str = "019d9daf-e720-7131-ba9a-b771f5c2b2f1";
    const TABLE_1: &str = "019d4111-2222-3333-4444-555555555555";
    const TABLE_2: &str = "019d6666-7777-8888-9999-aaaaaaaaaaaa";
    const TABLE_3: &str = "019dbbbb-cccc-dddd-eeee-ffffffffffff";

    async fn put(store: &InMemory, key: &str) {
        store
            .put(&ObjPath::from(key), PutPayload::from_static(b""))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn detects_table_level_when_metadata_is_a_direct_subdir() {
        let store = InMemory::new();
        put(&store, &format!("iceberg/{NS_A}/{TABLE_1}/metadata/00001.metadata.json")).await;

        let pairs = discover(&store, &format!("iceberg/{NS_A}/{TABLE_1}"))
            .await
            .unwrap();

        assert_eq!(
            pairs,
            vec![TableEntry {
                namespace: NS_A.to_string(),
                table: TABLE_1.to_string(),
            }]
        );
    }

    #[tokio::test]
    async fn detects_namespace_level_and_lists_each_table() {
        let store = InMemory::new();
        put(&store, &format!("iceberg/{NS_A}/{TABLE_1}/metadata/00001.metadata.json")).await;
        put(&store, &format!("iceberg/{NS_A}/{TABLE_2}/metadata/00001.metadata.json")).await;
        // Junk sibling — must be ignored, since it isn't UUID-named.
        put(&store, &format!("iceberg/{NS_A}/notes/readme.txt")).await;

        let mut pairs = discover(&store, &format!("iceberg/{NS_A}")).await.unwrap();
        pairs.sort_by(|a, b| a.table.cmp(&b.table));

        assert_eq!(
            pairs,
            vec![
                TableEntry { namespace: NS_A.to_string(), table: TABLE_1.to_string() },
                TableEntry { namespace: NS_A.to_string(), table: TABLE_2.to_string() },
            ]
        );
    }

    #[tokio::test]
    async fn detects_root_level_and_walks_namespaces() {
        let store = InMemory::new();
        put(&store, &format!("iceberg/{NS_A}/{TABLE_1}/metadata/00001.metadata.json")).await;
        put(&store, &format!("iceberg/{NS_B}/{TABLE_2}/metadata/00001.metadata.json")).await;
        put(&store, &format!("iceberg/{NS_B}/{TABLE_3}/metadata/00001.metadata.json")).await;

        let mut pairs = discover(&store, "iceberg").await.unwrap();
        pairs.sort_by(|a, b| (a.namespace.as_str(), a.table.as_str())
            .cmp(&(b.namespace.as_str(), b.table.as_str())));

        assert_eq!(
            pairs,
            vec![
                TableEntry { namespace: NS_A.to_string(), table: TABLE_1.to_string() },
                TableEntry { namespace: NS_B.to_string(), table: TABLE_2.to_string() },
                TableEntry { namespace: NS_B.to_string(), table: TABLE_3.to_string() },
            ]
        );
    }

    #[tokio::test]
    async fn returns_empty_when_no_uuid_subdirs() {
        let store = InMemory::new();
        put(&store, "iceberg/notes/readme.txt").await;
        assert!(discover(&store, "iceberg").await.unwrap().is_empty());
    }

    #[test]
    fn split_ns_and_table_handles_short_paths() {
        assert_eq!(
            split_ns_and_table("iceberg/ns-uuid/table-uuid"),
            ("ns-uuid".to_string(), "table-uuid".to_string())
        );
        // Trailing slash shouldn't shift the segments — we filter empties.
        assert_eq!(
            split_ns_and_table("iceberg/ns-uuid/table-uuid/"),
            ("ns-uuid".to_string(), "table-uuid".to_string())
        );
        // Bare path with no namespace parent: namespace falls back to UNKNOWN.
        assert_eq!(
            split_ns_and_table("table-uuid"),
            (UNKNOWN.to_string(), "table-uuid".to_string())
        );
    }
}
