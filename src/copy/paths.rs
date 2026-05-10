//! Path-string helpers shared across the copy machinery.
//!
//! Iceberg's relative paths (under a table prefix) and the absolute
//! `s3://…` URLs embedded in metadata both need consistent handling.
//! These helpers are tiny on purpose — the rules they encode show up
//! in enough places that having them named makes call sites readable.

use object_store::path::Path as ObjPath;

/// Strip a known leading `<src-prefix>/` from an `ObjPath` and return
/// the remainder. We never want a leading slash in our relative keys
/// because they're concatenated with `<dst-prefix>/`.
pub(super) fn relative_under<'a>(path: &'a ObjPath, prefix: &str) -> &'a str {
    path.as_ref()
        .strip_prefix(prefix)
        .unwrap_or(path.as_ref())
        .trim_start_matches('/')
}

/// Strip a `s3://bucket/<table>/` URL prefix from an absolute path stored
/// inside metadata. Falls back to returning the input on mismatch — the
/// caller should treat that as a copy-as-is signal rather than aborting.
pub(super) fn strip_url_prefix<'a>(src_url: &str, abs: &'a str) -> &'a str {
    let prefix = format!("{src_url}/");
    abs.strip_prefix(prefix.as_str())
        .unwrap_or_else(|| abs.strip_prefix(src_url).unwrap_or(abs).trim_start_matches('/'))
}

pub(super) fn filename_of(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_url_prefix_handles_with_and_without_trailing_slash() {
        let src = "s3://bucket/iceberg/ns/uuid";
        assert_eq!(
            strip_url_prefix(src, "s3://bucket/iceberg/ns/uuid/metadata/foo.avro"),
            "metadata/foo.avro"
        );
        // No trailing slash form (paranoid input).
        assert_eq!(strip_url_prefix(src, "s3://bucket/iceberg/ns/uuid"), "");
    }

    #[test]
    fn filename_of_returns_last_segment() {
        assert_eq!(
            filename_of("metadata/00005-uuid.metadata.json"),
            "00005-uuid.metadata.json"
        );
        assert_eq!(filename_of("nofileinpath"), "nofileinpath");
    }

    #[test]
    fn relative_under_strips_prefix_and_leading_slash() {
        let p = ObjPath::from("ns/uuid/data/file.parquet");
        assert_eq!(relative_under(&p, "ns/uuid"), "data/file.parquet");
    }
}
