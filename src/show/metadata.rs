//! Locating and reading Iceberg `metadata.json` files on S3.
//!
//! Used by every `show` subcommand: they all need to find the latest
//! `<version>-<uuid>.metadata.json[.gz]` file and, for richer views, read
//! and decompress its contents.

use anyhow::{Context, Result};
use futures::TryStreamExt;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use uuid::Uuid;

/// Fixed width of a hyphenated UUID string (8-4-4-4-12 hex digits + 4 dashes).
const UUID_LEN: usize = 36;

/// The parsed `<version>-<uuid>.metadata.json[.gz]` descriptor used by
/// non-Hadoop catalogs. The UUID here is independent of any UUID in the
/// metadata payload itself (see reference/iceberg-naming-conventions.md).
pub struct MetadataFile {
    pub version: u32,
    pub uuid: Uuid,
    pub path: ObjPath,
}

impl MetadataFile {
    /// Returns `None` for any name that doesn't match the Iceberg metadata
    /// pattern — the `metadata/` directory also contains manifest lists,
    /// manifests, puffin files, and version-hint.text which we must ignore.
    ///
    /// We deliberately don't anchor on a fixed `.metadata.json[.gz]` suffix:
    /// some engines write `<version>-<uuid>.gz.metadata.json` (gzip marker
    /// before the JSON extension). Instead we require a valid UUID directly
    /// after the version, then check that the remainder looks like a
    /// metadata extension — that locks out manifests, puffin files, etc.
    pub fn parse(path: &ObjPath) -> Option<Self> {
        let filename = path.as_ref().rsplit('/').next()?;
        let (version_str, rest) = filename.split_once('-')?;
        // Version is always zero-padded to 5 digits — reject anything else
        // so we don't confuse unrelated filenames with metadata.
        if version_str.len() != 5 {
            return None;
        }
        let version: u32 = version_str.parse().ok()?;

        if rest.len() < UUID_LEN {
            return None;
        }
        let uuid = Uuid::try_parse(&rest[..UUID_LEN]).ok()?;

        let tail = &rest[UUID_LEN..];
        if !tail.starts_with('.') || !tail.contains(".metadata.json") {
            return None;
        }

        Some(MetadataFile {
            version,
            uuid,
            path: path.clone(),
        })
    }
}

pub async fn find_latest(
    store: &impl ObjectStore,
    prefix: &ObjPath,
) -> Result<Option<MetadataFile>> {
    let mut stream = store.list(Some(prefix));
    let mut latest: Option<MetadataFile> = None;
    while let Some(obj) = stream
        .try_next()
        .await
        .context("failed to list metadata objects")?
    {
        // Metadata filenames always start with a zero-padded 5-digit
        // version, and S3 returns keys in lexicographic order. So anything
        // whose first byte is not an ASCII digit (snap-*.avro, version-
        // hint.text, puffin/stats files, letter-prefixed manifest UUIDs, …)
        // guarantees no further metadata can follow — we bail out rather
        // than walk the whole metadata/ listing, which on long-lived
        // tables can hold hundreds of thousands of manifest entries.
        //
        // We do *not* stop on non-matching digit-prefixed names: manifest
        // files whose commit UUID happens to begin with a digit interleave
        // between versions, so stopping there would miss higher versions.
        let filename = obj.location.as_ref().rsplit('/').next().unwrap_or("");
        if !filename.starts_with(|c: char| c.is_ascii_digit()) {
            break;
        }

        if let Some(candidate) = MetadataFile::parse(&obj.location) {
            if latest.as_ref().is_none_or(|b| candidate.version > b.version) {
                latest = Some(candidate);
            }
        }
    }
    Ok(latest)
}

/// Reads a metadata.json file and returns the raw JSON bytes, transparently
/// decompressing gzip content.
///
/// We sniff the gzip magic bytes rather than trusting the filename suffix:
/// in practice some engines emit gzipped content with a plain
/// `.metadata.json` extension (or put the `.gz` marker in the middle of the
/// name), and we've hit table layouts that break filename-based detection.
pub async fn read_json(store: &impl ObjectStore, path: &ObjPath) -> Result<Vec<u8>> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let data = store.get(path).await?.bytes().await?;
    let is_gzip = data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b;

    if is_gzip {
        let mut decoder = GzDecoder::new(&data[..]);
        let mut out = Vec::new();
        decoder
            .read_to_end(&mut out)
            .context("failed to decompress gzip metadata")?;
        Ok(out)
    } else {
        Ok(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_metadata_name() {
        let m = MetadataFile::parse(&ObjPath::from(
            "warehouse/tbl/metadata/00005-65f87f03-6d7e-41be-8dce-c813ffe70937.metadata.json",
        ))
        .unwrap();
        assert_eq!(m.version, 5);
        assert_eq!(
            m.uuid,
            Uuid::parse_str("65f87f03-6d7e-41be-8dce-c813ffe70937").unwrap()
        );
    }

    #[test]
    fn parses_metadata_name_with_trailing_gz() {
        let m = MetadataFile::parse(&ObjPath::from(
            "metadata/00123-019d9daf-e720-7131-ba9a-b771f5c2b2f1.metadata.json.gz",
        ))
        .unwrap();
        assert_eq!(m.version, 123);
        assert_eq!(
            m.uuid,
            Uuid::parse_str("019d9daf-e720-7131-ba9a-b771f5c2b2f1").unwrap()
        );
    }

    #[test]
    fn parses_metadata_name_with_gz_before_json() {
        // Observed in the wild: some engines insert the gzip marker between
        // the UUID and the `.metadata.json` suffix. Covers the case that
        // originally leaked `.gz` into the UUID field.
        let m = MetadataFile::parse(&ObjPath::from(
            "metadata/44282-019d9daf-e720-7131-ba9a-b771f5c2b2f1.gz.metadata.json",
        ))
        .unwrap();
        assert_eq!(m.version, 44282);
        assert_eq!(
            m.uuid,
            Uuid::parse_str("019d9daf-e720-7131-ba9a-b771f5c2b2f1").unwrap()
        );
    }

    #[test]
    fn rejects_invalid_uuid() {
        // Version part is valid, but the tail isn't a real UUID — reject it
        // rather than silently accepting whatever string follows the dash.
        assert!(MetadataFile::parse(&ObjPath::from(
            "metadata/00001-not-a-real-uuid-at-all-xxxxxxxxxxxx.metadata.json",
        ))
        .is_none());
    }

    #[test]
    fn rejects_manifest_list() {
        assert!(MetadataFile::parse(&ObjPath::from(
            "metadata/snap-1081867561747206961-1-fc6cefef-bfb2-4c11-a105-205785bcb5ac.avro",
        ))
        .is_none());
    }

    #[test]
    fn rejects_manifest_file() {
        assert!(MetadataFile::parse(&ObjPath::from(
            "metadata/fc6cefef-bfb2-4c11-a105-205785bcb5ac-m0.avro",
        ))
        .is_none());
    }

    #[test]
    fn rejects_hadoop_style_name() {
        assert!(MetadataFile::parse(&ObjPath::from("metadata/v2.metadata.json")).is_none());
    }

    #[test]
    fn rejects_version_hint_file() {
        assert!(MetadataFile::parse(&ObjPath::from("metadata/version-hint.text")).is_none());
    }

    #[tokio::test]
    async fn find_latest_skips_interleaved_manifests_and_stops_at_non_digit() {
        use object_store::memory::InMemory;
        use object_store::PutPayload;

        let store = InMemory::new();
        let seed = [
            // Metadata files.
            "metadata/00001-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.metadata.json",
            "metadata/00002-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.metadata.json",
            // Manifest whose commit UUID starts with a digit: in lex order
            // this sorts *after* metadata 00002 but *before* metadata 00003,
            // so naïvely "stop on first non-match" would miss 00003.
            "metadata/00099abc-1234-5678-9abc-def012345678-m0.avro",
            "metadata/00003-cccccccc-cccc-cccc-cccc-cccccccccccc.metadata.json",
            // Files past the digit range — skipped by the early-stop.
            "metadata/abcdef01-2345-6789-abcd-ef0123456789-m0.avro",
            "metadata/snap-123-1-abcdef01-2345-6789-abcd-ef0123456789.avro",
            "metadata/version-hint.text",
        ];
        for key in seed {
            store
                .put(&ObjPath::from(key), PutPayload::from_static(b""))
                .await
                .unwrap();
        }

        let latest = find_latest(&store, &ObjPath::from("metadata"))
            .await
            .unwrap()
            .expect("should find the highest-version metadata file");
        assert_eq!(latest.version, 3);
    }

    #[tokio::test]
    async fn read_json_passes_plain_bytes_through() {
        use object_store::memory::InMemory;
        use object_store::PutPayload;

        let store = InMemory::new();
        let path = ObjPath::from("metadata/00001.metadata.json");
        let body = br#"{"format-version":2}"#.to_vec();
        store
            .put(&path, PutPayload::from(bytes::Bytes::from(body.clone())))
            .await
            .unwrap();

        assert_eq!(read_json(&store, &path).await.unwrap(), body);
    }

    #[tokio::test]
    async fn read_json_decompresses_gzipped_bytes_regardless_of_suffix() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use object_store::memory::InMemory;
        use object_store::PutPayload;
        use std::io::Write;

        let plain = br#"{"format-version":2}"#;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(plain).unwrap();
        let gzipped = encoder.finish().unwrap();

        let store = InMemory::new();
        // Note: filename doesn't end in `.gz` — we still detect gzip by
        // magic bytes, which is the case that caused the first `show
        // snapshot` attempt to fail with a JSON parse error.
        let path = ObjPath::from("metadata/00001-uuid.gz.metadata.json");
        store
            .put(&path, PutPayload::from(bytes::Bytes::from(gzipped)))
            .await
            .unwrap();

        assert_eq!(read_json(&store, &path).await.unwrap(), plain);
    }
}
