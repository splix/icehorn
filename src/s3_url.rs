//! Parsing of `s3://bucket/prefix` URLs into their components.
//!
//! Kept in its own module so both the `copy` and `show` commands can share
//! the same parser without reaching into each other.

use anyhow::{Context, Result};

/// A parsed `s3://` (or `s3a://`) URL pointing at a prefix inside a bucket.
pub struct S3Location {
    pub bucket: String,
    /// The key prefix within the bucket, with any trailing `/` trimmed.
    pub prefix: String,
}

impl S3Location {
    pub fn parse(url: &str) -> Result<Self> {
        let rest = url
            .strip_prefix("s3://")
            .or_else(|| url.strip_prefix("s3a://"))
            .with_context(|| format!("URL must start with s3:// or s3a://: {url}"))?;
        let (bucket, path) = rest
            .split_once('/')
            .with_context(|| format!("URL must contain a path after bucket: {url}"))?;
        Ok(S3Location {
            bucket: bucket.to_string(),
            prefix: path.trim_end_matches('/').to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_s3_url() {
        let loc = S3Location::parse("s3://my-bucket/warehouse/db/table").unwrap();
        assert_eq!(loc.bucket, "my-bucket");
        assert_eq!(loc.prefix, "warehouse/db/table");
    }

    #[test]
    fn parses_s3a_url_and_trims_trailing_slash() {
        let loc = S3Location::parse("s3a://my-bucket/warehouse/db/table/").unwrap();
        assert_eq!(loc.bucket, "my-bucket");
        assert_eq!(loc.prefix, "warehouse/db/table");
    }

    #[test]
    fn rejects_missing_scheme() {
        assert!(S3Location::parse("my-bucket/warehouse").is_err());
    }

    #[test]
    fn rejects_missing_path() {
        assert!(S3Location::parse("s3://my-bucket").is_err());
    }
}
