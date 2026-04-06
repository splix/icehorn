use anyhow::{Context, Result};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use std::collections::HashMap;
use std::path::Path;

/// S3 connection parameters parsed from an s3cmd-style config file.
pub struct S3Config {
    pub access_key: String,
    pub secret_key: String,
    pub host_base: String,
    pub bucket_location: String,
    pub use_https: bool,
    pub virtual_hosted_style: bool,
}

impl S3Config {
    /// Read and parse an s3cmd config file (~/.s3cfg format).
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config: {}", path.display()))?;
        Self::parse(&content)
    }

    fn parse(content: &str) -> Result<Self> {
        let mut kv = HashMap::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty()
                || line.starts_with('#')
                || line.starts_with(';')
                || line.starts_with('[')
            {
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                kv.insert(k.trim().to_string(), v.trim().to_string());
            }
        }

        let host_bucket = kv.get("host_bucket").map(|s| s.as_str()).unwrap_or("");
        let virtual_hosted =
            !host_bucket.eq_ignore_ascii_case("false") && !host_bucket.is_empty();

        Ok(S3Config {
            access_key: kv.get("access_key").context("missing access_key")?.clone(),
            secret_key: kv.get("secret_key").context("missing secret_key")?.clone(),
            host_base: kv
                .get("host_base")
                .cloned()
                .unwrap_or_else(|| "s3.amazonaws.com".into()),
            bucket_location: kv
                .get("bucket_location")
                .cloned()
                .unwrap_or_else(|| "us-east-1".into()),
            use_https: kv
                .get("use_https")
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(true),
            virtual_hosted_style: virtual_hosted,
        })
    }

    /// Build an `object_store` S3 client for the given bucket.
    pub fn build_store(&self, bucket: &str) -> Result<AmazonS3> {
        let scheme = if self.use_https { "https" } else { "http" };
        let endpoint = format!("{scheme}://{}", self.host_base);

        let mut builder = AmazonS3Builder::new()
            .with_access_key_id(&self.access_key)
            .with_secret_access_key(&self.secret_key)
            .with_endpoint(&endpoint)
            .with_region(&self.bucket_location)
            .with_bucket_name(bucket);

        if !self.use_https {
            builder = builder.with_allow_http(true);
        }

        if !self.virtual_hosted_style {
            builder = builder.with_virtual_hosted_style_request(false);
        }

        builder.build().context("failed to build S3 client")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_config() {
        let cfg = S3Config::parse(
            "\
[default]
access_key = AKIA1234
secret_key = s3cr3t
host_base = s3.us-east-va.io.cloud.ovh.us
host_bucket = false
bucket_location = us-east-va
use_https = True
",
        )
        .unwrap();

        assert_eq!(cfg.access_key, "AKIA1234");
        assert_eq!(cfg.secret_key, "s3cr3t");
        assert_eq!(cfg.host_base, "s3.us-east-va.io.cloud.ovh.us");
        assert_eq!(cfg.bucket_location, "us-east-va");
        assert!(cfg.use_https);
        assert!(!cfg.virtual_hosted_style);
    }

    #[test]
    fn defaults_for_aws() {
        let cfg = S3Config::parse(
            "\
[default]
access_key = AKIA1234
secret_key = s3cr3t
",
        )
        .unwrap();

        assert_eq!(cfg.host_base, "s3.amazonaws.com");
        assert_eq!(cfg.bucket_location, "us-east-1");
        assert!(cfg.use_https);
        assert!(!cfg.virtual_hosted_style);
    }
}
