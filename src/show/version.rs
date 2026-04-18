//! `show version` — print the current metadata version and filename UUID.

use anyhow::{Context, Result};
use object_store::path::Path as ObjPath;

use super::metadata::find_latest;
use crate::cli::TableArgs;
use crate::config::S3Config;
use crate::s3_url::S3Location;

pub async fn run(args: TableArgs) -> Result<()> {
    let config = S3Config::from_file(&args.config)?;
    let location = S3Location::parse(&args.location)?;
    let store = config.build_store(&location.bucket)?;

    let metadata_prefix = ObjPath::from(format!("{}/metadata", location.prefix));
    let latest = find_latest(&store, &metadata_prefix)
        .await?
        .context("no <NNNNN>-<uuid>.metadata.json files found under metadata/")?;

    println!("Current version: {}", latest.version);
    println!("UUID: {}", latest.uuid);

    Ok(())
}
