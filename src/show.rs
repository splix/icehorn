//! `show` subcommands — read-only inspection of an Iceberg table on S3.
//!
//! Each subcommand lives in its own submodule so adding a new inspector
//! (schema, partitions, manifest-list, …) stays a single-file change.

mod snapshot;
mod version;

use anyhow::Result;

use crate::cli::ShowCommand;

pub async fn run(cmd: ShowCommand) -> Result<()> {
    match cmd {
        ShowCommand::Version(args) => version::run(args).await,
        ShowCommand::Snapshot(args) => snapshot::run(args).await,
    }
}
