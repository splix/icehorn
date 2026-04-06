use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "icehorn", about = "Iceberg data lake utilities")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Copy an Iceberg table between S3 locations, rewriting metadata paths
    Copy(CopyArgs),
}

#[derive(clap::Args)]
pub struct CopyArgs {
    /// Path to s3cmd config file for the source S3
    #[arg(long = "from.config")]
    pub from_config: PathBuf,

    /// Source S3 URL (e.g. s3://bucket/path/to/table)
    #[arg(long)]
    pub from: String,

    /// Path to s3cmd config file for the destination S3
    #[arg(long = "to.config")]
    pub to_config: PathBuf,

    /// Destination S3 URL (e.g. s3://bucket/path/to/table)
    #[arg(long)]
    pub to: String,

    /// Number of parallel copy tasks (default: 32)
    #[arg(long = "copy.parallel", default_value_t = 32)]
    pub copy_parallel: usize,
}
