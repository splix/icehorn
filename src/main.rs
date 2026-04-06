mod cli;
mod config;
mod copy;
mod sync_log;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Copy(args) => copy::run(args).await?,
    }

    Ok(())
}
