mod cli;
mod config;
mod copy;
mod iceberg;
mod s3_url;
mod show;
mod sync_log;
mod ui;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        // The TUI captures `tracing` events into its own log pane. For
        // every other command (and for the plain mode of copy) we keep
        // the previous behavior: humans-readable formatted logs to stderr.
        Command::Copy(args) if !cli.plain => run_copy_with_tui(args).await,
        Command::Copy(args) => {
            init_plain_logging();
            log_levels_enabled();
            copy::run(args, ui::Reporter::plain()).await
        }
        Command::Show { command } => {
            init_plain_logging();
            log_levels_enabled();
            show::run(command).await
        }
    }
}

fn init_plain_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}

fn log_levels_enabled() {
    tracing::info!("INFO log enabled");
    tracing::debug!("DEBUG log enabled");
    tracing::trace!("TRACE log enabled");
}

/// Runs the copy with the inline ratatui renderer. Tracing events go
/// through `tui_logger`'s tracing layer into its own circular buffer;
/// the [`tui_logger::TuiLoggerWidget`] in the renderer reads from that
/// buffer when drawing. The renderer runs on a blocking thread because
/// crossterm's I/O is sync.
async fn run_copy_with_tui(args: cli::CopyArgs) -> Result<()> {
    init_tui_logger()?;

    let (reporter, rx) = ui::Reporter::channel();
    let ui_handle = tokio::task::spawn_blocking(move || ui::copy::run(rx));

    let copy_reporter = reporter.clone();
    let copy_result = copy::run(args, copy_reporter).await;
    reporter.done();

    // Wait for the renderer to drain remaining events and exit cleanly
    // before we propagate the copy result.
    let _ = ui_handle.await;
    copy_result
}

/// Initialise the `tui_logger` backend and route tracing events into
/// it. We honour `RUST_LOG` for the captured-level filter, falling
/// back to INFO when unset, so the TUI behaves the same as plain mode.
fn init_tui_logger() -> Result<()> {
    use tracing::Level;

    tui_logger::init_logger(tui_logger::LevelFilter::Trace)
        .map_err(|e| anyhow::anyhow!("init tui_logger: {e}"))?;
    tui_logger::set_default_level(tui_logger::LevelFilter::Info);
    // Apply RUST_LOG-style filtering on the layer so target-level
    // overrides (e.g. `RUST_LOG=icehorn=debug`) work as expected.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(Level::INFO.to_string()));
    tracing_subscriber::registry()
        .with(tui_logger::TuiTracingSubscriberLayer.with_filter(filter))
        .init();
    Ok(())
}
