//! Reporting + presentation for long-running commands.
//!
//! Long-running commands (today: `copy`) emit a stream of events through
//! a [`Reporter`]. The default `--plain` mode discards them — `tracing`
//! already produces a useful log. The TUI mode drains the same stream
//! and renders a Docker-build-style view in an inline terminal viewport.
//! Tracing events are captured separately by the `tui_logger` crate and
//! shown in their own pane.
//!
//! Splitting the emitter (Reporter) from the consumer (TUI / plain)
//! keeps the copy code unaware of how its progress is shown.

pub mod copy;
pub mod event;
pub mod reporter;
pub mod speed;

pub use reporter::Reporter;
