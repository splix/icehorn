//! Update events flowing from `copy` (and any future long-running
//! command) into the TUI. Kept as a single enum so the receiver can
//! pattern-match in one place.
//!
//! Tracing events are *not* delivered through this channel — they go
//! through the `tui_logger` crate's own circular buffer and are
//! rendered by [`tui_logger::TuiLoggerWidget`] directly.

#[derive(Debug, Clone)]
pub enum CopyEvent {
    /// Copy of a table started. `key` uniquely identifies the table for
    /// later events; `namespace` and `table` are the human-readable
    /// (UUID, usually) labels shown in the header.
    TableStarted {
        key: String,
        namespace: String,
        table: String,
    },

    /// The size of the per-table copy queue changed. `queued` is the
    /// number of files still waiting; we receive this whenever discovery
    /// finds more work or files get filtered out as already-present.
    TableQueueChanged {
        key: String,
        queued: u64,
        copied: u64,
        skipped: u64,
    },

    /// A table finished (success or failure). The TUI uses this to drop
    /// the table from its active set when picking which one to feature.
    TableFinished {
        key: String,
    },

    /// Short, human-readable description of what the table is currently
    /// doing — `"loading metadata"`, `"scanning destination"`, etc. The
    /// TUI shows this beneath the header so the user has feedback even
    /// when no files are in flight yet (the discovery phase is otherwise
    /// silent for minutes on large tables). `None` clears the status.
    TableStatus {
        key: String,
        message: Option<String>,
    },

    /// Status not tied to any single table — used during the initial
    /// `discover_tables` walk and any other pre-table work. Falls
    /// through to the renderer when no table-specific status is set.
    GlobalStatus {
        message: Option<String>,
    },

    /// A file copy began. Emitted only for files we'll actually copy —
    /// skipped files never appear as a progress bar. `table_key`
    /// associates the file with the table in the renderer state; it's
    /// not used by the current view but is kept so a future per-table
    /// breakdown doesn't need a schema change.
    FileStarted {
        id: u64,
        #[allow(dead_code)]
        table_key: String,
        name: String,
        total_bytes: u64,
    },

    /// Bytes-so-far for an in-flight file. May be sent many times per
    /// file as chunks arrive from the source.
    FileProgress {
        id: u64,
        copied_bytes: u64,
    },

    /// File copy completed (success or skipped late). The TUI removes
    /// the file from its in-flight list.
    FileFinished {
        id: u64,
    },

    /// All work completed. Sent by the orchestrator once after the copy
    /// finishes (success or error). The TUI exits its draw loop on this.
    Done,
}
