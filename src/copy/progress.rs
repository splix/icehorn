//! Reporter-bridging counters for a single table copy.
//!
//! Two structs:
//!   * [`ScanProgress`] tracks the discovery phase (manifest walk +
//!     destination LIST). Both sub-tasks update the same instance and
//!     it emits a single combined status line.
//!   * [`TableProgress`] tracks the copy phase (file counts: total /
//!     copied / skipped) and emits `TableQueueChanged` events.
//!
//! The structs exist because the copy code shouldn't have to know how
//! the TUI formats progress, only that it has values to report.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::ui::Reporter;

/// Live counters shared between concurrent discovery tasks. Each
/// counter update re-emits the table status as a single combined line
/// so the TUI shows steady progress through the otherwise-silent scan
/// phase. The string is rebuilt on every update; this is fine because
/// the TUI redraws on a 100ms tick and only reads the latest value.
pub(super) struct ScanProgress {
    reporter: Reporter,
    table_key: String,
    manifest_lists_total: AtomicUsize,
    manifest_lists_done: AtomicUsize,
    manifests_total: AtomicUsize,
    manifests_done: AtomicUsize,
    dst_objects: AtomicU64,
}

impl ScanProgress {
    pub(super) fn new(reporter: Reporter, table_key: String) -> Arc<Self> {
        Arc::new(Self {
            reporter,
            table_key,
            manifest_lists_total: AtomicUsize::new(0),
            manifest_lists_done: AtomicUsize::new(0),
            manifests_total: AtomicUsize::new(0),
            manifests_done: AtomicUsize::new(0),
            dst_objects: AtomicU64::new(0),
        })
    }

    pub(super) fn record_manifest_list(&self, done: usize, total: usize) {
        self.manifest_lists_done.store(done, Ordering::Relaxed);
        self.manifest_lists_total.store(total, Ordering::Relaxed);
        self.emit();
    }

    pub(super) fn record_manifest(&self, done: usize, total: usize) {
        self.manifests_done.store(done, Ordering::Relaxed);
        self.manifests_total.store(total, Ordering::Relaxed);
        self.emit();
    }

    pub(super) fn record_dst_objects(&self, count: u64) {
        self.dst_objects.store(count, Ordering::Relaxed);
        self.emit();
    }

    fn emit(&self) {
        // Build a single status line covering both concurrent sub-tasks
        // (manifest walk + dst scan). Whichever phase is active wins
        // priority; we always tack on the dst-scan counter when present.
        let mll_done = self.manifest_lists_done.load(Ordering::Relaxed);
        let mll_total = self.manifest_lists_total.load(Ordering::Relaxed);
        let m_done = self.manifests_done.load(Ordering::Relaxed);
        let m_total = self.manifests_total.load(Ordering::Relaxed);
        let dst = self.dst_objects.load(Ordering::Relaxed);

        let mut parts: Vec<String> = Vec::new();
        if m_total > 0 {
            parts.push(format!("manifests {m_done}/{m_total}"));
        } else if mll_total > 0 {
            parts.push(format!("manifest lists {mll_done}/{mll_total}"));
        }
        if dst > 0 {
            parts.push(format!("dst {dst} objects"));
        }
        if parts.is_empty() {
            return;
        }
        self.reporter
            .table_status(&self.table_key, parts.join(" · "));
    }
}

/// Per-table counters shared with the file-copy closures. Every
/// increment re-emits a `TableQueueChanged` so the TUI shows steady
/// progress without each callsite needing to know about the renderer.
///
/// `failed` is tracked separately from `skipped`: a "skip" is a
/// deliberate decision (already on the destination), while a "fail" is
/// an I/O error we couldn't recover from. The next run will retry
/// failed files because we never write a sync-log entry for them.
pub(super) struct TableProgress {
    reporter: Reporter,
    key: String,
    /// Total files known so far. Grows during discovery (`run_all`) or
    /// is set once after planning (`run_filtered`).
    total: AtomicU64,
    copied: AtomicU64,
    skipped: AtomicU64,
    failed: AtomicU64,
}

impl TableProgress {
    pub(super) fn new(reporter: Reporter, key: String) -> Arc<Self> {
        Arc::new(Self {
            reporter,
            key,
            total: AtomicU64::new(0),
            copied: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        })
    }

    pub(super) fn set_total(&self, n: u64) {
        self.total.store(n, Ordering::Relaxed);
        self.emit();
    }

    pub(super) fn add_total(&self, n: u64) {
        self.total.fetch_add(n, Ordering::Relaxed);
        self.emit();
    }

    pub(super) fn record_copied(&self) {
        self.copied.fetch_add(1, Ordering::Relaxed);
        self.emit();
    }

    pub(super) fn record_skipped(&self) {
        self.skipped.fetch_add(1, Ordering::Relaxed);
        self.emit();
    }

    pub(super) fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        self.emit();
    }

    pub(super) fn copied_count(&self) -> u64 {
        self.copied.load(Ordering::Relaxed)
    }

    pub(super) fn skipped_count(&self) -> u64 {
        self.skipped.load(Ordering::Relaxed)
    }

    pub(super) fn failed_count(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    fn emit(&self) {
        let total = self.total.load(Ordering::Relaxed);
        let copied = self.copied_count();
        let skipped = self.skipped_count();
        let failed = self.failed_count();
        // Failed files leave the queue too — otherwise the progress
        // bar would stick at "queued > 0" forever after a failure.
        let queued = total.saturating_sub(copied + skipped + failed);
        self.reporter
            .table_queue_changed(&self.key, queued, copied, skipped);
    }
}
