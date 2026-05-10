//! `Reporter` — the handle the copy code uses to emit progress events.
//!
//! In `--plain` mode this is a no-op (events are dropped before
//! allocation), so the hot path stays cheap. In TUI mode every call
//! pushes a [`CopyEvent`] onto an unbounded channel that the renderer
//! drains on its own thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;

use super::event::CopyEvent;

/// Cloneable handle to the event sink.
#[derive(Clone)]
pub struct Reporter {
    inner: Option<Inner>,
}

#[derive(Clone)]
struct Inner {
    tx: mpsc::UnboundedSender<CopyEvent>,
    next_file_id: Arc<AtomicU64>,
}

/// Reserved for future use — we may want to make `file_started` return
/// a typed handle that carries the id around. For now callers pass the
/// `u64` id directly because that's the simplest thing to capture in a
/// progress callback closure.
pub type FileToken = u64;

impl Reporter {
    /// Build a no-op reporter. All emitted events are discarded; useful
    /// in `--plain` mode and in tests.
    pub fn plain() -> Self {
        Self { inner: None }
    }

    /// Build a channel-backed reporter, returning the receiver the
    /// renderer (or layer) should drain.
    pub fn channel() -> (Self, mpsc::UnboundedReceiver<CopyEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let me = Self {
            inner: Some(Inner {
                tx,
                next_file_id: Arc::new(AtomicU64::new(1)),
            }),
        };
        (me, rx)
    }

    /// True when events are actually delivered. Used by callers that
    /// want to skip work that's only meaningful to the TUI.
    pub fn is_active(&self) -> bool {
        self.inner.is_some()
    }

    pub fn send(&self, event: CopyEvent) {
        if let Some(inner) = &self.inner {
            // Errors mean the receiver hung up. We still want the copy
            // to keep running (the renderer might have crashed), so we
            // swallow the failure.
            let _ = inner.tx.send(event);
        }
    }

    pub fn table_started(&self, key: &str, namespace: &str, table: &str) {
        self.send(CopyEvent::TableStarted {
            key: key.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
        });
    }

    pub fn table_queue_changed(&self, key: &str, queued: u64, copied: u64, skipped: u64) {
        self.send(CopyEvent::TableQueueChanged {
            key: key.to_string(),
            queued,
            copied,
            skipped,
        });
    }

    pub fn table_finished(&self, key: &str) {
        self.send(CopyEvent::TableFinished {
            key: key.to_string(),
        });
    }

    pub fn table_status(&self, key: &str, message: impl Into<String>) {
        self.send(CopyEvent::TableStatus {
            key: key.to_string(),
            message: Some(message.into()),
        });
    }

    pub fn global_status(&self, message: impl Into<String>) {
        self.send(CopyEvent::GlobalStatus {
            message: Some(message.into()),
        });
    }

    pub fn clear_global_status(&self) {
        self.send(CopyEvent::GlobalStatus { message: None });
    }

    /// Allocate a unique id for an in-flight file copy and announce its
    /// start. Returns the id; callers pass it back to
    /// [`file_progress`] and [`file_finished`]. In plain mode the id is
    /// always 0 — no event is sent and no progress will be shown.
    pub fn file_started(&self, table_key: &str, name: &str, total_bytes: u64) -> FileToken {
        let id = match &self.inner {
            Some(inner) => inner.next_file_id.fetch_add(1, Ordering::Relaxed),
            None => return 0,
        };
        self.send(CopyEvent::FileStarted {
            id,
            table_key: table_key.to_string(),
            name: name.to_string(),
            total_bytes,
        });
        id
    }

    pub fn file_progress(&self, id: FileToken, copied_bytes: u64) {
        if !self.is_active() {
            return;
        }
        self.send(CopyEvent::FileProgress { id, copied_bytes });
    }

    pub fn file_finished(&self, id: FileToken) {
        if !self.is_active() {
            return;
        }
        self.send(CopyEvent::FileFinished { id });
    }

    /// Final event — the renderer exits its loop after this. Safe to
    /// call multiple times; the channel will just see extras and ignore
    /// them once the renderer has shut down.
    pub fn done(&self) {
        self.send(CopyEvent::Done);
    }
}
