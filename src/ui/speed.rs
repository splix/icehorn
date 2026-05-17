//! Rolling bytes-per-second tracker for the copy renderer.
//!
//! Splits elapsed time into fixed-width buckets and sums byte counts
//! within each one. The window slides forward as time advances, so
//! the reported rate reflects recent throughput rather than the
//! lifetime average — useful for spotting whether a copy is speeding
//! up or stalling, which the lifetime average would hide.

use std::time::Instant;

/// Width of one bucket in seconds. Large enough that a short lull
/// doesn't crater the displayed rate; small enough that throughput
/// changes show up within roughly half a minute.
const BUCKET_SECS: u64 = 15;

/// Number of buckets retained; `NUM_BUCKETS * BUCKET_SECS` is the
/// total observation window. Five minutes smooths over typical TCP
/// hiccups and per-file scheduling gaps without averaging away
/// genuine throughput changes.
const NUM_BUCKETS: usize = 20;

/// Total observation window in seconds.
const WINDOW_SECS: u64 = NUM_BUCKETS as u64 * BUCKET_SECS;

#[derive(Debug, Default)]
pub struct SpeedTracker {
    /// `None` until the first byte is actually recorded. The copy
    /// command spends its first 30–120 seconds in discovery (listing
    /// the source, scanning the destination, walking manifests)
    /// before any byte hits the wire — anchoring the divisor to app
    /// start instead of first byte would drag the displayed rate
    /// down by ~10× for the entire first 5-minute window. We anchor
    /// to first-byte time so the very first second of real transfer
    /// reads a realistic figure.
    first_byte_at: Option<Instant>,
    buckets: [u64; NUM_BUCKETS],
    /// Most recent bucket index seen, relative to `first_byte_at`.
    /// Lets us detect when wall time has skipped past one or more
    /// buckets — those slots in the ring would otherwise still hold
    /// stale bytes from the previous lap.
    last_bucket_index: u64,
}

impl SpeedTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `bytes` to the bucket that contains `now`, clearing any
    /// buckets we've stepped past since the last call so stale bytes
    /// from a previous lap of the ring don't leak into the sum.
    pub fn record(&mut self, bytes: u64, now: Instant) {
        let start = *self.first_byte_at.get_or_insert(now);
        self.advance_to(now, start);
        let idx = (self.last_bucket_index as usize) % NUM_BUCKETS;
        self.buckets[idx] = self.buckets[idx].saturating_add(bytes);
    }

    /// Bytes/sec averaged across the active window.
    ///
    /// `None` until the first byte arrives so the renderer can show
    /// `--` instead of a misleading "0.0KB/s" during the discovery
    /// phase. Past that, divides by elapsed-since-first-byte (capped
    /// at the full window) so a fresh copy reports a realistic figure
    /// during its first five minutes instead of one-twentieth of the
    /// truth. A genuine mid-run lull still reports `Some(0)` — once
    /// activity has started, zero is a meaningful reading.
    pub fn bytes_per_sec(&mut self, now: Instant) -> Option<u64> {
        let start = self.first_byte_at?;
        self.advance_to(now, start);
        let elapsed = now.saturating_duration_since(start).as_secs().max(1);
        let window = elapsed.min(WINDOW_SECS);
        let total: u64 = self.buckets.iter().sum();
        Some(total / window)
    }

    fn advance_to(&mut self, now: Instant, start: Instant) {
        let now_index = now.saturating_duration_since(start).as_secs() / BUCKET_SECS;
        if now_index <= self.last_bucket_index {
            return;
        }
        let skipped = (now_index - self.last_bucket_index) as usize;
        if skipped >= NUM_BUCKETS {
            self.buckets = [0; NUM_BUCKETS];
        } else {
            for step in 1..=skipped {
                let idx = ((self.last_bucket_index + step as u64) as usize) % NUM_BUCKETS;
                self.buckets[idx] = 0;
            }
        }
        self.last_bucket_index = now_index;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn at(t0: Instant, secs: u64) -> Instant {
        t0 + Duration::from_secs(secs)
    }

    /// The regression test for the bug the user reported: the copy
    /// command spends its first ~90 seconds in discovery before any
    /// byte arrives. If we anchored the divisor to app start, a
    /// 10 MB transfer in the first second after discovery would
    /// read as ~110 KB/s (10 MB / 91 s) instead of ~10 MB/s.
    #[test]
    fn discovery_idle_time_does_not_drag_down_first_rate() {
        let t0 = Instant::now();
        let mut s = SpeedTracker::new();
        // 90 s of nothing — discovery phase, no records.
        s.record(10_000_000, at(t0, 90));
        // One second after the first byte we should read ~10 MB/s,
        // not ~110 KB/s.
        assert_eq!(s.bytes_per_sec(at(t0, 91)), Some(10_000_000));
    }

    /// Until anything is recorded, the rate is unknown — return
    /// `None` so the renderer can show `--` rather than a misleading
    /// "0.0KB/s" reading during the discovery phase.
    #[test]
    fn reports_none_before_first_record() {
        let t0 = Instant::now();
        let mut s = SpeedTracker::new();
        assert_eq!(s.bytes_per_sec(at(t0, 0)), None);
        assert_eq!(s.bytes_per_sec(at(t0, 600)), None);
    }

    /// Once we've recorded at least one byte, a subsequent quiet
    /// period reports `Some(0)` — not `None`. Distinguishing
    /// "haven't started" from "stalled" matters in the UI.
    #[test]
    fn reports_some_zero_after_activity_stops() {
        let t0 = Instant::now();
        let mut s = SpeedTracker::new();
        s.record(1_000_000, at(t0, 0));
        assert_eq!(
            s.bytes_per_sec(at(t0, WINDOW_SECS + BUCKET_SECS + 1)),
            Some(0)
        );
    }

    /// The window divisor caps at `WINDOW_SECS` so a long-running
    /// copy doesn't keep growing the denominator forever. Demonstrated
    /// by recording two samples 290 s apart and querying 20 s after
    /// the second — the first sample ages out of the ring, and the
    /// remaining 1 MB is reported as `1 MB / WINDOW_SECS`, not
    /// `1 MB / 310`.
    #[test]
    fn divisor_saturates_at_window_secs() {
        let t0 = Instant::now();
        let mut s = SpeedTracker::new();
        s.record(1_000_000, at(t0, 0));
        s.record(1_000_000, at(t0, 290));
        let rate = s.bytes_per_sec(at(t0, 310));
        assert_eq!(rate, Some(1_000_000 / WINDOW_SECS));
    }

    /// Skipping just one bucket clears only that bucket, not the
    /// whole ring — bytes recorded earlier should still count.
    #[test]
    fn partial_skip_only_clears_passed_buckets() {
        let t0 = Instant::now();
        let mut s = SpeedTracker::new();
        s.record(600, at(t0, 0));
        s.record(900, at(t0, BUCKET_SECS * 2));
        let elapsed = BUCKET_SECS * 2;
        assert_eq!(s.bytes_per_sec(at(t0, elapsed)), Some((600 + 900) / elapsed));
    }

    #[test]
    fn records_accumulate_within_one_bucket() {
        let t0 = Instant::now();
        let mut s = SpeedTracker::new();
        s.record(100, at(t0, 0));
        s.record(200, at(t0, 5));
        s.record(300, at(t0, 14));
        // All within bucket 0 (relative to first-byte time);
        // reading from t=14 still uses bucket 0.
        assert_eq!(s.bytes_per_sec(at(t0, 14)), Some(600 / 14));
    }
}
