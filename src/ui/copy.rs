//! Inline ratatui renderer for the `copy` command.
//!
//! Layout (top → bottom):
//!   1. **Header** — elapsed time, current namespace, current table
//!      (the active table with the smallest queue, so the user sees the
//!      one that's about to finish), plus compact totals on the right.
//!   2. **Status** — a single line describing what the featured table
//!      is doing right now.
//!   3. **Logs** — recent `tracing` events captured by `tui_logger`.
//!   4. **Per-table rows** — one summary row per active table.
//!   5. **In-flight files** — one byte-progress bar per active file copy.
//!
//! Colors lean grayscale; only the log levels keep semantic colors so
//! warnings and errors still stand out.

use std::collections::HashMap;
use std::io::{self, Write};
use std::time::{Duration, Instant};

use anyhow::Result;
use ratatui::DefaultTerminal;
use ratatui::crossterm::QueueableCommand;
use ratatui::crossterm::cursor::MoveToPreviousLine;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::crossterm::terminal::{Clear, ClearType};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Gauge, Paragraph};
use ratatui::{TerminalOptions, Viewport};
use tokio::sync::mpsc;
use tui_logger::{TuiLoggerLevelOutput, TuiLoggerWidget};

use super::event::CopyEvent;
use super::speed::SpeedTracker;

/// Total rows reserved for the log panel — six rows of content plus
/// one row each for the top and bottom rule that separates the panel
/// from the header above and the per-table grid below.
const LOG_LINES: u16 = 6 + 2;
/// Per-table summary rows. Caps at the default `--tables.parallel`
/// value (4) since that's how many tables are actively copying at
/// once; tables waiting in the discovery phase still show in this list.
const MAX_TABLE_ROWS: u16 = 4;
/// Maximum simultaneous file rows to show. The copy code may run more
/// in flight (`--copy.parallel` defaults to 32) but we cap the visual
/// list at something the eye can actually scan.
const MAX_FILE_ROWS: u16 = 5;
/// Total inline height of the rendered region. The viewport is fixed
/// so the rows above the cursor don't reflow under the user's
/// scrollback.
const VIEWPORT_HEIGHT: u16 = 1 + 1 + LOG_LINES + MAX_TABLE_ROWS + MAX_FILE_ROWS;

/// Single accent used for both progress bar fills. Deliberately one
/// step darker than the surrounding text (`Color::Gray`) so the bar
/// reads as a background fill rather than as foreground content —
/// keeps it from competing with the actually-meaningful colors of
/// WARN / ERROR log lines.
const BAR_FILL: Color = Color::DarkGray;
/// Slightly dimmer gray for status / hint text.
const DIM: Color = Color::DarkGray;

pub fn run(rx: mpsc::UnboundedReceiver<CopyEvent>, shutdown: shutdown::Shutdown) -> Result<()> {
    let mut terminal = ratatui::init_with_options(TerminalOptions {
        viewport: Viewport::Inline(VIEWPORT_HEIGHT),
    });
    let result = main_loop(&mut terminal, rx, &shutdown);

    // Reclaim the inline-viewport rows before tearing down raw mode.
    // ratatui's inline viewport doesn't clean itself up on drop — the
    // last frame stays frozen in the terminal, and any summary
    // printed afterwards lands beneath that ghost frame, which is the
    // "messy interleaved output" users see at the end of a run.
    let _ = clear_viewport_region();
    ratatui::restore();

    if let Ok(state) = &result {
        print_summary(state);
    }
    result.map(|_| ())
}

/// Walk the cursor back to the top of the inline viewport region and
/// wipe everything from there to the end of the screen. After the
/// last `terminal.draw`, ratatui leaves the cursor at the line just
/// below the viewport, so `MoveToPreviousLine(VIEWPORT_HEIGHT)` lands
/// us exactly at the first row the viewport occupied.
///
/// Errors are intentionally swallowed by the caller — failing to
/// pretty-clean the terminal shouldn't fail the whole run, the worst
/// case is the user sees the same stale-frame mess we get today.
fn clear_viewport_region() -> io::Result<()> {
    let mut stdout = io::stdout();
    stdout.queue(MoveToPreviousLine(VIEWPORT_HEIGHT))?;
    stdout.queue(Clear(ClearType::FromCursorDown))?;
    stdout.flush()
}

/// Drive the draw loop. We poll the event channel (non-blocking) and
/// crossterm input (also non-blocking) on a tick, so neither side
/// starves the other. `CopyEvent::Done` triggers a clean exit.
fn main_loop(
    terminal: &mut DefaultTerminal,
    mut rx: mpsc::UnboundedReceiver<CopyEvent>,
    shutdown: &shutdown::Shutdown,
) -> Result<State> {
    let mut state = State::new();
    let tick = Duration::from_millis(100);

    loop {
        // Drain everything pending without blocking — bursts of file
        // progress shouldn't cause N redraws, just one.
        let mut closed = false;
        loop {
            match rx.try_recv() {
                Ok(CopyEvent::Done) => {
                    state.apply(CopyEvent::Done);
                    closed = true;
                }
                Ok(ev) => state.apply(ev),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    closed = true;
                    break;
                }
            }
        }

        // Sample throughput just before drawing — keeps the displayed
        // rate decaying smoothly even when no FileProgress events
        // arrived this tick (e.g. all in-flight files stalled).
        state.refresh_speed(Instant::now());
        terminal.draw(|frame| draw(frame, &state))?;

        if closed {
            return Ok(state);
        }

        // Quit keys (Ctrl+C, q, Esc) signal the shared shutdown so
        // the copy task aborts in addition to the UI tearing down.
        // Crossterm's raw mode swallows the real SIGINT — without
        // this hand-off, Ctrl+C felt completely dead during the
        // metadata-discovery phase of a run.
        if event::poll(tick)?
            && let Event::Key(k) = event::read()?
            && k.kind == KeyEventKind::Press
            && is_quit_key(&k)
        {
            shutdown.signal();
            return Ok(state);
        }
    }
}

fn is_quit_key(k: &ratatui::crossterm::event::KeyEvent) -> bool {
    let ctrl_c =
        matches!(k.code, KeyCode::Char('c')) && k.modifiers.contains(KeyModifiers::CONTROL);
    let q_or_esc = matches!(k.code, KeyCode::Char('q') | KeyCode::Esc);
    ctrl_c || q_or_esc
}

/// Render the final stats to stderr. We print to stderr (not stdout)
/// so anyone redirecting `copy`'s stdout to `tee` or another consumer
/// gets a clean stream — same convention as `tracing` logs in
/// `--plain` mode.
fn print_summary(state: &State) {
    let elapsed_dur = state.started_at.elapsed();
    let elapsed = format_elapsed(elapsed_dur);
    let total_files = state.overall_copied + state.overall_skipped;
    let bytes_str = format_size(state.overall_bytes_copied);
    // Lifetime average — the rolling-window rate is meaningless after
    // the copy has ended, so the summary reports total/elapsed for an
    // honest "what did we sustain across the whole run" number. When
    // nothing was actually copied (all-skipped run) it's `None` and
    // the renderer prints `--` rather than a misleading "0.0KB/s".
    let avg_rate = lifetime_rate(state.overall_bytes_copied, elapsed_dur);

    eprintln!();
    eprintln!("Copy summary");
    eprintln!("  Elapsed : {elapsed}");
    eprintln!("  Tables  : {}", state.tables.len());
    eprintln!(
        "  Files   : {} copied, {} skipped ({total_files} total)",
        state.overall_copied, state.overall_skipped
    );
    eprintln!("  Bytes   : {bytes_str} transferred ({})", format_rate(avg_rate));

    // Always list the tables — the user explicitly wants to see which
    // ones participated, not just the count. UUIDs aren't pretty but
    // they're what we have at this layer.
    if !state.tables.is_empty() {
        eprintln!();
        eprintln!("Tables:");
        let mut tables: Vec<&TableState> = state.tables.values().collect();
        tables.sort_by(|a, b| a.table.cmp(&b.table));
        for t in &tables {
            let total = t.copied + t.skipped;
            eprintln!(
                "  {}  {} copied, {} skipped ({total} total)",
                t.table, t.copied, t.skipped
            );
        }
    }
}

// ---------------------------------------------------------------------------
// State — accumulates the latest snapshot the renderer needs to draw a frame.
// ---------------------------------------------------------------------------

struct State {
    started_at: Instant,
    /// Per-table progress, keyed by `Reporter`'s `key` string.
    tables: HashMap<String, TableState>,
    /// In-flight file rows in insertion order, so the visible list is
    /// stable as new files come and go.
    files: Vec<FileState>,
    /// Cumulative bytes that have actually crossed the wire — updated
    /// live on every `FileProgress` delta, not just on finish, so the
    /// header total ticks during a long-running file instead of
    /// jumping by half a gigabyte at the end of each one.
    overall_bytes_copied: u64,
    overall_copied: u64,
    overall_skipped: u64,
    overall_queued: u64,
    /// Status text shown when no table-specific status applies
    /// (typically: pre-table discovery phase).
    global_status: Option<String>,
    /// Rolling-window throughput tracker. Fed the same byte deltas
    /// that grow `overall_bytes_copied`.
    speed: SpeedTracker,
    /// Last sampled speed (bytes/sec) — recomputed once per draw tick
    /// by `main_loop` so the rate still falls to zero when nothing is
    /// streaming, and so draw functions can stay `&State`. `None`
    /// until the first byte is recorded, which the renderer displays
    /// as `--` rather than a misleading "0.0KB/s" during discovery.
    cached_bytes_per_sec: Option<u64>,
}

struct TableState {
    namespace: String,
    table: String,
    queued: u64,
    copied: u64,
    skipped: u64,
    finished: bool,
    /// What this table is doing right now ("loading metadata",
    /// "scanning destination", …). Cleared when the table finishes.
    status: Option<String>,
}

struct FileState {
    id: u64,
    name: String,
    total_bytes: u64,
    copied_bytes: u64,
}

impl State {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            tables: HashMap::new(),
            files: Vec::new(),
            overall_bytes_copied: 0,
            overall_copied: 0,
            overall_skipped: 0,
            overall_queued: 0,
            global_status: None,
            speed: SpeedTracker::new(),
            cached_bytes_per_sec: None,
        }
    }

    /// Resample the throughput tracker. Called once per draw tick so
    /// the displayed rate decays during quiet periods even when no
    /// new bytes are arriving. `None` while the tracker hasn't seen
    /// its first byte yet (the discovery phase).
    fn refresh_speed(&mut self, now: Instant) {
        self.cached_bytes_per_sec = self.speed.bytes_per_sec(now);
    }

    fn apply(&mut self, ev: CopyEvent) {
        match ev {
            CopyEvent::TableStarted { key, namespace, table } => {
                self.tables.insert(
                    key,
                    TableState {
                        namespace,
                        table,
                        queued: 0,
                        copied: 0,
                        skipped: 0,
                        finished: false,
                        status: None,
                    },
                );
            }
            CopyEvent::TableQueueChanged { key, queued, copied, skipped } => {
                if let Some(t) = self.tables.get_mut(&key) {
                    t.queued = queued;
                    t.copied = copied;
                    t.skipped = skipped;
                }
                self.recompute_totals();
            }
            CopyEvent::TableFinished { key } => {
                if let Some(t) = self.tables.get_mut(&key) {
                    t.finished = true;
                    t.status = None;
                }
            }
            CopyEvent::TableStatus { key, message } => {
                if let Some(t) = self.tables.get_mut(&key) {
                    t.status = message;
                }
            }
            CopyEvent::GlobalStatus { message } => {
                self.global_status = message;
            }
            CopyEvent::FileStarted { id, name, total_bytes, .. } => {
                if self.files.iter().all(|f| f.id != id) {
                    self.files.push(FileState {
                        id,
                        name,
                        total_bytes,
                        copied_bytes: 0,
                    });
                }
            }
            CopyEvent::FileProgress { id, copied_bytes } => {
                if let Some(f) = self.files.iter_mut().find(|f| f.id == id) {
                    // Per-chunk events report cumulative bytes for the
                    // file; the global total + speed tracker want
                    // deltas, so we diff against what we last saw.
                    // `saturating_sub` guards against any out-of-order
                    // event that would otherwise underflow.
                    let delta = copied_bytes.saturating_sub(f.copied_bytes);
                    f.copied_bytes = copied_bytes;
                    if delta > 0 {
                        self.overall_bytes_copied += delta;
                        self.speed.record(delta, Instant::now());
                    }
                }
            }
            CopyEvent::FileFinished { id } => {
                // `overall_bytes_copied` already includes every byte
                // this file streamed (we summed deltas as they arrived
                // via `FileProgress`), so removing the row is enough —
                // no extra accounting needed.
                if let Some(idx) = self.files.iter().position(|f| f.id == id) {
                    self.files.remove(idx);
                }
            }
            CopyEvent::Done => {}
        }
    }

    fn recompute_totals(&mut self) {
        let mut q = 0;
        let mut c = 0;
        let mut s = 0;
        for t in self.tables.values() {
            q += t.queued;
            c += t.copied;
            s += t.skipped;
        }
        self.overall_queued = q;
        self.overall_copied = c;
        self.overall_skipped = s;
    }

    /// Pick the table to feature in the header. Active tables only;
    /// among them, the one with the smallest remaining queue — that's
    /// the one closest to finishing, which is what the user usually
    /// wants to watch.
    fn featured_table(&self) -> Option<&TableState> {
        self.tables
            .values()
            .filter(|t| !t.finished)
            .min_by_key(|t| t.queued)
    }
}

// ---------------------------------------------------------------------------
// Drawing
// ---------------------------------------------------------------------------

fn draw(frame: &mut ratatui::Frame, state: &State) {
    let area = frame.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),                // header
            Constraint::Length(1),                // status
            Constraint::Length(LOG_LINES),        // logs
            Constraint::Length(MAX_TABLE_ROWS),   // per-table summary rows
            Constraint::Length(MAX_FILE_ROWS),    // per-file byte progress
        ])
        .split(area);

    draw_header(frame, chunks[0], state);
    draw_status(frame, chunks[1], state);
    draw_logs(frame, chunks[2]);
    draw_tables(frame, chunks[3], state);
    draw_files(frame, chunks[4], state);
}

fn draw_status(frame: &mut ratatui::Frame, area: Rect, state: &State) {
    // Per-table status takes priority — it's almost always more
    // specific than the global one. We fall back to the global status
    // (typically only set during the pre-table discovery phase).
    let text = state
        .featured_table()
        .and_then(|t| t.status.as_deref())
        .or(state.global_status.as_deref())
        .unwrap_or("");

    let line = Line::from(vec![
        Span::styled("→ ", Style::new().fg(DIM)),
        Span::styled(
            text.to_string(),
            Style::new().fg(Color::Gray).add_modifier(Modifier::ITALIC),
        ),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

fn draw_header(frame: &mut ratatui::Frame, area: Rect, state: &State) {
    let elapsed = state.started_at.elapsed();
    let elapsed_str = format_elapsed(elapsed);

    let (ns, tbl) = match state.featured_table() {
        Some(t) => (t.namespace.as_str(), t.table.as_str()),
        None => ("—", "—"),
    };

    // Compact totals on the right — replaces the old standalone bar
    // since per-file totals at the namespace level were dominated by
    // skips and stuck near 100% almost immediately.
    let total = state.overall_queued + state.overall_copied + state.overall_skipped;
    let done = state.overall_copied + state.overall_skipped;
    let bytes_str = format_size(state.overall_bytes_copied);
    let rate_str = format_rate(state.cached_bytes_per_sec);
    let totals = format!("  ·  {done}/{total} files  ·  {bytes_str} copied  ·  {rate_str}");

    let line = Line::from(vec![
        Span::styled("[", Style::new().fg(DIM)),
        Span::styled(elapsed_str, Style::new().fg(Color::Gray)),
        Span::styled("] ", Style::new().fg(DIM)),
        Span::styled("ns ", Style::new().fg(DIM)),
        Span::raw(ns.to_string()),
        Span::styled("  table ", Style::new().fg(DIM)),
        Span::raw(tbl.to_string()),
        Span::styled(totals, Style::new().fg(DIM)),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

/// Render the log pane via [`TuiLoggerWidget`]. Tracing events arrive
/// through the `tui_logger` crate's own subscriber layer — see
/// `init_tui_logger` in `main.rs` — and the widget tails them.
///
/// The panel sits between the per-table summary above and the file
/// progress bars below; thin top + bottom rules give the eye a clear
/// boundary so the log lines don't blur into the surrounding rows.
fn draw_logs(frame: &mut ratatui::Frame, area: Rect) {
    if area.height == 0 {
        return;
    }
    let block = Block::default()
        .borders(Borders::TOP | Borders::BOTTOM)
        .border_style(Style::new().fg(DIM));
    let widget = TuiLoggerWidget::default()
        .block(block)
        .output_separator(' ')
        .output_timestamp(None)
        .output_level(Some(TuiLoggerLevelOutput::Abbreviated))
        .output_target(false)
        .output_file(false)
        .output_line(false)
        // Semantic colors stay for WARN / ERROR; INFO/DEBUG/TRACE all
        // drop to dim gray so the panel reads as background context.
        .style_error(Style::new().fg(Color::Red).add_modifier(Modifier::BOLD))
        .style_warn(Style::new().fg(Color::Yellow))
        .style_info(Style::new().fg(DIM))
        .style_debug(Style::new().fg(DIM))
        .style_trace(Style::new().fg(DIM));
    frame.render_widget(widget, area);
}

fn draw_files(frame: &mut ratatui::Frame, area: Rect, state: &State) {
    if area.height == 0 {
        return;
    }

    // Reserve one row per slot up to MAX_FILE_ROWS so the layout
    // doesn't shift as files come and go. Empty slots stay blank.
    let slots = MAX_FILE_ROWS as usize;
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![Constraint::Length(1); slots])
        .split(area);

    if state.files.is_empty() {
        // The empty file list almost always means we're still in the
        // discovery phase (listing source, walking manifests, scanning
        // dst). A spinner makes it obvious the program is alive — the
        // old static "(no files in flight)" looked like a hang during
        // the minute-long discovery on large tables.
        let spinner = spinner_frame(state.started_at.elapsed());
        let line = Line::from(vec![
            Span::styled(format!("{spinner} "), Style::new().fg(DIM)),
            Span::styled(
                "preparing the files list",
                Style::new().fg(DIM).add_modifier(Modifier::ITALIC),
            ),
        ]);
        frame.render_widget(Paragraph::new(line), rows[0]);
        return;
    }

    for (file, row_area) in state.files.iter().take(slots).zip(rows.iter()) {
        draw_file_row(frame, *row_area, file);
    }
}

fn draw_file_row(frame: &mut ratatui::Frame, area: Rect, file: &FileState) {
    // 20% name | 1 space | gauge | 1 space | size column (fixed)
    let total_w = area.width as usize;
    if total_w < 20 {
        // Tiny terminal — just print the name.
        let line = Line::from(file.name.clone());
        frame.render_widget(Paragraph::new(line), area);
        return;
    }
    let name_w = (total_w / 5).max(8);
    // `<copied> / <total>` — 22 leaves headroom for 4-digit MB values
    // (e.g. `1234.56MB / 5678.90MB`) without truncating.
    let size_w = 22;
    let gap = 2;
    let bar_w = total_w
        .saturating_sub(name_w)
        .saturating_sub(size_w)
        .saturating_sub(gap);

    let parts = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(name_w as u16),
            Constraint::Length(1),
            Constraint::Length(bar_w as u16),
            Constraint::Length(1),
            Constraint::Length(size_w as u16),
        ])
        .split(area);

    let name_line = Line::from(Span::raw(truncate(&file.name, name_w)));
    frame.render_widget(Paragraph::new(name_line), parts[0]);

    let ratio = if file.total_bytes == 0 {
        0.0
    } else {
        (file.copied_bytes as f64 / file.total_bytes as f64).clamp(0.0, 1.0)
    };
    let gauge = Gauge::default()
        .gauge_style(Style::new().fg(BAR_FILL))
        .ratio(ratio)
        .label("");
    frame.render_widget(gauge, parts[2]);

    let size_label = format!(
        "{:>w$}",
        format!(
            "{} / {}",
            format_size(file.copied_bytes),
            format_size(file.total_bytes)
        ),
        w = size_w,
    );
    frame.render_widget(
        Paragraph::new(Line::from(Span::raw(size_label))),
        parts[4],
    );
}

fn draw_tables(frame: &mut ratatui::Frame, area: Rect, state: &State) {
    if area.height == 0 {
        return;
    }

    let slots = MAX_TABLE_ROWS as usize;
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![Constraint::Length(1); slots])
        .split(area);

    // Sort by table key so the row order is stable across redraws.
    // Featured-table selection (smallest queue) drives the *header*,
    // not the row order — moving a table around mid-copy is more
    // distracting than helpful.
    let mut active: Vec<&TableState> = state
        .tables
        .values()
        .filter(|t| !t.finished)
        .collect();
    active.sort_by(|a, b| a.table.cmp(&b.table));

    if active.is_empty() {
        let line = Line::from(Span::styled("(no active tables)", Style::new().fg(DIM)));
        frame.render_widget(Paragraph::new(line), rows[0]);
        return;
    }

    for (table, row_area) in active.iter().take(slots).zip(rows.iter()) {
        draw_table_row(frame, *row_area, table);
    }
}

fn draw_table_row(frame: &mut ratatui::Frame, area: Rect, table: &TableState) {
    let total_w = area.width as usize;
    if total_w < 30 {
        let line = Line::from(table.table.clone());
        frame.render_widget(Paragraph::new(line), area);
        return;
    }
    let name_w = (total_w / 5).max(10);
    let count_w = 18;
    let gap = 2;
    let bar_w = total_w
        .saturating_sub(name_w)
        .saturating_sub(count_w)
        .saturating_sub(gap);

    let parts = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(name_w as u16),
            Constraint::Length(1),
            Constraint::Length(bar_w as u16),
            Constraint::Length(1),
            Constraint::Length(count_w as u16),
        ])
        .split(area);

    let name_line = Line::from(Span::raw(truncate(&table.table, name_w)));
    frame.render_widget(Paragraph::new(name_line), parts[0]);

    let total = table.queued + table.copied + table.skipped;
    let done = table.copied + table.skipped;

    if total == 0 {
        // Pre-copy phase — show the per-table status text so the user
        // sees what each table is doing (loading metadata, walking
        // manifest tree, etc.) rather than an empty bar.
        let status = table.status.as_deref().unwrap_or("(starting)");
        let line = Line::from(Span::styled(
            status.to_string(),
            Style::new().fg(Color::Gray).add_modifier(Modifier::ITALIC),
        ));
        frame.render_widget(Paragraph::new(line), parts[2]);
    } else {
        let ratio = (done as f64 / total as f64).clamp(0.0, 1.0);
        let gauge = Gauge::default()
            .gauge_style(Style::new().fg(BAR_FILL))
            .ratio(ratio)
            .label("");
        frame.render_widget(gauge, parts[2]);
    }

    let count_label = if total == 0 {
        String::new()
    } else {
        format!("{:>w$}", format!("{done}/{total}"), w = count_w)
    };
    frame.render_widget(
        Paragraph::new(Line::from(Span::raw(count_label))),
        parts[4],
    );
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn format_elapsed(d: Duration) -> String {
    let secs = d.as_secs();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}:{m:02}:{s:02}")
    } else {
        format!("{m:02}:{s:02}")
    }
}

/// Round to MB with two decimals when ≥ 1MB, otherwise KB with one
/// decimal. We deliberately don't show bytes — they're noisy and the
/// source files are nearly always ≥ KiBs.
fn format_size(bytes: u64) -> String {
    const MB: f64 = 1024.0 * 1024.0;
    const KB: f64 = 1024.0;
    let b = bytes as f64;
    if b >= MB {
        format!("{:.2}MB", b / MB)
    } else {
        format!("{:.1}KB", b / KB)
    }
}

/// Throughput display — same MB/KB conventions as `format_size`,
/// suffixed with `/s`. `None` means "no transfer activity yet" (the
/// pre-first-byte discovery phase, or an all-skipped run); we render
/// `--` so the user can tell that state apart from a real 0.0KB/s
/// stall.
fn format_rate(bytes_per_sec: Option<u64>) -> String {
    match bytes_per_sec {
        Some(bps) => format!("{}/s", format_size(bps)),
        None => "--".to_string(),
    }
}

/// Total bytes divided by elapsed seconds, with a 1-second floor so
/// a sub-second elapsed time doesn't blow up the divisor. `None` when
/// no bytes were copied — distinguishes an all-skipped run from a
/// sustained-zero-throughput one.
fn lifetime_rate(bytes: u64, elapsed: Duration) -> Option<u64> {
    if bytes == 0 {
        return None;
    }
    let secs = elapsed.as_secs().max(1);
    Some(bytes / secs)
}

/// One frame of a braille-dot spinner, advancing every 100 ms — the
/// renderer's draw tick. Driven by elapsed wall time rather than a
/// frame counter so a pause in redraws doesn't leave the spinner
/// stuck on the same glyph.
fn spinner_frame(elapsed: Duration) -> char {
    const FRAMES: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
    let idx = (elapsed.as_millis() / 100) as usize % FRAMES.len();
    FRAMES[idx]
}

fn truncate(s: &str, max: usize) -> String {
    // Iceberg filenames are usually `<UUID>-mNN.avro` or long-hash
    // parquet files — the tail carries no extra signal once truncated,
    // so we keep the head.
    if s.chars().count() <= max {
        return s.to_string();
    }
    if max <= 1 {
        return "…".to_string();
    }
    let head: String = s.chars().take(max - 1).collect();
    format!("{head}…")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_size_picks_mb_above_one_megabyte() {
        assert_eq!(format_size(0), "0.0KB");
        assert_eq!(format_size(512), "0.5KB");
        assert_eq!(format_size(1024 * 1024), "1.00MB");
        assert_eq!(format_size(2 * 1024 * 1024 + 512 * 1024), "2.50MB");
    }

    #[test]
    fn format_rate_suffixes_with_per_second_or_dashes_when_none() {
        assert_eq!(format_rate(Some(1024 * 1024)), "1.00MB/s");
        assert_eq!(format_rate(Some(512)), "0.5KB/s");
        assert_eq!(format_rate(Some(0)), "0.0KB/s");
        // `--` distinguishes "no activity yet" from a real 0.0KB/s
        // stall; the renderer relies on this.
        assert_eq!(format_rate(None), "--");
    }

    #[test]
    fn lifetime_rate_floors_divisor_at_one_second_and_is_none_when_empty() {
        assert_eq!(
            lifetime_rate(2_000_000, Duration::from_millis(0)),
            Some(2_000_000)
        );
        assert_eq!(
            lifetime_rate(2_000_000, Duration::from_secs(2)),
            Some(1_000_000)
        );
        assert_eq!(lifetime_rate(0, Duration::from_secs(60)), None);
    }

    /// The spinner must advance over time and wrap cleanly — a
    /// stuck-glyph regression would look like a hung process.
    #[test]
    fn spinner_advances_and_wraps() {
        let f0 = spinner_frame(Duration::from_millis(0));
        let f1 = spinner_frame(Duration::from_millis(100));
        assert_ne!(f0, f1, "spinner must change between adjacent 100ms ticks");
        // 10 frames * 100ms = one full cycle.
        assert_eq!(f0, spinner_frame(Duration::from_millis(1000)));
    }

    /// Bytes total must reflect in-flight transfers (via `FileProgress`
    /// deltas), not jump only when a file finishes — the header
    /// would otherwise sit at zero for the whole life of a long
    /// single-file copy.
    #[test]
    fn overall_bytes_updates_live_from_file_progress_deltas() {
        let mut state = State::new();
        state.apply(CopyEvent::FileStarted {
            id: 1,
            table_key: "t".into(),
            name: "a.parquet".into(),
            total_bytes: 1_000,
        });
        state.apply(CopyEvent::FileProgress {
            id: 1,
            copied_bytes: 300,
        });
        assert_eq!(state.overall_bytes_copied, 300);

        state.apply(CopyEvent::FileProgress {
            id: 1,
            copied_bytes: 1_000,
        });
        assert_eq!(state.overall_bytes_copied, 1_000);

        state.apply(CopyEvent::FileFinished { id: 1 });
        // FileFinished must not double-count — the bytes were already
        // captured by the FileProgress deltas above.
        assert_eq!(state.overall_bytes_copied, 1_000);
    }

    #[test]
    fn truncate_keeps_head_and_appends_ellipsis() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("verylongfilename", 8), "verylon…");
    }

    #[test]
    fn format_elapsed_drops_hours_when_under_one() {
        assert_eq!(format_elapsed(Duration::from_secs(0)), "00:00");
        assert_eq!(format_elapsed(Duration::from_secs(65)), "01:05");
        assert_eq!(format_elapsed(Duration::from_secs(3725)), "1:02:05");
    }

    #[test]
    fn featured_table_picks_smallest_active_queue() {
        let mut state = State::new();
        state.apply(CopyEvent::TableStarted {
            key: "a".into(),
            namespace: "ns-a".into(),
            table: "t-a".into(),
        });
        state.apply(CopyEvent::TableStarted {
            key: "b".into(),
            namespace: "ns-b".into(),
            table: "t-b".into(),
        });
        state.apply(CopyEvent::TableQueueChanged {
            key: "a".into(),
            queued: 100,
            copied: 0,
            skipped: 0,
        });
        state.apply(CopyEvent::TableQueueChanged {
            key: "b".into(),
            queued: 5,
            copied: 0,
            skipped: 0,
        });
        let featured = state.featured_table().unwrap();
        assert_eq!(featured.table, "t-b");

        // Once `b` finishes, `a` takes over even though its queue is
        // larger — it's the only one still active.
        state.apply(CopyEvent::TableFinished { key: "b".into() });
        assert_eq!(state.featured_table().unwrap().table, "t-a");
    }
}
