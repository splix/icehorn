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
use std::time::{Duration, Instant};

use anyhow::Result;
use ratatui::DefaultTerminal;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Gauge, Paragraph};
use ratatui::{TerminalOptions, Viewport};
use tokio::sync::mpsc;
use tui_logger::{TuiLoggerLevelOutput, TuiLoggerWidget};

use super::event::CopyEvent;

/// Lines reserved for log output. Fits the recent useful chatter
/// without dominating the view.
const LOG_LINES: u16 = 6;
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

/// Single accent used for both progress bar fills. Grayscale on purpose
/// — the bars convey position, not severity, so they shouldn't compete
/// with the actually-meaningful colors of WARN / ERROR log lines.
const BAR_FILL: Color = Color::Gray;
/// Slightly dimmer gray for status / hint text.
const DIM: Color = Color::DarkGray;

pub fn run(rx: mpsc::UnboundedReceiver<CopyEvent>) -> Result<()> {
    let mut terminal = ratatui::init_with_options(TerminalOptions {
        viewport: Viewport::Inline(VIEWPORT_HEIGHT),
    });
    let result = main_loop(&mut terminal, rx);
    ratatui::restore();

    // After restore the inline viewport is gone, so any rich state we
    // showed is too. Print a permanent summary into the scrollback so
    // users who weren't watching the live view still see what happened.
    if let Ok(state) = &result {
        print_summary(state);
    }
    result.map(|_| ())
}

/// Drive the draw loop. We poll the event channel (non-blocking) and
/// crossterm input (also non-blocking) on a tick, so neither side
/// starves the other. `CopyEvent::Done` triggers a clean exit.
fn main_loop(
    terminal: &mut DefaultTerminal,
    mut rx: mpsc::UnboundedReceiver<CopyEvent>,
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

        terminal.draw(|frame| draw(frame, &state))?;

        if closed {
            return Ok(state);
        }

        // Allow Ctrl+C / q to drop the UI early. The copy keeps running
        // — we only let go of the screen.
        if event::poll(tick)?
            && let Event::Key(k) = event::read()?
            && k.kind == KeyEventKind::Press
            && matches!(k.code, KeyCode::Char('q') | KeyCode::Esc)
        {
            return Ok(state);
        }
    }
}

/// Render the final stats to stderr. We print to stderr (not stdout)
/// so anyone redirecting `copy`'s stdout to `tee` or another consumer
/// gets a clean stream — same convention as `tracing` logs in
/// `--plain` mode.
fn print_summary(state: &State) {
    let elapsed = format_elapsed(state.started_at.elapsed());
    let total_files = state.overall_copied + state.overall_skipped;
    let bytes_str = format_size(state.overall_bytes_copied);

    eprintln!();
    eprintln!("Copy summary");
    eprintln!("  Elapsed : {elapsed}");
    eprintln!("  Tables  : {}", state.tables.len());
    eprintln!(
        "  Files   : {} copied, {} skipped ({total_files} total)",
        state.overall_copied, state.overall_skipped
    );
    eprintln!("  Bytes   : {bytes_str} transferred");

    // Per-table breakdown when there's more than one — single-table
    // runs would just duplicate the totals above.
    if state.tables.len() > 1 {
        eprintln!();
        eprintln!("Per-table:");
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
    /// Total bytes transferred across every file copy that has finished
    /// this session — copied or partial. Skipped files don't add. We
    /// take the value from each `FileState.copied_bytes` at finish so a
    /// failed mid-transfer still reflects the truth.
    overall_bytes_copied: u64,
    overall_copied: u64,
    overall_skipped: u64,
    overall_queued: u64,
    /// Status text shown when no table-specific status applies
    /// (typically: pre-table discovery phase).
    global_status: Option<String>,
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
        }
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
                    f.copied_bytes = copied_bytes;
                }
            }
            CopyEvent::FileFinished { id } => {
                if let Some(idx) = self.files.iter().position(|f| f.id == id) {
                    self.overall_bytes_copied += self.files[idx].copied_bytes;
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
    let totals = format!("  ·  {done}/{total} files  ·  {bytes_str} copied");

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
fn draw_logs(frame: &mut ratatui::Frame, area: Rect) {
    if area.height == 0 {
        return;
    }
    let widget = TuiLoggerWidget::default()
        .output_separator(' ')
        .output_timestamp(None)
        .output_level(Some(TuiLoggerLevelOutput::Abbreviated))
        .output_target(false)
        .output_file(false)
        .output_line(false)
        // Semantic colors for severity stay; INFO/DEBUG/TRACE go gray
        // so they don't shout for attention.
        .style_error(Style::new().fg(Color::Red).add_modifier(Modifier::BOLD))
        .style_warn(Style::new().fg(Color::Yellow))
        .style_info(Style::new().fg(Color::Gray))
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
        let line = Line::from(Span::styled("(no files in flight)", Style::new().fg(DIM)));
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
