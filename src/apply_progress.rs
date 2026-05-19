//! Per-statement progress reporting for `--apply` (#45).
//!
//! Headless rendering: one stderr line per executed statement plus a
//! final class-breakdown summary. The TUI's interactive apply path
//! stays on `tui/mod.rs`; this module is only used by `main.rs`'s
//! `apply_inline` / `apply_manifest`.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Write};
use std::time::Duration;

use crate::db::StmtResult;

/// User-facing setting for progress emission.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ProgressMode {
    /// Emit when stderr is a TTY; silent otherwise. Default.
    #[default]
    Auto,
    /// Always emit progress.
    On,
    /// Never emit progress.
    Off,
}

impl ProgressMode {
    /// Resolve to a definite "should I emit?" decision at apply time.
    /// Auto consults `stderr.is_terminal()`; explicit modes win.
    pub fn resolved(self) -> bool {
        match self {
            ProgressMode::On => true,
            ProgressMode::Off => false,
            ProgressMode::Auto => std::io::stderr().is_terminal(),
        }
    }
}

/// Width chosen so the right-padded SQL preview leaves room for the
/// time column without wrapping at typical terminal widths.
const PREVIEW_MAX: usize = 60;

/// Print one `[i/total] <preview>  <ms>ms` line to stderr. Errors are
/// suffixed with `FAIL`. Swallowed if stderr is closed (broken pipe)
/// to satisfy the issue's `--apply 2>/dev/null` acceptance criterion.
pub fn print_progress(result: &StmtResult, index: usize, total: usize) {
    let preview = sql_one_line(&result.sql, PREVIEW_MAX);
    let ms = result.duration.as_millis();
    let width = digit_count(total);
    let status = if result.error.is_some() { "  FAIL" } else { "" };
    let mut stderr = std::io::stderr().lock();
    // Best-effort write — a broken pipe (e.g. `2>/dev/null` with an
    // OS that closes the descriptor early) must not abort the apply.
    let _ = writeln!(
        stderr,
        "[{idx:0w$}/{total}] {preview:<pw$} {ms:>6}ms{status}",
        idx = index,
        w = width,
        total = total,
        preview = preview,
        pw = PREVIEW_MAX,
        ms = ms,
        status = status,
    );
}

/// Per-class accumulator for the final summary line.
#[derive(Default)]
pub struct ApplyStats {
    by_class: BTreeMap<&'static str, usize>,
    count: usize,
    total_dur: Duration,
    max_dur: Duration,
}

impl ApplyStats {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one *successful* statement's contribution. Failed statements
    /// are deliberately excluded so `render_summary`'s count matches
    /// the apply-summary line ("uvg: applied N statement(s)"), which
    /// only counts successes. A failure's per-statement progress line
    /// already carries the FAIL suffix so the user sees where it died.
    pub fn record(&mut self, result: &StmtResult) {
        if result.error.is_some() {
            return;
        }
        self.count += 1;
        self.total_dur += result.duration;
        if result.duration > self.max_dur {
            self.max_dur = result.duration;
        }
        *self.by_class.entry(classify(&result.sql)).or_insert(0) += 1;
    }

    /// Render the final summary line. Empty string when no statements
    /// were recorded — the caller decides whether to print anything.
    pub fn render_summary(&self) -> String {
        if self.count == 0 {
            return String::new();
        }
        let total_ms = self.total_dur.as_millis();
        let avg_ms = total_ms / self.count as u128;
        let max_ms = self.max_dur.as_millis();
        let parts: Vec<String> = self
            .by_class
            .iter()
            .map(|(k, v)| format!("{v} {k}"))
            .collect();
        format!(
            "Applied {} statement(s) in {}ms (avg {avg_ms}ms, max {max_ms}ms): {}",
            self.count,
            total_ms,
            parts.join(", "),
        )
    }
}

/// Collapse all whitespace, drop leading/trailing space, truncate with
/// an ellipsis. `CREATE TABLE\n  "users" (...)` becomes
/// `CREATE TABLE "users" (...)` — fits one terminal line.
fn sql_one_line(sql: &str, max: usize) -> String {
    let collapsed: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= max {
        return collapsed;
    }
    // `max < 3` can't fit even the ellipsis without exceeding the
    // caller's char budget. Degenerate to a plain prefix in that
    // window so the function's contract ("≤ max chars out") holds at
    // every input size.
    if max < 3 {
        return collapsed.chars().take(max).collect();
    }
    // Cut on a char boundary that's the END of the (max-3)th char, so
    // appending "..." yields exactly `max` chars out.
    let end = collapsed
        .char_indices()
        .nth(max - 3)
        .map(|(byte_idx, _)| byte_idx)
        .unwrap_or(collapsed.len());
    format!("{}...", &collapsed[..end])
}

/// Classify a DDL statement by its leading tokens. Buckets match the
/// issue's "tables / indexes / FKs / CHECKs / other" categories.
fn classify(sql: &str) -> &'static str {
    let upper = sql.trim_start().to_uppercase();
    if upper.starts_with("CREATE TABLE") {
        "tables"
    } else if upper.starts_with("CREATE UNIQUE INDEX") || upper.starts_with("CREATE INDEX") {
        "indexes"
    } else if upper.starts_with("CREATE TYPE") {
        "types"
    } else if upper.starts_with("COMMENT ON") {
        "comments"
    } else if upper.starts_with("ALTER TABLE") {
        // ALTER TABLE wears many hats. Disambiguate via the
        // ADD CONSTRAINT prefix so we don't mis-classify a column or
        // identifier that happens to contain the substring "CHECK" or
        // "FOREIGN KEY". Order matters: an FK-add CONSTRAINT clause
        // can mention CHECK in a column comment, so check FK first.
        if upper.contains("ADD CONSTRAINT") {
            if upper.contains("FOREIGN KEY") {
                "FKs"
            } else if upper.contains(" CHECK") {
                "CHECKs"
            } else {
                "alters"
            }
        } else {
            "alters"
        }
    } else if upper.starts_with("DROP") {
        "drops"
    } else {
        "other"
    }
}

fn digit_count(n: usize) -> usize {
    if n == 0 {
        1
    } else {
        (n as f64).log10().floor() as usize + 1
    }
}

#[cfg(test)]
#[path = "apply_progress_tests.rs"]
mod tests;
