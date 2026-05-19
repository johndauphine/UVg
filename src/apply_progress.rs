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
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ProgressMode {
    /// Emit when stderr is a TTY; silent otherwise. Default.
    Auto,
    /// Always emit progress.
    On,
    /// Never emit progress.
    Off,
}

impl Default for ProgressMode {
    fn default() -> Self {
        ProgressMode::Auto
    }
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

    /// Add one statement's contribution. Called by the on-statement
    /// observer regardless of progress emission.
    pub fn record(&mut self, result: &StmtResult) {
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
    // Cut on char boundaries; never split a multi-byte char in half.
    let mut end = 0;
    for (i, _) in collapsed.char_indices().take(max.saturating_sub(3)) {
        end = i;
    }
    // Walk one more char so `end` becomes the byte index AFTER the
    // (max-3)th char rather than the start of it.
    if let Some((byte_after, _)) = collapsed.char_indices().nth(max.saturating_sub(3)) {
        end = byte_after;
    }
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
        // ALTER TABLE wears many hats. Disambiguate by inspecting the
        // body for the action keyword. Order matters: FK references
        // can mention CHECK in column comments, etc., so check FK first.
        if upper.contains(" FOREIGN KEY") {
            "FKs"
        } else if upper.contains(" CHECK") {
            "CHECKs"
        } else if upper.contains(" ADD COLUMN") || upper.contains(" ADD ") {
            "alters"
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
mod tests {
    use super::*;

    fn r(sql: &str, ms: u64) -> StmtResult {
        StmtResult {
            sql: sql.to_string(),
            error: None,
            duration: Duration::from_millis(ms),
        }
    }

    #[test]
    fn sql_one_line_collapses_and_keeps_short_text() {
        assert_eq!(sql_one_line("SELECT 1", 60), "SELECT 1");
        assert_eq!(
            sql_one_line("CREATE TABLE\n  \"users\" (id int)", 60),
            "CREATE TABLE \"users\" (id int)"
        );
    }

    #[test]
    fn sql_one_line_truncates_with_ellipsis() {
        let long = "CREATE INDEX very_long_name_that_will_definitely_overflow_the_preview_width ON t (col)";
        let out = sql_one_line(long, 20);
        assert_eq!(out.chars().count(), 20);
        assert!(out.ends_with("..."));
        assert!(out.starts_with("CREATE INDEX"));
    }

    #[test]
    fn sql_one_line_respects_char_boundaries() {
        // Multi-byte chars must not be cut mid-byte. 5-char preview
        // limit with a string of multi-byte chars should return chars
        // not bytes.
        let s = "αβγδεζηθικλμνξο";
        let out = sql_one_line(s, 8);
        assert!(out.chars().count() <= 8);
        assert!(out.is_char_boundary(out.len()));
    }

    #[test]
    fn classify_buckets() {
        assert_eq!(classify("CREATE TABLE foo (id int)"), "tables");
        assert_eq!(classify("create table foo (id int)"), "tables");
        assert_eq!(classify("CREATE INDEX ix_foo ON foo(id)"), "indexes");
        assert_eq!(classify("CREATE UNIQUE INDEX uq_foo ON foo(id)"), "indexes");
        assert_eq!(classify("CREATE TYPE color AS ENUM ('r','g','b')"), "types");
        assert_eq!(classify("COMMENT ON TABLE foo IS 'x'"), "comments");
        assert_eq!(
            classify("ALTER TABLE foo ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES b(id)"),
            "FKs"
        );
        assert_eq!(
            classify("ALTER TABLE foo ADD CONSTRAINT ck CHECK (x > 0)"),
            "CHECKs"
        );
        assert_eq!(classify("ALTER TABLE foo ADD COLUMN bar int"), "alters");
        assert_eq!(classify("DROP TABLE foo"), "drops");
        assert_eq!(classify("VACUUM"), "other");
    }

    #[test]
    fn stats_summary_breaks_down_by_class() {
        let mut stats = ApplyStats::new();
        stats.record(&r("CREATE TABLE users (id int)", 10));
        stats.record(&r("CREATE INDEX ix_users_id ON users(id)", 20));
        stats.record(&r("CREATE INDEX ix_users_name ON users(name)", 50));
        stats.record(&r("ALTER TABLE users ADD CONSTRAINT fk FOREIGN KEY (org_id) REFERENCES orgs(id)", 5));
        let s = stats.render_summary();
        assert!(s.starts_with("Applied 4 statement(s)"), "got: {s}");
        assert!(s.contains("1 tables"), "got: {s}");
        assert!(s.contains("2 indexes"), "got: {s}");
        assert!(s.contains("1 FKs"), "got: {s}");
        assert!(s.contains("max 50ms"), "got: {s}");
    }

    #[test]
    fn stats_summary_empty_returns_empty_string() {
        let stats = ApplyStats::new();
        assert_eq!(stats.render_summary(), "");
    }

    #[test]
    fn digit_count_basic() {
        assert_eq!(digit_count(0), 1);
        assert_eq!(digit_count(1), 1);
        assert_eq!(digit_count(9), 1);
        assert_eq!(digit_count(10), 2);
        assert_eq!(digit_count(99), 2);
        assert_eq!(digit_count(100), 3);
        assert_eq!(digit_count(999), 3);
        assert_eq!(digit_count(1000), 4);
    }

    #[test]
    fn progress_mode_resolved_respects_explicit_overrides() {
        // Explicit modes never consult the TTY.
        assert!(ProgressMode::On.resolved());
        assert!(!ProgressMode::Off.resolved());
        // Auto's behavior depends on the test runner's stderr — don't
        // assert a specific value; just confirm it returns SOMETHING
        // without panicking.
        let _ = ProgressMode::Auto.resolved();
    }
}
