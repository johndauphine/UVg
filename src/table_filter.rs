//! Glob-based table inclusion/exclusion. Built from `--tables` and
//! `--exclude-tables`, evaluated against bare table names during
//! introspection.
//!
//! Pattern syntax is standard glob (`*`, `?`, `[abc]`), per the `glob`
//! crate. A bare name with no metacharacters degenerates to an exact
//! match — back-compat with the original `--tables foo,bar` form.
//!
//! Match order is: an empty `includes` list means "all tables"; non-empty
//! `includes` filters to only tables matching at least one pattern; then
//! `excludes` removes any matching table.

use glob::Pattern;

use crate::error::UvgError;

/// Decision oracle: "should this table name be introspected?"
#[derive(Debug, Default)]
pub(crate) struct TableFilter {
    includes: Vec<Pattern>,
    excludes: Vec<Pattern>,
}

impl TableFilter {
    /// Parse and validate `--tables` and `--exclude-tables` patterns.
    /// Returns `Err` on the first malformed pattern so the user sees the
    /// problem before any DB connection is opened.
    pub(crate) fn new(includes: &[String], excludes: &[String]) -> Result<Self, UvgError> {
        Ok(Self {
            includes: parse_patterns(includes, "tables")?,
            excludes: parse_patterns(excludes, "exclude-tables")?,
        })
    }

    /// Convenience constructor for the empty filter (matches everything).
    pub(crate) fn allow_all() -> Self {
        Self::default()
    }

    /// `true` when the table should be introspected. Empty `includes`
    /// means "all"; any include match qualifies; any exclude match
    /// disqualifies. Exclude wins over include.
    pub(crate) fn matches(&self, name: &str) -> bool {
        let included = self.includes.is_empty() || self.includes.iter().any(|p| p.matches(name));
        if !included {
            return false;
        }
        !self.excludes.iter().any(|p| p.matches(name))
    }
}

fn parse_patterns(raw: &[String], flag: &'static str) -> Result<Vec<Pattern>, UvgError> {
    raw.iter()
        .map(|s| {
            Pattern::new(s).map_err(|e| UvgError::InvalidTablePattern {
                flag,
                pattern: s.clone(),
                reason: e.to_string(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn empty_filter_allows_everything() {
        let f = TableFilter::allow_all();
        assert!(f.matches("anything"));
        assert!(f.matches("_pg_internal"));
        assert!(f.matches(""));
    }

    #[test]
    fn exact_name_matches_only_that_name() {
        // No metacharacters: behaves like the original `--tables foo` form.
        let f = TableFilter::new(&s(&["users"]), &s(&[])).unwrap();
        assert!(f.matches("users"));
        assert!(!f.matches("users_archive"));
        assert!(!f.matches("orders"));
    }

    #[test]
    fn glob_star_matches_prefix() {
        let f = TableFilter::new(&s(&["users_*"]), &s(&[])).unwrap();
        assert!(f.matches("users_active"));
        assert!(f.matches("users_archive"));
        assert!(!f.matches("users")); // `*` requires at least one char before "users_"
        assert!(!f.matches("orders"));
    }

    #[test]
    fn multiple_includes_or_together() {
        let f = TableFilter::new(&s(&["users_*", "orders_*"]), &s(&[])).unwrap();
        assert!(f.matches("users_active"));
        assert!(f.matches("orders_pending"));
        assert!(!f.matches("invoices"));
    }

    #[test]
    fn exclude_only_drops_matches() {
        let f = TableFilter::new(&s(&[]), &s(&["__*"])).unwrap();
        assert!(f.matches("users"));
        assert!(!f.matches("__migrations"));
        assert!(!f.matches("__pgbench_history"));
    }

    #[test]
    fn exclude_wins_over_include() {
        // Per the docs: includes first, then excludes drop.
        let f = TableFilter::new(&s(&["*"]), &s(&["audit_*", "logs_*"])).unwrap();
        assert!(f.matches("users"));
        assert!(f.matches("orders"));
        assert!(!f.matches("audit_trail"));
        assert!(!f.matches("logs_2026"));
    }

    #[test]
    fn glob_question_mark_matches_single_char() {
        let f = TableFilter::new(&s(&["t?bl"]), &s(&[])).unwrap();
        assert!(f.matches("tabl"));
        assert!(f.matches("tibl"));
        assert!(!f.matches("table"));
        assert!(!f.matches("tbl"));
    }

    #[test]
    fn glob_charset_matches_class() {
        let f = TableFilter::new(&s(&["[ab]_x"]), &s(&[])).unwrap();
        assert!(f.matches("a_x"));
        assert!(f.matches("b_x"));
        assert!(!f.matches("c_x"));
    }

    #[test]
    fn invalid_pattern_in_includes_errors_with_flag_context() {
        let err = TableFilter::new(&s(&["[unclosed"]), &s(&[])).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("tables"), "expected flag name in error: {msg}");
        assert!(
            msg.contains("[unclosed"),
            "expected pattern in error: {msg}"
        );
    }

    #[test]
    fn metacharacters_in_real_table_names_can_be_escaped() {
        // A table literally named `users_*` (yes, MySQL/PG allow it with
        // quoting) can be matched by escaping the `*` as `[*]` per glob
        // syntax. Documents the escape path for the rare case where a real
        // identifier contains a glob metacharacter.
        let f = TableFilter::new(&s(&["users_[*]"]), &s(&[])).unwrap();
        assert!(f.matches("users_*"));
        assert!(!f.matches("users_active"));
    }

    #[test]
    fn invalid_pattern_in_excludes_errors_with_flag_context() {
        let err = TableFilter::new(&s(&[]), &s(&["[unclosed"])).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("exclude-tables"),
            "expected exclude-tables flag in error: {msg}"
        );
    }
}
