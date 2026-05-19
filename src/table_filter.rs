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
#[path = "table_filter_tests.rs"]
mod tests;
