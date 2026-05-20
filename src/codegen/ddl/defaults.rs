use crate::dialect::Dialect;

/// Format a default value expression for the target dialect.
/// When `is_boolean` is true, translates 0/1 ↔ true/false across dialects.
pub(in crate::codegen) fn format_ddl_default_typed(
    default: &str,
    source_dialect: Dialect,
    target_dialect: Dialect,
    is_boolean: bool,
) -> String {
    // Step 1: Strip source-specific syntax
    let cleaned = match source_dialect {
        Dialect::Postgres => super::super::strip_pg_typecast(default),
        Dialect::Mssql => super::super::strip_mssql_parens(default),
        Dialect::Mysql | Dialect::Sqlite => default.trim(),
    };

    // Step 2: Boolean literal translation (only when column is boolean)
    if is_boolean {
        let lower = cleaned.trim().to_lowercase();
        if (lower == "1" || lower == "true")
            && (target_dialect == Dialect::Postgres || target_dialect == Dialect::Sqlite)
        {
            return "true".to_string();
        }
        if (lower == "0" || lower == "false")
            && (target_dialect == Dialect::Postgres || target_dialect == Dialect::Sqlite)
        {
            return "false".to_string();
        }
        if (lower == "true" || lower == "1")
            && (target_dialect == Dialect::Mysql || target_dialect == Dialect::Mssql)
        {
            return "1".to_string();
        }
        if (lower == "false" || lower == "0")
            && (target_dialect == Dialect::Mysql || target_dialect == Dialect::Mssql)
        {
            return "0".to_string();
        }
    }

    // Step 3: Translate common function names
    let result = translate_default_function(cleaned, target_dialect);

    // Step 4: Quote bare string defaults that MySQL stores without quotes.
    // Numeric values, SQL keywords, function calls, and already-quoted strings pass through.
    ensure_default_quoting(&result)
}

/// Ensure a default value is properly quoted if it's a bare string literal.
/// MySQL stores string defaults without quotes (e.g. `member` instead of `'member'`).
/// Numbers, NULL, function calls (containing parens), boolean keywords,
/// and already-quoted strings are left as-is.
pub(super) fn ensure_default_quoting(expr: &str) -> String {
    let trimmed = expr.trim();

    // Already quoted
    if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
    {
        return trimmed.to_string();
    }

    // NULL
    if trimmed.eq_ignore_ascii_case("null") {
        return trimmed.to_string();
    }

    // Boolean keywords
    if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false") {
        return trimmed.to_string();
    }

    // Numeric (integer or decimal)
    if trimmed.parse::<f64>().is_ok() {
        return trimmed.to_string();
    }

    // Function call (contains parentheses)
    if trimmed.contains('(') {
        return trimmed.to_string();
    }

    // SQL keywords that are valid unquoted defaults
    let upper = trimmed.to_uppercase();
    if matches!(
        upper.as_str(),
        "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME" | "CURRENT_USER"
    ) {
        return trimmed.to_string();
    }

    // Bare string — needs quoting
    format!("'{}'", trimmed.replace('\'', "''"))
}

/// Translate common SQL functions between dialects.
pub(super) fn translate_default_function(expr: &str, target: Dialect) -> String {
    let lower = expr.trim().to_lowercase();

    // Strip a trailing precision suffix like CURRENT_TIMESTAMP(6). The (6)
    // is meaningful on the source (sub-second precision matches the column's
    // precision), but for cross-dialect translation we collapse to the base
    // function name and let the column-type emission carry precision via
    // CanonicalType::Timestamp.precision (#36). Without this strip, the
    // translation match below misses anything with a precision suffix.
    let base = strip_precision_suffix(&lower);

    // Normalize common "now-style" functions returning a full date+time.
    // Includes MSSQL variants that #32 was missing: GETUTCDATE, SYSDATETIME,
    // SYSDATETIMEOFFSET, plus PG/SQL standard LOCALTIMESTAMP. None of these
    // are TZ-class load-bearing here — the column's TZ is preserved by the
    // type-mapping path; this function just picks a target-dialect-idiomatic
    // "now" function so the apply step doesn't fail with "function does not
    // exist" on PG/MySQL.
    let is_now_dt = matches!(
        base.as_str(),
        "now()"
            | "now"
            | "current_timestamp"
            | "current_timestamp()"
            | "localtimestamp"
            | "localtimestamp()"
            | "getdate()"
            | "getutcdate()"
            | "sysdatetime()"
            | "sysdatetimeoffset()"
    );
    if is_now_dt {
        return match target {
            Dialect::Postgres => "now()".to_string(),
            Dialect::Mysql | Dialect::Sqlite => "CURRENT_TIMESTAMP".to_string(),
            Dialect::Mssql => "GETDATE()".to_string(),
        };
    }

    // UUID generators
    if base == "gen_random_uuid()" || base == "uuid()" || base == "newid()" {
        return match target {
            Dialect::Postgres => "gen_random_uuid()".to_string(),
            Dialect::Mysql => "(UUID())".to_string(),
            Dialect::Mssql => "NEWID()".to_string(),
            Dialect::Sqlite => "NULL".to_string(), // No native UUID in SQLite
        };
    }

    // Pass through as-is (string literals, numbers, etc.)
    // Note: boolean 0/1 ↔ true/false translation is handled in format_ddl_default_typed()
    // with is_boolean flag to avoid converting integer defaults.
    expr.to_string()
}

/// Extract the sub-second precision from a CanonicalType::Time / Timestamp,
/// or None for non-temporal types or temporals without a stored precision.
/// Used for MySQL DATETIME(N)/TIMESTAMP(N) default-precision symmetry (#36).
pub(super) fn temporal_precision(ct: &crate::ddl_typemap::CanonicalType) -> Option<u8> {
    match ct {
        crate::ddl_typemap::CanonicalType::Time { precision, .. }
        | crate::ddl_typemap::CanonicalType::Timestamp { precision, .. } => *precision,
        _ => None,
    }
}

/// Re-attach a precision suffix `(N)` to a now-family default expression
/// (CURRENT_TIMESTAMP / GETDATE() / now() / etc.) when targeting MySQL.
/// Idempotent: if `default` already has a parenthesized arg list, leave it
/// alone. The translate_default_function path strips precision for
/// cross-dialect translation; this re-attachment runs after that strip,
/// re-establishing column-type ↔ default-precision symmetry.
pub(super) fn reattach_now_family_precision(default: &str, precision: u8) -> String {
    let trimmed = default.trim_end();
    // Already has a non-empty paren-arg list (e.g. CURRENT_TIMESTAMP(6),
    // SYSDATETIMEOFFSET(3)) — leave verbatim. Empty parens like `now()`
    // are handled below by replacing them with the precision suffix.
    if trimmed.ends_with(')') && !trimmed.ends_with("()") {
        return default.to_string();
    }
    // Recognize the now-family keywords that ACCEPT a fractional-seconds
    // precision argument (per MySQL grammar). CURRENT_DATE explicitly does
    // NOT — `CURRENT_DATE(6)` is a parser error in MySQL — so it's omitted
    // here. The set is: CURRENT_TIMESTAMP, CURRENT_TIME, LOCALTIME,
    // LOCALTIMESTAMP, NOW (and the function variants handled below).
    let lower = trimmed.to_lowercase();
    let is_now_keyword = matches!(
        lower.as_str(),
        "current_timestamp" | "current_time" | "localtimestamp" | "localtime"
    );
    let is_now_func_no_args = lower.ends_with("()")
        && (lower.starts_with("now")
            || lower.starts_with("getdate")
            || lower.starts_with("getutcdate")
            || lower.starts_with("sysdatetime")
            || lower.starts_with("current_timestamp"));
    if is_now_keyword {
        return format!("{trimmed}({precision})");
    }
    if is_now_func_no_args {
        // Replace the trailing "()" with "(N)".
        let stripped = &trimmed[..trimmed.len() - 2];
        return format!("{stripped}({precision})");
    }
    default.to_string()
}

/// Strip a trailing `(N)` precision suffix from a function-call expression.
/// `current_timestamp(6)` → `current_timestamp()`; `now()` → `now()`;
/// `getdate()` → `getdate()`. Caller passes a lowercased string. Used by
/// the now-family translation so a precision-bearing source default can
/// still match the dialect-idiomatic mapping table.
pub(super) fn strip_precision_suffix(lower: &str) -> String {
    if let Some(open) = lower.rfind('(') {
        let close = lower.len();
        if close >= open + 2 && lower.ends_with(')') {
            // Inside the parens — if it's all digits (or empty), strip them.
            let inside = &lower[open + 1..close - 1];
            if inside.is_empty() || inside.chars().all(|c| c.is_ascii_digit()) {
                let mut out = lower[..open].to_string();
                out.push_str("()");
                return out;
            }
        }
    }
    lower.to_string()
}
