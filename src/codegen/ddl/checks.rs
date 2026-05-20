use crate::dialect::Dialect;

/// Conservative cross-dialect portability check for a CHECK predicate.
/// Returns false when the predicate contains tokens we know don't translate
/// without AST-level rewriting — emit-then-fail would abort the table apply
/// (#33 scope note: predicate semantic translation is a separate effort).
///
/// Currently flags:
///   - PG `~` / `~*` regex operators (no MSSQL/MySQL equivalent)
///   - PG `ARRAY[...]` literals (no non-PG dialect has array literal syntax)
///   - PG `ANY(ARRAY[...])` / `ALL(ARRAY[...])` quantified expressions
///
/// All flags fire for any non-PG target — none of MSSQL/MySQL/SQLite have
/// equivalents for these PG-specific constructs. (Earlier doc said "when
/// target=MSSQL" for the array case; the implementation has always been
/// broader than that.)
///
/// Heuristic by token search; misclassifies if a string literal happens to
/// contain `~` or `ARRAY[`, but that's rare in CHECK predicates.
pub(super) fn check_predicate_is_portable(expr: &str, source: Dialect, target: Dialect) -> bool {
    if source == target {
        return true;
    }
    if source == Dialect::Postgres {
        // PG regex operators don't exist on MSSQL/MySQL.
        if expr.contains('~') {
            return false;
        }
        // PG array literals — `ARRAY[1,2,3]` in CHECK is not portable.
        let lower = expr.to_lowercase();
        if lower.contains("array[") || lower.contains(" any(") || lower.contains(" all(") {
            return false;
        }
    }
    if source == Dialect::Mysql {
        let lower = expr.to_lowercase();
        // MySQL TINYINT(1) round-trips to a real BOOLEAN on PG/MSSQL.
        // The source's typical boolean-range CHECK is `<col> in (0,1)` —
        // a literal int comparison. PG rejects with "operator does not
        // exist: boolean = integer"; the check is also redundant on a
        // target where the column type IS BOOLEAN (the constraint is
        // implicit). Drop these on cross-dialect mysql→{pg,mssql,sqlite}.
        // Match common shapes (with or without spaces).
        if lower.contains(" in (0,1)")
            || lower.contains(" in (0, 1)")
            || lower.contains(" in (1,0)")
            || lower.contains(" in (1, 0)")
        {
            return false;
        }
        // regexp_like() is a MySQL function; MSSQL has no built-in regex
        // and PG uses the `~` operator instead. Drop these CHECK predicates
        // when crossing dialects rather than emit DDL that fails at apply.
        if lower.contains("regexp_like(") {
            return false;
        }
    }
    // MySQL-specific function names like `IF()` are still passed through —
    // those would fail at apply time. Adding more patterns is incremental.
    true
}

/// Translate dialect-specific syntax in a CHECK predicate when crossing
/// dialects. Each source dialect has quirks the target dialect's parser
/// rejects:
///   - MySQL: backtick-quoted identifiers `\`col\`` (target=non-mysql)
///   - MSSQL: bracket-quoted identifiers `[col]` (target=non-mssql)
///   - PostgreSQL: `::type` cast suffixes (target=non-pg)
///
/// Same-dialect emission passes through verbatim. Other quirks (regex
/// operators, function-call differences) are out of scope — the predicate
/// would need AST-level translation to handle those, which is a separate
/// effort. See #35.
pub(super) fn translate_check_predicate(expr: &str, source: Dialect, target: Dialect) -> String {
    if source == target {
        return expr.to_string();
    }
    match source {
        Dialect::Mysql => expr.replace('`', "\""),
        Dialect::Mssql => translate_mssql_check_predicate(expr),
        Dialect::Postgres => strip_pg_casts_in_predicate(expr),
        Dialect::Sqlite => expr.to_string(),
    }
}

/// Replace `[ident]` with `"ident"` in a MSSQL predicate. Doesn't try to
/// parse — just swaps the bracket characters for double-quotes. Edge case:
/// brackets inside a string literal would be miscounted, but real CHECK
/// predicates don't typically embed `[` in strings.
fn translate_mssql_check_predicate(expr: &str) -> String {
    expr.replace(['[', ']'], "\"")
}

/// Strip PG `::type` and `::type(N)` cast suffixes from a predicate. PG's
/// pg_get_constraintdef emits explicit casts like `(code)::text = upper(...)`
/// or `id::int4 > 0`. Other dialects' parsers reject `::`. We scan for
/// `::`, then consume an identifier and an optional parenthesized arg
/// list. Everything else passes through unchanged.
///
/// Implementation note: copy non-cast spans as UTF-8 string slices via
/// `push_str(&expr[last..i])` rather than byte-by-byte. The cast tokens
/// themselves are pure ASCII (`::`, identifier, parens, digits) so byte-
/// level scanning of the cast region is safe; only the surrounding text
/// might contain non-ASCII (accented string literals in CHECK predicates),
/// which the slice-copy approach preserves intact.
pub(super) fn strip_pg_casts_in_predicate(expr: &str) -> String {
    let bytes = expr.as_bytes();
    let mut out = String::with_capacity(expr.len());
    let mut i = 0;
    let mut last_copied = 0;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b':' && bytes[i + 1] == b':' {
            // Flush the run of bytes since the previous skip — slice copy
            // preserves any non-ASCII content (accented string literals,
            // etc.) intact.
            out.push_str(&expr[last_copied..i]);
            // Skip "::"
            i += 2;
            // Skip the type identifier — alphanumeric and underscore only.
            // Don't consume spaces; trailing space is part of the surrounding
            // expression (e.g. `(code)::text = ...` — the space before `=`
            // must survive). PG's multi-word type names (`timestamp without
            // time zone`) aren't typically used in CHECK predicates with
            // explicit casts, so this short-form match is sufficient.
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            // Skip optional parenthesized arg list, e.g. ::numeric(10,2)
            if i < bytes.len() && bytes[i] == b'(' {
                let mut depth = 1;
                i += 1;
                while i < bytes.len() && depth > 0 {
                    match bytes[i] {
                        b'(' => depth += 1,
                        b')' => depth -= 1,
                        _ => {}
                    }
                    i += 1;
                }
            }
            last_copied = i;
        } else {
            i += 1;
        }
    }
    // Tail flush — copy whatever's after the last cast (or the entire
    // input if no cast was found).
    out.push_str(&expr[last_copied..]);
    out
}
