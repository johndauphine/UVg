use std::collections::BTreeMap;

use sqlx::SqlitePool;

use crate::error::UvgError;
use crate::schema::{ConstraintInfo, ConstraintType, ForeignKeyInfo};

pub async fn query_constraints(
    pool: &SqlitePool,
    table_name: &str,
    create_sql: &str,
) -> Result<Vec<ConstraintInfo>, UvgError> {
    let mut constraints: Vec<ConstraintInfo> = Vec::new();

    // Primary key — from pragma_table_info
    let pk_cols = query_primary_key(pool, table_name).await?;
    if !pk_cols.is_empty() {
        constraints.push(ConstraintInfo {
            name: format!("pk_{table_name}"),
            constraint_type: ConstraintType::PrimaryKey,
            columns: pk_cols,
            foreign_key: None,
            check_expression: None,
        });
    }

    // Foreign keys — from pragma_foreign_key_list
    let fk_constraints = query_foreign_keys(pool, table_name).await?;
    constraints.extend(fk_constraints);

    // Unique constraints — from pragma_index_list where origin = 'u'
    let uq_constraints = query_unique_constraints(pool, table_name).await?;
    constraints.extend(uq_constraints);

    // Check constraints — parsed from CREATE TABLE SQL
    let check_constraints = parse_check_constraints(create_sql);
    constraints.extend(check_constraints);

    Ok(constraints)
}

async fn query_primary_key(
    pool: &SqlitePool,
    table_name: &str,
) -> Result<Vec<String>, UvgError> {
    let rows = sqlx::query_as::<_, PkRow>(
        "SELECT name, pk FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk",
    )
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|r| r.name).collect())
}

async fn query_foreign_keys(
    pool: &SqlitePool,
    table_name: &str,
) -> Result<Vec<ConstraintInfo>, UvgError> {
    let rows = sqlx::query_as::<_, FkRow>(
        r#"
        SELECT id, seq, "table", "from", "to", on_update, on_delete
        FROM pragma_foreign_key_list(?)
        ORDER BY id, seq
        "#,
    )
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    let mut fk_map: BTreeMap<i32, FkAccumulator> = BTreeMap::new();
    for row in rows {
        let acc = fk_map.entry(row.id).or_insert_with(|| FkAccumulator {
            columns: Vec::new(),
            ref_table: row.table.clone(),
            ref_columns: Vec::new(),
            update_rule: normalize_fk_rule(&row.on_update),
            delete_rule: normalize_fk_rule(&row.on_delete),
        });
        acc.columns.push(row.from);
        acc.ref_columns.push(row.to);
    }

    let constraints = fk_map
        .into_iter()
        .map(|(id, acc)| {
            let name = format!("fk_{table_name}_{id}");
            ConstraintInfo {
                name,
                constraint_type: ConstraintType::ForeignKey,
                columns: acc.columns,
                foreign_key: Some(ForeignKeyInfo {
                    ref_schema: "main".to_string(),
                    ref_table: acc.ref_table,
                    ref_columns: acc.ref_columns,
                    update_rule: acc.update_rule,
                    delete_rule: acc.delete_rule,
                }),
                check_expression: None,
            }
        })
        .collect();

    Ok(constraints)
}

fn normalize_fk_rule(rule: &str) -> String {
    match rule.to_uppercase().as_str() {
        "CASCADE" => "CASCADE".to_string(),
        "SET NULL" => "SET NULL".to_string(),
        "SET DEFAULT" => "SET DEFAULT".to_string(),
        "RESTRICT" => "RESTRICT".to_string(),
        _ => "NO ACTION".to_string(),
    }
}

async fn query_unique_constraints(
    pool: &SqlitePool,
    table_name: &str,
) -> Result<Vec<ConstraintInfo>, UvgError> {
    // Get indexes that were created from UNIQUE constraints (origin = 'u')
    let index_rows = sqlx::query_as::<_, IndexListRow>(
        r#"SELECT name, "unique", origin FROM pragma_index_list(?) WHERE origin = 'u'"#,
    )
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    let mut constraints = Vec::new();
    for idx in index_rows {
        let col_rows = sqlx::query_as::<_, IndexInfoRow>(
            "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
        )
        .bind(&idx.name)
        .fetch_all(pool)
        .await?;

        let columns: Vec<String> = col_rows.into_iter().map(|r| r.name).collect();
        constraints.push(ConstraintInfo {
            name: idx.name,
            constraint_type: ConstraintType::Unique,
            columns,
            foreign_key: None,
            check_expression: None,
        });
    }

    Ok(constraints)
}

/// Parse CHECK constraints from CREATE TABLE SQL.
/// Handles both table-level (`CHECK(...)`) and column-level
/// (`col TYPE CHECK(...)`) constraints.
fn parse_check_constraints(create_sql: &str) -> Vec<ConstraintInfo> {
    if create_sql.is_empty() {
        return vec![];
    }

    // Find the body between outer parentheses
    let body = match (create_sql.find('('), create_sql.rfind(')')) {
        (Some(start), Some(end)) if start < end => &create_sql[start + 1..end],
        _ => return vec![],
    };

    let fragments = split_respecting_parens(body);
    let mut checks = Vec::new();
    let mut idx = 0;

    for fragment in fragments {
        let trimmed = fragment.trim();

        // Case-insensitive search for "CHECK" on the original string using
        // byte-level ASCII comparison to avoid UTF-8 index mismatch.
        if let Some(check_pos) = find_keyword_ascii(trimmed, "CHECK") {
            let after_check = trimmed[check_pos + 5..].trim_start();
            if after_check.starts_with('(') {
                if let Some(expr) = extract_check_expression(after_check) {
                    let name = format!("ck_{idx}");
                    checks.push(ConstraintInfo {
                        name,
                        constraint_type: ConstraintType::Check,
                        columns: vec![],
                        foreign_key: None,
                        check_expression: Some(expr),
                    });
                    idx += 1;
                }
            }
        }
    }

    checks
}

/// Case-insensitive ASCII search for a keyword in a string.
/// Returns the byte offset in the original string, safe for slicing
/// as long as the keyword is pure ASCII.
fn find_keyword_ascii(haystack: &str, needle: &str) -> Option<usize> {
    let needle_bytes = needle.as_bytes();
    haystack
        .as_bytes()
        .windows(needle_bytes.len())
        .position(|window| {
            window
                .iter()
                .zip(needle_bytes.iter())
                .all(|(h, n)| h.to_ascii_uppercase() == n.to_ascii_uppercase())
        })
}

/// Extract the expression from "(...)" respecting nested parentheses.
fn extract_check_expression(s: &str) -> Option<String> {
    if !s.starts_with('(') {
        return None;
    }
    let mut depth = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(s[1..i].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

/// Split a string by commas, respecting nested parentheses and quoted strings.
fn split_respecting_parens(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth: i32 = 0;
    let mut in_quote = false;
    let mut start = 0;

    for (i, ch) in s.char_indices() {
        if in_quote {
            if ch == '\'' {
                in_quote = false;
            }
            continue;
        }
        match ch {
            '\'' => in_quote = true,
            '(' => depth += 1,
            ')' => depth = (depth - 1).max(0),
            ',' if depth == 0 => {
                result.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    result.push(&s[start..]);
    result
}

struct FkAccumulator {
    columns: Vec<String>,
    ref_table: String,
    ref_columns: Vec<String>,
    update_rule: String,
    delete_rule: String,
}

#[derive(sqlx::FromRow)]
struct PkRow {
    name: String,
    #[allow(dead_code)]
    pk: i32,
}

#[derive(sqlx::FromRow)]
struct FkRow {
    id: i32,
    #[allow(dead_code)]
    seq: i32,
    table: String,
    from: String,
    to: String,
    on_update: String,
    on_delete: String,
}

#[derive(sqlx::FromRow)]
struct IndexListRow {
    name: String,
    #[allow(dead_code)]
    unique: bool,
    #[allow(dead_code)]
    origin: String,
}

#[derive(sqlx::FromRow)]
struct IndexInfoRow {
    name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_check_constraints() {
        let sql = "CREATE TABLE t (id INTEGER, status TEXT, CHECK(status IN ('a', 'b')))";
        let checks = parse_check_constraints(sql);
        assert_eq!(checks.len(), 1);
        assert_eq!(
            checks[0].check_expression.as_deref(),
            Some("status IN ('a', 'b')")
        );
    }

    #[test]
    fn test_parse_check_no_checks() {
        let sql = "CREATE TABLE t (id INTEGER, name TEXT)";
        let checks = parse_check_constraints(sql);
        assert!(checks.is_empty());
    }

    #[test]
    fn test_parse_check_column_level() {
        let sql =
            "CREATE TABLE t (id INTEGER, status TEXT CHECK(status IN ('active', 'inactive')))";
        let checks = parse_check_constraints(sql);
        assert_eq!(checks.len(), 1);
        assert_eq!(
            checks[0].check_expression.as_deref(),
            Some("status IN ('active', 'inactive')")
        );
    }

    #[test]
    fn test_parse_check_mixed_levels() {
        let sql = "CREATE TABLE t (id INTEGER, val INTEGER CHECK(val > 0), CHECK(id > 0))";
        let checks = parse_check_constraints(sql);
        assert_eq!(checks.len(), 2);
    }

    #[test]
    fn test_normalize_fk_rule() {
        assert_eq!(normalize_fk_rule("CASCADE"), "CASCADE");
        assert_eq!(normalize_fk_rule("NO ACTION"), "NO ACTION");
        assert_eq!(normalize_fk_rule("SET NULL"), "SET NULL");
        assert_eq!(normalize_fk_rule(""), "NO ACTION");
    }

    #[test]
    fn test_split_respecting_parens_with_quoted_comma() {
        // Commas inside string literals should not split
        let result = split_respecting_parens("a TEXT DEFAULT 'x,y', b INTEGER");
        assert_eq!(result.len(), 2);
        assert!(result[0].contains("'x,y'"));
        assert!(result[1].contains("b INTEGER"));
    }

    #[test]
    fn test_parse_check_with_default_containing_comma() {
        let sql =
            "CREATE TABLE t (id INTEGER, label TEXT DEFAULT 'a,b', CHECK(id > 0))";
        let checks = parse_check_constraints(sql);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].check_expression.as_deref(), Some("id > 0"));
    }
}
