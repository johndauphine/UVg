use sqlx::SqlitePool;

use crate::error::UvgError;
use crate::schema::ColumnInfo;

pub async fn query_columns(
    pool: &SqlitePool,
    table_name: &str,
    create_sql: &str,
) -> Result<Vec<ColumnInfo>, UvgError> {
    let rows = sqlx::query_as::<_, ColumnRow>(
        "SELECT cid, name, type, \"notnull\", dflt_value, pk FROM pragma_table_info(?)",
    )
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    let columns = rows
        .into_iter()
        .map(|row| {
            let (base_type, max_len, precision, scale) = parse_declared_type(&row.type_name);
            let has_autoincrement = row.pk > 0 && detect_autoincrement(create_sql, &row.name);

            ColumnInfo {
                name: row.name,
                ordinal_position: row.cid + 1, // SQLite cid is 0-based, convert to 1-based
                is_nullable: row.notnull == 0,
                data_type: row.type_name.clone(),
                udt_name: base_type,
                character_maximum_length: max_len,
                numeric_precision: precision,
                numeric_scale: scale,
                column_default: row.dflt_value,
                is_identity: has_autoincrement,
                identity_generation: None,
                identity: None,
                comment: None, // SQLite has no column comments
                collation: None,
                autoincrement: if has_autoincrement { Some(true) } else { None },
            }
        })
        .collect();

    Ok(columns)
}

/// Parse a SQLite declared type string into (base_type, max_length, precision, scale).
/// e.g. "VARCHAR(255)" -> ("varchar", Some(255), None, None)
///      "DECIMAL(10,2)" -> ("decimal", None, Some(10), Some(2))
///      "INTEGER"       -> ("integer", None, None, None)
///      ""              -> ("", None, None, None)
fn parse_declared_type(declared: &str) -> (String, Option<i32>, Option<i32>, Option<i32>) {
    let declared = declared.trim();
    if declared.is_empty() {
        return (String::new(), None, None, None);
    }

    let (base, params) = if let Some(paren_start) = declared.find('(') {
        let base = declared[..paren_start].trim().to_lowercase();
        let paren_end = declared.rfind(')').unwrap_or(declared.len());
        let params_str = &declared[paren_start + 1..paren_end];
        (base, Some(params_str.to_string()))
    } else {
        (declared.to_lowercase(), None)
    };

    match params {
        None => (base, None, None, None),
        Some(p) => {
            let parts: Vec<&str> = p.split(',').map(|s| s.trim()).collect();
            if parts.len() == 2 {
                // DECIMAL(p, s) or similar
                let precision = parts[0].parse::<i32>().ok();
                let scale = parts[1].parse::<i32>().ok();
                (base, None, precision, scale)
            } else if parts.len() == 1 {
                // VARCHAR(255) or NUMERIC(10)
                let val = parts[0].parse::<i32>().ok();
                // If it's a string-like type, it's max_length; otherwise precision
                if is_string_type(&base) {
                    (base, val, None, None)
                } else {
                    (base, None, val, None)
                }
            } else {
                (base, None, None, None)
            }
        }
    }
}

fn is_string_type(base: &str) -> bool {
    matches!(
        base,
        "varchar" | "char" | "character" | "nchar" | "nvarchar" | "character varying"
    )
}

/// Detect AUTOINCREMENT keyword in the CREATE TABLE SQL for a specific column.
fn detect_autoincrement(create_sql: &str, column_name: &str) -> bool {
    if create_sql.is_empty() {
        return false;
    }

    let upper = create_sql.to_uppercase();
    if !upper.contains("AUTOINCREMENT") {
        return false;
    }

    // Split the CREATE TABLE body into column definitions
    // Find the content between the outer parentheses
    let body = match (create_sql.find('('), create_sql.rfind(')')) {
        (Some(start), Some(end)) if start < end => &create_sql[start + 1..end],
        _ => return false,
    };

    // Split by commas, respecting nested parentheses
    let fragments = split_respecting_parens(body);
    let col_upper = column_name.to_uppercase();

    for fragment in fragments {
        let trimmed = fragment.trim();
        let upper_frag = trimmed.to_uppercase();

        // Check if this fragment is the column definition for our column
        // Column name is the first token (possibly quoted)
        let first_token = extract_first_token(trimmed);
        if first_token.to_uppercase() == col_upper
            || first_token.to_uppercase() == format!("\"{}\"", col_upper)
        {
            return upper_frag.contains("AUTOINCREMENT");
        }
    }

    false
}

/// Split a string by commas but respect nested parentheses.
fn split_respecting_parens(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0;
    let mut start = 0;

    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
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

/// Extract the first token from a column definition, handling quoted identifiers.
fn extract_first_token(s: &str) -> &str {
    let s = s.trim();
    if let Some(stripped) = s.strip_prefix('"') {
        // Quoted identifier
        if let Some(end) = stripped.find('"') {
            return &s[..end + 2];
        }
    }
    // Unquoted: first word
    s.split_whitespace().next().unwrap_or(s)
}

#[derive(sqlx::FromRow)]
struct ColumnRow {
    cid: i32,
    name: String,
    #[sqlx(rename = "type")]
    type_name: String,
    notnull: i32,
    dflt_value: Option<String>,
    pk: i32,
}

#[cfg(test)]
#[path = "columns_tests.rs"]
mod tests;
