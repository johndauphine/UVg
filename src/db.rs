use anyhow::Result;

use crate::cli::{ConnectionConfig, GeneratorOptions};
use crate::introspect;
use crate::schema::IntrospectedSchema;
use crate::table_filter::TableFilter;

/// Introspect a database given a ConnectionConfig.
pub(crate) async fn introspect_with_config(
    config: ConnectionConfig,
    schemas: &[String],
    table_filter: &TableFilter,
    noviews: bool,
    options: &GeneratorOptions,
) -> Result<IntrospectedSchema> {
    match config {
        ConnectionConfig::Postgres(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            let s = introspect::pg::introspect(&pool, schemas, table_filter, noviews, options).await;
            pool.close().await;
            Ok(s?)
        }
        ConnectionConfig::Mssql {
            host,
            port,
            database,
            user,
            password,
            trust_cert,
        } => {
            let mut client =
                introspect::mssql::connect(&host, port, &database, &user, &password, trust_cert)
                    .await?;
            Ok(
                introspect::mssql::introspect(
                    &mut client,
                    schemas,
                    table_filter,
                    noviews,
                    options,
                )
                .await?,
            )
        }
        ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            let s =
                introspect::mysql::introspect(&pool, schemas, table_filter, noviews, options).await;
            pool.close().await;
            Ok(s?)
        }
        ConnectionConfig::Sqlite(url) => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            let s = introspect::sqlite::introspect(&pool, table_filter, noviews, options).await;
            pool.close().await;
            Ok(s?)
        }
    }
}

/// Result of executing a single DDL statement.
pub(crate) struct StmtResult {
    pub sql: String,
    pub error: Option<String>,
}

/// Split DDL output into individual statements using a SQL-aware splitter.
/// Handles semicolons inside single-quoted strings (with `''` escape),
/// dollar-quoted strings (PostgreSQL `$$...$$` / `$tag$...$tag$`), and
/// line comments (`--`). Strips leading comment-only/blank lines from each
/// statement chunk so header comments don't become empty executions.
pub(crate) fn split_statements(ddl: &str) -> Vec<String> {
    let bytes = ddl.as_bytes();
    let mut statements = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut in_single_quote = false;
    let mut in_line_comment = false;
    let mut dollar_tag: Option<String> = None;

    while i < bytes.len() {
        // Inside a dollar-quoted string: scan for closing tag
        if let Some(ref tag) = dollar_tag {
            if ddl[i..].starts_with(tag.as_str()) {
                i += tag.len();
                dollar_tag = None;
            } else {
                i += 1;
            }
            continue;
        }

        // Inside a line comment: skip until newline
        if in_line_comment {
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }

        // Inside a single-quoted string
        if in_single_quote {
            if bytes[i] == b'\'' {
                // Check for escaped quote ('')
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                } else {
                    in_single_quote = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }

        match bytes[i] {
            b'\'' => {
                in_single_quote = true;
                i += 1;
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                in_line_comment = true;
                i += 2;
            }
            b'$' => {
                if let Some(tag) = dollar_quote_tag_at(ddl, i) {
                    i += tag.len();
                    dollar_tag = Some(tag);
                } else {
                    i += 1;
                }
            }
            b';' => {
                let chunk = &ddl[start..i];
                if let Some(stmt) = strip_leading_comments(chunk) {
                    statements.push(stmt);
                }
                i += 1;
                start = i;
            }
            _ => {
                i += 1;
            }
        }
    }

    // Trailing content after last semicolon
    if start < ddl.len() {
        if let Some(stmt) = strip_leading_comments(&ddl[start..]) {
            statements.push(stmt);
        }
    }

    statements
}

/// Try to match a dollar-quote tag at position `start` (e.g. `$$` or `$foo$`).
fn dollar_quote_tag_at(s: &str, start: usize) -> Option<String> {
    let bytes = s.as_bytes();
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut end = start + 1;
    while let Some(&b) = bytes.get(end) {
        if b == b'$' {
            return Some(s[start..=end].to_string());
        }
        if !(b == b'_' || b.is_ascii_alphanumeric()) {
            return None;
        }
        end += 1;
    }
    None
}

/// Strip leading blank/comment-only lines from a statement chunk.
fn strip_leading_comments(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    let stripped: String = trimmed
        .lines()
        .skip_while(|line| {
            let t = line.trim();
            t.is_empty() || t.starts_with("--")
        })
        .collect::<Vec<_>>()
        .join("\n");
    if stripped.is_empty() {
        None
    } else {
        Some(stripped)
    }
}

/// Execute DDL statements one-by-one against the target database.
/// Stops on first error.
pub(crate) async fn execute_ddl(config: &ConnectionConfig, ddl: &str) -> Result<Vec<StmtResult>> {
    let statements = split_statements(ddl);
    let mut results = Vec::new();

    match config {
        ConnectionConfig::Postgres(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            for stmt in &statements {
                let r = sqlx::query(stmt).execute(&pool).await;
                let error = r.err().map(|e| e.to_string());
                let failed = error.is_some();
                results.push(StmtResult {
                    sql: stmt.to_string(),
                    error,
                });
                if failed {
                    break;
                }
            }
            pool.close().await;
        }
        ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            for stmt in &statements {
                let r = sqlx::query(stmt).execute(&pool).await;
                let error = r.err().map(|e| e.to_string());
                let failed = error.is_some();
                results.push(StmtResult {
                    sql: stmt.to_string(),
                    error,
                });
                if failed {
                    break;
                }
            }
            pool.close().await;
        }
        ConnectionConfig::Sqlite(url) => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            for stmt in &statements {
                let r = sqlx::query(stmt).execute(&pool).await;
                let error = r.err().map(|e| e.to_string());
                let failed = error.is_some();
                results.push(StmtResult {
                    sql: stmt.to_string(),
                    error,
                });
                if failed {
                    break;
                }
            }
            pool.close().await;
        }
        ConnectionConfig::Mssql {
            host,
            port,
            database,
            user,
            password,
            trust_cert,
        } => {
            let mut client =
                introspect::mssql::connect(host, *port, database, user, password, *trust_cert)
                    .await?;
            for stmt in &statements {
                let r = client.execute(stmt.to_string(), &[]).await;
                let error = r.err().map(|e| e.to_string());
                let failed = error.is_some();
                results.push(StmtResult {
                    sql: stmt.to_string(),
                    error,
                });
                if failed {
                    break;
                }
            }
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_split() {
        let ddl = "CREATE TABLE a (id INT); CREATE TABLE b (id INT);";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "CREATE TABLE a (id INT)");
        assert_eq!(stmts[1], "CREATE TABLE b (id INT)");
    }

    #[test]
    fn test_semicolon_in_single_quotes() {
        let ddl = "COMMENT ON TABLE foo IS 'has; semicolons; inside';\nCREATE TABLE bar (id INT);";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "COMMENT ON TABLE foo IS 'has; semicolons; inside'");
        assert_eq!(stmts[1], "CREATE TABLE bar (id INT)");
    }

    #[test]
    fn test_escaped_single_quotes() {
        let ddl = "COMMENT ON TABLE foo IS 'it''s a test; with quotes';\nSELECT 1;";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "COMMENT ON TABLE foo IS 'it''s a test; with quotes'");
        assert_eq!(stmts[1], "SELECT 1");
    }

    #[test]
    fn test_dollar_quoting() {
        let ddl = "CREATE FUNCTION f() RETURNS void AS $$ BEGIN; END; $$ LANGUAGE plpgsql;\nSELECT 1;";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("BEGIN; END;"));
        assert_eq!(stmts[1], "SELECT 1");
    }

    #[test]
    fn test_named_dollar_quoting() {
        let ddl = "CREATE FUNCTION f() AS $body$ x; y; $body$ LANGUAGE sql;\nSELECT 2;";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("x; y;"));
    }

    #[test]
    fn test_line_comments_skipped() {
        let ddl = "-- header comment\n-- another\nCREATE TABLE a (id INT);";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "CREATE TABLE a (id INT)");
    }

    #[test]
    fn test_semicolon_in_line_comment() {
        let ddl = "-- this; has; semicolons\nCREATE TABLE a (id INT);";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "CREATE TABLE a (id INT)");
    }

    #[test]
    fn test_comment_only_blocks_stripped() {
        let ddl = "-- just a comment;\n-- nothing here;\n";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 0);
    }

    #[test]
    fn test_trailing_content_without_semicolon() {
        let ddl = "CREATE TABLE a (id INT)";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "CREATE TABLE a (id INT)");
    }

    #[test]
    fn test_empty_input() {
        assert_eq!(split_statements("").len(), 0);
        assert_eq!(split_statements("  \n  ").len(), 0);
        assert_eq!(split_statements(";;;").len(), 0);
    }
}
