use anyhow::Result;

use crate::cli::{ConnectionConfig, GeneratorOptions};
use crate::introspect;
use crate::schema::IntrospectedSchema;

/// Introspect a database given a ConnectionConfig.
pub(crate) async fn introspect_with_config(
    config: ConnectionConfig,
    schemas: &[String],
    table_filter: &[String],
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

/// Strip leading comment/blank lines from a statement chunk, but preserve
/// `-- WARNING:` lines that precede the actual SQL.
fn strip_leading_comments(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }

    let lines: Vec<&str> = trimmed.lines().collect();
    let first_sql_idx = lines.iter().position(|line| {
        let t = line.trim();
        !t.is_empty() && !t.starts_with("--")
    })?;

    // Keep WARNING comments that immediately precede the SQL
    let mut kept_lines: Vec<&str> = lines[..first_sql_idx]
        .iter()
        .copied()
        .filter(|line| line.trim().starts_with("-- WARNING:"))
        .collect();
    kept_lines.extend_from_slice(&lines[first_sql_idx..]);

    let result = kept_lines.join("\n");
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// Split DDL output into individual statements using a SQL-aware splitter.
/// Handles single-quoted strings (with '' escaping) and PostgreSQL dollar-quoting
/// so that semicolons inside string literals are not treated as statement boundaries.
/// Leading header comments are stripped; `-- WARNING:` comments are preserved.
pub(crate) fn split_statements(ddl: &str) -> Vec<String> {
    let bytes = ddl.as_bytes();
    let mut statements = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut in_single_quote = false;
    let mut dollar_tag: Option<&str> = None;

    while i < bytes.len() {
        // Inside a dollar-quoted string: scan for closing tag
        if let Some(tag) = dollar_tag {
            if ddl[i..].starts_with(tag) {
                i += tag.len();
                dollar_tag = None;
            } else {
                i += 1;
            }
            continue;
        }

        match bytes[i] {
            b'\'' if !in_single_quote => {
                in_single_quote = true;
                i += 1;
            }
            b'\'' if in_single_quote => {
                // '' is an escaped single quote inside a string
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                } else {
                    in_single_quote = false;
                    i += 1;
                }
            }
            b'$' if !in_single_quote => {
                if let Some(tag) = dollar_quote_tag_at(ddl, i) {
                    dollar_tag = Some(tag);
                    i += tag.len();
                } else {
                    i += 1;
                }
            }
            b';' if !in_single_quote => {
                if let Some(stmt) = strip_leading_comments(&ddl[start..i]) {
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

    // Handle trailing content after the last semicolon
    if let Some(stmt) = strip_leading_comments(&ddl[start..]) {
        statements.push(stmt);
    }

    statements
}

/// Try to match a dollar-quote tag starting at position `start`.
/// Dollar-quote tags have the form `$tag$` where tag is empty or [a-zA-Z0-9_]+.
fn dollar_quote_tag_at<'a>(ddl: &'a str, start: usize) -> Option<&'a str> {
    let bytes = ddl.as_bytes();
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut end = start + 1;
    while let Some(&b) = bytes.get(end) {
        if b == b'$' {
            return Some(&ddl[start..=end]);
        }
        if !(b == b'_' || b.is_ascii_alphanumeric()) {
            return None;
        }
        end += 1;
    }
    None
}

/// Count the number of executable statements in DDL output.
pub(crate) fn count_statements(ddl: &str) -> usize {
    split_statements(ddl).len()
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
    fn test_split_simple_statements() {
        let ddl = "CREATE TABLE foo (id INT);\nCREATE TABLE bar (id INT);";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("CREATE TABLE foo"));
        assert!(stmts[1].contains("CREATE TABLE bar"));
    }

    #[test]
    fn test_split_strips_header_comments() {
        let ddl = "-- Generated by uvg\n-- Source: postgres\n\nCREATE TABLE foo (id INT);";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "CREATE TABLE foo (id INT)");
    }

    #[test]
    fn test_split_preserves_warning_comments() {
        let ddl = "-- WARNING: destructive operation\nALTER TABLE foo DROP COLUMN bar;";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].starts_with("-- WARNING:"));
        assert!(stmts[0].contains("ALTER TABLE"));
    }

    #[test]
    fn test_split_strips_non_warning_comments() {
        let ddl = "-- This is a header\n-- Another comment\nCREATE TABLE foo (id INT);";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "CREATE TABLE foo (id INT)");
    }

    #[test]
    fn test_split_semicolon_in_single_quotes() {
        let ddl = "COMMENT ON TABLE foo IS 'has; semicolon';";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("has; semicolon"));
    }

    #[test]
    fn test_split_escaped_single_quotes() {
        let ddl = "COMMENT ON TABLE foo IS 'it''s a test; really';";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("it''s a test; really"));
    }

    #[test]
    fn test_split_dollar_quoting() {
        let ddl = "CREATE FUNCTION foo() RETURNS void AS $$\nBEGIN\n  RAISE NOTICE 'hello;world';\nEND;\n$$ LANGUAGE plpgsql;";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("RAISE NOTICE"));
    }

    #[test]
    fn test_split_trailing_whitespace() {
        let ddl = "CREATE TABLE foo (id INT);  \n  \n";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_split_comment_only_ddl() {
        let ddl = "-- No schema changes detected.\n";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 0);
    }

    #[test]
    fn test_split_mixed_header_and_statements() {
        let ddl = "-- Generated by uvg (diff)\n-- Source: postgres, Target: postgres\n\nCREATE TABLE posts (\n    id SERIAL\n);\n\nCREATE INDEX idx ON posts (id);\n\n-- WARNING: destructive operation\nALTER TABLE users DROP COLUMN old;";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 3);
        assert!(stmts[0].starts_with("CREATE TABLE"));
        assert!(stmts[1].starts_with("CREATE INDEX"));
        assert!(stmts[2].starts_with("-- WARNING:"));
        assert!(stmts[2].contains("DROP COLUMN"));
    }

    #[test]
    fn test_count_statements() {
        let ddl = "-- header\nCREATE TABLE foo (id INT);\nCREATE TABLE bar (id INT);";
        assert_eq!(count_statements(ddl), 2);
    }

    #[test]
    fn test_count_no_changes() {
        let ddl = "-- No schema changes detected.\n";
        assert_eq!(count_statements(ddl), 0);
    }
}
