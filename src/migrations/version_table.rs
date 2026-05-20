use std::io::{self, Write};

use anyhow::Result;

use crate::cli::ConnectionConfig;

use super::model::MigrationFile;

const VERSION_TABLE: &str = "uvg_version";

pub(super) async fn stamp_revision(
    config: &ConnectionConfig,
    migration: &MigrationFile,
) -> Result<()> {
    ensure_version_table(config).await?;
    record_revision(config, &migration.revision, &migration.description).await
}

pub(super) fn confirm_stamp(target_url: &str, revision: &str) -> Result<bool> {
    eprintln!(
        "About to stamp {} at revision {} without running any migration SQL.",
        redact_url(target_url),
        revision
    );
    eprintln!("The schema must already match this revision.");
    eprint!("Continue? [y/N] ");
    io::stderr().flush()?;

    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    Ok(matches!(answer.trim(), "y" | "Y" | "yes" | "YES" | "Yes"))
}

pub(super) fn redact_url(raw: &str) -> String {
    crate::redaction::redact_connection_url(raw)
}

pub(super) async fn ensure_version_table(config: &ConnectionConfig) -> Result<()> {
    match config {
        ConnectionConfig::Postgres(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS uvg_version (
                    revision VARCHAR(64) NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL,
                    description TEXT
                )",
            )
            .execute(&pool)
            .await?;
            pool.close().await;
        }
        ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS uvg_version (
                    revision VARCHAR(64) NOT NULL,
                    applied_at TIMESTAMP NOT NULL,
                    description TEXT
                )",
            )
            .execute(&pool)
            .await?;
            pool.close().await;
        }
        ConnectionConfig::Sqlite(url) => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS uvg_version (
                    revision TEXT NOT NULL,
                    applied_at TEXT NOT NULL,
                    description TEXT
                )",
            )
            .execute(&pool)
            .await?;
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
            let mut client = crate::introspect::mssql::connect(
                host,
                *port,
                database,
                user,
                password,
                *trust_cert,
            )
            .await?;
            client
                .execute(
                    "IF OBJECT_ID(N'uvg_version', N'U') IS NULL
                     BEGIN
                         CREATE TABLE uvg_version (
                             revision NVARCHAR(64) NOT NULL,
                             applied_at DATETIMEOFFSET NOT NULL,
                             description NVARCHAR(MAX) NULL
                         )
                     END",
                    &[],
                )
                .await?;
        }
    }
    Ok(())
}

pub(super) async fn current_revision(config: &ConnectionConfig) -> Result<Option<String>> {
    if !version_table_exists(config).await? {
        return Ok(None);
    }
    match config {
        ConnectionConfig::Postgres(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            let row: Option<(String,)> =
                sqlx::query_as("SELECT revision FROM uvg_version ORDER BY applied_at DESC LIMIT 1")
                    .fetch_optional(&pool)
                    .await?;
            pool.close().await;
            Ok(row.map(|r| r.0))
        }
        ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            let row: Option<(String,)> =
                sqlx::query_as("SELECT revision FROM uvg_version ORDER BY applied_at DESC LIMIT 1")
                    .fetch_optional(&pool)
                    .await?;
            pool.close().await;
            Ok(row.map(|r| r.0))
        }
        ConnectionConfig::Sqlite(url) => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            let row: Option<(String,)> =
                sqlx::query_as("SELECT revision FROM uvg_version ORDER BY applied_at DESC LIMIT 1")
                    .fetch_optional(&pool)
                    .await?;
            pool.close().await;
            Ok(row.map(|r| r.0))
        }
        ConnectionConfig::Mssql {
            host,
            port,
            database,
            user,
            password,
            trust_cert,
        } => {
            let mut client = crate::introspect::mssql::connect(
                host,
                *port,
                database,
                user,
                password,
                *trust_cert,
            )
            .await?;
            let rows = client
                .query(
                    "SELECT TOP 1 revision FROM uvg_version ORDER BY applied_at DESC",
                    &[],
                )
                .await?
                .into_first_result()
                .await?;
            Ok(rows
                .first()
                .and_then(|row| row.get::<&str, _>("revision"))
                .map(ToString::to_string))
        }
    }
}

pub(super) async fn version_table_exists(config: &ConnectionConfig) -> Result<bool> {
    match config {
        ConnectionConfig::Postgres(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            let exists: bool = sqlx::query_scalar("SELECT to_regclass('uvg_version') IS NOT NULL")
                .fetch_one(&pool)
                .await?;
            pool.close().await;
            Ok(exists)
        }
        ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)
                 FROM information_schema.tables
                 WHERE table_schema = DATABASE() AND table_name = 'uvg_version'",
            )
            .fetch_one(&pool)
            .await?;
            pool.close().await;
            Ok(count > 0)
        }
        ConnectionConfig::Sqlite(url) => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'uvg_version'",
            )
            .fetch_one(&pool)
            .await?;
            pool.close().await;
            Ok(count > 0)
        }
        ConnectionConfig::Mssql {
            host,
            port,
            database,
            user,
            password,
            trust_cert,
        } => {
            let mut client = crate::introspect::mssql::connect(
                host,
                *port,
                database,
                user,
                password,
                *trust_cert,
            )
            .await?;
            let rows = client
                .query(
                    "SELECT CASE WHEN OBJECT_ID(N'uvg_version', N'U') IS NULL THEN 0 ELSE 1 END AS exists",
                    &[],
                )
                .await?
                .into_first_result()
                .await?;
            let exists = rows
                .first()
                .and_then(|row| row.get::<i32, _>("exists"))
                .unwrap_or(0);
            Ok(exists == 1)
        }
    }
}

pub(super) async fn record_revision(
    config: &ConnectionConfig,
    revision: &str,
    description: &str,
) -> Result<()> {
    match config {
        ConnectionConfig::Postgres(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            sqlx::query(&format!("DELETE FROM {VERSION_TABLE}"))
                .execute(&pool)
                .await?;
            sqlx::query(&format!(
                "INSERT INTO {VERSION_TABLE} (revision, applied_at, description)
                 VALUES ($1, CURRENT_TIMESTAMP, $2)"
            ))
            .bind(revision)
            .bind(description)
            .execute(&pool)
            .await?;
            pool.close().await;
        }
        ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            sqlx::query(&format!("DELETE FROM {VERSION_TABLE}"))
                .execute(&pool)
                .await?;
            sqlx::query(&format!(
                "INSERT INTO {VERSION_TABLE} (revision, applied_at, description)
                 VALUES (?, CURRENT_TIMESTAMP, ?)"
            ))
            .bind(revision)
            .bind(description)
            .execute(&pool)
            .await?;
            pool.close().await;
        }
        ConnectionConfig::Sqlite(url) => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            sqlx::query(&format!("DELETE FROM {VERSION_TABLE}"))
                .execute(&pool)
                .await?;
            sqlx::query(&format!(
                "INSERT INTO {VERSION_TABLE} (revision, applied_at, description)
                 VALUES (?1, CURRENT_TIMESTAMP, ?2)"
            ))
            .bind(revision)
            .bind(description)
            .execute(&pool)
            .await?;
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
            let mut client = crate::introspect::mssql::connect(
                host,
                *port,
                database,
                user,
                password,
                *trust_cert,
            )
            .await?;
            client
                .execute(&format!("DELETE FROM {VERSION_TABLE}"), &[])
                .await?;
            client
                .execute(
                    &format!(
                        "INSERT INTO {VERSION_TABLE} (revision, applied_at, description)
                         VALUES (@P1, SYSUTCDATETIME(), @P2)"
                    ),
                    &[&revision, &description],
                )
                .await?;
        }
    }
    Ok(())
}

pub(super) async fn clear_revision(config: &ConnectionConfig) -> Result<()> {
    match config {
        ConnectionConfig::Postgres(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            sqlx::query(&format!("DELETE FROM {VERSION_TABLE}"))
                .execute(&pool)
                .await?;
            pool.close().await;
        }
        ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            sqlx::query(&format!("DELETE FROM {VERSION_TABLE}"))
                .execute(&pool)
                .await?;
            pool.close().await;
        }
        ConnectionConfig::Sqlite(url) => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            sqlx::query(&format!("DELETE FROM {VERSION_TABLE}"))
                .execute(&pool)
                .await?;
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
            let mut client = crate::introspect::mssql::connect(
                host,
                *port,
                database,
                user,
                password,
                *trust_cert,
            )
            .await?;
            client
                .execute(&format!("DELETE FROM {VERSION_TABLE}"), &[])
                .await?;
        }
    }
    Ok(())
}
