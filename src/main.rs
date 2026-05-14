mod cli;
mod codegen;
mod db;
mod ddl_typemap;
mod dialect;
mod error;
mod introspect;
mod naming;
mod output;
mod schema;
#[cfg(test)]
mod testutil;
mod tui;
mod typemap;

use std::fs;

use anyhow::Result;
use clap::Parser;
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

use crate::cli::{redact_url, Cli, ConnectionConfig};
use crate::codegen::declarative::DeclarativeGenerator;
use crate::codegen::ddl_diff::compute_changes;
use crate::codegen::tables::TablesGenerator;
use crate::codegen::Generator;
use crate::output::{apply_order, write_split_changes, Manifest, OutputContext};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    if cli.interactive {
        return tui::run(cli).await;
    }

    // --apply preflight: validate configuration BEFORE we open any
    // database connections, so a misconfigured invocation doesn't
    // first stall on an unreachable source URL.
    if cli.apply {
        if cli.generator != "ddl" {
            return Err(anyhow::anyhow!(
                "--apply only works with --generator ddl (current: {})",
                cli.generator,
            ));
        }
        let Some(ref target_url) = cli.target_url else {
            return Err(anyhow::anyhow!(
                "--apply requires a target database URL to execute against"
            ));
        };
        if cli.split_tables {
            return Err(anyhow::anyhow!(
                "--apply with --split-tables is not supported (use --out-dir for per-table apply)"
            ));
        }
        // Refuse a --target-dialect that disagrees with the target URL's
        // scheme — applying mysql-flavored DDL to a postgres database
        // would fail at parse time with a cryptic engine error; better
        // to surface the mismatch up front. Building the source dialect
        // from the URL doesn't open a connection.
        let src_dialect = cli.parse_connection()?.dialect();
        let target_dialect = cli.ddl_options(src_dialect)?.target_dialect;
        let url_dialect = cli.parse_target_connection(target_url)?.dialect();
        if target_dialect != url_dialect {
            return Err(anyhow::anyhow!(
                "--apply: --target-dialect ({}) does not match the dialect inferred from the target URL ({}). \
                 Drop --target-dialect, or change the URL scheme to match.",
                target_dialect,
                url_dialect,
            ));
        }
    }

    let config = cli.parse_connection()?;
    let dialect = config.dialect();
    // MySQL default schema = database name from URL; others use static defaults.
    let schemas = if let Some(db) = config.database_name() {
        cli.schema_list_or(&db)
    } else {
        cli.schema_list_or(dialect.default_schema())
    };
    let table_filter = cli.table_list();
    let options = cli.generator_options();

    tracing::debug!("Connecting to database...");

    let schema = match config {
        ConnectionConfig::Postgres(url) => {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            tracing::debug!("Introspecting schema...");
            let s = introspect::pg::introspect(
                &pool,
                &schemas,
                &table_filter,
                cli.noviews,
                &options,
            )
            .await;
            pool.close().await;
            s?
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
            tracing::debug!("Introspecting schema...");
            introspect::mssql::introspect(
                &mut client,
                &schemas,
                &table_filter,
                cli.noviews,
                &options,
            )
            .await?
        }
        ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            tracing::debug!("Introspecting schema...");
            let s = introspect::mysql::introspect(
                &pool,
                &schemas,
                &table_filter,
                cli.noviews,
                &options,
            )
            .await;
            pool.close().await;
            s?
        }
        ConnectionConfig::Sqlite(url) => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            tracing::debug!("Introspecting schema...");
            let s = introspect::sqlite::introspect(
                &pool,
                &table_filter,
                cli.noviews,
                &options,
            )
            .await;
            pool.close().await;
            s?
        }
    };

    tracing::debug!("Found {} tables/views", schema.tables.len());

    match cli.generator.as_str() {
        "tables" => {
            if cli.split_tables {
                let files = TablesGenerator.generate_split(&schema, &options);
                write_split_output(&files, &cli.outfile)?;
            } else {
                write_output(&TablesGenerator.generate(&schema, &options), &cli.outfile)?;
            }
        }
        "declarative" => {
            if cli.split_tables {
                let files = DeclarativeGenerator.generate_split(&schema, &options);
                write_split_output(&files, &cli.outfile)?;
            } else {
                write_output(&DeclarativeGenerator.generate(&schema, &options), &cli.outfile)?;
            }
        }
        "ddl" => {
            use crate::codegen::ddl::{DdlGenerator, DdlOutput};

            let ddl_opts = cli.ddl_options(dialect)?;

            // --apply preflight (generator, target URL, --split-tables,
            // dialect-mismatch) already ran at the top of main, before
            // any database connections. By the time we get here the
            // configuration is known to be coherent.

            // If a target URL is provided, introspect it for diff
            let target_schema = if let Some(ref target_url) = cli.target_url {
                let target_config = cli.parse_target_connection(target_url)?;
                let target_dialect = target_config.dialect();
                let target_schemas = if let Some(db) = target_config.database_name() {
                    cli.schema_list_or(&db)
                } else {
                    cli.schema_list_or(target_dialect.default_schema())
                };
                Some(
                    db::introspect_with_config(
                        target_config,
                        &target_schemas,
                        &table_filter,
                        cli.noviews,
                        &options,
                    )
                    .await?,
                )
            } else {
                None
            };

            // --out-dir: per-table diff layout. Only kicks in when there's
            // a target to diff against and --outfile is not set (--outfile
            // wins per docs/migration-output-layout.md).
            if cli.outfile.is_none() {
                if let Some(ref out_dir) = cli.out_dir {
                    let Some(target) = target_schema.as_ref() else {
                        return Err(anyhow::anyhow!(
                            "--out-dir requires a target database URL to diff against"
                        ));
                    };
                    let changes = compute_changes(&schema, target, &ddl_opts);
                    let ctx = OutputContext::now(
                        out_dir.clone(),
                        cli.name.clone(),
                        dialect,
                        ddl_opts.target_dialect,
                    );
                    let run_id = ctx.run_id.clone();
                    match write_split_changes(&changes, &ctx)? {
                        None => {
                            eprintln!("uvg: no schema changes");
                        }
                        Some(manifest) => {
                            eprintln!(
                                "uvg: wrote {} file(s) under {} (manifest: _runs/{}.json)",
                                manifest.files.len(),
                                out_dir.display(),
                                run_id,
                            );
                            if cli.apply {
                                let target_url = cli.target_url.as_ref().unwrap();
                                let target_config = cli.parse_target_connection(target_url)?;
                                let applied = apply_manifest(&target_config, out_dir, &manifest).await?;
                                eprintln!(
                                    "uvg: applied {applied} statement(s) across {} table(s) to {}",
                                    manifest.files.len(),
                                    redact_url(target_url),
                                );
                            }
                        }
                    }
                    return Ok(());
                }
            }

            let gen = DdlGenerator;
            let ddl_output = gen.generate(&schema, target_schema.as_ref(), &ddl_opts);

            match ddl_output {
                DdlOutput::Single(content) => {
                    // --apply: execute against the target and suppress stdout
                    // (the user got what they asked for via the eprintln summary;
                    // dumping the DDL to stdout in addition is noise). If they
                    // also want a file artifact they can pass --outfile.
                    if cli.apply {
                        let target_url = cli.target_url.as_ref().unwrap();
                        let target_config = cli.parse_target_connection(target_url)?;
                        if cli.outfile.is_some() {
                            write_output(&content, &cli.outfile)?;
                        }
                        let applied = apply_blob(&target_config, &content).await?;
                        eprintln!(
                            "uvg: applied {applied} statement(s) to {}",
                            redact_url(target_url),
                        );
                    } else {
                        write_output(&content, &cli.outfile)?;
                    }
                }
                DdlOutput::Split(files) => {
                    // --apply + --split-tables is rejected at the top-of-main
                    // preflight, so this arm only fires under no-target /
                    // codegen-style usage.
                    match cli.outfile {
                        Some(ref dir) => {
                            let dir_path = std::path::PathBuf::from(dir);
                            fs::create_dir_all(&dir_path)?;
                            for (filename, content) in &files {
                                let path = dir_path.join(filename);
                                fs::write(&path, content)?;
                                tracing::info!("Written {}", path.display());
                            }
                        }
                        None => {
                            for (filename, content) in &files {
                                println!("-- File: {filename}");
                                print!("{content}\n");
                            }
                        }
                    }
                }
            }
        }
        other => {
            return Err(error::UvgError::UnknownGenerator(other.to_string()).into());
        }
    };

    Ok(())
}

fn write_split_output(files: &[(String, String)], outfile: &Option<String>) -> anyhow::Result<()> {
    match outfile {
        Some(ref dir) => {
            let dir_path = std::path::PathBuf::from(dir);
            fs::create_dir_all(&dir_path)?;
            for (filename, content) in files {
                let path = dir_path.join(filename);
                fs::write(&path, content)?;
                tracing::info!("Written {}", path.display());
            }
        }
        None => {
            for (filename, content) in files {
                println!("# --- {filename} ---");
                print!("{content}");
            }
        }
    }
    Ok(())
}

fn write_output(output: &str, outfile: &Option<String>) -> anyhow::Result<()> {
    match outfile {
        Some(ref path) => {
            fs::write(path, output)?;
            tracing::info!("Output written to {path}");
        }
        None => {
            print!("{output}");
        }
    }
    Ok(())
}

/// Substrings that signal a chunk of DDL needs manual schema work
/// uvg can't perform. If any of these appear in a blob queued for
/// `--apply`, the entire operation is refused before any statement
/// runs. Without this guard, mixed diffs (real `ADD COLUMN` plus a
/// comment-only warning) would partially apply and exit 0, leaving
/// the target out of sync with the source — bad for CI.
///
/// `-- DROPPED CHECK ...` is emitted by `generate_create_table` when
/// a CHECK predicate is not portable to the target dialect; without
/// the marker check here, uvg would create the table without that
/// CHECK and silently drop the constraint from the source schema.
const UNAPPLIABLE_MARKERS: &[&str] = &[
    "-- WARNING: SQLite does not support ALTER COLUMN",
    "-- NOTE: MSSQL requires dropping the named default constraint",
    "-- DROPPED CHECK ",
];

/// Validate that a DDL blob is safe to hand to `db::execute_ddl` under
/// `--apply` semantics — that is, it neither contains an unappliable
/// warning marker nor is a comment-only diff (other than the explicit
/// "No schema changes detected" sentinel). Pure check; no I/O.
fn validate_apply_blob(sql: &str, source_label: &str) -> anyhow::Result<()> {
    // First gate: any unappliable warning marker — even if real DDL
    // sits alongside it — disqualifies the entire blob. The user must
    // fall back to --outfile / --out-dir and reconcile by hand.
    if let Some(marker) = UNAPPLIABLE_MARKERS.iter().find(|m| sql.contains(*m)) {
        return Err(anyhow::anyhow!(
            "refusing to apply ({source_label}): contains an instruction uvg cannot execute on its own:\n  {marker}\n\
             Inspect the full diff with `--outfile` or `--out-dir` and apply the actionable parts \
             manually so the target doesn't end up partially migrated."
        ));
    }

    // Second gate: an all-comment blob that didn't trip the marker
    // gate (defense-in-depth against future emit patterns uvg doesn't
    // yet recognise). The "no schema changes" sentinel is the one
    // legitimate zero-statement case.
    let statements = db::split_statements(sql);
    if statements.is_empty() {
        let trimmed = sql.trim();
        let is_noop_sentinel = trimmed.is_empty()
            || trimmed.starts_with("-- No schema changes detected");
        if !is_noop_sentinel {
            return Err(anyhow::anyhow!(
                "refusing to apply ({source_label}): produced changes but they're all non-executable text. \
                 Inspect with `--outfile` or `--out-dir` and apply the actionable parts by hand."
            ));
        }
    }
    Ok(())
}

/// Apply a single DDL blob to the target. Returns the count of
/// successful statements. On any failure, returns a contextual error
/// quoting the offending statement and the database's error message —
/// the binary then exits non-zero, which is load-bearing for CI/scripted
/// callers per issue #57's "side benefits" section.
///
/// Runs `validate_apply_blob` first. Callers iterating multiple blobs
/// (notably `apply_manifest`) should also pre-validate the full set
/// before applying any so a refused later blob can't leave the target
/// partially migrated by earlier already-applied blobs.
async fn apply_blob(target_config: &ConnectionConfig, sql: &str) -> anyhow::Result<usize> {
    validate_apply_blob(sql, "blob")?;
    let results = db::execute_ddl(target_config, sql).await?;
    let applied = results.iter().filter(|r| r.error.is_none()).count();
    if let Some(failed) = results.iter().find(|r| r.error.is_some()) {
        let first_line = failed.sql.lines().next().unwrap_or("").trim();
        return Err(anyhow::anyhow!(
            "DDL apply failed after {applied} statement(s); first failure:\n  {first_line}\n  Error: {}",
            failed.error.as_ref().unwrap()
        ));
    }
    Ok(applied)
}

/// Apply every `.sql` file referenced by a manifest, in manifest order
/// (which is `_schema/` first, then table files in topological FK order
/// — see [`output::apply_order`] and `test_manifest_preserves_topological_order`).
/// Returns the total count of statements applied across all files.
///
/// **Preflights every file before applying any.** If a later file
/// would be rejected (unappliable marker, comment-only diff), the
/// preflight catches it before earlier files mutate the target.
/// Without this, the per-file apply loop would leave the target
/// partially migrated — recreating the exact bug `apply_blob`'s
/// single-blob marker guard avoids.
async fn apply_manifest(
    target_config: &ConnectionConfig,
    out_dir: &std::path::Path,
    manifest: &Manifest,
) -> anyhow::Result<usize> {
    let paths = apply_order(manifest, out_dir);

    // Phase 1: read + validate every blob. No I/O on the target.
    let mut bodies: Vec<(std::path::PathBuf, String)> = Vec::with_capacity(paths.len());
    for path in &paths {
        let sql = fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
        // Use the file path as the source label so a rejection points
        // the user at the specific file to fix.
        validate_apply_blob(&sql, &path.display().to_string())?;
        bodies.push((path.clone(), sql));
    }

    // Phase 2: every blob is structurally valid; execute in order.
    // apply_blob re-runs validate_apply_blob (cheap, no I/O) and then
    // executes. Per-statement DB errors still halt mid-stream — the
    // preflight only catches structural rejections.
    let mut total = 0;
    for (_, sql) in &bodies {
        total += apply_blob(target_config, sql).await?;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    //! Inline unit tests for the `--apply` validation logic. We test
    //! the pure validator (no I/O) here rather than via integration
    //! tests because some failure paths — cross-dialect
    //! `DROPPED CHECK` markers in particular — require a target URL
    //! whose dialect differs from the source, which the SQLite-only
    //! integration test environment can't produce in isolation.
    use super::*;

    #[test]
    fn test_validate_rejects_sqlite_alter_column_warning() {
        let sql = "ALTER TABLE \"users\" ADD COLUMN \"phone\" TEXT;\n\n\
                   -- WARNING: SQLite does not support ALTER COLUMN. Table recreation required.\n\
                   -- ALTER TABLE \"users\" ALTER COLUMN \"email\" TYPE TEXT;";
        let err = validate_apply_blob(sql, "test").unwrap_err();
        assert!(err.to_string().contains("refusing to apply"));
        assert!(err.to_string().contains("ALTER COLUMN"));
    }

    #[test]
    fn test_validate_rejects_mssql_default_constraint_note() {
        let sql = "-- NOTE: MSSQL requires dropping the named default constraint first.\n\
                   -- Run: SELECT name FROM sys.default_constraints ...\n\
                   ALTER TABLE [users] ADD DEFAULT 1 FOR [count];";
        let err = validate_apply_blob(sql, "test").unwrap_err();
        assert!(err.to_string().contains("MSSQL requires dropping"));
    }

    #[test]
    fn test_validate_rejects_dropped_check_marker() {
        // Cross-dialect CREATE TABLE emits "-- DROPPED CHECK ..." when
        // a source CHECK predicate isn't portable to the target dialect.
        // Without this entry in UNAPPLIABLE_MARKERS, --apply would
        // create the table without the CHECK and exit 0 — silently
        // losing a constraint from the source schema.
        let sql = "-- DROPPED CHECK ck_orders_positive: predicate uses non-portable syntax\n\
                   --   source: total > 0\n\
                   CREATE TABLE orders (id INT, total DECIMAL(10,2));";
        let err = validate_apply_blob(sql, "test").unwrap_err();
        assert!(err.to_string().contains("DROPPED CHECK"));
    }

    #[test]
    fn test_validate_accepts_no_changes_sentinel() {
        // The one legitimate zero-statement case.
        validate_apply_blob("-- No schema changes detected.\n", "test").unwrap();
        validate_apply_blob("", "test").unwrap();
    }

    #[test]
    fn test_validate_accepts_normal_ddl() {
        let sql = "CREATE TABLE users (\n  id INTEGER PRIMARY KEY,\n  email TEXT NOT NULL\n);";
        validate_apply_blob(sql, "test").unwrap();
    }

    #[test]
    fn test_validate_rejects_comment_only_non_sentinel() {
        // Defense-in-depth: a future emit pattern that produces only
        // comments (with no recognized marker) still gets caught by
        // the second gate.
        let sql = "-- some weird future warning we don't know about\n-- still all comments though";
        let err = validate_apply_blob(sql, "test").unwrap_err();
        assert!(err.to_string().contains("non-executable"));
    }

    #[test]
    fn test_validate_error_includes_source_label() {
        // apply_manifest passes the file path so a rejection points
        // the user at the specific file to fix.
        let sql = "-- DROPPED CHECK foo: predicate uses non-portable syntax";
        let err = validate_apply_blob(sql, "/tmp/migrations/orders/2026...sql").unwrap_err();
        assert!(
            err.to_string().contains("/tmp/migrations/orders/2026...sql"),
            "error must name the source: {err}"
        );
    }
}
