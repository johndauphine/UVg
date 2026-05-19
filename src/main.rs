mod apply_progress;
mod cli;
mod codegen;
mod db;
mod ddl_typemap;
mod dialect;
mod error;
mod init;
mod introspect;
mod migrations;
mod naming;
mod output;
mod profile;
mod risk_classify;
mod schema;
mod snapshot;
mod table_filter;
#[cfg(test)]
mod testutil;
mod tui;
mod typemap;

use std::fs;
use std::path::Path;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

use crate::cli::{Cli, Command, ConnectionConfig, GeneratorOptions, SnapshotCommand};
use crate::codegen::ddl_diff::{compute_changes, render_changes};
use crate::codegen::declarative::DeclarativeGenerator;
use crate::codegen::tables::TablesGenerator;
use crate::codegen::Generator;
use crate::output::{apply_order, write_split_changes, Manifest, OutputContext};
use crate::schema::{IntrospectedSchema, TableType};
use crate::table_filter::TableFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse_with_profile()?;

    if let Some(command) = cli.command.as_ref() {
        return match command {
            Command::Snapshot(args) => run_snapshot(&cli, args).await,
            _ => migrations::run(&cli, command).await,
        };
    }

    if cli.interactive {
        return tui::run(cli).await;
    }

    let table_filter = cli.table_filter()?;
    let options = cli.generator_options();
    let source_input = cli
        .url
        .as_deref()
        .ok_or_else(|| error::UvgError::Connection("database URL is required".to_string()))?;

    tracing::debug!("Connecting to database...");

    let schema =
        load_schema_input(&cli, source_input, &table_filter, cli.noviews, &options).await?;
    let dialect = schema.dialect;

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
                write_output(
                    &DeclarativeGenerator.generate(&schema, &options),
                    &cli.outfile,
                )?;
            }
        }
        "ddl" => {
            use crate::codegen::ddl::{DdlGenerator, DdlOutput};

            // --apply needs a target to execute against. Fail fast before we
            // do any work the user would have to throw away.
            if cli.apply && cli.target_url.is_none() {
                return Err(anyhow::anyhow!("--apply requires a target database URL"));
            }
            if cli.apply && cli.target_url.as_deref().is_some_and(is_snapshot_input) {
                return Err(anyhow::anyhow!(
                    "--apply requires a live target database URL, not a snapshot"
                ));
            }

            // If a target URL or snapshot is provided, load it for diff.
            let target_schema = if let Some(ref target_url) = cli.target_url {
                Some(
                    load_schema_input(&cli, target_url, &table_filter, cli.noviews, &options)
                        .await?,
                )
            } else {
                None
            };
            let ddl_opts = if let Some(target) = target_schema.as_ref() {
                cli.ddl_options_with_target_dialect(dialect, Some(target.dialect))?
            } else {
                cli.ddl_options(dialect)?
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
                    let changes =
                        classify_or_warn(&cli, compute_changes(&schema, target, &ddl_opts)).await?;
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
                            if ddl_opts.apply {
                                // target_url is guaranteed Some here: --out-dir
                                // already errored above without one, and the
                                // early --apply check enforces it too.
                                let target_url = cli.target_url.as_deref().unwrap();
                                let target_config = cli.parse_target_connection(target_url)?;
                                apply_manifest(
                                    &target_config,
                                    &manifest,
                                    out_dir,
                                    target_url,
                                    cli.progress.resolved(),
                                    cli.apply_retries,
                                    !cli.no_parse_check,
                                )
                                .await?;
                            }
                        }
                    }
                    return Ok(());
                }
            }

            if cli.risk_classify {
                let Some(target) = target_schema.as_ref() else {
                    return Err(anyhow::anyhow!(
                        "--risk-classify requires a target database URL or @snapshot to diff against"
                    ));
                };
                let changes =
                    classify_or_warn(&cli, compute_changes(&schema, target, &ddl_opts)).await?;
                let content = render_changes(&changes, dialect, ddl_opts.target_dialect);
                write_output(&content, &cli.outfile)?;
                if ddl_opts.apply {
                    let target_url = cli.target_url.as_deref().unwrap();
                    let target_config = cli.parse_target_connection(target_url)?;
                    apply_inline(
                        &target_config,
                        &content,
                        target_url,
                        cli.progress.resolved(),
                        cli.apply_retries,
                        !cli.no_parse_check,
                    )
                    .await?;
                }
                return Ok(());
            }

            let gen = DdlGenerator;
            let ddl_output = gen.generate(&schema, target_schema.as_ref(), &ddl_opts);

            match ddl_output {
                DdlOutput::Single(content) => {
                    write_output(&content, &cli.outfile)?;
                    if ddl_opts.apply {
                        // target_url is Some: enforced by the early --apply
                        // guard at the top of this arm.
                        let target_url = cli.target_url.as_deref().unwrap();
                        let target_config = cli.parse_target_connection(target_url)?;
                        apply_inline(
                            &target_config,
                            &content,
                            target_url,
                            cli.progress.resolved(),
                            cli.apply_retries,
                            !cli.no_parse_check,
                        )
                        .await?;
                    }
                }
                DdlOutput::Split(files) => match cli.outfile {
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
                            println!("{content}");
                        }
                    }
                },
            }
        }
        other => {
            return Err(error::UvgError::UnknownGenerator(other.to_string()).into());
        }
    };

    Ok(())
}

async fn run_snapshot(cli: &Cli, args: &SnapshotCommand) -> Result<()> {
    let table_filter = cli.table_filter()?;
    let options = cli.generator_options();
    let schema = load_schema_input(cli, &args.url, &table_filter, cli.noviews, &options).await?;
    snapshot::write(&args.output, &schema)?;
    eprintln!("uvg: wrote snapshot {}", args.output.display());
    Ok(())
}

async fn load_schema_input(
    cli: &Cli,
    raw: &str,
    table_filter: &TableFilter,
    noviews: bool,
    options: &GeneratorOptions,
) -> Result<IntrospectedSchema> {
    if let Some(path) = raw.strip_prefix('@') {
        if path.is_empty() {
            return Err(anyhow::anyhow!("snapshot input must be @<path>"));
        }
        let mut schema = snapshot::load(Path::new(path))?;
        schema.tables.retain(|table| {
            (!noviews || table.table_type != TableType::View) && table_filter.matches(&table.name)
        });
        return Ok(schema);
    }

    let config = cli.parse_connection_url(raw)?;
    let schemas = schemas_for_config(cli, &config);
    tracing::debug!("Introspecting schema...");
    db::introspect_with_config(
        config,
        &schemas,
        table_filter,
        noviews,
        options,
        cli.introspect_concurrency,
    )
    .await
}

fn is_snapshot_input(raw: &str) -> bool {
    raw.starts_with('@')
}

fn schemas_for_config(cli: &Cli, config: &ConnectionConfig) -> Vec<String> {
    if let Some(db) = config.database_name() {
        cli.schema_list_or(&db)
    } else {
        cli.schema_list_or(config.dialect().default_schema())
    }
}

async fn classify_or_warn(
    cli: &Cli,
    changes: Vec<crate::output::Change>,
) -> Result<Vec<crate::output::Change>> {
    if !cli.risk_classify {
        return Ok(changes);
    }
    let config = risk_classify::AnthropicConfig::from_env()?;
    if changes.is_empty() {
        return Ok(changes);
    }
    match risk_classify::classify_changes(&config, &changes).await {
        Ok(risks) => risk_classify::annotate_changes(&changes, &risks),
        Err(err) => {
            eprintln!("uvg: risk classification failed: {err}; continuing without annotations");
            Ok(changes)
        }
    }
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

/// Strip userinfo from a connection URL before it lands in a stderr
/// message. Database URLs commonly carry credentials in the
/// `scheme://user:pass@host/db` form; emitting them verbatim leaks
/// secrets into CI logs and terminal scrollback. Best-effort: a URL
/// the `url` crate can't parse (e.g. `sqlite:relative/path`) is
/// returned unchanged, since those forms don't carry credentials.
fn redact_target_url(raw: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(raw) else {
        return raw.to_string();
    };
    if parsed.username().is_empty() && parsed.password().is_none() {
        return raw.to_string();
    }
    let _ = parsed.set_username("***");
    let _ = parsed.set_password(None);
    parsed.into()
}

/// Run the per-dialect parse-check probe. Aborts with all collected
/// parse errors on any failure (not just the first) so the user can
/// fix everything in one round. Silently skipped on dialects that
/// don't support parse-only mode (MySQL, SQLite) — print a one-line
/// note instead of aborting, since the apply will still surface real
/// errors at exec time and we don't want to falsely block on dialects
/// we just can't probe.
async fn run_parse_check(config: &ConnectionConfig, content: &str) -> Result<()> {
    if !db::supports_parse_check(config) {
        eprintln!(
            "uvg: parse-check skipped (no parse-only mode for this dialect; pass --no-parse-check to silence)"
        );
        return Ok(());
    }
    let errors = db::parse_check_ddl(config, content).await?;
    if errors.is_empty() {
        return Ok(());
    }
    // Compose a multi-line error report so every parse failure shows
    // up at once, not just the first one. Each entry includes the
    // SQL preview (truncated like the progress line) plus the raw
    // dialect error.
    let mut msg = format!(
        "uvg: parse-check found {} error(s) before applying — fix and retry, or pass --no-parse-check to skip:\n",
        errors.len()
    );
    for (i, e) in errors.iter().enumerate() {
        let collapsed: String = e.sql.split_whitespace().collect::<Vec<_>>().join(" ");
        let preview = if collapsed.chars().count() > 120 {
            let cut: String = collapsed.chars().take(117).collect();
            format!("{cut}...")
        } else {
            collapsed
        };
        msg.push_str(&format!(
            "  [{}/{}] {}\n      {}\n",
            i + 1,
            errors.len(),
            preview,
            e.error,
        ));
    }
    Err(anyhow::anyhow!(msg))
}

/// Apply a freshly-rendered diff (single SQL blob) against `config`.
/// Empty diffs report "no schema changes" and succeed; first failed
/// statement bubbles up as a non-zero exit. `target_url` is redacted
/// before being printed. When `progress_enabled` is true, one
/// `[i/total] <preview>  <ms>ms` line is emitted per statement and a
/// class-breakdown summary follows.
async fn apply_inline(
    config: &ConnectionConfig,
    content: &str,
    target_url: &str,
    progress_enabled: bool,
    max_retries: u8,
    parse_check: bool,
) -> Result<()> {
    if parse_check {
        run_parse_check(config, content).await?;
    }
    let mut stats = apply_progress::ApplyStats::new();
    let results = {
        let observer = |r: &db::StmtResult, i: usize, total: usize| {
            if progress_enabled {
                apply_progress::print_progress(r, i, total);
            }
            stats.record(r);
        };
        db::execute_ddl(config, content, max_retries, observer).await?
    };
    if results.is_empty() {
        eprintln!("uvg: no schema changes");
        return Ok(());
    }
    let label = redact_target_url(target_url);
    let applied = results.iter().take_while(|r| r.error.is_none()).count();
    if let Some(failed) = results.iter().find(|r| r.error.is_some()) {
        return Err(anyhow::anyhow!(
            "uvg: apply failed on statement {}/{} against {}: {}\n--- SQL ---\n{}",
            applied + 1,
            results.len(),
            label,
            failed.error.as_deref().unwrap_or(""),
            failed.sql,
        ));
    }
    eprintln!("uvg: applied {} statement(s) to {}", applied, label);
    if progress_enabled {
        eprintln!("{}", stats.render_summary());
    }
    Ok(())
}

/// Apply a manifest's per-table files in `apply_order` (schema-scoped
/// first, then tables in topo order). Each file is parsed and executed
/// independently so the error message can pinpoint which file failed.
/// `target_url` is redacted before being printed. `progress_enabled`
/// behaves the same as `apply_inline`: per-statement lines + a single
/// final class-breakdown summary across ALL files (not per file).
async fn apply_manifest(
    config: &ConnectionConfig,
    manifest: &Manifest,
    out_dir: &std::path::Path,
    target_url: &str,
    progress_enabled: bool,
    max_retries: u8,
    parse_check: bool,
) -> Result<()> {
    let paths = apply_order(manifest, out_dir);
    let mut total_applied = 0usize;
    let mut stats = apply_progress::ApplyStats::new();
    if parse_check {
        // Concatenate every file's contents and parse-check the whole
        // batch in one pass. Per-statement parse errors carry the SQL
        // text so the user can still locate the offending statement
        // even though the file boundary is lost in the error list.
        let combined = paths
            .iter()
            .map(|p| fs::read_to_string(p).map(|s| s + "\n"))
            .collect::<std::io::Result<String>>()?;
        run_parse_check(config, &combined).await?;
    }
    for path in &paths {
        let content = fs::read_to_string(path)?;
        let results = {
            let observer = |r: &db::StmtResult, i: usize, total: usize| {
                if progress_enabled {
                    apply_progress::print_progress(r, i, total);
                }
                stats.record(r);
            };
            db::execute_ddl(config, &content, max_retries, observer).await?
        };
        let applied_here = results.iter().take_while(|r| r.error.is_none()).count();
        total_applied += applied_here;
        if let Some(failed) = results.iter().find(|r| r.error.is_some()) {
            return Err(anyhow::anyhow!(
                "uvg: apply failed in {} (statement {}/{}): {}\n--- SQL ---\n{}",
                path.display(),
                applied_here + 1,
                results.len(),
                failed.error.as_deref().unwrap_or(""),
                failed.sql,
            ));
        }
    }
    eprintln!(
        "uvg: applied {} statement(s) across {} file(s) to {}",
        total_applied,
        paths.len(),
        redact_target_url(target_url),
    );
    if progress_enabled {
        eprintln!("{}", stats.render_summary());
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

#[cfg(test)]
mod tests {
    use super::redact_target_url;

    #[test]
    fn test_redact_target_url_strips_password() {
        assert_eq!(
            redact_target_url("postgres://alice:hunter2@db.example.com:5432/orders"),
            "postgres://***@db.example.com:5432/orders",
        );
    }

    #[test]
    fn test_redact_target_url_strips_username_only() {
        assert_eq!(
            redact_target_url("mysql://root@localhost/mydb"),
            "mysql://***@localhost/mydb",
        );
    }

    #[test]
    fn test_redact_target_url_leaves_credential_free_urls_alone() {
        assert_eq!(
            redact_target_url("sqlite:///tmp/data.db"),
            "sqlite:///tmp/data.db",
        );
        assert_eq!(
            redact_target_url("postgres://db.example.com:5432/orders"),
            "postgres://db.example.com:5432/orders",
        );
    }

    #[test]
    fn test_redact_target_url_passes_through_unparseable() {
        // sqlite:relative form skips url::Url::parse — returned as-is.
        assert_eq!(
            redact_target_url("sqlite:relative.db"),
            "sqlite:relative.db"
        );
    }

    #[test]
    fn test_redact_target_url_preserves_query_and_path() {
        assert_eq!(
            redact_target_url("mysql://u:p@host/db?charset=utf8mb4"),
            "mysql://***@host/db?charset=utf8mb4",
        );
    }
}
