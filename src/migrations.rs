//! Versioned migration workflow (`uvg revision`, `upgrade`, `current`, `history`).
//!
//! This is intentionally separate from the existing `--out-dir` layout. `--out-dir`
//! organizes one live diff into reviewable files; this module manages a revision
//! chain and records the target database's current revision in `uvg_version`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};

use crate::cli::{
    Cli, Command, ConnectionConfig, DdlOptions, HistoryCommand, RevisionCommand, StampCommand,
    UpgradeCommand,
};
use crate::codegen::ddl_diff::compute_changes;
use crate::db;
use crate::dialect::Dialect;
use crate::output::{format_utc_compact, Change};

const VERSION_TABLE: &str = "uvg_version";

pub(crate) async fn run(cli: &Cli, command: &Command) -> Result<()> {
    match command {
        Command::Init(args) => crate::init::run(args),
        Command::Revision(args) => run_revision(cli, args).await,
        Command::Upgrade(args) => run_upgrade(cli, args).await,
        Command::Stamp(args) => run_stamp(cli, args).await,
        Command::Current(args) => {
            let config = cli.parse_connection_url(&args.target_url)?;
            match current_revision(&config).await? {
                Some(revision) => println!("{revision}"),
                None => println!(),
            }
            Ok(())
        }
        Command::History(args) => run_history(cli, args).await,
    }
}

async fn run_revision(cli: &Cli, args: &RevisionCommand) -> Result<()> {
    let source_config = cli.parse_connection_url(&args.source_url)?;
    let target_config = cli.parse_connection_url(&args.target_url)?;
    let source_dialect = source_config.dialect();
    let target_dialect = target_config.dialect();
    let source_schemas = schemas_for(cli, &source_config);
    let target_schemas = schemas_for(cli, &target_config);
    let table_filter = cli.table_filter()?;
    let gen_opts = cli.generator_options();

    let source = db::introspect_with_config(
        source_config,
        &source_schemas,
        &table_filter,
        cli.noviews,
        &gen_opts,
    )
    .await?;
    let target = db::introspect_with_config(
        target_config,
        &target_schemas,
        &table_filter,
        cli.noviews,
        &gen_opts,
    )
    .await?;

    let options = DdlOptions {
        target_dialect,
        split_tables: false,
        apply: false,
        noindexes: gen_opts.noindexes,
        noconstraints: gen_opts.noconstraints,
        nocomments: gen_opts.nocomments,
    };
    let changes = compute_changes(&source, &target, &options);
    if changes.is_empty() {
        eprintln!("uvg: no schema changes");
        return Ok(());
    }

    let graph = MigrationGraph::load(&args.migrations_dir)?;
    let parent = graph.single_head()?;
    let revision = next_revision_id(&graph);
    let path = write_revision_file(
        &args.migrations_dir,
        &revision,
        parent.as_deref(),
        &args.message,
        source_dialect,
        target_dialect,
        &changes,
    )?;

    let refreshed = MigrationGraph::load(&args.migrations_dir)?;
    write_meta_file(&args.migrations_dir, &refreshed)?;

    println!("Wrote: {}", path.display());
    Ok(())
}

async fn run_upgrade(cli: &Cli, args: &UpgradeCommand) -> Result<()> {
    let graph = MigrationGraph::load(&args.migrations_dir)?;
    if graph.is_empty() {
        eprintln!(
            "uvg: no migrations found in {}",
            args.migrations_dir.display()
        );
        return Ok(());
    }
    let target = graph.resolve_target(args.revision.as_deref())?;
    let config = cli.parse_connection_url(&args.target_url)?;

    ensure_version_table(&config).await?;
    let current = current_revision(&config).await?;
    let plan = graph.plan_upgrade(current.as_deref(), target.as_deref())?;
    if plan.is_empty() {
        let label = current.as_deref().unwrap_or("base");
        eprintln!("uvg: already at {label}");
        return Ok(());
    }

    for migration in plan {
        apply_migration(&config, migration).await?;
        record_revision(&config, &migration.revision, &migration.description).await?;
        eprintln!("uvg: applied {}", migration.revision);
    }

    Ok(())
}

async fn run_stamp(cli: &Cli, args: &StampCommand) -> Result<()> {
    let graph = MigrationGraph::load(&args.migrations_dir)?;
    let migration = graph.require_revision(&args.revision)?;
    let config = cli.parse_connection_url(&args.target_url)?;

    if !args.yes && !confirm_stamp(&args.target_url, &args.revision)? {
        eprintln!("uvg: stamp cancelled");
        return Ok(());
    }

    stamp_revision(&config, migration).await?;
    eprintln!(
        "uvg: stamped {} at revision {}",
        redact_url(&args.target_url),
        migration.revision
    );
    Ok(())
}

async fn run_history(cli: &Cli, args: &HistoryCommand) -> Result<()> {
    let graph = MigrationGraph::load(&args.migrations_dir)?;
    if graph.is_empty() {
        eprintln!(
            "uvg: no migrations found in {}",
            args.migrations_dir.display()
        );
        return Ok(());
    }

    let current = if let Some(url) = args.target_url.as_deref() {
        let config = cli.parse_connection_url(url)?;
        current_revision(&config).await?
    } else {
        None
    };
    let applied = current
        .as_deref()
        .map(|revision| graph.ancestor_set(revision))
        .transpose()?
        .unwrap_or_default();
    let heads: HashSet<String> = graph.heads().into_iter().collect();

    for migration in graph.ordered() {
        let parent = if migration.parents.is_empty() {
            "base".to_string()
        } else {
            migration.parents.join(",")
        };
        let mut markers = Vec::new();
        if applied.contains(&migration.revision) {
            markers.push("applied");
        }
        if current.as_deref() == Some(migration.revision.as_str()) {
            markers.push("current");
        }
        if heads.contains(&migration.revision) {
            markers.push("head");
        }
        let suffix = if markers.is_empty() {
            String::new()
        } else {
            format!(" [{}]", markers.join(", "))
        };
        println!(
            "{}  {}  (parent: {}){}",
            migration.revision, migration.description, parent, suffix
        );
    }

    Ok(())
}

fn schemas_for(cli: &Cli, config: &ConnectionConfig) -> Vec<String> {
    if let Some(db) = config.database_name() {
        cli.schema_list_or(&db)
    } else {
        cli.schema_list_or(config.dialect().default_schema())
    }
}

#[derive(Debug, Clone)]
struct MigrationFile {
    revision: String,
    parents: Vec<String>,
    description: String,
    path: PathBuf,
    up_sql: String,
}

#[derive(Debug, Clone)]
struct MigrationGraph {
    migrations: BTreeMap<String, MigrationFile>,
}

impl MigrationGraph {
    fn load(dir: &Path) -> Result<Self> {
        if !dir.exists() {
            return Ok(Self {
                migrations: BTreeMap::new(),
            });
        }
        let mut paths = Vec::new();
        for entry in fs::read_dir(dir)
            .with_context(|| format!("failed to read migrations directory {}", dir.display()))?
        {
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                paths.push(path);
            }
        }
        paths.sort();

        let mut migrations = BTreeMap::new();
        for path in paths {
            let body = fs::read_to_string(&path)
                .with_context(|| format!("failed to read migration {}", path.display()))?;
            let migration = parse_migration_file(&body, path.clone())?;
            if migrations
                .insert(migration.revision.clone(), migration.clone())
                .is_some()
            {
                return Err(anyhow!(
                    "duplicate migration revision `{}`",
                    migration.revision
                ));
            }
        }

        Ok(Self { migrations })
    }

    fn is_empty(&self) -> bool {
        self.migrations.is_empty()
    }

    fn single_head(&self) -> Result<Option<String>> {
        let heads = self.heads();
        match heads.len() {
            0 => Ok(None),
            1 => Ok(heads.into_iter().next()),
            _ => Err(anyhow!(
                "multiple migration heads found: {}. `uvg merge` is not implemented yet",
                heads.join(", ")
            )),
        }
    }

    fn heads(&self) -> Vec<String> {
        let mut referenced = BTreeSet::new();
        for migration in self.migrations.values() {
            for parent in &migration.parents {
                referenced.insert(parent.clone());
            }
        }
        self.migrations
            .keys()
            .filter(|revision| !referenced.contains(*revision))
            .cloned()
            .collect()
    }

    fn resolve_target(&self, requested: Option<&str>) -> Result<Option<String>> {
        match requested {
            Some("base") => Ok(None),
            Some(revision) => {
                if !self.migrations.contains_key(revision) {
                    return Err(anyhow!(
                        "unknown migration revision `{}`. Valid revisions: {}",
                        revision,
                        self.valid_revisions()
                    ));
                }
                Ok(Some(revision.to_string()))
            }
            None => self.single_head(),
        }
    }

    fn require_revision(&self, revision: &str) -> Result<&MigrationFile> {
        self.migrations.get(revision).ok_or_else(|| {
            anyhow!(
                "unknown migration revision `{}`. Valid revisions: {}",
                revision,
                self.valid_revisions()
            )
        })
    }

    fn plan_upgrade<'a>(
        &'a self,
        current: Option<&str>,
        target: Option<&str>,
    ) -> Result<Vec<&'a MigrationFile>> {
        if current == target {
            return Ok(Vec::new());
        }
        if let Some(current_revision) = current {
            if !self.migrations.contains_key(current_revision) {
                return Err(anyhow!(
                    "target database is stamped at unknown revision `{}`. Valid revisions: {}",
                    current_revision,
                    self.valid_revisions()
                ));
            }
        }

        let Some(target_revision) = target else {
            return Ok(Vec::new());
        };
        let mut chain = Vec::new();
        let mut cursor = Some(target_revision.to_string());

        while let Some(revision) = cursor {
            if current == Some(revision.as_str()) {
                chain.reverse();
                return Ok(chain);
            }
            let migration = self
                .migrations
                .get(&revision)
                .ok_or_else(|| anyhow!("migration `{revision}` is missing from the graph"))?;
            if migration.parents.len() > 1 {
                return Err(anyhow!(
                    "revision `{}` has multiple parents; merge migrations are not supported yet",
                    migration.revision
                ));
            }
            chain.push(migration);
            cursor = migration.parents.first().cloned();
        }

        if current.is_none() {
            chain.reverse();
            return Ok(chain);
        }

        let current_revision = current.unwrap_or("base");
        Err(anyhow!(
            "revision `{}` is not an ancestor of `{}`; branched upgrade paths are not supported yet",
            current_revision,
            target_revision
        ))
    }

    fn ordered(&self) -> Vec<&MigrationFile> {
        let mut children: HashMap<Option<String>, Vec<String>> = HashMap::new();
        for migration in self.migrations.values() {
            let parent = migration.parents.first().cloned();
            children
                .entry(parent)
                .or_default()
                .push(migration.revision.clone());
        }
        for revisions in children.values_mut() {
            revisions.sort();
        }

        let mut ordered = Vec::new();
        let mut stack = children.remove(&None).unwrap_or_default();
        stack.reverse();
        while let Some(revision) = stack.pop() {
            if let Some(migration) = self.migrations.get(&revision) {
                ordered.push(migration);
                if let Some(mut kids) = children.remove(&Some(revision.clone())) {
                    kids.reverse();
                    stack.extend(kids);
                }
            }
        }

        if ordered.len() < self.migrations.len() {
            let seen: HashSet<&str> = ordered.iter().map(|m| m.revision.as_str()).collect();
            for migration in self.migrations.values() {
                if !seen.contains(migration.revision.as_str()) {
                    ordered.push(migration);
                }
            }
        }
        ordered
    }

    fn ancestor_set(&self, revision: &str) -> Result<HashSet<String>> {
        if !self.migrations.contains_key(revision) {
            return Err(anyhow!(
                "target database is stamped at unknown revision `{}`. Valid revisions: {}",
                revision,
                self.valid_revisions()
            ));
        }
        let mut seen = HashSet::new();
        let mut cursor = Some(revision.to_string());
        while let Some(rev) = cursor {
            let migration = self
                .migrations
                .get(&rev)
                .ok_or_else(|| anyhow!("migration `{rev}` is missing from the graph"))?;
            seen.insert(rev.clone());
            cursor = migration.parents.first().cloned();
        }
        Ok(seen)
    }

    fn valid_revisions(&self) -> String {
        if self.migrations.is_empty() {
            "(none)".to_string()
        } else {
            self.migrations
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        }
    }
}

fn parse_migration_file(body: &str, path: PathBuf) -> Result<MigrationFile> {
    let mut revision = None;
    let mut parents: Option<Vec<String>> = None;
    let mut description = String::new();
    let mut in_up = false;
    let mut up_lines = Vec::new();

    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.eq_ignore_ascii_case("-- UP") {
            in_up = true;
            continue;
        }
        if in_up && is_section_marker(trimmed) {
            break;
        }
        if in_up {
            up_lines.push(line);
            continue;
        }

        if let Some(value) = trimmed.strip_prefix("-- uvg revision:") {
            revision = Some(value.trim().to_string());
        } else if let Some(value) = trimmed.strip_prefix("-- parent:") {
            let value = value.trim();
            parents = Some(if value.is_empty() {
                Vec::new()
            } else {
                vec![value.to_string()]
            });
        } else if let Some(value) = trimmed.strip_prefix("-- parents:") {
            parents = Some(
                value
                    .split(',')
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty())
                    .collect(),
            );
        } else if let Some(value) = trimmed.strip_prefix("-- description:") {
            description = value.trim().to_string();
        }
    }

    let revision = revision.ok_or_else(|| {
        anyhow!(
            "migration {} is missing `-- uvg revision:` header",
            path.display()
        )
    })?;
    if !in_up {
        return Err(anyhow!(
            "migration {} is missing required `-- UP` section",
            path.display()
        ));
    }
    let up_sql = up_lines.join("\n").trim().to_string();

    Ok(MigrationFile {
        revision,
        parents: parents.unwrap_or_default(),
        description,
        path,
        up_sql,
    })
}

fn is_section_marker(trimmed: &str) -> bool {
    matches!(
        trimmed.to_ascii_uppercase().as_str(),
        "-- PRE" | "-- POST" | "-- DOWN" | "-- POST DOWN" | "-- PRE DOWN"
    )
}

fn write_revision_file(
    migrations_dir: &Path,
    revision: &str,
    parent: Option<&str>,
    description: &str,
    source_dialect: Dialect,
    target_dialect: Dialect,
    changes: &[Change],
) -> Result<PathBuf> {
    fs::create_dir_all(migrations_dir)
        .with_context(|| format!("failed to create {}", migrations_dir.display()))?;
    let filename = format!("{}_{}.sql", revision, slugify(description));
    let path = migrations_dir.join(filename);
    if path.exists() {
        return Err(anyhow!(
            "refusing to overwrite existing migration {}",
            path.display()
        ));
    }

    let mut body = String::new();
    body.push_str(&format!(
        "-- uvg revision: {}\n",
        flatten_for_comment(revision)
    ));
    body.push_str(&format!(
        "-- parent: {}\n",
        flatten_for_comment(parent.unwrap_or(""))
    ));
    body.push_str(&format!(
        "-- description: {}\n",
        flatten_for_comment(description)
    ));
    body.push_str(&format!("-- source: {source_dialect}\n"));
    body.push_str(&format!("-- target: {target_dialect}\n\n"));
    body.push_str("-- UP\n");
    body.push_str(&render_up_sql(changes));
    if !body.ends_with('\n') {
        body.push('\n');
    }

    fs::write(&path, body).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

fn write_meta_file(migrations_dir: &Path, graph: &MigrationGraph) -> Result<()> {
    let heads = graph.heads();
    let head = if heads.len() == 1 {
        heads[0].clone()
    } else {
        String::new()
    };
    let mut body = String::new();
    body.push_str("# Generated by uvg. Revision files are the source of truth.\n");
    body.push_str(&format!("head: {}\n", yaml_quote(&head)));
    body.push_str("revisions:\n");
    for migration in graph.ordered() {
        body.push_str(&format!(
            "  - revision: {}\n",
            yaml_quote(&migration.revision)
        ));
        body.push_str(&format!(
            "    parent: {}\n",
            yaml_quote(migration.parents.first().map(String::as_str).unwrap_or(""))
        ));
        body.push_str(&format!(
            "    description: {}\n",
            yaml_quote(&migration.description)
        ));
    }

    fs::write(migrations_dir.join("meta.yaml"), body).with_context(|| {
        format!(
            "failed to write {}",
            migrations_dir.join("meta.yaml").display()
        )
    })
}

fn render_up_sql(changes: &[Change]) -> String {
    changes
        .iter()
        .map(|change| change.sql.as_str())
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn next_revision_id(graph: &MigrationGraph) -> String {
    let base = revision_id_from_epoch(now_epoch_secs());
    if !graph.migrations.contains_key(&base) {
        return base;
    }
    for i in 1..100 {
        let candidate = format!("{base}_{i:02}");
        if !graph.migrations.contains_key(&candidate) {
            return candidate;
        }
    }
    base
}

fn revision_id_from_epoch(epoch_secs: u64) -> String {
    let compact = format_utc_compact(epoch_secs);
    format!("{}_{}", &compact[0..8], &compact[9..15])
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn slugify(input: &str) -> String {
    let mut out = String::new();
    let mut last_dash = false;
    for c in input.chars().flat_map(|c| c.to_lowercase()) {
        if c.is_ascii_alphanumeric() {
            out.push(c);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
        if out.len() >= 64 {
            break;
        }
    }
    let trimmed = out.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "migration".to_string()
    } else {
        trimmed
    }
}

fn flatten_for_comment(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '\n' => "\\n".to_string(),
            '\r' => "\\r".to_string(),
            '\t' => "\\t".to_string(),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => format!("\\x{:02x}", c as u32),
            c => c.to_string(),
        })
        .collect()
}

fn yaml_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

async fn apply_migration(config: &ConnectionConfig, migration: &MigrationFile) -> Result<()> {
    let results = db::execute_ddl(config, &migration.up_sql, 3, |_, _, _| {}).await?;
    let applied = results.iter().take_while(|r| r.error.is_none()).count();
    if let Some(failed) = results.iter().find(|r| r.error.is_some()) {
        return Err(anyhow!(
            "uvg: migration {} failed in {} at statement {}/{}: {}\n--- SQL ---\n{}",
            migration.revision,
            migration.path.display(),
            applied + 1,
            results.len(),
            failed.error.as_deref().unwrap_or(""),
            failed.sql
        ));
    }
    Ok(())
}

async fn stamp_revision(config: &ConnectionConfig, migration: &MigrationFile) -> Result<()> {
    ensure_version_table(config).await?;
    record_revision(config, &migration.revision, &migration.description).await
}

fn confirm_stamp(target_url: &str, revision: &str) -> Result<bool> {
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

fn redact_url(raw: &str) -> String {
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

async fn ensure_version_table(config: &ConnectionConfig) -> Result<()> {
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

async fn current_revision(config: &ConnectionConfig) -> Result<Option<String>> {
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

async fn version_table_exists(config: &ConnectionConfig) -> Result<bool> {
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

async fn record_revision(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ConnectionConfig;
    use crate::output::Change;

    fn tmpdir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "uvg-migrations-test-{label}-{}-{nanos}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn migration(revision: &str, parent: &str, description: &str) -> String {
        format!(
            "-- uvg revision: {revision}\n-- parent: {parent}\n-- description: {description}\n\n-- UP\nCREATE TABLE t_{revision}(id integer);\n"
        )
    }

    #[test]
    fn test_revision_id_from_epoch() {
        assert_eq!(revision_id_from_epoch(1_778_700_600), "20260513_193000");
    }

    #[test]
    fn test_slugify() {
        assert_eq!(slugify("Add users.email index"), "add-users-email-index");
        assert_eq!(slugify("///"), "migration");
    }

    #[test]
    fn test_parse_migration_file() {
        let path = PathBuf::from("migrations/20260513_193000_initial.sql");
        let parsed = parse_migration_file(
            "-- uvg revision: 20260513_193000\n-- parent: \n-- description: initial\n\n-- UP\nCREATE TABLE users(id integer);\n\n-- DOWN\nDROP TABLE users;\n",
            path.clone(),
        )
        .unwrap();
        assert_eq!(parsed.revision, "20260513_193000");
        assert!(parsed.parents.is_empty());
        assert_eq!(parsed.description, "initial");
        assert_eq!(parsed.path, path);
        assert!(parsed.up_sql.contains("CREATE TABLE users"));
        assert!(!parsed.up_sql.contains("DROP TABLE"));
    }

    #[test]
    fn test_graph_plans_linear_upgrade() {
        let dir = tmpdir("linear");
        fs::write(
            dir.join("20260513_193000_initial.sql"),
            migration("20260513_193000", "", "initial"),
        )
        .unwrap();
        fs::write(
            dir.join("20260514_084500_add_email.sql"),
            migration("20260514_084500", "20260513_193000", "add email"),
        )
        .unwrap();

        let graph = MigrationGraph::load(&dir).unwrap();
        let plan = graph
            .plan_upgrade(Some("20260513_193000"), Some("20260514_084500"))
            .unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].revision, "20260514_084500");
        assert_eq!(
            graph.single_head().unwrap().as_deref(),
            Some("20260514_084500")
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_graph_rejects_unknown_current_revision() {
        let dir = tmpdir("unknown-current");
        fs::write(
            dir.join("20260513_193000_initial.sql"),
            migration("20260513_193000", "", "initial"),
        )
        .unwrap();
        let graph = MigrationGraph::load(&dir).unwrap();
        let err = graph
            .plan_upgrade(Some("missing"), Some("20260513_193000"))
            .unwrap_err();
        assert!(err.to_string().contains("unknown revision"));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_require_revision_rejects_unknown_stamp_target() {
        let dir = tmpdir("unknown-stamp");
        fs::write(
            dir.join("20260513_193000_initial.sql"),
            migration("20260513_193000", "", "initial"),
        )
        .unwrap();
        let graph = MigrationGraph::load(&dir).unwrap();
        let err = graph.require_revision("missing").unwrap_err();
        assert!(err.to_string().contains("unknown migration revision"));
        assert!(err.to_string().contains("20260513_193000"));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_write_revision_file_and_meta() {
        let dir = tmpdir("write");
        let changes = vec![Change {
            table_schema: "".into(),
            table_name: Some("users".into()),
            sql: "CREATE TABLE users(id integer);".into(),
        }];
        let path = write_revision_file(
            &dir,
            "20260513_193000",
            None,
            "initial schema",
            Dialect::Postgres,
            Dialect::Postgres,
            &changes,
        )
        .unwrap();
        let body = fs::read_to_string(&path).unwrap();
        assert!(body.contains("-- uvg revision: 20260513_193000"));
        assert!(body.contains("-- UP\nCREATE TABLE users"));

        let graph = MigrationGraph::load(&dir).unwrap();
        write_meta_file(&dir, &graph).unwrap();
        let meta = fs::read_to_string(dir.join("meta.yaml")).unwrap();
        assert!(meta.contains("head: '20260513_193000'"));
        assert!(meta.contains("description: 'initial schema'"));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_graph_loads_dot_prefixed_baseline_file() {
        let dir = tmpdir("dot-baseline");
        fs::write(
            dir.join(".uvg-revision-00000000_000000_initial.sql"),
            migration("00000000_000000", "", "initial baseline"),
        )
        .unwrap();

        let graph = MigrationGraph::load(&dir).unwrap();
        assert_eq!(
            graph.single_head().unwrap().as_deref(),
            Some("00000000_000000")
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_sqlite_current_and_record_revision() {
        let dir = tmpdir("sqlite-version");
        let db_path = dir.join("target.db");
        fs::File::create(&db_path).unwrap();
        let config = ConnectionConfig::Sqlite(format!("sqlite://{}", db_path.display()));

        assert_eq!(current_revision(&config).await.unwrap(), None);
        ensure_version_table(&config).await.unwrap();
        record_revision(&config, "20260513_193000", "initial")
            .await
            .unwrap();
        assert_eq!(
            current_revision(&config).await.unwrap().as_deref(),
            Some("20260513_193000")
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_sqlite_stamp_revision_creates_version_table_without_running_up_sql() {
        let dir = tmpdir("sqlite-stamp");
        let db_path = dir.join("target.db");
        fs::File::create(&db_path).unwrap();
        let config = ConnectionConfig::Sqlite(format!("sqlite://{}", db_path.display()));
        let migration = MigrationFile {
            revision: "20260513_193000".into(),
            parents: Vec::new(),
            description: "initial".into(),
            path: dir.join("20260513_193000_initial.sql"),
            up_sql: "CREATE TABLE users(id integer primary key);".into(),
        };

        stamp_revision(&config, &migration).await.unwrap();
        assert_eq!(
            current_revision(&config).await.unwrap().as_deref(),
            Some("20260513_193000")
        );

        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{}", db_path.display()))
            .await
            .unwrap();
        let users_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name = 'users'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let version_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name = 'uvg_version'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let stamped_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM uvg_version")
            .fetch_one(&pool)
            .await
            .unwrap();
        pool.close().await;

        assert_eq!(users_count, 0, "stamp must not execute migration SQL");
        assert_eq!(version_count, 1, "stamp should create uvg_version");
        assert_eq!(stamped_rows, 1, "stamp should write one version row");

        fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_sqlite_apply_migration_executes_up_sql() {
        let dir = tmpdir("sqlite-apply");
        let db_path = dir.join("target.db");
        fs::File::create(&db_path).unwrap();
        let config = ConnectionConfig::Sqlite(format!("sqlite://{}", db_path.display()));
        let migration = MigrationFile {
            revision: "20260513_193000".into(),
            parents: Vec::new(),
            description: "initial".into(),
            path: dir.join("20260513_193000_initial.sql"),
            up_sql: "CREATE TABLE users(id integer primary key);".into(),
        };

        apply_migration(&config, &migration).await.unwrap();

        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{}", db_path.display()))
            .await
            .unwrap();
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name = 'users'")
                .fetch_one(&pool)
                .await
                .unwrap();
        pool.close().await;
        assert_eq!(count, 1);

        fs::remove_dir_all(&dir).ok();
    }
}
