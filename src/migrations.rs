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
    Cli, Command, ConnectionConfig, DdlOptions, DowngradeCommand, HistoryCommand, MergeCommand,
    RevisionCommand, StampCommand, UpgradeCommand,
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
        Command::Downgrade(args) => run_downgrade(cli, args).await,
        Command::Merge(args) => run_merge(args),
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
        Command::Snapshot(_) => unreachable!("snapshot is handled before migration dispatch"),
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
        cli.introspect_concurrency,
    )
    .await?;
    let target = db::introspect_with_config(
        target_config,
        &target_schemas,
        &table_filter,
        cli.noviews,
        &gen_opts,
        cli.introspect_concurrency,
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

    let parse_check = migration_parse_check_enabled(cli, &config);
    for migration in plan {
        if parse_check {
            parse_check_migration(&config, migration, MigrationDirection::Up).await?;
        }
        apply_migration(&config, migration).await.with_context(|| {
            format!(
                "uvg: migration {} UP failed before uvg_version was changed",
                migration.revision
            )
        })?;
        record_revision(&config, &migration.revision, &migration.description)
            .await
            .with_context(|| {
                format!(
                    "uvg: migration {} SQL was applied, but failed to record uvg_version at {}; verify the target, then run `uvg stamp <target-url> {} --yes` if it already matches",
                    migration.revision, migration.revision, migration.revision
                )
            })?;
        eprintln!("uvg: applied {}", migration.revision);
    }

    Ok(())
}

async fn run_downgrade(cli: &Cli, args: &DowngradeCommand) -> Result<()> {
    let graph = MigrationGraph::load(&args.migrations_dir)?;
    if graph.is_empty() {
        eprintln!(
            "uvg: no migrations found in {}",
            args.migrations_dir.display()
        );
        return Ok(());
    }
    let config = cli.parse_connection_url(&args.target_url)?;

    ensure_version_table(&config).await?;
    let current = current_revision(&config).await?;
    let plan = graph.plan_downgrade(current.as_deref(), args.revision.as_deref())?;
    if plan.is_empty() {
        let label = current.as_deref().unwrap_or("base");
        eprintln!("uvg: already at {label}");
        return Ok(());
    }

    let parse_check = migration_parse_check_enabled(cli, &config);
    for migration in plan {
        if parse_check {
            parse_check_migration(&config, migration, MigrationDirection::Down).await?;
        }
        apply_down_migration(&config, migration)
            .await
            .with_context(|| {
                format!(
                    "uvg: migration {} DOWN failed before uvg_version was changed",
                    migration.revision
                )
            })?;
        if let Some(parent) = migration.parents.first() {
            record_revision(&config, parent, parent_description(&graph, parent))
                .await
                .with_context(|| {
                    format!(
                        "uvg: migration {} DOWN SQL was applied, but failed to record uvg_version at parent {}; verify the target, then run `uvg stamp <target-url> {} --yes` if it already matches",
                        migration.revision, parent, parent
                    )
                })?;
            eprintln!("uvg: downgraded {} -> {}", migration.revision, parent);
        } else {
            clear_revision(&config).await.with_context(|| {
                format!(
                    "uvg: migration {} DOWN SQL was applied, but failed to clear uvg_version; verify the target is at base, then clear uvg_version manually",
                    migration.revision
                )
            })?;
            eprintln!("uvg: downgraded {} -> base", migration.revision);
        }
    }

    Ok(())
}

fn run_merge(args: &MergeCommand) -> Result<()> {
    let graph = MigrationGraph::load(&args.migrations_dir)?;
    let heads = graph.heads();
    if heads.len() < 2 {
        return Err(anyhow!(
            "uvg merge requires at least two heads; current heads: {}",
            if heads.is_empty() {
                "(none)".to_string()
            } else {
                heads.join(", ")
            }
        ));
    }

    let revision = next_revision_id(&graph);
    let path = write_merge_revision_file(&args.migrations_dir, &revision, &heads, &args.message)?;
    let refreshed = MigrationGraph::load(&args.migrations_dir)?;
    write_meta_file(&args.migrations_dir, &refreshed)?;

    println!("Wrote: {}", path.display());
    eprintln!(
        "uvg: merge revision {} joins heads {}",
        revision,
        heads.join(", ")
    );
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
        let (parent_label, parent_value) = if migration.parents.is_empty() {
            ("parent", "base".to_string())
        } else if migration.parents.len() == 1 {
            ("parent", migration.parents[0].clone())
        } else {
            ("parents", migration.parents.join(","))
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
            "{}  {}  ({}: {}){}",
            migration.revision, migration.description, parent_label, parent_value, suffix
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
    pre_sql: String,
    up_sql: String,
    post_sql: String,
    pre_down_sql: String,
    down_sql: Option<String>,
    post_down_sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MigrationSection {
    Pre,
    Up,
    Post,
    PostDown,
    Down,
    PreDown,
}

impl MigrationSection {
    fn label(self) -> &'static str {
        match self {
            Self::Pre => "PRE",
            Self::Up => "UP",
            Self::Post => "POST",
            Self::PostDown => "POST DOWN",
            Self::Down => "DOWN",
            Self::PreDown => "PRE DOWN",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum MigrationDirection {
    Up,
    Down,
}

impl MigrationDirection {
    fn label(self) -> &'static str {
        match self {
            Self::Up => "UP",
            Self::Down => "DOWN",
        }
    }
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
                "multiple migration heads found: {}. Run `uvg merge --message <name>` or pass an explicit revision",
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
        let target_ancestors = self.ancestor_set(target_revision)?;
        let current_ancestors = if let Some(current_revision) = current {
            if !target_ancestors.contains(current_revision) {
                return Err(anyhow!(
                    "revision `{}` is not an ancestor of `{}`; branched upgrade paths are not supported yet",
                    current_revision,
                    target_revision
                ));
            }
            self.ancestor_set(current_revision)?
        } else {
            HashSet::new()
        };
        let pending: HashSet<&str> = target_ancestors
            .iter()
            .map(String::as_str)
            .filter(|revision| !current_ancestors.contains(*revision))
            .collect();

        Ok(self
            .ordered()
            .into_iter()
            .filter(|migration| pending.contains(migration.revision.as_str()))
            .collect())
    }

    fn plan_downgrade<'a>(
        &'a self,
        current: Option<&str>,
        requested: Option<&str>,
    ) -> Result<Vec<&'a MigrationFile>> {
        let Some(current_revision) = current else {
            if matches!(requested, None | Some("base")) {
                return Ok(Vec::new());
            }
            return Err(anyhow!(
                "target database has no current revision; cannot downgrade to `{}`",
                requested.unwrap_or("base")
            ));
        };
        let current_migration = self.require_revision(current_revision)?;

        if requested.is_none() {
            if current_migration.parents.len() > 1 {
                return Err(anyhow!(
                    "cannot downgrade through merge revision `{}` because uvg_version tracks a single current revision; resolve manually and use `uvg stamp`",
                    current_migration.revision
                ));
            }
            return Ok(vec![current_migration]);
        }

        let target = match requested {
            Some("base") => None,
            Some(revision) => {
                self.require_revision(revision)?;
                Some(revision)
            }
            None => unreachable!(),
        };
        if target == Some(current_revision) {
            return Ok(Vec::new());
        }

        let current_ancestors = self.ancestor_set(current_revision)?;
        let target_ancestors = if let Some(target_revision) = target {
            if !current_ancestors.contains(target_revision) {
                return Err(anyhow!(
                    "revision `{}` is not an ancestor of `{}`; cannot downgrade across unrelated branches",
                    target_revision,
                    current_revision
                ));
            }
            self.ancestor_set(target_revision)?
        } else {
            HashSet::new()
        };
        let pending: HashSet<&str> = current_ancestors
            .iter()
            .map(String::as_str)
            .filter(|revision| !target_ancestors.contains(*revision))
            .collect();

        let plan = self
            .ordered()
            .into_iter()
            .rev()
            .filter(|migration| pending.contains(migration.revision.as_str()))
            .collect::<Vec<_>>();
        if let Some(merge) = plan.iter().find(|migration| migration.parents.len() > 1) {
            return Err(anyhow!(
                "cannot downgrade through merge revision `{}` because uvg_version tracks a single current revision; resolve manually and use `uvg stamp`",
                merge.revision
            ));
        }

        Ok(plan)
    }

    fn ordered(&self) -> Vec<&MigrationFile> {
        let mut indegree: HashMap<String, usize> = self
            .migrations
            .keys()
            .map(|revision| (revision.clone(), 0))
            .collect();
        let mut children: HashMap<String, Vec<String>> = HashMap::new();
        for migration in self.migrations.values() {
            for parent in &migration.parents {
                if self.migrations.contains_key(parent) {
                    *indegree.entry(migration.revision.clone()).or_default() += 1;
                    children
                        .entry(parent.clone())
                        .or_default()
                        .push(migration.revision.clone());
                }
            }
        }
        for revisions in children.values_mut() {
            revisions.sort();
        }

        let mut ordered = Vec::new();
        let mut ready: BTreeSet<String> = indegree
            .iter()
            .filter_map(|(revision, count)| {
                if *count == 0 {
                    Some(revision.clone())
                } else {
                    None
                }
            })
            .collect();
        while let Some(revision) = ready.iter().next().cloned() {
            ready.remove(&revision);
            if let Some(migration) = self.migrations.get(&revision) {
                ordered.push(migration);
                if let Some(kids) = children.get(&revision) {
                    for child in kids {
                        if let Some(count) = indegree.get_mut(child) {
                            *count -= 1;
                            if *count == 0 {
                                ready.insert(child.clone());
                            }
                        }
                    }
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
        let mut stack = vec![revision.to_string()];
        while let Some(rev) = stack.pop() {
            if !seen.insert(rev.clone()) {
                continue;
            }
            let migration = self
                .migrations
                .get(&rev)
                .ok_or_else(|| anyhow!("migration `{rev}` is missing from the graph"))?;
            for parent in &migration.parents {
                stack.push(parent.clone());
            }
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
    let mut current_section = None;
    let mut section_lines: HashMap<MigrationSection, Vec<&str>> = HashMap::new();

    for line in body.lines() {
        let trimmed = line.trim();
        if let Some(section) = section_marker(trimmed) {
            current_section = Some(section);
            section_lines.entry(section).or_default();
            continue;
        }
        if let Some(section) = current_section {
            section_lines.entry(section).or_default().push(line);
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

    let section_sql = |section| {
        section_lines
            .get(&section)
            .map(|lines| lines.join("\n").trim().to_string())
            .unwrap_or_default()
    };
    let revision = revision.ok_or_else(|| {
        anyhow!(
            "migration {} is missing `-- uvg revision:` header",
            path.display()
        )
    })?;
    if !section_lines.contains_key(&MigrationSection::Up) {
        return Err(anyhow!(
            "migration {} is missing required `-- UP` section",
            path.display()
        ));
    }

    Ok(MigrationFile {
        revision,
        parents: parents.unwrap_or_default(),
        description,
        path,
        pre_sql: section_sql(MigrationSection::Pre),
        up_sql: section_sql(MigrationSection::Up),
        post_sql: section_sql(MigrationSection::Post),
        pre_down_sql: section_sql(MigrationSection::PreDown),
        down_sql: section_lines
            .contains_key(&MigrationSection::Down)
            .then(|| section_sql(MigrationSection::Down)),
        post_down_sql: section_sql(MigrationSection::PostDown),
    })
}

fn section_marker(trimmed: &str) -> Option<MigrationSection> {
    match trimmed.to_ascii_uppercase().as_str() {
        "-- PRE" => Some(MigrationSection::Pre),
        "-- UP" => Some(MigrationSection::Up),
        "-- POST" => Some(MigrationSection::Post),
        "-- POST DOWN" => Some(MigrationSection::PostDown),
        "-- DOWN" => Some(MigrationSection::Down),
        "-- PRE DOWN" => Some(MigrationSection::PreDown),
        _ => None,
    }
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
    body.push('\n');
    body.push_str("-- DOWN\n");
    body.push_str(&render_down_sql(changes, target_dialect));
    if !body.ends_with('\n') {
        body.push('\n');
    }

    fs::write(&path, body).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

fn write_merge_revision_file(
    migrations_dir: &Path,
    revision: &str,
    parents: &[String],
    description: &str,
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
        "-- parents: {}\n",
        parents
            .iter()
            .map(|parent| flatten_for_comment(parent))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    body.push_str(&format!(
        "-- description: {}\n\n",
        flatten_for_comment(description)
    ));
    body.push_str("-- UP\n");
    body.push_str("-- Empty merge revision. Branch migrations already carry the SQL.\n\n");
    body.push_str("-- DOWN\n");
    body.push_str(
        "-- Merge downgrade is not automatic because uvg_version tracks one current revision.\n",
    );

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
        if migration.parents.len() > 1 {
            body.push_str(&format!(
                "    parents: {}\n",
                yaml_inline_list(&migration.parents)
            ));
        } else {
            body.push_str(&format!(
                "    parent: {}\n",
                yaml_quote(migration.parents.first().map(String::as_str).unwrap_or(""))
            ));
        }
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

fn render_down_sql(changes: &[Change], target_dialect: Dialect) -> String {
    let mut reversed = changes
        .iter()
        .rev()
        .map(|change| reverse_change_sql(&change.sql, target_dialect))
        .collect::<Vec<_>>()
        .join("\n\n");
    if reversed.is_empty() {
        reversed.push_str("-- No reverse SQL generated.");
    }
    reversed
}

fn reverse_change_sql(sql: &str, target_dialect: Dialect) -> String {
    let Some(statement) = executable_statement(sql) else {
        return "-- IRREVERSIBLE: no executable SQL found to reverse.".to_string();
    };
    let upper = statement.to_ascii_uppercase();

    if upper.starts_with("CREATE TABLE ") {
        let rest = statement["CREATE TABLE".len()..].trim();
        let table = rest
            .split_once('(')
            .map(|(name, _)| name)
            .unwrap_or(rest)
            .trim();
        return format!("DROP TABLE IF EXISTS {table};");
    }
    if upper.starts_with("DROP TABLE ") {
        return irreversible_down(
            "this migration drops a table; original schema/data is lost",
            sql,
        );
    }
    if upper.starts_with("ALTER TABLE ") {
        if let Some(reverse) = reverse_alter_table_add_column(&statement, target_dialect) {
            return reverse;
        }
        if upper.contains(" DROP COLUMN ") {
            return irreversible_down("this migration drops a column; column data is lost", sql);
        }
    }
    if upper.starts_with("CREATE INDEX ") || upper.starts_with("CREATE UNIQUE INDEX ") {
        if let Some(reverse) = reverse_create_index(&statement, target_dialect) {
            return reverse;
        }
    }
    if upper.starts_with("DROP INDEX ") {
        return irreversible_down(
            "this migration drops an index; original definition is not available here",
            sql,
        );
    }

    irreversible_down("uvg cannot automatically reverse this statement", sql)
}

fn executable_statement(sql: &str) -> Option<String> {
    let statement = sql
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("--"))
        .collect::<Vec<_>>()
        .join(" ");
    if statement.is_empty() {
        None
    } else {
        Some(statement)
    }
}

fn reverse_alter_table_add_column(statement: &str, target_dialect: Dialect) -> Option<String> {
    let upper = statement.to_ascii_uppercase();
    let add_idx = upper.find(" ADD COLUMN ").or_else(|| {
        if target_dialect == Dialect::Mssql {
            upper.find(" ADD ")
        } else {
            None
        }
    })?;
    let table = statement["ALTER TABLE".len()..add_idx].trim();
    let after_add = if upper[add_idx..].starts_with(" ADD COLUMN ") {
        &statement[add_idx + " ADD COLUMN ".len()..]
    } else {
        &statement[add_idx + " ADD ".len()..]
    };
    let column = first_sql_token(after_add)?;
    let column_upper = column
        .trim_matches(|c| matches!(c, '"' | '`' | '[' | ']'))
        .to_ascii_uppercase();
    if matches!(column_upper.as_str(), "CONSTRAINT" | "DEFAULT" | "CHECK") {
        return None;
    }
    Some(format!("ALTER TABLE {table} DROP COLUMN {column};"))
}

fn reverse_create_index(statement: &str, target_dialect: Dialect) -> Option<String> {
    let upper = statement.to_ascii_uppercase();
    let prefix_len = if upper.starts_with("CREATE UNIQUE INDEX ") {
        "CREATE UNIQUE INDEX ".len()
    } else {
        "CREATE INDEX ".len()
    };
    let rest = &statement[prefix_len..];
    let on_idx = rest.to_ascii_uppercase().find(" ON ")?;
    let index = rest[..on_idx].trim();
    if matches!(target_dialect, Dialect::Mysql | Dialect::Mssql) && index.contains('.') {
        return None;
    }
    let after_on = rest[on_idx + " ON ".len()..].trim();
    let table = after_on
        .split_once('(')
        .map(|(table, _)| table)
        .unwrap_or(after_on)
        .trim();
    match target_dialect {
        Dialect::Mysql | Dialect::Mssql => Some(format!("DROP INDEX {index} ON {table};")),
        Dialect::Postgres | Dialect::Sqlite => Some(format!("DROP INDEX {index};")),
    }
}

fn first_sql_token(input: &str) -> Option<&str> {
    let input = input.trim_start();
    let mut chars = input.char_indices();
    match chars.next()? {
        (_, '"') => quoted_sql_token(input, '"'),
        (_, '`') => quoted_sql_token(input, '`'),
        (_, '[') => bracketed_sql_token(input),
        _ => input
            .find(|c: char| c.is_whitespace() || c == ';' || c == ',')
            .map(|idx| &input[..idx])
            .or(Some(input)),
    }
}

fn quoted_sql_token(input: &str, quote: char) -> Option<&str> {
    let mut chars = input.char_indices().peekable();
    chars.next()?;
    while let Some((idx, ch)) = chars.next() {
        if ch == quote {
            if chars.peek().is_some_and(|(_, next)| *next == quote) {
                chars.next();
                continue;
            }
            return Some(&input[..idx + ch.len_utf8()]);
        }
    }
    None
}

fn bracketed_sql_token(input: &str) -> Option<&str> {
    let mut chars = input.char_indices().peekable();
    chars.next()?;
    while let Some((idx, ch)) = chars.next() {
        if ch == ']' {
            if chars.peek().is_some_and(|(_, next)| *next == ']') {
                chars.next();
                continue;
            }
            return Some(&input[..idx + ch.len_utf8()]);
        }
    }
    None
}

fn irreversible_down(reason: &str, original_sql: &str) -> String {
    format!(
        "-- IRREVERSIBLE: {reason}.\n-- Original SQL:\n{}",
        original_sql
            .lines()
            .map(|line| format!("--   {}", flatten_for_comment(line)))
            .collect::<Vec<_>>()
            .join("\n")
    )
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

fn yaml_inline_list(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| yaml_quote(value))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn migration_parse_check_enabled(cli: &Cli, config: &ConnectionConfig) -> bool {
    if cli.no_parse_check {
        return false;
    }
    if !db::supports_parse_check(config) {
        eprintln!(
            "uvg: migration parse-check skipped (no parse-only mode for this dialect; pass --no-parse-check before the subcommand to silence)"
        );
        return false;
    }
    true
}

async fn parse_check_migration(
    config: &ConnectionConfig,
    migration: &MigrationFile,
    direction: MigrationDirection,
) -> Result<()> {
    let sql = migration_plan_sql(migration, direction)?;
    if sql.trim().is_empty() {
        return Ok(());
    }

    let errors = db::parse_check_ddl(config, &sql).await.with_context(|| {
        format!(
            "uvg: migration {} {} parse-check could not run for {}; no migration SQL was applied and uvg_version was not changed",
            migration.revision,
            direction.label(),
            migration.path.display()
        )
    })?;
    if errors.is_empty() {
        return Ok(());
    }

    Err(anyhow!(
        "uvg: migration {} {} parse-check found {} error(s) in {} before applying; uvg_version was not changed. Fix the migration SQL and retry, or pass --no-parse-check before the subcommand to skip:\n{}",
        migration.revision,
        direction.label(),
        errors.len(),
        migration.path.display(),
        format_parse_error_lines(&errors)
    ))
}

fn migration_plan_sql(migration: &MigrationFile, direction: MigrationDirection) -> Result<String> {
    let mut sql = String::new();
    match direction {
        MigrationDirection::Up => {
            append_migration_section(&mut sql, MigrationSection::Pre, &migration.pre_sql);
            append_migration_section(&mut sql, MigrationSection::Up, &migration.up_sql);
            append_migration_section(&mut sql, MigrationSection::Post, &migration.post_sql);
        }
        MigrationDirection::Down => {
            let down_sql = checked_down_sql(migration)?;
            append_migration_section(
                &mut sql,
                MigrationSection::PostDown,
                &migration.post_down_sql,
            );
            append_migration_section(&mut sql, MigrationSection::Down, down_sql);
            append_migration_section(&mut sql, MigrationSection::PreDown, &migration.pre_down_sql);
        }
    }
    Ok(sql)
}

fn append_migration_section(sql: &mut String, section: MigrationSection, section_sql: &str) {
    let trimmed = section_sql.trim();
    if trimmed.is_empty() {
        return;
    }
    if !sql.is_empty() && !sql.ends_with('\n') {
        sql.push('\n');
    }
    if !sql.is_empty() {
        sql.push('\n');
    }
    sql.push_str("-- ");
    sql.push_str(section.label());
    sql.push('\n');
    sql.push_str(trimmed);
    sql.push('\n');
}

fn checked_down_sql(migration: &MigrationFile) -> Result<&str> {
    let down_sql = migration.down_sql.as_deref().ok_or_else(|| {
        anyhow!(
            "uvg: migration {} in {} is missing required DOWN section",
            migration.revision,
            migration.path.display()
        )
    })?;
    if down_sql
        .lines()
        .any(|line| line.trim_start().starts_with("-- IRREVERSIBLE:"))
    {
        return Err(anyhow!(
            "uvg: migration {} in {} has an irreversible DOWN section; refusing to change uvg_version",
            migration.revision,
            migration.path.display()
        ));
    }
    Ok(down_sql)
}

fn format_parse_error_lines(errors: &[db::ParseError]) -> String {
    let mut msg = String::new();
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
            e.error
        ));
    }
    msg
}

async fn apply_migration(config: &ConnectionConfig, migration: &MigrationFile) -> Result<()> {
    execute_migration_section(config, migration, MigrationSection::Pre, &migration.pre_sql).await?;
    execute_migration_section(config, migration, MigrationSection::Up, &migration.up_sql).await?;
    execute_migration_section(
        config,
        migration,
        MigrationSection::Post,
        &migration.post_sql,
    )
    .await
}

async fn apply_down_migration(config: &ConnectionConfig, migration: &MigrationFile) -> Result<()> {
    let down_sql = checked_down_sql(migration)?;
    execute_migration_section(
        config,
        migration,
        MigrationSection::PostDown,
        &migration.post_down_sql,
    )
    .await?;
    execute_migration_section(config, migration, MigrationSection::Down, down_sql).await?;
    execute_migration_section(
        config,
        migration,
        MigrationSection::PreDown,
        &migration.pre_down_sql,
    )
    .await
}

async fn execute_migration_section(
    config: &ConnectionConfig,
    migration: &MigrationFile,
    section: MigrationSection,
    sql: &str,
) -> Result<()> {
    if sql.trim().is_empty() {
        return Ok(());
    }
    let results = db::execute_ddl(config, sql, 3, |_, _, _| {}).await?;
    let applied = results.iter().take_while(|r| r.error.is_none()).count();
    if let Some(failed) = results.iter().find(|r| r.error.is_some()) {
        return Err(anyhow!(
            "uvg: migration {} failed in {} section of {} at statement {}/{}: {}\nEarlier statements in this migration may have been applied; uvg_version was not changed. Fix the target manually if needed, then retry or use `uvg stamp` after verification.\n--- SQL ---\n{}",
            migration.revision,
            section.label(),
            migration.path.display(),
            applied + 1,
            results.len(),
            failed.error.as_deref().unwrap_or(""),
            failed.sql
        ));
    }
    Ok(())
}

fn parent_description<'a>(graph: &'a MigrationGraph, parent: &str) -> &'a str {
    graph
        .migrations
        .get(parent)
        .map(|migration| migration.description.as_str())
        .unwrap_or("")
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

async fn clear_revision(config: &ConnectionConfig) -> Result<()> {
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

#[cfg(test)]
#[path = "migrations_tests.rs"]
mod tests;
