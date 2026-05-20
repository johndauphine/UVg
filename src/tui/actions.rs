use anyhow::Result;

use super::app::{App, TreeNode};
use crate::cli::{Cli, DdlOptions};
use crate::codegen::ddl_diff::compute_changes;
use crate::db;
use crate::output::Change;

pub(super) async fn generate_ddl(app: &mut App) -> Result<Vec<Change>> {
    let source_url = app.source_url.trim().to_string();
    let target_url = app.target_url.trim().to_string();

    // Parse connection configs using a helper Cli
    let source_cli = make_cli(&source_url, app.trust_cert);
    let source_config = source_cli.parse_connection()?;
    let source_dialect = source_config.dialect();

    let target_cli = make_cli(&target_url, app.trust_cert);
    let target_config = target_cli.parse_connection()?;
    let target_dialect = target_config.dialect();

    // Introspect source
    let source_schemas = if let Some(db) = source_config.database_name() {
        vec![db]
    } else {
        vec![source_dialect.default_schema().to_string()]
    };
    let options = crate::cli::GeneratorOptions::default();
    let source_schema = db::introspect_with_config(
        source_config,
        &source_schemas,
        &crate::table_filter::TableFilter::allow_all(),
        false,
        &options,
        crate::cli::DEFAULT_INTROSPECT_CONCURRENCY,
    )
    .await?;

    // Introspect target
    let target_schemas = if let Some(db) = target_config.database_name() {
        vec![db]
    } else {
        vec![target_dialect.default_schema().to_string()]
    };

    let target_schema_data = db::introspect_with_config(
        target_config,
        &target_schemas,
        &crate::table_filter::TableFilter::allow_all(),
        false,
        &options,
        crate::cli::DEFAULT_INTROSPECT_CONCURRENCY,
    )
    .await?;

    let ddl_opts = DdlOptions {
        target_dialect,
        split_tables: false,
        apply: false,
        noindexes: false,
        noconstraints: false,
        nocomments: false,
    };

    Ok(compute_changes(
        &source_schema,
        &target_schema_data,
        &ddl_opts,
    ))
}

pub(super) async fn apply_ddl(app: &mut App) -> Result<Vec<db::StmtResult>> {
    let target_url = app.target_url.trim().to_string();
    let config = make_cli(&target_url, app.trust_cert).parse_connection()?;
    let sql = collect_apply_sql(&app.nodes);
    // TUI renders its own per-statement status from the returned
    // Vec<StmtResult>; the per-statement progress reporter is for the
    // headless --apply path only (see apply_progress::print_progress).
    // TUI doesn't take a CLI flag for retries (it's an interactive
    // path); use the same default the headless --apply does.
    db::execute_ddl(&config, &sql, 3, |_, _, _| {}).await
}

/// Concatenate the SQL of every checked node into a single blob suitable
/// for `db::execute_ddl()`. `_schema` always sorts first so enums and
/// schemas exist before tables that reference them; the remaining
/// per-table nodes keep their original (topo-sorted) order.
pub(super) fn collect_apply_sql(nodes: &[TreeNode]) -> String {
    let mut ordered: Vec<&TreeNode> = nodes.iter().filter(|n| n.checked).collect();
    ordered.sort_by_key(|n| if n.name == "_schema" { 0 } else { 1 });
    let mut parts: Vec<String> = Vec::new();
    for node in ordered {
        for change in &node.changes {
            parts.push(change.sql.clone());
        }
    }
    parts.join("\n\n")
}

fn make_cli(url: &str, trust_cert: bool) -> Cli {
    Cli {
        command: None,
        profile: None,
        url: Some(url.to_string()),
        target_url: None,
        generator: "ddl".to_string(),
        target_dialect: None,
        split_tables: false,
        apply: false,
        progress: crate::apply_progress::ProgressMode::Auto,
        apply_retries: 3,
        no_parse_check: false,
        risk_classify: false,
        introspect_concurrency: crate::cli::DEFAULT_INTROSPECT_CONCURRENCY,
        tables: None,
        exclude_tables: None,
        schemas: None,
        noviews: false,
        options: None,
        outfile: None,
        out_dir: None,
        name: None,
        trust_cert,
        interactive: false,
    }
}

#[cfg(test)]
pub(super) fn count_statements(ddl: &str) -> usize {
    db::split_statements(ddl).len()
}
