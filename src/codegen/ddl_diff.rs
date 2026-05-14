//! Schema diff engine for the DDL generator.
//!
//! Compares source and target `IntrospectedSchema` and emits ALTER statements
//! for new/dropped/modified tables and columns. Inspired by Alembic's autogenerate.

use std::collections::{HashMap, HashSet};

use crate::cli::DdlOptions;
use crate::codegen::{is_auto_increment_column, topo_sort_tables};
use crate::ddl_typemap;
use crate::dialect::Dialect;
use crate::schema::{ColumnInfo, IntrospectedSchema, TableInfo, TableType};

use super::ddl::{
    format_ddl_default_typed, generate_column_def, generate_create_table, generate_indexes,
    qualified_table_name, quote_identifier,
};

/// A single SQL statement emitted by the diff engine, tagged with the
/// table it pertains to. The tag lets the per-table output splitter route
/// the statement into the right subdirectory; non-table-scoped DDL
/// (enums, `CREATE SCHEMA`, etc.) uses `table_name: None`.
///
/// `table_schema` is normalized: default schemas (`public`, `dbo`, `main`,
/// the MySQL default database, and `""`) are stored as `""`, so the splitter
/// doesn't need dialect awareness.
// The tag fields are populated now but only read by the per-table splitter
// landing in step 2 (`src/output.rs`). Suppress the warning until then.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Change {
    pub table_schema: String,
    pub table_name: Option<String>,
    pub sql: String,
}

/// Compute the schema diff as a stream of tagged `Change` records.
///
/// Pure data — no formatting concerns. Use `render_changes()` to serialize
/// for stdout or `--outfile`, or pass directly into the per-table splitter.
pub fn compute_changes(
    source: &IntrospectedSchema,
    target: &IntrospectedSchema,
    options: &DdlOptions,
) -> Vec<Change> {
    let source_dialect = source.dialect;
    let target_dialect = options.target_dialect;

    // For MySQL, the schema is the database name. When each side has exactly
    // one schema (the common case), treat those as defaults so sourcedb.users
    // matches targetdb.users. Non-default schemas are preserved for multi-schema diffs.
    let mysql_defaults = build_mysql_defaults(source, target, source_dialect, target_dialect);

    let source_map: HashMap<(&str, &str), &TableInfo> = source
        .tables
        .iter()
        .map(|t| {
            (
                (
                    normalize_schema(&t.schema, &mysql_defaults),
                    t.name.as_str(),
                ),
                t,
            )
        })
        .collect();
    let target_map: HashMap<(&str, &str), &TableInfo> = target
        .tables
        .iter()
        .map(|t| {
            (
                (
                    normalize_schema(&t.schema, &mysql_defaults),
                    t.name.as_str(),
                ),
                t,
            )
        })
        .collect();

    let mut changes: Vec<Change> = Vec::new();

    // New tables (in source, not in target)
    let sorted_source = topo_sort_tables(&source.tables);
    for table in &sorted_source {
        if table.table_type != TableType::Table {
            continue;
        }
        let key = (
            normalize_schema(&table.schema, &mysql_defaults),
            table.name.as_str(),
        );
        if !target_map.contains_key(&key) {
            let schema = normalize_schema(&table.schema, &mysql_defaults).to_string();
            let name = table.name.clone();
            changes.push(Change {
                table_schema: schema.clone(),
                table_name: Some(name.clone()),
                sql: generate_create_table(table, source_dialect, target_dialect, options),
            });
            if !options.noindexes {
                for sql in generate_indexes(table, source_dialect, target_dialect) {
                    changes.push(Change {
                        table_schema: schema.clone(),
                        table_name: Some(name.clone()),
                        sql,
                    });
                }
            }
        }
    }

    // Modified tables (in both): compare columns
    for table in &sorted_source {
        if table.table_type != TableType::Table {
            continue;
        }
        let key = (
            normalize_schema(&table.schema, &mysql_defaults),
            table.name.as_str(),
        );
        if let Some(target_table) = target_map.get(&key) {
            let schema = normalize_schema(&table.schema, &mysql_defaults).to_string();
            let name = table.name.clone();
            for sql in diff_table_columns(table, target_table, source_dialect, target_dialect) {
                changes.push(Change {
                    table_schema: schema.clone(),
                    table_name: Some(name.clone()),
                    sql,
                });
            }
        }
    }

    // Dropped tables (in target, not in source)
    let mut dropped: Vec<(&str, &str)> = target_map
        .keys()
        .filter(|key| !source_map.contains_key(*key))
        .copied()
        .collect();
    dropped.sort();
    for (schema, name) in dropped {
        // Dropped tables come from the target's introspection — the schema
        // here is already in the target's namespace, so source_dialect is
        // immaterial for the qualification rule. Pass target_dialect for
        // both sides to mean "no source-specific suppression."
        let qname = qualified_table_name(schema, name, target_dialect, target_dialect);
        changes.push(Change {
            table_schema: schema.to_string(),
            table_name: Some(name.to_string()),
            sql: format!("-- WARNING: destructive operation\nDROP TABLE IF EXISTS {qname};"),
        });
    }

    changes
}

/// Serialize a sequence of `Change` records into the legacy single-blob
/// format that `diff_schemas()` returns. Empty input yields the
/// "no schema changes detected" sentinel so existing string-grep callers
/// (e.g. the TUI's empty-check at `src/tui/mod.rs:307`) keep working.
pub fn render_changes(
    changes: &[Change],
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> String {
    if changes.is_empty() {
        return "-- No schema changes detected.\n".to_string();
    }
    let header = format!(
        "-- Generated by uvg (diff)\n-- Source: {source_dialect}, Target: {target_dialect}\n\n"
    );
    let stmts: Vec<&str> = changes.iter().map(|c| c.sql.as_str()).collect();
    format!("{header}{}\n", stmts.join("\n\n"))
}

/// Diff two schemas and emit ALTER statements.
/// Detects new/dropped tables and new/dropped/modified columns.
pub fn diff_schemas(
    source: &IntrospectedSchema,
    target: &IntrospectedSchema,
    options: &DdlOptions,
) -> String {
    let source_dialect = source.dialect;
    let target_dialect = options.target_dialect;
    let changes = compute_changes(source, target, options);
    render_changes(&changes, source_dialect, target_dialect)
}

/// Build the set of MySQL database names to treat as defaults for diff normalization.
fn build_mysql_defaults(
    source: &IntrospectedSchema,
    target: &IntrospectedSchema,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> HashSet<String> {
    let mut defaults = HashSet::new();
    if source_dialect == Dialect::Mysql {
        let schemas: HashSet<&str> = source.tables.iter().map(|t| t.schema.as_str()).collect();
        if schemas.len() == 1 {
            defaults.insert(schemas.into_iter().next().unwrap().to_string());
        }
    }
    if target_dialect == Dialect::Mysql {
        let schemas: HashSet<&str> = target.tables.iter().map(|t| t.schema.as_str()).collect();
        if schemas.len() == 1 {
            defaults.insert(schemas.into_iter().next().unwrap().to_string());
        }
    }
    defaults
}

/// Normalize default schemas to empty string for cross-dialect comparison.
/// PG "public", MSSQL "dbo", SQLite "main" are well-known defaults.
/// MySQL database names in `mysql_defaults` are also treated as defaults.
fn normalize_schema<'a>(schema: &'a str, mysql_defaults: &HashSet<String>) -> &'a str {
    if matches!(schema, "public" | "dbo" | "main" | "") {
        return "";
    }
    if mysql_defaults.contains(schema) {
        return "";
    }
    schema
}

/// Compare columns between source and target tables, emit ALTER statements.
fn diff_table_columns(
    source: &TableInfo,
    target: &TableInfo,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> Vec<String> {
    let mut stmts = Vec::new();
    let tname = qualified_table_name(&source.schema, &source.name, source_dialect, target_dialect);

    let source_cols: HashMap<&str, &ColumnInfo> = source
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();
    let target_cols: HashMap<&str, &ColumnInfo> = target
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    // New columns
    for col in &source.columns {
        if !target_cols.contains_key(col.name.as_str()) {
            let col_def =
                generate_column_def(col, &source.constraints, source_dialect, target_dialect);
            let col_def = col_def.trim();
            let add_clause = match target_dialect {
                Dialect::Mssql => "ADD",
                _ => "ADD COLUMN",
            };
            stmts.push(format!("ALTER TABLE {tname} {add_clause} {col_def};"));
        }
    }

    // Modified columns
    for col in &source.columns {
        if let Some(target_col) = target_cols.get(col.name.as_str()) {
            let alters = diff_column(
                col,
                target_col,
                &source.schema,
                &source.name,
                source_dialect,
                target_dialect,
            );
            stmts.extend(alters);
        }
    }

    // Dropped columns
    let mut dropped: Vec<&str> = target_cols
        .keys()
        .filter(|name| !source_cols.contains_key(*name))
        .copied()
        .collect();
    dropped.sort();
    for name in dropped {
        let qcol = quote_identifier(name, target_dialect);
        stmts.push(format!(
            "-- WARNING: destructive operation\nALTER TABLE {tname} DROP COLUMN {qcol};"
        ));
    }

    stmts
}

/// Compare a single column and emit ALTER statements if different.
/// Compares type, nullability, and default values.
fn diff_column(
    source: &ColumnInfo,
    target: &ColumnInfo,
    table_schema: &str,
    table_name: &str,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> Vec<String> {
    let mut stmts = Vec::new();
    let tname = qualified_table_name(table_schema, table_name, source_dialect, target_dialect);
    let cname = quote_identifier(&source.name, target_dialect);

    let source_type = ddl_typemap::map_ddl_type(source, source_dialect, target_dialect);
    let target_type = ddl_typemap::map_ddl_type(target, target_dialect, target_dialect);

    let type_changed = source_type.sql_type != target_type.sql_type;
    let nullable_changed = source.is_nullable != target.is_nullable;

    // Compare defaults with boolean-aware normalization
    let canonical = ddl_typemap::to_canonical(source, source_dialect);
    let is_boolean = matches!(canonical, ddl_typemap::CanonicalType::Boolean);
    let source_default = source
        .column_default
        .as_deref()
        .map(|d| format_ddl_default_typed(d, source_dialect, target_dialect, is_boolean));
    let target_default = target
        .column_default
        .as_deref()
        .map(|d| format_ddl_default_typed(d, target_dialect, target_dialect, is_boolean));
    // Auto-increment columns express their default through dialect-specific
    // mechanisms (MSSQL IDENTITY → no default; PG SERIAL → nextval(...)). For
    // cross-dialect diffs, ignore the resulting default-string mismatch when
    // both sides are auto-increment. Same-dialect diffs keep the literal
    // comparison so divergent sequences (e.g. nextval('a') vs nextval('b'))
    // still surface as real drift.
    let source_auto = is_auto_increment_column(source, source_dialect);
    let target_auto = is_auto_increment_column(target, target_dialect);
    let default_changed = if source_auto && target_auto && source_dialect != target_dialect {
        false
    } else {
        source_default != target_default
    };

    if !type_changed && !nullable_changed && !default_changed {
        return stmts;
    }

    match target_dialect {
        Dialect::Postgres => {
            if type_changed {
                stmts.push(format!(
                    "ALTER TABLE {tname} ALTER COLUMN {cname} TYPE {};",
                    source_type.sql_type
                ));
            }
            if nullable_changed {
                if source.is_nullable {
                    stmts.push(format!(
                        "ALTER TABLE {tname} ALTER COLUMN {cname} DROP NOT NULL;"
                    ));
                } else {
                    stmts.push(format!(
                        "ALTER TABLE {tname} ALTER COLUMN {cname} SET NOT NULL;"
                    ));
                }
            }
            if default_changed {
                match &source_default {
                    Some(d) => stmts.push(format!(
                        "ALTER TABLE {tname} ALTER COLUMN {cname} SET DEFAULT {d};"
                    )),
                    None => stmts.push(format!(
                        "ALTER TABLE {tname} ALTER COLUMN {cname} DROP DEFAULT;"
                    )),
                }
            }
        }
        Dialect::Mysql => {
            let not_null = if !source.is_nullable { " NOT NULL" } else { "" };
            let default_clause = match &source_default {
                Some(d) => format!(" DEFAULT {d}"),
                None => String::new(),
            };
            stmts.push(format!(
                "ALTER TABLE {tname} MODIFY COLUMN {cname} {}{not_null}{default_clause};",
                source_type.sql_type
            ));
        }
        Dialect::Mssql => {
            if type_changed || nullable_changed {
                let not_null = if !source.is_nullable {
                    " NOT NULL"
                } else {
                    " NULL"
                };
                stmts.push(format!(
                    "ALTER TABLE {tname} ALTER COLUMN {cname} {}{not_null};",
                    source_type.sql_type
                ));
            }
            if default_changed {
                stmts.push(format!(
                    "-- NOTE: MSSQL requires dropping the named default constraint first.\n-- Run: SELECT name FROM sys.default_constraints WHERE parent_object_id = OBJECT_ID('{tname_raw}') AND col_name(parent_object_id, parent_column_id) = '{col_name}'\n-- Then: ALTER TABLE {tname} DROP CONSTRAINT <name>;",
                    tname_raw = table_name,
                    col_name = source.name
                ));
                if let Some(ref d) = source_default {
                    stmts.push(format!("ALTER TABLE {tname} ADD DEFAULT {d} FOR {cname};"));
                }
            }
        }
        Dialect::Sqlite => {
            stmts.push(format!(
                "-- WARNING: SQLite does not support ALTER COLUMN. Table recreation required.\n-- ALTER TABLE {tname} ALTER COLUMN {cname} TYPE {};",
                source_type.sql_type
            ));
        }
    }

    stmts
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::DdlOptions;
    use crate::testutil::{col, schema_mssql, schema_pg, table};

    fn default_options(target: Dialect) -> DdlOptions {
        DdlOptions {
            target_dialect: target,
            split_tables: false,
            apply: false,
            noindexes: false,
            noconstraints: false,
            nocomments: false,
        }
    }

    #[test]
    fn test_diff_new_table() {
        let source = schema_pg(vec![table("users")
            .column(col("id").build())
            .pk("pk", &["id"])
            .build()]);
        let target = schema_pg(vec![]);
        let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
        assert!(ddl.contains("CREATE TABLE \"users\""));
    }

    #[test]
    fn test_diff_dropped_table() {
        let source = schema_pg(vec![]);
        let target = schema_pg(vec![table("old")
            .column(col("id").build())
            .pk("pk", &["id"])
            .build()]);
        let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
        assert!(ddl.contains("DROP TABLE IF EXISTS"));
        assert!(ddl.contains("WARNING: destructive"));
    }

    #[test]
    fn test_diff_new_column() {
        let source = schema_pg(vec![table("users")
            .column(col("id").build())
            .column(col("email").udt("varchar").max_length(255).build())
            .pk("pk", &["id"])
            .build()]);
        let target = schema_pg(vec![table("users")
            .column(col("id").build())
            .pk("pk", &["id"])
            .build()]);
        let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
        assert!(ddl.contains("ADD COLUMN \"email\" VARCHAR(255) NOT NULL"));
    }

    #[test]
    fn test_diff_no_changes() {
        let schema = schema_pg(vec![table("users")
            .column(col("id").build())
            .pk("pk", &["id"])
            .build()]);
        let ddl = diff_schemas(&schema, &schema, &default_options(Dialect::Postgres));
        assert!(ddl.contains("No schema changes detected"));
    }

    #[test]
    fn test_diff_cross_dialect_default_schemas_match() {
        let source = schema_pg(vec![table("users")
            .column(col("id").build())
            .pk("pk", &["id"])
            .build()]);
        let target = schema_pg(vec![table("users")
            .schema("dbo")
            .column(col("id").build())
            .pk("pk", &["id"])
            .build()]);
        let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
        assert!(
            ddl.contains("No schema changes detected"),
            "public should match dbo: {ddl}"
        );
    }

    #[test]
    fn test_diff_mssql_identity_to_pg_serial_converges() {
        // MSSQL source: IDENTITY column with no SQL default.
        let source = schema_mssql(vec![table("Badges")
            .schema("dbo")
            .column(col("Id").udt("int").identity().build())
            .pk("PK_Badges", &["Id"])
            .build()]);
        // PG target: same logical column expressed as SERIAL (nextval(...) default).
        let target = schema_pg(vec![table("Badges")
            .column(
                col("Id")
                    .udt("int4")
                    .default_val("nextval('\"Badges_Id_seq\"'::regclass)")
                    .build(),
            )
            .pk("Badges_pkey", &["Id"])
            .build()]);
        let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
        assert!(
            ddl.contains("No schema changes detected"),
            "MSSQL IDENTITY ↔ PG SERIAL should round-trip with zero diff, got: {ddl}"
        );
    }

    #[test]
    fn test_diff_pg_serial_with_divergent_sequences_still_drifts() {
        // Same-dialect (PG→PG): two SERIAL-shaped columns pointing at different
        // sequences should NOT be silently treated as equivalent — that would
        // hide real drift from custom or renamed sequences.
        let source = schema_pg(vec![table("users")
            .column(
                col("id")
                    .udt("int4")
                    .default_val("nextval('seq_a'::regclass)")
                    .build(),
            )
            .pk("pk", &["id"])
            .build()]);
        let target = schema_pg(vec![table("users")
            .column(
                col("id")
                    .udt("int4")
                    .default_val("nextval('seq_b'::regclass)")
                    .build(),
            )
            .pk("pk", &["id"])
            .build()]);
        let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
        assert!(
            ddl.contains("SET DEFAULT") || ddl.contains("DROP DEFAULT"),
            "Same-dialect divergent sequences should drift, got: {ddl}"
        );
    }

    #[test]
    fn test_diff_multi_schema_preserved() {
        let source = schema_pg(vec![table("users")
            .schema("schema_a")
            .column(col("id").build())
            .pk("pk", &["id"])
            .build()]);
        let target = schema_pg(vec![table("users")
            .schema("schema_b")
            .column(col("id").build())
            .pk("pk", &["id"])
            .build()]);
        let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
        assert!(
            ddl.contains("CREATE TABLE") && ddl.contains("DROP TABLE"),
            "Non-default schemas should not match: {ddl}"
        );
    }

    // -------- compute_changes / render_changes tagging tests --------

    #[test]
    fn test_compute_changes_new_table_tagged() {
        let source = schema_pg(vec![table("users")
            .column(col("id").build())
            .pk("pk", &["id"])
            .build()]);
        let target = schema_pg(vec![]);
        let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

        assert_eq!(changes.len(), 1, "expected one CREATE TABLE change");
        let c = &changes[0];
        assert_eq!(c.table_schema, "", "PG public should normalize to empty");
        assert_eq!(c.table_name.as_deref(), Some("users"));
        assert!(c.sql.contains("CREATE TABLE \"users\""));
    }

    #[test]
    fn test_compute_changes_alter_column_tagged() {
        let source = schema_pg(vec![table("users")
            .column(col("id").build())
            .column(col("email").udt("varchar").max_length(255).build())
            .pk("pk", &["id"])
            .build()]);
        let target = schema_pg(vec![table("users")
            .column(col("id").build())
            .pk("pk", &["id"])
            .build()]);
        let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

        assert_eq!(changes.len(), 1, "expected one ALTER COLUMN change");
        let c = &changes[0];
        assert_eq!(c.table_schema, "");
        assert_eq!(c.table_name.as_deref(), Some("users"));
        assert!(c.sql.contains("ADD COLUMN \"email\""));
    }

    #[test]
    fn test_compute_changes_dropped_table_tagged() {
        let source = schema_pg(vec![]);
        let target = schema_pg(vec![table("old_events")
            .column(col("id").build())
            .pk("pk", &["id"])
            .build()]);
        let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

        assert_eq!(changes.len(), 1, "expected one DROP TABLE change");
        let c = &changes[0];
        assert_eq!(c.table_schema, "", "default PG schema normalizes to empty");
        assert_eq!(c.table_name.as_deref(), Some("old_events"));
        assert!(c.sql.contains("DROP TABLE IF EXISTS"));
    }

    #[test]
    fn test_compute_changes_pg_type_null_default_split() {
        // PG ALTER for type + nullability + default emits three separate
        // statements. Each must be its own Change so the per-table splitter
        // can place them in the same file without re-parsing.
        let source = schema_pg(vec![table("users")
            .column(
                col("name")
                    .udt("text")
                    .nullable()
                    .default_val("'anon'::text")
                    .build(),
            )
            .pk("pk", &["name"])
            .build()]);
        let target = schema_pg(vec![table("users")
            .column(col("name").udt("varchar").max_length(50).build()) // not-null, no default
            .pk("pk", &["name"])
            .build()]);
        let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

        assert_eq!(
            changes.len(),
            3,
            "type/null/default should split into 3 changes, got: {changes:#?}"
        );
        for c in &changes {
            assert_eq!(c.table_name.as_deref(), Some("users"));
            assert_eq!(c.table_schema, "");
            assert!(
                c.sql.starts_with("ALTER TABLE"),
                "each change is a standalone ALTER: {}",
                c.sql
            );
        }
    }

    #[test]
    fn test_compute_changes_non_default_schema_preserved() {
        let source = schema_pg(vec![table("orders")
            .schema("billing")
            .column(col("id").build())
            .pk("pk", &["id"])
            .build()]);
        let target = schema_pg(vec![]);
        let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

        assert_eq!(changes.len(), 1);
        assert_eq!(
            changes[0].table_schema, "billing",
            "non-default schema should be preserved verbatim in the tag"
        );
        assert_eq!(changes[0].table_name.as_deref(), Some("orders"));
    }

    #[test]
    fn test_compute_changes_new_table_with_index_tagged_together() {
        // A new table plus its indexes all tag to the same table — the splitter
        // writes them into one file, not separate ones.
        let source = schema_pg(vec![table("users")
            .column(col("id").build())
            .column(col("email").udt("varchar").max_length(255).build())
            .pk("pk", &["id"])
            .index("ix_users_email", &["email"], false)
            .build()]);
        let target = schema_pg(vec![]);
        let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

        assert!(
            changes.len() >= 2,
            "expected CREATE TABLE + CREATE INDEX, got {}",
            changes.len()
        );
        for c in &changes {
            assert_eq!(
                c.table_name.as_deref(),
                Some("users"),
                "every change for a new table tags to that table: {}",
                c.sql
            );
        }
        assert!(changes.iter().any(|c| c.sql.contains("CREATE TABLE")));
        assert!(changes.iter().any(|c| c.sql.contains("CREATE INDEX")));
    }

    #[test]
    fn test_render_changes_empty_returns_sentinel() {
        let out = render_changes(&[], Dialect::Postgres, Dialect::Postgres);
        assert_eq!(out, "-- No schema changes detected.\n");
    }

    #[test]
    fn test_render_changes_round_trip_with_diff_schemas() {
        // diff_schemas() must produce byte-identical output to
        // render_changes(&compute_changes(...), ...). This protects the
        // CRM matrix and all existing string-grep tests.
        let source = schema_pg(vec![
            table("users")
                .column(col("id").build())
                .column(col("email").udt("varchar").max_length(255).build())
                .pk("pk_users", &["id"])
                .build(),
            table("posts")
                .column(col("id").build())
                .pk("pk_posts", &["id"])
                .build(),
        ]);
        let target = schema_pg(vec![table("posts")
            .column(col("id").build())
            .pk("pk_posts", &["id"])
            .build()]);
        let options = default_options(Dialect::Postgres);

        let direct = diff_schemas(&source, &target, &options);
        let via_changes = render_changes(
            &compute_changes(&source, &target, &options),
            Dialect::Postgres,
            Dialect::Postgres,
        );
        assert_eq!(direct, via_changes);
    }
}
