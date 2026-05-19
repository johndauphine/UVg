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
