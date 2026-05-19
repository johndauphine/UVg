//! Tests for the tables generator.

use super::*;
use crate::testutil::*;

fn make_simple_schema() -> IntrospectedSchema {
    schema_pg(vec![table("users")
        .column(col("id").build())
        .column(col("name").udt("varchar").max_length(100).build())
        .column(col("email").udt("text").nullable().build())
        .pk("users_pkey", &["id"])
        .build()])
}

#[test]
fn test_tables_generator_basic() {
    let schema = make_simple_schema();
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("t_users = Table("));
    assert!(output.contains("'users', metadata,"));
    assert!(output.contains("Column('id', Integer, primary_key=True)"));
    assert!(output.contains("Column('name', String(100), nullable=False)"));
    assert!(output.contains("Column('email', Text)"));
    assert!(output.contains("metadata = MetaData()"));
}

#[test]
fn test_tables_generator_snapshot() {
    let schema = make_simple_schema();
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    insta::assert_yaml_snapshot!(output);
}

fn make_no_pk_schema() -> IntrospectedSchema {
    schema_pg(vec![table("audit_log")
        .column(col("ts").udt("timestamptz").build())
        .column(col("action").udt("text").build())
        .column(col("detail").udt("text").nullable().build())
        .build()])
}

#[test]
fn test_tables_generator_no_pk() {
    let schema = make_no_pk_schema();
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());

    // Should generate a Table() without any primary_key=True
    assert!(output.contains("t_audit_log = Table("));
    assert!(!output.contains("primary_key=True"));
    assert!(!output.contains("PrimaryKeyConstraint"));
    assert!(output.contains("Column('ts', DateTime(True), nullable=False)"));
    assert!(output.contains("Column('action', Text, nullable=False)"));
    assert!(output.contains("Column('detail', Text)"));
}

#[test]
fn test_tables_generator_no_pk_snapshot() {
    let schema = make_no_pk_schema();
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    insta::assert_yaml_snapshot!(output);
}

// --- Tier 1: Tests adapted from sqlacodegen test_generator_tables.py ---

/// Adapted from sqlacodegen test_indexes.
/// Tests index rendering in Table() output.
#[test]
fn test_tables_indexes() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").nullable().build())
        .column(col("number").nullable().build())
        .column(col("text").udt("varchar").nullable().build())
        .index("ix_number", &["number"], false)
        .index("ix_text_number", &["text", "number"], true)
        .index("ix_text", &["text"], true)
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Column('id', Integer)"));
    assert!(output.contains("Column('number', Integer)"));
    assert!(output.contains("Column('text', String)"));
    assert!(output.contains("Index('ix_number', 'number')"));
    assert!(output.contains("Index('ix_text_number', 'text', 'number', unique=True)"));
    assert!(output.contains("Index('ix_text', 'text', unique=True)"));
}

/// Adapted from sqlacodegen test_constraints (UniqueConstraint portion).
/// Note: CheckConstraint is not yet supported in uvg (Tier 2).
#[test]
fn test_tables_unique_constraint() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").nullable().build())
        .column(col("number").nullable().build())
        .unique("uq_id_number", &["id", "number"])
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Column('id', Integer)"));
    assert!(output.contains("Column('number', Integer)"));
    assert!(output.contains("UniqueConstraint('id', 'number', name='uq_id_number')"));
}

/// Adapted from sqlacodegen test_table_comment.
#[test]
fn test_tables_table_comment() {
    let schema = schema_pg(vec![table("simple")
        .column(col("id").build())
        .pk("simple_pkey", &["id"])
        .comment("this is a 'comment'")
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Column('id', Integer, primary_key=True)"));
    assert!(output.contains("comment=\"this is a 'comment'\""));
}

/// Adapted from sqlacodegen test_table_name_identifiers.
/// Tests that non-identifier table names are sanitized in variable names.
#[test]
fn test_tables_table_name_identifiers() {
    let schema = schema_pg(vec![table("simple-items table")
        .column(col("id").build())
        .pk("simple_items_table_pkey", &["id"])
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Variable name should sanitize non-identifier chars
    assert!(output.contains("t_simple_items_table = Table("));
    // But the table name string should preserve original
    assert!(output.contains("'simple-items table', metadata,"));
}

/// Adapted from sqlacodegen test_option_noindexes.
#[test]
fn test_tables_option_noindexes() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("number").nullable().build())
        .unique("uq_number", &["number"])
        .index("idx_number", &["number"], false)
        .build()]);
    let opts = GeneratorOptions {
        noindexes: true,
        ..GeneratorOptions::default()
    };
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &opts);
    assert!(output.contains("Column('number', Integer)"));
    assert!(output.contains("UniqueConstraint('number', name='uq_number')"));
    // Index should be suppressed
    assert!(!output.contains("Index("));
}

/// Adapted from sqlacodegen test_option_noconstraints.
#[test]
fn test_tables_option_noconstraints() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("number").nullable().build())
        .unique("uq_number", &["number"])
        .index("idx_number", &["number"], false)
        .build()]);
    let opts = GeneratorOptions {
        noconstraints: true,
        ..GeneratorOptions::default()
    };
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &opts);
    assert!(output.contains("Column('number', Integer)"));
    // Constraint should be suppressed
    assert!(!output.contains("UniqueConstraint("));
    // Index should still be present
    assert!(output.contains("Index('idx_number', 'number')"));
}

/// Adapted from sqlacodegen test_option_nocomments.
#[test]
fn test_tables_option_nocomments() {
    let schema = schema_pg(vec![table("simple")
        .column(col("id").comment("pk column comment").build())
        .pk("simple_pkey", &["id"])
        .comment("this is a 'comment'")
        .build()]);
    let opts = GeneratorOptions {
        nocomments: true,
        ..GeneratorOptions::default()
    };
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &opts);
    assert!(output.contains("Column('id', Integer, primary_key=True)"));
    // Comments should be suppressed
    assert!(!output.contains("comment="));
}

/// Adapted from sqlacodegen test_schema.
#[test]
fn test_tables_schema() {
    let schema = schema_pg(vec![table("simple_items")
        .schema("testschema")
        .column(col("name").udt("varchar").nullable().build())
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("t_simple_items = Table("));
    assert!(output.contains("Column('name', String)"));
    assert!(output.contains("schema='testschema'"));
}

/// Adapted from sqlacodegen test_pk_default.
#[test]
fn test_tables_pk_default() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").default_val("uuid_generate_v4()").build())
        .pk("simple_items_pkey", &["id"])
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains(
        "Column('id', Integer, primary_key=True, server_default=text('uuid_generate_v4()'))"
    ));
}

/// Adapted from sqlacodegen test_identity_column.
#[test]
fn test_tables_identity_column() {
    use crate::schema::IdentityInfo;
    let schema = schema_pg(vec![table("simple_items")
        .column(
            col("id")
                .identity_info(IdentityInfo {
                    start: 1,
                    increment: 2,
                    min_value: 1,
                    max_value: 2147483647,
                    cycle: false,
                    cache: 1,
                })
                .build(),
        )
        .pk("simple_items_pkey", &["id"])
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Identity("));
    assert!(output.contains("start=1"));
    assert!(output.contains("increment=2"));
    assert!(output.contains("primary_key=True"));
}

// --- Tier 2: Tests adapted from sqlacodegen test_generator_tables.py ---

/// Adapted from sqlacodegen test_multiline_column_comment.
#[test]
fn test_tables_multiline_column_comment() {
    let schema = schema_pg(vec![table("simple_items")
        .column(
            col("id")
                .nullable()
                .comment("This\nis a multi-line\ncomment")
                .build(),
        )
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("comment='This\\nis a multi-line\\ncomment'"));
}

/// Adapted from sqlacodegen test_multiline_table_comment.
#[test]
fn test_tables_multiline_table_comment() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").nullable().build())
        .comment("This\nis a multi-line\ncomment")
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("comment='This\\nis a multi-line\\ncomment'"));
}

/// Adapted from sqlacodegen test_server_default_multiline.
#[test]
fn test_tables_server_default_multiline() {
    let schema = schema_pg(vec![table("simple_items")
        .column(
            col("id")
                .default_val("/*Comment*/\n/*Next line*/\nsomething()")
                .build(),
        )
        .pk("simple_items_pkey", &["id"])
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("server_default=text('/*Comment*/\\n/*Next line*/\\nsomething()')"));
}

/// Adapted from sqlacodegen test_server_default_colon.
#[test]
fn test_tables_server_default_colon() {
    let schema = schema_pg(vec![table("simple_items")
        .column(
            col("problem")
                .udt("varchar")
                .nullable()
                .default_val("':001'")
                .build(),
        )
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("server_default=text(\"':001'\")"));
}

/// Adapted from sqlacodegen test_null_type.
#[test]
fn test_tables_null_type() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("problem").udt("").nullable().build())
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Column('problem', NullType)"));
    assert!(output.contains("from sqlalchemy.sql.sqltypes import NullType"));
}

/// Adapted from sqlacodegen test_foreign_key_options.
#[test]
fn test_tables_foreign_key_options() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("name").udt("varchar").nullable().build())
        .fk_full(
            "simple_items_name_fkey",
            &["name"],
            "public",
            "simple_items",
            &["name"],
            "CASCADE",
            "CASCADE",
        )
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("ondelete='CASCADE'"));
    assert!(output.contains("onupdate='CASCADE'"));
}

/// Adapted from sqlacodegen test_identity_column_decimal_values.
/// MSSQL reflects Identity parameters as Decimal; uvg stores them as i64.
/// The output should be identical to test_identity_column.
#[test]
fn test_tables_identity_column_decimal_values() {
    use crate::schema::IdentityInfo;
    let schema = schema_mssql(vec![table("simple_items")
        .schema("dbo")
        .column(
            col("id")
                .identity_info(IdentityInfo {
                    start: 1,
                    increment: 2,
                    min_value: 1,
                    max_value: 2147483647,
                    cycle: false,
                    cache: 1,
                })
                .build(),
        )
        .pk("simple_items_pkey", &["id"])
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Identity(start=1, increment=2)"));
    assert!(output.contains("primary_key=True"));
}

// --- Tier 4: Enum tests ---

/// Adapted from sqlacodegen test_enum_shared_values (tables).
#[test]
fn test_tables_enum_shared_values() {
    use crate::schema::EnumInfo;
    let schema = schema_pg_with_enums(
        vec![
            table("accounts")
                .column(col("id").build())
                .column(col("status").udt("status_enum").nullable().build())
                .pk("accounts_pkey", &["id"])
                .build(),
            table("users")
                .column(col("id").build())
                .column(col("status").udt("status_enum").nullable().build())
                .pk("users_pkey", &["id"])
                .build(),
        ],
        vec![EnumInfo {
            name: "status_enum".to_string(),
            schema: None,
            values: vec![
                "active".to_string(),
                "inactive".to_string(),
                "pending".to_string(),
            ],
        }],
    );
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Enum class generated
    assert!(output.contains("class StatusEnum(str, enum.Enum):"));
    assert!(output.contains("ACTIVE = 'active'"));
    assert!(output.contains("INACTIVE = 'inactive'"));
    assert!(output.contains("PENDING = 'pending'"));
    // Enum used in both tables
    assert!(output.contains("Enum(StatusEnum, values_callable=lambda cls: [member.value for member in cls], name='status_enum')"));
    // import enum
    assert!(output.contains("import enum"));
}

/// Adapted from sqlacodegen test_synthetic_enum_generation.
#[test]
fn test_tables_synthetic_enum_generation() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").build())
        .column(col("status").udt("varchar").nullable().build())
        .pk("simple_items_pkey", &["id"])
        .check(
            "",
            "simple_items.status IN ('active', 'inactive', 'pending')",
        )
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());

    // Synthetic enum class generated
    assert!(output.contains("class SimpleItemsStatus(str, enum.Enum):"));
    assert!(output.contains("ACTIVE = 'active'"));
    assert!(output.contains("INACTIVE = 'inactive'"));
    assert!(output.contains("PENDING = 'pending'"));
    // Column uses Enum type (without name= since it's synthetic)
    assert!(output.contains(
        "Enum(SimpleItemsStatus, values_callable=lambda cls: [member.value for member in cls])"
    ));
    // CheckConstraint preserved
    assert!(output.contains("CheckConstraint("));
}

/// Adapted from sqlacodegen test_enum_named_with_schema (tables).
#[test]
fn test_tables_enum_named_with_schema() {
    use crate::schema::EnumInfo;
    let schema = schema_pg_with_enums(
        vec![table("simple_items")
            .column(col("id").build())
            .column(col("status").udt("status_enum").nullable().build())
            .pk("simple_items_pkey", &["id"])
            .build()],
        vec![EnumInfo {
            name: "status_enum".to_string(),
            schema: Some("someschema".to_string()),
            values: vec!["active".to_string(), "inactive".to_string()],
        }],
    );
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Enum() includes schema kwarg
    assert!(output.contains("schema='someschema'"));
    assert!(output.contains("name='status_enum'"));
}

// --- PR 7: Sequences and computed columns ---

/// Adapted from sqlacodegen test_postgresql_sequence_standard_name.
/// Standard sequence naming is stripped (no Sequence() in output).
#[test]
fn test_tables_postgresql_sequence_standard_name() {
    let schema = schema_pg(vec![table("simple_items")
        .column(
            col("id")
                .default_val("nextval('simple_items_id_seq'::regclass)")
                .build(),
        )
        .pk("simple_items_pkey", &["id"])
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Standard sequence stripped — just primary_key=True, no server_default
    assert!(output.contains("Column('id', Integer, primary_key=True)"));
    assert!(!output.contains("Sequence"));
    assert!(!output.contains("server_default"));
}

/// Adapted from sqlacodegen test_postgresql_sequence_nonstandard_name.
/// Non-standard sequence name preserved as Sequence().
#[test]
fn test_tables_postgresql_sequence_nonstandard_name() {
    let schema = schema_pg(vec![table("simple_items")
        .column(
            col("id")
                .default_val("nextval('test_seq'::regclass)")
                .build(),
        )
        .pk("simple_items_pkey", &["id"])
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Sequence('test_seq')"));
    assert!(output.contains("primary_key=True"));
    assert!(output.contains("from sqlalchemy import"));
    assert!(output.contains("Sequence"));
}

/// Adapted from sqlacodegen test_computed_column (persisted=None).
#[test]
fn test_tables_computed_column() {
    let schema = schema_pg(vec![table("computed")
        .column(col("id").build())
        .column(col("computed").nullable().default_val("1 + 2").build())
        .pk("computed_pkey", &["id"])
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // For now, computed columns render as server_default (full Computed() support is future work)
    assert!(output.contains("Column('id', Integer, primary_key=True)"));
    assert!(output.contains("server_default=text('1 + 2')"));
}

// --- PR 8: Misc feature tests ---

/// Adapted from sqlacodegen test_column_adaptation.
/// PG dialect types should map to generic SA types via udt_name.
#[test]
fn test_tables_column_adaptation() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").udt("int8").nullable().build())
        .column(col("length").udt("float8").nullable().build())
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Column('id', BigInteger)"));
    assert!(output.contains("Column('length', Double)"));
    assert!(output.contains("BigInteger"));
    assert!(output.contains("Double"));
}

/// Adapted from sqlacodegen test_jsonb_default.
/// Plain JSONB column (no parameters).
#[test]
fn test_tables_jsonb_default() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("jsonb").udt("jsonb").nullable().build())
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Column('jsonb', JSONB)"));
    assert!(output.contains("from sqlalchemy.dialects.postgresql import JSONB"));
}

/// Adapted from sqlacodegen test_json_default.
/// Plain JSON column.
#[test]
fn test_tables_json_default() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("json").udt("json").nullable().build())
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Column('json', JSON)"));
    assert!(output.contains("from sqlalchemy.dialects.postgresql import JSON"));
}

/// Adapted from sqlacodegen test_arrays (basic).
/// Integer array column.
#[test]
fn test_tables_arrays() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("int_array").udt("_int4").nullable().build())
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Column('int_array', ARRAY(Integer))"));
    assert!(output.contains("from sqlalchemy import ARRAY"));
}

/// Adapted from sqlacodegen test_check_constraint_preserved.
/// Check constraint preserved in output (not consumed by synthetic enum).
#[test]
fn test_tables_check_constraint_preserved() {
    let schema = schema_pg(vec![table("simple_items")
        .column(
            col("status")
                .udt("varchar")
                .max_length(255)
                .nullable()
                .build(),
        )
        .check("", "simple_items.status IN ('A', 'B', 'C')")
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Check constraint is preserved
    assert!(output.contains("CheckConstraint("));
    // Synthetic enum is also generated
    assert!(output.contains("class SimpleItemsStatus(str, enum.Enum):"));
}

/// Adapted from sqlacodegen test_synthetic_enum_nosyntheticenums_option.
#[test]
fn test_tables_synthetic_enum_nosyntheticenums() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").build())
        .column(col("status").udt("varchar").nullable().build())
        .pk("simple_items_pkey", &["id"])
        .check("", "simple_items.status IN ('active', 'inactive')")
        .build()]);
    let opts = GeneratorOptions {
        nosyntheticenums: true,
        ..GeneratorOptions::default()
    };
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &opts);
    // No enum class generated
    assert!(!output.contains("class SimpleItemsStatus"));
    assert!(!output.contains("import enum"));
    // Check constraint still preserved
    assert!(output.contains("CheckConstraint("));
    // Column uses regular type
    assert!(output.contains("Column('status', String)"));
}

/// Adapted from sqlacodegen test_synthetic_enum_shared_values.
#[test]
fn test_tables_synthetic_enum_shared_values() {
    let schema = schema_pg(vec![
        table("table1")
            .column(col("id").build())
            .column(col("status").udt("varchar").nullable().build())
            .pk("table1_pkey", &["id"])
            .check("", "table1.status IN ('active', 'inactive')")
            .build(),
        table("table2")
            .column(col("id").build())
            .column(col("status").udt("varchar").nullable().build())
            .pk("table2_pkey", &["id"])
            .check("", "table2.status IN ('active', 'inactive')")
            .build(),
    ]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Each table gets its own enum class
    assert!(output.contains("class Table1Status(str, enum.Enum):"));
    assert!(output.contains("class Table2Status(str, enum.Enum):"));
}

// --- PR 12: Boolean detection and domain tests ---

/// Adapted from sqlacodegen test_boolean_detection.
#[test]
fn test_tables_boolean_detection() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("bool1").nullable().build())
        .column(col("bool2").udt("int2").nullable().build())
        .check("", "simple_items.bool1 IN (0, 1)")
        .check("", "simple_items.bool2 IN (0, 1)")
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Column('bool1', Boolean)"));
    assert!(output.contains("Column('bool2', Boolean)"));
    // Check constraints suppressed (boolean detection consumed them)
    assert!(!output.contains("CheckConstraint"));
    assert!(output.contains("from sqlalchemy import Boolean"));
}

/// Adapted from sqlacodegen test_schema_boolean.
#[test]
fn test_tables_schema_boolean() {
    let schema = schema_pg(vec![table("simple_items")
        .schema("testschema")
        .column(col("bool1").nullable().build())
        .check("", "testschema.simple_items.bool1 IN (0, 1)")
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Column('bool1', Boolean)"));
    assert!(output.contains("schema='testschema'"));
}

/// Adapted from sqlacodegen test_domain_text.
#[test]
fn test_tables_domain_text() {
    use crate::schema::{DomainInfo, IntrospectedSchema};
    let schema = IntrospectedSchema {
        dialect: crate::dialect::Dialect::Postgres,
        tables: vec![table("simple_items")
            .column(col("postal_code").udt("us_postal_code").build())
            .build()],
        enums: vec![],
        domains: vec![DomainInfo {
            name: "us_postal_code".to_string(),
            schema: None,
            base_type: "text".to_string(),
            constraint_name: Some("valid_us_postal_code".to_string()),
            not_null: false,
            check_expression: Some("VALUE ~ '^\\d{5}$'".to_string()),
        }],
    };
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("DOMAIN("));
    assert!(output.contains("'us_postal_code'"));
    assert!(output.contains("Text()"));
    assert!(output.contains("constraint_name='valid_us_postal_code'"));
    assert!(output.contains("from sqlalchemy.dialects.postgresql import DOMAIN"));
}

/// Adapted from sqlacodegen test_domain_int.
#[test]
fn test_tables_domain_int() {
    use crate::schema::{DomainInfo, IntrospectedSchema};
    let schema = IntrospectedSchema {
        dialect: crate::dialect::Dialect::Postgres,
        tables: vec![table("simple_items")
            .column(col("n").udt("positive_int").build())
            .build()],
        enums: vec![],
        domains: vec![DomainInfo {
            name: "positive_int".to_string(),
            schema: None,
            base_type: "int4".to_string(),
            constraint_name: Some("positive".to_string()),
            not_null: false,
            check_expression: Some("VALUE > 0".to_string()),
        }],
    };
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("DOMAIN("));
    assert!(output.contains("'positive_int'"));
    assert!(output.contains("Integer()"));
    assert!(output.contains("constraint_name='positive'"));
}

// --- PR 13: Sequence with schema ---

/// Adapted from sqlacodegen test_postgresql_sequence_with_schema.
#[test]
fn test_tables_postgresql_sequence_with_schema() {
    let schema = schema_pg(vec![table("simple_items")
        .schema("testschema")
        .column(
            col("id")
                .default_val("nextval('testschema.test_seq'::regclass)")
                .build(),
        )
        .pk("simple_items_pkey", &["id"])
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Schema-qualified sequence: split into name + schema kwarg
    assert!(output.contains("'test_seq'"));
    assert!(output.contains("schema='testschema'"));
    assert!(!output.contains("'testschema.test_seq'"));
}

// --- keep_dialect_types tests ---

/// Test keep_dialect_types for PostgreSQL tables generator.
/// Types should stay as PG-specific instead of adapting to generic.
#[test]
fn test_tables_keep_dialect_types_pg() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").udt("int8").nullable().build())
        .column(
            col("name")
                .udt("varchar")
                .max_length(100)
                .nullable()
                .build(),
        )
        .column(col("score").udt("float8").nullable().build())
        .column(col("data").udt("jsonb").nullable().build())
        .build()]);
    let opts = GeneratorOptions {
        keep_dialect_types: true,
        ..GeneratorOptions::default()
    };
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &opts);
    // PG dialect types preserved
    assert!(output.contains("BIGINT"));
    assert!(output.contains("VARCHAR(100)"));
    assert!(output.contains("DOUBLE_PRECISION"));
    assert!(output.contains("JSONB"));
    assert!(output.contains("from sqlalchemy.dialects.postgresql import"));
}

/// Test default (no keep_dialect_types) — types adapted to generic.
#[test]
fn test_tables_no_keep_dialect_types_pg() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").udt("int8").nullable().build())
        .column(col("score").udt("float8").nullable().build())
        .build()]);
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Generic types
    assert!(output.contains("BigInteger"));
    assert!(output.contains("Double"));
    assert!(!output.contains("BIGINT"));
    assert!(!output.contains("DOUBLE_PRECISION"));
}

/// Test keep_dialect_types for MSSQL tables generator.
#[test]
fn test_tables_keep_dialect_types_mssql() {
    let schema = schema_mssql(vec![table("simple_items")
        .schema("dbo")
        .column(col("id").udt("int").nullable().build())
        .column(col("small").udt("smallint").nullable().build())
        .column(col("guid").udt("uniqueidentifier").nullable().build())
        .build()]);
    let opts = GeneratorOptions {
        keep_dialect_types: true,
        ..GeneratorOptions::default()
    };
    let gen = TablesGenerator;
    let output = gen.generate(&schema, &opts);
    // MSSQL dialect types preserved
    assert!(output.contains("INTEGER"));
    assert!(output.contains("SMALLINT"));
    assert!(output.contains("UNIQUEIDENTIFIER"));
    assert!(output.contains("from sqlalchemy.dialects.mssql import"));
}
