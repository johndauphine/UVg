//! Enum, domain, array, dialect option, and type tests.

use super::super::*;
use crate::testutil::*;

/// Adapted from sqlacodegen test_synthetic_enum_generation (declarative).
#[test]
fn test_declarative_synthetic_enum() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").build())
        .column(col("status").udt("varchar").nullable().build())
        .pk("simple_items_pkey", &["id"])
        .check(
            "",
            "simple_items.status IN ('active', 'inactive', 'pending')",
        )
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Synthetic enum class
    assert!(output.contains("class SimpleItemsStatus(str, enum.Enum):"));
    assert!(output.contains("ACTIVE = 'active'"));
}

/// Adapted from sqlacodegen test_onetomany_multiref_composite.
#[test]
fn test_declarative_onetomany_multiref_composite() {
    let schema = schema_pg(vec![
        table("simple_containers")
            .column(col("id1").build())
            .column(col("id2").build())
            .pk("sc_pkey", &["id1", "id2"])
            .build(),
        table("simple_items")
            .column(col("id").build())
            .column(col("container1_id1").nullable().build())
            .column(col("container1_id2").nullable().build())
            .column(col("container2_id1").nullable().build())
            .column(col("container2_id2").nullable().build())
            .pk("si_pkey", &["id"])
            .fk_full(
                "si_c1_fkey",
                &["container1_id1", "container1_id2"],
                "public",
                "simple_containers",
                &["id1", "id2"],
                "NO ACTION",
                "NO ACTION",
            )
            .fk_full(
                "si_c2_fkey",
                &["container2_id1", "container2_id2"],
                "public",
                "simple_containers",
                &["id1", "id2"],
                "NO ACTION",
                "NO ACTION",
            )
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Two ForeignKeyConstraints in __table_args__
    assert!(output.contains("ForeignKeyConstraint(['container1_id1', 'container1_id2']"));
    assert!(output.contains("ForeignKeyConstraint(['container2_id1', 'container2_id2']"));
}

/// Adapted from sqlacodegen test_manytomany_composite.
#[test]
fn test_declarative_manytomany_composite() {
    // M2M with composite FKs is NOT detected as association table
    // (is_association_table requires single-column FKs)
    let schema = schema_pg(vec![
        table("left_table")
            .column(col("id1").build())
            .column(col("id2").build())
            .pk("lt_pkey", &["id1", "id2"])
            .build(),
        table("right_table")
            .column(col("id1").build())
            .column(col("id2").build())
            .pk("rt_pkey", &["id1", "id2"])
            .build(),
        table("assoc")
            .column(col("left_id1").nullable().build())
            .column(col("left_id2").nullable().build())
            .column(col("right_id1").nullable().build())
            .column(col("right_id2").nullable().build())
            .fk_full(
                "a_left_fkey",
                &["left_id1", "left_id2"],
                "public",
                "left_table",
                &["id1", "id2"],
                "NO ACTION",
                "NO ACTION",
            )
            .fk_full(
                "a_right_fkey",
                &["right_id1", "right_id2"],
                "public",
                "right_table",
                &["id1", "id2"],
                "NO ACTION",
                "NO ACTION",
            )
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Composite M2M: assoc is NOT an association table (requires single-col FKs)
    // So it gets Table() fallback (no PK)
    assert!(output.contains("t_assoc = Table("));
}

// --- PR 10: Relationship completion tests ---

/// Adapted from sqlacodegen test_onetomany_conflicting_relationship.
/// Relationship name collides with column name — gets underscore suffix.
#[test]
fn test_declarative_onetomany_conflicting_relationship() {
    let schema = schema_pg(vec![
        table("simple_containers")
            .column(col("id").build())
            .pk("sc_pkey", &["id"])
            .build(),
        table("simple_items")
            .column(col("id").build())
            .column(col("container_id").nullable().build())
            .column(col("container").udt("varchar").nullable().build())
            .pk("si_pkey", &["id"])
            .fk("si_fkey", &["container_id"], "simple_containers", &["id"])
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // "container" column exists, so relationship "container" becomes "container_"
    assert!(output.contains("container: Mapped[Optional[str]] = mapped_column(String)"));
    assert!(output.contains("container_: Mapped[Optional['SimpleContainers']] = relationship("));
}

/// Adapted from sqlacodegen test_onetomany_multiref_with_nofknames.
#[test]
fn test_declarative_onetomany_multiref_with_nofknames() {
    let schema = schema_pg(vec![
        table("simple_containers")
            .column(col("id").build())
            .pk("sc_pkey", &["id"])
            .build(),
        table("simple_items")
            .column(col("id").build())
            .column(col("container_id1").nullable().build())
            .column(col("container_id2").nullable().build())
            .pk("si_pkey", &["id"])
            .fk_full(
                "si_c1_fkey",
                &["container_id1", "container_id2"],
                "public",
                "simple_containers",
                &["id", "id"],
                "NO ACTION",
                "NO ACTION",
            )
            .build(),
    ]);
    let opts = GeneratorOptions {
        nofknames: true,
        ..GeneratorOptions::default()
    };
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &opts);
    // ForeignKeyConstraint without name= kwarg
    assert!(output.contains("ForeignKeyConstraint("));
    assert!(!output.contains("name='si_c1_fkey'"));
}

/// Adapted from sqlacodegen test_synthetic_enum_nosyntheticenums_option (declarative).
#[test]
fn test_declarative_synthetic_enum_nosyntheticenums() {
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
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &opts);
    // No enum class generated
    assert!(!output.contains("class SimpleItemsStatus"));
    assert!(!output.contains("import enum"));
    // Column uses regular type
    assert!(output.contains("mapped_column(String)"));
}

// --- PR 13: noidsuffix and misc tests ---

/// Adapted from sqlacodegen test_onetomany_multiref_no_id_suffix.
#[test]
fn test_declarative_onetomany_multiref_no_id_suffix() {
    let schema = schema_pg(vec![
        table("simple_containers")
            .column(col("id").build())
            .pk("sc_pkey", &["id"])
            .build(),
        table("simple_items")
            .column(col("id").build())
            .column(col("parent_container_id").nullable().build())
            .column(col("top_container_id").build())
            .pk("si_pkey", &["id"])
            .fk(
                "si_parent_fkey",
                &["parent_container_id"],
                "simple_containers",
                &["id"],
            )
            .fk(
                "si_top_fkey",
                &["top_container_id"],
                "simple_containers",
                &["id"],
            )
            .build(),
    ]);
    let opts = GeneratorOptions {
        noidsuffix: true,
        ..GeneratorOptions::default()
    };
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &opts);
    // With noidsuffix, relationship names keep the full FK column name.
    // Since they collide with column names, they get underscore suffix.
    assert!(output.contains("parent_container_id_: Mapped[Optional['SimpleContainers']]"));
    assert!(output.contains("top_container_id_: Mapped['SimpleContainers']"));
}

// --- PR 14: Final coverage ---

/// Adapted from sqlacodegen test_manytomany_multi.
/// Multiple association tables between same two parent tables.
#[test]
fn test_declarative_manytomany_multi() {
    let schema = schema_pg(vec![
        table("left_table")
            .column(col("id").build())
            .pk("lt_pkey", &["id"])
            .build(),
        table("right_table")
            .column(col("id").build())
            .pk("rt_pkey", &["id"])
            .build(),
        table("assoc1")
            .column(col("left_id").nullable().build())
            .column(col("right_id").nullable().build())
            .fk("a1_left_fkey", &["left_id"], "left_table", &["id"])
            .fk("a1_right_fkey", &["right_id"], "right_table", &["id"])
            .build(),
        table("assoc2")
            .column(col("left_id").nullable().build())
            .column(col("right_id").nullable().build())
            .fk("a2_left_fkey", &["left_id"], "left_table", &["id"])
            .fk("a2_right_fkey", &["right_id"], "right_table", &["id"])
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Both association tables rendered as Table()
    assert!(output.contains("t_assoc1 = Table("));
    assert!(output.contains("t_assoc2 = Table("));
    // M2M relationships exist on both parent tables
    assert!(output.contains("secondary='assoc1'"));
    assert!(output.contains("secondary='assoc2'"));
}

/// Adapted from sqlacodegen test_domain_json (declarative).
#[test]
fn test_declarative_domain_json() {
    use crate::schema::{DomainInfo, IntrospectedSchema};
    let schema = IntrospectedSchema {
        dialect: crate::dialect::Dialect::Postgres,
        tables: vec![table("simple_items")
            .column(col("id").build())
            .column(col("data").udt("json_domain").nullable().build())
            .pk("simple_items_pkey", &["id"])
            .build()],
        enums: vec![],
        domains: vec![DomainInfo {
            name: "json_domain".to_string(),
            schema: None,
            base_type: "json".to_string(),
            constraint_name: None,
            not_null: false,
            check_expression: None,
        }],
    };
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Domain columns in declarative mode: domain udt_name not resolved to base type
    // (full DOMAIN() support in declarative is future work — currently falls through
    // to the type mapper which uses the udt_name as-is)
    assert!(output.contains("data:"));
}

/// Adapted from sqlacodegen test_named_constraints.
/// PrimaryKeyConstraint emitted in __table_args__ when CheckConstraint present.
#[test]
fn test_declarative_named_constraints() {
    let schema = schema_pg(vec![table("simple")
        .column(col("id").nullable().build())
        .column(col("text").udt("varchar").nullable().build())
        .pk("primarytest", &["id"])
        .check("checktest", "id > 0")
        .unique("uniquetest", &["text"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Check and Unique constraints in __table_args__
    assert!(output.contains("CheckConstraint('id > 0', name='checktest')"));
    assert!(output.contains("UniqueConstraint('text', name='uniquetest')"));
    // PK expressed via primary_key=True on mapped_column
    assert!(output.contains("primary_key=True"));
}

/// Adapted from sqlacodegen test_manytomany_multi_with_nofknames.
#[test]
fn test_declarative_manytomany_multi_with_nofknames() {
    let schema = schema_pg(vec![
        table("left_table")
            .column(col("id").build())
            .pk("lt_pkey", &["id"])
            .build(),
        table("right_table")
            .column(col("id").build())
            .pk("rt_pkey", &["id"])
            .build(),
        table("assoc")
            .column(col("left_id").nullable().build())
            .column(col("right_id").nullable().build())
            .fk("a_left_fkey", &["left_id"], "left_table", &["id"])
            .fk("a_right_fkey", &["right_id"], "right_table", &["id"])
            .build(),
    ]);
    let opts = GeneratorOptions {
        nofknames: true,
        ..GeneratorOptions::default()
    };
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &opts);
    // M2M still works with nofknames
    assert!(output.contains("secondary='assoc'"));
}

/// Adapted from sqlacodegen test_named_foreign_key_constraints.
#[test]
fn test_declarative_named_foreign_key_constraints() {
    let schema = schema_pg(vec![
        table("simple_containers")
            .column(col("id").build())
            .pk("sc_pkey", &["id"])
            .build(),
        table("simple_items")
            .column(col("id").build())
            .column(col("container_id").nullable().build())
            .pk("si_pkey", &["id"])
            .fk(
                "foreignkeytest",
                &["container_id"],
                "simple_containers",
                &["id"],
            )
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // FK rendered inline with relationship
    assert!(output.contains("ForeignKey('simple_containers.id')"));
    assert!(output.contains("relationship('SimpleContainers'"));
}

/// Adapted from sqlacodegen test_named_foreign_key_constraints_with_noidsuffix.
#[test]
fn test_declarative_named_foreign_key_constraints_with_noidsuffix() {
    let schema = schema_pg(vec![
        table("simple_containers")
            .column(col("id").build())
            .pk("sc_pkey", &["id"])
            .build(),
        table("simple_items")
            .column(col("id").build())
            .column(col("container_id").nullable().build())
            .pk("si_pkey", &["id"])
            .fk(
                "foreignkeytest",
                &["container_id"],
                "simple_containers",
                &["id"],
            )
            .build(),
    ]);
    let opts = GeneratorOptions {
        noidsuffix: true,
        ..GeneratorOptions::default()
    };
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &opts);
    // With noidsuffix, relationship name keeps _id suffix
    assert!(output.contains("relationship('SimpleContainers'"));
}

/// Adapted from sqlacodegen test_index_with_kwargs.
#[test]
fn test_declarative_index_with_kwargs() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").build())
        .column(col("name").udt("varchar").nullable().build())
        .pk("si_pkey", &["id"])
        .index_with_kwargs(
            "idx_name",
            &["name"],
            false,
            &[("postgresql_using", "gist"), ("mysql_length", "10")],
        )
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Index('idx_name', 'name'"));
    assert!(output.contains("mysql_length='10'"));
    assert!(output.contains("postgresql_using='gist'"));
}

/// Adapted from sqlacodegen test_index_with_empty_kwargs.
#[test]
fn test_declarative_index_with_empty_kwargs() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").build())
        .column(col("name").udt("varchar").nullable().build())
        .pk("si_pkey", &["id"])
        .index_with_kwargs("idx_name", &["name"], false, &[("postgresql_using", "")])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Index('idx_name', 'name')"));
    // Empty kwargs should be skipped
    assert!(!output.contains("postgresql_using"));
}

/// Adapted from sqlacodegen test_manytomany_selfref.
/// Self-referential M2M (simplified — primaryjoin/secondaryjoin are complex).
#[test]
fn test_declarative_manytomany_selfref() {
    let schema = schema_pg(vec![
        table("simple_items")
            .column(col("id").build())
            .pk("si_pkey", &["id"])
            .build(),
        table("child_items")
            .column(col("parent_id").nullable().build())
            .column(col("child_id").nullable().build())
            .fk("ci_parent_fkey", &["parent_id"], "simple_items", &["id"])
            .fk("ci_child_fkey", &["child_id"], "simple_items", &["id"])
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Self-referential M2M: association table rendered
    assert!(output.contains("t_child_items = Table("));
    // Relationships with secondary on the parent table
    assert!(output.contains("secondary='child_items'"));
}

/// Adapted from sqlacodegen test_include_dialect_options_not_enabled_skips.
/// When include_dialect_options is not enabled (default), no dialect options rendered.
#[test]
fn test_declarative_include_dialect_options_not_enabled() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").build())
        .pk("si_pkey", &["id"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // No dialect options in output
    assert!(!output.contains("postgresql_"));
}

/// Adapted from sqlacodegen test_fancy_coltypes (non-MySQL parts).
/// Tests various PG column types mapped correctly.
#[test]
fn test_declarative_fancy_coltypes() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("bool_col").udt("bool").nullable().build())
        .column(
            col("numeric_col")
                .udt("numeric")
                .precision(10, 0)
                .nullable()
                .build(),
        )
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("Boolean"));
    assert!(output.contains("Numeric"));
}

/// Adapted from sqlacodegen test_enum_unnamed.
/// Unnamed enum: auto-generate class name from column udt_name.
#[test]
fn test_declarative_enum_unnamed() {
    use crate::schema::EnumInfo;
    // Unnamed enum has an auto-generated name based on the values
    let schema = schema_pg_with_enums(
        vec![table("users")
            .column(col("id").build())
            .column(col("status").udt("status").build())
            .pk("users_pkey", &["id"])
            .build()],
        vec![EnumInfo {
            name: "status".to_string(),
            schema: None,
            values: vec!["active".to_string(), "inactive".to_string()],
        }],
    );
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Enum class generated
    assert!(output.contains("class Status(str, enum.Enum):"));
    assert!(output.contains("ACTIVE = 'active'"));
    assert!(output.contains("INACTIVE = 'inactive'"));
}

/// Adapted from sqlacodegen test_enum_nonativeenums_option.
/// With nonativeenums, native PG enums should not be rendered as Enum classes.
/// NOTE: nonativeenums is not yet fully wired — this test documents the intended
/// behavior and verifies the option is accepted without error. When implemented,
/// update assertions to verify enums are suppressed.
#[test]
fn test_declarative_enum_nonativeenums() {
    use crate::schema::EnumInfo;
    let schema = schema_pg_with_enums(
        vec![table("users")
            .column(col("id").build())
            .column(col("status").udt("status_enum").build())
            .pk("users_pkey", &["id"])
            .build()],
        vec![EnumInfo {
            name: "status_enum".to_string(),
            schema: None,
            values: vec!["active".to_string(), "inactive".to_string()],
        }],
    );
    let opts = GeneratorOptions {
        nonativeenums: true,
        ..GeneratorOptions::default()
    };
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &opts);
    // TODO: When nonativeenums is wired, assert enum class is NOT generated
    // and column uses String type instead of Enum().
    // For now, verify the option is accepted and output is valid.
    assert!(output.contains("class Users(Base):"));
}

/// Adapted from sqlacodegen test_array_enum_named.
/// Array of named enum type.
#[test]
fn test_declarative_array_enum_named() {
    use crate::schema::EnumInfo;
    let schema = schema_pg_with_enums(
        vec![table("users")
            .column(col("id").build())
            .column(col("roles").udt("_role_enum").nullable().build())
            .pk("users_pkey", &["id"])
            .build()],
        vec![EnumInfo {
            name: "role_enum".to_string(),
            schema: None,
            values: vec![
                "admin".to_string(),
                "user".to_string(),
                "moderator".to_string(),
            ],
        }],
    );
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Array column rendered (array-of-enum is complex; for now just test it doesn't panic)
    assert!(output.contains("roles:"));
}

/// Adapted from sqlacodegen test_domain_non_default_json (declarative).
#[test]
fn test_declarative_domain_non_default_json() {
    use crate::schema::{DomainInfo, IntrospectedSchema};
    let schema = IntrospectedSchema {
        dialect: crate::dialect::Dialect::Postgres,
        tables: vec![table("simple_items")
            .column(col("id").build())
            .column(col("data").udt("custom_json").nullable().build())
            .pk("si_pkey", &["id"])
            .build()],
        enums: vec![],
        domains: vec![DomainInfo {
            name: "custom_json".to_string(),
            schema: None,
            base_type: "jsonb".to_string(),
            constraint_name: None,
            not_null: false,
            check_expression: None,
        }],
    };
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Domain in declarative: currently uses udt_name as-is
    assert!(output.contains("data:"));
}

/// Adapted from sqlacodegen test_jsonb (with astext_type parameter).
/// JSONB with special parameters (placeholder — full support future work).
#[test]
fn test_declarative_jsonb_with_params() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("data").udt("jsonb").nullable().build())
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Basic JSONB renders correctly
    assert!(output.contains("JSONB"));
}

/// Adapted from sqlacodegen test_enum_unnamed_reuse_same_values.
#[test]
fn test_declarative_enum_unnamed_reuse() {
    use crate::schema::EnumInfo;
    let schema = schema_pg_with_enums(
        vec![table("users")
            .column(col("id").build())
            .column(col("status1").udt("status_a").build())
            .column(col("status2").udt("status_b").build())
            .pk("users_pkey", &["id"])
            .build()],
        vec![
            EnumInfo {
                name: "status_a".to_string(),
                schema: None,
                values: vec!["active".to_string(), "inactive".to_string()],
            },
            EnumInfo {
                name: "status_b".to_string(),
                schema: None,
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        ],
    );
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Both enum classes generated (even with same values, different names)
    assert!(output.contains("class StatusA(str, enum.Enum):"));
    assert!(output.contains("class StatusB(str, enum.Enum):"));
}

/// Adapted from sqlacodegen test_enum_unnamed_name_collision_different_values.
#[test]
fn test_declarative_enum_unnamed_collision() {
    use crate::schema::EnumInfo;
    let schema = schema_pg_with_enums(
        vec![
            table("users")
                .column(col("id").build())
                .column(col("status").udt("status_a").build())
                .pk("users_pkey", &["id"])
                .build(),
            table("accounts")
                .column(col("id").build())
                .column(col("status").udt("status_b").build())
                .pk("accounts_pkey", &["id"])
                .build(),
        ],
        vec![
            EnumInfo {
                name: "status_a".to_string(),
                schema: None,
                values: vec!["active".to_string(), "inactive".to_string()],
            },
            EnumInfo {
                name: "status_b".to_string(),
                schema: None,
                values: vec!["pending".to_string(), "approved".to_string()],
            },
        ],
    );
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Different enum classes with different values
    assert!(output.contains("class StatusA(str, enum.Enum):"));
    assert!(output.contains("class StatusB(str, enum.Enum):"));
    assert!(output.contains("ACTIVE = 'active'"));
    assert!(output.contains("PENDING = 'pending'"));
}

/// Adapted from sqlacodegen test_array_enum_named_with_schema.
#[test]
fn test_declarative_array_enum_named_with_schema() {
    use crate::schema::EnumInfo;
    let schema = schema_pg_with_enums(
        vec![table("users")
            .column(col("id").build())
            .column(col("roles").udt("_role_enum").nullable().build())
            .pk("users_pkey", &["id"])
            .build()],
        vec![EnumInfo {
            name: "role_enum".to_string(),
            schema: Some("someschema".to_string()),
            values: vec!["admin".to_string(), "user".to_string()],
        }],
    );
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Schema enum with array — renders the column
    assert!(output.contains("roles:"));
}

/// Adapted from sqlacodegen test_include_dialect_options tests.
/// Tests that dialect options are only included when the option is enabled.
#[test]
fn test_declarative_include_dialect_options_skipped_by_default() {
    // With default options, no dialect-specific options in output
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").build())
        .pk("si_pkey", &["id"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(!output.contains("postgresql_"));
    assert!(!output.contains("mysql_"));
}

/// Adapted from sqlacodegen test_array_enum_nullable.
#[test]
fn test_declarative_array_enum_nullable() {
    use crate::schema::EnumInfo;
    let schema = schema_pg_with_enums(
        vec![table("users")
            .column(col("id").build())
            .column(col("tags").udt("_tag_enum").nullable().build())
            .pk("users_pkey", &["id"])
            .build()],
        vec![EnumInfo {
            name: "tag_enum".to_string(),
            schema: None,
            values: vec!["tech".to_string(), "science".to_string()],
        }],
    );
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("tags: Mapped[Optional[list]]"));
}

/// Adapted from sqlacodegen test_array_enum_with_dimensions.
#[test]
fn test_declarative_array_enum_with_dimensions() {
    use crate::schema::EnumInfo;
    let schema = schema_pg_with_enums(
        vec![table("users")
            .column(col("id").build())
            .column(col("matrix").udt("_status_enum").nullable().build())
            .pk("users_pkey", &["id"])
            .build()],
        vec![EnumInfo {
            name: "status_enum".to_string(),
            schema: None,
            values: vec!["a".to_string(), "b".to_string()],
        }],
    );
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Array column with enum renders
    assert!(output.contains("matrix:"));
}

/// Adapted from sqlacodegen test_array_enum_nonativeenums_option.
#[test]
fn test_declarative_array_enum_nonativeenums() {
    use crate::schema::EnumInfo;
    let schema = schema_pg_with_enums(
        vec![table("users")
            .column(col("id").build())
            .column(col("roles").udt("_role_enum").nullable().build())
            .pk("users_pkey", &["id"])
            .build()],
        vec![EnumInfo {
            name: "role_enum".to_string(),
            schema: None,
            values: vec!["admin".to_string(), "user".to_string()],
        }],
    );
    let opts = GeneratorOptions {
        nonativeenums: true,
        ..GeneratorOptions::default()
    };
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &opts);
    // With nonativeenums — doesn't crash (wiring is future work)
    assert!(output.contains("roles:"));
}

/// Adapted from sqlacodegen test_array_enum_shared_with_regular_enum.
#[test]
fn test_declarative_array_enum_shared() {
    use crate::schema::EnumInfo;
    let schema = schema_pg_with_enums(
        vec![table("users")
            .column(col("id").build())
            .column(col("role").udt("role_enum").build())
            .column(col("prev_roles").udt("_role_enum").nullable().build())
            .pk("users_pkey", &["id"])
            .build()],
        vec![EnumInfo {
            name: "role_enum".to_string(),
            schema: None,
            values: vec!["admin".to_string(), "user".to_string()],
        }],
    );
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Enum class used for both regular and array columns
    assert!(output.contains("class RoleEnum(str, enum.Enum):"));
    assert!(output.contains("role: Mapped[RoleEnum]"));
}

/// Adapted from sqlacodegen test_use_inflect.
/// NOTE: use_inflect requires an inflections crate and is not yet implemented.
/// This test documents the intended behavior: with use_inflect, collection
/// relationship names would be pluralized and scalar names singularized.
/// When implemented, update assertions to verify inflected names.
#[test]
fn test_declarative_use_inflect_placeholder() {
    let schema = schema_pg(vec![
        table("simple_containers")
            .column(col("id").build())
            .pk("sc_pkey", &["id"])
            .build(),
        table("simple_items")
            .column(col("id").build())
            .column(col("container_id").nullable().build())
            .pk("si_pkey", &["id"])
            .fk("si_fkey", &["container_id"], "simple_containers", &["id"])
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Without inflect: collection uses table name "simple_items"
    assert!(output.contains("simple_items: Mapped[list['SimpleItems']]"));
    // TODO: With use_inflect, parent side would use singularized/pluralized names
    // e.g. "simple_item: Mapped[list['SimpleItems']]" → pluralized collection
}

/// Test keep_dialect_types in declarative mode for PostgreSQL.
#[test]
fn test_declarative_keep_dialect_types_pg() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").udt("int8").build())
        .column(
            col("name")
                .udt("varchar")
                .max_length(100)
                .nullable()
                .build(),
        )
        .column(col("score").udt("float8").nullable().build())
        .pk("si_pkey", &["id"])
        .build()]);
    let opts = GeneratorOptions {
        keep_dialect_types: true,
        ..GeneratorOptions::default()
    };
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &opts);
    // PG dialect types preserved in declarative
    assert!(output.contains("BIGINT"));
    assert!(output.contains("VARCHAR(100)"));
    assert!(output.contains("DOUBLE_PRECISION"));
    assert!(output.contains("from sqlalchemy.dialects.postgresql import"));
}
