//! Basic declarative generator tests: output format, naming, constraints, comments.

use super::super::*;
use crate::testutil::*;

fn make_simple_schema() -> IntrospectedSchema {
    schema_pg(vec![
        table("users")
            .column(col("id").build())
            .column(col("name").udt("varchar").max_length(100).build())
            .column(col("email").udt("varchar").max_length(255).build())
            .column(col("bio").udt("text").nullable().build())
            .column(
                col("created_at")
                    .udt("timestamptz")
                    .nullable()
                    .default_val("now()")
                    .build(),
            )
            .pk("users_pkey", &["id"])
            .unique("users_email_key", &["email"])
            .build(),
        table("posts")
            .column(col("id").udt("int8").build())
            .column(col("user_id").build())
            .column(col("title").udt("varchar").max_length(200).build())
            .column(col("body").udt("text").build())
            .pk("posts_pkey", &["id"])
            .fk("posts_user_id_fkey", &["user_id"], "users", &["id"])
            .build(),
    ])
}

#[test]
fn test_declarative_generator_basic() {
    let schema = make_simple_schema();
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("class Users(Base):"));
    assert!(output.contains("__tablename__ = 'users'"));
    // PrimaryKeyConstraint is NOT in __table_args__ for declarative mode
    assert!(!output.contains("PrimaryKeyConstraint"));
    assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True)"));
    assert!(output.contains("name: Mapped[str] = mapped_column(String(100), nullable=False)"));
    assert!(output.contains("email: Mapped[str] = mapped_column(String(255), nullable=False)"));
    assert!(output.contains("bio: Mapped[Optional[str]] = mapped_column(Text)"));
    assert!(output.contains("class Posts(Base):"));
    assert!(output
        .contains("user_id: Mapped[int] = mapped_column(ForeignKey('users.id'), nullable=False)"));
    assert!(output.contains("UniqueConstraint('email', name='users_email_key')"));
    // Single-column FK is now inline on mapped_column, not in __table_args__
    assert!(!output.contains("ForeignKeyConstraint"));
}

#[test]
fn test_declarative_generator_snapshot() {
    let schema = make_simple_schema();
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    insta::assert_yaml_snapshot!(output);
}

fn make_mixed_pk_schema() -> IntrospectedSchema {
    schema_pg(vec![
        table("users")
            .column(col("id").build())
            .column(col("name").udt("varchar").max_length(100).build())
            .pk("users_pkey", &["id"])
            .build(),
        table("audit_log")
            .column(col("ts").udt("timestamptz").build())
            .column(col("action").udt("text").build())
            .column(col("detail").udt("text").nullable().build())
            .build(),
    ])
}

#[test]
fn test_declarative_no_pk_fallback_to_table() {
    let schema = make_mixed_pk_schema();
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());

    // The PK table should be a class
    assert!(output.contains("class Users(Base):"));
    assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True)"));

    // The no-PK table should be a Table() assignment
    assert!(output.contains("t_audit_log = Table("));
    assert!(output.contains("'audit_log', Base.metadata,"));
    assert!(output.contains("Column('ts', DateTime(True), nullable=False)"));
    assert!(output.contains("Column('action', Text, nullable=False)"));
    assert!(output.contains("Column('detail', Text)"));

    // Should NOT generate a class for no-PK table
    assert!(!output.contains("class AuditLog(Base):"));

    // With topo sort + alphabetical tiebreak, audit_log comes before users
    let table_pos = output.find("t_audit_log = Table(").unwrap();
    let class_pos = output.find("class Users(Base):").unwrap();
    assert!(table_pos < class_pos);
}

#[test]
fn test_declarative_no_pk_fallback_snapshot() {
    let schema = make_mixed_pk_schema();
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    insta::assert_yaml_snapshot!(output);
}

#[test]
fn test_declarative_all_no_pk() {
    let schema = schema_pg(vec![table("events")
        .column(col("ts").udt("timestamptz").build())
        .column(col("data").udt("text").build())
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());

    // All no-PK: should fall back to MetaData() instead of DeclarativeBase
    assert!(output.contains("metadata = MetaData()"));
    assert!(!output.contains("class Base(DeclarativeBase):"));
    assert!(!output.contains("DeclarativeBase"));
    // Should have Table() output using standalone metadata
    assert!(output.contains("t_events = Table("));
    assert!(output.contains("'events', metadata,"));
    // Should NOT have Mapped or mapped_column imports
    assert!(!output.contains("Mapped"));
    assert!(!output.contains("mapped_column"));
    // Should have Table/Column imports
    assert!(output.contains("Column"));
    assert!(output.contains("Table"));
}

#[test]
fn test_declarative_all_no_pk_snapshot() {
    let schema = schema_pg(vec![table("events")
        .column(col("ts").udt("timestamptz").build())
        .column(col("data").udt("text").build())
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    insta::assert_yaml_snapshot!(output);
}

// --- Tier 1: Tests adapted from sqlacodegen test_generator_declarative.py ---

/// Adapted from sqlacodegen test_indexes.
#[test]
fn test_declarative_indexes() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").build())
        .column(col("number").nullable().build())
        .column(col("text").udt("varchar").nullable().build())
        .pk("simple_items_pkey", &["id"])
        .index("idx_number", &["number"], false)
        .index("idx_text", &["text"], true)
        .index("idx_text_number", &["text", "number"], false)
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("class SimpleItems(Base):"));
    assert!(output.contains("__table_args__ = ("));
    assert!(output.contains("Index('idx_number', 'number')"));
    assert!(output.contains("Index('idx_text', 'text', unique=True)"));
    assert!(output.contains("Index('idx_text_number', 'text', 'number')"));
    assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True)"));
    assert!(output.contains("number: Mapped[Optional[int]] = mapped_column(Integer)"));
    assert!(output.contains("text: Mapped[Optional[str]] = mapped_column(String)"));
}

/// Adapted from sqlacodegen test_table_kwargs.
/// Tests dict-only __table_args__ for schema.
#[test]
fn test_declarative_table_kwargs() {
    let schema = schema_pg(vec![table("simple_items")
        .schema("testschema")
        .column(col("id").build())
        .pk("simple_items_pkey", &["id"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("class SimpleItems(Base):"));
    assert!(output.contains("__table_args__ = {'schema': 'testschema'}"));
    assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True)"));
}

/// Adapted from sqlacodegen test_table_args_kwargs.
/// Tests mixed tuple+dict __table_args__.
#[test]
fn test_declarative_table_args_kwargs() {
    let schema = schema_pg(vec![table("simple_items")
        .schema("testschema")
        .column(col("id").build())
        .column(col("name").udt("varchar").nullable().build())
        .pk("simple_items_pkey", &["id"])
        .index("testidx", &["id", "name"], false)
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("__table_args__ = ("));
    assert!(output.contains("Index('testidx', 'id', 'name'),"));
    assert!(output.contains("{'schema': 'testschema'}"));
}

/// Adapted from sqlacodegen test_only_tables (all no-PK fallback).
#[test]
fn test_declarative_only_tables() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").nullable().build())
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("metadata = MetaData()"));
    assert!(output.contains("t_simple_items = Table("));
    assert!(!output.contains("class "));
    assert!(!output.contains("DeclarativeBase"));
}

/// Adapted from sqlacodegen test_column_comment (without nocomments).
#[test]
fn test_declarative_column_comment() {
    let schema = schema_pg(vec![table("simple")
        .column(col("id").comment("this is a 'comment'").build())
        .pk("simple_pkey", &["id"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True, comment=\"this is a 'comment'\")"));
}

/// Adapted from sqlacodegen test_column_comment with nocomments option.
#[test]
fn test_declarative_column_comment_nocomments() {
    let schema = schema_pg(vec![table("simple")
        .column(col("id").comment("this is a 'comment'").build())
        .pk("simple_pkey", &["id"])
        .build()]);
    let opts = GeneratorOptions {
        nocomments: true,
        ..GeneratorOptions::default()
    };
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &opts);
    assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True)"));
    assert!(!output.contains("comment="));
}

/// Adapted from sqlacodegen test_table_comment (declarative).
#[test]
fn test_declarative_table_comment() {
    let schema = schema_pg(vec![table("simple")
        .column(col("id").build())
        .pk("simple_pkey", &["id"])
        .comment("this is a 'comment'")
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("__table_args__ = {'comment': \"this is a 'comment'\"}"));
}

/// Adapted from sqlacodegen test_pascal.
#[test]
fn test_declarative_pascal() {
    // Note: sqlacodegen preserves "CustomerAPIPreference" as-is for the class name.
    // heck's to_upper_camel_case normalizes to "CustomerApiPreference".
    // This is a known difference (consecutive uppercase letters are lowercased by heck).
    let schema = schema_pg(vec![table("CustomerAPIPreference")
        .column(col("id").build())
        .pk("customer_api_preference_pkey", &["id"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("class CustomerApiPreference(Base):"));
    assert!(output.contains("__tablename__ = 'CustomerAPIPreference'"));
}

/// Adapted from sqlacodegen test_underscore.
#[test]
fn test_declarative_underscore() {
    let schema = schema_pg(vec![table("customer_api_preference")
        .column(col("id").build())
        .pk("customer_api_preference_pkey", &["id"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("class CustomerApiPreference(Base):"));
    assert!(output.contains("__tablename__ = 'customer_api_preference'"));
}

/// Adapted from sqlacodegen test_pascal_multiple_underscore.
#[test]
fn test_declarative_pascal_multiple_underscore() {
    let schema = schema_pg(vec![table("customer_API__Preference")
        .column(col("id").build())
        .pk("customer_api_preference_pkey", &["id"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // heck's UpperCamelCase handling of double underscores
    assert!(output.contains("__tablename__ = 'customer_API__Preference'"));
}

// --- Tier 2: Tests adapted from sqlacodegen test_generator_declarative.py ---

/// Adapted from sqlacodegen test_invalid_attribute_names.
#[test]
fn test_declarative_invalid_attribute_names() {
    let schema = schema_pg(vec![table("simple-items")
        .column(col("id-test").build())
        .column(col("4test").nullable().build())
        .column(col("_4test").nullable().build())
        .column(col("def").nullable().build())
        .pk("simple_items_pkey", &["id-test"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Hyphens replaced with underscores, explicit column name
    assert!(output
        .contains("id_test: Mapped[int] = mapped_column('id-test', Integer, primary_key=True)"));
    // Leading digit gets underscore prefix
    assert!(output.contains("_4test: Mapped[Optional[int]] = mapped_column('4test', Integer)"));
    // _4test collides with sanitized '4test', so gets trailing underscore
    assert!(output.contains("_4test_: Mapped[Optional[int]] = mapped_column('_4test', Integer)"));
    // Python keyword gets trailing underscore
    assert!(output.contains("def_: Mapped[Optional[int]] = mapped_column('def', Integer)"));
}

/// Adapted from sqlacodegen test_metadata_column.
#[test]
fn test_declarative_metadata_column() {
    let schema = schema_pg(vec![table("simple")
        .column(col("id").build())
        .column(col("metadata").udt("varchar").nullable().build())
        .pk("simple_pkey", &["id"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // "metadata" is reserved by SQLAlchemy
    assert!(output.contains("metadata_: Mapped[Optional[str]] = mapped_column('metadata', String)"));
}

/// Adapted from sqlacodegen test_invalid_variable_name_from_column.
#[test]
fn test_declarative_invalid_variable_name_from_column() {
    let schema = schema_pg(vec![table("simple")
        .column(col(" id ").build())
        .pk("simple_pkey", &[" id "])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Spaces trimmed and mapped, explicit column name preserved
    assert!(output.contains("id: Mapped[int] = mapped_column(' id ', Integer, primary_key=True)"));
}

/// Adapted from sqlacodegen test_constraints (declarative).
#[test]
fn test_declarative_constraints() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").build())
        .column(col("number").nullable().build())
        .pk("simple_items_pkey", &["id"])
        .check("", "number > 0")
        .unique("uq_id_number", &["id", "number"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("CheckConstraint('number > 0')"));
    assert!(output.contains("UniqueConstraint('id', 'number', name='uq_id_number')"));
    assert!(output.contains("from sqlalchemy import CheckConstraint"));
}

/// Adapted from sqlacodegen test_colname_import_conflict.
#[test]
fn test_declarative_colname_import_conflict() {
    let schema = schema_pg(vec![table("simple")
        .column(col("id").build())
        .column(col("text").udt("varchar").nullable().build())
        .column(
            col("textwithdefault")
                .udt("varchar")
                .nullable()
                .default_val("'test'")
                .build(),
        )
        .pk("simple_pkey", &["id"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // "text" conflicts with sqlalchemy.text import (from server_default)
    assert!(output.contains("text_: Mapped[Optional[str]] = mapped_column('text', String)"));
    assert!(output.contains(
        "textwithdefault: Mapped[Optional[str]] = mapped_column(String, server_default=text"
    ));
}

/// Adapted from sqlacodegen test_composite_autoincrement_pk.
#[test]
fn test_declarative_composite_autoincrement_pk() {
    let schema = schema_pg(vec![table("simple_autoincrement_items")
        .column(col("id1").autoincrement().build())
        .column(col("id2").build())
        .pk("simple_autoincrement_items_pkey", &["id1", "id2"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains(
        "id1: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)"
    ));
    assert!(output.contains("id2: Mapped[int] = mapped_column(Integer, primary_key=True)"));
}

/// Adapted from sqlacodegen test_composite_nullable_pk.
#[test]
fn test_declarative_composite_nullable_pk() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id1").build())
        .column(col("id2").nullable().build())
        .pk("simple_items_pkey", &["id1", "id2"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("id1: Mapped[int] = mapped_column(Integer, primary_key=True)"));
    // Nullable PK column should show Optional and nullable=True
    assert!(output.contains(
        "id2: Mapped[Optional[int]] = mapped_column(Integer, primary_key=True, nullable=True)"
    ));
}

/// Adapted from sqlacodegen test_pascal_underscore.
#[test]
fn test_declarative_pascal_underscore() {
    // Note: sqlacodegen preserves "CustomerAPIPreference" for "customer_API_Preference".
    // heck normalizes it to "CustomerApiPreference". Known difference.
    let schema = schema_pg(vec![table("customer_API_Preference")
        .column(col("id").build())
        .pk("customer_api_preference_pkey", &["id"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("__tablename__ = 'customer_API_Preference'"));
}

// --- Tier 3: Relationship tests adapted from sqlacodegen ---
