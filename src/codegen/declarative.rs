use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::{
    escape_python_string, format_python_string_literal, format_server_default, has_primary_key,
    is_primary_key_column, is_serial_default, is_unique_constraint_index,
    quote_constraint_columns, topo_sort_tables, Generator,
};
use crate::dialect::Dialect;
use crate::naming::{table_to_class_name, table_to_variable_name};
use crate::schema::{ConstraintType, IntrospectedSchema, TableInfo};
use crate::typemap::map_column_type;

pub struct DeclarativeGenerator;

impl Generator for DeclarativeGenerator {
    fn generate(&self, schema: &IntrospectedSchema, options: &GeneratorOptions) -> String {
        let mut imports = ImportCollector::new();
        let mut blocks: Vec<String> = Vec::new();
        let mut needs_optional = false;
        let mut needs_datetime = false;
        let mut needs_decimal = false;
        let mut needs_uuid = false;

        let has_any_pk = schema.tables.iter().any(|t| has_primary_key(&t.constraints));
        let has_any_no_pk = schema.tables.iter().any(|t| !has_primary_key(&t.constraints));

        if has_any_pk {
            imports.add("sqlalchemy.orm", "DeclarativeBase");
            imports.add("sqlalchemy.orm", "Mapped");
            imports.add("sqlalchemy.orm", "mapped_column");
        } else {
            imports.add("sqlalchemy", "MetaData");
        }

        if has_any_no_pk {
            imports.add("sqlalchemy", "Table");
            imports.add("sqlalchemy", "Column");
        }

        let metadata_ref = if has_any_pk { "Base.metadata" } else { "metadata" };

        let sorted_tables = topo_sort_tables(&schema.tables);
        for table in sorted_tables {
            if has_primary_key(&table.constraints) {
                let (block, meta) = generate_class(table, &mut imports, options, schema.dialect);
                if meta.needs_optional {
                    needs_optional = true;
                }
                if meta.needs_datetime {
                    needs_datetime = true;
                }
                if meta.needs_decimal {
                    needs_decimal = true;
                }
                if meta.needs_uuid {
                    needs_uuid = true;
                }
                blocks.push(block);
            } else {
                let block =
                    generate_table_fallback(table, &mut imports, options, schema.dialect, metadata_ref);
                blocks.push(block);
            }
        }

        if needs_optional {
            imports.add("typing", "Optional");
        }
        if needs_datetime {
            imports.add_bare("datetime");
        }
        if needs_decimal {
            imports.add_bare("decimal");
        }
        if needs_uuid {
            imports.add_bare("uuid");
        }

        let mut output = imports.render();

        if has_any_pk {
            output.push_str("\n\nclass Base(DeclarativeBase):\n    pass");
        } else {
            output.push_str("\n\nmetadata = MetaData()");
        }

        for block in blocks {
            output.push_str("\n\n\n");
            output.push_str(&block);
        }

        output.push('\n');
        output
    }
}

struct ClassMeta {
    needs_optional: bool,
    needs_datetime: bool,
    needs_decimal: bool,
    needs_uuid: bool,
}

fn generate_class(
    table: &TableInfo,
    imports: &mut ImportCollector,
    options: &GeneratorOptions,
    dialect: Dialect,
) -> (String, ClassMeta) {
    let class_name = table_to_class_name(&table.name);
    let mut lines: Vec<String> = Vec::new();
    let mut meta = ClassMeta {
        needs_optional: false,
        needs_datetime: false,
        needs_decimal: false,
        needs_uuid: false,
    };

    lines.push(format!("class {class_name}(Base):"));
    lines.push(format!("    __tablename__ = '{}'", table.name));

    // Table-level args (multi-column unique constraints, indexes, comments, schema)
    let table_args = build_table_args(table, imports, options, dialect);
    if let Some(args_str) = table_args {
        if args_str.starts_with('{') {
            // Dict-only form
            lines.push(format!("    __table_args__ = {args_str}"));
        } else {
            // Tuple form
            lines.push(format!("    __table_args__ = (\n{args_str}\n    )"));
        }
    }

    // Blank line before columns
    lines.push(String::new());

    // Build column lines
    struct ColLine {
        is_pk: bool,
        is_nullable: bool,
        line: String,
    }
    let mut col_lines: Vec<ColLine> = Vec::new();

    for col in &table.columns {
        let mapped = map_column_type(col, dialect);
        imports.add(&mapped.import_module, &mapped.import_name);
        if let Some((ref elem_mod, ref elem_name)) = mapped.element_import {
            imports.add(elem_mod, elem_name);
        }

        // Track stdlib import needs
        if mapped.python_type.starts_with("datetime.") {
            meta.needs_datetime = true;
        }
        if mapped.python_type.starts_with("decimal.") {
            meta.needs_decimal = true;
        }
        if mapped.python_type.starts_with("uuid.") {
            meta.needs_uuid = true;
        }

        let is_pk = is_primary_key_column(&col.name, &table.constraints);

        // Python type annotation
        let python_type = &mapped.python_type;
        let type_annotation = if col.is_nullable && !is_pk {
            meta.needs_optional = true;
            format!("Optional[{python_type}]")
        } else {
            python_type.clone()
        };

        // mapped_column arguments
        let mut mc_args: Vec<String> = Vec::new();

        // Type argument
        mc_args.push(mapped.sa_type.clone());

        // Identity — dialect-aware output
        if let Some(ref identity) = col.identity {
            imports.add("sqlalchemy", "Identity");
            match dialect {
                Dialect::Postgres => {
                    mc_args.push(format!(
                        "Identity(start={}, increment={}, minvalue={}, maxvalue={}, cycle=False, cache={})",
                        identity.start, identity.increment, identity.min_value, identity.max_value, identity.cache
                    ));
                }
                Dialect::Mssql => {
                    mc_args.push(format!(
                        "Identity(start={}, increment={})",
                        identity.start, identity.increment
                    ));
                }
            }
        }

        // nullable=False on non-nullable non-PK columns
        if !col.is_nullable && !is_pk {
            mc_args.push("nullable=False".to_string());
        }

        // Primary key
        if is_pk {
            mc_args.push("primary_key=True".to_string());
        }

        // Server default
        if let Some(ref default) = col.column_default {
            if !is_serial_default(default, dialect) {
                imports.add("sqlalchemy", "text");
                let formatted = format_server_default(default, dialect);
                mc_args.push(format!("server_default={formatted}"));
            }
        }

        // Comment
        if !options.nocomments {
            if let Some(ref comment) = col.comment {
                mc_args.push(format!("comment={}", format_python_string_literal(comment)));
            }
        }

        let mc_str = mc_args.join(", ");
        let line = format!(
            "    {}: Mapped[{type_annotation}] = mapped_column({mc_str})",
            col.name
        );
        col_lines.push(ColLine {
            is_pk,
            is_nullable: col.is_nullable,
            line,
        });
    }

    // Sort columns: PK first, then non-nullable non-PK, then nullable — all preserving ordinal order
    let pk_cols: Vec<&ColLine> = col_lines.iter().filter(|c| c.is_pk).collect();
    let non_nullable: Vec<&ColLine> = col_lines
        .iter()
        .filter(|c| !c.is_pk && !c.is_nullable)
        .collect();
    let nullable: Vec<&ColLine> = col_lines
        .iter()
        .filter(|c| !c.is_pk && c.is_nullable)
        .collect();

    for col_line in pk_cols.iter().chain(non_nullable.iter()).chain(nullable.iter()) {
        lines.push(col_line.line.clone());
    }

    (lines.join("\n"), meta)
}

fn build_table_args(
    table: &TableInfo,
    imports: &mut ImportCollector,
    options: &GeneratorOptions,
    dialect: Dialect,
) -> Option<String> {
    let mut positional_args: Vec<String> = Vec::new();
    let mut kwargs: Vec<String> = Vec::new();

    // Foreign key constraints
    if !options.noconstraints {
        for constraint in &table.constraints {
            if constraint.constraint_type == ConstraintType::ForeignKey {
                if let Some(ref fk) = constraint.foreign_key {
                    imports.add("sqlalchemy", "ForeignKeyConstraint");
                    let local_cols: Vec<String> =
                        constraint.columns.iter().map(|c| format!("'{c}'")).collect();
                    let ref_cols: Vec<String> = fk
                        .ref_columns
                        .iter()
                        .map(|c| format!("'{}.{c}'", fk.ref_table))
                        .collect();
                    positional_args.push(format!(
                        "ForeignKeyConstraint([{}], [{}], name='{}')",
                        local_cols.join(", "),
                        ref_cols.join(", "),
                        constraint.name
                    ));
                }
            }
        }
    }

    // Note: PrimaryKeyConstraint is NOT emitted in declarative __table_args__
    // because it's already expressed via primary_key=True on mapped_column().

    // Unique constraints (all, not just multi-column)
    if !options.noconstraints {
        for constraint in &table.constraints {
            if constraint.constraint_type == ConstraintType::Unique {
                imports.add("sqlalchemy", "UniqueConstraint");
                let cols = quote_constraint_columns(&constraint.columns);
                positional_args.push(format!(
                    "UniqueConstraint({}, name='{}')",
                    cols.join(", "),
                    constraint.name
                ));
            }
        }
    }

    // Indexes
    if !options.noindexes {
        for index in &table.indexes {
            if is_unique_constraint_index(index, &table.constraints) {
                continue;
            }
            imports.add("sqlalchemy", "Index");
            let cols = quote_constraint_columns(&index.columns);
            let unique_str = if index.is_unique { ", unique=True" } else { "" };
            positional_args.push(format!(
                "Index('{}', {}{})",
                index.name,
                cols.join(", "),
                unique_str
            ));
        }
    }

    // Table comment (kwarg)
    if !options.nocomments {
        if let Some(ref comment) = table.comment {
            let lit = format_python_string_literal(comment);
            kwargs.push(format!("'comment': {lit}"));
        }
    }

    // Schema (kwarg, if not default)
    if table.schema != dialect.default_schema() {
        kwargs.push(format!("'schema': '{}'", table.schema));
    }

    if positional_args.is_empty() && kwargs.is_empty() {
        return None;
    }

    // Dict-only form: __table_args__ = {'key': 'value'}
    if positional_args.is_empty() {
        let dict_str = format!("{{{}}}", kwargs.join(", "));
        return Some(dict_str);
    }

    // Tuple form: if kwargs exist, append dict as last item
    if !kwargs.is_empty() {
        positional_args.push(format!("{{{}}}", kwargs.join(", ")));
    }

    let last = positional_args.len() - 1;
    let formatted: Vec<String> = positional_args
        .iter()
        .enumerate()
        .map(|(i, a)| {
            if i < last {
                format!("        {a},")
            } else {
                format!("        {a}")
            }
        })
        .collect();
    Some(formatted.join("\n"))
}

/// Generate a Table() assignment for a table without a primary key.
/// Uses the provided `metadata_ref` (e.g. `Base.metadata` or standalone `metadata`) as the metadata reference.
fn generate_table_fallback(
    table: &TableInfo,
    imports: &mut ImportCollector,
    options: &GeneratorOptions,
    dialect: Dialect,
    metadata_ref: &str,
) -> String {
    let var_name = table_to_variable_name(&table.name);
    let mut lines: Vec<String> = Vec::new();

    lines.push(format!("{var_name} = Table("));
    lines.push(format!("    '{}', {metadata_ref},", table.name));

    // Collect all body items (columns, constraints, indexes, schema)
    let mut body_items: Vec<String> = Vec::new();

    for col in &table.columns {
        let mapped = map_column_type(col, dialect);
        imports.add(&mapped.import_module, &mapped.import_name);
        if let Some((ref elem_mod, ref elem_name)) = mapped.element_import {
            imports.add(elem_mod, elem_name);
        }

        let mut col_args: Vec<String> = Vec::new();
        col_args.push(format!("'{}'", col.name));
        col_args.push(mapped.sa_type.clone());

        // Identity
        if let Some(ref identity) = col.identity {
            imports.add("sqlalchemy", "Identity");
            match dialect {
                Dialect::Postgres => {
                    col_args.push(format!(
                        "Identity(start={}, increment={}, minvalue={}, maxvalue={}, cycle=False, cache={})",
                        identity.start, identity.increment, identity.min_value, identity.max_value, identity.cache
                    ));
                }
                Dialect::Mssql => {
                    col_args.push(format!(
                        "Identity(start={}, increment={})",
                        identity.start, identity.increment
                    ));
                }
            }
        }

        // Nullable (only emit if explicitly False)
        if !col.is_nullable {
            col_args.push("nullable=False".to_string());
        }

        // Server default
        if let Some(ref default) = col.column_default {
            if !is_serial_default(default, dialect) {
                imports.add("sqlalchemy", "text");
                let formatted = format_server_default(default, dialect);
                col_args.push(format!("server_default={formatted}"));
            }
        }

        // Comment
        if !options.nocomments {
            if let Some(ref comment) = col.comment {
                col_args.push(format!("comment='{}'", escape_python_string(comment)));
            }
        }

        body_items.push(format!("Column({})", col_args.join(", ")));
    }

    // Foreign key constraints
    if !options.noconstraints {
        for constraint in &table.constraints {
            if constraint.constraint_type == ConstraintType::ForeignKey {
                if let Some(ref fk) = constraint.foreign_key {
                    imports.add("sqlalchemy", "ForeignKeyConstraint");
                    let local_cols: Vec<String> =
                        constraint.columns.iter().map(|c| format!("'{c}'")).collect();
                    let ref_cols: Vec<String> = fk
                        .ref_columns
                        .iter()
                        .map(|c| format!("'{}.{c}'", fk.ref_table))
                        .collect();
                    body_items.push(format!(
                        "ForeignKeyConstraint([{}], [{}], name='{}')",
                        local_cols.join(", "),
                        ref_cols.join(", "),
                        constraint.name
                    ));
                }
            }
        }
    }

    // Unique constraints (all, not just multi-column)
    if !options.noconstraints {
        for constraint in &table.constraints {
            if constraint.constraint_type == ConstraintType::Unique {
                imports.add("sqlalchemy", "UniqueConstraint");
                let cols = quote_constraint_columns(&constraint.columns);
                body_items.push(format!(
                    "UniqueConstraint({}, name='{}')",
                    cols.join(", "),
                    constraint.name
                ));
            }
        }
    }

    // Indexes
    if !options.noindexes {
        for index in &table.indexes {
            if is_unique_constraint_index(index, &table.constraints) {
                continue;
            }
            imports.add("sqlalchemy", "Index");
            let cols = quote_constraint_columns(&index.columns);
            let unique_str = if index.is_unique { ", unique=True" } else { "" };
            body_items.push(format!(
                "Index('{}', {}{})",
                index.name,
                cols.join(", "),
                unique_str
            ));
        }
    }

    // Schema (only if not default)
    if table.schema != dialect.default_schema() {
        body_items.push(format!("schema='{}'", table.schema));
    }

    // Add body items with commas on all but the last
    let last = body_items.len().saturating_sub(1);
    for (i, item) in body_items.iter().enumerate() {
        if i < last {
            lines.push(format!("    {item},"));
        } else {
            lines.push(format!("    {item}"));
        }
    }

    lines.push(")".to_string());

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::*;

    fn make_simple_schema() -> IntrospectedSchema {
        schema_pg(vec![
            table("users")
                .column(col("id").build())
                .column(col("name").udt("varchar").max_length(100).build())
                .column(col("email").udt("varchar").max_length(255).build())
                .column(col("bio").udt("text").nullable().build())
                .column(col("created_at").udt("timestamptz").nullable().default_val("now()").build())
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
            .contains("user_id: Mapped[int] = mapped_column(Integer, nullable=False)"));
        assert!(output.contains("UniqueConstraint('email', name='users_email_key')"));
        assert!(output.contains("ForeignKeyConstraint(['user_id'], ['users.id'], name='posts_user_id_fkey')"));
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
        let schema = schema_pg(vec![
            table("events")
                .column(col("ts").udt("timestamptz").build())
                .column(col("data").udt("text").build())
                .build(),
        ]);
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
        let schema = schema_pg(vec![
            table("events")
                .column(col("ts").udt("timestamptz").build())
                .column(col("data").udt("text").build())
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        insta::assert_yaml_snapshot!(output);
    }

    // --- Tier 1: Tests adapted from sqlacodegen test_generator_declarative.py ---

    /// Adapted from sqlacodegen test_indexes.
    #[test]
    fn test_declarative_indexes() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("number").nullable().build())
                .column(col("text").udt("varchar").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .index("idx_number", &["number"], false)
                .index("idx_text", &["text"], true)
                .index("idx_text_number", &["text", "number"], false)
                .build(),
        ]);
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
        let schema = schema_pg(vec![
            table("simple_items")
                .schema("testschema")
                .column(col("id").build())
                .pk("simple_items_pkey", &["id"])
                .build(),
        ]);
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
        let schema = schema_pg(vec![
            table("simple_items")
                .schema("testschema")
                .column(col("id").build())
                .column(col("name").udt("varchar").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .index("testidx", &["id", "name"], false)
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("__table_args__ = ("));
        assert!(output.contains("Index('testidx', 'id', 'name'),"));
        assert!(output.contains("{'schema': 'testschema'}"));
    }

    /// Adapted from sqlacodegen test_only_tables (all no-PK fallback).
    #[test]
    fn test_declarative_only_tables() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").nullable().build())
                .build(),
        ]);
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
        let schema = schema_pg(vec![
            table("simple")
                .column(col("id").comment("this is a 'comment'").build())
                .pk("simple_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True, comment=\"this is a 'comment'\")"));
    }

    /// Adapted from sqlacodegen test_column_comment with nocomments option.
    #[test]
    fn test_declarative_column_comment_nocomments() {
        let schema = schema_pg(vec![
            table("simple")
                .column(col("id").comment("this is a 'comment'").build())
                .pk("simple_pkey", &["id"])
                .build(),
        ]);
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
        let schema = schema_pg(vec![
            table("simple")
                .column(col("id").build())
                .pk("simple_pkey", &["id"])
                .comment("this is a 'comment'")
                .build(),
        ]);
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
        let schema = schema_pg(vec![
            table("CustomerAPIPreference")
                .column(col("id").build())
                .pk("customer_api_preference_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("class CustomerApiPreference(Base):"));
        assert!(output.contains("__tablename__ = 'CustomerAPIPreference'"));
    }

    /// Adapted from sqlacodegen test_underscore.
    #[test]
    fn test_declarative_underscore() {
        let schema = schema_pg(vec![
            table("customer_api_preference")
                .column(col("id").build())
                .pk("customer_api_preference_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("class CustomerApiPreference(Base):"));
        assert!(output.contains("__tablename__ = 'customer_api_preference'"));
    }

    /// Adapted from sqlacodegen test_pascal_multiple_underscore.
    #[test]
    fn test_declarative_pascal_multiple_underscore() {
        let schema = schema_pg(vec![
            table("customer_API__Preference")
                .column(col("id").build())
                .pk("customer_api_preference_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // heck's UpperCamelCase handling of double underscores
        assert!(output.contains("__tablename__ = 'customer_API__Preference'"));
    }
}
