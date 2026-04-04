use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::{
    enum_class_name, escape_python_string, find_enum_for_column, format_fk_options,
    format_python_string_literal, format_server_default, generate_enum_class,
    is_primary_key_column, is_serial_default, is_unique_constraint_index,
    quote_constraint_columns, topo_sort_tables, Generator,
};
use crate::dialect::Dialect;
use crate::naming::table_to_variable_name;
use crate::schema::{ConstraintType, IntrospectedSchema, TableInfo};
use crate::typemap::map_column_type;

pub struct TablesGenerator;

impl Generator for TablesGenerator {
    fn generate(&self, schema: &IntrospectedSchema, options: &GeneratorOptions) -> String {
        let mut imports = ImportCollector::new();
        let mut table_blocks: Vec<String> = Vec::new();

        // Always need MetaData and Table for tables generator
        imports.add("sqlalchemy", "MetaData");
        imports.add("sqlalchemy", "Table");
        imports.add("sqlalchemy", "Column");

        // Collect which enums are actually used
        let mut used_enums: Vec<&crate::schema::EnumInfo> = Vec::new();

        let sorted_tables = topo_sort_tables(&schema.tables);
        for table in sorted_tables {
            // Track which enums are referenced
            for col in &table.columns {
                if let Some(ei) = find_enum_for_column(&col.udt_name, &schema.enums) {
                    if !used_enums.iter().any(|e| e.schema == ei.schema && e.name == ei.name) {
                        used_enums.push(ei);
                    }
                }
            }
            let block = generate_table(table, &mut imports, options, schema.dialect, &schema.enums);
            table_blocks.push(block);
        }

        // Add enum imports if any enums used
        if !used_enums.is_empty() {
            imports.add_bare("enum");
            imports.add("sqlalchemy", "Enum");
        }

        let mut output = imports.render();
        output.push_str("\n\nmetadata = MetaData()\n");

        // Enum class definitions
        for ei in &used_enums {
            output.push_str("\n\n");
            output.push_str(&generate_enum_class(ei));
        }

        for block in table_blocks {
            output.push_str("\n\n");
            output.push_str(&block);
        }

        output.push('\n');
        output
    }
}

fn generate_table(
    table: &TableInfo,
    imports: &mut ImportCollector,
    options: &GeneratorOptions,
    dialect: Dialect,
    enums: &[crate::schema::EnumInfo],
) -> String {
    let var_name = table_to_variable_name(&table.name);
    let mut lines: Vec<String> = Vec::new();

    lines.push(format!("{var_name} = Table("));
    lines.push(format!("    '{}', metadata,", table.name));

    // Collect all body items (columns, constraints, indexes, PK, schema)
    let mut body_items: Vec<String> = Vec::new();

    // Columns
    for col in &table.columns {
        let mut col_args: Vec<String> = Vec::new();
        col_args.push(format!("'{}'", col.name));

        // Check if column type is a named enum
        if let Some(ei) = find_enum_for_column(&col.udt_name, enums) {
            let cls = enum_class_name(&ei.name);
            let mut enum_parts = vec![
                cls,
                "values_callable=lambda cls: [member.value for member in cls]".to_string(),
            ];
            if !ei.name.is_empty() {
                enum_parts.push(format!("name={}", format_python_string_literal(&ei.name)));
            }
            if let Some(ref schema) = ei.schema {
                if !schema.is_empty() {
                    enum_parts.push(format!("schema={}", format_python_string_literal(schema)));
                }
            }
            col_args.push(format!("Enum({})", enum_parts.join(", ")));
        } else {
            let mapped = map_column_type(col, dialect);
            imports.add(&mapped.import_module, &mapped.import_name);
            if let Some((ref elem_mod, ref elem_name)) = mapped.element_import {
                imports.add(elem_mod, elem_name);
            }
            col_args.push(mapped.sa_type.clone());
        }

        // Identity — dialect-aware output
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

        // Primary key
        if is_primary_key_column(&col.name, &table.constraints) {
            col_args.push("primary_key=True".to_string());
        }

        // Nullable (only emit if explicitly False for non-PK columns)
        if !col.is_nullable && !is_primary_key_column(&col.name, &table.constraints) {
            col_args.push("nullable=False".to_string());
        }

        // Server default
        if let Some(ref default) = col.column_default {
            // Skip nextval defaults (auto-generated for serial columns)
            if !is_serial_default(default, dialect) {
                imports.add("sqlalchemy", "text");
                let formatted = format_server_default(default, dialect);
                col_args.push(format!("server_default={formatted}"));
            }
        }

        // Comment
        if !options.nocomments {
            if let Some(ref comment) = col.comment {
                col_args.push(format!("comment={}", format_python_string_literal(comment)));
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
                    let fk_opts = format_fk_options(fk);
                    body_items.push(format!(
                        "ForeignKeyConstraint([{}], [{}], name='{}'{})",
                        local_cols.join(", "),
                        ref_cols.join(", "),
                        constraint.name,
                        fk_opts
                    ));
                }
            }
        }
    }

    // Check constraints
    if !options.noconstraints {
        for constraint in &table.constraints {
            if constraint.constraint_type == ConstraintType::Check {
                if let Some(ref expr) = constraint.check_expression {
                    imports.add("sqlalchemy", "CheckConstraint");
                    let expr_literal = format_python_string_literal(expr);
                    if constraint.name.is_empty() {
                        body_items.push(format!("CheckConstraint({expr_literal})"));
                    } else {
                        body_items.push(format!(
                            "CheckConstraint({expr_literal}, name='{}')",
                            constraint.name
                        ));
                    }
                }
            }
        }
    }

    // Primary key constraint
    if !options.noconstraints {
        for constraint in &table.constraints {
            if constraint.constraint_type == ConstraintType::PrimaryKey {
                imports.add("sqlalchemy", "PrimaryKeyConstraint");
                let cols = quote_constraint_columns(&constraint.columns);
                body_items.push(format!(
                    "PrimaryKeyConstraint({}, name='{}')",
                    cols.join(", "),
                    constraint.name
                ));
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
            // Skip indexes that back unique constraints (already handled)
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

    // Table comment
    if !options.nocomments {
        if let Some(ref comment) = table.comment {
            body_items.push(format!("comment={}", format_python_string_literal(comment)));
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
                .column(col("email").udt("text").nullable().build())
                .pk("users_pkey", &["id"])
                .build(),
        ])
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
        schema_pg(vec![
            table("audit_log")
                .column(col("ts").udt("timestamptz").build())
                .column(col("action").udt("text").build())
                .column(col("detail").udt("text").nullable().build())
                .build(),
        ])
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
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").nullable().build())
                .column(col("number").nullable().build())
                .column(col("text").udt("varchar").nullable().build())
                .index("ix_number", &["number"], false)
                .index("ix_text_number", &["text", "number"], true)
                .index("ix_text", &["text"], true)
                .build(),
        ]);
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
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").nullable().build())
                .column(col("number").nullable().build())
                .unique("uq_id_number", &["id", "number"])
                .build(),
        ]);
        let gen = TablesGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("Column('id', Integer)"));
        assert!(output.contains("Column('number', Integer)"));
        assert!(output.contains("UniqueConstraint('id', 'number', name='uq_id_number')"));
    }

    /// Adapted from sqlacodegen test_table_comment.
    #[test]
    fn test_tables_table_comment() {
        let schema = schema_pg(vec![
            table("simple")
                .column(col("id").build())
                .pk("simple_pkey", &["id"])
                .comment("this is a 'comment'")
                .build(),
        ]);
        let gen = TablesGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("Column('id', Integer, primary_key=True)"));
        assert!(output.contains("comment=\"this is a 'comment'\""));
    }

    /// Adapted from sqlacodegen test_table_name_identifiers.
    /// Tests that non-identifier table names are sanitized in variable names.
    #[test]
    fn test_tables_table_name_identifiers() {
        let schema = schema_pg(vec![
            table("simple-items table")
                .column(col("id").build())
                .pk("simple_items_table_pkey", &["id"])
                .build(),
        ]);
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
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("number").nullable().build())
                .unique("uq_number", &["number"])
                .index("idx_number", &["number"], false)
                .build(),
        ]);
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
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("number").nullable().build())
                .unique("uq_number", &["number"])
                .index("idx_number", &["number"], false)
                .build(),
        ]);
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
        let schema = schema_pg(vec![
            table("simple")
                .column(col("id").comment("pk column comment").build())
                .pk("simple_pkey", &["id"])
                .comment("this is a 'comment'")
                .build(),
        ]);
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
        let schema = schema_pg(vec![
            table("simple_items")
                .schema("testschema")
                .column(col("name").udt("varchar").nullable().build())
                .build(),
        ]);
        let gen = TablesGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("t_simple_items = Table("));
        assert!(output.contains("Column('name', String)"));
        assert!(output.contains("schema='testschema'"));
    }

    /// Adapted from sqlacodegen test_pk_default.
    #[test]
    fn test_tables_pk_default() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").default_val("uuid_generate_v4()").build())
                .pk("simple_items_pkey", &["id"])
                .build(),
        ]);
        let gen = TablesGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("Column('id', Integer, primary_key=True, server_default=text('uuid_generate_v4()'))"));
    }

    /// Adapted from sqlacodegen test_identity_column.
    #[test]
    fn test_tables_identity_column() {
        use crate::schema::IdentityInfo;
        let schema = schema_pg(vec![
            table("simple_items")
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
                .build(),
        ]);
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
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").nullable().comment("This\nis a multi-line\ncomment").build())
                .build(),
        ]);
        let gen = TablesGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("comment='This\\nis a multi-line\\ncomment'"));
    }

    /// Adapted from sqlacodegen test_multiline_table_comment.
    #[test]
    fn test_tables_multiline_table_comment() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").nullable().build())
                .comment("This\nis a multi-line\ncomment")
                .build(),
        ]);
        let gen = TablesGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("comment='This\\nis a multi-line\\ncomment'"));
    }

    /// Adapted from sqlacodegen test_server_default_multiline.
    #[test]
    fn test_tables_server_default_multiline() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(
                    col("id")
                        .default_val("/*Comment*/\n/*Next line*/\nsomething()")
                        .build(),
                )
                .pk("simple_items_pkey", &["id"])
                .build(),
        ]);
        let gen = TablesGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains(
            "server_default=text('/*Comment*/\\n/*Next line*/\\nsomething()')"
        ));
    }

    /// Adapted from sqlacodegen test_server_default_colon.
    #[test]
    fn test_tables_server_default_colon() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("problem").udt("varchar").nullable().default_val("':001'").build())
                .build(),
        ]);
        let gen = TablesGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("server_default=text(\"':001'\")"));
    }

    /// Adapted from sqlacodegen test_null_type.
    #[test]
    fn test_tables_null_type() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("problem").udt("").nullable().build())
                .build(),
        ]);
        let gen = TablesGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("Column('problem', NullType)"));
        assert!(output.contains("from sqlalchemy.sql.sqltypes import NullType"));
    }

    /// Adapted from sqlacodegen test_foreign_key_options.
    #[test]
    fn test_tables_foreign_key_options() {
        let schema = schema_pg(vec![
            table("simple_items")
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
                .build(),
        ]);
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
        let schema = schema_mssql(vec![
            table("simple_items")
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
                .build(),
        ]);
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
}
