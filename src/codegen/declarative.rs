use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::{
    escape_python_string, format_server_default, has_primary_key, is_primary_key_column,
    is_serial_default, is_unique_constraint_index, quote_constraint_columns, topo_sort_tables,
    Generator,
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
        lines.push(format!("    __table_args__ = (\n{args_str}\n    )"));
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
                mc_args.push(format!("comment='{}'", escape_python_string(comment)));
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
    let mut args: Vec<String> = Vec::new();

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
                    args.push(format!(
                        "ForeignKeyConstraint([{}], [{}], name='{}')",
                        local_cols.join(", "),
                        ref_cols.join(", "),
                        constraint.name
                    ));
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
                args.push(format!(
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
                args.push(format!(
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
            args.push(format!(
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
            args.push(format!("{{'comment': '{}'}}", escape_python_string(comment)));
        }
    }

    // Schema (if not default)
    if table.schema != dialect.default_schema() {
        args.push(format!("{{'schema': '{}'}}", table.schema));
    }

    if args.is_empty() {
        None
    } else {
        let last = args.len() - 1;
        let formatted: Vec<String> = args
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
    use crate::schema::*;
    use crate::testutil::test_column;

    fn make_simple_schema() -> IntrospectedSchema {
        IntrospectedSchema {
            dialect: Dialect::Postgres,
            tables: vec![
                TableInfo {
                    schema: "public".to_string(),
                    name: "users".to_string(),
                    table_type: TableType::Table,
                    comment: None,
                    columns: vec![
                        test_column("id"),
                        ColumnInfo {
                            udt_name: "varchar".to_string(),
                            character_maximum_length: Some(100),
                            ..test_column("name")
                        },
                        ColumnInfo {
                            udt_name: "varchar".to_string(),
                            character_maximum_length: Some(255),
                            ..test_column("email")
                        },
                        ColumnInfo {
                            is_nullable: true,
                            udt_name: "text".to_string(),
                            ..test_column("bio")
                        },
                        ColumnInfo {
                            is_nullable: true,
                            udt_name: "timestamptz".to_string(),
                            column_default: Some("now()".to_string()),
                            ..test_column("created_at")
                        },
                    ],
                    constraints: vec![
                        ConstraintInfo {
                            name: "users_pkey".to_string(),
                            constraint_type: ConstraintType::PrimaryKey,
                            columns: vec!["id".to_string()],
                            foreign_key: None,
                        },
                        ConstraintInfo {
                            name: "users_email_key".to_string(),
                            constraint_type: ConstraintType::Unique,
                            columns: vec!["email".to_string()],
                            foreign_key: None,
                        },
                    ],
                    indexes: vec![],
                },
                TableInfo {
                    schema: "public".to_string(),
                    name: "posts".to_string(),
                    table_type: TableType::Table,
                    comment: None,
                    columns: vec![
                        ColumnInfo {
                            udt_name: "int8".to_string(),
                            ..test_column("id")
                        },
                        test_column("user_id"),
                        ColumnInfo {
                            udt_name: "varchar".to_string(),
                            character_maximum_length: Some(200),
                            ..test_column("title")
                        },
                        ColumnInfo {
                            udt_name: "text".to_string(),
                            ..test_column("body")
                        },
                    ],
                    constraints: vec![
                        ConstraintInfo {
                            name: "posts_pkey".to_string(),
                            constraint_type: ConstraintType::PrimaryKey,
                            columns: vec!["id".to_string()],
                            foreign_key: None,
                        },
                        ConstraintInfo {
                            name: "posts_user_id_fkey".to_string(),
                            constraint_type: ConstraintType::ForeignKey,
                            columns: vec!["user_id".to_string()],
                            foreign_key: Some(ForeignKeyInfo {
                                ref_schema: "public".to_string(),
                                ref_table: "users".to_string(),
                                ref_columns: vec!["id".to_string()],
                                update_rule: "NO ACTION".to_string(),
                                delete_rule: "NO ACTION".to_string(),
                            }),
                        },
                    ],
                    indexes: vec![],
                },
            ],
        }
    }

    #[test]
    fn test_declarative_generator_basic() {
        let schema = make_simple_schema();
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("class Users(Base):"));
        assert!(output.contains("__tablename__ = 'users'"));
        assert!(output.contains("PrimaryKeyConstraint('id', name='users_pkey'),"));
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

    /// Schema with a mix: one table with PK (users), one without (audit_log).
    fn make_mixed_pk_schema() -> IntrospectedSchema {
        IntrospectedSchema {
            dialect: Dialect::Postgres,
            tables: vec![
                TableInfo {
                    schema: "public".to_string(),
                    name: "users".to_string(),
                    table_type: TableType::Table,
                    comment: None,
                    columns: vec![
                        test_column("id"),
                        ColumnInfo {
                            udt_name: "varchar".to_string(),
                            character_maximum_length: Some(100),
                            ..test_column("name")
                        },
                    ],
                    constraints: vec![ConstraintInfo {
                        name: "users_pkey".to_string(),
                        constraint_type: ConstraintType::PrimaryKey,
                        columns: vec!["id".to_string()],
                        foreign_key: None,
                    }],
                    indexes: vec![],
                },
                TableInfo {
                    schema: "public".to_string(),
                    name: "audit_log".to_string(),
                    table_type: TableType::Table,
                    comment: None,
                    columns: vec![
                        ColumnInfo {
                            udt_name: "timestamptz".to_string(),
                            ..test_column("ts")
                        },
                        ColumnInfo {
                            udt_name: "text".to_string(),
                            ..test_column("action")
                        },
                        ColumnInfo {
                            is_nullable: true,
                            udt_name: "text".to_string(),
                            ..test_column("detail")
                        },
                    ],
                    constraints: vec![],
                    indexes: vec![],
                },
            ],
        }
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
        let schema = IntrospectedSchema {
            dialect: Dialect::Postgres,
            tables: vec![TableInfo {
                schema: "public".to_string(),
                name: "events".to_string(),
                table_type: TableType::Table,
                comment: None,
                columns: vec![
                    ColumnInfo {
                        udt_name: "timestamptz".to_string(),
                        ..test_column("ts")
                    },
                    ColumnInfo {
                        udt_name: "text".to_string(),
                        ..test_column("data")
                    },
                ],
                constraints: vec![],
                indexes: vec![],
            }],
        };
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
        let schema = IntrospectedSchema {
            dialect: Dialect::Postgres,
            tables: vec![TableInfo {
                schema: "public".to_string(),
                name: "events".to_string(),
                table_type: TableType::Table,
                comment: None,
                columns: vec![
                    ColumnInfo {
                        udt_name: "timestamptz".to_string(),
                        ..test_column("ts")
                    },
                    ColumnInfo {
                        udt_name: "text".to_string(),
                        ..test_column("data")
                    },
                ],
                constraints: vec![],
                indexes: vec![],
            }],
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        insta::assert_yaml_snapshot!(output);
    }
}
