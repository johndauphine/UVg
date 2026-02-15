use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::{
    escape_python_string, format_server_default, get_foreign_key_for_column, has_unique_constraint,
    is_primary_key_column, is_serial_default, is_unique_constraint_index,
    quote_constraint_columns, Generator,
};
use crate::dialect::Dialect;
use crate::naming::table_to_class_name;
use crate::schema::{ConstraintType, IntrospectedSchema, TableInfo};
use crate::typemap::map_column_type;

pub struct DeclarativeGenerator;

impl Generator for DeclarativeGenerator {
    fn generate(&self, schema: &IntrospectedSchema, options: &GeneratorOptions) -> String {
        let mut imports = ImportCollector::new();
        let mut class_blocks: Vec<String> = Vec::new();
        let mut needs_optional = false;
        let mut needs_datetime = false;
        let mut needs_decimal = false;
        let mut needs_uuid = false;

        // Always need these for declarative
        imports.add("sqlalchemy.orm", "DeclarativeBase");
        imports.add("sqlalchemy.orm", "Mapped");
        imports.add("sqlalchemy.orm", "mapped_column");

        for table in &schema.tables {
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
            class_blocks.push(block);
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
        output.push_str("\n\nclass Base(DeclarativeBase):\n    pass");

        for block in class_blocks {
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

        // Foreign key
        if !options.noconstraints {
            if let Some(fk_constraint) = get_foreign_key_for_column(&col.name, &table.constraints) {
                if let Some(ref fk) = fk_constraint.foreign_key {
                    imports.add("sqlalchemy", "ForeignKey");
                    let ref_col = format!("{}.{}", fk.ref_table, fk.ref_columns[0]);
                    mc_args.push(format!("ForeignKey('{ref_col}')"));
                }
            }
        }

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

        // Unique (single-column)
        if !options.noconstraints && has_unique_constraint(&col.name, &table.constraints) {
            mc_args.push("unique=True".to_string());
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

    // Multi-column unique constraints
    if !options.noconstraints {
        for constraint in &table.constraints {
            if constraint.constraint_type == ConstraintType::Unique && constraint.columns.len() > 1
            {
                imports.add("sqlalchemy", "UniqueConstraint");
                let cols = quote_constraint_columns(&constraint.columns);
                args.push(format!("UniqueConstraint({})", cols.join(", ")));
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
        let formatted: Vec<String> = args.iter().map(|a| format!("        {a},")).collect();
        Some(formatted.join("\n"))
    }
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
        assert!(output.contains("email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)"));
        assert!(output.contains("bio: Mapped[Optional[str]] = mapped_column(Text)"));
        assert!(output.contains("class Posts(Base):"));
        assert!(output
            .contains("user_id: Mapped[int] = mapped_column(Integer, ForeignKey('users.id'), nullable=False)"));
    }

    #[test]
    fn test_declarative_generator_snapshot() {
        let schema = make_simple_schema();
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        insta::assert_yaml_snapshot!(output);
    }
}
