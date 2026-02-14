use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::{
    format_server_default, get_foreign_key_for_column, has_unique_constraint,
    is_primary_key_column, Generator,
};
use crate::naming::table_to_class_name;
use crate::schema::{ConstraintType, IndexInfo, IntrospectedSchema, TableInfo};
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
            let (block, meta) = generate_class(table, &mut imports, options);
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
        output.push_str("\n\n\nclass Base(DeclarativeBase):\n    pass\n");

        for block in class_blocks {
            output.push('\n');
            output.push('\n');
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
    let table_args = build_table_args(table, imports, options);
    if !table_args.is_empty() {
        lines.push(format!("    __table_args__ = ({table_args})"));
    }

    // Blank line before columns
    lines.push(String::new());

    // Columns
    for col in &table.columns {
        let mapped = map_column_type(col);
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

        // Python type annotation
        let python_type = &mapped.python_type;
        let type_annotation =
            if col.is_nullable && !is_primary_key_column(&col.name, &table.constraints) {
                meta.needs_optional = true;
                format!("Optional[{python_type}]")
            } else {
                python_type.clone()
            };

        // mapped_column arguments
        let mut mc_args: Vec<String> = Vec::new();

        // Type argument - only include if it's not a simple mapping
        // (always include for clarity in this generator)
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

        // Primary key
        if is_primary_key_column(&col.name, &table.constraints) {
            mc_args.push("primary_key=True".to_string());
        }

        // Unique (single-column)
        if !options.noconstraints && has_unique_constraint(&col.name, &table.constraints) {
            mc_args.push("unique=True".to_string());
        }

        // Server default
        if let Some(ref default) = col.column_default {
            if !default.starts_with("nextval(") {
                imports.add("sqlalchemy", "text");
                let formatted = format_server_default(default);
                mc_args.push(format!("server_default={formatted}"));
            }
        }

        // Comment
        if !options.nocomments {
            if let Some(ref comment) = col.comment {
                mc_args.push(format!("comment='{}'", comment.replace('\'', "\\'")));
            }
        }

        let mc_str = mc_args.join(", ");
        lines.push(format!(
            "    {}: Mapped[{type_annotation}] = mapped_column({mc_str})",
            col.name
        ));
    }

    (lines.join("\n"), meta)
}

fn build_table_args(
    table: &TableInfo,
    imports: &mut ImportCollector,
    options: &GeneratorOptions,
) -> String {
    let mut args: Vec<String> = Vec::new();

    // Multi-column unique constraints
    if !options.noconstraints {
        for constraint in &table.constraints {
            if constraint.constraint_type == ConstraintType::Unique && constraint.columns.len() > 1
            {
                imports.add("sqlalchemy", "UniqueConstraint");
                let cols: Vec<String> = constraint
                    .columns
                    .iter()
                    .map(|c| format!("'{c}'"))
                    .collect();
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
            let cols: Vec<String> = index.columns.iter().map(|c| format!("'{c}'")).collect();
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
            args.push(format!("{{'comment': '{}'}}", comment.replace('\'', "\\'")));
        }
    }

    // Schema (if not 'public')
    if table.schema != "public" {
        args.push(format!("{{'schema': '{}'}}", table.schema));
    }

    if args.is_empty() {
        String::new()
    } else {
        // Single item needs trailing comma to be a tuple
        if args.len() == 1 {
            format!("{},", args[0])
        } else {
            args.join(", ")
        }
    }
}

fn is_unique_constraint_index(
    index: &IndexInfo,
    constraints: &[crate::schema::ConstraintInfo],
) -> bool {
    if !index.is_unique {
        return false;
    }
    constraints
        .iter()
        .any(|c| c.constraint_type == ConstraintType::Unique && c.columns == index.columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;

    fn make_simple_schema() -> IntrospectedSchema {
        IntrospectedSchema {
            tables: vec![
                TableInfo {
                    schema: "public".to_string(),
                    name: "users".to_string(),
                    table_type: TableType::Table,
                    comment: None,
                    columns: vec![
                        ColumnInfo {
                            name: "id".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "integer".to_string(),
                            udt_name: "int4".to_string(),
                            character_maximum_length: None,
                            numeric_precision: None,
                            numeric_scale: None,
                            column_default: None,
                            is_identity: false,
                            identity_generation: None,
                            comment: None,
                        },
                        ColumnInfo {
                            name: "name".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "character varying".to_string(),
                            udt_name: "varchar".to_string(),
                            character_maximum_length: Some(100),
                            numeric_precision: None,
                            numeric_scale: None,
                            column_default: None,
                            is_identity: false,
                            identity_generation: None,
                            comment: None,
                        },
                        ColumnInfo {
                            name: "email".to_string(),
                            ordinal_position: 3,
                            is_nullable: false,
                            data_type: "character varying".to_string(),
                            udt_name: "varchar".to_string(),
                            character_maximum_length: Some(255),
                            numeric_precision: None,
                            numeric_scale: None,
                            column_default: None,
                            is_identity: false,
                            identity_generation: None,
                            comment: None,
                        },
                        ColumnInfo {
                            name: "bio".to_string(),
                            ordinal_position: 4,
                            is_nullable: true,
                            data_type: "text".to_string(),
                            udt_name: "text".to_string(),
                            character_maximum_length: None,
                            numeric_precision: None,
                            numeric_scale: None,
                            column_default: None,
                            is_identity: false,
                            identity_generation: None,
                            comment: None,
                        },
                        ColumnInfo {
                            name: "created_at".to_string(),
                            ordinal_position: 5,
                            is_nullable: true,
                            data_type: "timestamp with time zone".to_string(),
                            udt_name: "timestamptz".to_string(),
                            character_maximum_length: None,
                            numeric_precision: None,
                            numeric_scale: None,
                            column_default: Some("now()".to_string()),
                            is_identity: false,
                            identity_generation: None,
                            comment: None,
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
                            name: "id".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "bigint".to_string(),
                            udt_name: "int8".to_string(),
                            character_maximum_length: None,
                            numeric_precision: None,
                            numeric_scale: None,
                            column_default: None,
                            is_identity: false,
                            identity_generation: None,
                            comment: None,
                        },
                        ColumnInfo {
                            name: "user_id".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "integer".to_string(),
                            udt_name: "int4".to_string(),
                            character_maximum_length: None,
                            numeric_precision: None,
                            numeric_scale: None,
                            column_default: None,
                            is_identity: false,
                            identity_generation: None,
                            comment: None,
                        },
                        ColumnInfo {
                            name: "title".to_string(),
                            ordinal_position: 3,
                            is_nullable: false,
                            data_type: "character varying".to_string(),
                            udt_name: "varchar".to_string(),
                            character_maximum_length: Some(200),
                            numeric_precision: None,
                            numeric_scale: None,
                            column_default: None,
                            is_identity: false,
                            identity_generation: None,
                            comment: None,
                        },
                        ColumnInfo {
                            name: "body".to_string(),
                            ordinal_position: 4,
                            is_nullable: false,
                            data_type: "text".to_string(),
                            udt_name: "text".to_string(),
                            character_maximum_length: None,
                            numeric_precision: None,
                            numeric_scale: None,
                            column_default: None,
                            is_identity: false,
                            identity_generation: None,
                            comment: None,
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
        assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True)"));
        assert!(output.contains("name: Mapped[str] = mapped_column(String(100))"));
        assert!(output.contains("email: Mapped[str] = mapped_column(String(255), unique=True)"));
        assert!(output.contains("bio: Mapped[Optional[str]] = mapped_column(Text)"));
        assert!(output.contains("class Posts(Base):"));
        assert!(output
            .contains("user_id: Mapped[int] = mapped_column(Integer, ForeignKey('users.id'))"));
    }

    #[test]
    fn test_declarative_generator_snapshot() {
        let schema = make_simple_schema();
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        insta::assert_yaml_snapshot!(output);
    }
}
