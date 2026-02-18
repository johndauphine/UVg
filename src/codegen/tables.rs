use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::{
    escape_python_string, format_server_default, is_primary_key_column, is_serial_default,
    is_unique_constraint_index, quote_constraint_columns, topo_sort_tables, Generator,
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

        let sorted_tables = topo_sort_tables(&schema.tables);
        for table in sorted_tables {
            let block = generate_table(table, &mut imports, options, schema.dialect);
            table_blocks.push(block);
        }

        let mut output = imports.render();
        output.push_str("\n\nmetadata = MetaData()\n");

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
) -> String {
    let var_name = table_to_variable_name(&table.name);
    let mut lines: Vec<String> = Vec::new();

    lines.push(format!("{var_name} = Table("));
    lines.push(format!("    '{}', metadata,", table.name));

    // Collect all body items (columns, constraints, indexes, PK, schema)
    let mut body_items: Vec<String> = Vec::new();

    // Columns
    for col in &table.columns {
        let mapped = map_column_type(col, dialect);
        imports.add(&mapped.import_module, &mapped.import_name);
        if let Some((ref elem_mod, ref elem_name)) = mapped.element_import {
            imports.add(elem_mod, elem_name);
        }

        let mut col_args: Vec<String> = Vec::new();
        col_args.push(format!("'{}'", col.name));
        col_args.push(mapped.sa_type.clone());

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
            tables: vec![TableInfo {
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
                        is_nullable: true,
                        udt_name: "text".to_string(),
                        ..test_column("email")
                    },
                ],
                constraints: vec![ConstraintInfo {
                    name: "users_pkey".to_string(),
                    constraint_type: ConstraintType::PrimaryKey,
                    columns: vec!["id".to_string()],
                    foreign_key: None,
                }],
                indexes: vec![],
            }],
        }
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

    #[test]
    fn test_tables_generator_no_pk() {
        let schema = IntrospectedSchema {
            dialect: Dialect::Postgres,
            tables: vec![TableInfo {
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
            }],
        };
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
        let schema = IntrospectedSchema {
            dialect: Dialect::Postgres,
            tables: vec![TableInfo {
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
            }],
        };
        let gen = TablesGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        insta::assert_yaml_snapshot!(output);
    }
}
