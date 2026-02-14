use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::{
    format_server_default, get_foreign_key_for_column, has_unique_constraint,
    is_primary_key_column, Generator,
};
use crate::naming::table_to_variable_name;
use crate::schema::{ConstraintType, IndexInfo, IntrospectedSchema, TableInfo};
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

        for table in &schema.tables {
            let block = generate_table(table, &mut imports, options);
            table_blocks.push(block);
        }

        let mut output = imports.render();
        output.push_str("\n\nmetadata = MetaData()\n");

        for block in table_blocks {
            output.push('\n');
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
) -> String {
    let var_name = table_to_variable_name(&table.name);
    let mut lines: Vec<String> = Vec::new();

    lines.push(format!("{var_name} = Table("));
    lines.push(format!("    '{}', metadata,", table.name));

    // Columns
    for col in &table.columns {
        let mapped = map_column_type(col);
        imports.add(&mapped.import_module, &mapped.import_name);
        if let Some((ref elem_mod, ref elem_name)) = mapped.element_import {
            imports.add(elem_mod, elem_name);
        }

        let mut col_args: Vec<String> = Vec::new();
        col_args.push(format!("'{}'", col.name));
        col_args.push(mapped.sa_type.clone());

        // Foreign key
        if !options.noconstraints {
            if let Some(fk_constraint) = get_foreign_key_for_column(&col.name, &table.constraints) {
                if let Some(ref fk) = fk_constraint.foreign_key {
                    imports.add("sqlalchemy", "ForeignKey");
                    let ref_col = format!("{}.{}", fk.ref_table, fk.ref_columns[0]);
                    col_args.push(format!("ForeignKey('{ref_col}')"));
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

        // Unique (single-column)
        if !options.noconstraints && has_unique_constraint(&col.name, &table.constraints) {
            col_args.push("unique=True".to_string());
        }

        // Server default
        if let Some(ref default) = col.column_default {
            // Skip nextval defaults (auto-generated for serial columns)
            if !default.starts_with("nextval(") {
                imports.add("sqlalchemy", "text");
                let formatted = format_server_default(default);
                col_args.push(format!("server_default={formatted}"));
            }
        }

        // Comment
        if !options.nocomments {
            if let Some(ref comment) = col.comment {
                col_args.push(format!("comment='{}'", comment.replace('\'', "\\'")));
            }
        }

        lines.push(format!("    Column({}),", col_args.join(", ")));
    }

    // Multi-column unique constraints as table-level args
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
                lines.push(format!("    UniqueConstraint({}),", cols.join(", ")));
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
            let cols: Vec<String> = index.columns.iter().map(|c| format!("'{c}'")).collect();
            let unique_str = if index.is_unique { ", unique=True" } else { "" };
            lines.push(format!(
                "    Index('{}', {}{}),",
                index.name,
                cols.join(", "),
                unique_str
            ));
        }
    }

    // Schema
    lines.push(format!("    schema='{}'", table.schema));
    lines.push(")".to_string());

    lines.join("\n")
}

/// Check if an index is just backing a unique constraint (same columns).
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
            tables: vec![TableInfo {
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
}
