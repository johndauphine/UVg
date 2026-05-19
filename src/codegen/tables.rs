use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::{
    enum_class_name, find_enum_for_column, format_fk_options, format_index_kwargs,
    format_python_string_literal, format_server_default, generate_enum_class,
    is_primary_key_column, is_serial_default, is_standard_sequence_name,
    is_unique_constraint_index, parse_check_boolean, parse_check_enum, parse_sequence_name,
    quote_constraint_columns, topo_sort_tables, Generator,
};
use crate::dialect::Dialect;
use crate::naming::table_to_variable_name;
use crate::schema::EnumInfo;
use crate::schema::{ConstraintType, IntrospectedSchema, TableInfo};
use crate::typemap::{map_column_type, map_column_type_dialect};

pub struct TablesGenerator;

impl Generator for TablesGenerator {
    fn generate(&self, schema: &IntrospectedSchema, options: &GeneratorOptions) -> String {
        let mut imports = ImportCollector::new();
        let mut table_blocks: Vec<String> = Vec::new();

        // Always need MetaData and Table for tables generator
        imports.add("sqlalchemy", "MetaData");
        imports.add("sqlalchemy", "Table");
        imports.add("sqlalchemy", "Column");

        // Collect named enums and synthetic enums from check constraints
        let mut all_enums: Vec<EnumInfo> = schema.enums.clone();
        let mut synthetic_enum_cols: std::collections::HashMap<(String, String), String> =
            std::collections::HashMap::new();
        // Boolean columns detected from IN (0, 1) check constraints
        let mut boolean_cols: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();

        let sorted_tables = topo_sort_tables(&schema.tables);

        // Detect boolean columns from check constraints
        for table in &sorted_tables {
            for constraint in &table.constraints {
                if constraint.constraint_type == ConstraintType::Check {
                    if let Some(ref expr) = constraint.check_expression {
                        if let Some(col_name) = parse_check_boolean(expr) {
                            boolean_cols.insert((table.name.clone(), col_name));
                        }
                    }
                }
            }
        }

        // Extract synthetic enums from check constraints (unless nosyntheticenums)
        if !options.nosyntheticenums {
            for table in &sorted_tables {
                for constraint in &table.constraints {
                    if constraint.constraint_type == ConstraintType::Check {
                        if let Some(ref expr) = constraint.check_expression {
                            if let Some((col_name, values)) = parse_check_enum(expr) {
                                let key = (table.name.clone(), col_name.clone());
                                if let std::collections::hash_map::Entry::Vacant(entry) =
                                    synthetic_enum_cols.entry(key)
                                {
                                    use heck::ToUpperCamelCase;
                                    let enum_name = format!("{}_{}", table.name, col_name)
                                        .to_upper_camel_case();
                                    let ei = EnumInfo {
                                        name: enum_name.clone(),
                                        schema: None,
                                        values,
                                    };
                                    all_enums.push(ei);
                                    entry.insert(enum_name);
                                }
                            }
                        }
                    }
                }
            }
        } // end nosyntheticenums guard

        // Track which enums are actually used
        let mut used_enum_names: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for table in &sorted_tables {
            // Track named enum usage
            for col_info in &table.columns {
                if find_enum_for_column(&col_info.udt_name, &all_enums).is_some() {
                    used_enum_names.insert(col_info.udt_name.clone());
                }
                // Track synthetic enum usage via direct lookup
                let key = (table.name.clone(), col_info.name.clone());
                if let Some(class_name) = synthetic_enum_cols.get(&key) {
                    used_enum_names.insert(class_name.clone());
                }
            }

            let block = generate_table(
                table,
                &mut imports,
                options,
                schema.dialect,
                &all_enums,
                &synthetic_enum_cols,
                &boolean_cols,
                &schema.domains,
            );
            table_blocks.push(block);
        }

        // Collect used enum infos for class generation
        let used_enums: Vec<&EnumInfo> = all_enums
            .iter()
            .filter(|ei| {
                used_enum_names.contains(&ei.name)
                    || used_enum_names.contains(&enum_class_name(&ei.name))
            })
            .collect();

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

#[allow(clippy::too_many_arguments)]
fn generate_table(
    table: &TableInfo,
    imports: &mut ImportCollector,
    options: &GeneratorOptions,
    dialect: Dialect,
    enums: &[EnumInfo],
    synthetic_enum_cols: &std::collections::HashMap<(String, String), String>,
    boolean_cols: &std::collections::HashSet<(String, String)>,
    schema_domains: &[crate::schema::DomainInfo],
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

        // Check if column is a boolean (detected from IN (0, 1) check on integer types)
        let bool_key = (table.name.clone(), col.name.clone());
        let is_integer_type = matches!(
            col.udt_name.as_str(),
            "int2" | "int4" | "int8" | "integer" | "smallint" | "bigint" | "tinyint" | "int"
        );
        if boolean_cols.contains(&bool_key) && is_integer_type {
            imports.add("sqlalchemy", "Boolean");
            col_args.push("Boolean".to_string());
        }
        // Check if column has a synthetic enum from check constraint
        else if let Some(class_name) = synthetic_enum_cols.get(&bool_key) {
            col_args.push(format!(
                "Enum({class_name}, values_callable=lambda cls: [member.value for member in cls])"
            ));
            // Note: sqlacodegen doesn't emit native_enum/create_constraint for synthetic enums.
            // If needed for DDL correctness, add: native_enum=False, create_constraint=False
        }
        // Check if column type is a named enum
        else if let Some(ei) = find_enum_for_column(&col.udt_name, enums) {
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
            // Check for domain type — resolve to DOMAIN('name', BaseType(), ...) (PG only)
            let domain = if dialect == Dialect::Postgres {
                schema_domains.iter().find(|d| d.name == col.udt_name)
            } else {
                None
            };
            if let Some(di) = domain {
                imports.add("sqlalchemy.dialects.postgresql", "DOMAIN");
                // Resolve base type
                let base_col = crate::schema::ColumnInfo {
                    udt_name: di.base_type.clone(),
                    ..col.clone()
                };
                let base_mapped = if options.keep_dialect_types {
                    map_column_type_dialect(&base_col, dialect)
                } else {
                    map_column_type(&base_col, dialect)
                };
                imports.add(&base_mapped.import_module, &base_mapped.import_name);

                let mut domain_args = vec![
                    format_python_string_literal(&di.name),
                    format!("{}()", base_mapped.sa_type),
                ];
                if let Some(ref cn) = di.constraint_name {
                    domain_args.push(format!(
                        "constraint_name={}",
                        format_python_string_literal(cn)
                    ));
                }
                domain_args.push(format!(
                    "not_null={}",
                    if di.not_null { "True" } else { "False" }
                ));
                if let Some(ref check) = di.check_expression {
                    imports.add("sqlalchemy", "text");
                    domain_args.push(format!("check={}", format_server_default(check, dialect)));
                }
                col_args.push(format!("DOMAIN({})", domain_args.join(", ")));
            } else {
                let mapped = if options.keep_dialect_types {
                    map_column_type_dialect(col, dialect)
                } else {
                    map_column_type(col, dialect)
                };
                imports.add(&mapped.import_module, &mapped.import_name);
                if let Some((ref elem_mod, ref elem_name)) = mapped.element_import {
                    imports.add(elem_mod, elem_name);
                }
                col_args.push(mapped.sa_type.clone());
            }
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
                Dialect::Mssql | Dialect::Mysql | Dialect::Sqlite => {
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

        // Server default / Sequence
        if let Some(ref default) = col.column_default {
            if is_serial_default(default, dialect) {
                // Check for non-standard sequence name → emit Sequence()
                if let Some(full_seq_name) = parse_sequence_name(default) {
                    // Strip schema prefix for standard name check
                    let bare_name = full_seq_name.rsplit('.').next().unwrap_or(&full_seq_name);
                    if !is_standard_sequence_name(bare_name, &table.name, &col.name) {
                        imports.add("sqlalchemy", "Sequence");
                        // Split schema.name if present (use last dot for robustness)
                        if let Some((seq_schema, seq_name)) = full_seq_name.rsplit_once('.') {
                            col_args.push(format!(
                                "Sequence({}, schema={})",
                                format_python_string_literal(seq_name),
                                format_python_string_literal(seq_schema)
                            ));
                        } else {
                            col_args.push(format!(
                                "Sequence({})",
                                format_python_string_literal(&full_seq_name)
                            ));
                        }
                    }
                }
            } else {
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
                    let local_cols: Vec<String> = constraint
                        .columns
                        .iter()
                        .map(|c| format!("'{c}'"))
                        .collect();
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
                    // Skip boolean detection check constraints (already handled as Boolean type)
                    if parse_check_boolean(expr).is_some() {
                        continue;
                    }
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
            let kwargs_str = format_index_kwargs(&index.kwargs);
            body_items.push(format!(
                "Index('{}', {}{}{})",
                index.name,
                cols.join(", "),
                unique_str,
                kwargs_str
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
#[path = "tables_tests.rs"]
mod tests;
