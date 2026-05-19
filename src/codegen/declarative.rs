use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::relationships::{
    find_inheritance_parent, find_inline_fk, generate_child_relationships,
    generate_m2m_relationships, generate_parent_relationships, has_unique_constraint,
    is_association_table, render_relationship,
};
use crate::codegen::{
    enum_class_name, escape_python_string, find_enum_for_column, format_fk_options,
    format_index_kwargs, format_python_string_literal, format_server_default, generate_enum_class,
    has_primary_key, is_primary_key_column, is_serial_default, is_unique_constraint_index,
    parse_check_enum, quote_constraint_columns, topo_sort_tables, Generator,
};
use crate::dialect::Dialect;
use crate::naming::{column_to_attr_name, table_to_class_name, table_to_variable_name};
use crate::schema::EnumInfo;
use crate::schema::{ConstraintType, IntrospectedSchema, TableInfo};
use crate::typemap::{map_column_type, map_column_type_dialect};

pub struct DeclarativeGenerator;

impl Generator for DeclarativeGenerator {
    fn generate(&self, schema: &IntrospectedSchema, options: &GeneratorOptions) -> String {
        let mut imports = ImportCollector::new();
        let mut blocks: Vec<String> = Vec::new();
        let mut needs_optional = false;
        let mut needs_datetime = false;
        let mut needs_decimal = false;
        let mut needs_uuid = false;

        let has_any_pk = schema
            .tables
            .iter()
            .any(|t| has_primary_key(&t.constraints));
        let has_any_no_pk = schema
            .tables
            .iter()
            .any(|t| !has_primary_key(&t.constraints));

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

        let metadata_ref = if has_any_pk {
            "Base.metadata"
        } else {
            "metadata"
        };

        // Collect named enums and synthetic enums from check constraints
        let mut all_enums: Vec<EnumInfo> = schema.enums.clone();
        let mut synthetic_enum_cols: std::collections::HashMap<(String, String), String> =
            std::collections::HashMap::new();

        let sorted_tables = topo_sort_tables(&schema.tables);

        // Extract synthetic enums from check constraints (unless nosyntheticenums)
        if !options.nosyntheticenums {
            for table_ref in &sorted_tables {
                for constraint in &table_ref.constraints {
                    if constraint.constraint_type == ConstraintType::Check {
                        if let Some(ref expr) = constraint.check_expression {
                            if let Some((col_name, values)) = parse_check_enum(expr) {
                                let key = (table_ref.name.clone(), col_name.clone());
                                if let std::collections::hash_map::Entry::Vacant(entry) =
                                    synthetic_enum_cols.entry(key)
                                {
                                    use heck::ToUpperCamelCase;
                                    let enum_name = format!("{}_{}", table_ref.name, col_name)
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

        // Track which enums are used
        let mut used_enum_names: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for table in &sorted_tables {
            // Only track enum usage for tables that will render Enum() types
            // (classes with PK, not fallback Table() or association tables)
            let renders_enums = has_primary_key(&table.constraints) && !is_association_table(table);
            if renders_enums {
                for col_info in &table.columns {
                    if find_enum_for_column(&col_info.udt_name, &all_enums).is_some() {
                        used_enum_names.insert(col_info.udt_name.clone());
                    }
                    let key = (table.name.clone(), col_info.name.clone());
                    if let Some(class_name) = synthetic_enum_cols.get(&key) {
                        used_enum_names.insert(class_name.clone());
                    }
                }
            }

            if is_association_table(table) {
                // M2M association table: render as Table() with ForeignKey on columns
                let block = generate_association_table(
                    table,
                    &mut imports,
                    options,
                    schema.dialect,
                    metadata_ref,
                );
                blocks.push(block);
            } else if has_primary_key(&table.constraints) {
                let (block, meta) = generate_class(
                    table,
                    &mut imports,
                    options,
                    schema.dialect,
                    schema,
                    &all_enums,
                    &synthetic_enum_cols,
                );
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
                let block = generate_table_fallback(
                    table,
                    &mut imports,
                    options,
                    schema.dialect,
                    metadata_ref,
                );
                blocks.push(block);
            }
        }

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

        // Enum class definitions
        for ei in &used_enums {
            output.push_str("\n\n");
            output.push_str(&generate_enum_class(ei));
        }

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
    schema: &IntrospectedSchema,
    all_enums: &[EnumInfo],
    synthetic_enum_cols: &std::collections::HashMap<(String, String), String>,
) -> (String, ClassMeta) {
    let class_name = table_to_class_name(&table.name);
    let mut lines: Vec<String> = Vec::new();
    let mut meta = ClassMeta {
        needs_optional: false,
        needs_datetime: false,
        needs_decimal: false,
        needs_uuid: false,
    };

    // Check for joined table inheritance
    let parent_table_name = find_inheritance_parent(table, schema);
    let base_class = if let Some(parent_name) = parent_table_name {
        table_to_class_name(parent_name)
    } else {
        "Base".to_string()
    };

    lines.push(format!("class {class_name}({base_class}):"));
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

    // Pre-scan: check if any column has a server_default (which imports `text`)
    let will_import_text = table.columns.iter().any(|c| {
        c.column_default
            .as_ref()
            .is_some_and(|d| !is_serial_default(d, dialect))
    });

    // Pre-compute attribute names and resolve collisions
    let mut attr_names = resolve_attr_names(&table.columns);
    // Resolve import-level conflicts (e.g. column "text" conflicts with imported `text`)
    if will_import_text {
        for name in &mut attr_names {
            if name == "text" {
                name.push('_');
            }
        }
    }

    for (idx, col) in table.columns.iter().enumerate() {
        let attr_name = &attr_names[idx];

        // Resolve column type: check for synthetic enum, then named enum, then regular type mapping
        let synthetic_key = (table.name.clone(), col.name.clone());
        let synthetic_class = synthetic_enum_cols.get(&synthetic_key);
        let enum_info = if synthetic_class.is_some() {
            None // synthetic enums handled separately
        } else {
            find_enum_for_column(&col.udt_name, all_enums)
        };
        let (sa_type_str, python_type) = if let Some(cls) = synthetic_class {
            let sa = format!(
                "Enum({cls}, values_callable=lambda cls: [member.value for member in cls])"
            );
            (sa, cls.clone())
        } else if let Some(ei) = enum_info {
            let cls = enum_class_name(&ei.name);
            let mut enum_parts = vec![
                cls.clone(),
                "values_callable=lambda cls: [member.value for member in cls]".to_string(),
                format!("name={}", format_python_string_literal(&ei.name)),
            ];
            if let Some(ref sch) = ei.schema {
                if !sch.is_empty() {
                    enum_parts.push(format!("schema={}", format_python_string_literal(sch)));
                }
            }
            let sa = format!("Enum({})", enum_parts.join(", "));
            (sa, cls)
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
            if mapped.python_type.starts_with("datetime.") {
                meta.needs_datetime = true;
            }
            if mapped.python_type.starts_with("decimal.") {
                meta.needs_decimal = true;
            }
            if mapped.python_type.starts_with("uuid.") {
                meta.needs_uuid = true;
            }
            (mapped.sa_type.clone(), mapped.python_type.clone())
        };

        let is_pk = is_primary_key_column(&col.name, &table.constraints);

        // Python type annotation
        let type_annotation = if col.is_nullable {
            meta.needs_optional = true;
            format!("Optional[{python_type}]")
        } else {
            python_type.clone()
        };

        // mapped_column arguments
        let mut mc_args: Vec<String> = Vec::new();

        // Explicit column name when attribute name differs
        if *attr_name != col.name {
            mc_args.push(format_python_string_literal(&col.name));
        }

        // Check for single-column FK — use ForeignKey() instead of type
        let inline_fk = if !options.noconstraints {
            find_inline_fk(&col.name, &table.constraints)
        } else {
            None
        };
        if let Some(fk_constraint) = inline_fk {
            if let Some(ref fk) = fk_constraint.foreign_key {
                imports.add("sqlalchemy", "ForeignKey");
                let target = if fk.ref_schema != dialect.default_schema() {
                    format!("'{}.{}.{}'", fk.ref_schema, fk.ref_table, fk.ref_columns[0])
                } else {
                    format!("'{}.{}'", fk.ref_table, fk.ref_columns[0])
                };
                mc_args.push(format!("ForeignKey({target})"));
            }
            // unique=True if FK column has a unique constraint (one-to-one)
            if has_unique_constraint(&col.name, &table.constraints) {
                mc_args.push("unique=True".to_string());
            }
        } else {
            // No inline FK — use SA type
            mc_args.push(sa_type_str.clone());
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
                Dialect::Mssql | Dialect::Mysql | Dialect::Sqlite => {
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
            // Explicitly emit nullable=True for nullable PK columns (composite PKs)
            if col.is_nullable {
                mc_args.push("nullable=True".to_string());
            }
            // Autoincrement on composite PK columns
            if col.autoincrement == Some(true) {
                mc_args.push("autoincrement=True".to_string());
            }
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
        let line = format!("    {attr_name}: Mapped[{type_annotation}] = mapped_column({mc_str})");
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

    for col_line in pk_cols
        .iter()
        .chain(non_nullable.iter())
        .chain(nullable.iter())
    {
        lines.push(col_line.line.clone());
    }

    // Relationships (suppressed when noconstraints)
    let (mut parent_rels, mut child_rels, mut m2m_rels) = if !options.noconstraints {
        let parent = if !options.nobidi {
            generate_parent_relationships(table, schema, options.noidsuffix)
        } else {
            vec![]
        };
        let child = generate_child_relationships(table, schema, options.noidsuffix);
        let m2m =
            generate_m2m_relationships(table, schema, dialect.default_schema(), options.noidsuffix);
        (parent, child, m2m)
    } else {
        (vec![], vec![], vec![])
    };

    // When nobidi, strip back_populates from child and M2M relationships
    if options.nobidi {
        for rel in &mut child_rels {
            rel.back_populates.clear();
        }
        for rel in &mut m2m_rels {
            rel.back_populates.clear();
        }
    }

    // Resolve relationship name conflicts with column attribute names
    let col_attr_names: std::collections::HashSet<&str> =
        attr_names.iter().map(|s| s.as_str()).collect();
    let mut rel_attr_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut renames: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    for rel in parent_rels
        .iter_mut()
        .chain(child_rels.iter_mut())
        .chain(m2m_rels.iter_mut())
    {
        let original = rel.attr_name.clone();
        while col_attr_names.contains(rel.attr_name.as_str())
            || rel_attr_names.contains(&rel.attr_name)
        {
            rel.attr_name.push('_');
        }
        if rel.attr_name != original {
            renames.insert(original, rel.attr_name.clone());
        }
        rel_attr_names.insert(rel.attr_name.clone());
    }

    // Update back_populates references to match renamed attributes
    if !renames.is_empty() {
        for rel in parent_rels
            .iter_mut()
            .chain(child_rels.iter_mut())
            .chain(m2m_rels.iter_mut())
        {
            if let Some(new_name) = renames.get(&rel.back_populates) {
                rel.back_populates = new_name.clone();
            }
        }
    }

    let all_rels_empty = parent_rels.is_empty() && child_rels.is_empty() && m2m_rels.is_empty();
    if !all_rels_empty {
        imports.add("sqlalchemy.orm", "relationship");
        lines.push(String::new()); // blank line before relationships

        for rel in parent_rels
            .iter()
            .chain(m2m_rels.iter())
            .chain(child_rels.iter())
        {
            if rel.is_nullable && !rel.is_collection {
                meta.needs_optional = true;
            }
            lines.push(render_relationship(rel));
        }
    }

    (lines.join("\n"), meta)
}

/// Pre-compute sanitized attribute names for all columns, resolving collisions.
/// When two columns sanitize to the same name, the later one gets a trailing `_`.
fn resolve_attr_names(columns: &[crate::schema::ColumnInfo]) -> Vec<String> {
    let mut names: Vec<String> = columns
        .iter()
        .map(|c| column_to_attr_name(&c.name))
        .collect();

    // Resolve collisions: if name[i] == name[j] where j > i, append _ to name[j]
    for i in 0..names.len() {
        for j in (i + 1)..names.len() {
            if names[j] == names[i] {
                names[j].push('_');
            }
        }
    }

    names
}

fn build_table_args(
    table: &TableInfo,
    imports: &mut ImportCollector,
    options: &GeneratorOptions,
    dialect: Dialect,
) -> Option<String> {
    let mut positional_args: Vec<String> = Vec::new();
    let mut kwargs: Vec<String> = Vec::new();

    // Foreign key constraints (only multi-column; single-column FKs are inline on mapped_column)
    if !options.noconstraints {
        for constraint in &table.constraints {
            if constraint.constraint_type == ConstraintType::ForeignKey
                && constraint.columns.len() > 1
            {
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
                    let name_part = if !options.nofknames {
                        format!(", name='{}'", constraint.name)
                    } else {
                        String::new()
                    };
                    positional_args.push(format!(
                        "ForeignKeyConstraint([{}], [{}]{}{})",
                        local_cols.join(", "),
                        ref_cols.join(", "),
                        name_part,
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
                        positional_args.push(format!("CheckConstraint({expr_literal})"));
                    } else {
                        positional_args.push(format!(
                            "CheckConstraint({expr_literal}, name='{}')",
                            constraint.name
                        ));
                    }
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
            let kwargs_str = format_index_kwargs(&index.kwargs);
            positional_args.push(format!(
                "Index('{}', {}{}{})",
                index.name,
                cols.join(", "),
                unique_str,
                kwargs_str
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
/// Generate a Table() for M2M association tables.
/// Columns use ForeignKey() inline (not ForeignKeyConstraint).
fn generate_association_table(
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

    let mut body_items: Vec<String> = Vec::new();

    for col_info in &table.columns {
        // Find FK for this column
        let fk = find_inline_fk(&col_info.name, &table.constraints);
        if let Some(fk_constraint) = fk {
            if let Some(ref fk_info) = fk_constraint.foreign_key {
                imports.add("sqlalchemy", "ForeignKey");
                let target = if fk_info.ref_schema != dialect.default_schema() {
                    format!(
                        "{}.{}.{}",
                        fk_info.ref_schema, fk_info.ref_table, fk_info.ref_columns[0]
                    )
                } else {
                    format!("{}.{}", fk_info.ref_table, fk_info.ref_columns[0])
                };
                body_items.push(format!(
                    "Column('{}', ForeignKey('{}'))",
                    col_info.name, target
                ));
            }
        } else {
            let mapped = if options.keep_dialect_types {
                map_column_type_dialect(col_info, dialect)
            } else {
                map_column_type(col_info, dialect)
            };
            imports.add(&mapped.import_module, &mapped.import_name);
            body_items.push(format!("Column('{}', {})", col_info.name, mapped.sa_type));
        }
    }

    // Schema (only if not default)
    if table.schema != dialect.default_schema() {
        body_items.push(format!("schema='{}'", table.schema));
    }

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
        let mapped = if options.keep_dialect_types {
            map_column_type_dialect(col, dialect)
        } else {
            map_column_type(col, dialect)
        };
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
                Dialect::Mssql | Dialect::Mysql | Dialect::Sqlite => {
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
                    let name_part = if !options.nofknames {
                        format!(", name='{}'", constraint.name)
                    } else {
                        String::new()
                    };
                    body_items.push(format!(
                        "ForeignKeyConstraint([{}], [{}]{}{})",
                        local_cols.join(", "),
                        ref_cols.join(", "),
                        name_part,
                        fk_opts
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
#[path = "declarative_tests/mod.rs"]
mod tests;
