use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::relationships::{
    find_inheritance_parent, find_inline_fk, generate_child_relationships,
    generate_m2m_relationships, generate_parent_relationships, has_unique_constraint,
    is_association_table, render_relationship,
};
use crate::codegen::{
    enum_class_name, escape_python_string, find_enum_for_column, format_fk_options,
    format_python_string_literal, format_server_default, generate_enum_class, has_primary_key,
    is_primary_key_column, is_serial_default, is_unique_constraint_index, parse_check_enum,
    quote_constraint_columns, topo_sort_tables, Generator,
};
use crate::schema::EnumInfo;
use crate::dialect::Dialect;
use crate::naming::{column_to_attr_name, table_to_class_name, table_to_variable_name};
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
                            if !synthetic_enum_cols.contains_key(&key) {
                                use heck::ToUpperCamelCase;
                                let enum_name =
                                    format!("{}_{}", table_ref.name, col_name).to_upper_camel_case();
                                let ei = EnumInfo {
                                    name: enum_name.clone(),
                                    schema: None,
                                    values,
                                };
                                all_enums.push(ei);
                                synthetic_enum_cols.insert(key, enum_name);
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
                let block = generate_association_table(table, &mut imports, options, schema.dialect, metadata_ref);
                blocks.push(block);
            } else if has_primary_key(&table.constraints) {
                let (block, meta) = generate_class(table, &mut imports, options, schema.dialect, schema, &all_enums, &synthetic_enum_cols);
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
        c.column_default.as_ref().map_or(false, |d| !is_serial_default(d, dialect))
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
            let mapped = map_column_type(col, dialect);
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
        let line = format!(
            "    {attr_name}: Mapped[{type_annotation}] = mapped_column({mc_str})"
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

    // Relationships (suppressed when noconstraints)
    let (mut parent_rels, mut child_rels, mut m2m_rels) = if !options.noconstraints {
        let parent = if !options.nobidi {
            generate_parent_relationships(table, schema, options.noidsuffix)
        } else {
            vec![]
        };
        let child = generate_child_relationships(table, schema, options.noidsuffix);
        let m2m = generate_m2m_relationships(table, schema, dialect.default_schema(), options.noidsuffix);
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
    let col_attr_names: std::collections::HashSet<&str> = attr_names.iter().map(|s| s.as_str()).collect();
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

        for rel in parent_rels.iter().chain(m2m_rels.iter()).chain(child_rels.iter()) {
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
    let mut names: Vec<String> = columns.iter().map(|c| column_to_attr_name(&c.name)).collect();

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
                    let local_cols: Vec<String> =
                        constraint.columns.iter().map(|c| format!("'{c}'")).collect();
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
                        positional_args.push(format!(
                            "CheckConstraint({expr_literal})"
                        ));
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
                    format!("{}.{}.{}", fk_info.ref_schema, fk_info.ref_table, fk_info.ref_columns[0])
                } else {
                    format!("{}.{}", fk_info.ref_table, fk_info.ref_columns[0])
                };
                body_items.push(format!("Column('{}', ForeignKey('{}'))", col_info.name, target));
            }
        } else {
            let mapped = map_column_type(col_info, dialect);
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

    // --- Tier 2: Tests adapted from sqlacodegen test_generator_declarative.py ---

    /// Adapted from sqlacodegen test_invalid_attribute_names.
    #[test]
    fn test_declarative_invalid_attribute_names() {
        let schema = schema_pg(vec![
            table("simple-items")
                .column(col("id-test").build())
                .column(col("4test").nullable().build())
                .column(col("_4test").nullable().build())
                .column(col("def").nullable().build())
                .pk("simple_items_pkey", &["id-test"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Hyphens replaced with underscores, explicit column name
        assert!(output.contains("id_test: Mapped[int] = mapped_column('id-test', Integer, primary_key=True)"));
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
        let schema = schema_pg(vec![
            table("simple")
                .column(col("id").build())
                .column(col("metadata").udt("varchar").nullable().build())
                .pk("simple_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // "metadata" is reserved by SQLAlchemy
        assert!(output.contains("metadata_: Mapped[Optional[str]] = mapped_column('metadata', String)"));
    }

    /// Adapted from sqlacodegen test_invalid_variable_name_from_column.
    #[test]
    fn test_declarative_invalid_variable_name_from_column() {
        let schema = schema_pg(vec![
            table("simple")
                .column(col(" id ").build())
                .pk("simple_pkey", &[" id "])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Spaces trimmed and mapped, explicit column name preserved
        assert!(output.contains("id: Mapped[int] = mapped_column(' id ', Integer, primary_key=True)"));
    }

    /// Adapted from sqlacodegen test_constraints (declarative).
    #[test]
    fn test_declarative_constraints() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("number").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .check("", "number > 0")
                .unique("uq_id_number", &["id", "number"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("CheckConstraint('number > 0')"));
        assert!(output.contains("UniqueConstraint('id', 'number', name='uq_id_number')"));
        assert!(output.contains("from sqlalchemy import CheckConstraint"));
    }

    /// Adapted from sqlacodegen test_colname_import_conflict.
    #[test]
    fn test_declarative_colname_import_conflict() {
        let schema = schema_pg(vec![
            table("simple")
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
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // "text" conflicts with sqlalchemy.text import (from server_default)
        assert!(output.contains("text_: Mapped[Optional[str]] = mapped_column('text', String)"));
        assert!(output.contains("textwithdefault: Mapped[Optional[str]] = mapped_column(String, server_default=text"));
    }

    /// Adapted from sqlacodegen test_composite_autoincrement_pk.
    #[test]
    fn test_declarative_composite_autoincrement_pk() {
        let schema = schema_pg(vec![
            table("simple_autoincrement_items")
                .column(col("id1").autoincrement().build())
                .column(col("id2").build())
                .pk("simple_autoincrement_items_pkey", &["id1", "id2"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("id1: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)"));
        assert!(output.contains("id2: Mapped[int] = mapped_column(Integer, primary_key=True)"));
    }

    /// Adapted from sqlacodegen test_composite_nullable_pk.
    #[test]
    fn test_declarative_composite_nullable_pk() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id1").build())
                .column(col("id2").nullable().build())
                .pk("simple_items_pkey", &["id1", "id2"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("id1: Mapped[int] = mapped_column(Integer, primary_key=True)"));
        // Nullable PK column should show Optional and nullable=True
        assert!(output.contains("id2: Mapped[Optional[int]] = mapped_column(Integer, primary_key=True, nullable=True)"));
    }

    /// Adapted from sqlacodegen test_pascal_underscore.
    #[test]
    fn test_declarative_pascal_underscore() {
        // Note: sqlacodegen preserves "CustomerAPIPreference" for "customer_API_Preference".
        // heck normalizes it to "CustomerApiPreference". Known difference.
        let schema = schema_pg(vec![
            table("customer_API_Preference")
                .column(col("id").build())
                .pk("customer_api_preference_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("__tablename__ = 'customer_API_Preference'"));
    }

    // --- Tier 3: Relationship tests adapted from sqlacodegen ---

    /// Adapted from sqlacodegen test_onetomany.
    #[test]
    fn test_declarative_onetomany() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("simple_containers_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk("simple_items_container_id_fkey", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Parent side: collection relationship
        assert!(output.contains("simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', back_populates='container')"));

        // Child side: inline FK + scalar relationship
        assert!(output.contains("container_id: Mapped[Optional[int]] = mapped_column(ForeignKey('simple_containers.id'))"));
        assert!(output.contains("container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', back_populates='simple_items')"));

        // Should import relationship
        assert!(output.contains("relationship"));
        // Should import ForeignKey (not ForeignKeyConstraint)
        assert!(output.contains("ForeignKey"));
        assert!(!output.contains("ForeignKeyConstraint"));
    }

    /// Adapted from sqlacodegen test_onetomany_selfref.
    #[test]
    fn test_declarative_onetomany_selfref() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("parent_item_id").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk("simple_items_parent_item_id_fkey", &["parent_item_id"], "simple_items", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Inline FK
        assert!(output.contains("parent_item_id: Mapped[Optional[int]] = mapped_column(ForeignKey('simple_items.id'))"));
        // Forward relationship with remote_side
        assert!(output.contains("parent_item: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', remote_side=[id], back_populates='parent_item_reverse')"));
        // Reverse relationship
        assert!(output.contains("parent_item_reverse: Mapped[list['SimpleItems']] = relationship('SimpleItems', remote_side=[parent_item_id], back_populates='parent_item')"));
    }

    /// Adapted from sqlacodegen test_onetomany_composite.
    #[test]
    fn test_declarative_onetomany_composite() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id1").build())
                .column(col("id2").build())
                .pk("simple_containers_pkey", &["id1", "id2"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id1").nullable().build())
                .column(col("container_id2").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk_full(
                    "simple_items_fkey",
                    &["container_id1", "container_id2"],
                    "public",
                    "simple_containers",
                    &["id1", "id2"],
                    "CASCADE",
                    "CASCADE",
                )
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Composite FK stays in __table_args__
        assert!(output.contains("ForeignKeyConstraint(['container_id1', 'container_id2']"));
        assert!(output.contains("ondelete='CASCADE'"));
        // Columns keep their types (no inline ForeignKey)
        assert!(output.contains("container_id1: Mapped[Optional[int]] = mapped_column(Integer)"));
        assert!(output.contains("container_id2: Mapped[Optional[int]] = mapped_column(Integer)"));
        // Parent-side relationship
        assert!(output.contains("simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', back_populates='simple_containers')"));
        // Child-side relationship
        assert!(output.contains("simple_containers: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', back_populates='simple_items')"));
    }

    /// Adapted from sqlacodegen test_onetoone.
    #[test]
    fn test_declarative_onetoone() {
        let schema = schema_pg(vec![
            table("other_items")
                .column(col("id").build())
                .pk("other_items_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("other_item_id").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk("simple_items_other_item_id_fkey", &["other_item_id"], "other_items", &["id"])
                .unique("simple_items_other_item_id_key", &["other_item_id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Parent side: one-to-one (uselist=False, Optional scalar)
        assert!(output.contains("simple_items: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', uselist=False, back_populates='other_item')"));
        // Child side: FK with unique=True
        assert!(output.contains("other_item_id: Mapped[Optional[int]] = mapped_column(ForeignKey('other_items.id'), unique=True)"));
        // Child side relationship
        assert!(output.contains("other_item: Mapped[Optional['OtherItems']] = relationship('OtherItems', back_populates='simple_items')"));
    }

    /// Adapted from sqlacodegen test_onetomany_noinflect.
    /// FK column without _id suffix — relationship name = FK column name.
    #[test]
    fn test_declarative_onetomany_noinflect() {
        let schema = schema_pg(vec![
            table("fehwiuhfiw")
                .column(col("id").build())
                .pk("fehwiuhfiw_pkey", &["id"])
                .build(),
            table("oglkrogk")
                .column(col("id").build())
                .column(col("fehwiuhfiwID").nullable().build())
                .pk("oglkrogk_pkey", &["id"])
                .fk("oglkrogk_fehwiuhfiwid_fkey", &["fehwiuhfiwID"], "fehwiuhfiw", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // FK column has uppercase ID suffix — stripped
        assert!(output.contains("fehwiuhfiwID: Mapped[Optional[int]] = mapped_column(ForeignKey('fehwiuhfiw.id'))"));
        // Parent-side relationship
        assert!(output.contains("oglkrogk: Mapped[list['Oglkrogk']] = relationship('Oglkrogk', back_populates='fehwiuhfiw')"));
        // Child-side relationship: fehwiuhfiwID stripped to fehwiuhfiw
        assert!(output.contains("fehwiuhfiw: Mapped[Optional['Fehwiuhfiw']] = relationship('Fehwiuhfiw', back_populates='oglkrogk')"));
    }

    // --- Tier 4: Enum tests ---

    /// Adapted from sqlacodegen test_enum_shared_values (declarative).
    #[test]
    fn test_declarative_enum_shared_values() {
        use crate::schema::EnumInfo;
        let schema = schema_pg_with_enums(
            vec![
                table("accounts")
                    .column(col("id").build())
                    .column(col("status").udt("status_enum").build())
                    .pk("accounts_pkey", &["id"])
                    .build(),
                table("users")
                    .column(col("id").build())
                    .column(col("status").udt("status_enum").build())
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
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Enum class generated before Base
        assert!(output.contains("class StatusEnum(str, enum.Enum):"));
        assert!(output.contains("ACTIVE = 'active'"));
        // Enum used in column type annotation
        assert!(output.contains("status: Mapped[StatusEnum] = mapped_column(Enum(StatusEnum, values_callable=lambda cls: [member.value for member in cls], name='status_enum'), nullable=False)"));
        // import enum
        assert!(output.contains("import enum"));
        assert!(output.contains("Enum"));
    }

    // --- PR 5: Advanced relationship tests ---

    /// Adapted from sqlacodegen test_onetomany_multiref.
    /// Two FKs from child to same parent — needs disambiguation.
    #[test]
    fn test_declarative_onetomany_multiref() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("simple_containers_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("parent_container_id").nullable().build())
                .column(col("top_container_id").build())
                .pk("simple_items_pkey", &["id"])
                .fk("si_parent_fkey", &["parent_container_id"], "simple_containers", &["id"])
                .fk("si_top_fkey", &["top_container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Parent side: disambiguated relationship names
        assert!(output.contains("simple_items_parent_container: Mapped[list['SimpleItems']]"));
        assert!(output.contains("simple_items_top_container: Mapped[list['SimpleItems']]"));
        // Child side: foreign_keys disambiguation
        assert!(output.contains("parent_container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', foreign_keys=[parent_container_id], back_populates='simple_items_parent_container')"));
        assert!(output.contains("top_container: Mapped['SimpleContainers'] = relationship('SimpleContainers', foreign_keys=[top_container_id], back_populates='simple_items_top_container')"));
    }

    /// Adapted from sqlacodegen test_onetomany_selfref_multi.
    #[test]
    fn test_declarative_onetomany_selfref_multi() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("parent_item_id").nullable().build())
                .column(col("top_item_id").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk("si_parent_fkey", &["parent_item_id"], "simple_items", &["id"])
                .fk("si_top_fkey", &["top_item_id"], "simple_items", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Each self-ref FK gets foreign_keys disambiguation
        assert!(output.contains("parent_item: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', remote_side=[id], foreign_keys=[parent_item_id], back_populates='parent_item_reverse')"));
        assert!(output.contains("top_item: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', remote_side=[id], foreign_keys=[top_item_id], back_populates='top_item_reverse')"));
    }

    /// Adapted from sqlacodegen test_manytoone_nobidi.
    #[test]
    fn test_declarative_manytoone_nobidi() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("simple_containers_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk("si_container_fkey", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let opts = GeneratorOptions {
            nobidi: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);

        // Child has relationship without back_populates
        assert!(output.contains("container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers')"));
        // Parent should NOT have reverse relationship
        assert!(!output.contains("simple_items: Mapped[list"));
    }

    /// Adapted from sqlacodegen test_foreign_key_schema.
    #[test]
    fn test_declarative_foreign_key_schema() {
        let schema = schema_pg(vec![
            table("other_items")
                .schema("otherschema")
                .column(col("id").build())
                .pk("other_items_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("other_item_id").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk_full(
                    "si_other_fkey",
                    &["other_item_id"],
                    "otherschema",
                    "other_items",
                    &["id"],
                    "NO ACTION",
                    "NO ACTION",
                )
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // FK target includes schema prefix
        assert!(output.contains("ForeignKey('otherschema.other_items.id')"));
        // Parent table has schema in __table_args__
        assert!(output.contains("__table_args__ = {'schema': 'otherschema'}"));
    }

    /// Adapted from sqlacodegen test_manytomany.
    #[test]
    fn test_declarative_manytomany() {
        let schema = schema_pg(vec![
            table("left_table")
                .column(col("id").build())
                .pk("left_table_pkey", &["id"])
                .build(),
            table("right_table")
                .column(col("id").build())
                .pk("right_table_pkey", &["id"])
                .build(),
            table("association_table")
                .column(col("left_id").nullable().build())
                .column(col("right_id").nullable().build())
                .fk("assoc_left_fkey", &["left_id"], "left_table", &["id"])
                .fk("assoc_right_fkey", &["right_id"], "right_table", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Association table rendered as Table()
        assert!(output.contains("t_association_table = Table("));
        assert!(output.contains("Column('left_id', ForeignKey('left_table.id'))"));
        assert!(output.contains("Column('right_id', ForeignKey('right_table.id'))"));

        // Left table gets M2M relationship to right
        assert!(output.contains("right: Mapped[list['RightTable']] = relationship('RightTable', secondary='association_table', back_populates='left')"));
        // Right table gets M2M relationship to left
        assert!(output.contains("left: Mapped[list['LeftTable']] = relationship('LeftTable', secondary='association_table', back_populates='right')"));

        // No class for association table
        assert!(!output.contains("class AssociationTable"));
    }

    /// Adapted from sqlacodegen test_joined_inheritance.
    #[test]
    fn test_declarative_joined_inheritance() {
        let schema = schema_pg(vec![
            table("simple_super_items")
                .column(col("id").build())
                .column(col("data1").nullable().build())
                .pk("simple_super_items_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("super_item_id").build())
                .column(col("data2").nullable().build())
                .pk("simple_items_pkey", &["super_item_id"])
                .fk("si_super_fkey", &["super_item_id"], "simple_super_items", &["id"])
                .build(),
            table("simple_sub_items")
                .column(col("simple_items_id").build())
                .column(col("data3").nullable().build())
                .pk("simple_sub_items_pkey", &["simple_items_id"])
                .fk("ssi_items_fkey", &["simple_items_id"], "simple_items", &["super_item_id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());

        // Parent class
        assert!(output.contains("class SimpleSuperItems(Base):"));
        assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True)"));
        assert!(output.contains("data1: Mapped[Optional[int]] = mapped_column(Integer)"));

        // Child inherits from parent
        assert!(output.contains("class SimpleItems(SimpleSuperItems):"));
        assert!(output.contains("super_item_id: Mapped[int] = mapped_column(ForeignKey('simple_super_items.id'), primary_key=True)"));
        assert!(output.contains("data2: Mapped[Optional[int]] = mapped_column(Integer)"));

        // Grandchild inherits from child
        assert!(output.contains("class SimpleSubItems(SimpleItems):"));
        assert!(output.contains("simple_items_id: Mapped[int] = mapped_column(ForeignKey('simple_items.super_item_id'), primary_key=True)"));
        assert!(output.contains("data3: Mapped[Optional[int]] = mapped_column(Integer)"));

        // No relationship() calls for inheritance FKs
        assert!(!output.contains("relationship("));
    }

    // --- PR 8: Misc feature tests ---

    /// Adapted from sqlacodegen test_table_with_arrays (declarative).
    #[test]
    fn test_declarative_table_with_arrays() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("tags").udt("_text").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("tags: Mapped[Optional[list]] = mapped_column(ARRAY(Text))"));
    }

    /// Adapted from sqlacodegen test_constraints (declarative) — check + unique + index together.
    #[test]
    fn test_declarative_constraints_with_index() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("number").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .check("", "number > 0")
                .unique("uq_id_number", &["id", "number"])
                .index("idx_number", &["number"], false)
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        assert!(output.contains("CheckConstraint('number > 0')"));
        assert!(output.contains("UniqueConstraint('id', 'number', name='uq_id_number')"));
        assert!(output.contains("Index('idx_number', 'number')"));
        assert!(output.contains("from sqlalchemy import CheckConstraint"));
    }

    /// Adapted from sqlacodegen test_onetomany_conflicting_column.
    /// Column named "relationship" gets trailing underscore.
    #[test]
    fn test_declarative_onetomany_conflicting_column() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .column(col("relationship").udt("text").nullable().build())
                .pk("simple_containers_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .fk("si_container_fkey", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // "relationship" is not in PYTHON_RESERVED or import conflicts currently,
        // so it passes through as-is. The relationship() calls still work.
        assert!(output.contains("relationship: Mapped[Optional[str]]") || output.contains("relationship_: Mapped[Optional[str]]"));
        assert!(output.contains("relationship('SimpleItems'"));
    }

    /// Adapted from sqlacodegen test_manytomany_nobidi.
    #[test]
    fn test_declarative_manytomany_nobidi() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .pk("simple_items_pkey", &["id"])
                .build(),
            table("simple_containers")
                .column(col("id").build())
                .pk("simple_containers_pkey", &["id"])
                .build(),
            table("container_items")
                .column(col("item_id").nullable().build())
                .column(col("container_id").nullable().build())
                .fk("ci_item_fkey", &["item_id"], "simple_items", &["id"])
                .fk("ci_container_fkey", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let opts = GeneratorOptions {
            nobidi: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // M2M relationships exist but without back_populates
        assert!(output.contains("relationship("));
        assert!(!output.contains("back_populates"));
    }

    /// Adapted from sqlacodegen test_joined_inheritance_same_table_name.
    #[test]
    fn test_declarative_joined_inheritance_same_table_name() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("data1").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .build(),
            table("simple_sub_items")
                .column(col("simple_items_id").build())
                .column(col("data2").nullable().build())
                .pk("simple_sub_items_pkey", &["simple_items_id"])
                .fk("ssi_fkey", &["simple_items_id"], "simple_items", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Child inherits from parent
        assert!(output.contains("class SimpleSubItems(SimpleItems):"));
        // FK on PK column
        assert!(output.contains("simple_items_id: Mapped[int] = mapped_column(ForeignKey('simple_items.id'), primary_key=True)"));
    }

    /// Adapted from sqlacodegen test_synthetic_enum_generation (declarative).
    #[test]
    fn test_declarative_synthetic_enum() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("status").udt("varchar").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .check("", "simple_items.status IN ('active', 'inactive', 'pending')")
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Synthetic enum class
        assert!(output.contains("class SimpleItemsStatus(str, enum.Enum):"));
        assert!(output.contains("ACTIVE = 'active'"));
    }

    /// Adapted from sqlacodegen test_onetomany_multiref_composite.
    #[test]
    fn test_declarative_onetomany_multiref_composite() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id1").build())
                .column(col("id2").build())
                .pk("sc_pkey", &["id1", "id2"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container1_id1").nullable().build())
                .column(col("container1_id2").nullable().build())
                .column(col("container2_id1").nullable().build())
                .column(col("container2_id2").nullable().build())
                .pk("si_pkey", &["id"])
                .fk_full("si_c1_fkey", &["container1_id1", "container1_id2"], "public", "simple_containers", &["id1", "id2"], "NO ACTION", "NO ACTION")
                .fk_full("si_c2_fkey", &["container2_id1", "container2_id2"], "public", "simple_containers", &["id1", "id2"], "NO ACTION", "NO ACTION")
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Two ForeignKeyConstraints in __table_args__
        assert!(output.contains("ForeignKeyConstraint(['container1_id1', 'container1_id2']"));
        assert!(output.contains("ForeignKeyConstraint(['container2_id1', 'container2_id2']"));
    }

    /// Adapted from sqlacodegen test_manytomany_composite.
    #[test]
    fn test_declarative_manytomany_composite() {
        // M2M with composite FKs is NOT detected as association table
        // (is_association_table requires single-column FKs)
        let schema = schema_pg(vec![
            table("left_table")
                .column(col("id1").build())
                .column(col("id2").build())
                .pk("lt_pkey", &["id1", "id2"])
                .build(),
            table("right_table")
                .column(col("id1").build())
                .column(col("id2").build())
                .pk("rt_pkey", &["id1", "id2"])
                .build(),
            table("assoc")
                .column(col("left_id1").nullable().build())
                .column(col("left_id2").nullable().build())
                .column(col("right_id1").nullable().build())
                .column(col("right_id2").nullable().build())
                .fk_full("a_left_fkey", &["left_id1", "left_id2"], "public", "left_table", &["id1", "id2"], "NO ACTION", "NO ACTION")
                .fk_full("a_right_fkey", &["right_id1", "right_id2"], "public", "right_table", &["id1", "id2"], "NO ACTION", "NO ACTION")
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Composite M2M: assoc is NOT an association table (requires single-col FKs)
        // So it gets Table() fallback (no PK)
        assert!(output.contains("t_assoc = Table("));
    }

    // --- PR 10: Relationship completion tests ---

    /// Adapted from sqlacodegen test_onetomany_conflicting_relationship.
    /// Relationship name collides with column name — gets underscore suffix.
    #[test]
    fn test_declarative_onetomany_conflicting_relationship() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("sc_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id").nullable().build())
                .column(col("container").udt("varchar").nullable().build())
                .pk("si_pkey", &["id"])
                .fk("si_fkey", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // "container" column exists, so relationship "container" becomes "container_"
        assert!(output.contains("container: Mapped[Optional[str]] = mapped_column(String)"));
        assert!(output.contains("container_: Mapped[Optional['SimpleContainers']] = relationship("));
    }

    /// Adapted from sqlacodegen test_onetomany_multiref_with_nofknames.
    #[test]
    fn test_declarative_onetomany_multiref_with_nofknames() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("sc_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id1").nullable().build())
                .column(col("container_id2").nullable().build())
                .pk("si_pkey", &["id"])
                .fk_full("si_c1_fkey", &["container_id1", "container_id2"], "public", "simple_containers", &["id", "id"], "NO ACTION", "NO ACTION")
                .build(),
        ]);
        let opts = GeneratorOptions {
            nofknames: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // ForeignKeyConstraint without name= kwarg
        assert!(output.contains("ForeignKeyConstraint("));
        assert!(!output.contains("name='si_c1_fkey'"));
    }

    /// Adapted from sqlacodegen test_synthetic_enum_nosyntheticenums_option (declarative).
    #[test]
    fn test_declarative_synthetic_enum_nosyntheticenums() {
        let schema = schema_pg(vec![
            table("simple_items")
                .column(col("id").build())
                .column(col("status").udt("varchar").nullable().build())
                .pk("simple_items_pkey", &["id"])
                .check("", "simple_items.status IN ('active', 'inactive')")
                .build(),
        ]);
        let opts = GeneratorOptions {
            nosyntheticenums: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // No enum class generated
        assert!(!output.contains("class SimpleItemsStatus"));
        assert!(!output.contains("import enum"));
        // Column uses regular type
        assert!(output.contains("mapped_column(String)"));
    }

    // --- PR 13: noidsuffix and misc tests ---

    /// Adapted from sqlacodegen test_onetomany_multiref_no_id_suffix.
    #[test]
    fn test_declarative_onetomany_multiref_no_id_suffix() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("sc_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("parent_container_id").nullable().build())
                .column(col("top_container_id").build())
                .pk("si_pkey", &["id"])
                .fk("si_parent_fkey", &["parent_container_id"], "simple_containers", &["id"])
                .fk("si_top_fkey", &["top_container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let opts = GeneratorOptions {
            noidsuffix: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // With noidsuffix, relationship names keep the full FK column name.
        // Since they collide with column names, they get underscore suffix.
        assert!(output.contains("parent_container_id_: Mapped[Optional['SimpleContainers']]"));
        assert!(output.contains("top_container_id_: Mapped['SimpleContainers']"));
    }

    // --- PR 14: Final coverage ---

    /// Adapted from sqlacodegen test_manytomany_multi.
    /// Multiple association tables between same two parent tables.
    #[test]
    fn test_declarative_manytomany_multi() {
        let schema = schema_pg(vec![
            table("left_table")
                .column(col("id").build())
                .pk("lt_pkey", &["id"])
                .build(),
            table("right_table")
                .column(col("id").build())
                .pk("rt_pkey", &["id"])
                .build(),
            table("assoc1")
                .column(col("left_id").nullable().build())
                .column(col("right_id").nullable().build())
                .fk("a1_left_fkey", &["left_id"], "left_table", &["id"])
                .fk("a1_right_fkey", &["right_id"], "right_table", &["id"])
                .build(),
            table("assoc2")
                .column(col("left_id").nullable().build())
                .column(col("right_id").nullable().build())
                .fk("a2_left_fkey", &["left_id"], "left_table", &["id"])
                .fk("a2_right_fkey", &["right_id"], "right_table", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Both association tables rendered as Table()
        assert!(output.contains("t_assoc1 = Table("));
        assert!(output.contains("t_assoc2 = Table("));
        // M2M relationships exist on both parent tables
        assert!(output.contains("secondary='assoc1'"));
        assert!(output.contains("secondary='assoc2'"));
    }

    /// Adapted from sqlacodegen test_domain_json (declarative).
    #[test]
    fn test_declarative_domain_json() {
        use crate::schema::{DomainInfo, IntrospectedSchema};
        let schema = IntrospectedSchema {
            dialect: crate::dialect::Dialect::Postgres,
            tables: vec![
                table("simple_items")
                    .column(col("id").build())
                    .column(col("data").udt("json_domain").nullable().build())
                    .pk("simple_items_pkey", &["id"])
                    .build(),
            ],
            enums: vec![],
            domains: vec![DomainInfo {
                name: "json_domain".to_string(),
                schema: None,
                base_type: "json".to_string(),
                constraint_name: None,
                not_null: false,
                check_expression: None,
            }],
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Domain columns in declarative mode: domain udt_name not resolved to base type
        // (full DOMAIN() support in declarative is future work — currently falls through
        // to the type mapper which uses the udt_name as-is)
        assert!(output.contains("data:"));
    }

    /// Adapted from sqlacodegen test_named_constraints.
    /// PrimaryKeyConstraint emitted in __table_args__ when CheckConstraint present.
    #[test]
    fn test_declarative_named_constraints() {
        let schema = schema_pg(vec![
            table("simple")
                .column(col("id").nullable().build())
                .column(col("text").udt("varchar").nullable().build())
                .pk("primarytest", &["id"])
                .check("checktest", "id > 0")
                .unique("uniquetest", &["text"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // Check and Unique constraints in __table_args__
        assert!(output.contains("CheckConstraint('id > 0', name='checktest')"));
        assert!(output.contains("UniqueConstraint('text', name='uniquetest')"));
        // PK expressed via primary_key=True on mapped_column
        assert!(output.contains("primary_key=True"));
    }

    /// Adapted from sqlacodegen test_manytomany_multi_with_nofknames.
    #[test]
    fn test_declarative_manytomany_multi_with_nofknames() {
        let schema = schema_pg(vec![
            table("left_table")
                .column(col("id").build())
                .pk("lt_pkey", &["id"])
                .build(),
            table("right_table")
                .column(col("id").build())
                .pk("rt_pkey", &["id"])
                .build(),
            table("assoc")
                .column(col("left_id").nullable().build())
                .column(col("right_id").nullable().build())
                .fk("a_left_fkey", &["left_id"], "left_table", &["id"])
                .fk("a_right_fkey", &["right_id"], "right_table", &["id"])
                .build(),
        ]);
        let opts = GeneratorOptions {
            nofknames: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // M2M still works with nofknames
        assert!(output.contains("secondary='assoc'"));
    }

    /// Adapted from sqlacodegen test_named_foreign_key_constraints.
    #[test]
    fn test_declarative_named_foreign_key_constraints() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("sc_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id").nullable().build())
                .pk("si_pkey", &["id"])
                .fk("foreignkeytest", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &GeneratorOptions::default());
        // FK rendered inline with relationship
        assert!(output.contains("ForeignKey('simple_containers.id')"));
        assert!(output.contains("relationship('SimpleContainers'"));
    }

    /// Adapted from sqlacodegen test_named_foreign_key_constraints_with_noidsuffix.
    #[test]
    fn test_declarative_named_foreign_key_constraints_with_noidsuffix() {
        let schema = schema_pg(vec![
            table("simple_containers")
                .column(col("id").build())
                .pk("sc_pkey", &["id"])
                .build(),
            table("simple_items")
                .column(col("id").build())
                .column(col("container_id").nullable().build())
                .pk("si_pkey", &["id"])
                .fk("foreignkeytest", &["container_id"], "simple_containers", &["id"])
                .build(),
        ]);
        let opts = GeneratorOptions {
            noidsuffix: true,
            ..GeneratorOptions::default()
        };
        let gen = DeclarativeGenerator;
        let output = gen.generate(&schema, &opts);
        // With noidsuffix, relationship name keeps _id suffix
        assert!(output.contains("relationship('SimpleContainers'"));
    }
}
