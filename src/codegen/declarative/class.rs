use super::attrs::resolve_attr_names;
use super::table_args::build_table_args;
use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::relationships::{
    find_inheritance_parent, find_inline_fk, generate_child_relationships,
    generate_m2m_relationships, generate_parent_relationships, has_unique_constraint,
    render_relationship,
};
use crate::codegen::{
    enum_class_name, find_enum_for_column, format_python_string_literal, format_server_default,
    is_primary_key_column, is_serial_default,
};
use crate::dialect::Dialect;
use crate::naming::table_to_class_name;
use crate::schema::{EnumInfo, IntrospectedSchema, TableInfo};
use crate::typemap::{map_column_type, map_column_type_dialect};
use std::collections::{HashMap, HashSet};

pub(super) struct ClassMeta {
    pub(super) needs_optional: bool,
    pub(super) needs_datetime: bool,
    pub(super) needs_decimal: bool,
    pub(super) needs_uuid: bool,
}

pub(super) fn generate_class(
    table: &TableInfo,
    imports: &mut ImportCollector,
    options: &GeneratorOptions,
    dialect: Dialect,
    schema: &IntrospectedSchema,
    all_enums: &[EnumInfo],
    synthetic_enum_cols: &HashMap<(String, String), String>,
) -> (String, ClassMeta) {
    let class_name = table_to_class_name(&table.name);
    let mut lines: Vec<String> = Vec::new();
    let mut meta = ClassMeta {
        needs_optional: false,
        needs_datetime: false,
        needs_decimal: false,
        needs_uuid: false,
    };

    // Check for joined table inheritance.
    let parent_table_name = find_inheritance_parent(table, schema);
    let base_class = if let Some(parent_name) = parent_table_name {
        table_to_class_name(parent_name)
    } else {
        "Base".to_string()
    };

    lines.push(format!("class {class_name}({base_class}):"));
    lines.push(format!("    __tablename__ = '{}'", table.name));

    let table_args = build_table_args(table, imports, options, dialect);
    if let Some(args_str) = table_args {
        if args_str.starts_with('{') {
            lines.push(format!("    __table_args__ = {args_str}"));
        } else {
            lines.push(format!("    __table_args__ = (\n{args_str}\n    )"));
        }
    }

    lines.push(String::new());

    struct ColLine {
        is_pk: bool,
        is_nullable: bool,
        line: String,
    }
    let mut col_lines: Vec<ColLine> = Vec::new();

    let will_import_text = table.columns.iter().any(|c| {
        c.column_default
            .as_ref()
            .is_some_and(|d| !is_serial_default(d, dialect))
    });

    let mut attr_names = resolve_attr_names(&table.columns);
    if will_import_text {
        for name in &mut attr_names {
            if name == "text" {
                name.push('_');
            }
        }
    }

    for (idx, col) in table.columns.iter().enumerate() {
        let attr_name = &attr_names[idx];

        let synthetic_key = (table.name.clone(), col.name.clone());
        let synthetic_class = synthetic_enum_cols.get(&synthetic_key);
        let enum_info = if synthetic_class.is_some() {
            None
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

        let type_annotation = if col.is_nullable {
            meta.needs_optional = true;
            format!("Optional[{python_type}]")
        } else {
            python_type.clone()
        };

        let mut mc_args: Vec<String> = Vec::new();

        if *attr_name != col.name {
            mc_args.push(format_python_string_literal(&col.name));
        }

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
            if has_unique_constraint(&col.name, &table.constraints) {
                mc_args.push("unique=True".to_string());
            }
        } else {
            mc_args.push(sa_type_str.clone());
        }

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

        if !col.is_nullable && !is_pk {
            mc_args.push("nullable=False".to_string());
        }

        if is_pk {
            mc_args.push("primary_key=True".to_string());
            if col.is_nullable {
                mc_args.push("nullable=True".to_string());
            }
            if col.autoincrement == Some(true) {
                mc_args.push("autoincrement=True".to_string());
            }
        }

        if let Some(ref default) = col.column_default {
            if !is_serial_default(default, dialect) {
                imports.add("sqlalchemy", "text");
                let formatted = format_server_default(default, dialect);
                mc_args.push(format!("server_default={formatted}"));
            }
        }

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

    if options.nobidi {
        for rel in &mut child_rels {
            rel.back_populates.clear();
        }
        for rel in &mut m2m_rels {
            rel.back_populates.clear();
        }
    }

    let col_attr_names: HashSet<&str> = attr_names.iter().map(|s| s.as_str()).collect();
    let mut rel_attr_names: HashSet<String> = HashSet::new();
    let mut renames: HashMap<String, String> = HashMap::new();

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
        lines.push(String::new());

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
