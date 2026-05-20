use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::{
    format_fk_options, format_index_kwargs, format_python_string_literal,
    is_unique_constraint_index, quote_constraint_columns,
};
use crate::dialect::Dialect;
use crate::schema::{ConstraintType, TableInfo};

pub(super) fn build_table_args(
    table: &TableInfo,
    imports: &mut ImportCollector,
    options: &GeneratorOptions,
    dialect: Dialect,
) -> Option<String> {
    let mut positional_args: Vec<String> = Vec::new();
    let mut kwargs: Vec<String> = Vec::new();

    // Foreign key constraints (only multi-column; single-column FKs are inline on mapped_column).
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

    // Check constraints.
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

    // PrimaryKeyConstraint is not emitted in declarative __table_args__ because
    // it is already expressed via primary_key=True on mapped_column().

    // Unique constraints (all, not just multi-column).
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

    // Indexes.
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

    // Table comment (kwarg).
    if !options.nocomments {
        if let Some(ref comment) = table.comment {
            let lit = format_python_string_literal(comment);
            kwargs.push(format!("'comment': {lit}"));
        }
    }

    // Schema (kwarg, if not default).
    if table.schema != dialect.default_schema() {
        kwargs.push(format!("'schema': '{}'", table.schema));
    }

    if positional_args.is_empty() && kwargs.is_empty() {
        return None;
    }

    if positional_args.is_empty() {
        let dict_str = format!("{{{}}}", kwargs.join(", "));
        return Some(dict_str);
    }

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
