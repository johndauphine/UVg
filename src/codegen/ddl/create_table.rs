use crate::cli::DdlOptions;
use crate::codegen::{is_auto_increment_column, is_primary_key_column};
use crate::dialect::Dialect;
use crate::schema::{ConstraintType, TableInfo};

use super::checks::{check_predicate_is_portable, translate_check_predicate};
use super::column::generate_column_def;
use super::ident::{qualified_table_name, quote_identifier};

/// Generate a CREATE TABLE statement.
pub(in crate::codegen) fn generate_create_table(
    table: &TableInfo,
    source_dialect: Dialect,
    target_dialect: Dialect,
    options: &DdlOptions,
) -> String {
    let qname = qualified_table_name(&table.schema, &table.name, source_dialect, target_dialect);
    let mut parts: Vec<String> = Vec::new();
    // Comments for CHECK constraints we dropped because their predicate
    // wasn't portable — kept separate from `parts` so they don't end up
    // on the comma-joined body (would produce trailing/double commas).
    // Emitted after the CREATE TABLE statement closes.
    let mut dropped_check_comments: Vec<String> = Vec::new();

    // Detect if any column has inline PK AUTOINCREMENT (SQLite)
    let has_inline_pk = target_dialect == Dialect::Sqlite
        && table.columns.iter().any(|col| {
            is_auto_increment_column(col, source_dialect)
                && is_primary_key_column(&col.name, &table.constraints)
        });

    // Columns
    for col in &table.columns {
        parts.push(generate_column_def(
            col,
            &table.constraints,
            source_dialect,
            target_dialect,
        ));
    }

    // Constraints
    if !options.noconstraints {
        // Primary key (suppress if SQLite auto-increment already emitted inline PK)
        if !has_inline_pk {
            for c in &table.constraints {
                if c.constraint_type == ConstraintType::PrimaryKey {
                    let cols: Vec<String> = c
                        .columns
                        .iter()
                        .map(|col| quote_identifier(col, target_dialect))
                        .collect();
                    // MySQL stores the PK constraint name as the literal
                    // sentinel "PRIMARY" — that's a magic identifier in
                    // information_schema. Emitting `CONSTRAINT "PRIMARY"`
                    // on PG / MSSQL fails: PG reserves that name as the
                    // underlying index identifier and the second table
                    // hits "relation PRIMARY already exists". Drop the
                    // constraint name when it's the MySQL sentinel and
                    // we're targeting non-mysql; the engine will assign
                    // a sensible default (e.g. PG `<table>_pkey`).
                    let drop_pk_name = c.name == "PRIMARY"
                        && source_dialect == Dialect::Mysql
                        && target_dialect != Dialect::Mysql;
                    if drop_pk_name {
                        parts.push(format!("    PRIMARY KEY ({})", cols.join(", ")));
                    } else {
                        parts.push(format!(
                            "    CONSTRAINT {} PRIMARY KEY ({})",
                            quote_identifier(&c.name, target_dialect),
                            cols.join(", ")
                        ));
                    }
                }
            }
        }

        // Unique constraints
        for c in &table.constraints {
            if c.constraint_type == ConstraintType::Unique {
                let cols: Vec<String> = c
                    .columns
                    .iter()
                    .map(|col| quote_identifier(col, target_dialect))
                    .collect();
                parts.push(format!(
                    "    CONSTRAINT {} UNIQUE ({})",
                    quote_identifier(&c.name, target_dialect),
                    cols.join(", ")
                ));
            }
        }

        // Foreign keys
        for c in &table.constraints {
            if c.constraint_type == ConstraintType::ForeignKey {
                if let Some(ref fk) = c.foreign_key {
                    let cols: Vec<String> = c
                        .columns
                        .iter()
                        .map(|col| quote_identifier(col, target_dialect))
                        .collect();
                    let ref_table = qualified_table_name(
                        &fk.ref_schema,
                        &fk.ref_table,
                        source_dialect,
                        target_dialect,
                    );
                    let ref_cols: Vec<String> = fk
                        .ref_columns
                        .iter()
                        .map(|col| quote_identifier(col, target_dialect))
                        .collect();
                    let mut fk_str = format!(
                        "    CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})",
                        quote_identifier(&c.name, target_dialect),
                        cols.join(", "),
                        ref_table,
                        ref_cols.join(", ")
                    );
                    if fk.delete_rule != "NO ACTION" {
                        fk_str.push_str(&format!(" ON DELETE {}", fk.delete_rule));
                    }
                    if fk.update_rule != "NO ACTION" {
                        fk_str.push_str(&format!(" ON UPDATE {}", fk.update_rule));
                    }
                    parts.push(fk_str);
                }
            }
        }

        // Check constraints. Predicate text is source-verbatim from
        // introspection. Identifier-quote translation (backticks → quotes,
        // brackets → quotes, PG ::casts stripped) is in
        // translate_check_predicate. Cross-dialect predicate-SEMANTICS
        // translation (regex → LIKE, ARRAY[...] → comma-separated lists,
        // PG operator classes → equivalent MSSQL/MySQL forms) is the
        // bigger AST-level problem called out in #33; out of scope here.
        //
        // For cross-dialect runs (source != target), we skip CHECK
        // constraints whose predicate contains tokens we know don't port —
        // emitting them would fail at apply time and abort the table.
        // Same-dialect runs always emit verbatim. The dropped predicate is
        // surfaced as a `-- ` comment so the user can hand-translate.
        //
        // Dropped-check comments go in a parallel vec rather than into
        // `parts`: `parts.join(",\n")` below would leave a trailing comma
        // before `)` if a `-- DROPPED ...` line were the last entry, or
        // produce two commas in a row mid-body. Comments don't get joined
        // with a comma — they're appended after the body close.
        for c in &table.constraints {
            if c.constraint_type == ConstraintType::Check {
                if let Some(ref expr) = c.check_expression {
                    if source_dialect != target_dialect
                        && !check_predicate_is_portable(expr, source_dialect, target_dialect)
                    {
                        dropped_check_comments.push(format!(
                            "-- DROPPED CHECK {}: predicate uses non-portable syntax\n--   source: {}",
                            c.name,
                            expr.replace('\n', " ")
                        ));
                        continue;
                    }
                    let translated_expr =
                        translate_check_predicate(expr, source_dialect, target_dialect);
                    parts.push(format!(
                        "    CONSTRAINT {} CHECK ({})",
                        quote_identifier(&c.name, target_dialect),
                        translated_expr
                    ));
                }
            }
        }
    }

    let body = parts.join(",\n");

    // MySQL table comment is inline
    let table_comment = if !options.nocomments && target_dialect == Dialect::Mysql {
        table
            .comment
            .as_ref()
            .map(|c| format!(" COMMENT '{}'", c.replace('\'', "''")))
            .unwrap_or_default()
    } else {
        String::new()
    };

    let mut output = format!("CREATE TABLE {qname} (\n{body}\n){table_comment};");
    if !dropped_check_comments.is_empty() {
        // Emit dropped-check comments after the CREATE TABLE — they're not
        // part of the statement body, just human-readable notes about
        // constraints uvg couldn't translate.
        output.push('\n');
        for comment in &dropped_check_comments {
            output.push_str(comment);
            output.push('\n');
        }
    }
    output
}
