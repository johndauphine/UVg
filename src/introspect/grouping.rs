use std::collections::BTreeMap;

use crate::schema::{ConstraintInfo, ConstraintType, ForeignKeyInfo, IndexInfo};

pub(crate) struct ForeignKeyColumn {
    pub(crate) constraint_name: String,
    pub(crate) column: String,
    pub(crate) ref_schema: String,
    pub(crate) ref_table: String,
    pub(crate) ref_column: String,
    pub(crate) update_rule: String,
    pub(crate) delete_rule: String,
}

pub(crate) struct IndexColumn {
    pub(crate) index_name: String,
    pub(crate) is_unique: bool,
    pub(crate) column: Option<String>,
}

pub(crate) fn primary_key_constraints<R>(
    rows: impl IntoIterator<Item = R>,
    split: impl FnMut(R) -> (String, String),
) -> Vec<ConstraintInfo> {
    simple_column_constraints(rows, split, ConstraintType::PrimaryKey)
}

pub(crate) fn unique_constraints<R>(
    rows: impl IntoIterator<Item = R>,
    split: impl FnMut(R) -> (String, String),
) -> Vec<ConstraintInfo> {
    simple_column_constraints(rows, split, ConstraintType::Unique)
}

pub(crate) fn typed_column_constraints<R>(
    rows: impl IntoIterator<Item = R>,
    mut split: impl FnMut(R) -> Option<(String, ConstraintType, String)>,
) -> Vec<ConstraintInfo> {
    let mut groups: BTreeMap<String, (ConstraintType, Vec<String>)> = BTreeMap::new();
    for row in rows {
        let Some((name, constraint_type, column)) = split(row) else {
            continue;
        };
        groups
            .entry(name)
            .or_insert_with(|| (constraint_type, Vec::new()))
            .1
            .push(column);
    }

    groups
        .into_iter()
        .filter_map(|(name, (constraint_type, columns))| {
            column_constraint(name, constraint_type, columns)
        })
        .collect()
}

pub(crate) fn foreign_key_constraints(
    rows: impl IntoIterator<Item = ForeignKeyColumn>,
) -> Vec<ConstraintInfo> {
    let mut groups: BTreeMap<String, ForeignKeyAccumulator> = BTreeMap::new();
    for row in rows {
        let acc = groups
            .entry(row.constraint_name)
            .or_insert_with(|| ForeignKeyAccumulator {
                columns: Vec::new(),
                ref_schema: row.ref_schema,
                ref_table: row.ref_table,
                ref_columns: Vec::new(),
                update_rule: row.update_rule,
                delete_rule: row.delete_rule,
            });
        push_unique(&mut acc.columns, row.column);
        push_unique(&mut acc.ref_columns, row.ref_column);
    }

    groups
        .into_iter()
        .map(|(name, acc)| {
            ConstraintInfo::foreign_key(
                name,
                acc.columns,
                ForeignKeyInfo::new(
                    acc.ref_schema,
                    acc.ref_table,
                    acc.ref_columns,
                    acc.update_rule,
                    acc.delete_rule,
                ),
            )
        })
        .collect()
}

pub(crate) fn grouped_indexes(rows: impl IntoIterator<Item = IndexColumn>) -> Vec<IndexInfo> {
    let mut groups: BTreeMap<String, (bool, Vec<String>)> = BTreeMap::new();
    for row in rows {
        let entry = groups
            .entry(row.index_name)
            .or_insert_with(|| (row.is_unique, Vec::new()));
        if let Some(column) = row.column {
            entry.1.push(column);
        }
    }

    groups
        .into_iter()
        .filter(|(_, (_, columns))| !columns.is_empty())
        .map(|(name, (is_unique, columns))| IndexInfo::new(name, is_unique, columns))
        .collect()
}

fn simple_column_constraints<R>(
    rows: impl IntoIterator<Item = R>,
    mut split: impl FnMut(R) -> (String, String),
    constraint_type: ConstraintType,
) -> Vec<ConstraintInfo> {
    let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in rows {
        let (name, column) = split(row);
        groups.entry(name).or_default().push(column);
    }

    groups
        .into_iter()
        .filter_map(|(name, columns)| column_constraint(name, constraint_type.clone(), columns))
        .collect()
}

fn column_constraint(
    name: String,
    constraint_type: ConstraintType,
    columns: Vec<String>,
) -> Option<ConstraintInfo> {
    match constraint_type {
        ConstraintType::PrimaryKey => Some(ConstraintInfo::primary_key(name, columns)),
        ConstraintType::Unique => Some(ConstraintInfo::unique(name, columns)),
        _ => None,
    }
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
    }
}

struct ForeignKeyAccumulator {
    columns: Vec<String>,
    ref_schema: String,
    ref_table: String,
    ref_columns: Vec<String>,
    update_rule: String,
    delete_rule: String,
}
