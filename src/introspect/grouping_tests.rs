use super::grouping::{
    foreign_key_constraints, grouped_indexes, primary_key_constraints, typed_column_constraints,
    ForeignKeyColumn, IndexColumn,
};
use crate::schema::ConstraintType;

#[test]
fn primary_key_constraints_group_columns_by_name() {
    let constraints = primary_key_constraints(
        [
            ("pk_accounts", "tenant_id"),
            ("pk_accounts", "account_id"),
            ("pk_users", "id"),
        ],
        |(name, column)| (name.to_string(), column.to_string()),
    );

    assert_eq!(constraints.len(), 2);
    assert_eq!(constraints[0].name, "pk_accounts");
    assert_eq!(constraints[0].constraint_type, ConstraintType::PrimaryKey);
    assert_eq!(constraints[0].columns, ["tenant_id", "account_id"]);
    assert_eq!(constraints[1].name, "pk_users");
    assert_eq!(constraints[1].columns, ["id"]);
}

#[test]
fn typed_column_constraints_group_pk_and_unique_rows() {
    let constraints = typed_column_constraints(
        [
            ("pk_orders", "PRIMARY KEY", "id"),
            ("uq_orders_ref", "UNIQUE", "tenant_id"),
            ("uq_orders_ref", "UNIQUE", "external_ref"),
            ("ignored", "CHECK", "amount"),
        ],
        |(name, constraint_type, column)| {
            let constraint_type = match constraint_type {
                "PRIMARY KEY" => ConstraintType::PrimaryKey,
                "UNIQUE" => ConstraintType::Unique,
                _ => return None,
            };
            Some((name.to_string(), constraint_type, column.to_string()))
        },
    );

    assert_eq!(constraints.len(), 2);
    assert_eq!(constraints[0].constraint_type, ConstraintType::PrimaryKey);
    assert_eq!(constraints[0].columns, ["id"]);
    assert_eq!(constraints[1].constraint_type, ConstraintType::Unique);
    assert_eq!(constraints[1].columns, ["tenant_id", "external_ref"]);
}

#[test]
fn foreign_key_constraints_group_and_deduplicate_columns() {
    let constraints = foreign_key_constraints([
        fk_part(
            "fk_orders_accounts",
            "tenant_id",
            "public",
            "accounts",
            "tenant_id",
        ),
        fk_part(
            "fk_orders_accounts",
            "account_id",
            "public",
            "accounts",
            "id",
        ),
        fk_part(
            "fk_orders_accounts",
            "account_id",
            "public",
            "accounts",
            "id",
        ),
    ]);

    assert_eq!(constraints.len(), 1);
    let constraint = &constraints[0];
    assert_eq!(constraint.name, "fk_orders_accounts");
    assert_eq!(constraint.constraint_type, ConstraintType::ForeignKey);
    assert_eq!(constraint.columns, ["tenant_id", "account_id"]);

    let foreign_key = constraint.foreign_key.as_ref().unwrap();
    assert_eq!(foreign_key.ref_schema, "public");
    assert_eq!(foreign_key.ref_table, "accounts");
    assert_eq!(foreign_key.ref_columns, ["tenant_id", "id"]);
    assert_eq!(foreign_key.update_rule, "CASCADE");
    assert_eq!(foreign_key.delete_rule, "NO ACTION");
}

#[test]
fn grouped_indexes_skip_expression_only_indexes() {
    let indexes = grouped_indexes([
        index_part("idx_accounts_name", false, Some("name")),
        index_part("idx_accounts_name", false, Some("tenant_id")),
        index_part("idx_expression_only", true, None),
    ]);

    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, "idx_accounts_name");
    assert!(!indexes[0].is_unique);
    assert_eq!(indexes[0].columns, ["name", "tenant_id"]);
}

fn fk_part(
    constraint_name: &str,
    column: &str,
    ref_schema: &str,
    ref_table: &str,
    ref_column: &str,
) -> ForeignKeyColumn {
    ForeignKeyColumn {
        constraint_name: constraint_name.to_string(),
        column: column.to_string(),
        ref_schema: ref_schema.to_string(),
        ref_table: ref_table.to_string(),
        ref_column: ref_column.to_string(),
        update_rule: "CASCADE".to_string(),
        delete_rule: "NO ACTION".to_string(),
    }
}

fn index_part(index_name: &str, is_unique: bool, column: Option<&str>) -> IndexColumn {
    IndexColumn {
        index_name: index_name.to_string(),
        is_unique,
        column: column.map(str::to_string),
    }
}
