//! Relationship inference from foreign key constraints.
//!
//! Analyzes FK constraints across tables to determine:
//! - Which columns should use inline `ForeignKey()` vs `ForeignKeyConstraint` in `__table_args__`
//! - What `relationship()` calls to generate on each class

use crate::naming::table_to_class_name;
use crate::schema::{ConstraintInfo, ConstraintType, IntrospectedSchema, TableInfo};

/// A relationship() call to generate on a class.
#[derive(Debug, Clone)]
pub struct RelationshipInfo {
    pub attr_name: String,
    pub target_class: String,
    pub is_collection: bool,
    pub is_nullable: bool,
    pub back_populates: String,
    pub remote_side: Option<String>,
    pub foreign_keys: Option<String>,
    /// Explicit uselist=False (for one-to-one on parent side)
    pub uselist_false: bool,
    /// For M2M: the secondary (association) table name
    pub secondary: Option<String>,
}

/// Find the single-column FK constraint for a given column, if any.
pub fn find_inline_fk<'a>(
    col_name: &str,
    constraints: &'a [ConstraintInfo],
) -> Option<&'a ConstraintInfo> {
    constraints.iter().find(|c| {
        c.constraint_type == ConstraintType::ForeignKey
            && c.columns.len() == 1
            && c.columns[0] == col_name
    })
}

/// Check if a FK constraint is single-column (should be rendered inline).
pub fn is_single_column_fk(constraint: &ConstraintInfo) -> bool {
    constraint.constraint_type == ConstraintType::ForeignKey && constraint.columns.len() == 1
}

/// Check if a column has a unique constraint on it (makes FK one-to-one).
pub fn has_unique_constraint(col_name: &str, constraints: &[ConstraintInfo]) -> bool {
    constraints.iter().any(|c| {
        c.constraint_type == ConstraintType::Unique
            && c.columns.len() == 1
            && c.columns[0] == col_name
    })
}

/// Derive the relationship attribute name on the child side.
/// Strips `_id` suffix from FK column name, also handles uppercase `ID` suffix.
fn fk_col_to_relationship_name(col_name: &str) -> String {
    col_name
        .strip_suffix("_id")
        .or_else(|| col_name.strip_suffix("ID"))
        .unwrap_or(col_name)
        .to_string()
}

/// Count how many FK constraints from `table` reference `target_table`.
fn count_fks_to_table(table: &TableInfo, target_table: &str) -> usize {
    table
        .constraints
        .iter()
        .filter(|c| {
            c.constraint_type == ConstraintType::ForeignKey
                && c.foreign_key
                    .as_ref()
                    .map_or(false, |fk| fk.ref_table == target_table)
        })
        .count()
}

/// Generate relationships for a table based on its FK constraints (child/many side).
pub fn generate_child_relationships(
    table: &TableInfo,
    _schema: &IntrospectedSchema,
) -> Vec<RelationshipInfo> {
    let mut rels = Vec::new();

    let fk_constraints: Vec<&ConstraintInfo> = table
        .constraints
        .iter()
        .filter(|c| c.constraint_type == ConstraintType::ForeignKey)
        .collect();

    // Check if this table uses inheritance (skip the inheritance FK for relationships)
    let inheritance_parent = find_inheritance_parent(table, _schema);

    for constraint in &fk_constraints {
        let fk = match &constraint.foreign_key {
            Some(fk) => fk,
            None => continue,
        };

        // Skip inheritance FK — it's rendered as ForeignKey on mapped_column, not as a relationship
        if inheritance_parent.is_some()
            && is_single_column_fk(constraint)
            && fk.ref_table == inheritance_parent.unwrap()
        {
            continue;
        }

        let target_class = table_to_class_name(&fk.ref_table);
        let is_selfref = fk.ref_table == table.name;
        let multi_ref = count_fks_to_table(table, &fk.ref_table) > 1;

        if is_single_column_fk(constraint) {
            let col_name = &constraint.columns[0];
            let rel_name = fk_col_to_relationship_name(col_name);

            let is_nullable = table
                .columns
                .iter()
                .find(|c| c.name == *col_name)
                .map_or(true, |c| c.is_nullable);

            if is_selfref {
                let reverse_name = format!("{rel_name}_reverse");
                let ref_col = &fk.ref_columns[0];

                rels.push(RelationshipInfo {
                    attr_name: rel_name.clone(),
                    target_class: target_class.clone(),
                    is_collection: false,
                    is_nullable,
                    back_populates: reverse_name.clone(),
                    remote_side: Some(ref_col.to_string()),
                    foreign_keys: if multi_ref {
                        Some(format!("[{col_name}]"))
                    } else {
                        None
                    },
                    uselist_false: false,
                    secondary: None,
                });
                rels.push(RelationshipInfo {
                    attr_name: reverse_name,
                    target_class,
                    is_collection: true,
                    is_nullable: false,
                    back_populates: rel_name,
                    remote_side: Some(col_name.to_string()),
                    foreign_keys: if multi_ref {
                        Some(format!("[{col_name}]"))
                    } else {
                        None
                    },
                    uselist_false: false,
                    secondary: None,
                });
            } else {
                let back_pop = if multi_ref {
                    format!("{}_{}", table.name, rel_name)
                } else {
                    table.name.clone()
                };

                rels.push(RelationshipInfo {
                    attr_name: rel_name,
                    target_class,
                    is_collection: false,
                    is_nullable,
                    back_populates: back_pop,
                    remote_side: None,
                    foreign_keys: if multi_ref {
                        Some(format!("[{col_name}]"))
                    } else {
                        None
                    },
                    uselist_false: false,
                    secondary: None,
                });
            }
        } else {
            // Composite FK
            let is_nullable = constraint.columns.iter().any(|col_name| {
                table
                    .columns
                    .iter()
                    .find(|c| c.name == *col_name)
                    .map_or(true, |c| c.is_nullable)
            });

            let rel_name = fk.ref_table.clone();
            let back_pop = table.name.clone();

            rels.push(RelationshipInfo {
                attr_name: rel_name,
                target_class,
                is_collection: false,
                is_nullable,
                back_populates: back_pop,
                remote_side: None,
                foreign_keys: None,
                uselist_false: false,
                secondary: None,
            });
        }
    }

    rels
}

/// Generate reverse relationships for a parent table based on child FKs pointing to it.
pub fn generate_parent_relationships(
    parent_table: &TableInfo,
    schema: &IntrospectedSchema,
) -> Vec<RelationshipInfo> {
    let mut rels = Vec::new();

    for child_table in &schema.tables {
        if child_table.name == parent_table.name {
            continue;
        }

        // Skip association tables — they generate M2M relationships instead
        if is_association_table(child_table) {
            continue;
        }

        // Skip inheritance children — the FK represents inheritance, not a relationship
        if find_inheritance_parent(child_table, schema).is_some() {
            continue;
        }

        let fk_constraints: Vec<&ConstraintInfo> = child_table
            .constraints
            .iter()
            .filter(|c| {
                c.constraint_type == ConstraintType::ForeignKey
                    && c.foreign_key
                        .as_ref()
                        .map_or(false, |fk| fk.ref_table == parent_table.name)
            })
            .collect();

        let multi_ref = fk_constraints.len() > 1;
        let child_class = table_to_class_name(&child_table.name);

        for constraint in &fk_constraints {
            if is_single_column_fk(constraint) {
                let col_name = &constraint.columns[0];
                let child_rel_name = fk_col_to_relationship_name(col_name);
                let is_onetoone =
                    has_unique_constraint(col_name, &child_table.constraints);

                let attr_name = if multi_ref {
                    format!("{}_{}", child_table.name, child_rel_name)
                } else {
                    child_table.name.clone()
                };

                if is_onetoone {
                    // One-to-one: scalar on parent side with uselist=False
                    rels.push(RelationshipInfo {
                        attr_name,
                        target_class: child_class.clone(),
                        is_collection: false,
                        is_nullable: true,
                        back_populates: child_rel_name,
                        remote_side: None,
                        foreign_keys: if multi_ref {
                            Some(format!("'[{}.{}]'", child_class, col_name))
                        } else {
                            None
                        },
                        uselist_false: true,
                        secondary: None,
                    });
                } else {
                    // One-to-many: list on parent side
                    rels.push(RelationshipInfo {
                        attr_name,
                        target_class: child_class.clone(),
                        is_collection: true,
                        is_nullable: false,
                        back_populates: child_rel_name,
                        remote_side: None,
                        foreign_keys: if multi_ref {
                            Some(format!("'[{}.{}]'", child_class, col_name))
                        } else {
                            None
                        },
                        uselist_false: false,
                        secondary: None,
                    });
                }
            } else {
                // Composite FK reverse
                let attr_name = child_table.name.clone();
                let back_pop = parent_table.name.clone();

                rels.push(RelationshipInfo {
                    attr_name,
                    target_class: child_class.clone(),
                    is_collection: true,
                    is_nullable: false,
                    back_populates: back_pop,
                    remote_side: None,
                    foreign_keys: None,
                    uselist_false: false,
                    secondary: None,
                });
            }
        }
    }

    rels
}

/// Check if a table is a many-to-many association table.
/// An association table has exactly 2 FKs and no columns that aren't part of those FKs.
pub fn is_association_table(table: &TableInfo) -> bool {
    let fk_constraints: Vec<&ConstraintInfo> = table
        .constraints
        .iter()
        .filter(|c| c.constraint_type == ConstraintType::ForeignKey)
        .collect();

    if fk_constraints.len() != 2 {
        return false;
    }

    // All columns must be FK columns
    let fk_cols: std::collections::HashSet<&str> = fk_constraints
        .iter()
        .flat_map(|c| c.columns.iter().map(|s| s.as_str()))
        .collect();

    table.columns.iter().all(|c| fk_cols.contains(c.name.as_str()))
}

/// For a many-to-many association table, get the two target table names and
/// the relationship info for each side.
pub fn get_m2m_targets(assoc_table: &TableInfo) -> Option<(String, String)> {
    let fk_constraints: Vec<&ConstraintInfo> = assoc_table
        .constraints
        .iter()
        .filter(|c| c.constraint_type == ConstraintType::ForeignKey)
        .collect();

    if fk_constraints.len() != 2 {
        return None;
    }

    let t1 = fk_constraints[0]
        .foreign_key
        .as_ref()
        .map(|fk| fk.ref_table.clone())?;
    let t2 = fk_constraints[1]
        .foreign_key
        .as_ref()
        .map(|fk| fk.ref_table.clone())?;

    Some((t1, t2))
}

/// Generate M2M relationships for a table based on association tables pointing to it.
pub fn generate_m2m_relationships(
    table: &TableInfo,
    schema: &IntrospectedSchema,
) -> Vec<RelationshipInfo> {
    let mut rels = Vec::new();

    for assoc_table in &schema.tables {
        if !is_association_table(assoc_table) {
            continue;
        }

        let (t1, t2) = match get_m2m_targets(assoc_table) {
            Some(targets) => targets,
            None => continue,
        };

        // Check if this table is one of the M2M targets
        if table.name != t1 && table.name != t2 {
            continue;
        }

        let other_table = if table.name == t1 { &t2 } else { &t1 };
        let other_class = table_to_class_name(other_table);

        // Determine the secondary table reference
        let secondary = if assoc_table.schema != "public" && !assoc_table.schema.is_empty() {
            format!("{}.{}", assoc_table.schema, assoc_table.name)
        } else {
            assoc_table.name.clone()
        };

        // Derive relationship name from the FK column targeting the other table
        let rel_name = derive_m2m_rel_name(assoc_table, other_table);

        // back_populates: the other table's relationship name for this table
        let back_pop = derive_m2m_rel_name(assoc_table, &table.name);

        rels.push(RelationshipInfo {
            attr_name: rel_name,
            target_class: other_class,
            is_collection: true,
            is_nullable: false,
            back_populates: back_pop,
            remote_side: None,
            foreign_keys: None,
            uselist_false: false,
            secondary: Some(secondary),
        });
    }

    rels
}

/// Derive the M2M relationship name from the FK column targeting the OTHER table.
/// E.g., for LeftTable looking through assoc with left_id/right_id FK columns,
/// the relationship name is "right" (from right_id pointing to RightTable).
fn derive_m2m_rel_name(assoc_table: &TableInfo, other_table: &str) -> String {
    // Find the FK column that points TO other_table
    for constraint in &assoc_table.constraints {
        if constraint.constraint_type == ConstraintType::ForeignKey {
            if let Some(ref fk) = constraint.foreign_key {
                if fk.ref_table == other_table && constraint.columns.len() == 1 {
                    return fk_col_to_relationship_name(&constraint.columns[0]);
                }
            }
        }
    }
    // Fallback: use the other table name
    other_table.to_string()
}

/// Detect joined table inheritance: returns the parent table name if this table's
/// PK column is also a single-column FK to another table's PK.
pub fn find_inheritance_parent<'a>(
    table: &TableInfo,
    schema: &'a IntrospectedSchema,
) -> Option<&'a str> {
    // Get PK columns
    let pk_constraint = table
        .constraints
        .iter()
        .find(|c| c.constraint_type == ConstraintType::PrimaryKey)?;

    if pk_constraint.columns.len() != 1 {
        return None;
    }

    let pk_col = &pk_constraint.columns[0];

    // Check if PK column is also a single-column FK
    let fk = table.constraints.iter().find(|c| {
        c.constraint_type == ConstraintType::ForeignKey
            && c.columns.len() == 1
            && c.columns[0] == *pk_col
    })?;

    let fk_info = fk.foreign_key.as_ref()?;

    // Verify the target is a PK in the parent table
    let parent = schema
        .tables
        .iter()
        .find(|t| t.name == fk_info.ref_table)?;

    let parent_pk = parent
        .constraints
        .iter()
        .find(|c| c.constraint_type == ConstraintType::PrimaryKey)?;

    if parent_pk.columns.contains(&fk_info.ref_columns[0]) {
        Some(&parent.name)
    } else {
        None
    }
}

/// Render a relationship line.
pub fn render_relationship(rel: &RelationshipInfo) -> String {
    let type_annotation = if rel.is_collection {
        format!("list['{}']", rel.target_class)
    } else if rel.is_nullable {
        format!("Optional['{}']", rel.target_class)
    } else {
        format!("'{}'", rel.target_class)
    };

    let mut args = Vec::new();
    args.push(format!("'{}'", rel.target_class));

    if rel.uselist_false {
        args.push("uselist=False".to_string());
    }

    if let Some(ref rs) = rel.remote_side {
        args.push(format!("remote_side=[{}]", rs));
    }

    if let Some(ref sec) = rel.secondary {
        args.push(format!("secondary='{sec}'"));
    }

    if let Some(ref fk) = rel.foreign_keys {
        args.push(format!("foreign_keys={fk}"));
    }

    if !rel.back_populates.is_empty() {
        args.push(format!("back_populates='{}'", rel.back_populates));
    }

    let args_str = args.join(", ");
    format!(
        "    {}: Mapped[{type_annotation}] = relationship({args_str})",
        rel.attr_name
    )
}
