use std::collections::{HashMap, HashSet};

use crate::schema::{ConstraintType, TableInfo};

/// Detect circular FK dependencies among tables.
pub(super) fn detect_fk_cycles(tables: &[TableInfo]) -> bool {
    type Key<'a> = (&'a str, &'a str);

    let table_keys: HashSet<Key> = tables
        .iter()
        .map(|t| (t.schema.as_str(), t.name.as_str()))
        .collect();
    let mut visited: HashSet<Key> = HashSet::new();
    let mut in_stack: HashSet<Key> = HashSet::new();

    fn dfs<'a>(
        node: Key<'a>,
        adj: &HashMap<Key<'a>, Vec<Key<'a>>>,
        visited: &mut HashSet<Key<'a>>,
        in_stack: &mut HashSet<Key<'a>>,
    ) -> bool {
        visited.insert(node);
        in_stack.insert(node);
        if let Some(neighbors) = adj.get(&node) {
            for &neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    if dfs(neighbor, adj, visited, in_stack) {
                        return true;
                    }
                } else if in_stack.contains(&neighbor) {
                    return true;
                }
            }
        }
        in_stack.remove(&node);
        false
    }

    let mut adj: HashMap<Key, Vec<Key>> = HashMap::new();
    for table in tables {
        let src = (table.schema.as_str(), table.name.as_str());
        for c in &table.constraints {
            if c.constraint_type == ConstraintType::ForeignKey {
                if let Some(ref fk) = c.foreign_key {
                    let dst = (fk.ref_schema.as_str(), fk.ref_table.as_str());
                    if table_keys.contains(&dst) && dst != src {
                        adj.entry(src).or_default().push(dst);
                    }
                }
            }
        }
    }

    for table in tables {
        let key = (table.schema.as_str(), table.name.as_str());
        if !visited.contains(&key) && dfs(key, &adj, &mut visited, &mut in_stack) {
            return true;
        }
    }
    false
}
