use crate::naming::column_to_attr_name;
use crate::schema::ColumnInfo;

/// Pre-compute sanitized attribute names for all columns, resolving collisions.
/// When two columns sanitize to the same name, the later one gets a trailing `_`.
pub(super) fn resolve_attr_names(columns: &[ColumnInfo]) -> Vec<String> {
    let mut names: Vec<String> = columns
        .iter()
        .map(|c| column_to_attr_name(&c.name))
        .collect();

    // Resolve collisions: if name[i] == name[j] where j > i, append _ to name[j].
    for i in 0..names.len() {
        for j in (i + 1)..names.len() {
            if names[j] == names[i] {
                names[j].push('_');
            }
        }
    }

    names
}
