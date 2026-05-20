use super::*;
use crate::schema::{TableInfo, TableType};

#[test]
fn restore_original_order_sorts_by_ordinal() {
    let restored = restore_original_order(vec![(2, "c"), (0, "a"), (1, "b")]);

    assert_eq!(restored, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn populate_tables_concurrently_restores_table_order() {
    let tables = vec![
        TableInfo::new("public", "a", TableType::Table),
        TableInfo::new("public", "b", TableType::Table),
        TableInfo::new("public", "c", TableType::Table),
    ];

    let populated = populate_tables_concurrently(tables, 0, |mut table| async move {
        table.comment = Some(format!("table {}", table.name));
        Ok(table)
    })
    .await
    .unwrap();

    let names: Vec<_> = populated.iter().map(|table| table.name.as_str()).collect();
    let comments: Vec<_> = populated
        .iter()
        .map(|table| table.comment.as_deref())
        .collect();

    assert_eq!(names, ["a", "b", "c"]);
    assert_eq!(
        comments,
        [Some("table a"), Some("table b"), Some("table c")]
    );
}
