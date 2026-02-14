use crate::schema::ColumnInfo;

/// Create a ColumnInfo with sensible defaults for testing.
/// Returns a non-nullable int4 column with no defaults, no identity, and no comment.
pub fn test_column(name: &str) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        ordinal_position: 1,
        is_nullable: false,
        data_type: String::new(),
        udt_name: "int4".to_string(),
        character_maximum_length: None,
        numeric_precision: None,
        numeric_scale: None,
        column_default: None,
        is_identity: false,
        identity_generation: None,
        identity: None,
        comment: None,
    }
}
