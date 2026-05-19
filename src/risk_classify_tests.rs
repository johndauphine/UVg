use super::*;

fn change(sql: &str) -> Change {
    Change {
        table_schema: "main".to_string(),
        table_name: Some("users".to_string()),
        sql: sql.to_string(),
    }
}

#[test]
fn risk_classes_parse_and_render() {
    let raw = r#"{"risks":[
        {"index":0,"risk":"safe"},
        {"index":1,"risk":"blocking"},
        {"index":2,"risk":"rebuild"},
        {"index":3,"risk":"data-loss-risk"}
    ]}"#;

    let risks = parse_classification_text(raw, 4).unwrap();

    assert_eq!(
        risks.iter().map(|r| r.as_str()).collect::<Vec<_>>(),
        vec!["safe", "blocking", "rebuild", "data-loss-risk"]
    );
}

#[test]
fn annotate_changes_adds_one_risk_comment_per_change() {
    let changes = vec![
        change("ALTER TABLE users ADD COLUMN email TEXT;"),
        change("DROP TABLE old_users;"),
    ];

    let annotated =
        annotate_changes(&changes, &[RiskClass::Safe, RiskClass::DataLossRisk]).unwrap();

    assert_eq!(annotated[0].sql.matches("-- RISK:").count(), 1);
    assert!(annotated[0].sql.starts_with("-- RISK: safe\nALTER TABLE"));
    assert_eq!(annotated[1].sql.matches("-- RISK:").count(), 1);
    assert!(annotated[1]
        .sql
        .starts_with("-- RISK: data-loss-risk\nDROP TABLE"));
    assert!(!changes[0].sql.contains("-- RISK:"));
}

#[test]
fn invalid_classification_count_errors_without_mutating_changes() {
    let changes = vec![change("ALTER TABLE users ADD COLUMN email TEXT;")];

    let err = annotate_changes(&changes, &[]).unwrap_err().to_string();

    assert!(err.contains("0 result(s) for 1 change(s)"));
    assert_eq!(changes[0].sql, "ALTER TABLE users ADD COLUMN email TEXT;");
}

#[test]
fn missing_api_key_is_clear() {
    let err = AnthropicConfig::from_api_key(None).unwrap_err().to_string();

    assert!(err.contains("ANTHROPIC_API_KEY is required"));
}
