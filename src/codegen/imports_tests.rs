use super::*;

#[test]
fn test_basic_imports() {
    let mut ic = ImportCollector::new();
    ic.add("sqlalchemy", "Integer");
    ic.add("sqlalchemy", "String");
    ic.add("sqlalchemy", "Column");
    let result = ic.render();
    assert_eq!(result, "from sqlalchemy import Column, Integer, String");
}

#[test]
fn test_mixed_imports() {
    let mut ic = ImportCollector::new();
    ic.add("typing", "Optional");
    ic.add_bare("datetime");
    ic.add("sqlalchemy", "Integer");
    ic.add("sqlalchemy.orm", "DeclarativeBase");
    let result = ic.render();
    let expected = "from typing import Optional\nimport datetime\n\nfrom sqlalchemy import Integer\nfrom sqlalchemy.orm import DeclarativeBase";
    assert_eq!(result, expected);
}

#[test]
fn test_dialect_imports() {
    let mut ic = ImportCollector::new();
    ic.add("sqlalchemy", "Integer");
    ic.add("sqlalchemy.dialects.postgresql", "JSONB");
    let result = ic.render();
    assert_eq!(
        result,
        "from sqlalchemy import Integer\nfrom sqlalchemy.dialects.postgresql import JSONB"
    );
}
