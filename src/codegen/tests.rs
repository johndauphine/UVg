use super::*;

#[test]
fn test_format_server_default_pg() {
    assert_eq!(
        format_server_default("now()", Dialect::Postgres),
        "text('now()')"
    );
    assert_eq!(format_server_default("0", Dialect::Postgres), "text('0')");
}

#[test]
fn test_strip_pg_typecast() {
    assert_eq!(strip_pg_typecast("0::integer"), "0");
    assert_eq!(strip_pg_typecast("'hello'::character varying"), "'hello'");
    assert_eq!(strip_pg_typecast("now()"), "now()");
    assert_eq!(
        strip_pg_typecast("nextval('seq'::regclass)"),
        "nextval('seq'::regclass)"
    );
}

#[test]
fn test_format_server_default_mssql() {
    assert_eq!(format_server_default("((0))", Dialect::Mssql), "text('0')");
    assert_eq!(
        format_server_default("(N'hello')", Dialect::Mssql),
        "text(\"'hello'\")"
    );
    assert_eq!(
        format_server_default("(getdate())", Dialect::Mssql),
        "text('getdate()')"
    );
}

#[test]
fn test_strip_mssql_parens() {
    assert_eq!(strip_mssql_parens("((0))"), "0");
    assert_eq!(strip_mssql_parens("(N'hello')"), "'hello'");
    assert_eq!(strip_mssql_parens("(getdate())"), "getdate()");
    assert_eq!(strip_mssql_parens("((1))"), "1");
}

#[test]
fn test_is_serial_default() {
    assert!(is_serial_default(
        "nextval('seq'::regclass)",
        Dialect::Postgres
    ));
    assert!(!is_serial_default("nextval('seq')", Dialect::Mssql));
    assert!(!is_serial_default("((1))", Dialect::Mssql));
}

#[test]
fn test_split_python_declarative() {
    let full = "\
from typing import Optional

from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Users(Base):
    __tablename__ = 'users'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)


class Posts(Base):
    __tablename__ = 'posts'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
";
    let files = split_python_output(full);
    let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();

    assert!(names.contains(&"base.py"), "missing base.py: {names:?}");
    assert!(names.contains(&"users.py"), "missing users.py: {names:?}");
    assert!(names.contains(&"posts.py"), "missing posts.py: {names:?}");
    assert!(
        names.contains(&"__init__.py"),
        "missing __init__.py: {names:?}"
    );

    // base.py should have imports and Base class
    let base = &files.iter().find(|(n, _)| n == "base.py").unwrap().1;
    assert!(base.contains("from sqlalchemy"), "base.py missing imports");
    assert!(base.contains("class Base"), "base.py missing Base class");

    // model files should have from .base import
    let users = &files.iter().find(|(n, _)| n == "users.py").unwrap().1;
    assert!(
        users.contains("from .base import"),
        "users.py missing base import"
    );
    assert!(
        users.contains("__tablename__"),
        "users.py missing tablename"
    );
}

#[test]
fn test_split_python_enum_stays_in_base() {
    let full = "\
import enum

from sqlalchemy import Enum, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class StatusEnum(str, enum.Enum):
    ACTIVE = 'active'
    INACTIVE = 'inactive'


class Base(DeclarativeBase):
    pass


class Users(Base):
    __tablename__ = 'users'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
";
    let files = split_python_output(full);
    let base = &files.iter().find(|(n, _)| n == "base.py").unwrap().1;
    assert!(base.contains("StatusEnum"), "enum should be in base.py");

    // Enum should NOT be split into its own file
    let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        !names.contains(&"status_enum.py"),
        "enum should not be a separate file"
    );
}

#[test]
fn test_split_python_tables_generator() {
    // Tables generator uses double-newline separators
    let full = "\
from sqlalchemy import Column, Integer, MetaData, String, Table

metadata = MetaData()

t_users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True)
)

t_posts = Table(
    'posts', metadata,
    Column('id', Integer, primary_key=True)
)
";
    let files = split_python_output(full);
    let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names.contains(&"t_users.py"),
        "missing t_users.py: {names:?}"
    );
    assert!(
        names.contains(&"t_posts.py"),
        "missing t_posts.py: {names:?}"
    );
}
