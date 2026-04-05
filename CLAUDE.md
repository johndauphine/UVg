# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

UVg is a Rust reimplementation of Python's [sqlacodegen](https://github.com/agronholm/sqlacodegen). It connects to PostgreSQL, MySQL, SQLite, or Microsoft SQL Server databases, introspects their schema, and generates SQLAlchemy Python model code. The goal is drop-in CLI compatibility with sqlacodegen -- same flags, same output format.

## Build & Test Commands

```bash
cargo build                    # Debug build
cargo build --release          # Release build
cargo install --path .         # Install binary locally
cargo test                     # Run all unit tests
cargo test test_name           # Run a single test by name/pattern
cargo test -- --nocapture      # Show println output during tests
```

Integration tests require a live database via `DATABASE_URL` (except SQLite which runs in-memory):
```bash
DATABASE_URL=postgresql://user:pass@localhost/testdb cargo test --test integration -- --ignored
MYSQL_URL=mysql://user:pass@localhost/testdb cargo test --test integration -- --ignored
DATABASE_URL=mssql://user:pass@localhost/testdb cargo test --test integration -- --ignored
cargo test --test integration test_introspect_sqlite_in_memory  # No server needed
```

Enable debug logging with `RUST_LOG=debug`.

### Snapshot tests

Snapshot tests use the `insta` crate with YAML format. Snapshots live in `src/codegen/snapshots/`. When updating expected output, either use `cargo insta review` (if cargo-insta is installed) or manually move `.snap.new` files to `.snap`.

## Architecture

The application follows a pipeline: **CLI parsing -> Connection -> Introspection -> Code Generation -> Output**.

### Key modules (`src/`)

- **`cli.rs`** -- Clap-based CLI parsing. Produces `ConnectionConfig` (Postgres/MySQL URL string, SQLite path, or MSSQL connection fields) and `GeneratorOptions` (`noindexes`, `noconstraints`, `nocomments`). The `--interactive` (`-i`) flag triggers the TUI mode.
- **`db.rs`** -- Shared database operations: `introspect_with_config()` connects to any dialect and returns `IntrospectedSchema`; `execute_ddl()` splits DDL into statements and executes them one-by-one against a target database.
- **`dialect.rs`** -- `Dialect` enum (Postgres, Mssql, Mysql, Sqlite) with `default_schema()` returning `"public"`, `"dbo"`, `""` (MySQL uses database name), or `"main"`.
- **`schema.rs`** -- Data structures representing introspected schema: `IntrospectedSchema`, `TableInfo`, `ColumnInfo`, `ConstraintInfo` (PrimaryKey/ForeignKey/Unique/Check), `IndexInfo`.
- **`introspect/pg/`** -- PostgreSQL introspection via sqlx. Queries `information_schema` and `pg_catalog` across submodules: `tables.rs`, `columns.rs`, `constraints.rs`, `indexes.rs`.
- **`introspect/mssql/`** -- MSSQL introspection via Tiberius. Same submodule structure as PG.
- **`introspect/mysql/`** -- MySQL introspection via sqlx. Queries `information_schema` tables. Same submodule structure as PG. All string columns use `CAST(... AS CHAR)` to avoid MySQL 8+ VARBINARY decoding issues. Connection URLs are automatically appended with `charset=utf8mb4` (via `ensure_mysql_charset()` in `cli.rs`) unless the user specifies a charset.
- **`introspect/sqlite/`** -- SQLite introspection via sqlx. Uses PRAGMA commands (`pragma_table_info`, `pragma_foreign_key_list`, `pragma_index_list`) and parses CREATE TABLE SQL for AUTOINCREMENT and CHECK constraints.
- **`typemap/pg.rs`**, **`typemap/mssql.rs`**, **`typemap/mysql.rs`**, **`typemap/sqlite.rs`** -- Map database column types (via `udt_name`) to SQLAlchemy type expressions, Python type annotations, and import requirements. Returns `MappedType`.
- **`codegen/`** -- `Generator` trait with three implementations:
  - `declarative.rs` -- Generates modern SQLAlchemy ORM classes with `Mapped[]` type annotations. Tables without primary keys fall back to `Table()` syntax within the same output. Tests in `declarative_tests/` (basic, relationships, enums_and_types).
  - `tables.rs` -- Generates `Table()` metadata objects for all tables. Tests in `tables_tests.rs`.
  - `ddl.rs` -- Generates raw SQL DDL (CREATE TABLE, CREATE INDEX, COMMENT ON, enum types) for cross-dialect schema migration. Functions are `pub(super)` for use by `ddl_diff.rs`.
  - `ddl_diff.rs` -- Schema diff engine: compares source and target schemas, emits ALTER TABLE statements for new/dropped/modified tables and columns. Cross-dialect schema normalization for default schemas.
  - `imports.rs` -- `ImportCollector` that accumulates, deduplicates, and renders import statements in a specific group order.
  - `relationships.rs` -- FK-based relationship inference for declarative mode.
  - `mod.rs` -- Shared helpers: `has_primary_key()`, `is_primary_key_column()`, `topo_sort_tables()`, `format_server_default()`, `split_python_output()` (for `--split-tables`).
- **`ddl_typemap/`** -- Cross-dialect type translation via `CanonicalType` intermediate representation. Maps source DB types to target DDL types (e.g., PG `jsonb` → MySQL `JSON`, PG `uuid` → MSSQL `UNIQUEIDENTIFIER`).
- **`naming.rs`** -- Table/column name transformations: `table_to_class_name()` (UpperCamelCase via `heck`), `table_to_variable_name()` (sanitized `t_` prefix).
- **`error.rs`** -- `UvgError` enum using `thiserror`.
- **`tui/mod.rs`** -- Interactive terminal UI (ratatui + crossterm). State machine with screens: URL input, DDL diff view (scrollable), apply confirmation, and result display. Reuses `db.rs` for introspection and DDL execution.

### Output fidelity

The generated Python code must match sqlacodegen's output exactly. Key ordering rules to preserve:

- **Constraints in `Table()`**: ForeignKeyConstraint -> PrimaryKeyConstraint -> UniqueConstraint -> Index
- **Declarative `__table_args__`**: ForeignKeyConstraint -> UniqueConstraint -> Index, then kwargs dict (comment, schema). PrimaryKeyConstraint is NOT emitted here (expressed via `primary_key=True` on `mapped_column` instead).
- **`__table_args__` format**: Dict-only `{'schema': '...'}` when only kwargs exist; tuple `(Index(...), {'schema': '...'})` when both positional args and kwargs exist.
- **Import groups** separated by blank lines: `typing` -> stdlib bare imports (datetime, decimal, uuid) -> `sqlalchemy` core -> `sqlalchemy.dialects.*` -> `sqlalchemy.orm`
- **Comment quoting**: Use double quotes when the comment string contains single quotes.

### No-PK fallback in declarative mode

When the declarative generator encounters tables without primary keys, it falls back to `Table()` syntax for those tables while keeping ORM classes for tables with PKs. The `metadata_ref` parameter switches between `Base.metadata` (when any class exists) and standalone `metadata = MetaData()` (when all tables lack PKs).

### Testing patterns

- Tests are separated from code for large files. `declarative.rs` tests live in `declarative_tests/` (basic.rs, relationships.rs, enums_and_types.rs). `tables.rs` tests live in `tables_tests.rs`. Smaller files keep tests inline.
- `src/testutil.rs` provides builder helpers for constructing test data: `col(name)` -> `ColumnInfoBuilder`, `table(name)` -> `TableInfoBuilder`, `schema_pg(tables)` / `schema_mssql(tables)` / `schema_mysql(tables)` / `schema_sqlite(tables)` -> `IntrospectedSchema`. Builders auto-increment `ordinal_position` and default to non-nullable int4 columns.
- Many codegen tests use `assert_eq!(output, expected)` with `indoc!` for exact string matching against sqlacodegen's expected output. Some older tests use `insta::assert_yaml_snapshot!()`.
- Integration tests in `tests/integration.rs` are marked `#[ignore]` and require a live database (except SQLite which runs in-memory).
- DDL cross-dialect tests verified via Docker E2E: all 16 source×target permutations pass DDL generation; same-dialect migrations achieve perfect convergence (diff after apply shows zero changes).

### Adding a new database dialect

1. Add an introspection module under `src/introspect/<dialect>/` with `tables.rs`, `columns.rs`, `constraints.rs`, `indexes.rs`.
2. Add a type mapper in `src/typemap/<dialect>.rs`.
3. Extend `Dialect` enum in `dialect.rs` and `ConnectionConfig` in `cli.rs`.
4. Add the connection/introspection branch in `main.rs`.
