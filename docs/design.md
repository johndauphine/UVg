# Design

## Architecture Overview

```
src/
  main.rs              Entry point: wires CLI -> connection -> introspection -> codegen
  cli.rs               Clap argument parsing, URL normalization, option extraction
  db.rs                Shared DB operations: introspect_with_config(), execute_ddl()
  dialect.rs           Dialect enum (Postgres, Mssql, Mysql, Sqlite) with default_schema()
  error.rs             UvgError enum (thiserror)
  schema.rs            Dialect-neutral schema representation structs
  naming.rs            Table name -> class name / variable name transforms

  introspect/
    pg/                PostgreSQL introspection via sqlx
      mod.rs           Orchestrates table/column/constraint/index queries
      tables.rs        information_schema.tables query
      columns.rs       information_schema.columns + pg_catalog enrichment
      constraints.rs   pg_constraint + information_schema joins
      indexes.rs       pg_indexes query
    mssql/             MSSQL introspection via tiberius (same submodule structure)
    mysql/             MySQL introspection via sqlx (information_schema queries)
      mod.rs           Orchestrates queries; schemas = database names
      tables.rs        information_schema.TABLES with TABLE_COMMENT
      columns.rs       information_schema.COLUMNS with COLUMN_TYPE, EXTRA, COLUMN_COMMENT
      constraints.rs   PK/Unique/FK via information_schema + CHECK (MySQL 8.0+)
      indexes.rs       information_schema.STATISTICS
    sqlite/            SQLite introspection via sqlx (PRAGMA commands)
      mod.rs           Orchestrates queries; no schema concept
      tables.rs        sqlite_master + query_create_sql() helper
      columns.rs       pragma_table_info + AUTOINCREMENT detection from CREATE TABLE SQL
      constraints.rs   PK/FK/Unique via PRAGMA + CHECK parsed from CREATE TABLE SQL
      indexes.rs       pragma_index_list + pragma_index_info

  typemap/
    mod.rs             Dispatch: calls dialect-specific mapper based on Dialect
    pg.rs              PostgreSQL udt_name -> MappedType
    mssql.rs           MSSQL udt_name -> MappedType
    mysql.rs           MySQL DATA_TYPE/COLUMN_TYPE -> MappedType (with ENUM/SET parsing)
    sqlite.rs          SQLite declared type -> MappedType (with affinity fallback)

  ddl_typemap/
    mod.rs             CanonicalType enum, cross-dialect type translation dispatch
    pg.rs              PostgreSQL ↔ canonical type mapping
    mysql.rs           MySQL ↔ canonical type mapping
    mssql.rs           MSSQL ↔ canonical type mapping
    sqlite.rs          SQLite ↔ canonical type mapping (affinity rules)

  codegen/
    mod.rs             Generator trait, topo_sort, split_python_output, shared helpers
    imports.rs         ImportCollector: accumulates and renders Python imports
    declarative.rs     DeclarativeGenerator: Mapped[] ORM classes
    tables.rs          TablesGenerator: Table() metadata objects
    ddl.rs             DdlGenerator: raw SQL DDL (CREATE TABLE, indexes, comments, enums)
    ddl_diff.rs        Schema diff engine: ALTER TABLE generation (Alembic-inspired)
    relationships.rs   FK-based relationship inference for declarative mode
    declarative_tests/ Tests for declarative generator (basic, relationships, enums_and_types)
    tables_tests.rs    Tests for tables generator
    snapshots/         insta snapshot files for codegen tests

  tui/
    mod.rs             Interactive TUI (ratatui + crossterm): URL input, DDL diff view, apply

  testutil.rs          Test builders: col(), table(), schema_pg/mssql/mysql/sqlite()

tests/
  integration.rs       Live database integration tests (#[ignore] for PG/MySQL/MSSQL; SQLite runs in-memory)
```

## Key Design Decisions

### Schema as the universal intermediate representation

The `IntrospectedSchema` struct is the only interface between introspection and code generation. The introspection modules know nothing about Python or SQLAlchemy. The code generators know nothing about SQL queries or database catalogs. This makes it possible to add a new database dialect by implementing only the introspection side, and to add a new output format by implementing only the generator side.

### Generator trait with three implementations

```rust
pub trait Generator {
    fn generate(&self, schema: &IntrospectedSchema, options: &GeneratorOptions) -> String;
}
```

The trait takes the entire schema and returns a complete Python source string. This makes generators stateless and testable -- construct a schema, call generate, check the string. The two implementations (DeclarativeGenerator, TablesGenerator) share helpers from `codegen/mod.rs` but are otherwise independent.

### ImportCollector: demand-driven imports

Generators don't declare imports upfront. Instead, as they emit each column type, constraint, or index, they call `imports.add("sqlalchemy", "Integer")` etc. The collector deduplicates and sorts everything, then `imports.render()` is called once at the end to produce the import block.

This avoids the fragile pattern of maintaining a separate import list that must stay in sync with the generated code. It also means adding a new type mapping automatically adds the right import.

### Topological sort with alphabetical tiebreak

Tables must be emitted in dependency order (a table referenced by a FK must appear before the table that references it). But multiple valid orderings often exist. The alphabetical tiebreak (via `BTreeSet`) ensures deterministic output that matches sqlacodegen.

Cycles (mutual FK references) are handled by exhausting the queue and appending remaining tables alphabetically. This matches sqlacodegen's behavior rather than failing.

### Dialect-specific Identity formatting

Each dialect exposes different identity column metadata. Rather than normalizing to a lowest common denominator, UVg preserves the full dialect-specific parameters:

- PostgreSQL includes `minvalue`, `maxvalue`, `cycle`, `cache` because sqlacodegen emits them.
- MSSQL, MySQL, and SQLite include only `start` and `increment`.
- MySQL uses AUTO_INCREMENT (detected via `EXTRA` column in `information_schema.COLUMNS`).
- SQLite uses AUTOINCREMENT (detected by parsing the CREATE TABLE SQL from `sqlite_master`).

The `IdentityInfo` struct stores the superset. The codegen layer dispatches on `Dialect` to decide which fields to emit.

### `__table_args__` format selection

The declarative generator must choose between three `__table_args__` formats:

1. **Omitted**: No constraints, indexes, comments, or non-default schema.
2. **Dict only**: `__table_args__ = {'schema': 'testschema'}` -- only kwargs, no positional args.
3. **Tuple with dict**: `__table_args__ = (Index(...), {'schema': '...'})` -- positional args with kwargs dict appended.

The `build_table_args` function separates positional args (constraints, indexes) from kwargs (comment, schema) and selects the format accordingly.

### PrimaryKeyConstraint omitted in declarative mode

The tables generator emits `PrimaryKeyConstraint('id', name='pk_name')` as an explicit constraint. The declarative generator does not -- instead, PK membership is expressed via `primary_key=True` on `mapped_column()`. This matches sqlacodegen's behavior where the declarative mode relies on SQLAlchemy's ORM layer to infer the PK constraint from column attributes.

### Comment quoting strategy

Python string quoting follows a simple rule: if the string contains single quotes, wrap in double quotes (`comment="this is a 'comment'"`). Otherwise, use single quotes (`comment='simple comment'`). This matches sqlacodegen's quoting behavior and avoids escape sequences.

### Default schema suppression

Each dialect has a default schema (`public` for PostgreSQL, `dbo` for MSSQL, the database name for MySQL, `main` for SQLite). When a table's schema matches the default, the `schema=` parameter is omitted from the output. This keeps generated code clean for the common case while correctly qualifying tables in non-default schemas.

### DDL generator: canonical type translation

The DDL generator uses a two-phase type translation to handle cross-dialect migration. Source column types are first normalized to a `CanonicalType` enum (Integer, Boolean, Varchar, Timestamp, Uuid, Json, etc.), then emitted as DDL for the target dialect. This avoids N×N mapping functions — each dialect only implements `to_canonical()` and `from_canonical()`.

Lossy translations (e.g. PG `JSONB` → MySQL `JSON`, PG `UUID` → MySQL `CHAR(36)`) are flagged with `is_approximate` and an optional warning string.

When source and target are the same dialect, the canonical translation is still used but produces identical output (identity mapping).

### DDL generator: schema diff

The diff engine compares two `IntrospectedSchema` instances and emits ALTER statements. Tables are matched by name after normalizing default schemas (`public`, `dbo`, `main`, MySQL database names) to a common key. This allows cross-dialect diffs (PG `public.users` matches MSSQL `dbo.users`).

Column comparison detects type changes, nullability changes, and default changes. Boolean defaults are normalized (0/1 ↔ true/false) before comparison to avoid false positives across dialects.

### DDL generator: default schema mapping

Cross-dialect DDL suppresses schema qualification when the source schema is a default for any dialect. MSSQL cannot create a `[public]` schema (reserved role name), so PG's `public` tables emit as unqualified names targeting MSSQL's `dbo`. MySQL's database-as-schema names are always suppressed since DDL targets the connected database.

### Interactive TUI

The `--interactive` (`-i`) flag launches a terminal UI built with ratatui and crossterm. It follows a linear state machine: `InputUrls → Generating → ViewDdl → Confirming → Applying → Done`. The TUI reuses the existing pipeline — `db::introspect_with_config()` for schema introspection and `DdlGenerator.generate()` for DDL diff — rather than reimplementing any logic.

DDL apply splits the generated output on `;` delimiters, strips leading comment lines from each chunk (so header comments like `-- Generated by uvg` don't prevent execution), and executes statements one-by-one via sqlx (or tiberius for MSSQL). Execution stops on first error. No transaction wrapper is used because DDL causes implicit commits on most engines.

The `db.rs` module was extracted from `main.rs` to share `introspect_with_config()` between the CLI and TUI code paths. It also houses `execute_ddl()` which is only used by the TUI.

## What's Not Implemented Yet

Features present in sqlacodegen but not yet in UVg (tracked for future work):

- **Computed columns**: `Computed()` column support.
- **Index promotion**: Promoting single-column indexes to `index=True` on the Column instead of a separate `Index()` object.
