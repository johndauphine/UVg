# Technical Specifications

## Pipeline

```
CLI parsing -> Connection -> Introspection -> Code Generation -> Output
```

Each stage is a pure transformation. The introspection stage queries the database and produces an `IntrospectedSchema` struct. The code generation stage consumes that struct and produces a Python source string. There are no callbacks, no shared mutable state between stages, and no intermediate files.

The interactive TUI (`--interactive` / `-i`) adds an optional apply stage to the DDL pipeline:

```
URL Input -> Introspection (source + target) -> DDL Diff -> View -> Apply (optional)
```

The apply stage splits DDL output on `;`, strips leading comments from each statement, and executes them sequentially against the target database via `db::execute_ddl()`.

## Data Model

All introspected metadata flows through these structs defined in `src/schema.rs`:

```
IntrospectedSchema
  dialect: Dialect (Postgres | Mssql | Mysql | Sqlite)
  tables: Vec<TableInfo>

TableInfo
  schema: String              "public", "dbo", etc.
  name: String                Table name as it appears in the database
  table_type: TableType       Table | View
  comment: Option<String>     Table-level comment
  columns: Vec<ColumnInfo>    Ordered by ordinal_position
  constraints: Vec<ConstraintInfo>
  indexes: Vec<IndexInfo>

ColumnInfo
  name, ordinal_position, is_nullable
  udt_name: String            Primary key for type mapping (e.g. "int4", "varchar")
  character_maximum_length    For String(N), Char(N)
  numeric_precision, numeric_scale   For Numeric(P, S)
  column_default              Raw SQL expression (e.g. "nextval('seq'::regclass)")
  is_identity, identity       Identity column metadata
  comment, collation

ConstraintInfo
  name: String
  constraint_type: PrimaryKey | ForeignKey | Unique
  columns: Vec<String>        Column names involved
  foreign_key: Option<ForeignKeyInfo>   ref_schema, ref_table, ref_columns, update/delete rules

IndexInfo
  name, is_unique, columns
```

## Type Mapping

The `typemap` module maps database-specific column types to three things:

1. **SQLAlchemy type expression** (`sa_type`): e.g. `"Integer"`, `"String(100)"`, `"DateTime(True)"`
2. **Python type annotation** (`python_type`): e.g. `"int"`, `"str"`, `"datetime.datetime"`
3. **Import requirements** (`import_module`, `import_name`): e.g. `("sqlalchemy", "Integer")`

The mapping key is `ColumnInfo.udt_name` -- the PostgreSQL user-defined type name, MSSQL equivalent, MySQL `DATA_TYPE`, or SQLite declared type. For MySQL, the full `COLUMN_TYPE` (e.g. `"enum('a','b')"`, `"int unsigned"`) is stored in `data_type` for cases where `udt_name` alone is insufficient (ENUM/SET value extraction, unsigned detection).

### PostgreSQL type mapping

| udt_name | sa_type | python_type |
|----------|---------|-------------|
| bool | Boolean | bool |
| int2 | SmallInteger | int |
| int4 | Integer | int |
| int8 | BigInteger | int |
| float4 | Float | float |
| float8 | Double | float |
| numeric | Numeric / Numeric(P,S) | decimal.Decimal |
| text | Text | str |
| varchar | String / String(N) | str |
| char, bpchar | String(N) | str |
| bytea | LargeBinary | bytes |
| date | Date | datetime.date |
| timestamp | DateTime | datetime.datetime |
| timestamptz | DateTime(True) | datetime.datetime |
| time | Time | datetime.time |
| timetz | Time(True) | datetime.time |
| interval | Interval | datetime.timedelta |
| uuid | UUID | uuid.UUID |
| json | JSON | dict |
| jsonb | JSONB | dict |
| inet, cidr | INET / CIDR | str |

PostgreSQL arrays (udt_name starting with `_`) are mapped to `ARRAY(element_type)` with recursive element type resolution.

### MSSQL type mapping

| udt_name | sa_type | python_type |
|----------|---------|-------------|
| bit | Boolean | bool |
| tinyint | SmallInteger | int |
| smallint | SmallInteger | int |
| int | Integer | int |
| bigint | BigInteger | int |
| real | Float | float |
| float | Float | float |
| decimal, numeric | Numeric(P,S) | decimal.Decimal |
| money, smallmoney | Numeric | decimal.Decimal |
| char, nchar | String(N) | str |
| varchar, nvarchar | String(N) / String | str |
| text, ntext | Text | str |
| date | Date | datetime.date |
| time | Time | datetime.time |
| datetime, datetime2, smalldatetime | DateTime | datetime.datetime |
| datetimeoffset | DateTime(True) | datetime.datetime |
| binary, varbinary | LargeBinary / LargeBinary(N) | bytes |
| image | LargeBinary | bytes |
| uniqueidentifier | UNIQUEIDENTIFIER | uuid.UUID |

Unknown types fall back to the uppercase type name (e.g. `mytype` -> `MYTYPE`).

### MySQL type mapping

**Note on `information_schema` encoding**: MySQL 8+ returns `information_schema` string columns as `VARBINARY` rather than `VARCHAR`, causing sqlx decode failures. All MySQL introspection queries use `CAST(... AS CHAR)` to work around this. Additionally, connections default to `charset=utf8mb4` (appended to the URL by `ensure_mysql_charset()` in `cli.rs` unless the user specifies a charset).

| udt_name | sa_type | python_type |
|----------|---------|-------------|
| tinyint (display_width=1) | Boolean | bool |
| tinyint | TINYINT | int |
| smallint | SmallInteger | int |
| mediumint | MEDIUMINT | int |
| int | Integer | int |
| bigint | BigInteger | int |
| float | Float | float |
| double | Double | float |
| decimal, numeric | Numeric(P,S) | decimal.Decimal |
| varchar | String(N) | str |
| char | String(N) | str |
| text | Text | str |
| tinytext | TINYTEXT | str |
| mediumtext | MEDIUMTEXT | str |
| longtext | LONGTEXT | str |
| binary, varbinary | LargeBinary(N) | bytes |
| blob | LargeBinary | bytes |
| tinyblob | TINYBLOB | bytes |
| mediumblob | MEDIUMBLOB | bytes |
| longblob | LONGBLOB | bytes |
| date | Date | datetime.date |
| time | Time | datetime.time |
| datetime | DateTime | datetime.datetime |
| timestamp | TIMESTAMP | datetime.datetime |
| year | YEAR | int |
| json | JSON | dict |
| enum | Enum('val1', 'val2') | str |
| set | SET('val1', 'val2') | str |
| bit | BIT | int |
| boolean | Boolean | bool |

Unsigned integer variants (e.g. `int unsigned`) map to the same generic SA type. In dialect mode (`keep_dialect_types`), they render as `INTEGER(unsigned=True)` etc. from `sqlalchemy.dialects.mysql`.

MySQL ENUM and SET values are parsed from the `COLUMN_TYPE` string (e.g. `enum('active','inactive')`).

Dialect-specific types (`TINYINT`, `MEDIUMINT`, `TINYTEXT`, `MEDIUMTEXT`, `LONGTEXT`, `TINYBLOB`, `MEDIUMBLOB`, `LONGBLOB`, `YEAR`, `SET`, `BIT`, `TIMESTAMP`) are imported from `sqlalchemy.dialects.mysql`.

### SQLite type mapping

| udt_name | sa_type | python_type |
|----------|---------|-------------|
| integer, int | Integer | int |
| smallint | SmallInteger | int |
| bigint | BigInteger | int |
| real, float, double | Float | float |
| numeric, decimal | Numeric(P,S) | decimal.Decimal |
| text, clob | Text | str |
| varchar, char | String(N) | str |
| blob | LargeBinary | bytes |
| date | Date | datetime.date |
| datetime, timestamp | DateTime | datetime.datetime |
| time | Time | datetime.time |
| boolean, bool | Boolean | bool |
| json | JSON | dict |
| (empty) | NullType | str |

Unknown declared types are mapped using SQLite type affinity rules:
- Contains "INT" -> Integer
- Contains "CHAR", "CLOB", or "TEXT" -> Text
- Contains "BLOB" or empty -> LargeBinary
- Contains "REAL", "FLOA", or "DOUB" -> Float
- Otherwise -> Numeric

## Import Ordering

The `ImportCollector` emits imports in this fixed order, with a blank line between groups 2 and 3:

1. `from typing import Optional`
2. `import datetime` / `import decimal` / `import uuid` (bare stdlib imports)
3. `from sqlalchemy import Column, Integer, MetaData, ...`
4. `from sqlalchemy.dialects.<dialect> import ...` (postgresql, mysql, mssql, etc.)
5. `from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column`

Within each `from X import` line, names are sorted alphabetically. Modules are sorted alphabetically within each group.

## Constraint Emission Order

### Tables generator (`Table()` body)

1. `Column(...)` for each column
2. `ForeignKeyConstraint(...)` for each FK
3. `PrimaryKeyConstraint(...)` for the PK
4. `UniqueConstraint(...)` for each unique constraint
5. `Index(...)` for each index (excluding indexes that back unique constraints)
6. `comment='...'` table comment
7. `schema='...'` if non-default

### Declarative generator (`__table_args__`)

1. `ForeignKeyConstraint(...)` for each FK
2. `UniqueConstraint(...)` for each unique constraint
3. `Index(...)` for each index
4. `{'comment': '...', 'schema': '...'}` kwargs dict (if any)

PrimaryKeyConstraint is **not** emitted in declarative `__table_args__`. It is expressed via `primary_key=True` on the `mapped_column()` call.

When `__table_args__` contains only kwargs (no positional constraints/indexes), it renders as a plain dict: `__table_args__ = {'schema': 'testschema'}`. When it contains both, it renders as a tuple with the dict as the last element.

## Server Default Handling

Raw SQL default expressions from the database are processed before emission:

- **PostgreSQL**: Type casts are stripped (`'hello'::character varying` -> `'hello'`). The `::` stripping handles nested expressions and respects quoted strings.
- **MSSQL**: Wrapping parentheses are stripped (`((0))` -> `0`). Leading `N` on string literals is stripped (`N'hello'` -> `'hello'`).
- **MySQL**: Defaults are trimmed but otherwise used as-is (MySQL defaults are clean expressions).
- **SQLite**: Defaults are trimmed but otherwise used as-is (SQLite defaults are literal values or function calls).
- **Serial detection**: PostgreSQL defaults matching `nextval('...'::regclass)` patterns are suppressed entirely (SQLAlchemy handles auto-increment). MySQL/SQLite auto-increment columns have `NULL` defaults, so serial detection returns `false`.
- All remaining defaults are wrapped in `text('...')` for SQLAlchemy's `server_default` parameter.

## Identity Columns

Identity column rendering is dialect-specific:

- **PostgreSQL**: `Identity(start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1)`
- **MSSQL**: `Identity(start=1, increment=1)` (only start and increment)
- **MySQL**: Auto-increment columns are detected via `EXTRA` in `information_schema.COLUMNS`. Identity info is not populated (MySQL has no sequences). Auto-increment is expressed via `autoincrement=True` for composite PKs.
- **SQLite**: AUTOINCREMENT is detected by parsing the CREATE TABLE SQL from `sqlite_master`. Same rendering as MSSQL if identity info were populated.

## Naming Transforms

- **Table -> class name**: `heck::ToUpperCamelCase` (`user_profiles` -> `UserProfiles`). Note: consecutive uppercase in input is normalized (`CustomerAPIPreference` -> `CustomerApiPreference`).
- **Table -> variable name**: `t_` prefix with non-identifier characters replaced by underscores (`simple-items table` -> `t_simple_items_table`).

## Topological Sort

Tables are sorted using Kahn's algorithm so that referenced tables appear before referencing tables in the output. FK constraints define the dependency edges. A `BTreeSet` queue provides alphabetical tiebreaking for deterministic output. If cycles exist (mutual FK references), remaining tables are appended alphabetically. The DDL generator detects cycles and emits a warning header.

## DDL Generator

The `--generator ddl` mode generates raw SQL DDL for cross-database schema migration.

### Cross-dialect type translation

Uses a canonical type system (`CanonicalType` enum in `ddl_typemap/`):

```
ColumnInfo (source) → to_canonical() → CanonicalType → from_canonical() → DDL type string
```

Key translations:

| Source | → PG | → MySQL | → MSSQL | → SQLite |
|--------|------|---------|---------|----------|
| PG `uuid` | UUID | CHAR(36) | UNIQUEIDENTIFIER | TEXT |
| PG `jsonb` | JSONB | JSON* | NVARCHAR(MAX)* | TEXT |
| PG `timestamptz` | TIMESTAMPTZ | DATETIME | DATETIMEOFFSET | DATETIME |
| PG `text[]` | TEXT[] | JSON* | NVARCHAR(MAX)* | TEXT* |
| MySQL `tinyint(1)` | BOOLEAN | TINYINT(1) | BIT | BOOLEAN |
| MSSQL `money` | NUMERIC(19,4) | DECIMAL(19,4) | MONEY | REAL |

\* = lossy/approximate

### Identifier quoting

| Dialect | Style | Example |
|---------|-------|---------|
| PostgreSQL | Double quotes | `"users"` |
| MySQL | Backticks | `` `users` `` |
| MSSQL | Brackets | `[users]` |
| SQLite | Double quotes | `"users"` |

### Auto-increment translation

| Target | DDL syntax |
|--------|-----------|
| PostgreSQL | `SERIAL` / `BIGSERIAL` |
| MySQL | `AUTO_INCREMENT` |
| MSSQL | `IDENTITY(1, 1)` |
| SQLite | `INTEGER PRIMARY KEY AUTOINCREMENT` |

### Default value translation

Source defaults are stripped of dialect-specific syntax, then translated:
- `now()` ↔ `CURRENT_TIMESTAMP` ↔ `GETDATE()`
- `gen_random_uuid()` ↔ `(UUID())` ↔ `NEWID()`
- Boolean: `1`/`0` ↔ `true`/`false` (only for boolean-typed columns)
- Bare string defaults (MySQL stores without quotes) are auto-quoted

### Schema diff

Compares source and target `IntrospectedSchema` and emits ALTER statements:

1. **New tables**: full `CREATE TABLE`
2. **New columns**: `ALTER TABLE ADD COLUMN`
3. **Modified columns**: `ALTER TABLE ALTER/MODIFY COLUMN` (type, nullable, default)
4. **Dropped columns**: `ALTER TABLE DROP COLUMN` (with warning)
5. **Dropped tables**: `DROP TABLE IF EXISTS` (with warning)

Default schemas (`public`, `dbo`, `main`, MySQL database names) are normalized for cross-dialect matching.

### Split-tables output

`--split-tables` produces one file per table. For DDL: `{table}.sql` + `_order.txt`. For Python: `base.py` + `{model}.py` + `__init__.py`.
