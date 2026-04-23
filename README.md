# UVg

[![Crates.io](https://img.shields.io/crates/v/uvg.svg)](https://crates.io/crates/uvg)
[![License](https://img.shields.io/crates/l/uvg.svg)](#license)

Fast schema introspection for PostgreSQL, MySQL, SQLite, and MSSQL. Generates SQLAlchemy models, cross-dialect DDL, or migration diffs — with an interactive TUI for reviewing and applying changes. Drop-in replacement for [sqlacodegen](https://github.com/agronholm/sqlacodegen).

## What UVg does

- **SQLAlchemy model generation** — declarative ORM classes or `Table()` metadata, byte-for-byte compatible with sqlacodegen output.
- **Cross-dialect DDL** — introspect one database and emit `CREATE TABLE` statements for a different engine, with automatic type translation (`jsonb` → `JSON`, `uuid` → `UNIQUEIDENTIFIER`, and so on across all 16 source×target permutations).
- **Schema diff** — compare a source database against a target and emit `ALTER TABLE` statements to converge them.
- **Interactive TUI** — enter two URLs, scroll through the generated diff, and apply it to the target with a confirmation prompt.
- **Fast** — single static binary; ~10× faster than sqlacodegen on PostgreSQL and ~40× on MSSQL.

## Installation

### From crates.io

```bash
cargo install uvg
```

### Prebuilt binaries

Download the archive for your platform from the [latest release](https://github.com/johndauphine/uvg/releases/latest) (Linux x86_64/aarch64, macOS x86_64/aarch64, Windows x86_64), extract, and place `uvg` on your `PATH`.

### From source

```bash
git clone https://github.com/johndauphine/uvg.git
cd uvg
cargo install --path .
```

## Usage

UVg accepts SQLAlchemy-style URLs for both the source and (optional) target database:

```
postgresql://user:pass@host/db
mysql://user:pass@host/db
sqlite:///path/to/db.sqlite
mssql://user:pass@host/db
```

### Generate SQLAlchemy models

```bash
# Modern declarative classes (default)
uvg postgresql://localhost/mydb -o models.py

# Core Table() metadata objects
uvg --generator tables postgresql://localhost/mydb -o models.py

# One file per table
uvg --split-tables --outfile models/ postgresql://localhost/mydb

# Filter specific tables
uvg --tables users,posts postgresql://localhost/mydb
```

### Generate DDL in another dialect

Introspect a database and emit `CREATE TABLE` statements for a different engine. Types are translated automatically.

```bash
# PostgreSQL schema -> MySQL DDL
uvg postgresql://localhost/mydb --generator ddl --target-dialect mysql -o schema.sql

# MSSQL schema -> SQLite DDL, one file per table
uvg mssql://localhost/mydb --generator ddl --target-dialect sqlite --split-tables -o ddl/
```

### Diff two schemas and generate a migration

Compare a source database against a live target and emit `ALTER TABLE` statements to converge them.

```bash
uvg postgresql://source/db mysql://target/db --generator ddl -o migration.sql
```

The target dialect is inferred from the target URL scheme. Same-dialect migrations converge cleanly — running the diff again after applying shows zero changes.

### Interactive TUI

Launch the TUI to enter source and target URLs, scroll through the generated diff, and apply it to the target with a confirmation prompt.

```bash
uvg -i                                              # prompts for URLs
uvg -i postgresql://source/db postgresql://target/db
```

### Options

| Flag | Description |
|---|---|
| `--generator <TYPE>` | `declarative` (default), `tables`, or `ddl` |
| `--target-dialect <DIALECT>` | Target SQL dialect for DDL: `postgres`, `mysql`, `sqlite`, `mssql` |
| `--split-tables` | Output one file per table (works with all generators) |
| `--tables <LIST>` | Comma-delimited table names to include |
| `--schemas <LIST>` | Schemas to introspect (default: `public` for PG, `dbo` for MSSQL, database name for MySQL) |
| `--noviews` | Skip views |
| `--options <LIST>` | `noindexes`, `noconstraints`, `nocomments`, `nobidi`, `nofknames`, `noidsuffix`, `nosyntheticenums`, `nonativeenums`, `keep_dialect_types` |
| `--outfile <PATH>` | Output file or directory (default: stdout) |
| `--interactive`, `-i` | Launch interactive TUI for DDL diff and apply |
| `--trust-cert` | Trust the server certificate (MSSQL only) |

## Output Examples

### Declarative generator

```python
from typing import Optional

import datetime
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class Users(Base):
    __tablename__ = 'users'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    email: Mapped[str] = mapped_column(String(255), unique=True)
    bio: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(timezone=True), server_default=text('now()')
    )
```

### Tables generator

```python
from sqlalchemy import Column, Integer, MetaData, String, Table, Text

metadata = MetaData()

t_users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(100), nullable=False),
    Column('email', Text),
    schema='public'
)
```

## Supported Databases

### PostgreSQL (tested: 16)

Scalars: `bool`, `int2`, `int4`, `int8`, `float4`, `float8`, `numeric`, `text`, `varchar`, `char`, `bytea`, `date`, `time`, `timetz`, `timestamp`, `timestamptz`, `interval`

Dialect types: `uuid`, `json`, `jsonb`, `inet`, `cidr`

Arrays: `_int4`, `_text`, and other array types via the `ARRAY()` wrapper

URL schemes: `postgresql://`, `postgresql+psycopg2://`

### Microsoft SQL Server (tested: 2019, 2022)

Scalars: `bit`, `tinyint`, `smallint`, `int`, `bigint`, `real`, `float`, `decimal`, `numeric`, `money`, `smallmoney`

Strings: `char`, `varchar`, `nchar`, `nvarchar`, `text`, `ntext` (with collation)

Date/time: `date`, `time`, `datetime`, `datetime2`, `smalldatetime`, `datetimeoffset`

Binary: `binary`, `varbinary`, `image`

Dialect types: `uniqueidentifier`

URL schemes: `mssql://`, `mssql+pytds://`, `mssql+pyodbc://`, `mssql+pymssql://`

### MySQL / MariaDB (tested: 8.0, 9.6)

Scalars: `tinyint`, `smallint`, `mediumint`, `int`, `bigint` (with unsigned variants), `float`, `double`, `decimal`

Strings: `char`, `varchar`, `text`, `tinytext`, `mediumtext`, `longtext`

Date/time: `date`, `time`, `datetime`, `timestamp`, `year`

Binary: `binary`, `varbinary`, `blob`, `tinyblob`, `mediumblob`, `longblob`

Special: `json`, `enum`, `set`, `bit`, `boolean`

Note: `tinyint(1)` is automatically mapped to `Boolean`.

URL schemes: `mysql://`, `mysql+pymysql://`, `mysql+mysqldb://`, `mysql+aiomysql://`, `mysql+asyncmy://`, `mariadb://`, `mariadb+pymysql://`

### SQLite

SQLite uses type affinity. Recognized types: `integer`, `smallint`, `bigint`, `real`, `float`, `double`, `numeric`, `decimal`, `text`, `varchar`, `char`, `blob`, `date`, `datetime`, `timestamp`, `time`, `boolean`, `json`

Unknown types are mapped using SQLite affinity rules (contains "INT" -> Integer, contains "TEXT"/"CHAR" -> Text, etc.).

AUTOINCREMENT is detected from the CREATE TABLE SQL. CHECK constraints are parsed for synthetic enum generation.

URL schemes: `sqlite:///relative/path`, `sqlite:////absolute/path`, `sqlite:///:memory:`

## Performance

Benchmarked against sqlacodegen 3.2.0 on the StackOverflow 2010 database (9 tables) using [hyperfine](https://github.com/sharkdp/hyperfine):

| Command | Mean | Min | Max |
|---|---|---|---|
| sqlacodegen (PostgreSQL) | 1.140s | 1.099s | 1.167s |
| **uvg (PostgreSQL)** | **113.4ms** | **108.1ms** | **130.5ms** |
| sqlacodegen (MSSQL) | 1.187s | 1.134s | 1.231s |
| **uvg (MSSQL)** | **29.9ms** | **28.1ms** | **32.1ms** |

**10x faster on PostgreSQL, 40x faster on MSSQL.**

## Running tests

```bash
cargo test
```

Integration tests require a live database (except SQLite which runs in-memory):

```bash
# PostgreSQL
DATABASE_URL=postgresql://user:pass@localhost/testdb cargo test --test integration -- --ignored

# MySQL
MYSQL_URL=mysql://user:pass@localhost/testdb cargo test --test integration -- --ignored

# Microsoft SQL Server
DATABASE_URL=mssql://user:pass@localhost/testdb cargo test --test integration -- --ignored

# SQLite (runs automatically, no server needed)
cargo test --test integration test_introspect_sqlite_in_memory
```

## Acknowledgments

UVg is a reimplementation of [sqlacodegen](https://github.com/agronholm/sqlacodegen) by Alex Grönholm, licensed under MIT. See [`NOTICE`](NOTICE) for full attribution.

## License

Dual-licensed under either of:

- MIT License ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual-licensed as above, without any additional terms or conditions.
