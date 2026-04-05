# UVg

A Rust reimplementation of [sqlacodegen](https://github.com/agronholm/sqlacodegen) — connects to a PostgreSQL, MySQL, SQLite, or Microsoft SQL Server database, introspects its schema, and generates SQLAlchemy Python model code.

Single binary, drop-in compatible CLI, same output.

## Installation

```bash
cargo install --path .
```

## Usage

```
uvg <database-url>
```

Accepts SQLAlchemy-style URLs:

```bash
# PostgreSQL
uvg postgresql://user:pass@localhost/mydb

# MySQL / MariaDB
uvg mysql://user:pass@localhost/mydb

# SQLite
uvg sqlite:///path/to/database.db

# Microsoft SQL Server
uvg mssql://user:pass@localhost/mydb

# Table objects instead of declarative classes
uvg --generator tables postgresql://user:pass@localhost/mydb

# Filter specific tables
uvg --tables users,posts mysql://user:pass@localhost/mydb

# Write to file
uvg --outfile models.py sqlite:///myapp.db

# One file per table/class
uvg --split-tables --outfile models/ postgresql://user:pass@localhost/mydb

# Generate DDL for a different database engine
uvg postgresql://source/db --generator ddl --target-dialect mysql

# Generate DDL with file per table
uvg postgresql://source/db --generator ddl --target-dialect mysql --split-tables --outfile ddl/

# Diff source against target, emit ALTER statements
uvg postgresql://source/db mysql://target/db --generator ddl

# Interactive TUI: enter URLs, view diff, optionally apply to target
uvg -i
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

## Building from source

```bash
git clone https://github.com/johndauphine/uvg.git
cd uvg
cargo build --release
```

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
