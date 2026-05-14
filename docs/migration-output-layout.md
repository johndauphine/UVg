# Migration Output Layout — Implementation Plan

Self-contained plan for organizing uvg's generated DDL into per-table,
git-trackable files. Read it cold — it includes the goal, rationale,
the exact files to touch, and the test plan.

## Background

uvg already captures schema differences. `diff_schemas()` in
`src/codegen/ddl_diff.rs` takes a source and target database, introspects
both, and emits the DDL (CREATE/ALTER/DROP) that converges target →
source. Today that output goes to stdout as a single blob.

To make those changes reviewable in git on a per-table basis — so
`git log -- migrations/users/` is the history of the `users` table —
the output needs to be split per-table with a stable filename
convention.

This is **purely an output-organization change**. uvg always captures
every change it detects. No policy layer, no state tracking, no
destination metadata table.

## Goal

Under a user-specified output directory, split generated DDL into
per-table subdirs with timestamped filenames:

```
migrations/
  users/
    20260513T193000Z__add-email.sql
  events/
    20260514T084500Z__rename-table.sql
  _schema/
    20260513T193000Z__add-email.sql       # enums, CREATE SCHEMA, etc.
  _runs/
    20260513T193000Z__add-email.json      # one manifest per uvg run
```

Add `--outfile <path>` (sqlacodegen-compatible single-file mode) and
`--out-dir <dir>` (per-table layout). Default remains stdout.

## Non-goals

- **No policy / gating.** No allow/freeze/deny. uvg always captures
  every change it detects. Gating happens in PR review of the generated
  files, not inside uvg.
- **No stateful tracking.** No `_uvg_migrations` table in the target.
  No `.applied` ledger file. uvg compares two live snapshots every run;
  the operator decides what to apply by reading git diff.
- **No rename detection.** uvg already treats renames as drop + add.
  Unchanged.
- **No changes to introspection.** Touch `src/codegen/`, `src/cli.rs`,
  `src/main.rs`, `src/tui/mod.rs`. Do not change `src/introspect/` or
  `src/schema.rs`.

## Known gaps (out of scope for this plan)

- **`CREATE SCHEMA` emission.** `compute_changes` does not emit
  `CREATE SCHEMA "billing"` ahead of `CREATE TABLE "billing"."orders"`.
  Pre-existing behavior — `diff_schemas` has never produced schema
  DDL — so non-default schemas must already exist in the target
  before applying the generated migration. Fixing this needs target
  introspection of `pg_namespace` / `information_schema.schemata`
  (we don't introspect schemas today) plus cross-dialect schema DDL.
  When implemented, the `_schema/` bucket and the apply-`_schema`-first
  ordering already in place are the right home for it.

## Design

### The change-tagging refactor

`diff_schemas()` currently builds `Vec<String>`. Refactor it to build
`Vec<Change>` first, then serialize. `Change` carries the SQL plus the
`(schema, table)` tuple the splitter needs.

```rust
// New module: src/output.rs

#[derive(Debug, Clone)]
pub struct Change {
    pub table_schema: String,        // "" for default schemas
    pub table_name: Option<String>,  // None for non-table-scoped DDL
                                     // (enums, CREATE SCHEMA, etc.)
    pub sql: String,
}
```

Split `diff_schemas()` into two public functions:

```rust
pub fn compute_changes(
    source: &IntrospectedSchema,
    target: &IntrospectedSchema,
    options: &DdlOptions,
) -> Vec<Change>;

pub fn render_changes(
    changes: &[Change],
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> String;

// Kept for backward compat (TUI + existing callers):
pub fn diff_schemas(...) -> String {
    render_changes(&compute_changes(source, target, options), ...)
}
```

The refactor is mechanical: every `stmts.push(s)` becomes a `Change`
push tagged with its `(schema, Option<table>)`. Where multiple SQL
statements emit for a single column (PG type+null+default), keep them
as separate `Change` records so each lands in the right file.

The empty-diff sentinel `"-- No schema changes detected.\n"` stays in
`render_changes()` for backward compat with the TUI's
`ddl_output.contains("No schema changes detected")` check at
`src/tui/mod.rs:307`.

### CLI flags

Three new fields on `Cli`:

```rust
/// Write the entire run to a single file (sqlacodegen-compatible).
#[arg(long)]
pub outfile: Option<PathBuf>,

/// Write per-table files into this directory.
#[arg(long)]
pub out_dir: Option<PathBuf>,

/// Slug used in generated filenames. Defaults to
/// "<source-dialect>_to_<target-dialect>".
#[arg(long)]
pub name: Option<String>,
```

Precedence: `--outfile` wins over `--out-dir` if both are set. Default
(neither set) keeps today's stdout behavior.

### Per-table layout

Under `--out-dir <dir>`:

```
<dir>/
  <table>/                  # one subdir per modified table
    <ts>__<tag>.sql
  _schema/                  # non-table-scoped DDL (enums, schemas)
    <ts>__<tag>.sql
  _runs/                    # one manifest per uvg run
    <ts>__<tag>.json
```

- `<ts>` = `YYYYMMDDTHHMMSSZ` (UTC, compact, sortable).
- `<tag>` = `--name` value, or default `<src>_to_<tgt>` (e.g.
  `pg_to_mysql`).
- Table subdir name: `<table_name>` for default-schema tables,
  `<schema>__<table>` for non-default schemas.
- `_schema/` and `_runs/` use the underscore prefix to sort visually
  distinct from real table names.

### File header

Every generated `.sql` file starts with a provenance header:

```sql
-- Generated by uvg <version> on 2026-05-13T19:30:00Z (UTC)
-- Run:    20260513T193000Z__add-email
-- Table:  public.users
-- Source: postgresql  ->  Target: postgresql
```

### Manifest

One JSON per **non-empty** run in `_runs/`:

```json
{
  "run_id": "20260513T193000Z__add-email",
  "generated_at": "2026-05-13T19:30:00Z",
  "uvg_version": "1.5.0",
  "source_dialect": "postgresql",
  "target_dialect": "postgresql",
  "files": [
    "users/20260513T193000Z__add-email.sql",
    "_schema/20260513T193000Z__add-email.sql"
  ],
  "stats": {"changes": 5}
}
```

**Empty diffs write nothing.** No `.sql` files, no manifest. The
mental model is: "if there are no schema changes, no new files appear
in git." A no-op run prints a single line to stderr
(`uvg: no schema changes`) and exits 0; that's the only signal that
uvg ran. If you need a persistent record of every invocation, redirect
stderr in your wrapper.

### Apply ordering

When applying multiple files, the order is:

1. All `_schema/` files at the earliest timestamp first (enums and
   schemas must exist before tables that reference them).
2. Per-table files at the same timestamp, in `topo_sort_tables()` order
   (FK dependencies — already implemented in `src/codegen/mod.rs`).
3. Next timestamp, repeat.

`find <dir> -name '*.sql' | sort` is **not** correct apply order on its
own — the schema-before-tables rule is load-bearing.

### TUI surface

The TUI today applies the whole DDL output via `db::execute_ddl()`.
With per-table output, the UI grows table selection:

1. Pass `Vec<Change>` (not just `ddl_output: String`) into `App`.
2. Tree pane on the left lists tables (and `_schema`); right pane shows
   the SQL for the selected node.
3. Each node has a checkbox; default = all checked.
4. Apply runs `db::execute_ddl()` only on SQL from checked nodes,
   honoring the apply ordering above.
5. Status bar shows `N table(s) selected · M statement(s)`.

The single-file / stdout path bypasses the tree pane and renders today's
flat view — TUI tree only activates when changes span multiple tables.

## File-by-file change list

| File | Change |
|---|---|
| `src/output.rs` | **New.** `Change` struct, `compute_changes()`, `render_changes()`, per-table file splitter, manifest writer. ~200 LOC. |
| `src/codegen/ddl_diff.rs` | `diff_schemas()` becomes a thin wrapper over `compute_changes()` + `render_changes()`. Tag each pushed statement with `(schema, Option<table>)`. ~50 LOC delta. |
| `src/cli.rs` | Three new `#[arg]` fields: `--outfile`, `--out-dir`, `--name`. ~10 LOC. |
| `src/main.rs` | Output dispatch: stdout (default) / `--outfile` / `--out-dir`. ~20 LOC. |
| `src/tui/mod.rs` | Tree pane grouping by table, per-table checkboxes, ordered apply on checked nodes. ~60 LOC delta. |
| `src/codegen/mod.rs` | Re-export `Change`, `compute_changes`, `render_changes`. |
| `Cargo.toml` | Add `serde_json` if not already present (for manifest writing). |
| `README.md` | New "Output layout" subsection with stdout / `--outfile` / `--out-dir` examples. |
| `tests/integration.rs` | Two new integration tests (see test plan). |

## Test plan

Unit tests inline in `src/output.rs`:

1. `compute_changes` returns one `Change` per ALTER/CREATE/DROP with
   correct `(schema, table)` tagging. Convert one or two existing
   string-grep tests in `ddl_diff` to `Change`-asserting tests; keep
   the rest pointing at `diff_schemas()` for the backward-compat
   regression.
2. PG type + nullability + default produces three separate `Change`
   entries — each must be placeable into its file separately.
3. Non-table-scoped DDL (enum CREATE TYPE) tags with `table_name: None`
   and lands in `_schema/`.
4. Manifest serializes/deserializes round-trip via serde.

CRM matrix regression: `./testdata/crm/run_matrix.sh` with no output
flag produces byte-identical stdout to before the refactor.

Integration tests in `tests/integration.rs`:

5. End-to-end per-table layout: run with `--out-dir <tmpdir>`. Assert:
   - One subdir per modified table, named after the table.
   - `_schema/` present iff non-table-scoped DDL was emitted.
   - `_runs/<run_id>.json` manifest exists and parses.
   - Every `.sql` file starts with the provenance header.
   - Re-running against unchanged source/target writes **nothing** —
     no `.sql`, no `_schema/`, no `_runs/`. `<tmpdir>` is byte-identical
     before and after the second run.
6. `--outfile`: single combined file with provenance header, body
   matches today's stdout.

## Implementation reality check

Most of the engine already exists.

| What you need | Status today |
|---|---|
| Source/target introspection | ✅ `src/introspect/*` |
| Cross-dialect schema diff | ✅ `src/codegen/ddl_diff.rs::diff_schemas()` |
| DDL rendering per dialect | ✅ `src/codegen/ddl.rs` + `src/ddl_typemap/` |
| Topological sort for FKs | ✅ `src/codegen/mod.rs::topo_sort_tables()` |
| TUI DDL view + apply | ✅ `src/tui/mod.rs` |
| **`Change`-tagged diff output** | ❌ refactor `diff_schemas()` |
| **Per-table file splitter** | ❌ new `src/output.rs` |
| **Manifest writer** | ❌ part of `src/output.rs` |
| **TUI per-table selection** | ❌ tree pane + checkbox state in `App` |

Net new code: ~300–350 LOC across the refactor, `output.rs`, and the
TUI tree pane. Plus tests.

## Implementation order

Each step has tests that gate the next.

1. **Refactor `diff_schemas()`** into `compute_changes()` +
   `render_changes()`. Existing tests pass unchanged. CRM matrix
   produces byte-identical output.
2. **Add `src/output.rs`** with the splitter and manifest writer.
   Unit tests 1–4. No CLI wiring yet.
3. **Wire `--outfile` / `--out-dir` / `--name`** in `main.rs`.
   Integration tests 5–6. Stdout path remains byte-identical when no
   flag is set.
4. **TUI per-table selection.** Tree pane, checkboxes, ordered apply.
   Manual smoke test only.
5. **Docs.** Update `README.md`.

Each step = one commit. Run `cargo test` and `cargo clippy --all-targets`
before each commit.
