# Schema Evolution Contracts — Implementation Plan

This document is a self-contained plan for an AI agent (or human) to implement
schema-evolution contracts in uvg. Read it cold — it includes the goal,
the rationale, the exact files to touch, the public surface, and the test
plan. Do not deviate from the scope in the **Non-goals** section without
flagging it back to the user first.

## Background

uvg already produces schema diffs: given a source database and a target
database, `diff_schemas()` in `src/codegen/ddl_diff.rs` emits a flat
`Vec<String>` of SQL statements (`CREATE TABLE`, `ALTER TABLE ADD COLUMN`,
`ALTER TABLE ALTER COLUMN ... TYPE`, `DROP TABLE`, `DROP COLUMN`) that
converge target → source. The TUI in `src/tui/mod.rs` lets a human review
that output and apply it.

The [dlt](https://github.com/dlt-hub/dlt) project popularized **schema
contracts** — a small governance layer that lets pipeline operators reject or
silently absorb specific kinds of schema change at load time. uvg's diff is
the natural moment to enforce the same governance, but on a *plan* rather
than a *load*. This plan adds that layer.

## Goal

Allow a uvg user (or CI job) to declare, per migration, which classes of
schema change are allowed:

- **Evolve** — emit the DDL (today's behavior, the default).
- **Freeze** — refuse: print the violation and exit non-zero so CI can gate.
- **Discard value** — silently omit the DDL from output.

Granularity matches dlt's three contract levels:

| Granularity | Changes it gates |
|---|---|
| `tables` | New tables (`CREATE TABLE`), dropped tables (`DROP TABLE`) |
| `columns` | New columns (`ADD COLUMN`), dropped columns (`DROP COLUMN`) |
| `data_type` | Type changes (`ALTER COLUMN ... TYPE`), nullability changes, default changes |

Per-table overrides are supported via a small YAML file. The CLI accepts
both an inline flag for the simple case and a file path for the per-table
case.

## Non-goals

These are out of scope for this change. Do not implement them. If the
implementer thinks one is needed, stop and ask the user.

- **Data ingestion / record-stream inference.** uvg remains metadata-only.
  No JSON/Parquet/CSV ingestion. No variant columns. No nested-table
  unpacking. dlt's `discard_rows` mode has no analogue here and is dropped.
- **Stateful evolution tracking.** No `_uvg_version` metadata table in the
  target. Contracts apply to the current diff only.
- **Slack notifications, webhooks, or any I/O beyond stdout/stderr/TUI.**
- **Changes to introspection.** Touch `src/codegen/` and `src/cli.rs` only;
  do not change `src/introspect/` or `src/schema.rs` data structures.
- **Renaming or merging tables/columns.** uvg already does not detect renames
  (a rename shows as a drop + add), and contracts do not change that.

## Design

### The change-tagging refactor

`diff_schemas()` currently builds `Vec<String>`. Refactor it to build a
`Vec<Change>` first, then serialize. `Change` is a tagged enum that carries
both the SQL text and a `ChangeKind` discriminator.

```rust
// New module: src/contract.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChangeKind {
    NewTable,
    DropTable,
    NewColumn,
    DropColumn,
    TypeChange,
    NullabilityChange,
    DefaultChange,
}

#[derive(Debug, Clone)]
pub struct Change {
    pub kind: ChangeKind,
    pub table_schema: String, // post-normalization (i.e. "" for default schemas)
    pub table_name: String,
    pub column: Option<String>, // None for table-level changes
    pub sql: String,            // the rendered SQL statement(s)
}

impl ChangeKind {
    pub fn granularity(self) -> Granularity {
        match self {
            ChangeKind::NewTable | ChangeKind::DropTable => Granularity::Tables,
            ChangeKind::NewColumn | ChangeKind::DropColumn => Granularity::Columns,
            ChangeKind::TypeChange
            | ChangeKind::NullabilityChange
            | ChangeKind::DefaultChange => Granularity::DataType,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Granularity {
    Tables,
    Columns,
    DataType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractMode {
    Evolve,  // default
    Freeze,
    DiscardValue,
}

#[derive(Debug, Clone)]
pub struct Contract {
    pub tables: ContractMode,
    pub columns: ContractMode,
    pub data_type: ContractMode,
    /// Per-table overrides keyed by (schema, name). Schema "" matches the
    /// default schema for the dialect (matches diff_schemas's normalization).
    pub per_table: HashMap<(String, String), TableContract>,
}

#[derive(Debug, Clone)]
pub struct TableContract {
    pub tables: Option<ContractMode>,
    pub columns: Option<ContractMode>,
    pub data_type: Option<ContractMode>,
}

impl Default for Contract {
    fn default() -> Self {
        Self {
            tables: ContractMode::Evolve,
            columns: ContractMode::Evolve,
            data_type: ContractMode::Evolve,
            per_table: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ContractViolation {
    pub change: Change,
    pub mode: ContractMode, // always Freeze when this is constructed
}

pub struct ApplyResult {
    pub allowed: Vec<Change>,
    pub discarded: Vec<Change>,
    pub violations: Vec<ContractViolation>,
}

impl Contract {
    /// Look up the effective mode for a given change, applying per-table
    /// override if present.
    pub fn mode_for(&self, change: &Change) -> ContractMode { /* ... */ }

    /// Partition changes into allowed / discarded / violations.
    /// Pure function; no I/O.
    pub fn apply(&self, changes: Vec<Change>) -> ApplyResult { /* ... */ }
}
```

### Refactor target

`diff_schemas()` in `src/codegen/ddl_diff.rs` is currently 230 lines of
imperative `stmts.push(format!(...))` calls. The refactor is mechanical:
every `stmts.push(s)` becomes `changes.push(Change { kind, schema, name, column, sql: s })`.

Split the public function into two:

```rust
// Public — kept for backward compatibility, calls the new pair.
pub fn diff_schemas(
    source: &IntrospectedSchema,
    target: &IntrospectedSchema,
    options: &DdlOptions,
) -> String {
    let changes = compute_changes(source, target, options);
    render_changes(&changes, source.dialect, options.target_dialect)
}

// New — used by the contract pipeline.
pub fn compute_changes(
    source: &IntrospectedSchema,
    target: &IntrospectedSchema,
    options: &DdlOptions,
) -> Vec<Change> { /* the existing body, but pushing Change instead of String */ }

pub fn render_changes(
    changes: &[Change],
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> String { /* prepends the same header, joins sql fields with "\n\n" */ }
```

The empty-diff sentinel string (`"-- No schema changes detected.\n"`) stays
in `render_changes()` for backward compat with the TUI's
`ddl_output.contains("No schema changes detected")` check at
`src/tui/mod.rs:307`.

Where a single column diff currently produces multiple SQL statements (e.g.
PG type + nullability + default), emit one `Change` per kind, not one
merged blob. This is important so the contract can rule on type changes
separately from nullability/default changes within the same column.

### CLI surface

Extend `src/cli.rs`:

```rust
// On struct Cli, add:

/// Contract rules. Either a bare mode applied to all granularities
/// (e.g. "freeze") or comma-delimited key=mode pairs
/// (e.g. "tables=freeze,columns=evolve,data_type=freeze").
/// Modes: evolve (default), freeze, discard_value.
#[arg(long)]
pub contract: Option<String>,

/// Path to a YAML contract file (overrides --contract for any keys it sets).
#[arg(long)]
pub contract_file: Option<String>,

/// Output format for contract violations on stderr. `text` is the
/// default human-readable form; `json` emits one JSON object per
/// violation (newline-delimited) for CI / governance tooling.
#[arg(long, value_parser = ["text", "json"], default_value = "text")]
pub contract_format: String,
```

Add a parser in `src/contract.rs`:

```rust
pub fn parse_contract_arg(s: &str) -> Result<Contract, UvgError>;
pub fn load_contract_file(path: &Path) -> Result<Contract, UvgError>;
pub fn merge_contracts(base: Contract, overlay: Contract) -> Contract;
```

`parse_contract_arg` accepts two forms, matching dlt's shorthand:

1. A bare mode — `"freeze"`, `"evolve"`, or `"discard_value"` — applied
   to all three granularities.
2. Comma-delimited `key=mode` pairs — `"tables=freeze,columns=evolve"`.
   Keys not mentioned default to `evolve`.

Mixing the two forms in one string is an error.

The YAML schema:

```yaml
# contract.yaml
defaults:
  tables: freeze
  columns: evolve
  data_type: freeze
tables:
  "public.users":
    columns: discard_value
  "public.events":
    tables: freeze
    columns: evolve
    data_type: evolve
```

Key format is `"schema.name"`. For default-schema tables, use either
`"public.users"` (PG), `"dbo.users"` (MSSQL), `"main.users"` (SQLite), or
the actual MySQL database name; uvg's existing schema normalization in
`normalize_schema()` (`src/codegen/ddl_diff.rs:155`) collapses all of these
to `""` internally, so the contract loader should normalize the same way
when populating `Contract::per_table`.

### Configuration discovery

Contracts are coupled to a specific source/target schema pair, so they
belong next to the repo that owns the schema — versioned in git, reviewed
in the same PRs as schema changes. uvg therefore looks for a contract in
three places, in precedence order:

1. **`--contract <inline>` / `--contract-file <path>`** — explicit,
   per-invocation. Wins if set. `--contract-file` overrides `--contract`
   for any granularity it sets; per-granularity keys merge via
   `merge_contracts()`.
2. **`./uvg.yaml`** in the process's current working directory —
   auto-discovered. The contract lives under a `contract:` key, leaving
   room for future settings without a second file. When an auto-discovered
   file is loaded, print one line to stderr so the operator can see it
   took effect:

   ```
   uvg: loaded contract from ./uvg.yaml
   ```

3. **No config** — all defaults (`evolve` everywhere). Today's behavior.

Auto-discovery is shallow (cwd only, no walking up parent directories).
Walking up risks picking up a contract from an unrelated repo when uvg is
run from a subdirectory; the explicit `--contract-file` flag covers that
case without surprise.

Two file layouts are accepted for `./uvg.yaml`:

```yaml
# Bare contract — everything under the top level is treated as contract config.
defaults:
  tables: freeze
  columns: evolve
  data_type: freeze
```

```yaml
# Nested under `contract:` — preferred, leaves room for other settings.
contract:
  defaults:
    tables: freeze
    columns: evolve
  tables:
    "public.users":
      columns: discard_value
```

If both `defaults:` (bare) and `contract:` (nested) keys are present at
the top level, the loader errors out — pick one. The nested form is
preferred and will be the only form documented in the README.

**Out of scope for this change:** a user-level config dir
(`~/.config/uvg/`, `%APPDATA%\uvg\`). uvg has no genuinely global
preferences today, and a user-level layer would let contracts vary by
*who runs uvg* rather than *which repo it runs against* — the opposite of
the governance shape we want. If a user-level config becomes warranted
later (e.g. shared `trust_cert` defaults), add it as a separate change.

### Main flow

In `src/main.rs`, after the DDL is produced but before output:

1. Build the `Contract` from, in precedence order: `Cli::contract_file`
   if set, else `Cli::contract` if set, else auto-discovered `./uvg.yaml`
   if present, else `Contract::default()`.
2. Call `compute_changes(...)` instead of `diff_schemas(...)` when a contract
   is set. (When no contract is set, the existing `diff_schemas()` path is
   preserved to keep the no-contract code path unchanged.)
3. Call `contract.apply(changes)`.
4. If `violations` is non-empty:
   - When `--contract-format=text` (default), print each violation to
     stderr in this format:

     ```
     uvg: contract violation [freeze on data_type]
       table: public.users
       column: email
       sql: ALTER TABLE "users" ALTER COLUMN "email" TYPE TEXT;
     ```

   - When `--contract-format=json`, emit one JSON object per violation,
     newline-delimited (NDJSON), to stderr:

     ```json
     {"mode":"freeze","granularity":"data_type","kind":"TypeChange","schema":"public","table":"users","column":"email","sql":"ALTER TABLE \"users\" ALTER COLUMN \"email\" TYPE TEXT;"}
     ```

   - Exit with status `2`.
5. Otherwise, render `allowed` via `render_changes()` and emit as today.
6. `discarded` changes are silently dropped from output. Add a single
   stderr summary line: `uvg: 3 change(s) discarded by contract`.

Exit codes:
- `0` — success (no changes, or all allowed/discarded)
- `1` — existing uvg errors (connection, introspection, etc.)
- `2` — contract violation (new)

### Output file layout

uvg today writes generated DDL to stdout. To make contract-vetted plans
reviewable in git **with per-table change history**, the output is split
by table into a hierarchical layout. uvg remains stateless: git is the
audit trail; uvg never tracks "what was applied where."

**Flags (all new on `Cli`):**
- **`--outfile <path>`** — write the entire run to a single explicit
  path. Matches sqlacodegen's `--outfile` for drop-in compatibility.
  Disables per-table splitting.
- **`--out-dir <dir>`** — write to a directory using the per-table
  layout below. Recommended for git-tracked migrations.
- **`--name <slug>`** — override the auto-generated tag in filenames.
  Defaults to `<source-dialect>_to_<target-dialect>`. Human-meaningful
  slugs (e.g. `add-users-email`) make `git log` reviewable.

Default (neither outfile nor out-dir set) remains stdout — today's
behavior is unchanged.

**Per-table layout** (`--out-dir migrations/ --name add-users-email`):

```
migrations/
  users/
    20260513T193000Z__add-users-email.sql       # ALTER TABLE users ...
    20260514T084500Z__rename-username.sql
  events/
    20260514T084500Z__rename-username.sql       # changes that touched events
  _schema/
    20260513T193000Z__add-users-email.sql       # enum types, CREATE SCHEMA,
                                                # non-table-scoped DDL
  _runs/
    20260513T193000Z__add-users-email.json      # manifest per run
    20260514T084500Z__rename-username.json
```

- **One subdirectory per table**, named after the table
  (lowercased, underscored, `<schema>__<table>` for non-default
  schemas). `git log -- migrations/users/` is that table's history.
- **`_schema/`** holds non-table-scoped DDL: enum type creates, schema
  creates, anything that isn't tied to one specific table. The
  underscore prefix sorts visually distinct from real table names.
- **`_runs/`** holds a JSON manifest per uvg run that ties the
  scattered per-table files back together. Manifest is metadata only —
  not stateful tracking — it records what *this run produced*, not
  what's been applied. Shape:

  ```json
  {
    "run_id": "20260513T193000Z__add-users-email",
    "generated_at": "2026-05-13T19:30:00Z",
    "uvg_version": "1.5.0",
    "source_dialect": "postgresql",
    "target_dialect": "postgresql",
    "contract": {"tables": "evolve", "columns": "freeze", "data_type": "evolve"},
    "files": [
      "users/20260513T193000Z__add-users-email.sql",
      "_schema/20260513T193000Z__add-users-email.sql"
    ],
    "stats": {"changes": 5, "violations": 0, "discarded": 0}
  }
  ```

- Files within a run share a timestamp prefix, so a chronological
  walk across all subdirs (`find migrations -name '*.sql' | sort`)
  yields the apply sequence.

**File header** (every `.sql` file):

```sql
-- Generated by uvg <version> on 2026-05-13T19:30:00Z (UTC)
-- Run:    20260513T193000Z__add-users-email
-- Table:  public.users
-- Source: postgresql  ->  Target: postgresql
-- Contract: tables=evolve, columns=freeze, data_type=evolve
```

**Apply ordering** (when applying multiple files):
`_schema/` files at a given timestamp apply before per-table files at
that timestamp. Within each directory, files apply in timestamp order.
FK creation already rides with the child table via the existing
`topo_sort_tables()` in `src/codegen/mod.rs`; that ordering is
preserved when populating per-table files.

**Re-runs and state**:
uvg is stateless — `git log` is the audit trail. Running uvg against an
unchanged source/target produces a manifest recording the no-op and
zero `.sql` files. Running against a target already caught up to the
source likewise produces no `.sql` files; only the manifest records the
run. This is the deliberate trade for not maintaining a state table.

**Contract violations**:
When `violations` is non-empty, uvg exits 2 and writes **no `.sql`
files and no manifest**. The frozen plan is the rejected artifact;
nothing gets persisted to disk. Fix the contract or the source/target,
then re-run.

**Discarded changes**:
Omitted from `.sql` output. The manifest's `stats.discarded` count
records how many were dropped, so the audit evidence survives even when
the DDL itself doesn't.

**`--outfile` (single-file) mode** keeps today's "one combined output"
shape: all tables interleaved in one file with a single header. Useful
for one-off diffs or piping into other tools. Per-table splitting only
kicks in under `--out-dir`.

### TUI surface

In `src/tui/mod.rs`, the diff view is restructured around per-table
grouping when changes span multiple tables:

1. Pass the `Contract` and the `Vec<Change>` (not just `ddl_output:
   String`) into `App`. The TUI groups changes by `(schema, table)`
   plus a `_schema` bucket for non-table-scoped changes.
2. **Tree-style left pane** lists tables (and `_schema`); the right
   pane shows the SQL for the selected node, colored per verdict:
   - **Green** — evolve (will be applied)
   - **Red** — freeze violation (blocks apply)
   - **Gray (dimmed)** — discard (omitted)
3. **Per-node selection**: each table-node has a checkbox. Default is
   "all checked." Operators can uncheck a table to skip applying it
   this run. Violations remain visible but their containing node is
   non-checkable.
4. **Status bar** (`render_status()`, `src/tui/mod.rs:446`) shows
   counts: `X allowed · Y discarded · Z blocked · N table(s) selected`.
5. **Apply** (`apply_ddl()`, `src/tui/mod.rs:638`) is disabled when
   `violations` is non-empty *for selected nodes*. Apply executes the
   `allowed` statements from the selected tables only, using
   `db::execute_ddl()`'s existing `;`-split semantics. `_schema/`
   statements always apply first (matches the file-layout ordering).
6. The hint text in `render_ddl_view()` at `src/tui/mod.rs:484` becomes
   `contract violations in selected tables — uncheck or fix to apply`
   when violations are present in checked nodes.

The TUI does not display per-table contract override granularity; the
contract is applied silently and the TUI shows the resulting verdict.
Operators inspecting *why* a change was discarded should re-run
non-interactively — the stderr output (or `--contract-format=json`) is
the audit trail.

## Differences from dlt

This design tracks dlt's schema-contract vocabulary (`evolve` / `freeze`
/ `discard_value`, granularities `tables` / `columns` / `data_type`)
closely so a dlt user reads the flags without surprises. The
diff-time-vs-load-time gap forces a few deliberate departures.
Implementers should **not** "fix" these without flagging back first.

- **No `discard_row` mode.** dlt drops offending rows when a column
  violates the contract; uvg has no rows at diff time. The third mode
  is `discard_value` only — applied to a planned DDL statement, it
  means "drop this statement from the output."
- **`data_type` includes default-value changes.** dlt's `data_type`
  granularity gates type, nullable, precision, scale, and timezone
  changes. uvg additionally folds `DefaultChange` under `data_type`,
  since column defaults are metadata in the same spirit. This is a
  deliberate uvg extension. If a user later needs to allow type swaps
  while freezing default churn (or vice versa), promote defaults to
  their own granularity *then* — don't pre-split.
- **CREATE TABLE columns are not re-evaluated as `NewColumn`.**
  `compute_changes()` emits a single `NewTable` Change carrying the
  full CREATE TABLE body; the columns inside that statement do not
  surface as separate `NewColumn` Changes, so `columns=freeze` does not
  block a fresh table from being created. This matches dlt's explicit
  "new tables allow columns" carve-out (dlt flips column mode to
  `evolve` for the first run of a new table) — uvg gets it for free
  from the diff engine's structure. Any future refactor of
  `compute_changes()` must preserve this property; add a test if you
  touch that path.
- **No Pydantic / typed-model integration.** dlt maps contracts onto
  Pydantic `extra` settings. uvg has no equivalent type layer; out of
  scope.
- **No stateful evolution tracking.** dlt stores per-resource schema
  state in the destination. uvg compares two live snapshots, so "new
  column" means "in source, not in target" — strictly stricter than
  dlt and more appropriate for CI gating of a planned migration.
- **CLI-first, not decorator-first.** dlt's primary surface is the
  `@dlt.resource(schema_contract=...)` decorator. uvg is a CLI; the
  equivalent surface is `--contract` / `--contract-file` / `./uvg.yaml`.

## File-by-file change list

| File | Change |
|---|---|
| `src/contract.rs` | **New.** Types (`ChangeKind`, `Change`, `Contract`, `ContractMode`, `Granularity`, `TableContract`, `ContractViolation`, `ApplyResult`); `Contract::apply()`; `parse_contract_arg()`; `load_contract_file()`; `discover_contract()` (looks for `./uvg.yaml` in cwd, returns `Option<Contract>`); `merge_contracts()`. ~220 LOC. |
| `src/main.rs` | Wire contract construction + violation handling between codegen and output. ~30 LOC. |
| `src/cli.rs` | Six new `#[arg]` fields on `Cli`: `--contract`, `--contract-file`, `--contract-format`, `--outfile`, `--out-dir`, `--name`. ~20 LOC. |
| `src/output.rs` | **New.** Output dispatch: stdout / single-file / per-table-dir. Walks `Vec<Change>`, buckets by `(schema, table)` + `_schema`, renders header + body per file, writes manifest JSON. ~150 LOC. |
| `src/main.rs` (output dispatch) | Pick output strategy from CLI flags; pass `Vec<Change>` to `output::write_*`. Skip writes on violations. ~15 LOC. |
| `src/codegen/ddl_diff.rs` | Extract `compute_changes()` and `render_changes()` from `diff_schemas()`. Mechanical refactor: every `stmts.push(s)` site becomes a `Change` push. Where multiple SQL statements were pushed in one site for a single column (PG type+null+default), split into separate `Change`s by kind. ~50 LOC delta. |
| `src/codegen/mod.rs` | Re-export `Change`, `compute_changes`, `render_changes`. |
| `src/tui/mod.rs` | Color verdicts in `render_ddl_view()`; new status counts; disable apply on violations. ~60 LOC delta. |
| `src/error.rs` | Add `UvgError::ContractParse(String)` and `UvgError::ContractViolation` variants. |
| `Cargo.toml` | If not already present, add `serde_yaml` (or equivalent) for contract file parsing. Check what YAML dependency is already in the tree — uvg may already have one. |
| `docs/design.md` | Add a paragraph under "Pipeline" noting the contract pass. Keep it short — one paragraph. |
| `README.md` | New "Contracts" subsection under Usage. Document the two flags and show one example each (inline + file). |
| `tests/integration.rs` | At least two new integration tests (see Test Plan). |

## Test plan

Add unit tests inline in `src/contract.rs`:

1. `parse_contract_arg` round-trips known good inputs.
2. `parse_contract_arg` rejects unknown modes/granularities with a clear error.
3. `merge_contracts` overlay semantics (overlay wins on set keys; base keys preserved when overlay key is `None`).
4. `Contract::apply` correctly partitions a hand-built `Vec<Change>`:
   - Default contract: everything goes to `allowed`.
   - All-freeze: everything goes to `violations`.
   - All-`discard_value`: everything goes to `discarded`.
   - Mixed per-granularity: tables=freeze, columns=evolve, data_type=discard_value.
   - Per-table override beats default.
5. Default-schema normalization: a contract keyed on `"public.users"`
   matches a change tagged with schema `""` after normalization.
5a. `discover_contract()` returns `None` when `./uvg.yaml` is absent,
   `Some(contract)` when present in either bare or nested form, and
   errors when both top-level forms coexist in the same file. Use a
   `tempfile` directory + `std::env::set_current_dir` to avoid polluting
   the workspace cwd.

Add diff-engine tests in `src/codegen/ddl_diff.rs`:

6. `compute_changes` returns the expected `ChangeKind`s for each existing
   test case (new table → `NewTable`, modified column → split into
   `TypeChange` + optionally `NullabilityChange`, etc.). Convert one or two
   existing string-grep tests to `ChangeKind`-asserting tests; keep the
   rest of the string-grep tests pointing at `diff_schemas()` to prove
   backward compat.
7. The "PG type + nullability + default" case produces three separate
   `Change` entries, not one merged statement. This is the load-bearing
   guarantee for the contract pipeline.

Add integration tests in `tests/integration.rs`:

8. End-to-end: PG source with a new column, target without. Run with
   `--contract columns=freeze`. Assert exit code 2 and stderr contains
   `contract violation`.
9. End-to-end: same setup, `--contract columns=discard_value`. Assert exit code
   0 and stdout does NOT contain `ADD COLUMN`.
10. End-to-end per-table layout: run with `--out-dir <tmpdir>`. Assert:
    - One subdir per modified table, named after the table.
    - `_schema/` dir present iff non-table-scoped DDL was emitted.
    - `_runs/<run_id>.json` manifest exists and round-trips via serde.
    - Every `.sql` file starts with the provenance header.
    - Re-running against unchanged source/target writes a manifest with
      zero `.sql` files and `stats.changes == 0`.
11. Violation + `--out-dir`: assert no `.sql` and no manifest files are
    written when exit code is 2.

CRM matrix (`testdata/crm/`) does not need new fixtures. Spot-check that
running the existing matrix with no `--contract` flag produces byte-identical
output to before the refactor — this is the regression guard for the
`compute_changes` / `render_changes` split.

## Implementation reality check

Most of the engine already exists in uvg. The work below is structuring
output on top of an existing pipeline, not building new introspection
or new DDL generation.

| What you need | Status today |
|---|---|
| Source/target introspection | ✅ `src/introspect/*` |
| Cross-dialect schema diff | ✅ `src/codegen/ddl_diff.rs::diff_schemas()` |
| DDL rendering per dialect | ✅ `src/codegen/ddl.rs` + `src/ddl_typemap/` |
| Topological sort for FKs | ✅ `src/codegen/mod.rs::topo_sort_tables()` |
| TUI DDL view + apply | ✅ `src/tui/mod.rs` |
| **Change kind tagging** | ❌ refactor `diff_schemas()` into `compute_changes()` |
| **Contract policy** | ❌ new `src/contract.rs` |
| **Per-table file splitter** | ❌ new `src/output.rs` |
| **Manifest writer** | ❌ part of `src/output.rs` |
| **TUI per-table selection** | ❌ tree pane + checkbox state in `App` |

Net new code: ~600–700 LOC across `contract.rs`, `output.rs`, the
`ddl_diff` refactor, and the TUI tree pane. Plus tests.

## Implementation order

Do these in order. Do not skip ahead — each step has tests that gate the
next one.

1. **Add `src/contract.rs`** with the types and unit tests 1–5 above.
   Don't wire it into anything yet. `cargo test contract::` passes.
2. **Refactor `diff_schemas()`** into `compute_changes()` + `render_changes()`.
   All existing tests in `src/codegen/ddl_diff.rs` continue to pass
   unchanged. Add diff-engine tests 6–7. Run the CRM matrix
   (`./testdata/crm/run_matrix.sh`) and confirm zero output changes.
3. **Wire `Contract` into `main.rs`** with the new CLI flags
   (`--contract`, `--contract-file`, `--contract-format`). Add
   integration tests 8–9. The no-contract path must remain
   byte-identical to step 2's output.
4. **Add `src/output.rs`** with the per-table file splitter and
   manifest writer. Wire `--outfile` / `--out-dir` / `--name`. Add
   integration tests 10–11. Stdout path remains byte-identical when no
   output flag is set.
5. **TUI integration.** Tree pane grouping changes by table, verdict
   coloring, per-table checkboxes, status counts, apply gated on
   selected nodes. Manual smoke test only — TUI is not covered by
   automated tests in this repo today.
6. **Docs.** Update `docs/design.md` and `README.md`. Add a contract
   example file at `testdata/contracts/example.yaml` if useful for the README.

Steps 1–3 are the contracts PR. Steps 4–5 are the per-table output PR
and can ship separately — they share the `Vec<Change>` refactor but are
otherwise independent. Splitting keeps each PR small enough to review
in one sitting; bundle only if the team prefers fewer PRs.

Each step should be one commit. Run `cargo test` and `cargo clippy
--all-targets` before each commit. Match uvg's existing commit style —
short imperative summary, optional body.

## Design calls already made

These were settled in the conversation that produced this plan. Do not
relitigate unless you have a strong reason and you are surfacing it
explicitly to the user.

- **No `discard_rows` mode.** uvg is metadata-only; rows don't exist at
  diff time. Dropping this mode from dlt's set is intentional, not an
  oversight.
- **Contracts are evaluated at diff time, not load time.** A freeze
  violation rejects the *plan*, not the offending rows. This is stricter
  than dlt's load-time contracts; it's the right shape for CI/governance.
- **`evolve` is the default for all three granularities.** No contract
  flag → no behavior change for existing users.
- **One `Change` per `ChangeKind`.** A column with both a type change and
  a nullability change produces two `Change` entries. The contract can
  then rule on each independently. The renderer joins them back into the
  same output order as today.
- **Exit code `2` for contract violations.** Distinct from the existing
  `1` so CI can distinguish "uvg failed" from "uvg ran fine, plan
  rejected by policy."
- **Per-table overrides are keyed by `(schema, name)`** with schema-name
  normalization applied symmetrically to both the contract file and the
  `Change` records. This matches the existing cross-dialect diff
  semantics.
- **Contract config lives next to the schema, not in a user dir.**
  Auto-discovery is shallow (cwd-only, no walking up). No
  `~/.config/uvg/` lookup. Contracts are repo-scoped governance; varying
  them by who runs the tool would defeat the purpose. See
  "Configuration discovery" for the full precedence.

## Things to ask the user before implementing

Nothing — the design above is complete. If the implementer encounters one
of these, stop and ask:

- A test that would require schema introspection changes.
- A case where the `Vec<Change>` refactor would cause an existing CRM
  matrix test to drift even by one byte.
- A need to add data-driven behavior (record streams, type inference) to
  enforce a contract — this is a non-goal and a sign the scope has
  shifted.
