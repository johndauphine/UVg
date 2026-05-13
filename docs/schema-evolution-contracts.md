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
- **Discard** — silently omit the DDL from output.

Granularity matches dlt's three contract levels:

| Granularity | Changes it gates |
|---|---|
| `tables` | New tables (`CREATE TABLE`), dropped tables (`DROP TABLE`) |
| `columns` | New columns (`ADD COLUMN`), dropped columns (`DROP COLUMN`) |
| `data_types` | Type changes (`ALTER COLUMN ... TYPE`), nullability changes, default changes |

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
            | ChangeKind::DefaultChange => Granularity::DataTypes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Granularity {
    Tables,
    Columns,
    DataTypes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractMode {
    Evolve,  // default
    Freeze,
    Discard,
}

#[derive(Debug, Clone)]
pub struct Contract {
    pub tables: ContractMode,
    pub columns: ContractMode,
    pub data_types: ContractMode,
    /// Per-table overrides keyed by (schema, name). Schema "" matches the
    /// default schema for the dialect (matches diff_schemas's normalization).
    pub per_table: HashMap<(String, String), TableContract>,
}

#[derive(Debug, Clone)]
pub struct TableContract {
    pub tables: Option<ContractMode>,
    pub columns: Option<ContractMode>,
    pub data_types: Option<ContractMode>,
}

impl Default for Contract {
    fn default() -> Self {
        Self {
            tables: ContractMode::Evolve,
            columns: ContractMode::Evolve,
            data_types: ContractMode::Evolve,
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

/// Contract rules: comma-delimited key=mode pairs, e.g.
/// "tables=freeze,columns=evolve,data_types=freeze".
/// Modes: evolve (default), freeze, discard.
#[arg(long)]
pub contract: Option<String>,

/// Path to a YAML contract file (overrides --contract for any keys it sets).
#[arg(long)]
pub contract_file: Option<String>,
```

Add a parser in `src/contract.rs`:

```rust
pub fn parse_contract_arg(s: &str) -> Result<Contract, UvgError>;
pub fn load_contract_file(path: &Path) -> Result<Contract, UvgError>;
pub fn merge_contracts(base: Contract, overlay: Contract) -> Contract;
```

The YAML schema:

```yaml
# contract.yaml
defaults:
  tables: freeze
  columns: evolve
  data_types: freeze
tables:
  "public.users":
    columns: discard
  "public.events":
    tables: freeze
    columns: evolve
    data_types: evolve
```

Key format is `"schema.name"`. For default-schema tables, use either
`"public.users"` (PG), `"dbo.users"` (MSSQL), `"main.users"` (SQLite), or
the actual MySQL database name; uvg's existing schema normalization in
`normalize_schema()` (`src/codegen/ddl_diff.rs:155`) collapses all of these
to `""` internally, so the contract loader should normalize the same way
when populating `Contract::per_table`.

### Main flow

In `src/main.rs`, after the DDL is produced but before output:

1. Build the `Contract` from CLI args (`Cli::contract` + `Cli::contract_file`).
2. Call `compute_changes(...)` instead of `diff_schemas(...)` when a contract
   is set. (When no contract is set, the existing `diff_schemas()` path is
   preserved to keep the no-contract code path unchanged.)
3. Call `contract.apply(changes)`.
4. If `violations` is non-empty:
   - Print each violation to stderr in this format:

     ```
     uvg: contract violation [freeze on data_types]
       table: public.users
       column: email
       sql: ALTER TABLE "users" ALTER COLUMN "email" TYPE TEXT;
     ```

   - Exit with status `2`.
5. Otherwise, render `allowed` via `render_changes()` and emit as today.
6. `discarded` changes are silently dropped from output. Add a single
   stderr summary line: `uvg: 3 change(s) discarded by contract`.

Exit codes:
- `0` — success (no changes, or all allowed/discarded)
- `1` — existing uvg errors (connection, introspection, etc.)
- `2` — contract violation (new)

### TUI surface

In `src/tui/mod.rs`, when a contract is active:

1. Pass the `Contract` into `App` alongside `ddl_output`. Replace
   `ddl_output: String` with a richer representation that retains the
   per-line verdict (or keep `ddl_output: String` for display and add a
   parallel `Vec<(LineRange, Verdict)>` for coloring).
2. Render each statement in color:
   - **Green** — evolve (will be applied)
   - **Red** — freeze violation (blocks apply)
   - **Gray (dimmed)** — discard (omitted)
3. In the status bar (`render_status()`, `src/tui/mod.rs:446`), show
   counts: `X allowed · Y discarded · Z blocked`.
4. The **Apply** action (`apply_ddl()`, `src/tui/mod.rs:638`) is disabled
   when `violations` is non-empty. The hint text in `render_ddl_view()` at
   `src/tui/mod.rs:484` becomes `contract violations — cannot apply` in
   that case.
5. Apply executes only the `allowed` statements (concatenated with the same
   `;`-split semantics `db::execute_ddl()` already uses).

The TUI does not display per-table override granularity; the contract is
applied silently and the TUI shows the resulting verdict. Operators
inspecting *why* a particular change was discarded should re-run with the
non-interactive flag — the stderr output is the audit trail.

## File-by-file change list

| File | Change |
|---|---|
| `src/contract.rs` | **New.** Types (`ChangeKind`, `Change`, `Contract`, `ContractMode`, `Granularity`, `TableContract`, `ContractViolation`, `ApplyResult`); `Contract::apply()`; `parse_contract_arg()`; `load_contract_file()`; `merge_contracts()`. ~200 LOC. |
| `src/main.rs` | Wire contract construction + violation handling between codegen and output. ~30 LOC. |
| `src/cli.rs` | Two new `#[arg]` fields on `Cli`. ~10 LOC. |
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
   - All-discard: everything goes to `discarded`.
   - Mixed per-granularity: tables=freeze, columns=evolve, data_types=discard.
   - Per-table override beats default.
5. Default-schema normalization: a contract keyed on `"public.users"`
   matches a change tagged with schema `""` after normalization.

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
9. End-to-end: same setup, `--contract columns=discard`. Assert exit code
   0 and stdout does NOT contain `ADD COLUMN`.

CRM matrix (`testdata/crm/`) does not need new fixtures. Spot-check that
running the existing matrix with no `--contract` flag produces byte-identical
output to before the refactor — this is the regression guard for the
`compute_changes` / `render_changes` split.

## Implementation order

Do these in order. Do not skip ahead — each step has tests that gate the
next one.

1. **Add `src/contract.rs`** with the types and unit tests 1–5 above.
   Don't wire it into anything yet. `cargo test contract::` passes.
2. **Refactor `diff_schemas()`** into `compute_changes()` + `render_changes()`.
   All existing tests in `src/codegen/ddl_diff.rs` continue to pass
   unchanged. Add diff-engine tests 6–7. Run the CRM matrix
   (`./testdata/crm/run_matrix.sh`) and confirm zero output changes.
3. **Wire `Contract` into `main.rs`** with the two new CLI flags. Add
   integration tests 8–9. The no-contract path must remain byte-identical
   to step 2's output.
4. **TUI integration.** Color verdicts, disable apply on violations,
   update status bar. Manual smoke test only — TUI is not covered by
   automated tests in this repo today.
5. **Docs.** Update `docs/design.md` and `README.md`. Add a contract
   example file at `testdata/contracts/example.yaml` if useful for the README.

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

## Things to ask the user before implementing

Nothing — the design above is complete. If the implementer encounters one
of these, stop and ask:

- A test that would require schema introspection changes.
- A case where the `Vec<Change>` refactor would cause an existing CRM
  matrix test to drift even by one byte.
- A need to add data-driven behavior (record streams, type inference) to
  enforce a contract — this is a non-goal and a sign the scope has
  shifted.
