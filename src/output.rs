//! Per-table output layout for `--out-dir` migrations.
//!
//! Splits a stream of `Change` records into one file per table, with a
//! provenance header on every `.sql` file and a single JSON manifest per
//! run. Non-table-scoped DDL (enum `CREATE TYPE`, `CREATE SCHEMA`, etc.)
//! lands in `_schema/`. Manifests live in `_runs/`.
//!
//! **Empty diffs write nothing.** No `.sql`, no `_schema/`, no `_runs/`.
//! The mental model: "no schema changes → no new files in git." See
//! `docs/migration-output-layout.md`.

// Step 3 wires `--out-dir`/`--name` into `main.rs`, at which point every
// item below has a non-test caller. Remove this allow then.
#![allow(dead_code)]

use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::dialect::Dialect;

/// A single SQL statement emitted by the diff engine, tagged with the
/// table it pertains to. The tag lets the per-table splitter route the
/// statement into the right subdirectory; non-table-scoped DDL
/// (enums, `CREATE SCHEMA`, etc.) uses `table_name: None`.
///
/// `table_schema` is normalized: default schemas (`public`, `dbo`, `main`,
/// the MySQL default database, and `""`) are stored as `""`, so the
/// splitter doesn't need dialect awareness.
#[derive(Debug, Clone)]
pub struct Change {
    pub table_schema: String,
    pub table_name: Option<String>,
    pub sql: String,
}

/// Context describing a single uvg invocation. Owns the timestamps and
/// version metadata the splitter stamps onto every file it writes.
#[derive(Debug, Clone)]
pub struct OutputContext {
    pub out_dir: PathBuf,
    pub tag: String,
    pub run_id: String,
    pub generated_at: String,
    pub uvg_version: String,
    pub source_dialect: Dialect,
    pub target_dialect: Dialect,
}

impl OutputContext {
    /// Build a context using the current UTC time. The default `tag` is
    /// `<source>_to_<target>`; pass `Some("...")` to override (the
    /// `--name` flag does this).
    pub fn now(
        out_dir: PathBuf,
        tag: Option<String>,
        source_dialect: Dialect,
        target_dialect: Dialect,
    ) -> Self {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self::at(out_dir, tag, source_dialect, target_dialect, secs)
    }

    /// Build a context at a fixed epoch second. Used by tests for
    /// deterministic filenames; production callers use `now()`.
    pub fn at(
        out_dir: PathBuf,
        tag: Option<String>,
        source_dialect: Dialect,
        target_dialect: Dialect,
        epoch_secs: u64,
    ) -> Self {
        // The tag flows into filenames, so any `/` or `..` would
        // break the write (or worse, escape out_dir). Sanitize before
        // it reaches run_id or the on-disk filenames so the value
        // shown to the user in the manifest matches what's on disk.
        let raw = tag.unwrap_or_else(|| format!("{source_dialect}_to_{target_dialect}"));
        let tag = sanitize_path_component(&raw);
        let ts_compact = format_utc_compact(epoch_secs);
        let run_id = format!("{ts_compact}__{tag}");
        let generated_at = format_utc_iso8601(epoch_secs);
        OutputContext {
            out_dir,
            tag,
            run_id,
            generated_at,
            uvg_version: env!("CARGO_PKG_VERSION").to_string(),
            source_dialect,
            target_dialect,
        }
    }

    fn filename(&self) -> String {
        format!("{}__{}.sql", self.compact_ts(), self.tag)
    }

    fn manifest_filename(&self) -> String {
        format!("{}__{}.json", self.compact_ts(), self.tag)
    }

    fn compact_ts(&self) -> &str {
        // run_id has the form `<compact>__<tag>`; the tag may itself
        // contain `__`, so split at the first occurrence only.
        match self.run_id.find("__") {
            Some(idx) => &self.run_id[..idx],
            None => &self.run_id,
        }
    }
}

/// Manifest describing every file produced by one uvg run. Written to
/// `_runs/<run_id>.json` whenever the run produces at least one change.
/// Per `docs/migration-output-layout.md`, no manifest is emitted for
/// no-op runs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub run_id: String,
    pub generated_at: String,
    pub uvg_version: String,
    pub source_dialect: String,
    pub target_dialect: String,
    /// Paths relative to `out_dir`, sorted for deterministic git diffs.
    pub files: Vec<String>,
    pub stats: Stats,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Stats {
    pub changes: usize,
}

/// Write a `Change` stream into the per-table layout under
/// `ctx.out_dir`. Returns `Ok(None)` for an empty diff (nothing
/// written), `Ok(Some(manifest))` otherwise.
///
/// File layout (see `docs/migration-output-layout.md`):
///
/// ```text
/// <out_dir>/
///   <table>/<run_id>.sql            # one file per modified table
///   _schema/<run_id>.sql            # non-table-scoped DDL (enums, etc.)
///   _runs/<run_id>.json             # manifest of this run
/// ```
pub fn write_split_changes(
    changes: &[Change],
    ctx: &OutputContext,
) -> io::Result<Option<Manifest>> {
    if changes.is_empty() {
        return Ok(None);
    }

    fs::create_dir_all(&ctx.out_dir)?;

    // Group changes by destination subdir (preserving insertion order so
    // FK / topo order from compute_changes survives into the file).
    let mut groups: Vec<(String, Vec<&Change>)> = Vec::new();
    for change in changes {
        let bucket = subdir_for(change);
        match groups.iter_mut().find(|(name, _)| name == &bucket) {
            Some((_, v)) => v.push(change),
            None => groups.push((bucket, vec![change])),
        }
    }

    let filename = ctx.filename();
    let runs_dir = ctx.out_dir.join("_runs");
    let manifest_path = runs_dir.join(ctx.manifest_filename());

    // Pre-probe every output path before touching the filesystem so a
    // same-second re-run cannot silently overwrite a prior migration's
    // artifacts AND a partial write can't be left behind when a planned
    // destination is an unexpected file type. Two failure modes are
    // checked in this order:
    //   (a) per-table subdir already exists but is a regular file
    //       (the next create_dir_all would later blow up mid-loop,
    //       leaving earlier table files orphaned);
    //   (b) the target .sql / .json file already exists (run_id
    //       collision — pick a different --name).
    let mut planned: Vec<PathBuf> = Vec::with_capacity(groups.len());
    for (subdir, _) in &groups {
        let dir = ctx.out_dir.join(subdir);
        if dir.exists() && !dir.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "uvg: cannot create migration subdirectory {}: a regular file with that name already exists",
                    dir.display(),
                ),
            ));
        }
        planned.push(dir.join(&filename));
    }
    if runs_dir.exists() && !runs_dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "uvg: cannot create manifest directory {}: a regular file with that name already exists",
                runs_dir.display(),
            ),
        ));
    }
    for path in &planned {
        if path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "uvg: refusing to overwrite existing migration artifact {} \
                     (run_id `{}` already used; pick a different --name)",
                    path.display(),
                    ctx.run_id,
                ),
            ));
        }
    }
    if manifest_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "uvg: refusing to overwrite existing manifest {} \
                 (run_id `{}` already used; pick a different --name)",
                manifest_path.display(),
                ctx.run_id,
            ),
        ));
    }

    let mut written: Vec<String> = Vec::new();

    for (subdir, group) in &groups {
        let dir = ctx.out_dir.join(subdir);
        fs::create_dir_all(&dir)?;
        let path = dir.join(&filename);

        let header_table = match (
            group.first().and_then(|c| c.table_name.as_deref()),
            subdir.as_str(),
        ) {
            (Some(name), _) => {
                let schema = &group.first().unwrap().table_schema;
                if schema.is_empty() {
                    name.to_string()
                } else {
                    format!("{schema}.{name}")
                }
            }
            (None, _) => "(schema-scoped DDL)".to_string(),
        };

        let mut body = format_header(ctx, &header_table);
        for (i, change) in group.iter().enumerate() {
            if i > 0 {
                body.push_str("\n\n");
            }
            body.push_str(&change.sql);
            if !change.sql.ends_with('\n') {
                body.push('\n');
            }
        }
        write_new(&path, body.as_bytes(), &ctx.run_id)?;
        written.push(format!("{subdir}/{filename}"));
    }

    // Do NOT sort `written`. `apply_order()` consumes `manifest.files`
    // and relies on the topological order that `compute_changes()` already
    // imposes (so a referencing table's CREATE follows the referenced
    // one's). Lexicographic sorting here breaks FK-dependent migrations
    // whose alphabetical order differs from their topological order.
    // For deterministic git diffs we rely on `compute_changes()` itself
    // being deterministic for a given (source, target) pair.

    fs::create_dir_all(&runs_dir)?;
    let manifest = Manifest {
        run_id: ctx.run_id.clone(),
        generated_at: ctx.generated_at.clone(),
        uvg_version: ctx.uvg_version.clone(),
        source_dialect: ctx.source_dialect.to_string(),
        target_dialect: ctx.target_dialect.to_string(),
        files: written,
        stats: Stats {
            changes: changes.len(),
        },
    };
    let manifest_json = serde_json::to_string_pretty(&manifest).map_err(io::Error::other)?;
    write_new(
        &manifest_path,
        (manifest_json + "\n").as_bytes(),
        &ctx.run_id,
    )?;

    Ok(Some(manifest))
}

/// Atomic "create or fail" file write. Uses `OpenOptions::create_new`
/// so the OS guarantees we never truncate an existing file — important
/// because two concurrent uvg processes can both pass the preflight in
/// `write_split_changes` and race to write the same path. On
/// `AlreadyExists`, the error is rewritten to the same friendly form
/// the preflight produces so the user always sees the same recovery
/// hint regardless of which side caught the collision.
fn write_new(path: &Path, body: &[u8], run_id: &str) -> io::Result<()> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut f) => f.write_all(body),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "uvg: refusing to overwrite existing migration artifact {} \
                 (run_id `{run_id}` already used; pick a different --name)",
                path.display(),
            ),
        )),
        Err(e) => Err(e),
    }
}

/// Subdirectories under `out_dir` that uvg reserves for its own
/// metadata. A real table whose normalized name collides with one of
/// these would be misrouted into the metadata bucket — `_schema` is
/// special-cased by [`apply_order`] and the TUI; `_runs` holds
/// manifests. Tables that hit either name are escaped via
/// [`escape_reserved`].
const RESERVED_SUBDIRS: &[&str] = &["_schema", "_runs"];

/// Determine the subdirectory under `out_dir` for a given change.
/// `_schema` for non-table-scoped DDL, `<table>` for default-schema
/// tables, `<schema>__<table>` for non-default schemas.
///
/// Identifiers are sanitized (`sanitize_path_component`) before being
/// folded into a path so a quoted table name like `../escape` cannot
/// write outside `out_dir`. A real table whose name collides with a
/// reserved metadata directory (`_schema`, `_runs`) is rewritten with
/// a `_table` suffix so it doesn't get treated as schema-scoped DDL or
/// manifest storage. The TUI uses this same function for its tree
/// labels, so what a user sees on screen always matches what lands on
/// disk.
pub(crate) fn subdir_for(change: &Change) -> String {
    match &change.table_name {
        None => "_schema".to_string(),
        Some(name) => {
            let safe_name = sanitize_path_component(name);
            let bucket = if change.table_schema.is_empty() {
                safe_name
            } else {
                let safe_schema = sanitize_path_component(&change.table_schema);
                format!("{safe_schema}__{safe_name}")
            };
            escape_reserved(&bucket)
        }
    }
}

/// Escape table buckets that would collide with uvg's reserved
/// metadata directories. Appending `_table` keeps the original
/// identifier readable while making it unambiguous on disk.
fn escape_reserved(bucket: &str) -> String {
    if RESERVED_SUBDIRS.contains(&bucket) {
        format!("{bucket}_table")
    } else {
        bucket.to_string()
    }
}

/// Make a string safe to use as a single path component:
/// - replace characters that have filesystem semantics (`/`, `\`, `:`,
///   `\0`) with `_` so the result is always a single directory level;
/// - rewrite empty / `.` / `..` to `_` so the path can't ascend out of
///   `out_dir` when joined.
///
/// Encoded names may collide (`a/b` and `a_b` both become `a_b`), but
/// that's acceptable: collisions concatenate SQL into one file rather
/// than overwriting an unrelated table's directory. The threat model
/// is filesystem escape, not perfect round-tripping of identifiers.
fn sanitize_path_component(s: &str) -> String {
    let mapped: String = s
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '\0' => '_',
            other => other,
        })
        .collect();
    if mapped.is_empty() || mapped == "." || mapped == ".." {
        "_".to_string()
    } else {
        mapped
    }
}

fn format_header(ctx: &OutputContext, header_table: &str) -> String {
    // Every interpolation must be flattened: SQL-quoted identifiers can
    // legally contain newlines, and a raw newline here would terminate
    // the `-- ...` comment and inject the rest of the line as
    // executable SQL into the migration file.
    format!(
        "-- Generated by uvg {ver} on {ts} (UTC)\n\
         -- Run:    {run}\n\
         -- Table:  {tbl}\n\
         -- Source: {src}  ->  Target: {tgt}\n\n",
        ver = flatten_for_comment(&ctx.uvg_version),
        ts = flatten_for_comment(&ctx.generated_at),
        run = flatten_for_comment(&ctx.run_id),
        tbl = flatten_for_comment(header_table),
        src = flatten_for_comment(&ctx.source_dialect.to_string()),
        tgt = flatten_for_comment(&ctx.target_dialect.to_string()),
    )
}

/// Render a value safe for inclusion in a `-- ...` SQL comment.
/// Escapes newlines, carriage returns, tabs, and other ASCII control
/// characters so the comment can't be broken out of via embedded
/// control bytes in a quoted identifier or `--name`. The user still
/// sees the original characters visibly (as `\n`, `\r`, `\xNN`) so
/// the header remains informative.
fn flatten_for_comment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                use std::fmt::Write;
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

// -------- time formatting (no chrono dep) --------

/// Format UTC epoch seconds as `YYYYMMDDTHHMMSSZ` (compact, sortable).
pub(crate) fn format_utc_compact(epoch_secs: u64) -> String {
    let (y, mo, d, h, mi, s) = epoch_to_ymdhms_utc(epoch_secs);
    format!("{y:04}{mo:02}{d:02}T{h:02}{mi:02}{s:02}Z")
}

/// Format UTC epoch seconds as ISO-8601 `YYYY-MM-DDTHH:MM:SSZ`.
pub(crate) fn format_utc_iso8601(epoch_secs: u64) -> String {
    let (y, mo, d, h, mi, s) = epoch_to_ymdhms_utc(epoch_secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

fn epoch_to_ymdhms_utc(epoch_secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let seconds_per_day: u64 = 86_400;
    let days = epoch_secs / seconds_per_day;
    let rem = epoch_secs % seconds_per_day;
    let h = (rem / 3600) as u32;
    let mi = ((rem % 3600) / 60) as u32;
    let s = (rem % 60) as u32;
    let (y, mo, d) = days_to_ymd(days as i64);
    (y, mo, d, h, mi, s)
}

/// Convert days since 1970-01-01 (UTC) to (year, month, day). Uses
/// Howard Hinnant's civil_from_days algorithm — short, branchless, and
/// correct for the full Gregorian range we care about.
fn days_to_ymd(days_since_epoch: i64) -> (u32, u32, u32) {
    // Shift so day 0 = 0000-03-01 (start of the era).
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year as u32, m as u32, d as u32)
}

/// Apply order for files produced by `write_split_changes`. `_schema/`
/// files first (enums and schemas must exist before tables that
/// reference them), then table files in the order the splitter emitted
/// them (which preserves `compute_changes`'s topo sort). Caller is
/// expected to read each path and execute its contents.
pub fn apply_order(manifest: &Manifest, out_dir: &Path) -> Vec<PathBuf> {
    let mut schema_files: Vec<&str> = Vec::new();
    let mut table_files: Vec<&str> = Vec::new();
    for f in &manifest.files {
        if f.starts_with("_schema/") {
            schema_files.push(f);
        } else {
            table_files.push(f);
        }
    }
    schema_files
        .into_iter()
        .chain(table_files)
        .map(|f| out_dir.join(f))
        .collect()
}

#[cfg(test)]
#[path = "output_tests.rs"]
mod tests;
