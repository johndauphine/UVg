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
use std::io;
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
        let tag = tag.unwrap_or_else(|| format!("{source_dialect}_to_{target_dialect}"));
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

    let mut written: Vec<String> = Vec::new();
    let filename = ctx.filename();

    for (subdir, group) in &groups {
        let dir = ctx.out_dir.join(subdir);
        fs::create_dir_all(&dir)?;
        let path = dir.join(&filename);

        let header_table = match (group.first().and_then(|c| c.table_name.as_deref()), subdir.as_str()) {
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
        fs::write(&path, body)?;
        written.push(format!("{subdir}/{filename}"));
    }

    written.sort();

    let runs_dir = ctx.out_dir.join("_runs");
    fs::create_dir_all(&runs_dir)?;
    let manifest = Manifest {
        run_id: ctx.run_id.clone(),
        generated_at: ctx.generated_at.clone(),
        uvg_version: ctx.uvg_version.clone(),
        source_dialect: ctx.source_dialect.to_string(),
        target_dialect: ctx.target_dialect.to_string(),
        files: written,
        stats: Stats { changes: changes.len() },
    };
    let manifest_json = serde_json::to_string_pretty(&manifest)
        .map_err(io::Error::other)?;
    fs::write(runs_dir.join(ctx.manifest_filename()), manifest_json + "\n")?;

    Ok(Some(manifest))
}

/// Determine the subdirectory under `out_dir` for a given change.
/// `_schema` for non-table-scoped DDL, `<table>` for default-schema
/// tables, `<schema>__<table>` for non-default schemas.
///
/// Exposed at the crate level so the TUI tree pane can show the same
/// node names a user would see on disk after `--out-dir`.
pub(crate) fn subdir_for(change: &Change) -> String {
    match &change.table_name {
        None => "_schema".to_string(),
        Some(name) => {
            if change.table_schema.is_empty() {
                name.clone()
            } else {
                format!("{}__{}", change.table_schema, name)
            }
        }
    }
}

fn format_header(ctx: &OutputContext, header_table: &str) -> String {
    format!(
        "-- Generated by uvg {ver} on {ts} (UTC)\n\
         -- Run:    {run}\n\
         -- Table:  {tbl}\n\
         -- Source: {src}  ->  Target: {tgt}\n\n",
        ver = ctx.uvg_version,
        ts = ctx.generated_at,
        run = ctx.run_id,
        tbl = header_table,
        src = ctx.source_dialect,
        tgt = ctx.target_dialect,
    )
}

// -------- time formatting (no chrono dep) --------

/// Format UTC epoch seconds as `YYYYMMDDTHHMMSSZ` (compact, sortable).
fn format_utc_compact(epoch_secs: u64) -> String {
    let (y, mo, d, h, mi, s) = epoch_to_ymdhms_utc(epoch_secs);
    format!("{y:04}{mo:02}{d:02}T{h:02}{mi:02}{s:02}Z")
}

/// Format UTC epoch seconds as ISO-8601 `YYYY-MM-DDTHH:MM:SSZ`.
fn format_utc_iso8601(epoch_secs: u64) -> String {
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
mod tests {
    use super::*;
    use std::time::SystemTime;

    /// Allocate a unique tmpdir under std::env::temp_dir() and return it.
    /// We avoid the `tempfile` crate to keep dev-deps minimal.
    fn tmpdir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let pid = std::process::id();
        let dir = std::env::temp_dir().join(format!("uvg-output-test-{label}-{pid}-{nanos}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn make_ctx(out_dir: PathBuf) -> OutputContext {
        // Fixed epoch = 2026-05-13T19:30:00Z so filenames are deterministic.
        OutputContext::at(
            out_dir,
            Some("add-email".to_string()),
            Dialect::Postgres,
            Dialect::Postgres,
            1_778_700_600,
        )
    }

    #[test]
    fn test_epoch_to_ymdhms_known_values() {
        // 1970-01-01T00:00:00Z
        assert_eq!(epoch_to_ymdhms_utc(0), (1970, 1, 1, 0, 0, 0));
        // 2000-01-01T00:00:00Z (leap-century check)
        assert_eq!(epoch_to_ymdhms_utc(946_684_800), (2000, 1, 1, 0, 0, 0));
        // 2026-05-13T19:30:00Z (our test fixture)
        assert_eq!(epoch_to_ymdhms_utc(1_778_700_600), (2026, 5, 13, 19, 30, 0));
    }

    #[test]
    fn test_format_utc_compact_and_iso() {
        assert_eq!(format_utc_compact(1_778_700_600), "20260513T193000Z");
        assert_eq!(format_utc_iso8601(1_778_700_600), "2026-05-13T19:30:00Z");
    }

    #[test]
    fn test_subdir_for_default_schema() {
        let c = Change {
            table_schema: "".into(),
            table_name: Some("users".into()),
            sql: "".into(),
        };
        assert_eq!(subdir_for(&c), "users");
    }

    #[test]
    fn test_subdir_for_non_default_schema() {
        let c = Change {
            table_schema: "billing".into(),
            table_name: Some("orders".into()),
            sql: "".into(),
        };
        assert_eq!(subdir_for(&c), "billing__orders");
    }

    #[test]
    fn test_subdir_for_schema_scoped_ddl() {
        let c = Change {
            table_schema: "".into(),
            table_name: None,
            sql: "CREATE TYPE ...".into(),
        };
        assert_eq!(subdir_for(&c), "_schema");
    }

    #[test]
    fn test_write_empty_changes_writes_nothing() {
        let dir = tmpdir("empty");
        let ctx = make_ctx(dir.clone());
        let result = write_split_changes(&[], &ctx).unwrap();

        assert!(result.is_none(), "empty diff returns None");

        // The dir we passed in may or may not exist; what matters is
        // that no children were created. (We pre-create the dir in the
        // tmpdir helper, so it exists but must be empty.)
        let children: Vec<_> = fs::read_dir(&dir).unwrap().collect();
        assert!(
            children.is_empty(),
            "empty diff should not write any files, found: {children:?}"
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_write_per_table_layout() {
        let dir = tmpdir("layout");
        let ctx = make_ctx(dir.clone());

        let changes = vec![
            Change {
                table_schema: "".into(),
                table_name: Some("users".into()),
                sql: "CREATE TABLE \"users\" (id integer);".into(),
            },
            Change {
                table_schema: "".into(),
                table_name: Some("users".into()),
                sql: "CREATE INDEX ix_users_email ON \"users\" (email);".into(),
            },
            Change {
                table_schema: "".into(),
                table_name: Some("posts".into()),
                sql: "ALTER TABLE \"posts\" ADD COLUMN \"body\" text;".into(),
            },
            Change {
                table_schema: "".into(),
                table_name: None,
                sql: "CREATE TYPE status AS ENUM ('a', 'b');".into(),
            },
        ];

        let manifest = write_split_changes(&changes, &ctx).unwrap().expect("non-empty diff returns Some");

        // Subdirs created
        assert!(dir.join("users").is_dir(), "users/ should exist");
        assert!(dir.join("posts").is_dir(), "posts/ should exist");
        assert!(dir.join("_schema").is_dir(), "_schema/ should exist");
        assert!(dir.join("_runs").is_dir(), "_runs/ should exist");

        // Files at deterministic paths
        let fname = "20260513T193000Z__add-email.sql";
        assert!(dir.join("users").join(fname).exists());
        assert!(dir.join("posts").join(fname).exists());
        assert!(dir.join("_schema").join(fname).exists());
        assert!(dir.join("_runs").join("20260513T193000Z__add-email.json").exists());

        // Two statements landed in users/ — one file, both statements
        let users_body = fs::read_to_string(dir.join("users").join(fname)).unwrap();
        assert!(users_body.contains("CREATE TABLE"));
        assert!(users_body.contains("CREATE INDEX"));

        // Manifest contents
        assert_eq!(manifest.stats.changes, 4);
        assert_eq!(manifest.files.len(), 3); // users + posts + _schema
        assert!(manifest.files.iter().any(|f| f == &format!("users/{fname}")));
        assert!(manifest.files.iter().any(|f| f == &format!("posts/{fname}")));
        assert!(manifest.files.iter().any(|f| f == &format!("_schema/{fname}")));
        assert_eq!(manifest.run_id, "20260513T193000Z__add-email");
        assert_eq!(manifest.generated_at, "2026-05-13T19:30:00Z");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_provenance_header_present() {
        let dir = tmpdir("header");
        let ctx = make_ctx(dir.clone());
        let changes = vec![Change {
            table_schema: "".into(),
            table_name: Some("users".into()),
            sql: "CREATE TABLE x();".into(),
        }];
        write_split_changes(&changes, &ctx).unwrap();
        let body = fs::read_to_string(dir.join("users").join("20260513T193000Z__add-email.sql")).unwrap();
        assert!(body.starts_with("-- Generated by uvg "), "header missing: {body}");
        assert!(body.contains("-- Run:    20260513T193000Z__add-email"));
        assert!(body.contains("-- Table:  users"));
        assert!(body.contains("-- Source: postgres  ->  Target: postgres"));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_non_default_schema_subdir() {
        let dir = tmpdir("nonschema");
        let ctx = make_ctx(dir.clone());
        let changes = vec![Change {
            table_schema: "billing".into(),
            table_name: Some("orders".into()),
            sql: "CREATE TABLE \"billing\".\"orders\" ();".into(),
        }];
        write_split_changes(&changes, &ctx).unwrap();

        let subdir = dir.join("billing__orders");
        assert!(subdir.is_dir(), "non-default schema should produce billing__orders/");
        let body = fs::read_to_string(subdir.join("20260513T193000Z__add-email.sql")).unwrap();
        assert!(body.contains("-- Table:  billing.orders"));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_manifest_round_trip() {
        let original = Manifest {
            run_id: "20260513T193000Z__add-email".into(),
            generated_at: "2026-05-13T19:30:00Z".into(),
            uvg_version: "1.5.0".into(),
            source_dialect: "postgres".into(),
            target_dialect: "mysql".into(),
            files: vec!["users/20260513T193000Z__add-email.sql".into()],
            stats: Stats { changes: 3 },
        };
        let json = serde_json::to_string(&original).unwrap();
        let parsed: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn test_apply_order_schema_first() {
        let manifest = Manifest {
            run_id: "x".into(),
            generated_at: "x".into(),
            uvg_version: "x".into(),
            source_dialect: "postgres".into(),
            target_dialect: "postgres".into(),
            files: vec![
                "users/20260513T193000Z__add-email.sql".into(),
                "_schema/20260513T193000Z__add-email.sql".into(),
                "posts/20260513T193000Z__add-email.sql".into(),
            ],
            stats: Stats { changes: 3 },
        };
        let out_dir = PathBuf::from("/tmp/uvg-test");
        let order = apply_order(&manifest, &out_dir);
        assert_eq!(order.len(), 3);
        assert!(
            order[0].to_string_lossy().contains("_schema/"),
            "_schema/ must come first, got: {order:?}"
        );
    }

    #[test]
    fn test_default_tag_format() {
        let ctx = OutputContext::at(
            PathBuf::from("/tmp/x"),
            None,
            Dialect::Postgres,
            Dialect::Mysql,
            1_778_700_600,
        );
        assert_eq!(ctx.tag, "postgres_to_mysql");
        assert_eq!(ctx.run_id, "20260513T193000Z__postgres_to_mysql");
    }
}
