/// Integration tests require a live database.
/// Set the appropriate env var to run these tests:
///   DATABASE_URL=postgresql://user:pass@localhost/testdb cargo test --test integration -- --ignored
///   MYSQL_URL=mysql://user:pass@localhost/testdb cargo test --test integration -- --ignored
#[cfg(test)]
mod tests {
    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn test_introspect_live_pg() {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("Failed to connect");

        // Just verify we can connect and query information_schema
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM information_schema.tables")
            .fetch_one(&pool)
            .await
            .expect("Failed to query");

        assert!(
            row.0 > 0,
            "Expected at least one table in information_schema"
        );
        pool.close().await;
    }

    #[tokio::test]
    #[ignore = "requires MYSQL_URL"]
    async fn test_introspect_live_mysql() {
        let url = std::env::var("MYSQL_URL").expect("MYSQL_URL must be set");
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("Failed to connect to MySQL");

        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM information_schema.TABLES")
            .fetch_one(&pool)
            .await
            .expect("Failed to query");

        assert!(
            row.0 > 0,
            "Expected at least one table in information_schema"
        );
        pool.close().await;
    }

    #[tokio::test]
    async fn test_introspect_sqlite_in_memory() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("Failed to connect to SQLite");

        // Create a test table
        sqlx::query(
            "CREATE TABLE test_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value REAL,
                data BLOB
            )",
        )
        .execute(&pool)
        .await
        .expect("Failed to create table");

        // Verify table exists in sqlite_master
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'test_table'",
        )
        .fetch_one(&pool)
        .await
        .expect("Failed to query");

        assert_eq!(row.0, 1);

        // Verify columns
        let cols: Vec<(String,)> =
            sqlx::query_as("SELECT name FROM pragma_table_info('test_table') ORDER BY cid")
                .fetch_all(&pool)
                .await
                .expect("Failed to query columns");

        assert_eq!(cols.len(), 4);
        assert_eq!(cols[0].0, "id");
        assert_eq!(cols[1].0, "name");
        assert_eq!(cols[2].0, "value");
        assert_eq!(cols[3].0, "data");

        pool.close().await;
    }

    // -------- per-table --out-dir end-to-end tests --------

    use std::path::{Path, PathBuf};

    /// Allocate a unique tmpdir for this test invocation.
    fn tmpdir(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let pid = std::process::id();
        let dir =
            std::env::temp_dir().join(format!("uvg-cli-test-{label}-{pid}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn exec_sql(db_path: &Path, sql: &str) {
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("sqlite connect");
        for stmt in sql.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
            sqlx::query(stmt).execute(&pool).await.expect("exec");
        }
        pool.close().await;
    }

    /// Snapshot of a directory: sorted list of `(relpath, byte-content)`
    /// for every regular file. Used to assert that a no-op uvg run leaves
    /// the directory byte-identical.
    fn snapshot_dir(root: &Path) -> Vec<(String, Vec<u8>)> {
        let mut out: Vec<(String, Vec<u8>)> = Vec::new();
        fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_dir() {
                    walk(root, &path, out);
                } else {
                    let rel = path.strip_prefix(root).unwrap().to_string_lossy().to_string();
                    let bytes = std::fs::read(&path).unwrap();
                    out.push((rel, bytes));
                }
            }
        }
        if root.exists() {
            walk(root, root, &mut out);
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn uvg_bin() -> PathBuf {
        // cargo populates this env var with the path to the built `uvg`
        // binary for integration tests; no need to find it manually.
        PathBuf::from(env!("CARGO_BIN_EXE_uvg"))
    }

    fn run_uvg(args: &[&str]) -> std::process::Output {
        std::process::Command::new(uvg_bin())
            .args(args)
            .output()
            .expect("spawn uvg")
    }

    #[tokio::test]
    async fn test_out_dir_first_run_then_noop() {
        let dir = tmpdir("outdir-first-then-noop");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        let migrations = dir.join("migrations");

        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT NOT NULL);
             CREATE TABLE posts(id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT,
                                FOREIGN KEY(user_id) REFERENCES users(id));",
        )
        .await;
        // Touch the target so the file exists. uvg still sees an empty
        // schema and emits CREATE TABLEs for every source table.
        exec_sql(&target, "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;").await;

        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());
        let mig_str = migrations.display().to_string();

        // First run — should produce per-table layout.
        let out = run_uvg(&[
            "--generator", "ddl",
            "--out-dir", &mig_str,
            "--name", "initial",
            &src_url, &tgt_url,
        ]);
        assert!(out.status.success(), "first run failed: {}", String::from_utf8_lossy(&out.stderr));

        assert!(migrations.join("users").is_dir(), "users/ subdir missing");
        assert!(migrations.join("posts").is_dir(), "posts/ subdir missing");
        assert!(migrations.join("_runs").is_dir(), "_runs/ missing");

        // One .sql per table, and the manifest names both.
        let users_sql: Vec<_> = std::fs::read_dir(migrations.join("users"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(users_sql.len(), 1, "expected one users sql, got {users_sql:?}");
        assert!(users_sql[0].ends_with("__initial.sql"));

        let body = std::fs::read_to_string(migrations.join("users").join(&users_sql[0])).unwrap();
        assert!(body.contains("-- Generated by uvg"), "missing provenance header: {body}");
        assert!(body.contains("CREATE TABLE \"users\""), "missing CREATE: {body}");

        // Snapshot the directory before the second (no-op) run.
        // To make the no-op meaningful, apply the migrations to the
        // target first so the schemas match.
        let mut migration_files: Vec<PathBuf> = Vec::new();
        for sub in ["users", "posts"] {
            for entry in std::fs::read_dir(migrations.join(sub)).unwrap() {
                migration_files.push(entry.unwrap().path());
            }
        }
        migration_files.sort();
        for path in &migration_files {
            let sql = std::fs::read_to_string(path).unwrap();
            // Strip comment-only header lines (they aren't valid SQL inside
            // executescript on some SQLite builds; sqlx tolerates them but
            // we filter anyway for clarity).
            let stripped: String = sql
                .lines()
                .filter(|l| !l.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n");
            exec_sql(&target, &stripped).await;
        }

        let before = snapshot_dir(&migrations);

        // Second run with identical source/target — must write nothing.
        let out2 = run_uvg(&[
            "--generator", "ddl",
            "--out-dir", &mig_str,
            "--name", "should-not-appear",
            &src_url, &tgt_url,
        ]);
        assert!(out2.status.success(), "noop run failed: {}", String::from_utf8_lossy(&out2.stderr));
        let stderr = String::from_utf8_lossy(&out2.stderr);
        assert!(stderr.contains("no schema changes"), "expected no-op message, got: {stderr}");

        let after = snapshot_dir(&migrations);
        assert_eq!(
            before, after,
            "no-op run must leave the out-dir byte-identical"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_out_dir_requires_target_url() {
        let dir = tmpdir("outdir-no-target");
        let source = dir.join("source.db");
        let migrations = dir.join("migrations");

        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        let src_url = format!("sqlite:///{}", source.display());
        let mig_str = migrations.display().to_string();

        // No target URL — uvg should refuse and explain.
        let out = run_uvg(&[
            "--generator", "ddl",
            "--out-dir", &mig_str,
            &src_url,
        ]);
        assert!(!out.status.success(), "expected non-zero exit");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("target database URL"),
            "expected helpful error about missing target URL, got: {stderr}"
        );
        assert!(
            !migrations.exists() || snapshot_dir(&migrations).is_empty(),
            "no files should be written when --out-dir is misconfigured"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Introspect a sqlite db and return the sorted list of `(table, column)`
    /// pairs. Used to assert that --apply made the target match the source.
    async fn schema_columns(db_path: &Path) -> Vec<(String, String)> {
        let url = format!("sqlite://{}?mode=ro", db_path.display());
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("sqlite connect");
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT m.name, p.name
             FROM sqlite_master m
             JOIN pragma_table_info(m.name) p
             WHERE m.type='table' AND m.name NOT LIKE 'sqlite_%'
             ORDER BY m.name, p.cid",
        )
        .fetch_all(&pool)
        .await
        .expect("schema query");
        pool.close().await;
        rows
    }

    #[tokio::test]
    async fn test_apply_blob_runs_diff_against_target() {
        // --apply (no --out-dir): generate the diff and execute it
        // against the target in one shot. The target schema must end
        // up matching the source, and a second run must report
        // "no schema changes" (the diff engine is idempotent).
        let dir = tmpdir("apply-blob");
        let source = dir.join("source.db");
        let target = dir.join("target.db");

        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT NOT NULL);
             CREATE TABLE posts(id INTEGER PRIMARY KEY, user_id INTEGER,
                                FOREIGN KEY(user_id) REFERENCES users(id));",
        )
        .await;
        // Touch the target so the file exists with an empty schema.
        exec_sql(&target, "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;").await;

        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(out.status.success(), "apply run failed: {}", String::from_utf8_lossy(&out.stderr));
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("uvg: applied") && stderr.contains("statement"),
            "expected applied-count summary, got: {stderr}"
        );

        // Target now has the same tables/columns as source.
        let src_cols = schema_columns(&source).await;
        let tgt_cols = schema_columns(&target).await;
        assert_eq!(
            src_cols, tgt_cols,
            "after --apply, target schema must match source"
        );

        // Idempotence: second run reports no changes.
        let out2 = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(out2.status.success());
        let stderr2 = String::from_utf8_lossy(&out2.stderr);
        // No-changes apply prints "uvg: applied 0 statement(s) to ...".
        assert!(
            stderr2.contains("applied 0 statement"),
            "second run must be a zero-statement no-op, got: {stderr2}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_out_dir_writes_and_executes() {
        // --apply with --out-dir: writes the per-table layout AND
        // executes each file in manifest order. After running, the
        // target must match the source AND the migration files must
        // be on disk for git review.
        let dir = tmpdir("apply-outdir");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        let migrations = dir.join("migrations");

        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT NOT NULL);
             CREATE TABLE posts(id INTEGER PRIMARY KEY, user_id INTEGER,
                                FOREIGN KEY(user_id) REFERENCES users(id));",
        )
        .await;
        exec_sql(&target, "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;").await;

        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());
        let mig_str = migrations.display().to_string();

        let out = run_uvg(&[
            "--generator", "ddl",
            "--apply",
            "--out-dir", &mig_str,
            "--name", "initial",
            &src_url, &tgt_url,
        ]);
        assert!(out.status.success(), "apply+outdir run failed: {}", String::from_utf8_lossy(&out.stderr));
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("wrote 2 file(s)"), "expected file-write summary: {stderr}");
        assert!(stderr.contains("applied") && stderr.contains("across 2 table(s)"), "expected apply summary: {stderr}");

        // Files are on disk for git.
        assert!(migrations.join("users").is_dir());
        assert!(migrations.join("posts").is_dir());
        assert!(migrations.join("_runs").is_dir());

        // Target matches source.
        let src_cols = schema_columns(&source).await;
        let tgt_cols = schema_columns(&target).await;
        assert_eq!(src_cols, tgt_cols);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_rejected_for_non_ddl_generator() {
        // Regression: codex round 1 caught that --apply with the
        // default declarative generator silently succeeded — uvg
        // generated Python models and exited 0 without touching the
        // target database. In a script that looks like a successful
        // apply, was actually a no-op. The pre-match guard now
        // rejects it with a clear message.
        let dir = tmpdir("apply-wrong-gen");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        exec_sql(&target, "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;").await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        // No --generator flag means default = declarative; --apply
        // with that combination must fail before any DB I/O.
        let out = run_uvg(&["--apply", &src_url, &tgt_url]);
        assert!(!out.status.success(), "must exit non-zero");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("--apply only works with --generator ddl"),
            "expected generator-mismatch error, got: {stderr}"
        );

        // Sanity: same args plus `--generator ddl` succeeds. Both
        // paths share preflight; this confirms only the guard fires.
        let out_ddl = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(out_ddl.status.success(), "ddl variant must succeed: {}", String::from_utf8_lossy(&out_ddl.stderr));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_rejects_target_dialect_url_mismatch() {
        // Regression: codex round 1 caught that --apply combined with
        // --target-dialect could send DDL for one engine to a database
        // of another (e.g. MySQL DDL into a PG target). Now rejected
        // at validation time with a clear message.
        let dir = tmpdir("apply-dialect-mismatch");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        exec_sql(&target, "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;").await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        // sqlite target but --target-dialect mysql: validation error.
        let out = run_uvg(&[
            "--generator", "ddl",
            "--apply",
            "--target-dialect", "mysql",
            &src_url, &tgt_url,
        ]);
        assert!(!out.status.success(), "must exit non-zero");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("--target-dialect")
                && stderr.contains("does not match")
                && stderr.contains("target URL"),
            "expected dialect-mismatch error, got: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_requires_target_url() {
        // --apply without a target URL must fail fast with a clear
        // message and non-zero exit, before any introspection.
        let dir = tmpdir("apply-no-target");
        let source = dir.join("source.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        let src_url = format!("sqlite:///{}", source.display());

        let out = run_uvg(&["--generator", "ddl", "--apply", &src_url]);
        assert!(!out.status.success(), "must exit non-zero");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("--apply requires a target database URL"),
            "expected friendly error, got: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_rejects_comment_only_diff() {
        // Regression: codex round 2 caught that some diffs produce
        // only warning comments (SQLite ALTER COLUMN, MSSQL constraint
        // drop) — db::execute_ddl runs zero statements and used to
        // report "applied 0 statement(s)" as success, leaving the
        // target unchanged. The guard in apply_blob now distinguishes
        // these from the legitimate "no schema changes" sentinel.
        let dir = tmpdir("apply-comment-only");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        // Same column name, different types — SQLite emits the diff
        // as an `-- ALTER TABLE ... ALTER COLUMN ... TYPE ...;` warning
        // comment (SQLite doesn't actually support ALTER COLUMN).
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT);").await;
        exec_sql(&target, "CREATE TABLE users(id INTEGER PRIMARY KEY, email INTEGER);").await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(!out.status.success(), "must exit non-zero on comment-only diff");
        let stderr = String::from_utf8_lossy(&out.stderr);
        // The error wording shifted between codex rounds; pin only the
        // structural promises: clear refusal, mention of --outfile/--out-dir
        // so the user knows where to look, and a hint about what triggered it.
        assert!(
            stderr.contains("refusing to apply")
                && (stderr.contains("--outfile") || stderr.contains("--out-dir")),
            "expected refusal + inspection hint, got: {stderr}"
        );

        // Target's schema must be unchanged — confirm column type stayed.
        let tgt_cols: Vec<(String, String)> = sqlx::query_as(
            "SELECT name, type FROM pragma_table_info('users') ORDER BY cid",
        )
        .fetch_all(
            &sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&format!("sqlite://{}?mode=ro", target.display()))
                .await
                .expect("sqlite connect"),
        )
        .await
        .expect("schema query");
        let email = tgt_cols.iter().find(|(n, _)| n == "email").expect("email column");
        assert_eq!(email.1, "INTEGER", "target schema must be untouched: {tgt_cols:?}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_rejects_mixed_diff_with_unappliable_warning() {
        // Regression: codex round 3 caught that round 2's guard only
        // fired when split_statements returned EMPTY. A diff that
        // contains a real ADD COLUMN AND a comment-only ALTER COLUMN
        // warning would silently execute the ADD COLUMN, drop the
        // warning chunk, and exit 0 — target partially migrated, CI
        // green. The marker check now rejects the whole apply before
        // any statement runs.
        let dir = tmpdir("apply-mixed-warning");
        let source = dir.join("source.db");
        let target = dir.join("target.db");

        // Source: email TEXT (type change vs target) + phone TEXT (new).
        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT, phone TEXT);",
        )
        .await;
        // Target: email INTEGER (drives the ALTER COLUMN warning), no
        // phone column (drives a real ADD COLUMN — the executable bit
        // that would have leaked through the round-2 guard).
        exec_sql(&target, "CREATE TABLE users(id INTEGER PRIMARY KEY, email INTEGER);").await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(!out.status.success(), "mixed diff must error before applying");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("refusing to apply") && stderr.contains("ALTER COLUMN"),
            "expected marker-specific error, got: {stderr}"
        );

        // The ADD COLUMN must NOT have run — target schema must still
        // lack the `phone` column. This is the bug codex caught: with
        // the round-2 guard, the ADD COLUMN would have applied and
        // phone would now be present.
        let cols: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM pragma_table_info('users')",
        )
        .fetch_all(
            &sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&format!("sqlite://{}?mode=ro", target.display()))
                .await
                .expect("sqlite connect"),
        )
        .await
        .expect("schema query");
        let names: Vec<&str> = cols.iter().map(|(n,)| n.as_str()).collect();
        assert!(!names.contains(&"phone"), "ADD COLUMN must not have run: {names:?}");
        assert!(names.contains(&"id") && names.contains(&"email"));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_rejects_split_tables_combo() {
        // Regression: codex round 2 caught that --apply --split-tables
        // was supposed to error, but the only rejection lived inside
        // DdlGenerator's Split arm — unreachable under --apply because
        // a diff (target_schema = Some) always returns Single. With
        // the rejection now hoisted to the top-of-main preflight,
        // the combination errors before any DB I/O.
        let dir = tmpdir("apply-split-combo");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        exec_sql(&target, "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;").await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&[
            "--generator", "ddl",
            "--apply",
            "--split-tables",
            &src_url, &tgt_url,
        ]);
        assert!(!out.status.success(), "combo must error");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("--apply with --split-tables")
                && stderr.contains("--out-dir"),
            "expected guidance pointing at --out-dir, got: {stderr}"
        );

        // Target schema must be unchanged — assert no `users` table.
        let tables: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'",
        )
        .fetch_all(
            &sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&format!("sqlite://{}?mode=ro", target.display()))
                .await
                .expect("sqlite connect"),
        )
        .await
        .expect("query");
        assert!(tables.is_empty(), "target must not have been mutated by the rejected combo");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_preflight_runs_before_source_connection() {
        // Regression: codex round 2 caught that the --apply preflight
        // ran after main had already connected to and introspected
        // the source DB. An invalid --apply invocation against an
        // unreachable source URL would error with a connection
        // failure instead of the friendlier validation message.
        // The preflight now happens before any DB I/O. We exercise
        // the missing-target-URL case against an unreachable
        // postgres URL: the error must be about the missing target,
        // NOT about the unreachable source.
        let out = run_uvg(&[
            "--generator", "ddl",
            "--apply",
            "postgres://nobody:nopass@unreachable.invalid:5432/none",
        ]);
        assert!(!out.status.success(), "must exit non-zero");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("--apply requires a target database URL"),
            "expected target-URL validation error before source connection, got: {stderr}"
        );
        // Conversely, must NOT have leaked the bad source URL's host
        // into a connection-error message — that would prove we
        // tried to connect first.
        assert!(
            !stderr.contains("unreachable.invalid"),
            "preflight must run before source connection attempt, got: {stderr}"
        );
    }
}
