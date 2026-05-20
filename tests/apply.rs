mod common;

#[cfg(test)]
mod tests {
    use super::common::{exec_sql, run_uvg, snapshot_dir, tmpdir};
    use std::path::Path;

    /// Read every table name from a SQLite DB. Used to verify that an
    /// `--apply` run actually created the source's tables on the target.
    async fn list_tables(db_path: &Path) -> Vec<String> {
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("sqlite connect");
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' \
             ORDER BY name",
        )
        .fetch_all(&pool)
        .await
        .expect("list tables");
        pool.close().await;
        rows.into_iter().map(|(n,)| n).collect()
    }

    async fn column_names(db_path: &Path, table: &str) -> Vec<String> {
        let url = format!("sqlite://{}?mode=ro", db_path.display());
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("sqlite connect");
        let sql = format!("SELECT name FROM pragma_table_info('{table}') ORDER BY cid");
        let rows: Vec<(String,)> = sqlx::query_as(&sql)
            .fetch_all(&pool)
            .await
            .expect("column query");
        pool.close().await;
        rows.into_iter().map(|(name,)| name).collect()
    }

    async fn column_types(db_path: &Path, table: &str) -> Vec<(String, String)> {
        let url = format!("sqlite://{}?mode=ro", db_path.display());
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("sqlite connect");
        let sql = format!("SELECT name, type FROM pragma_table_info('{table}') ORDER BY cid");
        let rows = sqlx::query_as(&sql)
            .fetch_all(&pool)
            .await
            .expect("column type query");
        pool.close().await;
        rows
    }

    #[tokio::test]
    async fn test_apply_inline_creates_target_tables() {
        // --apply (no --out-dir) should generate the diff and execute it
        // against the target in one shot.
        let dir = tmpdir("apply-inline");
        let source = dir.join("source.db");
        let target = dir.join("target.db");

        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT NOT NULL);
             CREATE TABLE posts(id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT,
                                FOREIGN KEY(user_id) REFERENCES users(id));",
        )
        .await;
        // Bring the target file into existence with an empty schema.
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;

        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        // Sanity: target starts empty.
        assert!(
            list_tables(&target).await.is_empty(),
            "target should start empty"
        );

        let out = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(
            out.status.success(),
            "apply run failed: stderr={}, stdout={}",
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout),
        );

        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("applied") && stderr.contains("statement"),
            "expected applied-summary on stderr, got: {stderr}"
        );

        // Target should now match the source's tables.
        let tables = list_tables(&target).await;
        assert_eq!(tables, vec!["posts".to_string(), "users".to_string()]);

        // Second run is a no-op: zero applied statements, "no schema changes" on stderr.
        let out2 = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(
            out2.status.success(),
            "noop apply failed: {}",
            String::from_utf8_lossy(&out2.stderr)
        );
        let stderr2 = String::from_utf8_lossy(&out2.stderr);
        assert!(
            stderr2.contains("no schema changes"),
            "expected no-schema-changes message, got: {stderr2}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_outdir_writes_and_applies() {
        // --apply with --out-dir should write per-table files AND execute
        // them in manifest order. The FK from posts -> users means the
        // apply only succeeds if users is created before posts.
        let dir = tmpdir("apply-outdir");
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
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;

        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());
        let mig_str = migrations.display().to_string();

        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--apply",
            "--out-dir",
            &mig_str,
            "--name",
            "initial",
            &src_url,
            &tgt_url,
        ]);
        assert!(
            out.status.success(),
            "apply+outdir run failed: stderr={}",
            String::from_utf8_lossy(&out.stderr),
        );

        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("wrote"),
            "expected write summary, got: {stderr}"
        );
        assert!(
            stderr.contains("applied") && stderr.contains("file"),
            "expected apply summary, got: {stderr}"
        );

        // Files were written under per-table layout.
        assert!(migrations.join("users").is_dir(), "users/ missing");
        assert!(migrations.join("posts").is_dir(), "posts/ missing");

        // Target now matches the source.
        assert_eq!(
            list_tables(&target).await,
            vec!["posts".to_string(), "users".to_string()]
        );

        // Re-run is a no-op: nothing applied, nothing written.
        let before = snapshot_dir(&migrations);
        let out2 = run_uvg(&[
            "--generator",
            "ddl",
            "--apply",
            "--out-dir",
            &mig_str,
            "--name",
            "should-not-appear",
            &src_url,
            &tgt_url,
        ]);
        assert!(
            out2.status.success(),
            "noop apply+outdir failed: {}",
            String::from_utf8_lossy(&out2.stderr),
        );
        let stderr2 = String::from_utf8_lossy(&out2.stderr);
        assert!(
            stderr2.contains("no schema changes"),
            "expected no-op message, got: {stderr2}"
        );
        let after = snapshot_dir(&migrations);
        assert_eq!(before, after, "no-op run must not touch the migrations dir");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_progress_emits_per_statement_lines_with_on() {
        // --progress=on must emit one `[i/total] <preview>  <ms>ms`
        // line per executed statement plus a class-breakdown summary.
        let dir = tmpdir("apply-progress-on");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT NOT NULL);
             CREATE TABLE posts(id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT,
                                FOREIGN KEY(user_id) REFERENCES users(id));",
        )
        .await;
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--apply",
            "--progress",
            "on",
            &src_url,
            &tgt_url,
        ]);
        assert!(
            out.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr),
        );

        let stderr = String::from_utf8_lossy(&out.stderr);
        // At least one per-statement line.
        let stmt_lines: Vec<_> = stderr.lines().filter(|l| l.starts_with('[')).collect();
        assert!(
            stmt_lines.len() >= 2,
            "expected >=2 per-statement progress lines, got {}: {stderr}",
            stmt_lines.len(),
        );
        // Each per-statement line carries `[i/total]` and `ms`.
        for line in &stmt_lines {
            assert!(line.contains('/'), "missing /: {line}");
            assert!(line.contains("ms"), "missing ms: {line}");
        }
        // Final summary line with class breakdown.
        assert!(
            stderr.contains("Applied") && stderr.contains("tables"),
            "missing class-breakdown summary: {stderr}",
        );
        // Progress is on stderr, not stdout.
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            !stdout.lines().any(|l| l.starts_with('[')),
            "progress leaked into stdout: {stdout}",
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_progress_off_is_silent() {
        // --progress=off must suppress per-statement lines and the
        // class-breakdown summary, while keeping the standard
        // "uvg: applied N statement(s)..." one-liner.
        let dir = tmpdir("apply-progress-off");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--apply",
            "--progress",
            "off",
            &src_url,
            &tgt_url,
        ]);
        assert!(
            out.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);

        assert!(
            !stderr.lines().any(|l| l.starts_with('[')),
            "per-statement line leaked with --progress=off: {stderr}",
        );
        assert!(
            !stderr.contains("Applied 1 statement(s) in"),
            "class-breakdown summary leaked with --progress=off: {stderr}",
        );
        // Standard apply-summary still present.
        assert!(
            stderr.contains("uvg: applied 1"),
            "missing apply summary: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_parse_check_skipped_note_on_sqlite() {
        // SQLite has no parse-only mode, so `run_parse_check` emits a
        // one-line skip note on stderr (rather than aborting the
        // apply). Default --apply enables parse-check, so the note
        // should appear on a normal apply run.
        let dir = tmpdir("parse-check-sqlite-skip");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(
            out.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("parse-check skipped"),
            "expected parse-check-skipped note on sqlite default apply: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_no_parse_check_suppresses_skip_note() {
        // --no-parse-check must avoid even the skip note, because the
        // user explicitly turned the phase off. Verifies the flag is
        // wired through end-to-end.
        let dir = tmpdir("parse-check-sqlite-suppressed");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--apply",
            "--no-parse-check",
            &src_url,
            &tgt_url,
        ]);
        assert!(
            out.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !stderr.contains("parse-check skipped"),
            "skip note must not appear with --no-parse-check: {stderr}"
        );
        // Sanity: the apply itself still happened.
        assert!(
            stderr.contains("uvg: applied"),
            "apply summary missing: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    // No E2E for the failed-statement progress path: forcing uvg to
    // emit a statement that fails-on-apply requires either custom
    // injection beyond the public CLI's surface (the diff engine
    // skips emit when source and target match) or a race-prone
    // "pre-create-after-introspection" trick. The FAIL suffix and
    // record-skipping-on-failure contracts are covered by unit tests
    // in `src/apply_progress.rs` (`stats_record_skips_failed_statements`).

    #[tokio::test]
    async fn test_apply_progress_auto_is_silent_when_stderr_redirected() {
        // Default --progress=auto consults stderr.is_terminal(); the
        // subprocess we spawn has a piped stderr (not a TTY), so auto
        // should resolve to off. Same assertion as --progress=off.
        let dir = tmpdir("apply-progress-auto");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(
            out.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !stderr.lines().any(|l| l.starts_with('[')),
            "auto should suppress when stderr isn't a TTY: {stderr}",
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_requires_target_url() {
        // --apply without a target URL must exit non-zero with a helpful message.
        let dir = tmpdir("apply-no-target");
        let source = dir.join("source.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        let src_url = format!("sqlite:///{}", source.display());

        let out = run_uvg(&["--generator", "ddl", "--apply", &src_url]);
        assert!(!out.status.success(), "expected non-zero exit");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("target database URL"),
            "expected helpful error, got: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_rejected_for_non_ddl_generator() {
        let dir = tmpdir("apply-wrong-generator");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&["--apply", &src_url, &tgt_url]);
        assert!(!out.status.success(), "must exit non-zero");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("--apply only works with --generator ddl"),
            "expected generator-mismatch error, got: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_rejects_target_dialect_url_mismatch() {
        let dir = tmpdir("apply-dialect-mismatch");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--apply",
            "--target-dialect",
            "mysql",
            &src_url,
            &tgt_url,
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
    async fn test_apply_rejects_split_tables_combo() {
        let dir = tmpdir("apply-split-combo");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--apply",
            "--split-tables",
            &src_url,
            &tgt_url,
        ]);
        assert!(!out.status.success(), "combo must error");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("--apply with --split-tables") && stderr.contains("--out-dir"),
            "expected guidance pointing at --out-dir, got: {stderr}"
        );
        assert!(
            !list_tables(&target).await.contains(&"users".to_string()),
            "target must not be mutated by the rejected combo"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_preflight_runs_before_source_connection() {
        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--apply",
            "postgres://nobody:nopass@unreachable.invalid:5432/none",
        ]);
        assert!(!out.status.success(), "must exit non-zero");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("--apply requires a target database URL"),
            "expected target-URL validation error before source connection, got: {stderr}"
        );
        assert!(
            !stderr.contains("unreachable.invalid"),
            "preflight must run before source connection attempt, got: {stderr}"
        );
    }

    #[tokio::test]
    async fn test_apply_rejects_comment_only_diff() {
        let dir = tmpdir("apply-comment-only");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT);",
        )
        .await;
        exec_sql(
            &target,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email INTEGER);",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(
            !out.status.success(),
            "must exit non-zero on comment-only diff"
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("refusing to apply")
                && (stderr.contains("--outfile") || stderr.contains("--out-dir")),
            "expected refusal + inspection hint, got: {stderr}"
        );
        let email = column_types(&target, "users")
            .await
            .into_iter()
            .find(|(name, _)| name == "email")
            .expect("email column");
        assert_eq!(email.1, "INTEGER", "target schema must be untouched");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_rejects_mixed_diff_with_unappliable_warning() {
        let dir = tmpdir("apply-mixed-warning");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT, phone TEXT);",
        )
        .await;
        exec_sql(
            &target,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email INTEGER);",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(
            !out.status.success(),
            "mixed diff must error before applying"
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("refusing to apply") && stderr.contains("ALTER COLUMN"),
            "expected marker-specific error, got: {stderr}"
        );
        let names = column_names(&target, "users").await;
        assert!(
            !names.contains(&"phone".to_string()),
            "ADD COLUMN must not have run: {names:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_out_dir_preflights_all_files_before_executing() {
        let dir = tmpdir("apply-outdir-preflight");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        let migrations = dir.join("migrations");
        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, phone TEXT);
             CREATE TABLE posts(id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT,
                                FOREIGN KEY(user_id) REFERENCES users(id));",
        )
        .await;
        exec_sql(
            &target,
            "CREATE TABLE users(id INTEGER PRIMARY KEY);
             CREATE TABLE posts(id INTEGER PRIMARY KEY, user_id INTEGER, body INTEGER,
                                FOREIGN KEY(user_id) REFERENCES users(id));",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());
        let mig_str = migrations.display().to_string();

        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--apply",
            "--out-dir",
            &mig_str,
            "--name",
            "mixed",
            &src_url,
            &tgt_url,
        ]);
        assert!(!out.status.success(), "preflight must reject the run");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("refusing to apply") && stderr.contains("ALTER COLUMN"),
            "expected marker-specific error, got: {stderr}"
        );
        let names = column_names(&target, "users").await;
        assert!(
            !names.contains(&"phone".to_string()),
            "users.phone must not have been added: {names:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
