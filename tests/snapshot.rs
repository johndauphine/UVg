mod common;

#[cfg(test)]
mod tests {
    use super::common::{exec_sql, run_uvg, tmpdir};

    #[tokio::test]
    async fn test_snapshot_cli_round_trip_and_diff_inputs() {
        let dir = tmpdir("snapshot-round-trip");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        let snapshot = dir.join("source.yaml");
        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
             CREATE TABLE posts(id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id));",
        )
        .await;
        exec_sql(
            &target,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());
        let snapshot_path = snapshot.display().to_string();
        let snapshot_ref = format!("@{snapshot_path}");

        let out = run_uvg(&["snapshot", &src_url, "-o", &snapshot_path]);
        assert!(
            out.status.success(),
            "snapshot failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let raw = std::fs::read_to_string(&snapshot).unwrap();
        assert!(
            raw.contains("format_version: 1"),
            "snapshot header missing: {raw}"
        );
        assert!(raw.contains("uvg_version:"), "uvg_version missing: {raw}");
        assert!(raw.contains("captured_at:"), "captured_at missing: {raw}");
        assert!(raw.contains("dialect: sqlite"), "dialect missing: {raw}");

        let live = run_uvg(&[
            "--generator",
            "ddl",
            "--target-dialect",
            "sqlite",
            &src_url,
            &tgt_url,
        ]);
        assert!(
            live.status.success(),
            "live diff failed: {}",
            String::from_utf8_lossy(&live.stderr)
        );
        let snap_source = run_uvg(&[
            "--generator",
            "ddl",
            "--target-dialect",
            "sqlite",
            &snapshot_ref,
            &tgt_url,
        ]);
        assert!(
            snap_source.status.success(),
            "snapshot source diff failed: {}",
            String::from_utf8_lossy(&snap_source.stderr)
        );
        assert_eq!(live.stdout, snap_source.stdout);

        let live_target = run_uvg(&[
            "--generator",
            "ddl",
            "--target-dialect",
            "sqlite",
            &tgt_url,
            &src_url,
        ]);
        assert!(
            live_target.status.success(),
            "live target diff failed: {}",
            String::from_utf8_lossy(&live_target.stderr)
        );
        let snap_target = run_uvg(&[
            "--generator",
            "ddl",
            "--target-dialect",
            "sqlite",
            &tgt_url,
            &snapshot_ref,
        ]);
        assert!(
            snap_target.status.success(),
            "snapshot target diff failed: {}",
            String::from_utf8_lossy(&snap_target.stderr)
        );
        assert_eq!(live_target.stdout, snap_target.stdout);

        let same = run_uvg(&["--generator", "ddl", &snapshot_ref, &src_url]);
        assert!(
            same.status.success(),
            "same-db snapshot diff failed: {}",
            String::from_utf8_lossy(&same.stderr)
        );
        let same_stdout = String::from_utf8_lossy(&same.stdout);
        assert!(
            same_stdout.contains("-- No schema changes detected."),
            "same-db diff was not empty: {same_stdout}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_snapshot_format_mismatch_is_clear() {
        let dir = tmpdir("snapshot-format-mismatch");
        let target = dir.join("target.db");
        let bad_snapshot = dir.join("bad.yaml");
        exec_sql(&target, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        std::fs::write(
            &bad_snapshot,
            "format_version: 999\nuvg_version: 1.5.0\ndialect: sqlite\ntables: []\nenums: []\ndomains: []\n",
        )
        .unwrap();
        let bad_ref = format!("@{}", bad_snapshot.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&["--generator", "ddl", &bad_ref, &tgt_url]);
        assert!(!out.status.success(), "expected format mismatch to fail");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("unsupported snapshot format_version 999"),
            "missing clear format error: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
