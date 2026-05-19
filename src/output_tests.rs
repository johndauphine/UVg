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

    let manifest = write_split_changes(&changes, &ctx)
        .unwrap()
        .expect("non-empty diff returns Some");

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
    assert!(dir
        .join("_runs")
        .join("20260513T193000Z__add-email.json")
        .exists());

    // Two statements landed in users/ — one file, both statements
    let users_body = fs::read_to_string(dir.join("users").join(fname)).unwrap();
    assert!(users_body.contains("CREATE TABLE"));
    assert!(users_body.contains("CREATE INDEX"));

    // Manifest contents
    assert_eq!(manifest.stats.changes, 4);
    assert_eq!(manifest.files.len(), 3); // users + posts + _schema
    assert!(manifest
        .files
        .iter()
        .any(|f| f == &format!("users/{fname}")));
    assert!(manifest
        .files
        .iter()
        .any(|f| f == &format!("posts/{fname}")));
    assert!(manifest
        .files
        .iter()
        .any(|f| f == &format!("_schema/{fname}")));
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
    let body =
        fs::read_to_string(dir.join("users").join("20260513T193000Z__add-email.sql")).unwrap();
    assert!(
        body.starts_with("-- Generated by uvg "),
        "header missing: {body}"
    );
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
    assert!(
        subdir.is_dir(),
        "non-default schema should produce billing__orders/"
    );
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

#[test]
fn test_manifest_preserves_topological_order() {
    // Regression: codex review caught that `written.sort()` was
    // re-sorting manifest.files alphabetically, which clobbered the
    // FK topological order from compute_changes. Here `users` is
    // referenced by `posts`; topo order is [users, posts] but
    // lexicographic order is [posts, users]. The manifest must hold
    // topo order so apply_order() runs `users` first.
    let dir = tmpdir("topo");
    let ctx = make_ctx(dir.clone());
    let changes = vec![
        Change {
            table_schema: "".into(),
            table_name: Some("users".into()),
            sql: "CREATE TABLE users();".into(),
        },
        Change {
            table_schema: "".into(),
            table_name: Some("posts".into()),
            sql: "CREATE TABLE posts(user_id int REFERENCES users(id));".into(),
        },
    ];
    let manifest = write_split_changes(&changes, &ctx).unwrap().unwrap();
    let users_idx = manifest
        .files
        .iter()
        .position(|f| f.starts_with("users/"))
        .expect("users entry");
    let posts_idx = manifest
        .files
        .iter()
        .position(|f| f.starts_with("posts/"))
        .expect("posts entry");
    assert!(
        users_idx < posts_idx,
        "manifest.files must keep users before posts (topo); got {:?}",
        manifest.files
    );

    // And apply_order must propagate that order to the final list of
    // paths handed to db::execute_ddl.
    let order = apply_order(&manifest, &dir);
    let order_strs: Vec<String> = order
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    let users_p = order_strs
        .iter()
        .position(|s| s.contains("users/"))
        .unwrap();
    let posts_p = order_strs
        .iter()
        .position(|s| s.contains("posts/"))
        .unwrap();
    assert!(
        users_p < posts_p,
        "apply_order must run users before posts: {order_strs:?}"
    );

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_sanitize_path_component_blocks_traversal() {
    assert_eq!(sanitize_path_component("../escape"), ".._escape");
    assert_eq!(sanitize_path_component(".."), "_");
    assert_eq!(sanitize_path_component("."), "_");
    assert_eq!(sanitize_path_component(""), "_");
    assert_eq!(sanitize_path_component("/etc/passwd"), "_etc_passwd");
    assert_eq!(sanitize_path_component("a\\b"), "a_b");
    assert_eq!(sanitize_path_component("c:\\windows"), "c__windows");
    assert_eq!(sanitize_path_component("with\0null"), "with_null");
    // Benign names pass through unchanged.
    assert_eq!(sanitize_path_component("users"), "users");
    assert_eq!(sanitize_path_component("billing"), "billing");
}

#[test]
fn test_table_named_schema_does_not_collide_with_metadata_dir() {
    // Regression: codex round 3 caught that a real table literally
    // named `_schema` returned the same subdir as non-table-scoped
    // DDL. The TUI and apply_order special-case `_schema`, so a
    // real `_schema` table would be applied first regardless of
    // its real FK position. The bucket is now escaped to
    // `_schema_table`.
    let schema_table = Change {
        table_schema: "".into(),
        table_name: Some("_schema".into()),
        sql: "CREATE TABLE \"_schema\"(id int);".into(),
    };
    assert_eq!(subdir_for(&schema_table), "_schema_table");

    let runs_table = Change {
        table_schema: "".into(),
        table_name: Some("_runs".into()),
        sql: "CREATE TABLE \"_runs\"(id int);".into(),
    };
    assert_eq!(subdir_for(&runs_table), "_runs_table");

    // Schema-scoped DDL still goes to `_schema`.
    let scoped = Change {
        table_schema: "".into(),
        table_name: None,
        sql: "CREATE TYPE color AS ENUM('r','g','b');".into(),
    };
    assert_eq!(subdir_for(&scoped), "_schema");
}

#[test]
fn test_table_named_schema_writes_to_distinct_subdir() {
    // End-to-end check: when a real `_schema` table coexists with
    // schema-scoped DDL, they land in distinct directories on disk.
    let dir = tmpdir("schema-collision");
    let ctx = make_ctx(dir.clone());
    let changes = vec![
        Change {
            table_schema: "".into(),
            table_name: None,
            sql: "CREATE TYPE color AS ENUM('r','g','b');".into(),
        },
        Change {
            table_schema: "".into(),
            table_name: Some("_schema".into()),
            sql: "CREATE TABLE \"_schema\"(id int);".into(),
        },
    ];
    let manifest = write_split_changes(&changes, &ctx).unwrap().unwrap();
    assert!(dir.join("_schema").is_dir(), "schema-scoped dir present");
    assert!(
        dir.join("_schema_table").is_dir(),
        "real `_schema` table goes to _schema_table"
    );
    // Each one has its own file, neither mixed.
    let schema_body =
        fs::read_to_string(dir.join("_schema").join("20260513T193000Z__add-email.sql")).unwrap();
    let table_body = fs::read_to_string(
        dir.join("_schema_table")
            .join("20260513T193000Z__add-email.sql"),
    )
    .unwrap();
    assert!(schema_body.contains("CREATE TYPE"));
    assert!(!schema_body.contains("CREATE TABLE"));
    assert!(table_body.contains("CREATE TABLE"));
    assert!(!table_body.contains("CREATE TYPE"));
    // Manifest references both.
    assert!(manifest.files.iter().any(|f| f.starts_with("_schema/")));
    assert!(manifest
        .files
        .iter()
        .any(|f| f.starts_with("_schema_table/")));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_preflight_aborts_when_subdir_is_regular_file() {
    // Regression: codex round 3 caught that if `migrations/posts`
    // is already a regular file, the per-target-file probe still
    // passed (posts/<filename> doesn't exist), and the write loop
    // would create `users/...sql` before dying on create_dir_all
    // for `posts`. The preflight must check each subdir's type
    // too, so we abort with zero partial writes.
    let dir = tmpdir("subdir-conflict");
    let ctx = make_ctx(dir.clone());
    // Pre-create `posts` as a regular file. The write attempt for
    // this run plans `users/` and `posts/` subdirs.
    fs::write(dir.join("posts"), b"not a directory").unwrap();
    let changes = vec![
        Change {
            table_schema: "".into(),
            table_name: Some("users".into()),
            sql: "CREATE TABLE users();".into(),
        },
        Change {
            table_schema: "".into(),
            table_name: Some("posts".into()),
            sql: "CREATE TABLE posts();".into(),
        },
    ];
    let result = write_split_changes(&changes, &ctx);
    assert!(result.is_err(), "preflight must fail");
    let err = result.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    // No partial state — no users/ subdir, no manifest, no _runs/.
    assert!(
        !dir.join("users").exists(),
        "must not write users/ on aborted preflight"
    );
    assert!(
        !dir.join("_runs").exists(),
        "must not create _runs/ on aborted preflight"
    );
    // The pre-existing posts file is untouched.
    let body = fs::read(dir.join("posts")).unwrap();
    assert_eq!(body, b"not a directory");
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_flatten_for_comment_escapes_control_chars() {
    // The header bakes interpolated values into `-- ...` comment
    // lines. A raw newline would terminate the comment and inject
    // executable SQL. flatten_for_comment must escape \n, \r, \t,
    // and other control characters into visible escapes.
    assert_eq!(
        flatten_for_comment("evil\nDROP TABLE users;"),
        "evil\\nDROP TABLE users;",
        "newline must become \\n"
    );
    assert_eq!(flatten_for_comment("with\rcarriage"), "with\\rcarriage");
    assert_eq!(flatten_for_comment("col\tname"), "col\\tname");
    assert_eq!(flatten_for_comment("nul\0byte"), "nul\\x00byte");
    assert_eq!(flatten_for_comment("del\x7f"), "del\\x7f");
    assert_eq!(flatten_for_comment("plain"), "plain");
    assert_eq!(flatten_for_comment("billing.orders"), "billing.orders");
}

#[test]
fn test_header_cannot_be_escaped_via_newline_in_table_name() {
    // End-to-end: a table identifier containing a newline must not
    // break out of the SQL comment in the generated migration file.
    let dir = tmpdir("header-injection");
    let ctx = make_ctx(dir.clone());
    let changes = vec![Change {
        table_schema: "".into(),
        table_name: Some("evil\nDROP TABLE users;".into()),
        sql: "CREATE TABLE evil(id int);".into(),
    }];
    write_split_changes(&changes, &ctx).unwrap();

    // The escaped identifier appears as a path: `evil\n...` is
    // sanitized by subdir_for to a single safe component. Find
    // it and inspect the generated file.
    let subdir_entry = std::fs::read_dir(&dir)
        .unwrap()
        .find_map(|e| {
            let p = e.unwrap().path();
            let name = p.file_name().unwrap().to_string_lossy().to_string();
            if name == "_runs" {
                None
            } else {
                Some(p)
            }
        })
        .expect("table subdir present");
    let sql_path = std::fs::read_dir(&subdir_entry)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let body = std::fs::read_to_string(&sql_path).unwrap();

    // Every line that came from the header must remain a comment.
    // The only non-comment line should be the actual CREATE TABLE.
    let header_section = &body[..body.find("CREATE TABLE").unwrap()];
    for line in header_section.lines() {
        let trimmed = line.trim_start();
        assert!(
            trimmed.is_empty() || trimmed.starts_with("--"),
            "header line escaped the comment: {line:?}"
        );
    }
    // And specifically, the injected `DROP TABLE users;` text from
    // the malicious name must not appear as standalone SQL.
    assert!(
        !body.contains("\nDROP TABLE users;"),
        "newline-then-DROP must be escaped, body was: {body}"
    );

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_write_new_is_atomic_create_or_fail() {
    // Regression: codex round 4 caught that path.exists() + fs::write
    // is a TOCTOU race — two concurrent processes can both pass the
    // preflight, then the second fs::write would truncate the first
    // process's output. write_new() now uses OpenOptions::create_new
    // so the create-or-fail is enforced by the kernel, not by a
    // racy two-step check.
    let dir = tmpdir("write-new");
    let path = dir.join("artifact.sql");

    write_new(&path, b"first write\n", "run-A").expect("first create_new must succeed");
    let body_before = fs::read(&path).unwrap();
    assert_eq!(body_before, b"first write\n");

    // A second write to the same path must fail atomically with the
    // friendly recovery message — no truncation of the first file.
    let err = write_new(&path, b"clobber attempt\n", "run-B").unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    assert!(
        err.to_string().contains("refusing to overwrite") && err.to_string().contains("run-B"),
        "error must explain how to recover and include the colliding run_id: {err}"
    );

    // First file's content is untouched.
    let body_after = fs::read(&path).unwrap();
    assert_eq!(body_after, b"first write\n");

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_collision_refuses_overwrite() {
    // Regression: codex round 2 caught that two `--out-dir` runs
    // with the same `--name` in the same second silently truncated
    // the earlier migration. The splitter must now refuse to
    // overwrite and tell the user which path collided.
    let dir = tmpdir("collision");
    let ctx = make_ctx(dir.clone());
    let changes = vec![Change {
        table_schema: "".into(),
        table_name: Some("users".into()),
        sql: "CREATE TABLE users();".into(),
    }];

    let first = write_split_changes(&changes, &ctx).unwrap();
    assert!(first.is_some(), "first write should succeed");

    // Capture the on-disk state so we can prove the second run
    // didn't touch it.
    let before =
        fs::read_to_string(dir.join("users").join("20260513T193000Z__add-email.sql")).unwrap();

    let second = write_split_changes(&changes, &ctx);
    assert!(second.is_err(), "second run with same ctx must error");
    let err = second.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    let msg = err.to_string();
    assert!(
        msg.contains("refusing to overwrite") && msg.contains("--name"),
        "error must explain how to recover: {msg}"
    );

    // First run's content is untouched.
    let after =
        fs::read_to_string(dir.join("users").join("20260513T193000Z__add-email.sql")).unwrap();
    assert_eq!(before, after, "first run's file must be preserved");

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_name_with_separators_sanitized() {
    // Regression: codex round 2 caught that `--name feature/x` left
    // a `/` inside the filename, which then failed with ENOENT
    // because write_split_changes() only mkdir'd the table subdir,
    // not the synthetic one introduced by the slash. The tag is now
    // sanitized at OutputContext construction.
    let dir = tmpdir("name-slash");
    let ctx = OutputContext::at(
        dir.clone(),
        Some("feature/add-email".to_string()),
        Dialect::Postgres,
        Dialect::Postgres,
        1_778_700_600,
    );
    // The slash is replaced with `_` in tag, run_id, and filenames.
    assert_eq!(ctx.tag, "feature_add-email");
    assert!(!ctx.run_id.contains('/'));

    let changes = vec![Change {
        table_schema: "".into(),
        table_name: Some("users".into()),
        sql: "CREATE TABLE users();".into(),
    }];
    let manifest = write_split_changes(&changes, &ctx)
        .expect("sanitized tag must let write_split_changes succeed")
        .expect("non-empty changes must produce a manifest");

    // The actual on-disk path matches the sanitized run_id. No
    // `feature/` subdir was created under users/.
    let expected = dir
        .join("users")
        .join("20260513T193000Z__feature_add-email.sql");
    assert!(expected.exists(), "expected file at {}", expected.display());
    assert!(
        !dir.join("users").join("feature").exists(),
        "no stray feature/ subdir from the unsanitized slash"
    );
    // Manifest references the same on-disk name.
    assert!(manifest
        .files
        .iter()
        .any(|f| f.ends_with("__feature_add-email.sql")));

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_malicious_table_name_cannot_escape_out_dir() {
    // Regression: codex review caught that raw identifiers were
    // joined under out_dir, so a table named `../escape` would write
    // outside the directory. The splitter must keep every written
    // file under ctx.out_dir.
    let dir = tmpdir("escape");
    let ctx = make_ctx(dir.clone());
    let changes = vec![
        Change {
            table_schema: "".into(),
            table_name: Some("../escape".into()),
            sql: "CREATE TABLE evil();".into(),
        },
        Change {
            table_schema: "".into(),
            table_name: Some("/etc/passwd".into()),
            sql: "CREATE TABLE worse();".into(),
        },
    ];
    let manifest = write_split_changes(&changes, &ctx).unwrap().unwrap();

    // Every manifest entry, when joined under out_dir, must resolve
    // to a path inside out_dir (no `..`, no absolute paths).
    let dir_canon = dir.canonicalize().unwrap();
    for f in &manifest.files {
        let full = dir.join(f);
        // The file actually exists where we recorded it.
        assert!(full.exists(), "manifest references missing path: {f}");
        // Its canonical form is under the canonical out_dir.
        let full_canon = full.canonicalize().unwrap();
        assert!(
            full_canon.starts_with(&dir_canon),
            "file {} resolved to {} which escapes {}",
            f,
            full_canon.display(),
            dir_canon.display(),
        );
    }

    // Nothing should have been written to the parent of out_dir.
    let parent = dir.parent().unwrap();
    for entry in fs::read_dir(parent).unwrap() {
        let p = entry.unwrap().path();
        assert!(
            p == dir || !p.file_name().unwrap().to_string_lossy().contains("escape"),
            "found escape file at {}",
            p.display()
        );
    }

    fs::remove_dir_all(&dir).ok();
}
