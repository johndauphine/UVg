use super::*;
use crate::cli::ConnectionConfig;
use crate::output::Change;

fn tmpdir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "uvg-migrations-test-{label}-{}-{nanos}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn migration(revision: &str, parent: &str, description: &str) -> String {
    format!(
        "-- uvg revision: {revision}\n-- parent: {parent}\n-- description: {description}\n\n-- UP\nCREATE TABLE t_{revision}(id integer);\n\n-- DOWN\nDROP TABLE t_{revision};\n"
    )
}

fn migration_file(revision: &str, up_sql: &str, down_sql: Option<&str>) -> MigrationFile {
    MigrationFile {
        revision: revision.into(),
        parents: Vec::new(),
        description: "test".into(),
        path: PathBuf::from(format!("migrations/{revision}_test.sql")),
        pre_sql: String::new(),
        up_sql: up_sql.into(),
        post_sql: String::new(),
        pre_down_sql: String::new(),
        down_sql: down_sql.map(str::to_string),
        post_down_sql: String::new(),
    }
}

#[test]
fn test_revision_id_from_epoch() {
    assert_eq!(revision_id_from_epoch(1_778_700_600), "20260513_193000");
}

#[test]
fn test_slugify() {
    assert_eq!(slugify("Add users.email index"), "add-users-email-index");
    assert_eq!(slugify("///"), "migration");
}

#[test]
fn test_parse_migration_file() {
    let path = PathBuf::from("migrations/20260513_193000_initial.sql");
    let parsed = parse_migration_file(
        "-- uvg revision: 20260513_193000\n-- parent: \n-- description: initial\n\n-- UP\nCREATE TABLE users(id integer);\n\n-- DOWN\nDROP TABLE users;\n",
        path.clone(),
    )
    .unwrap();
    assert_eq!(parsed.revision, "20260513_193000");
    assert!(parsed.parents.is_empty());
    assert_eq!(parsed.description, "initial");
    assert_eq!(parsed.path, path);
    assert!(parsed.up_sql.contains("CREATE TABLE users"));
    assert!(!parsed.up_sql.contains("DROP TABLE"));
    assert_eq!(parsed.down_sql.as_deref(), Some("DROP TABLE users;"));
}

#[test]
fn test_parse_migration_file_captures_hooks_and_down_sections() {
    let parsed = parse_migration_file(
        "-- uvg revision: 20260513_193000\n\
         -- parents: 20260512_100000, 20260512_110000\n\
         -- description: hooks\n\n\
         -- PRE\n\
         INSERT INTO log VALUES ('pre');\n\n\
         -- UP\n\
         INSERT INTO log VALUES ('up');\n\n\
         -- POST\n\
         INSERT INTO log VALUES ('post');\n\n\
         -- POST DOWN\n\
         INSERT INTO log VALUES ('post down');\n\n\
         -- DOWN\n\
         INSERT INTO log VALUES ('down');\n\n\
         -- PRE DOWN\n\
         INSERT INTO log VALUES ('pre down');\n",
        PathBuf::from("migrations/20260513_193000_hooks.sql"),
    )
    .unwrap();

    assert_eq!(
        parsed.parents,
        vec!["20260512_100000".to_string(), "20260512_110000".to_string()]
    );
    assert_eq!(parsed.pre_sql, "INSERT INTO log VALUES ('pre');");
    assert_eq!(parsed.up_sql, "INSERT INTO log VALUES ('up');");
    assert_eq!(parsed.post_sql, "INSERT INTO log VALUES ('post');");
    assert_eq!(
        parsed.post_down_sql,
        "INSERT INTO log VALUES ('post down');"
    );
    assert_eq!(
        parsed.down_sql.as_deref(),
        Some("INSERT INTO log VALUES ('down');")
    );
    assert_eq!(parsed.pre_down_sql, "INSERT INTO log VALUES ('pre down');");
}

#[test]
fn test_graph_plans_linear_upgrade() {
    let dir = tmpdir("linear");
    fs::write(
        dir.join("20260513_193000_initial.sql"),
        migration("20260513_193000", "", "initial"),
    )
    .unwrap();
    fs::write(
        dir.join("20260514_084500_add_email.sql"),
        migration("20260514_084500", "20260513_193000", "add email"),
    )
    .unwrap();

    let graph = MigrationGraph::load(&dir).unwrap();
    let plan = graph
        .plan_upgrade(Some("20260513_193000"), Some("20260514_084500"))
        .unwrap();
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].revision, "20260514_084500");
    assert_eq!(
        graph.single_head().unwrap().as_deref(),
        Some("20260514_084500")
    );

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_graph_plans_dag_upgrade_through_merge_revision() {
    let dir = tmpdir("dag-upgrade");
    fs::write(
        dir.join("20260513_193000_initial.sql"),
        migration("20260513_193000", "", "initial"),
    )
    .unwrap();
    fs::write(
        dir.join("20260514_080000_branch_a.sql"),
        migration("20260514_080000", "20260513_193000", "branch a"),
    )
    .unwrap();
    fs::write(
        dir.join("20260514_090000_branch_b.sql"),
        migration("20260514_090000", "20260513_193000", "branch b"),
    )
    .unwrap();
    fs::write(
        dir.join("20260514_100000_merge.sql"),
        "-- uvg revision: 20260514_100000\n-- parents: 20260514_080000, 20260514_090000\n-- description: merge branches\n\n-- UP\n-- empty\n\n-- DOWN\n-- empty\n",
    )
    .unwrap();

    let graph = MigrationGraph::load(&dir).unwrap();
    assert_eq!(
        graph.single_head().unwrap().as_deref(),
        Some("20260514_100000")
    );
    let from_base = graph
        .plan_upgrade(None, Some("20260514_100000"))
        .unwrap()
        .into_iter()
        .map(|migration| migration.revision.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        from_base,
        vec![
            "20260513_193000",
            "20260514_080000",
            "20260514_090000",
            "20260514_100000"
        ]
    );

    let from_branch = graph
        .plan_upgrade(Some("20260514_080000"), Some("20260514_100000"))
        .unwrap()
        .into_iter()
        .map(|migration| migration.revision.as_str())
        .collect::<Vec<_>>();
    assert_eq!(from_branch, vec!["20260514_090000", "20260514_100000"]);

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_graph_plans_linear_downgrade() {
    let dir = tmpdir("linear-downgrade");
    fs::write(
        dir.join("20260513_193000_initial.sql"),
        migration("20260513_193000", "", "initial"),
    )
    .unwrap();
    fs::write(
        dir.join("20260514_084500_add_email.sql"),
        migration("20260514_084500", "20260513_193000", "add email"),
    )
    .unwrap();
    fs::write(
        dir.join("20260515_090000_add_posts.sql"),
        migration("20260515_090000", "20260514_084500", "add posts"),
    )
    .unwrap();

    let graph = MigrationGraph::load(&dir).unwrap();
    let one_step = graph
        .plan_downgrade(Some("20260515_090000"), None)
        .unwrap()
        .into_iter()
        .map(|migration| migration.revision.as_str())
        .collect::<Vec<_>>();
    assert_eq!(one_step, vec!["20260515_090000"]);

    let to_initial = graph
        .plan_downgrade(Some("20260515_090000"), Some("20260513_193000"))
        .unwrap()
        .into_iter()
        .map(|migration| migration.revision.as_str())
        .collect::<Vec<_>>();
    assert_eq!(to_initial, vec!["20260515_090000", "20260514_084500"]);

    let to_base = graph
        .plan_downgrade(Some("20260515_090000"), Some("base"))
        .unwrap()
        .into_iter()
        .map(|migration| migration.revision.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        to_base,
        vec!["20260515_090000", "20260514_084500", "20260513_193000"]
    );

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_graph_rejects_downgrade_across_unrelated_branch() {
    let dir = tmpdir("downgrade-unrelated");
    fs::write(
        dir.join("20260513_193000_initial.sql"),
        migration("20260513_193000", "", "initial"),
    )
    .unwrap();
    fs::write(
        dir.join("20260514_080000_branch_a.sql"),
        migration("20260514_080000", "20260513_193000", "branch a"),
    )
    .unwrap();
    fs::write(
        dir.join("20260514_090000_branch_b.sql"),
        migration("20260514_090000", "20260513_193000", "branch b"),
    )
    .unwrap();

    let graph = MigrationGraph::load(&dir).unwrap();
    let err = graph
        .plan_downgrade(Some("20260514_080000"), Some("20260514_090000"))
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("cannot downgrade across unrelated branches"));

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_graph_rejects_downgrade_through_merge_revision() {
    let dir = tmpdir("downgrade-merge");
    fs::write(
        dir.join("20260513_193000_initial.sql"),
        migration("20260513_193000", "", "initial"),
    )
    .unwrap();
    fs::write(
        dir.join("20260514_080000_branch_a.sql"),
        migration("20260514_080000", "20260513_193000", "branch a"),
    )
    .unwrap();
    fs::write(
        dir.join("20260514_090000_branch_b.sql"),
        migration("20260514_090000", "20260513_193000", "branch b"),
    )
    .unwrap();
    fs::write(
        dir.join("20260514_100000_merge.sql"),
        "-- uvg revision: 20260514_100000\n-- parents: 20260514_080000, 20260514_090000\n-- description: merge branches\n\n-- UP\n-- empty\n\n-- DOWN\n-- empty\n",
    )
    .unwrap();

    let graph = MigrationGraph::load(&dir).unwrap();
    let err = graph
        .plan_downgrade(Some("20260514_100000"), None)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("cannot downgrade through merge revision"));

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_graph_rejects_unknown_current_revision() {
    let dir = tmpdir("unknown-current");
    fs::write(
        dir.join("20260513_193000_initial.sql"),
        migration("20260513_193000", "", "initial"),
    )
    .unwrap();
    let graph = MigrationGraph::load(&dir).unwrap();
    let err = graph
        .plan_upgrade(Some("missing"), Some("20260513_193000"))
        .unwrap_err();
    assert!(err.to_string().contains("unknown revision"));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_require_revision_rejects_unknown_stamp_target() {
    let dir = tmpdir("unknown-stamp");
    fs::write(
        dir.join("20260513_193000_initial.sql"),
        migration("20260513_193000", "", "initial"),
    )
    .unwrap();
    let graph = MigrationGraph::load(&dir).unwrap();
    let err = graph.require_revision("missing").unwrap_err();
    assert!(err.to_string().contains("unknown migration revision"));
    assert!(err.to_string().contains("20260513_193000"));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_write_revision_file_and_meta() {
    let dir = tmpdir("write");
    let changes = vec![Change {
        table_schema: "".into(),
        table_name: Some("users".into()),
        sql: "CREATE TABLE users(id integer);".into(),
    }];
    let path = write_revision_file(
        &dir,
        "20260513_193000",
        None,
        "initial schema",
        Dialect::Postgres,
        Dialect::Postgres,
        &changes,
    )
    .unwrap();
    let body = fs::read_to_string(&path).unwrap();
    assert!(body.contains("-- uvg revision: 20260513_193000"));
    assert!(body.contains("-- UP\nCREATE TABLE users"));
    assert!(body.contains("-- DOWN\nDROP TABLE IF EXISTS users;"));

    let graph = MigrationGraph::load(&dir).unwrap();
    write_meta_file(&dir, &graph).unwrap();
    let meta = fs::read_to_string(dir.join("meta.yaml")).unwrap();
    assert!(meta.contains("head: '20260513_193000'"));
    assert!(meta.contains("description: 'initial schema'"));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_write_merge_revision_file_and_meta() {
    let dir = tmpdir("write-merge");
    fs::write(
        dir.join("20260514_080000_branch_a.sql"),
        migration("20260514_080000", "", "branch a"),
    )
    .unwrap();
    fs::write(
        dir.join("20260514_090000_branch_b.sql"),
        migration("20260514_090000", "", "branch b"),
    )
    .unwrap();
    let path = write_merge_revision_file(
        &dir,
        "20260514_100000",
        &["20260514_080000".to_string(), "20260514_090000".to_string()],
        "merge branches",
    )
    .unwrap();
    let body = fs::read_to_string(&path).unwrap();
    assert!(body.contains("-- parents: 20260514_080000, 20260514_090000"));
    assert!(body.contains("-- UP\n-- Empty merge revision"));
    assert!(body.contains("-- DOWN\n-- Merge downgrade is not automatic"));

    let graph = MigrationGraph::load(&dir).unwrap();
    write_meta_file(&dir, &graph).unwrap();
    let meta = fs::read_to_string(dir.join("meta.yaml")).unwrap();
    assert!(meta.contains("head: '20260514_100000'"));
    assert!(meta.contains("parents: ['20260514_080000', '20260514_090000']"));

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_render_down_sql_reverses_known_changes_and_marks_irreversible() {
    let changes = vec![
        Change {
            table_schema: "".into(),
            table_name: Some("users".into()),
            sql: "CREATE TABLE \"users\" (id INTEGER);".into(),
        },
        Change {
            table_schema: "".into(),
            table_name: Some("users".into()),
            sql: "ALTER TABLE \"users\" ADD COLUMN \"email\" TEXT;".into(),
        },
        Change {
            table_schema: "".into(),
            table_name: Some("legacy".into()),
            sql: "DROP TABLE IF EXISTS \"legacy\";".into(),
        },
    ];

    let down = render_down_sql(&changes, Dialect::Postgres);
    assert!(
        down.find("-- IRREVERSIBLE").unwrap()
            < down
                .find("ALTER TABLE \"users\" DROP COLUMN \"email\";")
                .unwrap()
    );
    assert!(down.contains("DROP TABLE IF EXISTS \"users\";"));
}

#[test]
fn test_reverse_change_sql_does_not_treat_add_constraint_as_add_column() {
    let down = reverse_change_sql(
        "ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id);",
        Dialect::Postgres,
    );
    assert!(down.contains("-- IRREVERSIBLE"));
    assert!(!down.contains("DROP COLUMN CONSTRAINT"));
}

#[test]
fn test_first_sql_token_handles_escaped_quoted_identifiers() {
    assert_eq!(first_sql_token("\"co\"\"l\" TEXT"), Some("\"co\"\"l\""));
    assert_eq!(first_sql_token("`co``l` TEXT"), Some("`co``l`"));
    assert_eq!(first_sql_token("[co]]l] TEXT"), Some("[co]]l]"));
}

#[test]
fn test_migration_plan_sql_orders_up_hooks() {
    let mut migration = migration_file(
        "20260513_193000",
        "INSERT INTO events VALUES ('up');",
        Some(""),
    );
    migration.pre_sql = "INSERT INTO events VALUES ('pre');".into();
    migration.post_sql = "INSERT INTO events VALUES ('post');".into();

    let sql = migration_plan_sql(&migration, MigrationDirection::Up).unwrap();

    assert!(sql.find("-- PRE").unwrap() < sql.find("-- UP").unwrap());
    assert!(sql.find("-- UP").unwrap() < sql.find("-- POST").unwrap());
    assert!(sql.contains("INSERT INTO events VALUES ('pre');"));
    assert!(sql.contains("INSERT INTO events VALUES ('up');"));
    assert!(sql.contains("INSERT INTO events VALUES ('post');"));
}

#[test]
fn test_migration_plan_sql_orders_down_hooks() {
    let mut migration = migration_file(
        "20260513_193000",
        "",
        Some("INSERT INTO events VALUES ('down');"),
    );
    migration.post_down_sql = "INSERT INTO events VALUES ('post down');".into();
    migration.pre_down_sql = "INSERT INTO events VALUES ('pre down');".into();

    let sql = migration_plan_sql(&migration, MigrationDirection::Down).unwrap();

    assert!(sql.find("-- POST DOWN").unwrap() < sql.find("-- DOWN").unwrap());
    assert!(sql.find("-- DOWN").unwrap() < sql.find("-- PRE DOWN").unwrap());
    assert!(sql.contains("INSERT INTO events VALUES ('post down');"));
    assert!(sql.contains("INSERT INTO events VALUES ('down');"));
    assert!(sql.contains("INSERT INTO events VALUES ('pre down');"));
}

#[test]
fn test_migration_down_plan_refuses_irreversible_before_hooks() {
    let mut migration = migration_file(
        "20260513_193000",
        "",
        Some("-- IRREVERSIBLE: manual rollback required"),
    );
    migration.post_down_sql = "INSERT INTO events VALUES ('post down');".into();

    let err = migration_plan_sql(&migration, MigrationDirection::Down).unwrap_err();

    assert!(err.to_string().contains("irreversible DOWN section"));
}

#[test]
fn test_format_parse_error_lines_truncates_preview() {
    let sql = format!("CREATE TABLE {} (id integer);", "x".repeat(160));
    let errors = vec![db::ParseError {
        sql,
        error: "syntax error near table name".into(),
    }];

    let report = format_parse_error_lines(&errors);

    assert!(report.contains("[1/1] CREATE TABLE"));
    assert!(report.contains("..."));
    assert!(report.contains("syntax error near table name"));
}

#[test]
fn test_graph_loads_dot_prefixed_baseline_file() {
    let dir = tmpdir("dot-baseline");
    fs::write(
        dir.join(".uvg-revision-00000000_000000_initial.sql"),
        migration("00000000_000000", "", "initial baseline"),
    )
    .unwrap();

    let graph = MigrationGraph::load(&dir).unwrap();
    assert_eq!(
        graph.single_head().unwrap().as_deref(),
        Some("00000000_000000")
    );

    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_sqlite_current_and_record_revision() {
    let dir = tmpdir("sqlite-version");
    let db_path = dir.join("target.db");
    fs::File::create(&db_path).unwrap();
    let config = ConnectionConfig::Sqlite(format!("sqlite://{}", db_path.display()));

    assert_eq!(current_revision(&config).await.unwrap(), None);
    ensure_version_table(&config).await.unwrap();
    record_revision(&config, "20260513_193000", "initial")
        .await
        .unwrap();
    assert_eq!(
        current_revision(&config).await.unwrap().as_deref(),
        Some("20260513_193000")
    );

    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_sqlite_stamp_revision_creates_version_table_without_running_up_sql() {
    let dir = tmpdir("sqlite-stamp");
    let db_path = dir.join("target.db");
    fs::File::create(&db_path).unwrap();
    let config = ConnectionConfig::Sqlite(format!("sqlite://{}", db_path.display()));
    let migration = migration_file(
        "20260513_193000",
        "CREATE TABLE users(id integer primary key);",
        Some("DROP TABLE users;"),
    );

    stamp_revision(&config, &migration).await.unwrap();
    assert_eq!(
        current_revision(&config).await.unwrap().as_deref(),
        Some("20260513_193000")
    );

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", db_path.display()))
        .await
        .unwrap();
    let users_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name = 'users'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let version_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name = 'uvg_version'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let stamped_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM uvg_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    pool.close().await;

    assert_eq!(users_count, 0, "stamp must not execute migration SQL");
    assert_eq!(version_count, 1, "stamp should create uvg_version");
    assert_eq!(stamped_rows, 1, "stamp should write one version row");

    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_sqlite_apply_migration_executes_up_sql() {
    let dir = tmpdir("sqlite-apply");
    let db_path = dir.join("target.db");
    fs::File::create(&db_path).unwrap();
    let config = ConnectionConfig::Sqlite(format!("sqlite://{}", db_path.display()));
    let migration = migration_file(
        "20260513_193000",
        "CREATE TABLE users(id integer primary key);",
        Some("DROP TABLE users;"),
    );

    apply_migration(&config, &migration).await.unwrap();

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", db_path.display()))
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name = 'users'")
        .fetch_one(&pool)
        .await
        .unwrap();
    pool.close().await;
    assert_eq!(count, 1);

    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_sqlite_apply_migration_runs_pre_up_post_in_order() {
    let dir = tmpdir("sqlite-hooks-up");
    let db_path = dir.join("target.db");
    fs::File::create(&db_path).unwrap();
    let config = ConnectionConfig::Sqlite(format!("sqlite://{}", db_path.display()));
    let mut migration = migration_file(
        "20260513_193000",
        "INSERT INTO events VALUES ('up');",
        Some(""),
    );
    migration.pre_sql = "CREATE TABLE events(step text); INSERT INTO events VALUES ('pre');".into();
    migration.post_sql = "INSERT INTO events VALUES ('post');".into();

    apply_migration(&config, &migration).await.unwrap();

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", db_path.display()))
        .await
        .unwrap();
    let rows: Vec<String> = sqlx::query_scalar("SELECT step FROM events ORDER BY rowid")
        .fetch_all(&pool)
        .await
        .unwrap();
    pool.close().await;

    assert_eq!(rows, vec!["pre", "up", "post"]);
    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_sqlite_apply_migration_reports_failed_section() {
    let dir = tmpdir("sqlite-hooks-fail");
    let db_path = dir.join("target.db");
    fs::File::create(&db_path).unwrap();
    let config = ConnectionConfig::Sqlite(format!("sqlite://{}", db_path.display()));
    let mut migration = migration_file(
        "20260513_193000",
        "CREATE TABLE events(step text);",
        Some(""),
    );
    migration.post_sql = "INSERT INTO missing_table VALUES ('post');".into();

    let err = apply_migration(&config, &migration).await.unwrap_err();
    assert!(err.to_string().contains("POST section"));
    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_sqlite_apply_down_migration_runs_post_down_down_pre_down_in_order() {
    let dir = tmpdir("sqlite-hooks-down");
    let db_path = dir.join("target.db");
    fs::File::create(&db_path).unwrap();
    let config = ConnectionConfig::Sqlite(format!("sqlite://{}", db_path.display()));
    db::execute_ddl(&config, "CREATE TABLE events(step text);", 3, |_, _, _| {})
        .await
        .unwrap();
    let mut migration = migration_file(
        "20260513_193000",
        "",
        Some("INSERT INTO events VALUES ('down');"),
    );
    migration.post_down_sql = "INSERT INTO events VALUES ('post down');".into();
    migration.pre_down_sql = "INSERT INTO events VALUES ('pre down');".into();

    apply_down_migration(&config, &migration).await.unwrap();

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", db_path.display()))
        .await
        .unwrap();
    let rows: Vec<String> = sqlx::query_scalar("SELECT step FROM events ORDER BY rowid")
        .fetch_all(&pool)
        .await
        .unwrap();
    pool.close().await;

    assert_eq!(rows, vec!["post down", "down", "pre down"]);
    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_sqlite_downgrade_drops_table_and_clears_base_revision() {
    let dir = tmpdir("sqlite-downgrade");
    let db_path = dir.join("target.db");
    fs::File::create(&db_path).unwrap();
    let config = ConnectionConfig::Sqlite(format!("sqlite://{}", db_path.display()));
    let migration = migration_file(
        "20260513_193000",
        "CREATE TABLE users(id integer primary key);",
        Some("DROP TABLE users;"),
    );

    ensure_version_table(&config).await.unwrap();
    apply_migration(&config, &migration).await.unwrap();
    record_revision(&config, &migration.revision, &migration.description)
        .await
        .unwrap();
    apply_down_migration(&config, &migration).await.unwrap();
    clear_revision(&config).await.unwrap();

    assert_eq!(current_revision(&config).await.unwrap(), None);
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", db_path.display()))
        .await
        .unwrap();
    let users_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name = 'users'")
            .fetch_one(&pool)
            .await
            .unwrap();
    pool.close().await;
    assert_eq!(users_count, 0);
    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_sqlite_irreversible_down_refuses_to_change_revision() {
    let dir = tmpdir("sqlite-irreversible-down");
    let db_path = dir.join("target.db");
    fs::File::create(&db_path).unwrap();
    let config = ConnectionConfig::Sqlite(format!("sqlite://{}", db_path.display()));
    db::execute_ddl(&config, "CREATE TABLE events(step text);", 3, |_, _, _| {})
        .await
        .unwrap();
    let mut migration = migration_file(
        "20260513_193000",
        "CREATE TABLE users(id integer primary key);",
        Some("-- IRREVERSIBLE: this migration drops data."),
    );
    migration.post_down_sql = "INSERT INTO events VALUES ('post down');".into();

    ensure_version_table(&config).await.unwrap();
    record_revision(&config, &migration.revision, &migration.description)
        .await
        .unwrap();
    let err = apply_down_migration(&config, &migration).await.unwrap_err();
    assert!(err.to_string().contains("irreversible DOWN section"));
    assert_eq!(
        current_revision(&config).await.unwrap().as_deref(),
        Some("20260513_193000")
    );
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", db_path.display()))
        .await
        .unwrap();
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(&pool)
        .await
        .unwrap();
    pool.close().await;
    assert_eq!(event_count, 0, "POST DOWN must not run after guard fails");

    fs::remove_dir_all(&dir).ok();
}
