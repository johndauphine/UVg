/// Integration tests require a live database.
/// Set the appropriate env var to run these tests:
///   DATABASE_URL=postgresql://user:pass@localhost/testdb cargo test --test introspect -- --ignored
///   MYSQL_URL=mysql://user:pass@localhost/testdb cargo test --test introspect -- --ignored
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
}
