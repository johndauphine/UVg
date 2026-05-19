use super::*;
use crate::testutil::{col, schema_pg, table};

#[test]
fn test_quote_identifier_pg() {
    assert_eq!(quote_identifier("users", Dialect::Postgres), "\"users\"");
    assert_eq!(
        quote_identifier("my\"table", Dialect::Postgres),
        "\"my\"\"table\""
    );
}

#[test]
fn test_quote_identifier_mysql() {
    assert_eq!(quote_identifier("users", Dialect::Mysql), "`users`");
}

#[test]
fn test_quote_identifier_mssql() {
    assert_eq!(quote_identifier("users", Dialect::Mssql), "[users]");
}

#[test]
fn test_qualified_table_name_mysql_source_dropped_cross_dialect() {
    // #40 — MySQL source's "schema" is the database name. When emitting
    // for a non-MySQL target, that schema doesn't exist there; drop it.
    // Same-dialect (mysql→mysql) preserves it via the existing rules.
    let result = qualified_table_name("crm_mysql", "users", Dialect::Mysql, Dialect::Postgres);
    assert_eq!(result, "\"users\"");

    let result = qualified_table_name("crm_mysql", "users", Dialect::Mysql, Dialect::Mssql);
    assert_eq!(result, "[users]");

    // Same-dialect: behavior unchanged. (mysql → mysql actually goes
    // through the suppress_for_mysql_target branch and drops anyway,
    // since the target connection already specifies the database.)
    let result = qualified_table_name("crm_mysql", "users", Dialect::Mysql, Dialect::Mysql);
    assert_eq!(result, "`users`");

    // Negative: PG source with a real (non-default) schema must keep
    // the qualification — only mysql sources are special.
    let result = qualified_table_name("warehouse", "orders", Dialect::Postgres, Dialect::Mssql);
    assert_eq!(result, "[warehouse].[orders]");
}

#[test]
fn test_translate_default_now() {
    assert_eq!(
        translate_default_function("now()", Dialect::Mysql),
        "CURRENT_TIMESTAMP"
    );
    assert_eq!(
        translate_default_function("GETDATE()", Dialect::Postgres),
        "now()"
    );
    assert_eq!(
        translate_default_function("CURRENT_TIMESTAMP", Dialect::Mssql),
        "GETDATE()"
    );
}

#[test]
fn test_translate_default_now_extended() {
    // #32 — MSSQL now-family variants previously not in the keyword list.
    // Each MUST translate to a target-dialect-idiomatic "now" function;
    // leaving them verbatim caused the apply step to fail with
    // "function getutcdate() does not exist" on PG.
    assert_eq!(
        translate_default_function("getutcdate()", Dialect::Postgres),
        "now()"
    );
    assert_eq!(
        translate_default_function("getutcdate()", Dialect::Mysql),
        "CURRENT_TIMESTAMP"
    );
    assert_eq!(
        translate_default_function("sysdatetime()", Dialect::Postgres),
        "now()"
    );
    assert_eq!(
        translate_default_function("LOCALTIMESTAMP", Dialect::Mysql),
        "CURRENT_TIMESTAMP"
    );

    // Precision-suffix stripping: CURRENT_TIMESTAMP(6) and now()-with-precision
    // should still be recognized as the now-family. (#36 caveat: the column
    // type carries the precision via CanonicalType::Timestamp; the default
    // expression becomes plain CURRENT_TIMESTAMP / GETDATE() / now().)
    assert_eq!(
        translate_default_function("CURRENT_TIMESTAMP(6)", Dialect::Mysql),
        "CURRENT_TIMESTAMP"
    );
    assert_eq!(
        translate_default_function("now(3)", Dialect::Mssql),
        "GETDATE()"
    );
}

#[test]
fn test_translate_check_predicate_mssql_brackets() {
    // MSSQL→non-MSSQL: square-bracket identifiers must become
    // double-quotes (which PG and MySQL accept).
    let mssql = "([code]=upper([code]))";
    assert_eq!(
        translate_check_predicate(mssql, Dialect::Mssql, Dialect::Postgres),
        "(\"code\"=upper(\"code\"))"
    );
    // MSSQL→MSSQL: brackets pass through.
    assert_eq!(
        translate_check_predicate(mssql, Dialect::Mssql, Dialect::Mssql),
        mssql
    );
}

#[test]
fn test_check_predicate_is_portable() {
    // Portable: simple comparison + UPPER/IS NULL/IN — works across all dialects.
    assert!(check_predicate_is_portable(
        "(salary >= 0)",
        Dialect::Postgres,
        Dialect::Mssql
    ));
    assert!(check_predicate_is_portable(
        "(code = upper(code))",
        Dialect::Postgres,
        Dialect::Mssql
    ));
    assert!(check_predicate_is_portable(
        "(active in (0,1))",
        Dialect::Postgres,
        Dialect::Mysql
    ));
    assert!(check_predicate_is_portable(
        "(end_at >= start_at OR end_at IS NULL)",
        Dialect::Postgres,
        Dialect::Mssql
    ));

    // Non-portable: PG regex operators have no MSSQL/MySQL equivalent.
    assert!(!check_predicate_is_portable(
        r"(email ~ '^[^@]+@[^@]+\.[^@]+$')",
        Dialect::Postgres,
        Dialect::Mssql
    ));
    // Non-portable: PG ARRAY[...] literals.
    assert!(!check_predicate_is_portable(
        "(customer_type = ANY(ARRAY['individual'::text, 'company'::text]))",
        Dialect::Postgres,
        Dialect::Mssql
    ));

    // Same-dialect always portable, regardless of content.
    assert!(check_predicate_is_portable(
        r"(email ~ '^[^@]+@[^@]+\.[^@]+$')",
        Dialect::Postgres,
        Dialect::Postgres
    ));

    // mysql `<col> in (0,1)` boolean-range CHECK is non-portable to
    // PG (BOOLEAN ≠ integer) and redundant elsewhere — flagged.
    assert!(!check_predicate_is_portable(
        "(`is_active` in (0,1))",
        Dialect::Mysql,
        Dialect::Postgres
    ));
    assert!(!check_predicate_is_portable(
        "(`x` in (1, 0))",
        Dialect::Mysql,
        Dialect::Mssql
    ));
    // Same-dialect mysql→mysql preserves it.
    assert!(check_predicate_is_portable(
        "(`is_active` in (0,1))",
        Dialect::Mysql,
        Dialect::Mysql
    ));
}

#[test]
fn test_strip_pg_casts_in_predicate() {
    // PG→non-PG: `::type` casts must be stripped — other dialects'
    // parsers reject the `::` syntax outright.
    assert_eq!(
        strip_pg_casts_in_predicate("(code)::text = upper((code)::text)"),
        "(code) = upper((code))"
    );
    assert_eq!(
        strip_pg_casts_in_predicate("price::numeric(10, 2) > 0"),
        "price > 0"
    );
    // No-op on inputs without casts.
    assert_eq!(strip_pg_casts_in_predicate("salary >= 0"), "salary >= 0");
    // Same-dialect (PG→PG) leaves casts intact via translate_check_predicate.
    assert_eq!(
        translate_check_predicate(
            "(code)::text = upper((code)::text)",
            Dialect::Postgres,
            Dialect::Postgres
        ),
        "(code)::text = upper((code)::text)"
    );
}

#[test]
fn test_translate_check_predicate_backticks() {
    // #35 — MySQL information_schema returns CHECK predicates with
    // backtick-quoted identifiers. Other dialects don't accept backticks;
    // translate to double-quotes when source=mysql and target!=mysql.
    let mysql_predicate = "(`is_active` in (0,1))";

    // mysql → pg: backticks become double-quotes
    assert_eq!(
        translate_check_predicate(mysql_predicate, Dialect::Mysql, Dialect::Postgres),
        "(\"is_active\" in (0,1))"
    );
    // mysql → mssql: same translation (MSSQL accepts double-quotes with QUOTED_IDENTIFIER on)
    assert_eq!(
        translate_check_predicate(mysql_predicate, Dialect::Mysql, Dialect::Mssql),
        "(\"is_active\" in (0,1))"
    );
    // mysql → mysql: pass through
    assert_eq!(
        translate_check_predicate(mysql_predicate, Dialect::Mysql, Dialect::Mysql),
        mysql_predicate
    );
    // pg → mssql: no transformation needed (no backticks in pg predicates)
    assert_eq!(
        translate_check_predicate("(salary >= 0)", Dialect::Postgres, Dialect::Mssql),
        "(salary >= 0)"
    );
}

#[test]
fn test_reattach_now_family_precision() {
    // #36 — MySQL DATETIME(N) requires DEFAULT CURRENT_TIMESTAMP(N).
    // After translate_default_function strips precision, this re-attach
    // step puts it back when emitting against a mysql target with a
    // precision-bearing column type.
    assert_eq!(
        reattach_now_family_precision("CURRENT_TIMESTAMP", 6),
        "CURRENT_TIMESTAMP(6)"
    );
    assert_eq!(reattach_now_family_precision("now()", 3), "now(3)");
    assert_eq!(reattach_now_family_precision("GETDATE()", 6), "GETDATE(6)");
    // Idempotent: already-precise expression unchanged.
    assert_eq!(
        reattach_now_family_precision("CURRENT_TIMESTAMP(6)", 6),
        "CURRENT_TIMESTAMP(6)"
    );
    // Non-now-family: passes through (no inappropriate precision append).
    assert_eq!(reattach_now_family_precision("'pending'", 6), "'pending'");
    assert_eq!(reattach_now_family_precision("42", 6), "42");
    // CURRENT_DATE does NOT accept FSP in MySQL — it's a date-only
    // function, no fractional-seconds component. We must NOT append
    // precision, even though it's a "now"-family keyword textually.
    // Per Copilot review on PR #37.
    assert_eq!(
        reattach_now_family_precision("CURRENT_DATE", 6),
        "CURRENT_DATE"
    );
}

#[test]
fn test_strip_precision_suffix() {
    assert_eq!(strip_precision_suffix("now()"), "now()");
    assert_eq!(
        strip_precision_suffix("current_timestamp(6)"),
        "current_timestamp()"
    );
    assert_eq!(strip_precision_suffix("getdate(3)"), "getdate()");
    // Non-digit content stays as-is (defensive: don't strip user functions).
    assert_eq!(strip_precision_suffix("foo(bar)"), "foo(bar)");
}

#[test]
fn test_translate_default_uuid() {
    assert_eq!(
        translate_default_function("gen_random_uuid()", Dialect::Mysql),
        "(UUID())"
    );
    assert_eq!(
        translate_default_function("NEWID()", Dialect::Postgres),
        "gen_random_uuid()"
    );
}

#[test]
fn test_json_default_dropped_on_mysql_target() {
    // #34 — MySQL <8.0.13 rejects DEFAULT on JSON/TEXT/BLOB. uvg should
    // drop the default when targeting MySQL on these types rather than
    // emit a literal that fails at apply time with ERROR 1101.
    let mut json_col = col("settings").udt("jsonb").build();
    json_col.column_default = Some("'{}'::jsonb".to_string());
    let mut text_col = col("notes").udt("text").build();
    text_col.column_default = Some("'pending'::text".to_string());
    let mut blob_col = col("payload").udt("bytea").build();
    blob_col.column_default = Some("'\\x00'::bytea".to_string());
    let t = table("docs")
        .schema("")
        .column(col("id").build())
        .column(json_col)
        .column(text_col)
        .column(blob_col)
        .pk("pk_docs", &["id"])
        .build();

    let options = DdlOptions {
        target_dialect: Dialect::Mysql,
        split_tables: false,
        apply: false,
        noindexes: false,
        noconstraints: false,
        nocomments: false,
    };
    let ddl = generate_create_table(&t, Dialect::Postgres, Dialect::Mysql, &options);

    // The columns themselves are still emitted; only the DEFAULT is dropped.
    assert!(ddl.contains("`settings` JSON"), "DDL was: {ddl}");
    assert!(ddl.contains("`notes` TEXT"), "DDL was: {ddl}");
    assert!(ddl.contains("`payload` BLOB"), "DDL was: {ddl}");
    // No DEFAULT clause should appear for any of these columns.
    assert!(
        !ddl.contains("`settings` JSON NOT NULL DEFAULT"),
        "default leaked: {ddl}"
    );
    assert!(
        !ddl.contains("`notes` TEXT NOT NULL DEFAULT"),
        "default leaked: {ddl}"
    );
    assert!(
        !ddl.contains("`payload` BLOB NOT NULL DEFAULT"),
        "default leaked: {ddl}"
    );
}

#[test]
fn test_json_default_kept_on_pg_target() {
    // Negative case for #34 — only MySQL targets drop the default. PG
    // accepts JSON defaults so the default must pass through normally.
    let mut json_col = col("settings").udt("jsonb").build();
    json_col.column_default = Some("'{}'::jsonb".to_string());
    let t = table("docs")
        .schema("")
        .column(col("id").build())
        .column(json_col)
        .pk("pk_docs", &["id"])
        .build();
    let options = DdlOptions {
        target_dialect: Dialect::Postgres,
        split_tables: false,
        apply: false,
        noindexes: false,
        noconstraints: false,
        nocomments: false,
    };
    let ddl = generate_create_table(&t, Dialect::Postgres, Dialect::Postgres, &options);
    assert!(
        ddl.contains("DEFAULT '{}'"),
        "PG should preserve JSON default: {ddl}"
    );
}

#[test]
fn test_generate_create_table_pg_to_mysql() {
    let t = table("users")
        .schema("")
        .column(col("id").build())
        .column(col("name").udt("varchar").max_length(100).build())
        .column(col("bio").udt("text").nullable().build())
        .pk("pk_users", &["id"])
        .build();
    let options = DdlOptions {
        target_dialect: Dialect::Mysql,
        split_tables: false,
        apply: false,
        noindexes: false,
        noconstraints: false,
        nocomments: false,
    };
    let ddl = generate_create_table(&t, Dialect::Postgres, Dialect::Mysql, &options);
    assert!(ddl.contains("CREATE TABLE `users`"), "DDL was: {ddl}");
    assert!(ddl.contains("`id` INT NOT NULL"), "DDL was: {ddl}");
    assert!(
        ddl.contains("`name` VARCHAR(100) NOT NULL"),
        "DDL was: {ddl}"
    );
    assert!(ddl.contains("`bio` TEXT"), "DDL was: {ddl}");
    assert!(ddl.contains("PRIMARY KEY (`id`)"), "DDL was: {ddl}");
}

#[test]
fn test_generate_create_table_pg_to_pg() {
    let t = table("items")
        .column(col("id").build())
        .column(col("price").udt("numeric").precision(10, 2).build())
        .pk("pk_items", &["id"])
        .build();
    let options = DdlOptions {
        target_dialect: Dialect::Postgres,
        split_tables: false,
        apply: false,
        noindexes: false,
        noconstraints: false,
        nocomments: false,
    };
    let ddl = generate_create_table(&t, Dialect::Postgres, Dialect::Postgres, &options);
    assert!(ddl.contains("\"id\" INTEGER NOT NULL"));
    assert!(ddl.contains("\"price\" NUMERIC(10, 2) NOT NULL"));
}

#[test]
fn test_full_generate_single() {
    let schema = schema_pg(vec![table("users")
        .schema("")
        .column(col("id").build())
        .column(col("name").udt("varchar").max_length(100).build())
        .pk("pk_users", &["id"])
        .build()]);
    let options = DdlOptions {
        target_dialect: Dialect::Mysql,
        split_tables: false,
        apply: false,
        noindexes: false,
        noconstraints: false,
        nocomments: false,
    };
    let gen = DdlGenerator;
    match gen.generate(&schema, None, &options) {
        DdlOutput::Single(ddl) => {
            assert!(ddl.contains("CREATE TABLE `users`"));
            assert!(ddl.contains("Source: postgres, Target: mysql"));
        }
        DdlOutput::Split(_) => panic!("Expected single output"),
    }
}

#[test]
fn test_full_generate_split() {
    let schema = schema_pg(vec![
        table("users")
            .column(col("id").build())
            .pk("pk_users", &["id"])
            .build(),
        table("posts")
            .column(col("id").build())
            .pk("pk_posts", &["id"])
            .build(),
    ]);
    let options = DdlOptions {
        target_dialect: Dialect::Postgres,
        split_tables: true,
        apply: false,
        noindexes: false,
        noconstraints: false,
        nocomments: false,
    };
    match DdlGenerator.generate(&schema, None, &options) {
        DdlOutput::Split(files) => {
            let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
            assert!(names.contains(&"users.sql"));
            assert!(names.contains(&"posts.sql"));
            assert!(names.contains(&"_order.txt"));
        }
        DdlOutput::Single(_) => panic!("Expected split output"),
    }
}

#[test]
fn test_boolean_default_mssql_to_pg() {
    assert_eq!(
        format_ddl_default_typed("((1))", Dialect::Mssql, Dialect::Postgres, true),
        "true"
    );
    assert_eq!(
        format_ddl_default_typed("((0))", Dialect::Mssql, Dialect::Postgres, true),
        "false"
    );
}

#[test]
fn test_integer_default_not_converted_to_boolean() {
    assert_eq!(
        format_ddl_default_typed("((0))", Dialect::Mssql, Dialect::Postgres, false),
        "0"
    );
    assert_eq!(
        format_ddl_default_typed("((1))", Dialect::Mssql, Dialect::Postgres, false),
        "1"
    );
}

#[test]
fn test_boolean_default_pg_to_mysql() {
    assert_eq!(
        format_ddl_default_typed("true", Dialect::Postgres, Dialect::Mysql, true),
        "1"
    );
    assert_eq!(
        format_ddl_default_typed("false", Dialect::Postgres, Dialect::Mysql, true),
        "0"
    );
}

#[test]
fn test_boolean_default_pg_to_mssql() {
    assert_eq!(
        format_ddl_default_typed("true", Dialect::Postgres, Dialect::Mssql, true),
        "1"
    );
    assert_eq!(
        format_ddl_default_typed("false", Dialect::Postgres, Dialect::Mssql, true),
        "0"
    );
}

#[test]
fn test_ensure_default_quoting() {
    assert_eq!(ensure_default_quoting("member"), "'member'");
    assert_eq!(ensure_default_quoting("active"), "'active'");
    assert_eq!(ensure_default_quoting("'member'"), "'member'");
    assert_eq!(ensure_default_quoting("0"), "0");
    assert_eq!(ensure_default_quoting("3.14"), "3.14");
    assert_eq!(ensure_default_quoting("NULL"), "NULL");
    assert_eq!(ensure_default_quoting("true"), "true");
    assert_eq!(ensure_default_quoting("false"), "false");
    assert_eq!(ensure_default_quoting("now()"), "now()");
    assert_eq!(
        ensure_default_quoting("CURRENT_TIMESTAMP"),
        "CURRENT_TIMESTAMP"
    );
    assert_eq!(ensure_default_quoting("it's"), "'it''s'");
}
