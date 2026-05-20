# Operations and Security

This page describes how UVG handles connection details, credentials,
generated artifacts, TLS, privileges, and production workflows.

UVG reads database catalog metadata and emits SQLAlchemy models or DDL. It
does not scan application table data, but catalog metadata can still be
sensitive: object names, defaults, check constraints, indexes, and database
comments can appear in generated files and error output.

## Credential Handling

UVG accepts SQLAlchemy-style connection URLs on the command line, in the TUI,
and through profiles. Treat those URLs as secrets when they contain usernames,
passwords, tokens, or private-key paths.

- Command-line URLs may be captured by shell history, process inspection, job
  logs, terminal scrollback, or wrapper scripts before UVG can redact anything.
- The interactive TUI displays the source and target URL fields as typed.
  Avoid screen sharing or terminal recording when those fields contain
  credentials.
- Profiles can keep long URLs out of shell history, but profile files are still
  secrets. Store them outside the repo, restrict file permissions, and do not
  commit them.
- Prefer dedicated, least-privilege database accounts. Use short-lived
  credentials where your environment supports them.

Before UVG prints a connection URL in apply or stamp status messages, it uses a
best-effort redactor:

- URL userinfo is stripped: `postgres://alice:secret@db/app` becomes
  `postgres://***@db/app`.
- Sensitive query parameters are masked when the URL is parseable. The current
  mask list includes `password`, `pass`, `pwd`, `token`, `access_token`,
  `auth_token`, `secret`, `client_secret`, `sslkey`, `ssl-key`, and `ssl_key`.
- Non-sensitive query parameters, paths, and fragments are preserved so the
  target label remains useful.
- Unparseable URL strings are returned unchanged. UVG uses this mainly for
  SQLite relative forms, which do not carry network credentials.

Redaction only applies to UVG's own URL labels. It does not rewrite shell
history, process listings, profile files, database driver diagnostics, or SQL
that the database itself returns.

## Logs, Errors, and Generated Files

UVG's tracing logs describe phases such as connection, introspection, and file
writes. They do not intentionally include source or target URLs. Apply and stamp
messages redact the target URL before printing it.

Failure messages are intentionally operational:

- `--apply` failures include the redacted target label, database error, and the
  failed SQL statement.
- Versioned migration failures include the migration revision, section, file
  path, statement number, database error, and failed SQL. Earlier statements in
  the same migration may already have been applied; `uvg_version` is changed
  only after the migration SQL succeeds.
- Parse-check failures include a statement preview and the database error.

Generated SQL and Python can include sensitive catalog metadata:

- Per-table `--out-dir` SQL files and manifests record UVG version, timestamp,
  run id, table, source dialect, and target dialect. They do not record source
  or target URLs.
- The generated SQL header is flattened so embedded control characters in
  table names or run names cannot break out of SQL comments.
- Table and column comments from PostgreSQL, MySQL, and MSSQL are copied into
  generated output unless `--options nocomments` is set.
- Defaults, check constraints, object names, and index names are emitted as
  schema metadata. Do not put secrets in schema comments or identifiers if the
  generated artifacts will leave a trusted environment.

For CI, use disposable database credentials for test containers. If a workflow
must run UVG against a protected database, pass credentials through the CI
secret store, avoid echoing full URLs, avoid uploading generated artifacts that
contain sensitive schema metadata, and keep logs private.

## TLS and Certificate Behavior

UVG delegates PostgreSQL and MySQL connections to SQLx with rustls, and MSSQL
connections to Tiberius with rustls.

| Engine | Default | Production guidance |
| --- | --- | --- |
| PostgreSQL | SQLx defaults to `sslmode=prefer`: try TLS first, then fall back to plaintext if the server does not support TLS. The `tls-rustls` feature uses the webpki root set unless a root certificate is supplied. | Use `sslmode=require` to require encryption. Use `sslmode=verify-ca` or `sslmode=verify-full` when certificate validation matters; `verify-full` also checks the hostname. Supply `sslrootcert` or `PGSSLROOTCERT` for private CAs. `sslcert` and `sslkey` are supported for client certificates. |
| MySQL / MariaDB | SQLx defaults to `ssl-mode=PREFERRED`: use TLS when available and fall back to plaintext. UVG appends `charset=utf8mb4` unless a charset is already present. | Use `ssl-mode=required` to require encryption. Use `ssl-mode=verify_ca` or `ssl-mode=verify_identity` to validate the server certificate; `verify_identity` also checks the hostname. Supply `ssl-ca` for private CAs and `ssl-cert`/`ssl-key` for client certificates. |
| MSSQL | UVG sets Tiberius encryption to `Required` for every MSSQL connection. By default the server certificate is validated against the platform trust store used by Tiberius/rustls. | Use a certificate chain trusted by the host. `--trust-cert` disables server certificate validation and should be limited to local development or disposable CI containers. UVG currently exposes `--trust-cert`, not a custom MSSQL CA path flag. |
| SQLite | Local file or in-memory database; no network TLS. | Protect the database file and its directory permissions. |

TLS settings apply to introspection, diffing, parse checks, apply, and versioned
migration commands because all of those paths use the same connection parsing
and database clients.

## Required Database Privileges

Use separate accounts for read-only introspection and write-capable apply
workflows whenever possible.

| Workflow | Required privileges |
| --- | --- |
| Introspection and model generation | Connect to the database and read catalog metadata for the selected schemas/tables. PostgreSQL uses `information_schema` and `pg_catalog`; grant `CONNECT`, schema `USAGE`, and privileges that make the target objects visible. MySQL uses `information_schema`; grant visibility to the target database objects, commonly `SELECT` and `SHOW VIEW` where views are included. MSSQL uses `INFORMATION_SCHEMA` and `sys.*`; grant `CONNECT` and `VIEW DEFINITION` for reliable metadata visibility. SQLite requires read access to the file. |
| Diff generation without apply | Introspection privileges on both source and target. No DDL is executed. |
| Parse checks | PostgreSQL runs statements inside a rolled-back transaction with savepoints, so the role still needs the DDL privileges those statements require. MSSQL uses `SET PARSEONLY ON`, which catches syntax errors but defers name resolution to execution. MySQL and SQLite skip parse checks because they do not expose a safe parse-only DDL mode for this use. |
| `--apply`, TUI apply, `upgrade`, and `downgrade` | The DDL privileges required by the emitted statements: typically `CREATE`, `ALTER`, `DROP`, index creation, foreign-key/reference permissions, and comment privileges if comments are emitted. Versioned migrations also need to create, read, update, and delete rows in `uvg_version`. SQLite requires write access to the database file and directory. |
| `current`, `history <target-url>`, and `stamp` | `current` and target-aware `history` read `uvg_version`. `stamp` creates or updates `uvg_version` without running migration SQL and should only be used after independently verifying the schema already matches the requested revision. |

## Safe Production Workflows

Use UVG as a review-first tool in production:

1. Generate DDL without `--apply`; write it to a file or `--out-dir`.
2. Review the SQL in source control. Use `--options nocomments` if database
   comments may contain sensitive text.
3. Test the migration against a staging database or recent production clone.
4. Take backups or snapshots appropriate for the engine and blast radius.
5. Require TLS with certificate validation for network databases.
6. Run with a dedicated migration role during an approved maintenance window.
7. Use one migration runner per target database; avoid concurrent UVG apply or
   migration commands against the same target.
8. If a migration fails, read the failed statement, verify what already
   applied, repair the target manually if needed, and rerun only when the
   migration is safe and idempotent. Use `uvg stamp` only after verifying the
   target schema already matches the revision being recorded.

Remember that apply is statement-by-statement, not one cross-dialect
transaction. PostgreSQL parse checks reduce risk but still require privileges
and are not a substitute for review and staging. MySQL and SQLite skip parse
checks; MSSQL parse checks catch syntax but not missing objects or other
catalog-level failures.
