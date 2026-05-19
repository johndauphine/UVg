#!/usr/bin/env bash
# CRM 9-pair matrix runner — drives uvg through every (source, target)
# permutation of {mssql, postgres, mysql} and reports table/FK/CHECK/index
# counts plus apply success. In strict mode, also verifies MSSQL→PostgreSQL
# column metadata fidelity (length, precision/scale, nullability, identity,
# temporal TZ class, and default presence).
#
# Run from the repo root or `testdata/crm/`. Expects:
#   - Three Docker containers reachable from this host:
#       mssql-test  port 1433
#       pg-test     port 5432
#       mysql-test  port 3306
#     (Override with $MSSQL_CONTAINER / $PG_CONTAINER / $MYSQL_CONTAINER.)
#   - Source databases pre-loaded (see this directory's README.md).
#   - A `uvg` binary available at $UVG (default: target/release/uvg
#     relative to the script's repo root, falling back to `uvg` on PATH).
#
# Output: per-pair line + a final summary table written to
# /tmp/uvg-matrix/results.tsv. Per-pair logs, generated DDL, and optional
# column verification logs also land under /tmp/uvg-matrix/. Default exit
# behavior stays permissive: exit 0 if at least one pair succeeded. Flags
# below tighten that behavior.
#
# Flags:
#   --strict          Exit non-zero if ANY pair status is not "OK", and run
#                     the MSSQL→PostgreSQL column verifier. Used by CI to
#                     gate merges; local-dev default stays permissive so a
#                     one-off probe of a single failing pair doesn't abort
#                     the whole suite.
#   --verify-columns  Run the same MSSQL→PostgreSQL column verifier without
#                     enabling strict status handling for all 9 pairs.

set -uo pipefail

STRICT=0
VERIFY_COLUMNS=0
for arg in "$@"; do
  case "$arg" in
    --strict) STRICT=1 ;;
    --verify-columns) VERIFY_COLUMNS=1 ;;
    -h|--help)
      # Print the leading header comment block (everything from line 2
      # up to the first non-`#` line). Avoids hard-coding line numbers
      # that drift as the script evolves, and never leaks code lines
      # like `set -uo pipefail` into --help output.
      awk 'NR==1{next} /^[[:space:]]*#/ {sub(/^# ?/, ""); print; next} {exit}' "$0"
      exit 0
      ;;
    *)
      echo "error: unknown argument '$arg' (try --help)" >&2
      exit 2
      ;;
  esac
done

[[ $STRICT -eq 1 ]] && VERIFY_COLUMNS=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Locate the uvg binary. Prefer an explicit override, then a release
# build inside the repo, then $PATH.
UVG="${UVG:-}"
if [[ -z "$UVG" ]]; then
  if [[ -x "$REPO_ROOT/target/release/uvg" ]]; then
    UVG="$REPO_ROOT/target/release/uvg"
  elif command -v uvg >/dev/null 2>&1; then
    UVG="$(command -v uvg)"
  else
    echo "error: uvg binary not found. Build with 'cargo build --release' or set \$UVG." >&2
    exit 1
  fi
fi
echo "Using uvg at: $UVG"

# Container overrides — defaults match the repo convention.
MSSQL_CONTAINER="${MSSQL_CONTAINER:-mssql-test}"
PG_CONTAINER="${PG_CONTAINER:-pg-test}"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-mysql-test}"

# MSSQL clients on Go-based stacks (e.g. go-sqlcmd) refuse the
# self-signed certs that ship with azure-sql-edge. Setting GODEBUG to
# accept negative serials keeps the host-side sqlcmd working — no
# effect on `uvg` itself, which uses tiberius via tokio-rustls.
export GODEBUG=x509negativeserial=1

SOURCES=(mssql postgres mysql)
TARGETS=(mssql postgres mysql)

mkdir -p /tmp/uvg-matrix
echo -e "src\ttgt\twall\ttables\tFKs\tCHECKs\tindexes\tstatus" > /tmp/uvg-matrix/results.tsv
VERIFY_COLUMNS_RESULTS=/tmp/uvg-matrix/verify_columns.tsv
if [[ $VERIFY_COLUMNS -eq 1 ]]; then
  echo -e "pair\tstatus\tlog" > "$VERIFY_COLUMNS_RESULTS"
fi

src_url() {
  case "$1" in
    mssql)    echo "mssql://sa:TestPass2024@localhost:1433/CRM_MSSQL" ;;
    postgres) echo "postgresql://postgres:TestPass2024@localhost:5432/crm_pg" ;;
    mysql)    echo "mysql://root:TestPass2024@localhost:3306/crm_mysql" ;;
  esac
}

src_schema() {
  case "$1" in
    mssql)    echo "dbo" ;;
    postgres) echo "public" ;;
    mysql)    echo "crm_mysql" ;;
  esac
}

create_target_db() {
  local tgt=$1 tgt_db=$2
  case "$tgt" in
    mssql)
      sqlcmd -S localhost,1433 -U sa -P TestPass2024 -C -Q \
        "IF EXISTS (SELECT 1 FROM sys.databases WHERE name='$tgt_db') DROP DATABASE $tgt_db; CREATE DATABASE $tgt_db;" >/dev/null
      ;;
    postgres)
      PGPASSWORD=TestPass2024 docker exec "$PG_CONTAINER" psql -U postgres \
        -c "DROP DATABASE IF EXISTS $tgt_db" \
        -c "CREATE DATABASE $tgt_db" >/dev/null 2>&1
      ;;
    mysql)
      docker exec "$MYSQL_CONTAINER" mysql -uroot -pTestPass2024 \
        -e "DROP DATABASE IF EXISTS $tgt_db; CREATE DATABASE $tgt_db" 2>/dev/null
      ;;
  esac
}

# Apply the generated DDL file to the target. Returns the pipeline's
# exit code; the matrix scoring relies on whether any error lines
# appeared in the tool's stderr.
apply_ddl() {
  local tgt=$1 tgt_db=$2 ddl_file=$3
  case "$tgt" in
    mssql)
      sqlcmd -S localhost,1433 -U sa -P TestPass2024 -C -d "$tgt_db" -i "$ddl_file" 2>&1 \
        | grep -E "(Msg [0-9]+|Error)" | head -5
      return ${PIPESTATUS[0]}
      ;;
    postgres)
      PGPASSWORD=TestPass2024 docker exec -i "$PG_CONTAINER" psql -U postgres -d "$tgt_db" -v ON_ERROR_STOP=0 -q \
        < "$ddl_file" 2>&1 | grep -E "ERROR" | head -5
      return ${PIPESTATUS[0]}
      ;;
    mysql)
      docker exec -i "$MYSQL_CONTAINER" mysql -uroot -pTestPass2024 "$tgt_db" \
        < "$ddl_file" 2>&1 | grep -iE "ERROR" | head -5
      return ${PIPESTATUS[0]}
      ;;
  esac
}

# Count tables / FKs / CHECKs / indexes on the target. The same shape
# verify_columns.sh applies — keeps the matrix and harness aligned.
count_target() {
  local tgt=$1 tgt_db=$2 schema
  case "$tgt" in
    mssql)
      schema=dbo
      sqlcmd -S localhost,1433 -U sa -P TestPass2024 -C -d "$tgt_db" -h-1 -W -Q \
        "SET NOCOUNT ON; SELECT
          (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='$schema'),
          (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_TYPE='FOREIGN KEY' AND CONSTRAINT_SCHEMA='$schema'),
          (SELECT COUNT(*) FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='$schema'),
          (SELECT COUNT(*) FROM sys.indexes i JOIN sys.objects o ON i.object_id=o.object_id WHERE o.type='U' AND i.is_primary_key=0 AND i.type>0)" \
        2>/dev/null | head -1 | awk '{print $1"\t"$2"\t"$3"\t"$4}'
      ;;
    postgres)
      PGPASSWORD=TestPass2024 docker exec "$PG_CONTAINER" psql -U postgres -d "$tgt_db" -At -F $'\t' -c \
        "SELECT
          (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'),
          (SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema='public' AND constraint_type='FOREIGN KEY'),
          (SELECT COUNT(*) FROM pg_constraint c JOIN pg_namespace n ON c.connamespace=n.oid WHERE n.nspname='public' AND c.contype='c'),
          (SELECT COUNT(*) FROM pg_indexes WHERE schemaname='public' AND indexname NOT LIKE '%_pkey')" 2>/dev/null
      ;;
    mysql)
      docker exec "$MYSQL_CONTAINER" mysql -uroot -pTestPass2024 -N -B -e \
        "SELECT
          (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$tgt_db' AND table_type='BASE TABLE'),
          (SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema='$tgt_db' AND constraint_type='FOREIGN KEY'),
          (SELECT COUNT(*) FROM information_schema.check_constraints WHERE constraint_schema='$tgt_db'),
          (SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics s WHERE s.table_schema='$tgt_db' AND s.index_name<>'PRIMARY')" \
        2>/dev/null | head -1
      ;;
  esac
}

run_column_verifier() {
  local pair=$1 tgt_db=$2 column_log=$3

  echo "  verify_columns: checking MSSQL→PostgreSQL column metadata"
  if MSSQL_CONTAINER="$MSSQL_CONTAINER" PG_CONTAINER="$PG_CONTAINER" \
      "$SCRIPT_DIR/verify_columns.sh" CRM_MSSQL dbo "$tgt_db" public > "$column_log" 2>&1; then
    echo -e "${pair}\tOK\t${column_log}" >> "$VERIFY_COLUMNS_RESULTS"
    echo "  verify_columns: OK (log: $column_log)"
    ((column_ok_count++))
  else
    echo -e "${pair}\tFAIL\t${column_log}" >> "$VERIFY_COLUMNS_RESULTS"
    echo "  verify_columns: FAIL (log: $column_log)"
    tail -20 "$column_log" | sed 's/^/    /'
    ((column_fail_count++))
  fi
}

skip_column_verifier() {
  local pair=$1 status=$2 column_log=$3

  {
    echo "verify_columns: skipped"
    echo "pair=${pair}"
    echo "reason=matrix status ${status}"
  } > "$column_log"
  echo -e "${pair}\tSKIP_${status}\t${column_log}" >> "$VERIFY_COLUMNS_RESULTS"
  echo "  verify_columns: SKIP (${status}; log: $column_log)"
  ((column_fail_count++))
}

ok_count=0
fail_count=0
column_ok_count=0
column_fail_count=0
for src in "${SOURCES[@]}"; do
  for tgt in "${TARGETS[@]}"; do
    pair="${src}_to_${tgt}"
    tgt_db="uvg_matrix_${pair}"
    ddl_file=/tmp/uvg-matrix/${pair}.sql
    log_file=/tmp/uvg-matrix/${pair}.log

    echo "=== ${src} -> ${tgt} ==="
    create_target_db "$tgt" "$tgt_db"

    src_args=()
    [[ "$src" == "mssql" ]] && src_args+=("--trust-cert")
    src_args+=("$(src_url $src)")
    src_args+=("--schemas" "$(src_schema $src)")
    src_args+=("--generator" "ddl" "--target-dialect" "$tgt" "--outfile" "$ddl_file")

    start=$(date +%s)
    "$UVG" "${src_args[@]}" > "$log_file" 2>&1
    if [[ $? -ne 0 ]]; then
      wall=$(( $(date +%s) - start ))
      echo -e "${src}\t${tgt}\t${wall}s\t-\t-\t-\t-\tGEN_FAIL" >> /tmp/uvg-matrix/results.tsv
      echo "  GEN_FAIL ${wall}s"
      if [[ $VERIFY_COLUMNS -eq 1 && "$pair" == "mssql_to_postgres" ]]; then
        skip_column_verifier "$pair" "GEN_FAIL" "/tmp/uvg-matrix/${pair}.columns.log"
      fi
      ((fail_count++))
      continue
    fi

    apply_errors=$(apply_ddl "$tgt" "$tgt_db" "$ddl_file" 2>&1)
    wall=$(( $(date +%s) - start ))

    counts=$(count_target "$tgt" "$tgt_db")
    if [[ -z "$apply_errors" ]]; then
      status="OK"
      ((ok_count++))
    else
      status="APPLY_FAIL"
      ((fail_count++))
    fi
    echo -e "${src}\t${tgt}\t${wall}s\t${counts}\t${status}" >> /tmp/uvg-matrix/results.tsv
    echo -e "  ${counts}\t${wall}s\t${status}"
    [[ -n "$apply_errors" ]] && echo "  errors: $apply_errors" | head -3
    if [[ $VERIFY_COLUMNS -eq 1 && "$pair" == "mssql_to_postgres" ]]; then
      column_log=/tmp/uvg-matrix/${pair}.columns.log
      if [[ "$status" == "OK" ]]; then
        run_column_verifier "$pair" "$tgt_db" "$column_log"
      else
        skip_column_verifier "$pair" "$status" "$column_log"
      fi
    fi
  done
done

echo ""
echo "=== RESULTS ==="
column -t -s $'\t' /tmp/uvg-matrix/results.tsv
echo ""
echo "Summary: ${ok_count}/9 OK, ${fail_count}/9 failed"
if [[ $VERIFY_COLUMNS -eq 1 ]]; then
  echo "Column verification: ${column_ok_count}/1 OK, ${column_fail_count}/1 failed"
  echo "Column verification results: $VERIFY_COLUMNS_RESULTS"
fi

# --strict (CI): fail if any pair didn't reach OK. Default (local dev):
# fail only if nothing worked, so a single broken pair while you're
# iterating doesn't kill the whole suite.
if [[ $STRICT -eq 1 ]]; then
  [[ $fail_count -gt 0 || $column_fail_count -gt 0 ]] && exit 1
elif [[ $VERIFY_COLUMNS -eq 1 ]]; then
  [[ $ok_count -eq 0 || $column_fail_count -gt 0 ]] && exit 1
else
  [[ $ok_count -eq 0 ]] && exit 1
fi
exit 0
