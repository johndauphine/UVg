#!/usr/bin/env bash
# verify_columns.sh — column-level metadata equivalence check between an MSSQL
# source and PostgreSQL target after a CRM matrix run. Implements pass criteria
# 1–6 from CLAUDE.md "Cross-engine coverage status." Criterion 7 (computed-
# column presence + storage class) is a TODO — needs cross-dialect
# expression normalization. Exits non-zero with a diff report on any mismatch.
#
# Usage:
#   ./verify_columns.sh <mssql_db> <mssql_schema> <pg_db> <pg_schema> [table_filter]
#
# Example:
#   ./verify_columns.sh CRM_MSSQL dbo uvg_matrix_mssql_to_postgres public Companies
#
# Assumes docker containers `mssql-test` and `pg-test` with default
# CRM-fixture credentials. Tweak the password / host args at the top if not.

set -euo pipefail

MSSQL_DB="${1:?missing mssql db}"
MSSQL_SCHEMA="${2:?missing mssql schema}"
PG_DB="${3:?missing pg db}"
PG_SCHEMA="${4:?missing pg schema}"
TABLE_FILTER="${5:-%}"   # SQL LIKE pattern; default = all tables

MSSQL_CONTAINER="${MSSQL_CONTAINER:-mssql-test}"
MSSQL_USER="${MSSQL_USER:-sa}"
MSSQL_PASS="${MSSQL_PASS:-TestPass2024}"

PG_CONTAINER="${PG_CONTAINER:-pg-test}"
PG_USER="${PG_USER:-postgres}"
PG_PASS="${PG_PASS:-TestPass2024}"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

MSSQL_SQLCMD=""
for p in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do
  if docker exec "$MSSQL_CONTAINER" test -x "$p" 2>/dev/null; then
    MSSQL_SQLCMD="$p"
    break
  fi
done
if [[ -z "$MSSQL_SQLCMD" ]]; then
  echo "error: sqlcmd not found inside container '$MSSQL_CONTAINER' at either" \
    "/opt/mssql-tools18/bin/sqlcmd or /opt/mssql-tools/bin/sqlcmd" >&2
  exit 1
fi

# --- Source extraction --------------------------------------------------------
# Pull the metadata fields we'll compare. Lower-case the table/column names so
# the join across dialects works (PG folds to lowercase; we normalize MSSQL to
# match).
#
# tz: TZ-awareness CLASS rather than the source-specific type name, so we can
# compare across dialects. Only column-default presence is captured (Y/N), not
# the expression itself — full default-expression equivalence requires a
# dialect-aware normalizer (TODO; see CLAUDE.md criterion 6).
docker exec "$MSSQL_CONTAINER" "$MSSQL_SQLCMD" \
  -S localhost -U "$MSSQL_USER" -P "$MSSQL_PASS" -C -d "$MSSQL_DB" -h-1 -W -s '|' -Q "
SET NOCOUNT ON;
SELECT
  LOWER(c.TABLE_NAME) AS t,
  LOWER(c.COLUMN_NAME) AS c,
  LOWER(c.DATA_TYPE) AS dt,
  -- Normalize MSSQL's MAX sentinel (-1) to '' so it equates to unbounded PG
  -- targets (text, etc.) where character_maximum_length is NULL → ''.
  CASE
    WHEN c.CHARACTER_MAXIMUM_LENGTH IS NULL THEN ''
    WHEN c.CHARACTER_MAXIMUM_LENGTH = -1     THEN ''
    ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR)
  END AS len,
  CASE WHEN c.NUMERIC_PRECISION IS NULL THEN '' ELSE CAST(c.NUMERIC_PRECISION AS VARCHAR) END AS prec,
  CASE WHEN c.NUMERIC_SCALE IS NULL THEN '' ELSE CAST(c.NUMERIC_SCALE AS VARCHAR) END AS scl,
  CASE WHEN c.IS_NULLABLE = 'YES' THEN 'Y' ELSE 'N' END AS nul,
  CASE WHEN COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') = 1 THEN 'Y' ELSE 'N' END AS ident,
  CASE
    WHEN c.DATA_TYPE IN ('datetime','datetime2','smalldatetime') THEN 'naive_dt'
    WHEN c.DATA_TYPE = 'datetimeoffset' THEN 'tzaware_dt'
    WHEN c.DATA_TYPE = 'time' THEN 'naive_t'
    ELSE 'na'
  END AS tz,
  CASE WHEN c.COLUMN_DEFAULT IS NULL THEN 'N' ELSE 'Y' END AS deflt
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = '$MSSQL_SCHEMA'
  AND c.TABLE_NAME LIKE '$TABLE_FILTER'
ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION;
" 2>/dev/null | sed '/^$/d;/^-/d;/rows affected/d;/Changed database/d' > "$work/src.tsv"

# --- Target extraction --------------------------------------------------------
PGPASSWORD="$PG_PASS" docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -At -F '|' -c "
SELECT
  -- Lowercase the PG side too, even though PG folds unquoted identifiers to
  -- lowercase already, to defend against quoted/mixed-case identifiers in any
  -- AI-emitted DDL. Keeps the join symmetric with the MSSQL side.
  lower(table_name)  AS t,
  lower(column_name) AS c,
  CASE
    WHEN data_type = 'character varying' THEN 'varchar'
    WHEN data_type = 'character'         THEN 'char'
    WHEN data_type = 'integer'           THEN 'int'
    WHEN data_type = 'boolean'           THEN 'bit'
    ELSE data_type
  END AS dt,
  COALESCE(character_maximum_length::text, '') AS len,
  COALESCE(numeric_precision::text, '')        AS prec,
  COALESCE(numeric_scale::text, '')            AS scl,
  CASE WHEN is_nullable = 'YES' THEN 'Y' ELSE 'N' END AS nul,
  CASE WHEN is_identity = 'YES' OR column_default LIKE 'nextval(%' THEN 'Y' ELSE 'N' END AS ident,
  CASE
    WHEN data_type = 'timestamp without time zone' THEN 'naive_dt'
    WHEN data_type = 'timestamp with time zone'    THEN 'tzaware_dt'
    WHEN data_type = 'time without time zone'      THEN 'naive_t'
    WHEN data_type = 'time with time zone'         THEN 'tzaware_t'
    ELSE 'na'
  END AS tz,
  CASE WHEN column_default IS NULL OR column_default LIKE 'nextval(%' THEN 'N' ELSE 'Y' END AS deflt
FROM information_schema.columns
WHERE table_schema = '$PG_SCHEMA'
  AND lower(table_name) LIKE lower('$TABLE_FILTER')
ORDER BY table_name, ordinal_position;
" 2>/dev/null > "$work/tgt.tsv"

# --- Diff ---------------------------------------------------------------------
# Both files: pipe-separated "table|column|dt|len|prec|scl|nul|ident". Sort
# both by (table,column) and join. Report any per-field mismatch.

awk -F'|' 'NF>=10 {gsub(/[ \t]+$/, "", $0); print}' "$work/src.tsv" | sort > "$work/src.sorted"
awk -F'|' 'NF>=10 {gsub(/[ \t]+$/, "", $0); print}' "$work/tgt.tsv" | sort > "$work/tgt.sorted"

src_count=$(wc -l < "$work/src.sorted")
tgt_count=$(wc -l < "$work/tgt.sorted")

echo "== verify_columns: extracted $src_count source rows, $tgt_count target rows =="

# awk does the join + per-criterion comparison. Stays portable across bash
# versions (macOS bash 3.2 has no associative arrays).
awk -F'|' -v src="$work/src.sorted" -v tgt="$work/tgt.sorted" '
BEGIN {
  # Load source rows keyed by table|column
  while ((getline line < src) > 0) {
    n = split(line, f, "|")
    if (n < 10) continue
    key = f[1] "|" f[2]
    s_dt[key]    = f[3]; s_len[key]    = f[4]; s_prec[key]  = f[5]
    s_scl[key]   = f[6]; s_nul[key]    = f[7]; s_ident[key] = f[8]
    s_tz[key]    = f[9]; s_deflt[key]  = f[10]
    src_keys[++src_n] = key
  }
  close(src)
  while ((getline line < tgt) > 0) {
    n = split(line, f, "|")
    if (n < 10) continue
    key = f[1] "|" f[2]
    t_dt[key]    = f[3]; t_len[key]    = f[4]; t_prec[key]  = f[5]
    t_scl[key]   = f[6]; t_nul[key]    = f[7]; t_ident[key] = f[8]
    t_tz[key]    = f[9]; t_deflt[key]  = f[10]
    tgt_seen[key] = 1
  }
  close(tgt)

  fail = 0
  for (i = 1; i <= src_n; i++) {
    key = src_keys[i]
    if (!(key in tgt_seen)) {
      printf("  FAIL %s — column missing on target\n", key); fail++; continue
    }
    # 1: max_length
    if (s_len[key] != t_len[key]) {
      printf("  FAIL %s max_length: source=%s target=%s\n", key, s_len[key], t_len[key]); fail++
    }
    # 2: precision/scale (only for numeric/decimal where these are user-meaningful;
    # int columns also report precision/scale but those are dialect artifacts)
    if (s_dt[key] ~ /^(decimal|numeric|money|smallmoney)$/) {
      if (s_prec[key] != t_prec[key]) {
        printf("  FAIL %s precision: source=%s target=%s\n", key, s_prec[key], t_prec[key]); fail++
      }
      if (s_scl[key] != t_scl[key]) {
        printf("  FAIL %s scale: source=%s target=%s\n", key, s_scl[key], t_scl[key]); fail++
      }
    }
    # 3: nullability
    if (s_nul[key] != t_nul[key]) {
      printf("  FAIL %s nullable: source=%s target=%s\n", key, s_nul[key], t_nul[key]); fail++
    }
    # 4: TZ-awareness CLASS — covers all source temporal types. The class
    # encodes both TZ-awareness AND temporal family (dt vs t) so a datetime
    # accidentally mapped to time (or vice versa) fails: naive_dt != naive_t
    # even though both are TZ-naive.
    if (s_tz[key] != "na" && s_tz[key] != t_tz[key]) {
      printf("  FAIL %s tz-awareness: source=%s target=%s\n", key, s_tz[key], t_tz[key]); fail++
    }
    # 5: identity
    if (s_ident[key] != t_ident[key]) {
      printf("  FAIL %s identity: source=%s target=%s\n", key, s_ident[key], t_ident[key]); fail++
    }
    # 6: default-expression presence (binary check; full equivalence is a TODO
    # since cross-dialect expression normalization is non-trivial — getutcdate()
    # vs CURRENT_TIMESTAMP, NEWID() vs gen_random_uuid(), etc.). Catches the
    # most common regression: dropped default. Identity defaults (nextval / IDENTITY)
    # are excluded on both sides since they are recognized via the identity field.
    if (s_ident[key] == "N" && s_deflt[key] != t_deflt[key]) {
      printf("  FAIL %s has_default: source=%s target=%s\n", key, s_deflt[key], t_deflt[key]); fail++
    }
  }
  for (key in tgt_seen) {
    found = 0
    for (i = 1; i <= src_n; i++) if (src_keys[i] == key) { found = 1; break }
    if (!found) { printf("  FAIL %s — column on target but not in source\n", key); fail++ }
  }

  if (fail > 0) {
    print ""
    printf("== verify_columns: %d mismatch(es) ==\n", fail)
    exit 1
  }
  printf("== verify_columns: PASS (criteria 1–6 met across %d columns; criterion 7 (computed columns) is TODO) ==\n", src_n)
}'
