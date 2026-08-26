#!/usr/bin/env bash
# Create one fresh run-owned schema and its narrowly privileged principals.
set -euo pipefail

for name in BLACKBOX_ORACLE_SYS_PASSWORD BLACKBOX_ORACLE_OWNER_PASSWORD \
  BLACKBOX_ORACLE_APPLICATION_PASSWORD; do
  value=${!name:-}
  if [[ -z $value || $value == *[!A-Za-z0-9_]* ]]; then
    echo "$name must contain only letters, digits, and underscore" >&2
    exit 1
  fi
done
if [[ ! ${BLACKBOX_ORACLE_CAPTURE_TABLES:-} =~ ^BLACKBOX_OWNER\.[A-Z][A-Z0-9_]*(,BLACKBOX_OWNER\.[A-Z][A-Z0-9_]*)*$ ]]; then
  echo "BLACKBOX_ORACLE_CAPTURE_TABLES is not an exact uppercase table roster" >&2
  exit 1
fi

sqlplus -s "sys/${BLACKBOX_ORACLE_SYS_PASSWORD}@oracle:1521/FREEPDB1 as sysdba" <<SQL
whenever sqlerror exit failure rollback
define owner_password=${BLACKBOX_ORACLE_OWNER_PASSWORD}
define application_password=${BLACKBOX_ORACLE_APPLICATION_PASSWORD}
@/blackbox/common-schema.sql
@/blackbox/adapter-schema.sql
exit
SQL

# Oracle can raise ORA-08177 on the first SERIALIZABLE access immediately
# after DDL because no rollback data exists yet. Touch every captured table in
# one rolled-back lock-only transaction before CDC starts; no row is created.
warmup_sql=$(mktemp /tmp/blackbox-oracle-warmup.XXXXXX.sql)
trap 'rm -f -- "$warmup_sql"' EXIT
{
  printf '%s\n' 'whenever sqlerror exit failure rollback'
  printf '%s\n' 'ALTER SESSION SET CURRENT_SCHEMA=BLACKBOX_OWNER;'
  printf '%s\n' 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;'
  IFS=',' read -r -a capture_tables <<<"$BLACKBOX_ORACLE_CAPTURE_TABLES"
  for qualified_table in "${capture_tables[@]}"; do
    printf 'LOCK TABLE %s IN ROW SHARE MODE;\n' "${qualified_table#BLACKBOX_OWNER.}"
  done
  printf '%s\n' 'ROLLBACK;' 'exit'
} >"$warmup_sql"
sqlplus -s "blackbox_application/${BLACKBOX_ORACLE_APPLICATION_PASSWORD}@oracle:1521/FREEPDB1" \
  @"$warmup_sql"
sqlplus -s "blackbox_application/${BLACKBOX_ORACLE_APPLICATION_PASSWORD}@oracle:1521/FREEPDB1" \
  @/blackbox/adapter-warmup.sql
sqlplus -s "sys/${BLACKBOX_ORACLE_SYS_PASSWORD}@oracle:1521/FREEPDB1 as sysdba" <<'SQL'
whenever sqlerror exit failure rollback
DELETE FROM blackbox_owner.checker_run_fence WHERE phase='WARMUP';
DELETE FROM blackbox_owner.session_txn WHERE session_id='__oracle_warmup__';
COMMIT;
exit
SQL

touch /tmp/blackbox-oracle-schema.done
