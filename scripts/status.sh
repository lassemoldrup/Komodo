#!/usr/bin/env bash
# Health overview of whichever backend is running.
set -uo pipefail
cd "$(dirname "$0")/.."
. scripts/lib.sh

echo "=== Backend: $DB ==="
docker compose --profile "$DB" ps --format 'table {{.Name}}\t{{.Service}}\t{{.Status}}'

echo
if [ "$DB" = "oracle" ]; then
  echo "=== LogMiner prerequisites (need ARCHIVELOG / YES / YES) ==="
  # v$database needs SYSDBA, so this goes through a script file rather than
  # db_query (which connects as the workload user).
  ORA_TMP=$(mktemp /tmp/_st.XXXXXX)
  cat > "$ORA_TMP" <<'ORASQL'
set heading off feedback off pagesize 0 linesize 300
select 'log_mode=' || log_mode || ' force_logging=' || force_logging
       || ' suppl_min=' || supplemental_log_data_min from v$database;
exit
ORASQL
  docker exec -i cdc-oracle sqlplus -s / as sysdba < "$ORA_TMP" 2>/dev/null | grep -vE '^\s*$'
  rm -f "$ORA_TMP"
  echo
  echo "=== Default isolation (logon trigger sets SERIALIZABLE = SI) ==="
  db_query "select 'logon_trigger=' || status from user_triggers where trigger_name='SET_SI_ISOLATION';"
  echo
  echo "=== Registers ==="
  db_query "select 'id=' || id || ' val=' || val from sitest.registers order by id;"
  DBZ=cdc-debezium-oracle
elif [ "$DB" = "yugabyte" ]; then
  echo "=== Replication slot (must be HYBRID_TIME) ==="
  db_query "SELECT slot_name || '  ' || plugin || '  ' || yb_lsn_type || '  active=' || active FROM pg_replication_slots"
  echo
  echo "=== Replica identity (must be f = FULL) ==="
  db_query "SELECT relname || '=' || relreplident::text FROM pg_class WHERE relname IN ('registers','session_txn')"
  echo
  echo "=== Effective isolation (want repeatable read = SI) ==="
  db_query "SHOW default_transaction_isolation;"
  echo
  echo "=== Session markers (latest txn per session) ==="
  db_query "SELECT session_id || ' -> ' || txn_id FROM session_txn WHERE txn_id <> 0 ORDER BY session_id LIMIT 10"
  DBZ=cdc-debezium-yb
else
  echo "=== Binlog position ==="
  db_query "SELECT @@gtid_binlog_pos"
  echo "=== Isolation ==="
  db_query "SELECT CONCAT(@@innodb_snapshot_isolation, ' / ', @@transaction_isolation)"
  DBZ=cdc-debezium
fi

echo
echo "=== Debezium ($DBZ) ==="
# grep -c rather than grep -q: with `set -o pipefail`, grep -q exits on first
# match, docker logs dies of SIGPIPE, and the pipeline reports failure even
# though the pattern matched. grep -c consumes all input, so no SIGPIPE.
STREAMING=$(docker logs "$DBZ" 2>&1 | grep -c 'Starting streaming' || true)
if [ "${STREAMING:-0}" -gt 0 ]; then
  echo "streaming: yes"
else
  echo "streaming: NO"
fi
echo "error lines: $(docker logs "$DBZ" 2>&1 | grep -c 'ERROR' || true)"

echo
echo "=== Redis stream ==="
echo "entries: $(docker exec cdc-redis redis-cli XLEN cdc 2>/dev/null)"
docker exec cdc-redis redis-cli INFO memory 2>/dev/null | grep -E '^used_memory_human' | tr -d '\r'
