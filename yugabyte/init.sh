#!/usr/bin/env bash
# Initialise YugabyteDB for CDC, in the one order that works:
#   1. all schema + seed data
#   2. replication slot LAST
#
# The slot must be created here rather than letting Debezium auto-create it,
# because auto-creation defaults to yb_lsn_type=SEQUENCE. We need HYBRID_TIME
# so that source.lsn carries the commit hybrid time: that is what makes the
# stream a total commit order and gives every event of a transaction the same
# stable identifier.
set -euo pipefail

YB_HOST="${YB_HOST:-cdc-yugabyte}"
SLOT_NAME="${SLOT_NAME:-dbz_si}"

ysql() { bin/ysqlsh -h "$YB_HOST" -U yugabyte -d yugabyte -qtA "$@"; }

echo "[init] waiting for YSQL on ${YB_HOST} ..."
for _ in $(seq 1 60); do
  ysql -c "SELECT 1" >/dev/null 2>&1 && break
  sleep 3
done
ysql -c "SELECT 1" >/dev/null

# --- Idempotence: if the slot already exists, this stack is already set up ---
if [ "$(ysql -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name='${SLOT_NAME}'")" != "0" ]; then
  echo "[init] slot '${SLOT_NAME}' already exists, nothing to do."
  exit 0
fi

echo "[init] applying schema (before any slot exists) ..."
ysql -f /init/init.sql

echo "[init] verifying replica identity ..."
ysql -c "SELECT relname || '=' || relreplident::text FROM pg_class
         WHERE relname IN ('registers','session_txn')"

echo "[init] creating HYBRID_TIME replication slot '${SLOT_NAME}' ..."
ysql -c "SELECT pg_create_logical_replication_slot('${SLOT_NAME}','yboutput',false,false,'HYBRID_TIME')"

echo "[init] slots:"
ysql -c "SELECT slot_name || ' ' || plugin || ' ' || yb_lsn_type FROM pg_replication_slots"

echo "[init] done. session_txn starts empty; clients upsert their own rows."
