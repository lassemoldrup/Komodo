#!/usr/bin/env bash
# Reference implementation of the client-side correlation protocol.
#
#   ./scripts/txn.sh                       # default workload body
#   ./scripts/txn.sh "UPDATE registers SET val=9 WHERE id=2"
#   SESSION=sess7 TXN_ID=42 ./scripts/txn.sh          (yugabyte)
#
# The two backends correlate differently:
#
#   MariaDB     the server hands out the key. SELECT @@session.last_gtid on
#               the SAME session right after COMMIT returns the GTID that
#               every Debezium event for this transaction carries in
#               source.gtid / transaction.id.
#
#   YugabyteDB  no server-side key exists (see README). The client supplies
#               one: it upserts its own txn_id into its session's row in
#               session_txn as the LAST statement of the transaction, so the
#               marker travels inside the transaction's own change set and
#               arrives after that transaction's workload writes.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/lib.sh

BODY_DEFAULT="UPDATE registers SET val = val + 1 WHERE id = 1;
UPDATE registers SET val = val - 1 WHERE id = 2;"
BODY="${*:-$BODY_DEFAULT}"

# Unique temp file: txn.sh is run concurrently by multiple sessions.
# The X's must be the LAST characters of the template; a ".sql" suffix here
# makes mktemp treat the name literally and fail once the file exists.
TMP=$(mktemp /tmp/_txn.XXXXXX)
trap 'rm -f "$TMP"' EXIT

if [ "$DB" = "oracle" ]; then
  # Oracle hands out the key, like MariaDB. DBMS_TRANSACTION.LOCAL_TRANSACTION_ID
  # returns usn.slot.sqn; Debezium reports the same value in source.txId as
  # little-endian hex, so the two are the same id in different encodings.
  cat > "$TMP" <<SQL
set heading off feedback off pagesize 0
-- No isolation statement: a logon trigger sets SERIALIZABLE (= SI) for this
-- schema, so the session default is already correct.
${BODY}
SELECT 'XID=' || DBMS_TRANSACTION.LOCAL_TRANSACTION_ID FROM DUAL;
COMMIT;
exit
SQL
  OUT=$(db_run_file "$TMP" 2>&1)
  if echo "$OUT" | grep -q '^ORA-'; then
    echo "marker: ABORTED - $(echo "$OUT" | grep '^ORA-' | head -1)"
    exit 1
  fi
  echo "marker: xid=$(echo "$OUT" | grep -o 'XID=.*' | head -1 | cut -d= -f2)"
elif [ "$DB" = "yugabyte" ]; then
  SESSION="${SESSION:-sess1}"
  TXN_ID="${TXN_ID:-$(date +%s)}"
  # The marker is the LAST statement before COMMIT: the workload runs first,
  # so nothing about the transaction's own behaviour is perturbed by the
  # bookkeeping write. Upsert, so a session needs no pre-provisioned row.
  cat > "$TMP" <<SQL
-- Plain BEGIN: default_transaction_isolation is set to repeatable read
-- (= SI) at the database level.
BEGIN;
${BODY}
INSERT INTO session_txn (session_id, txn_id) VALUES ('${SESSION}', ${TXN_ID})
  ON CONFLICT (session_id) DO UPDATE SET txn_id = EXCLUDED.txn_id;
COMMIT;
SQL
  db_run_file "$TMP" | grep -vE '^\s*$' || true
  echo "marker: session_id=${SESSION} txn_id=${TXN_ID}"
else
  cat > "$TMP" <<SQL
START TRANSACTION;
${BODY}
COMMIT;
SELECT @@session.last_gtid;
SQL
  GTID=$(db_run_file "$TMP" | tail -1)
  echo "marker: gtid=${GTID}"
fi
