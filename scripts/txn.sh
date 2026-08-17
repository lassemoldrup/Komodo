#!/usr/bin/env bash
# Reference implementation of the client-side correlation protocol.
#
#   ./scripts/txn.sh                                  # default workload body
#   ./scripts/txn.sh "UPDATE registers SET val=9 WHERE id=2"
#   SESSION=sess7 TXN_ID=42 ./scripts/txn.sh
#
# One protocol for every backend: the client mints its own (session_id, txn_id)
# and upserts it into its session's row in session_txn as the LAST statement of
# the transaction. The marker then travels inside that transaction's own change
# set, arriving after the workload writes, and identifies the transaction in
# the CDC stream.
#
# Only the upsert dialect differs — ON CONFLICT (Postgres/YugabyteDB),
# ON DUPLICATE KEY (MariaDB), MERGE (Oracle). No isolation statement is issued
# anywhere: all three backends default to Snapshot Isolation server-side.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/lib.sh

BODY_DEFAULT="UPDATE registers SET val = val + 1 WHERE id = 1;
UPDATE registers SET val = val - 1 WHERE id = 2;"
BODY="${*:-$BODY_DEFAULT}"

SESSION="${SESSION:-sess1}"
TXN_ID="${TXN_ID:-$(date +%s)}"

# Unique temp file: txn.sh is run concurrently by multiple sessions.
# The X's must be the LAST characters of the template; a ".sql" suffix here
# makes mktemp treat the name literally and fail once the file exists.
TMP=$(mktemp /tmp/_txn.XXXXXX)
trap 'rm -f "$TMP"' EXIT

case "$DB" in
  yugabyte)
    cat > "$TMP" <<SQL
BEGIN;
${BODY}
INSERT INTO session_txn (session_id, txn_id) VALUES ('${SESSION}', ${TXN_ID})
  ON CONFLICT (session_id) DO UPDATE SET txn_id = EXCLUDED.txn_id;
COMMIT;
SQL
    OUT=$(db_run_file "$TMP" 2>&1)
    if echo "$OUT" | grep -qi '^ERROR'; then
      echo "marker: ABORTED - $(echo "$OUT" | grep -i '^ERROR' | head -1)"
      exit 1
    fi
    ;;

  mariadb)
    cat > "$TMP" <<SQL
START TRANSACTION;
${BODY}
INSERT INTO session_txn (session_id, txn_id) VALUES ('${SESSION}', ${TXN_ID})
  ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
COMMIT;
SQL
    OUT=$(db_run_file "$TMP" 2>&1)
    if echo "$OUT" | grep -qi 'ERROR'; then
      echo "marker: ABORTED - $(echo "$OUT" | grep -i 'ERROR' | head -1)"
      exit 1
    fi
    ;;

  oracle)
    # Oracle has no ON CONFLICT / ON DUPLICATE KEY; MERGE is the upsert.
    cat > "$TMP" <<SQL
set heading off feedback off pagesize 0
-- Without this, sqlplus carries on after an error and the trailing COMMIT
-- still fires -- committing the workload writes with NO marker, which the
-- monitor cannot correlate. Roll back the whole transaction instead.
WHENEVER SQLERROR EXIT ROLLBACK
WHENEVER OSERROR EXIT ROLLBACK
${BODY}
MERGE INTO sitest.session_txn t
  USING (SELECT '${SESSION}' AS session_id, ${TXN_ID} AS txn_id FROM DUAL) s
  ON (t.session_id = s.session_id)
  WHEN MATCHED THEN UPDATE SET t.txn_id = s.txn_id
  WHEN NOT MATCHED THEN INSERT (session_id, txn_id) VALUES (s.session_id, s.txn_id);
COMMIT;
exit
SQL
    OUT=$(db_run_file "$TMP" 2>&1)
    if echo "$OUT" | grep -q '^ORA-'; then
      echo "marker: ABORTED - $(echo "$OUT" | grep '^ORA-' | head -1)"
      exit 1
    fi
    ;;
esac

echo "marker: session_id=${SESSION} txn_id=${TXN_ID}"
