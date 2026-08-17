#!/usr/bin/env bash
# Manual CDC transaction helper.
#
#   ./scripts/txn.sh                                  # default workload body
#   ./scripts/txn.sh "UPDATE usertable SET field1='x' WHERE ycsb_key=2"
#   DB=yugabyte ./scripts/txn.sh "UPDATE registers SET val=9 WHERE id=2"
#   SESSION=sess7 TXN_ID=42 ./scripts/txn.sh
#
# MariaDB correlation is the connection-local GTID change and never adds a
# marker write. The YugabyteDB/Oracle exploratory adapters still use their
# pre-existing session marker; they are outside the frozen MariaDB scope.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/lib.sh

if (( $# > 0 )); then
  BODY=$*
elif [[ $DB == mariadb ]]; then
  BODY="INSERT INTO usertable (ycsb_key, field1) VALUES (1, 'a')
    ON DUPLICATE KEY UPDATE field1 = IF(field1 = 'a', 'b', 'a');"
else
  BODY="UPDATE registers SET val = val + 1 WHERE id = 1;
UPDATE registers SET val = val - 1 WHERE id = 2;"
fi

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
COMMIT;
SELECT @@session.last_gtid;
SQL
    OUT=$(db_run_file "$TMP" 2>&1)
    if echo "$OUT" | grep -qi 'ERROR'; then
      echo "transaction: ABORTED - $(echo "$OUT" | grep -i 'ERROR' | head -1)"
      exit 1
    fi
    echo "gtid: $(printf '%s' "$OUT" | tail -1)"
    exit 0
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
