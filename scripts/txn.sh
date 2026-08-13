#!/usr/bin/env bash
# Reference implementation of the client-side correlation protocol.
#
# Runs a write transaction and prints the GTID it committed under. That GTID
# is the join key: every Debezium event for this transaction carries the same
# value in source.gtid (and in transaction.id).
#
# The SELECT must run on the SAME session as the COMMIT, which is why the
# whole thing is piped into a single client invocation.
#
#   ./scripts/txn.sh                                   # default increment
#   ./scripts/txn.sh "UPDATE registers SET val=9 WHERE id=2"
set -euo pipefail

BODY="${*:-UPDATE registers SET val = val + 1 WHERE id = 1; UPDATE registers SET val = val - 1 WHERE id = 2;}"

docker exec -i cdc-mariadb mariadb --skip-ssl -uapp -papp_pw sitest -N 2>/dev/null <<SQL
START TRANSACTION;
${BODY}
COMMIT;
SELECT @@session.last_gtid;
SQL
