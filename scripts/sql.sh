#!/usr/bin/env bash
# SQL shell / one-shot query against the active backend.
#
#   ./scripts/sql.sh                                  # interactive
#   ./scripts/sql.sh "SELECT * FROM registers"
#   DB=yugabyte ./scripts/sql.sh "SELECT * FROM session_txn LIMIT 5"
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/lib.sh

if [ $# -gt 0 ]; then
  db_query "$*"
  exit 0
fi

if [ "$DB" = "yugabyte" ]; then
  exec docker exec -it cdc-yugabyte bin/ysqlsh -h cdc-yugabyte -U yugabyte -d yugabyte
else
  # --skip-ssl: the MariaDB 11.8+ client requires TLS by default.
  exec docker exec -it cdc-mariadb mariadb --skip-ssl \
    -u"${DB_USER:-app}" -p"${DB_PASSWORD:-app_pw}" sitest
fi
