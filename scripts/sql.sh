#!/usr/bin/env bash
# SQL shell / one-shot query against the active backend.
#
#   ./scripts/sql.sh                                  # interactive
#   ./scripts/sql.sh "SELECT * FROM usertable"
#   DB=yugabytedb_ysql ./scripts/sql.sh "SELECT * FROM session_txn LIMIT 5"
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/lib.sh

if [ $# -gt 0 ]; then
  db_query "$*"
  exit 0
fi

if [ "$DB" = "yugabytedb_ysql" ]; then
  exec docker exec -it -e PGPASSWORD="${DB_PASSWORD:-blackbox_application_password}" \
    cdc-yugabytedb-ysql bin/ysqlsh -h yugabytedb_ysql \
    -U "${DB_USER:-blackbox_application}" -d benchbase
else
  # --skip-ssl: the MariaDB 11.8+ client requires TLS by default.
  exec docker exec -it cdc-mariadb mariadb --skip-ssl \
    -u"${DB_USER:-app}" -p"${DB_PASSWORD:-app_pw}" benchbase
fi
