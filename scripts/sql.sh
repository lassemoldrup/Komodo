#!/usr/bin/env bash
# SQL shell against the workload database.
#
#   ./scripts/sql.sh                          # interactive
#   ./scripts/sql.sh "SELECT * FROM registers"
#   ./scripts/sql.sh < some_script.sql        # whole file in ONE session
#
# --skip-ssl: the MariaDB 11.8 client requires TLS by default; this listener
# is plaintext.
set -euo pipefail

FLAGS=(--skip-ssl -u"${DB_USER:-app}" -p"${DB_PASSWORD:-app_pw}" sitest)

if [ $# -gt 0 ]; then
  exec docker exec -i cdc-mariadb mariadb "${FLAGS[@]}" -e "$*"
elif [ -t 0 ]; then
  exec docker exec -it cdc-mariadb mariadb "${FLAGS[@]}"
else
  exec docker exec -i cdc-mariadb mariadb "${FLAGS[@]}"
fi
