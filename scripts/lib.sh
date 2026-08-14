#!/usr/bin/env bash
# Shared backend detection. Source this; don't run it.
#
# DB=mariadb (default) or DB=yugabyte. If DB is unset, infer from whichever
# backend container is actually running, so the scripts "just work" after
# `make up DB=yugabyte`.

if [ -z "${DB:-}" ]; then
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^cdc-yugabyte$'; then
    DB=yugabyte
  else
    DB=mariadb
  fi
fi
export DB

case "$DB" in
  mariadb)  DB_CONTAINER=cdc-mariadb ;;
  yugabyte) DB_CONTAINER=cdc-yugabyte ;;
  *) echo "unknown DB='$DB' (expected mariadb|yugabyte)" >&2; exit 2 ;;
esac
export DB_CONTAINER

# Run SQL from a file, in ONE session, against the active backend.
# YugabyteDB aborts multi-statement simple-query blocks on any error and does
# not retry them, so scripted transactions go through a file rather than -c.
db_run_file() {
  local f="$1"
  if [ "$DB" = "yugabyte" ]; then
    # Unique in-container path: concurrent callers must not share a temp file.
    local remote="/tmp/_run.$$.$RANDOM.sql"
    docker cp "$f" "cdc-yugabyte:${remote}" >/dev/null 2>&1
    docker exec cdc-yugabyte bin/ysqlsh -h cdc-yugabyte -U yugabyte -d yugabyte -qtA -f "$remote"
    docker exec cdc-yugabyte rm -f "$remote" >/dev/null 2>&1 || true
  else
    # --skip-ssl: the MariaDB 11.8+ client requires TLS by default.
    # -N -B: no column headers, tab-separated. (Do NOT use -t here: in the
    # MariaDB client -t means table-formatted output, not psql's "tuples only".)
    docker exec -i cdc-mariadb mariadb --skip-ssl \
      -u"${DB_USER:-app}" -p"${DB_PASSWORD:-app_pw}" sitest -N -B < "$f" 2>/dev/null
  fi
}

db_query() {
  local sql="$1"
  if [ "$DB" = "yugabyte" ]; then
    docker exec cdc-yugabyte bin/ysqlsh -h cdc-yugabyte -U yugabyte -d yugabyte -qtAc "$sql"
  else
    docker exec -i cdc-mariadb mariadb --skip-ssl \
      -u"${DB_USER:-app}" -p"${DB_PASSWORD:-app_pw}" sitest -N -B -e "$sql" 2>/dev/null
  fi
}
