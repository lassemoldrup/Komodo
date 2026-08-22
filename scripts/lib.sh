#!/usr/bin/env bash
# Shared backend detection. Source this; don't run it.
#
# DB=mariadb (default) or DB=yugabytedb_ysql. If DB is unset, infer from whichever
# backend container is actually running, so the scripts "just work" after
# `make up DB=yugabytedb_ysql`.

if [ -z "${DB:-}" ]; then
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^cdc-yugabytedb-ysql$'; then
    DB=yugabytedb_ysql
  elif docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^cdc-oracle$'; then
    DB=oracle
  else
    DB=mariadb
  fi
fi
export DB

case "$DB" in
  mariadb)  DB_CONTAINER=cdc-mariadb ;;
  yugabytedb_ysql) DB_CONTAINER=cdc-yugabytedb-ysql ;;
  oracle)   DB_CONTAINER=cdc-oracle ;;
  *) echo "unknown DB='$DB' (expected mariadb|yugabytedb_ysql|oracle)" >&2; exit 2 ;;
esac
export DB_CONTAINER

# Run SQL from a file, in ONE session, against the active backend.
# YugabyteDB aborts multi-statement simple-query blocks on any error and does
# not retry them, so scripted transactions go through a file rather than -c.
db_run_file() {
  local f="$1"
  if [ "$DB" = "oracle" ]; then
    # Pipe via stdin rather than docker cp: cp lands the file owned by root,
    # but sqlplus runs as the image's oracle user and then cannot read it
    # (SP2-0310). stdin also avoids any temp-file collision between sessions.
    docker exec -i cdc-oracle sqlplus -s sitest/sitestpw@localhost:1521/freepdb1 < "$f"
  elif [ "$DB" = "yugabytedb_ysql" ]; then
    # Unique in-container path: concurrent callers must not share a temp file.
    local remote="/tmp/_run.$$.$RANDOM.sql"
    docker cp "$f" "cdc-yugabytedb-ysql:${remote}" >/dev/null 2>&1
    docker exec -e PGPASSWORD="${DB_PASSWORD:-blackbox_application_password}" \
      cdc-yugabytedb-ysql bin/ysqlsh -h yugabytedb_ysql \
      -U "${DB_USER:-blackbox_application}" -d benchbase -qtA -f "$remote"
    docker exec cdc-yugabytedb-ysql rm -f "$remote" >/dev/null 2>&1 || true
  else
    # --skip-ssl: the MariaDB 11.8+ client requires TLS by default.
    # -N -B: no column headers, tab-separated. (Do NOT use -t here: in the
    # MariaDB client -t means table-formatted output, not psql's "tuples only".)
    docker exec -i cdc-mariadb mariadb --skip-ssl \
      -u"${DB_USER:-app}" -p"${DB_PASSWORD:-app_pw}" benchbase -N -B < "$f" 2>/dev/null
  fi
}

db_query() {
  local sql="$1"
  if [ "$DB" = "oracle" ]; then
    local t; t=$(mktemp /tmp/_q.XXXXXX)
    { echo "set heading off feedback off pagesize 0 linesize 300"; echo "$sql"; echo "exit"; } > "$t"
    db_run_file "$t" | grep -vE '^\s*$'
    rm -f "$t"
  elif [ "$DB" = "yugabytedb_ysql" ]; then
    docker exec -e PGPASSWORD="${DB_PASSWORD:-blackbox_application_password}" \
      cdc-yugabytedb-ysql bin/ysqlsh -h yugabytedb_ysql \
      -U "${DB_USER:-blackbox_application}" -d benchbase -qtAc "$sql"
  else
    docker exec -i cdc-mariadb mariadb --skip-ssl \
      -u"${DB_USER:-app}" -p"${DB_PASSWORD:-app_pw}" benchbase -N -B -e "$sql" 2>/dev/null
  fi
}
