#!/usr/bin/env bash
set -euo pipefail

readonly database_host=yugabytedb_ysql
readonly database_name=benchbase
readonly slot_name=${BLACKBOX_YSQL_SLOT_NAME:?BLACKBOX_YSQL_SLOT_NAME is required}
readonly publication_name=${BLACKBOX_YSQL_PUBLICATION_NAME:?BLACKBOX_YSQL_PUBLICATION_NAME is required}
readonly capture_tables=${BLACKBOX_YSQL_CAPTURE_TABLES:?BLACKBOX_YSQL_CAPTURE_TABLES is required}
readonly create_capture_objects=${BLACKBOX_YSQL_CREATE_CAPTURE_OBJECTS:-true}
readonly replica_identity=${BLACKBOX_YSQL_REPLICA_IDENTITY:-DEFAULT}
if [[ $create_capture_objects != true && $create_capture_objects != false ]]; then
  echo "invalid BLACKBOX_YSQL_CREATE_CAPTURE_OBJECTS: $create_capture_objects" >&2
  exit 2
fi
if [[ $replica_identity != FULL && $replica_identity != DEFAULT ]]; then
  echo "invalid BLACKBOX_YSQL_REPLICA_IDENTITY: $replica_identity" >&2
  exit 2
fi

for identifier in "$slot_name" "$publication_name"; do
  if [[ ! $identifier =~ ^[a-z][a-z0-9_]{0,62}$ ]]; then
    echo "invalid YugabyteDB capture identifier: $identifier" >&2
    exit 2
  fi
done
if [[ ! $capture_tables =~ ^benchbase\.[a-z][a-z0-9_]*(,benchbase\.[a-z][a-z0-9_]*)*$ ]]; then
  echo "invalid YugabyteDB publication table roster" >&2
  exit 2
fi

ysql_admin() {
  bin/ysqlsh --host "$database_host" --username yugabyte \
    --dbname "${1:-yugabyte}" --no-psqlrc --set ON_ERROR_STOP=on "${@:2}"
}

ysql_admin yugabyte --command \
  "CREATE ROLE blackbox_setup LOGIN PASSWORD 'blackbox_setup_password'"
ysql_admin yugabyte --command \
  "CREATE ROLE blackbox_application LOGIN PASSWORD 'blackbox_application_password'"
ysql_admin yugabyte --command \
  "CREATE ROLE blackbox_replication LOGIN REPLICATION PASSWORD 'blackbox_replication_password'"
ysql_admin yugabyte --command \
  "CREATE DATABASE benchbase OWNER blackbox_setup ENCODING 'UTF8'"
ysql_admin benchbase --command "CREATE SCHEMA benchbase AUTHORIZATION blackbox_setup"
ysql_admin benchbase --command "ALTER DATABASE benchbase SET search_path TO benchbase"
ysql_admin benchbase --command "ALTER DATABASE benchbase SET timezone TO 'UTC'"
ysql_admin benchbase --command \
  "ALTER DATABASE benchbase SET default_transaction_isolation TO 'repeatable read'"

PGPASSWORD=blackbox_setup_password bin/ysqlsh --host "$database_host" \
  --username blackbox_setup --dbname "$database_name" --no-psqlrc \
  --set ON_ERROR_STOP=on --file /blackbox/common-schema.sql
PGPASSWORD=blackbox_setup_password bin/ysqlsh --host "$database_host" \
  --username blackbox_setup --dbname "$database_name" --no-psqlrc \
  --set ON_ERROR_STOP=on --file /blackbox/adapter-schema.sql

while IFS= read -r table; do
  [[ $table =~ ^[a-z][a-z0-9_]*$ ]] || exit 2
  ysql_admin benchbase --command \
    "ALTER TABLE benchbase.$table REPLICA IDENTITY $replica_identity"
done < <(ysql_admin benchbase --tuples-only --no-align --command \
  "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='benchbase' AND c.relkind='r' ORDER BY c.relname")

ysql_admin benchbase --command \
  "GRANT USAGE ON SCHEMA benchbase TO blackbox_application; GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA benchbase TO blackbox_application"
ysql_admin benchbase --command \
  "GRANT USAGE ON SCHEMA benchbase TO blackbox_replication; GRANT SELECT ON ALL TABLES IN SCHEMA benchbase TO blackbox_replication"
if [[ $create_capture_objects == true ]]; then
  ysql_admin benchbase --command \
    "CREATE PUBLICATION $publication_name FOR TABLE $capture_tables"
  ysql_admin benchbase --command \
    "SELECT * FROM pg_create_logical_replication_slot('$slot_name', 'pgoutput', false, false, 'SEQUENCE')"
fi
