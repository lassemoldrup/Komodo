#!/usr/bin/env bash
set -euo pipefail

readonly database_host=${CONNECTOR_CONTRACT_DATABASE_HOST:-yugabytedb_ysql}
readonly database_name=benchbase
readonly plugin=${CONNECTOR_CONTRACT_PLUGIN:-pgoutput}
readonly slot=${CONNECTOR_CONTRACT_SLOT:-connector_contract_slot}

case "$plugin" in
  pgoutput | yboutput) ;;
  *)
    echo "unsupported connector-contract plugin: $plugin" >&2
    exit 2
    ;;
esac

if [[ ! "$slot" =~ ^[a-z][a-z0-9_]{0,62}$ ]]; then
  echo "invalid connector-contract slot name: $slot" >&2
  exit 2
fi

ysql_admin() {
  bin/ysqlsh \
    --host "$database_host" \
    --username yugabyte \
    --dbname "${1:-yugabyte}" \
    --set ON_ERROR_STOP=on \
    "${@:2}"
}

ysql_admin yugabyte --command "CREATE ROLE connector_contract_setup LOGIN PASSWORD 'connector_contract_setup_password'"
ysql_admin yugabyte --command "CREATE ROLE connector_contract_application LOGIN PASSWORD 'connector_contract_application_password'"
ysql_admin yugabyte --command "CREATE ROLE connector_contract_replication LOGIN REPLICATION PASSWORD 'connector_contract_replication_password'"
ysql_admin yugabyte --command "CREATE DATABASE benchbase OWNER connector_contract_setup ENCODING 'UTF8'"
ysql_admin benchbase --command "CREATE SCHEMA benchbase AUTHORIZATION connector_contract_setup"
ysql_admin benchbase --command "ALTER DATABASE benchbase SET search_path TO benchbase"
ysql_admin benchbase --command "ALTER DATABASE benchbase SET timezone TO 'UTC'"

PGPASSWORD=connector_contract_setup_password bin/ysqlsh \
  --host "$database_host" \
  --username connector_contract_setup \
  --dbname "$database_name" \
  --set ON_ERROR_STOP=on \
  --file /connector-contract/schema.sql

ysql_admin benchbase --command \
  "SELECT * FROM pg_create_logical_replication_slot('$slot', '$plugin', false, false, 'SEQUENCE')"

ysql_admin benchbase --command \
  "SELECT slot_name, plugin, database, yb_lsn_type, active FROM pg_replication_slots WHERE slot_name = '$slot'"
ysql_admin benchbase --command \
  "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'connector_contract_publication' ORDER BY schemaname, tablename"
ysql_admin benchbase --command \
  "SELECT c.relname, c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'benchbase' AND c.relkind = 'r' ORDER BY c.relname"
ysql_admin benchbase --command \
  "SELECT (SELECT count(*) FROM benchbase.usertable) AS usertable_rows, (SELECT count(*) FROM benchbase.checker_run_fence) AS fence_rows, (SELECT count(*) FROM benchbase.session_txn) AS marker_rows, (SELECT count(*) FROM benchbase.contract_account_a) + (SELECT count(*) FROM benchbase.contract_account_b) + (SELECT count(*) FROM benchbase.contract_account_c) + (SELECT count(*) FROM benchbase.contract_account_d) AS contract_rows, (SELECT count(*) FROM benchbase.warehouse) + (SELECT count(*) FROM benchbase.district) AS tpcc_rows"
