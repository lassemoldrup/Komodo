#!/usr/bin/env bash
set -euo pipefail

contract_root=/oceanbase-mysql-contract
tenant_name=${TENANT_NAME:?}
root_password=${PASSWORD:?}
expected_capture_tables_csv=${BLACKBOX_OCEANBASE_MYSQL_CAPTURE_TABLES:?}

IFS=',' read -r -a expected_capture_tables <<<"$expected_capture_tables_csv"
(( ${#expected_capture_tables[@]} > 0 ))

declare -A expected_table_names=()
correlation_table_present=false
fence_table_present=false
for qualified_table in "${expected_capture_tables[@]}"; do
  [[ $qualified_table =~ ^benchbase\.([a-z][a-z0-9_]*)$ ]]
  [[ -z ${expected_table_names[$qualified_table]+present} ]]
  expected_table_names[$qualified_table]=present
  case "$qualified_table" in
    benchbase.session_txn) correlation_table_present=true ;;
    benchbase.checker_run_fence) fence_table_present=true ;;
  esac
done
[[ $correlation_table_present == true ]]
[[ $fence_table_present == true ]]

obclient -h127.0.0.1 -P2881 -u"root@$tenant_name" -p"$root_password" -A \
  <"$contract_root/setup/create-principals.sql"

obclient -h127.0.0.1 -P2881 -u"blackbox_setup@$tenant_name" -pOceanBaseSetup_2026! -A \
  <"$contract_root/setup/create-schema.sql"

observed_table_count=$(obclient -N -s -h127.0.0.1 -P2881 \
  -u"blackbox_setup@$tenant_name" -pOceanBaseSetup_2026! -A -e \
  "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='benchbase' AND table_type='BASE TABLE';")
[[ $observed_table_count == "${#expected_capture_tables[@]}" ]]

for qualified_table in "${expected_capture_tables[@]}"; do
  table_name=${qualified_table#benchbase.}
  observed_table=$(obclient -N -s -h127.0.0.1 -P2881 \
    -u"blackbox_setup@$tenant_name" -pOceanBaseSetup_2026! -A -e \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='benchbase' AND table_name='$table_name' AND table_type='BASE TABLE';")
  [[ $observed_table == 1 ]]
  observed_rows=$(obclient -N -s -h127.0.0.1 -P2881 \
    -u"blackbox_setup@$tenant_name" -pOceanBaseSetup_2026! -A -e \
    "SELECT COUNT(*) FROM \`benchbase\`.\`$table_name\`;")
  [[ $observed_rows == 0 ]]
done

touch /tmp/oceanbase-mysql-schema-before-binlog.complete
