#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
readonly output_dir=${1:-"$script_dir/fixtures/pgoutput_production_assets_gate"}
if [[ -e $output_dir ]]; then
  echo "production-asset gate output already exists: $output_dir" >&2
  exit 2
fi
mkdir -p -- "$output_dir"

compose() {
  docker compose --file "$script_dir/docker-compose.yml" \
    --file "$script_dir/docker-compose.production-assets.yml" "$@"
}

cleanup_gate() {
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup_gate EXIT

wait_for_streaming() {
  local deadline=$((SECONDS + 180))
  while ((SECONDS < deadline)); do
    local active
    active=$(compose exec -T yugabytedb_ysql bin/ysqlsh \
      --host yugabytedb_ysql --username yugabyte --dbname benchbase \
      --tuples-only --no-align --command \
      "SELECT count(*) FROM pg_replication_slots WHERE slot_name='production_asset_slot' AND plugin='pgoutput' AND database='benchbase' AND yb_lsn_type='SEQUENCE' AND active AND active_pid IS NOT NULL" \
      | tr -d '\r') || true
    if [[ $active == 1 ]]; then
      return 0
    fi
    sleep 1
  done
  echo "production connector did not activate the admitted slot" >&2
  return 1
}

wait_for_end() {
  local deadline=$((SECONDS + 120))
  while ((SECONDS < deadline)); do
    local ends
    ends=$(compose exec -T redis redis-cli --raw XRANGE cdc - + \
      | grep -c '"status":"END"' || true)
    if ((ends == 1)); then
      return 0
    fi
    sleep 1
  done
  echo "production connector did not deliver one complete transaction" >&2
  return 1
}

compose down --volumes --remove-orphans
compose up --detach --build
wait_for_streaming

compose exec -T redis redis-cli --raw DBSIZE \
  >"$output_dir/startup-redis-dbsize.txt"
[[ $(tr -d '[:space:]' <"$output_dir/startup-redis-dbsize.txt") == 0 ]]
compose exec -T yugabytedb_ysql bin/ysqlsh \
  --host yugabytedb_ysql --username yugabyte --dbname benchbase \
  --no-psqlrc --set ON_ERROR_STOP=on --command \
  "SELECT count(*) AS publication_count FROM pg_publication; SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname='production_asset_publication' ORDER BY 1,2; SELECT slot_name, plugin, database, yb_lsn_type, active, active_pid FROM pg_replication_slots; SELECT n.nspname, c.relname, c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='benchbase' AND c.relkind='r' ORDER BY c.relname; SELECT (SELECT count(*) FROM benchbase.usertable) AS application_rows, (SELECT count(*) FROM benchbase.checker_run_fence) AS fence_rows, (SELECT count(*) FROM benchbase.session_txn) AS marker_rows;" \
  >"$output_dir/startup-catalog.txt"
grep -Eq '^\s*0\s*\|\s*0\s*\|\s*0\s*$' "$output_dir/startup-catalog.txt"
compose exec -T yugabytedb_ysql bin/ysqlsh \
  --host yugabytedb_ysql --username yugabyte --dbname benchbase \
  --no-psqlrc --set ON_ERROR_STOP=on --tuples-only --no-align \
  --field-separator=$'\t' --command \
  "SELECT 'DATABASE', current_database(), pg_encoding_to_char(encoding), datcollate FROM pg_database WHERE datname='benchbase'; SELECT 'TABLE', n.nspname || '.' || c.relname, c.relkind::text, c.relreplident::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='benchbase' AND c.relkind='r' ORDER BY c.relname; SELECT 'COLUMN', table_schema || '.' || table_name, column_name, ordinal_position, data_type, is_nullable, COALESCE(numeric_precision::text, '-'), COALESCE(numeric_scale::text, '-'), COALESCE(character_maximum_length::text, '-'), COALESCE(datetime_precision::text, '-'), '-', '-', CASE WHEN is_identity='YES' THEN 'identity' ELSE '-' END FROM information_schema.columns WHERE table_schema='benchbase' ORDER BY table_name, ordinal_position; SELECT 'PRIMARY', n.nspname || '.' || c.relname, a.attname, keys.ordinality FROM pg_constraint p JOIN pg_class c ON c.oid=p.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL unnest(p.conkey) WITH ORDINALITY AS keys(attnum, ordinality) JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=keys.attnum WHERE p.contype='p' AND n.nspname='benchbase' ORDER BY c.relname, keys.ordinality;" \
  >"$output_dir/startup-schema-metadata.tsv"
python3 "$script_dir/../../scripts/backends/yugabytedb_ysql_catalog.py" \
  --benchmark ycsb --emit-tables \
  <"$output_dir/startup-schema-metadata.tsv" \
  >"$output_dir/startup-normalized-tables.json"
compose exec -T -e PGPASSWORD=blackbox_application_password yugabytedb_ysql \
  bin/ysqlsh --host yugabytedb_ysql --username blackbox_application \
  --dbname benchbase --no-psqlrc --set ON_ERROR_STOP=on \
  --tuples-only --no-align --field-separator=$'\t' --command \
  "SELECT current_setting('client_encoding'), current_setting('TimeZone'), current_setting('search_path'), current_setting('default_transaction_isolation'), current_setting('yb_read_from_followers')" \
  >"$output_dir/startup-session-settings.tsv"
[[ $(tr -d '\r' <"$output_dir/startup-session-settings.tsv") == $'UTF8\tUTC\tbenchbase\trepeatable read\toff' ]]

(
  export BLACKBOX_BACKEND=yugabytedb_ysql
  export BLACKBOX_BENCHMARK=ycsb
  export BLACKBOX_DOCKER=docker
  export BLACKBOX_YSQL_SLOT_NAME=production_asset_slot
  export BLACKBOX_YSQL_PUBLICATION_NAME=production_asset_publication
  export BLACKBOX_YSQL_CAPTURE_TABLES=benchbase.usertable,benchbase.checker_run_fence,benchbase.session_txn
  # shellcheck source=scripts/blackbox_lifecycle.sh
  . "$script_dir/../../scripts/blackbox_lifecycle.sh"
  BLACKBOX_DATABASE_CONTAINER=connector-contract-yugabytedb-ysql
  BLACKBOX_CONNECTOR_CONTAINER=connector-contract-debezium-server
  blackbox_compose() {
    docker compose --file "$script_dir/docker-compose.yml" \
      --file "$script_dir/docker-compose.production-assets.yml" "$@"
  }
  wait_for_no_external_sessions() {
    local deadline=$((SECONDS + 5))
    while ((SECONDS < deadline)); do
      [[ $(blackbox_backend_active_external_connections | tr -d '[:space:]') == 0 ]] &&
        return 0
      sleep 0.05
    done
    blackbox_backend_admin_query \
      "SELECT pid, datname, usename, application_name, state, left(query, 160) FROM pg_stat_activity WHERE pid <> pg_backend_pid() ORDER BY datname, usename, pid" \
      >"$output_dir/external-session-timeout.txt" || true
    return 1
  }

  blackbox_backend_verify_schema
  blackbox_backend_capture_security_catalog \
    >"$output_dir/security-catalog-startup.txt"
  wait_for_no_external_sessions

  blackbox_compose exec -T -e PGPASSWORD=blackbox_setup_password \
    -e PGAPPNAME=privilege_gate_setup \
    yugabytedb_ysql bin/ysqlsh --host yugabytedb_ysql \
    --username blackbox_setup --dbname benchbase --no-psqlrc \
    --set ON_ERROR_STOP=on --command "SELECT pg_sleep(10)" \
    >"$output_dir/external-ddl-session.log" 2>&1 &
  external_session_pid=$!
  blackbox_compose exec -T -e PGAPPNAME=privilege_gate_admin \
    yugabytedb_ysql bin/ysqlsh --host yugabytedb_ysql \
    --username yugabyte --dbname yugabyte --no-psqlrc \
    --set ON_ERROR_STOP=on --command "SELECT pg_sleep(10)" \
    >"$output_dir/external-admin-session.log" 2>&1 &
  external_admin_pid=$!
  external_count=0
  deadline=$((SECONDS + 5))
  while ((SECONDS < deadline)); do
    external_count=$(blackbox_backend_active_external_connections | tr -d '[:space:]')
    (( external_count >= 2 )) && break
    sleep 0.05
  done
  printf '%s\n' "$external_count" >"$output_dir/external-ddl-session-count.txt"
  (( external_count >= 2 ))
  blackbox_backend_admin_query \
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name IN ('privilege_gate_setup','privilege_gate_admin') AND pid <> pg_backend_pid()" \
    >/dev/null
  wait "$external_session_pid" 2>/dev/null || true
  wait "$external_admin_pid" 2>/dev/null || true
  wait_for_no_external_sessions

  blackbox_backend_admin_query \
    "GRANT CREATE ON SCHEMA benchbase TO blackbox_application" >/dev/null
  blackbox_backend_capture_security_catalog \
    >"$output_dir/security-catalog-with-unauthorized-create.txt"
  if blackbox_backend_verify_schema \
      >"$output_dir/privilege-negative.log" 2>&1; then
    echo "schema CREATE privilege unexpectedly passed verification" >&2
    exit 1
  fi
  blackbox_backend_admin_query \
    "REVOKE CREATE ON SCHEMA benchbase FROM blackbox_application" >/dev/null
  blackbox_backend_verify_schema
  blackbox_backend_capture_security_catalog \
    >"$output_dir/security-catalog-restored.txt"
  cmp "$output_dir/security-catalog-startup.txt" \
    "$output_dir/security-catalog-restored.txt"
  sha256sum \
    "$output_dir/security-catalog-startup.txt" \
    "$output_dir/security-catalog-with-unauthorized-create.txt" \
    "$output_dir/security-catalog-restored.txt" \
    >"$output_dir/security-catalog-digests.txt"
  startup_digest=$(sha256sum "$output_dir/security-catalog-startup.txt" | cut -d' ' -f1)
  unauthorized_digest=$(sha256sum \
    "$output_dir/security-catalog-with-unauthorized-create.txt" | cut -d' ' -f1)
  [[ $startup_digest != "$unauthorized_digest" ]]
)

compose exec -T -e PGPASSWORD=blackbox_application_password yugabytedb_ysql \
  bin/ysqlsh --host yugabytedb_ysql --username blackbox_application \
  --dbname benchbase --no-psqlrc --set ON_ERROR_STOP=on \
  --file /connector-contract/workload/cross_tablet_marker_last.sql \
  >"$output_dir/workload.log" 2>&1
wait_for_end

compose exec -T redis redis-cli --json XRANGE cdc - + \
  >"$output_dir/redis-stream.json"
compose exec -T redis redis-cli --raw KEYS '*' \
  | LC_ALL=C sort >"$output_dir/redis-streams.txt"
compose exec -T yugabytedb_ysql bin/ysqlsh \
  --host yugabytedb_ysql --username yugabyte --dbname benchbase \
  --no-psqlrc --set ON_ERROR_STOP=on --command \
  "SELECT version(); SELECT rolname, rolsuper, rolcreaterole, rolcreatedb, rolreplication FROM pg_roles WHERE rolname IN ('blackbox_setup','blackbox_application','blackbox_replication') ORDER BY rolname; SELECT count(*) AS publication_count FROM pg_publication; SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname='production_asset_publication' ORDER BY 1,2; SELECT slot_name, plugin, database, yb_lsn_type, active, active_pid FROM pg_replication_slots; SELECT n.nspname, c.relname, c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='benchbase' AND c.relkind='r' ORDER BY c.relname; SELECT table_name, column_name, ordinal_position, data_type, is_nullable, character_maximum_length FROM information_schema.columns WHERE table_schema='benchbase' ORDER BY table_name, ordinal_position; SELECT c.relname, a.attname, keys.ordinality FROM pg_constraint p JOIN pg_class c ON c.oid=p.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL unnest(p.conkey) WITH ORDINALITY AS keys(attnum, ordinality) JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=keys.attnum WHERE p.contype='p' AND n.nspname='benchbase' ORDER BY c.relname, keys.ordinality; SELECT (SELECT count(*) FROM benchbase.usertable) AS application_rows, (SELECT count(*) FROM benchbase.checker_run_fence) AS fence_rows, (SELECT count(*) FROM benchbase.session_txn) AS marker_rows;" \
  >"$output_dir/database-catalog.txt"
compose exec -T yugabytedb_ysql bin/yb-admin --version \
  >"$output_dir/database-build.txt"
compose logs --no-color debezium_server >"$output_dir/debezium-server.log"
compose logs --no-color initialize >"$output_dir/initialize.log"
compose ps --all --format json >"$output_dir/compose-services.json"
compose config >"$output_dir/docker-compose-config.yml"
docker image inspect db-monitoring/yugabytedb-ysql:dz.2.5.2.yb.2026.1 \
  >"$output_dir/connector-image.json"
# shellcheck disable=SC2016
compose exec -T debezium_server sh -lc '
  set -eu
  jar=$(find /debezium/lib -maxdepth 1 -name "yugabytedb-source-connector-*.jar")
  test "$(printf "%s\n" "$jar" | grep -c .)" -eq 1
  test "$(find /debezium/lib -maxdepth 1 -name "debezium-connector-postgres-*.jar" | wc -l)" -eq 0
  sha256sum "$jar"
' >"$output_dir/connector-classpath.txt"
cp -- "$script_dir/provenance.lock.json" "$output_dir/provenance.lock.json"

python3 "$script_dir/verify_raw_contract.py" --plugin pgoutput \
  --redis-batch-size 2 "$output_dir/redis-stream.json"
(
  cd -- "$output_dir"
  # SHA256SUMS is explicitly excluded from its own input roster.
  # shellcheck disable=SC2094
  find . -maxdepth 1 -type f ! -name SHA256SUMS -printf '%f\0' \
    | LC_ALL=C sort -z | xargs -0 sha256sum >SHA256SUMS
  sha256sum --check SHA256SUMS
)
compose down --volumes --remove-orphans
trap - EXIT
echo "production YugabyteDB asset gate evidence preserved at $output_dir"
