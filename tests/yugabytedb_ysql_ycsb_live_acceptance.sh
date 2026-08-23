#!/usr/bin/env bash

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUTPUT_DIR=${BLACKBOX_YCSB_ACCEPTANCE_OUTPUT_DIR:?set BLACKBOX_YCSB_ACCEPTANCE_OUTPUT_DIR}
BENCHBASE_ARCHIVE=${BLACKBOX_BENCHBASE_ARCHIVE:?set BLACKBOX_BENCHBASE_ARCHIVE}
MONITOR_BIN=${BLACKBOX_MONITOR_BIN:?set BLACKBOX_MONITOR_BIN}
COMPOSE=(docker compose --file "$ROOT/docker-compose.yml" --profile yugabytedb_ysql)
DIAGNOSTIC_TIMEOUT_SECONDS=${BLACKBOX_ACCEPTANCE_DIAGNOSTIC_TIMEOUT_SECONDS:-60}
CLEANUP_TIMEOUT_SECONDS=${BLACKBOX_ACCEPTANCE_CLEANUP_TIMEOUT_SECONDS:-90}
KILL_AFTER_SECONDS=${BLACKBOX_ACCEPTANCE_KILL_AFTER_SECONDS:-10}

[[ ! -e $OUTPUT_DIR ]]
[[ -f $BENCHBASE_ARCHIVE ]]
[[ -x $MONITOR_BIN ]]
[[ $DIAGNOSTIC_TIMEOUT_SECONDS =~ ^[1-9][0-9]*$ ]]
[[ $CLEANUP_TIMEOUT_SECONDS =~ ^[1-9][0-9]*$ ]]
[[ $KILL_AFTER_SECONDS =~ ^[1-9][0-9]*$ ]]
mkdir -p "$OUTPUT_DIR"
DIST_PARENT=$(mktemp -d /tmp/yugabytedb-ycsb-ordinary-dist.XXXXXX)
CAPTURED=0

run_diagnostic() {
  timeout --foreground --kill-after="${KILL_AFTER_SECONDS}s" \
    "${DIAGNOSTIC_TIMEOUT_SECONDS}s" "$@"
}

run_cleanup() {
  timeout --foreground --kill-after="${KILL_AFTER_SECONDS}s" \
    "${CLEANUP_TIMEOUT_SECONDS}s" "$@"
}

write_checksums() {
  local manifest=$1
  local checksum_file
  checksum_file=$(mktemp /tmp/yugabytedb-ycsb-acceptance-checksums.XXXXXX)
  (
    cd "$OUTPUT_DIR"
    find . -type f ! -name SHA256SUMS ! -name FAILED-SHA256SUMS -print0 \
      | LC_ALL=C sort -z | xargs -0 sha256sum
  ) >"$checksum_file"
  mv -- "$checksum_file" "$OUTPUT_DIR/$manifest"
  (cd "$OUTPUT_DIR" && sha256sum -c "$manifest" >/dev/null)
  sha256sum "$OUTPUT_DIR/$manifest"
}

cleanup() {
  local status=$?
  trap - EXIT
  if (( CAPTURED == 0 )); then
    run_diagnostic "${COMPOSE[@]}" logs --no-color --timestamps \
      >"$OUTPUT_DIR/failure-compose.log" 2>&1 || true
  fi
  run_cleanup "${COMPOSE[@]}" down --volumes --remove-orphans \
    >"$OUTPUT_DIR/teardown.log" 2>&1 || true
  rm -rf -- "$DIST_PARENT"
  return "$status"
}
trap cleanup EXIT

tar -xzf "$BENCHBASE_ARCHIVE" -C "$DIST_PARENT"
DIST="$DIST_PARENT/benchbase-postgres"
CONFIG="$DIST/config/yugabytedb/blackbox_ycsb_config.xml"
LOADER_DECLARATION="$OUTPUT_DIR/loader-declaration.json"
printf -v LOADER_COMMAND \
  'cd %q && exec java -jar benchbase.jar -b ycsb -c %q --create=false --load=true --execute=false --directory %q' \
  "$DIST" "$CONFIG" "$OUTPUT_DIR/loader-results"
printf -v WORKLOAD_COMMAND \
  'cd %q && exec java -jar benchbase.jar -b ycsb -c %q --create=false --load=false --execute=true --directory %q' \
  "$DIST" "$CONFIG" "$OUTPUT_DIR/workload-results"
printf '%s\n' "$LOADER_COMMAND" >"$OUTPUT_DIR/loader-command.txt"
printf '%s\n' "$WORKLOAD_COMMAND" >"$OUTPUT_DIR/workload-command.txt"
cp "$BENCHBASE_ARCHIVE" "$OUTPUT_DIR/benchbase-postgres.tgz"
cp "$ROOT/connector-contract/yugabytedb_ysql/provenance.lock.json" \
  "$OUTPUT_DIR/provenance.lock.json"
cp "$MONITOR_BIN" "$OUTPUT_DIR/blackbox-run-monitor"
run_diagnostic "${COMPOSE[@]}" config >"$OUTPUT_DIR/docker-compose-config.yml"

if BLACKBOX_BACKEND=yugabytedb_ysql \
  BLACKBOX_BENCHMARK=ycsb \
  BLACKBOX_DECLARED_ISOLATION=REPEATABLE_READ \
  BLACKBOX_EXPECTED_SESSION_IDS=ycsb/worker/1,ycsb/worker/2 \
  BLACKBOX_LOADER_COMMAND="$LOADER_COMMAND" \
  BLACKBOX_WORKLOAD_COMMAND="$WORKLOAD_COMMAND" \
  BLACKBOX_LOADER_DECLARED_COUNTS_FILE="$LOADER_DECLARATION" \
  BLACKBOX_LOADER_VALIDATION_PATH="$OUTPUT_DIR/loader-validation.json" \
  BLACKBOX_BOOTSTRAP_MANIFEST_PATH="$OUTPUT_DIR/bootstrap-manifest.json" \
  BLACKBOX_RUN_ID_PATH="$OUTPUT_DIR/run-id.txt" \
  BLACKBOX_MONITOR_METRICS_PATH="$OUTPUT_DIR/monitor-metrics.json" \
  BLACKBOX_MONITOR_BIN="$MONITOR_BIN" \
  BLACKBOX_LOADER_BATCH_SIZE=128 \
  BLACKBOX_STACK_TIMEOUT_SECONDS=240 \
  BLACKBOX_LOADER_TIMEOUT_SECONDS=600 \
  BLACKBOX_WORKLOAD_TIMEOUT_SECONDS=180 \
  BLACKBOX_STATUS_TIMEOUT_SECONDS=240 \
  BLACKBOX_QUIESCENCE_TIMEOUT_SECONDS=60 \
  BLACKBOX_PROCESSING_MODE=audited \
  bash "$ROOT/scripts/e2e.sh" >"$OUTPUT_DIR/e2e.log" 2>&1; then
  E2E_STATUS=0
else
  E2E_STATUS=$?
fi
printf '%s\n' "$E2E_STATUS" >"$OUTPUT_DIR/e2e-exit-status.txt"

RUN_ID=$(tr -d '\r\n' <"$OUTPUT_DIR/run-id.txt")
run_diagnostic docker exec cdc-redis redis-cli --json XRANGE cdc - + >"$OUTPUT_DIR/redis-cdc.json"
run_diagnostic docker exec cdc-redis redis-cli --raw XRANGE cdc - + >"$OUTPUT_DIR/redis-cdc.raw.txt"
run_diagnostic docker exec cdc-redis redis-cli --json XRANGE "blackbox:client:$RUN_ID" - + \
  >"$OUTPUT_DIR/redis-client.json"
run_diagnostic docker exec cdc-redis redis-cli --raw XRANGE "blackbox:client:$RUN_ID" - + \
  >"$OUTPUT_DIR/redis-client.raw.txt"
run_diagnostic docker exec cdc-redis redis-cli --json XRANGE "blackbox:status:$RUN_ID" - + \
  >"$OUTPUT_DIR/redis-status.json"
run_diagnostic docker exec cdc-redis redis-cli --raw XRANGE "blackbox:status:$RUN_ID" - + \
  >"$OUTPUT_DIR/redis-status.raw.txt"
run_diagnostic docker exec cdc-redis redis-cli --raw INFO ALL >"$OUTPUT_DIR/redis-info.txt"
run_diagnostic "${COMPOSE[@]}" ps --all --format json >"$OUTPUT_DIR/compose-services.json"
run_diagnostic "${COMPOSE[@]}" logs --no-color --timestamps >"$OUTPUT_DIR/compose.log" 2>&1
for container_name in \
  cdc-yugabytedb-ysql cdc-yugabytedb-ysql-init \
  cdc-debezium-yugabytedb-ysql cdc-redis; do
  run_diagnostic docker inspect "$container_name" >"$OUTPUT_DIR/$container_name-inspect.json"
done
DATABASE_IMAGE=$(jq -er '.artifacts.database.image' "$OUTPUT_DIR/provenance.lock.json")
CONNECTOR_IMAGE=$(jq -er '.artifacts.runtime_connector_image.image' "$OUTPUT_DIR/provenance.lock.json")
REDIS_IMAGE=$(jq -er '.artifacts.redis.image' "$OUTPUT_DIR/provenance.lock.json")
run_diagnostic docker image inspect "$DATABASE_IMAGE" \
  >"$OUTPUT_DIR/database-image.json"
run_diagnostic docker image inspect "$CONNECTOR_IMAGE" \
  >"$OUTPUT_DIR/connector-image.json"
run_diagnostic docker image inspect "$REDIS_IMAGE" \
  >"$OUTPUT_DIR/redis-image.json"
run_diagnostic docker exec cdc-yugabytedb-ysql bin/ysqlsh -h yugabytedb_ysql -U yugabyte \
  -d benchbase --no-psqlrc --set ON_ERROR_STOP=on --echo-all --command \
  "SELECT version(); SELECT current_setting('default_transaction_isolation'), current_setting('yb_read_from_followers'), current_setting('TimeZone'), current_setting('search_path'); SELECT * FROM yb_table_properties('benchbase.usertable'::regclass); SELECT pubname,schemaname,tablename FROM pg_publication_tables ORDER BY 1,2,3; SELECT slot_name,plugin,database,active,active_pid,yb_lsn_type,confirmed_flush_lsn FROM pg_replication_slots ORDER BY slot_name; SELECT n.nspname,c.relname,c.relreplident::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='benchbase' AND c.relkind='r' ORDER BY c.relname; SELECT 'usertable' AS table_name,count(*) FROM benchbase.usertable UNION ALL SELECT 'checker_run_fence',count(*) FROM benchbase.checker_run_fence UNION ALL SELECT 'session_txn',count(*) FROM benchbase.session_txn; SELECT * FROM benchbase.checker_run_fence ORDER BY phase; SELECT * FROM benchbase.session_txn ORDER BY session_id;" \
  >"$OUTPUT_DIR/database-evidence.txt" 2>&1
run_diagnostic docker exec cdc-yugabytedb-ysql bin/ysqlsh -h yugabytedb_ysql -U yugabyte \
  -d benchbase --no-psqlrc --set ON_ERROR_STOP=on --tuples-only --no-align \
  --command "SELECT num_tablets FROM yb_table_properties('benchbase.usertable'::regclass)" \
  >"$OUTPUT_DIR/usertable-tablet-count.txt"
for table_name in usertable checker_run_fence session_txn; do
  run_diagnostic docker exec cdc-yugabytedb-ysql bin/yb-admin \
    --master_addresses yugabytedb_ysql:7100 \
    list_tablets ysql.benchbase "$table_name" 0 \
    >"$OUTPUT_DIR/$table_name-tablets.txt"
done
run_diagnostic docker exec cdc-yugabytedb-ysql bin/yb-admin --version \
  >"$OUTPUT_DIR/database-build.txt"
run_diagnostic docker logs --timestamps cdc-debezium-yugabytedb-ysql \
  >"$OUTPUT_DIR/connector.log" 2>&1
run_diagnostic docker exec cdc-debezium-yugabytedb-ysql sh -lc \
  'sha256sum /debezium/lib/yugabytedb-source-connector-*.jar; sed -n "1,240p" /debezium/conf/application.properties' \
  >"$OUTPUT_DIR/connector-identity.txt"

if (( E2E_STATUS != 0 && E2E_STATUS != 3 )); then
  run_cleanup "${COMPOSE[@]}" down --volumes --remove-orphans \
    >"$OUTPUT_DIR/teardown.log" 2>&1 || true
  CAPTURED=1
  trap - EXIT
  rm -rf -- "$DIST_PARENT"
  write_checksums FAILED-SHA256SUMS
  exit "$E2E_STATUS"
fi

OUTCOMES=$(find "$OUTPUT_DIR/workload-results" -name '*.outcomes.json' -print -quit)
[[ -n $OUTCOMES ]]
python3 "$ROOT/tests/verify_yugabytedb_ysql_ycsb_ordinary.py" \
  --monitor "$OUTPUT_DIR/monitor-metrics.json" \
  --client "$OUTPUT_DIR/redis-client.json" \
  --cdc "$OUTPUT_DIR/redis-cdc.json" \
  --status "$OUTPUT_DIR/redis-status.json" \
  --outcomes "$OUTCOMES" \
  --bootstrap "$OUTPUT_DIR/bootstrap-manifest.json" \
  --loader-validation "$OUTPUT_DIR/loader-validation.json" \
  --tablet-count "$OUTPUT_DIR/usertable-tablet-count.txt" \
  --provenance "$OUTPUT_DIR/provenance.lock.json" \
  --connector-container "$OUTPUT_DIR/cdc-debezium-yugabytedb-ysql-inspect.json" \
  --connector-image "$OUTPUT_DIR/connector-image.json" \
  --connector-identity "$OUTPUT_DIR/connector-identity.txt" \
  --output "$OUTPUT_DIR/acceptance-report.json"
(( E2E_STATUS == 0 || E2E_STATUS == 3 ))

run_cleanup "${COMPOSE[@]}" down --volumes --remove-orphans >"$OUTPUT_DIR/teardown.log" 2>&1
CAPTURED=1
trap - EXIT
rm -rf -- "$DIST_PARENT"
write_checksums SHA256SUMS
