#!/usr/bin/env bash
set -Eeuo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
output=${BLACKBOX_OCEANBASE_MYSQL_ACCEPTANCE_OUTPUT_DIR:?}
benchbase_archive=${BLACKBOX_BENCHBASE_ARCHIVE:?}
monitor_bin=${BLACKBOX_MONITOR_BIN:?}
compose=(docker compose --file "$root/docker-compose.yml" --profile oceanbase_mysql)
BLACKBOX_OCEANBASE_MYSQL_CAPTURE_TABLES=$(jq -er '
  [.application_tables[], "benchbase.checker_run_fence", "benchbase.session_txn"]
  | join(",")
' "$root/protocol/adapters/ycsb.json")
export BLACKBOX_OCEANBASE_MYSQL_CAPTURE_TABLES

[[ ! -e $output ]]
[[ -f $benchbase_archive ]]
[[ -x $monitor_bin ]]
mkdir -p "$output"
distribution_parent=$(mktemp -d /tmp/oceanbase-mysql-ycsb-dist.XXXXXX)
captured=0

cleanup() {
  local status=$?
  trap - EXIT
  if (( captured == 0 )); then
    "${compose[@]}" logs --no-color --timestamps >"$output/failure-compose.log" 2>&1 || true
  fi
  timeout --foreground --kill-after=10s 180s \
    "${compose[@]}" down --volumes --remove-orphans >"$output/teardown.log" 2>&1 || true
  rm -rf -- "$distribution_parent"
  return "$status"
}
trap cleanup EXIT

tar -xzf "$benchbase_archive" -C "$distribution_parent"
distribution="$distribution_parent/benchbase-oceanbase"
config="$distribution/config/oceanbase/blackbox_ycsb_config.xml"
loader_declaration="$output/loader-declaration.json"
printf -v loader_command \
  'cd %q && exec java -jar benchbase.jar -b ycsb -c %q --create=false --load=true --execute=false --directory %q' \
  "$distribution" "$config" "$output/loader-results"
printf -v workload_command \
  'cd %q && exec java -jar benchbase.jar -b ycsb -c %q --create=false --load=false --execute=true --directory %q' \
  "$distribution" "$config" "$output/workload-results"
printf '%s\n' "$loader_command" >"$output/loader-command.txt"
printf '%s\n' "$workload_command" >"$output/workload-command.txt"
cp "$benchbase_archive" "$output/benchbase-oceanbase.tgz"
cp "$root/connector-contract/oceanbase_mysql/provenance.lock.json" "$output/provenance.lock.json"
cp "$monitor_bin" "$output/blackbox-run-monitor"
"${compose[@]}" config >"$output/docker-compose-config.yml"

set +e
BLACKBOX_BACKEND=oceanbase_mysql \
BLACKBOX_BENCHMARK=ycsb \
BLACKBOX_DECLARED_ISOLATION=REPEATABLE_READ \
BLACKBOX_EXPECTED_SESSION_IDS=ycsb/worker/1,ycsb/worker/2 \
BLACKBOX_LOADER_COMMAND="$loader_command" \
BLACKBOX_WORKLOAD_COMMAND="$workload_command" \
BLACKBOX_LOADER_DECLARED_COUNTS_FILE="$loader_declaration" \
BLACKBOX_LOADER_VALIDATION_PATH="$output/loader-validation.json" \
BLACKBOX_BOOTSTRAP_MANIFEST_PATH="$output/bootstrap-manifest.json" \
BLACKBOX_RUN_ID_PATH="$output/run-id.txt" \
BLACKBOX_MONITOR_METRICS_PATH="$output/monitor-metrics.json" \
BLACKBOX_MONITOR_BIN="$monitor_bin" \
BLACKBOX_LOADER_BATCH_SIZE=128 \
BLACKBOX_STACK_TIMEOUT_SECONDS=600 \
BLACKBOX_LOADER_TIMEOUT_SECONDS=300 \
BLACKBOX_WORKLOAD_TIMEOUT_SECONDS=120 \
BLACKBOX_STATUS_TIMEOUT_SECONDS=240 \
BLACKBOX_QUIESCENCE_TIMEOUT_SECONDS=60 \
BLACKBOX_PROCESSING_MODE=audited \
  bash "$root/scripts/e2e.sh" >"$output/e2e.log" 2>&1
e2e_status=$?
set -e
printf '%s\n' "$e2e_status" >"$output/e2e-exit-status.txt"

[[ -f $output/run-id.txt ]] || exit "$e2e_status"

run_id=$(tr -d '\r\n' <"$output/run-id.txt")
docker exec cdc-redis redis-cli --json XRANGE cdc - + >"$output/redis-cdc.json"
docker exec cdc-redis redis-cli --json XRANGE "blackbox:client:$run_id" - + \
  >"$output/redis-client.json"
docker exec cdc-redis redis-cli --json XRANGE "blackbox:status:$run_id" - + \
  >"$output/redis-status.json"
docker exec cdc-redis redis-cli --raw INFO ALL >"$output/redis-info.txt"
"${compose[@]}" ps --all --format json >"$output/compose-services.json"
"${compose[@]}" logs --no-color --timestamps >"$output/compose.log" 2>&1
for container_name in cdc-oceanbase-mysql cdc-debezium-oceanbase-mysql cdc-redis; do
  if docker inspect "$container_name" >"$output/$container_name-inspect.json" 2>/dev/null; then
    :
  else
    rm -f -- "$output/$container_name-inspect.json"
  fi
done
[[ -f $output/cdc-debezium-oceanbase-mysql-inspect.json ]] || exit "$e2e_status"
docker exec cdc-oceanbase-mysql obclient -A -c -h127.0.0.1 -P2883 \
  -ublackbox_setup@blackbox -pOceanBaseSetup_2026! -e \
  'SELECT COUNT(*) FROM benchbase.usertable; SELECT COUNT(*) FROM benchbase.session_txn; SELECT * FROM benchbase.checker_run_fence ORDER BY phase; SHOW MASTER STATUS;' \
  >"$output/database-evidence.txt"
docker exec cdc-oceanbase-mysql bash -lc \
  'curl -fsS http://127.0.0.1:2984/metrics | grep -E "^(binlog_instance_(num|dump_count|convert_checkpoint|convert_delay|dump_delay))"' \
  >"$output/binlog-telemetry.txt"
docker logs --timestamps cdc-debezium-oceanbase-mysql >"$output/connector.log" 2>&1
docker exec cdc-debezium-oceanbase-mysql bash -lc \
  'sha256sum /debezium/lib/debezium-connector-mysql-3.6.1.Final.jar /debezium/lib/mysql-connector-j-9.6.0.jar /debezium/lib/debezium-server-redis-3.6.1.Final.jar; sed -n "1,240p" /debezium/config/application.properties' \
  >"$output/connector-identity.txt"

python3 "$root/tests/verify_oceanbase_mysql_ycsb_acceptance.py" \
  --status "$output/redis-status.json" \
  --client "$output/redis-client.json" \
  --cdc "$output/redis-cdc.json" \
  --connector "$output/cdc-debezium-oceanbase-mysql-inspect.json" \
  --output "$output/acceptance-report.json"

if (( e2e_status != 0 && e2e_status != 3 )); then
  exit "$e2e_status"
fi

"${compose[@]}" down --volumes --remove-orphans >"$output/teardown.log" 2>&1
captured=1
trap - EXIT
rm -rf -- "$distribution_parent"
(
  cd "$output"
  checksum_file=$(mktemp /tmp/oceanbase-mysql-ycsb-checksums.XXXXXX)
  find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum \
    >"$checksum_file"
  mv "$checksum_file" SHA256SUMS
  sha256sum --check SHA256SUMS >/dev/null
)
