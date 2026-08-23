#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
readonly scenario=${1:-cross_tablet_marker_last}
readonly redis_batch_size=${2:-2}
readonly source_batch_size=${CONNECTOR_CONTRACT_SOURCE_BATCH_SIZE:-2}
readonly backpressure_ms=5000
capture_timestamp=$(date -u +%Y%m%dT%H%M%SZ)
readonly capture_timestamp
readonly output_dir=${3:-"$script_dir/fixtures/pgoutput-${scenario}-${capture_timestamp}"}
readonly plugin=pgoutput
readonly cdc_codec_replay_bin=${BLACKBOX_CDC_CODEC_REPLAY_BIN:-}

case "$scenario" in
  cross_tablet_marker_last | concurrent_commit_order | redis_backpressure | ycsb_value_normalization) ;;
  *)
    echo "unsupported capture scenario: $scenario" >&2
    exit 2
    ;;
esac
if [[ ! "$redis_batch_size" =~ ^[1-9][0-9]*$ ]]; then
  echo "redis_batch_size must be a positive integer" >&2
  exit 2
fi
if [[ ! "$source_batch_size" =~ ^[1-9][0-9]*$ ]]; then
  echo "CONNECTOR_CONTRACT_SOURCE_BATCH_SIZE must be a positive integer" >&2
  exit 2
fi
if [[ -e "$output_dir" ]]; then
  echo "capture output already exists: $output_dir" >&2
  exit 2
fi
if [[ $scenario == ycsb_value_normalization &&
      ( -z $cdc_codec_replay_bin || ! -x $cdc_codec_replay_bin ) ]]; then
  echo "BLACKBOX_CDC_CODEC_REPLAY_BIN must name the built codec replay tool" >&2
  exit 2
fi

compose() {
  CONNECTOR_CONTRACT_PLUGIN="$plugin" \
  CONNECTOR_CONTRACT_REDIS_BATCH_SIZE="$redis_batch_size" \
  CONNECTOR_CONTRACT_SOURCE_BATCH_SIZE="$source_batch_size" \
    docker compose --file "$script_dir/docker-compose.yml" "$@"
}

ysql_application() {
  local application_name=$1
  shift
  compose exec -T \
    -e PGPASSWORD=connector_contract_application_password \
    -e PGAPPNAME="$application_name" \
    yugabytedb_ysql \
    bin/ysqlsh \
    --host yugabytedb_ysql \
    --username connector_contract_application \
    --dbname benchbase \
    --no-psqlrc \
    --set ON_ERROR_STOP=on \
    "$@"
}

ysql_admin() {
  compose exec -T yugabytedb_ysql \
    bin/ysqlsh \
    --host yugabytedb_ysql \
    --username yugabyte \
    --dbname benchbase \
    --no-psqlrc \
    --set ON_ERROR_STOP=on \
    "$@"
}

wait_for_streaming() {
  local deadline=$((SECONDS + 180))
  while ((SECONDS < deadline)); do
    if ysql_admin --tuples-only --no-align --command \
        "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'connector_contract_slot' AND plugin = '$plugin' AND yb_lsn_type = 'SEQUENCE' AND active AND active_pid IS NOT NULL" \
        | tr -d '\r' | grep -qx '1'; then
      return
    fi
    sleep 1
  done
  echo "connector did not establish the one expected active SEQUENCE slot" >&2
  return 1
}

wait_for_application_query() {
  local application_name=$1
  local query_pattern=$2
  local deadline=$((SECONDS + 20))
  while ((SECONDS < deadline)); do
    if ysql_admin --tuples-only --no-align --command \
        "SELECT count(*) FROM pg_stat_activity WHERE application_name = '$application_name' AND state = 'active' AND query LIKE '$query_pattern'" \
        | tr -d '\r' | grep -qx '1'; then
      return
    fi
    sleep 0.05
  done
  echo "did not observe $application_name executing $query_pattern" >&2
  return 1
}

start_connector_output_monitor() {
  : >"$output_dir/redis-monitor.log"
  compose exec -T redis redis-cli --raw MONITOR \
    >"$output_dir/redis-monitor.log" 2>&1 &
  connector_monitor_pid=$!
  local deadline=$((SECONDS + 20))
  while ((SECONDS < deadline)); do
    if grep -qx 'OK' "$output_dir/redis-monitor.log"; then
      return
    fi
    if ! kill -0 "$connector_monitor_pid" 2>/dev/null; then
      echo "Redis MONITOR exited before acknowledging capture" >&2
      return 1
    fi
    sleep 0.05
  done
  echo "timed out waiting for Redis MONITOR" >&2
  return 1
}

stop_connector_output_monitor() {
  if [[ -n ${connector_monitor_pid:-} ]] && kill -0 "$connector_monitor_pid" 2>/dev/null; then
    local monitor_client_id
    monitor_client_id=$(compose exec -T redis redis-cli --raw CLIENT LIST 2>/dev/null |
      awk '/(^| )cmd=monitor( |$)/ { for (i = 1; i <= NF; ++i) if ($i ~ /^id=/) { sub(/^id=/, "", $i); print $i } }')
    if [[ $monitor_client_id =~ ^[1-9][0-9]*$ ]]; then
      compose exec -T redis redis-cli --raw CLIENT KILL ID "$monitor_client_id" \
        >/dev/null 2>&1 || true
    else
      kill "$connector_monitor_pid" 2>/dev/null || true
    fi
    wait "$connector_monitor_pid" 2>/dev/null || true
  fi
  connector_monitor_pid=
}

redis_stream_length() {
  compose exec -T redis redis-cli --raw XLEN cdc | tr -d '\r[:space:]'
}

wait_for_paused_connector_write() {
  local deadline=$((SECONDS + 4))
  : >"$output_dir/redis-paused-clients.txt"
  while ((SECONDS < deadline)); do
    compose exec -T redis redis-cli --raw CLIENT LIST \
      >"$output_dir/redis-paused-clients.txt"
    if grep -Eqi '(^| )cmd=xadd( |$)' "$output_dir/redis-paused-clients.txt"; then
      connector_write_observed_ns=$(date +%s%N)
      return
    fi
    sleep 0.05
  done
  echo "connector XADD did not overlap the Redis WRITE pause" >&2
  return 1
}

wait_for_ends() {
  local expected=$1
  local deadline=$((SECONDS + 180))
  while ((SECONDS < deadline)); do
    local ends
    ends=$(compose exec -T redis redis-cli --raw XRANGE cdc - + \
      | grep -c '"status":"END"' || true)
    if ((ends >= expected)); then
      return
    fi
    sleep 1
  done
  echo "timed out waiting for $expected completed transactions" >&2
  return 1
}

run_cross_tablet_marker_last() {
  ysql_application contract_cross_tablet \
    --file /connector-contract/workload/cross_tablet_marker_last.sql \
    >"$output_dir/workload.log" 2>&1
  commit_order_logs=("$output_dir/workload.log")
}

run_concurrent_commit_order() {
  ysql_application contract_commit_a \
    --file /connector-contract/workload/contract_transactions.sql \
    --set ycsb_key=201 \
    --set field1=commit-a \
    --set delay_seconds=4 \
    --set session_id=commit_a \
    --set txn_id=1 \
    >"$output_dir/commit-a.log" 2>&1 &
  local commit_a_pid=$!

  wait_for_application_query contract_commit_a 'SELECT pg_sleep%'
  ysql_application contract_commit_b \
    --file /connector-contract/workload/contract_transactions.sql \
    --set ycsb_key=202 \
    --set field1=commit-b \
    --set delay_seconds=0 \
    --set session_id=commit_b \
    --set txn_id=1 \
    >"$output_dir/commit-b.log" 2>&1
  wait "$commit_a_pid"
  commit_order_logs=("$output_dir/commit-b.log" "$output_dir/commit-a.log")
}

run_redis_backpressure() {
  local workload_pid
  ysql_application contract_backpressure \
    --file /connector-contract/workload/backpressure_commit_order.sql \
    --set start_delay_seconds=2 \
    >"$output_dir/workload.log" 2>&1 &
  workload_pid=$!
  wait_for_application_query contract_backpressure 'SELECT pg_sleep%'

  stream_length_before=$(redis_stream_length)
  [[ $stream_length_before == 0 ]] || {
    echo "Redis stream was not empty before backpressure" >&2
    return 1
  }
  pause_started_ns=$(date +%s%N)
  compose exec -T redis redis-cli CLIENT PAUSE "$backpressure_ms" WRITE \
    >"$output_dir/redis-backpressure.log" 2>&1
  wait "$workload_pid"
  workload_committed_ns=$(date +%s%N)
  wait_for_paused_connector_write
  stream_length_during=$(redis_stream_length)
  [[ $stream_length_during == "$stream_length_before" ]] || {
    echo "Redis stream advanced while connector XADD was paused" >&2
    return 1
  }
  compose exec -T redis redis-cli CLIENT UNPAUSE \
    >>"$output_dir/redis-backpressure.log" 2>&1
  unpause_ns=$(date +%s%N)
  commit_order_logs=("$output_dir/workload.log")
}

run_ycsb_value_normalization() {
  jdbc_build_dir=$(mktemp -d /tmp/yugabytedb-ycsb-normalization-jdbc-build.XXXXXX)
  ysql_application ycsb_value_normalization \
    --file /connector-contract/workload/ycsb_value_normalization.sql \
    >"$output_dir/workload.log" 2>&1
  commit_order_logs=("$output_dir/workload.log")
  ysql_application ycsb_value_normalization_scan \
    --tuples-only --no-align --field-separator=$'\t' --command "
      SELECT ycsb_key,
             CASE WHEN field1 IS NULL THEN 'N' ELSE 'P' || replace(replace(encode(convert_to(field1, 'UTF8'), 'base64'), E'\\n', ''), E'\\r', '') END,
             CASE WHEN field2 IS NULL THEN 'N' ELSE 'P' || replace(replace(encode(convert_to(field2, 'UTF8'), 'base64'), E'\\n', ''), E'\\r', '') END,
             CASE WHEN field3 IS NULL THEN 'N' ELSE 'P' || replace(replace(encode(convert_to(field3, 'UTF8'), 'base64'), E'\\n', ''), E'\\r', '') END,
             CASE WHEN field4 IS NULL THEN 'N' ELSE 'P' || replace(replace(encode(convert_to(field4, 'UTF8'), 'base64'), E'\\n', ''), E'\\r', '') END,
             CASE WHEN field5 IS NULL THEN 'N' ELSE 'P' || replace(replace(encode(convert_to(field5, 'UTF8'), 'base64'), E'\\n', ''), E'\\r', '') END,
             CASE WHEN field6 IS NULL THEN 'N' ELSE 'P' || replace(replace(encode(convert_to(field6, 'UTF8'), 'base64'), E'\\n', ''), E'\\r', '') END,
             CASE WHEN field7 IS NULL THEN 'N' ELSE 'P' || replace(replace(encode(convert_to(field7, 'UTF8'), 'base64'), E'\\n', ''), E'\\r', '') END,
             CASE WHEN field8 IS NULL THEN 'N' ELSE 'P' || replace(replace(encode(convert_to(field8, 'UTF8'), 'base64'), E'\\n', ''), E'\\r', '') END,
             CASE WHEN field9 IS NULL THEN 'N' ELSE 'P' || replace(replace(encode(convert_to(field9, 'UTF8'), 'base64'), E'\\n', ''), E'\\r', '') END,
             CASE WHEN field10 IS NULL THEN 'N' ELSE 'P' || replace(replace(encode(convert_to(field10, 'UTF8'), 'base64'), E'\\n', ''), E'\\r', '') END
        FROM benchbase.usertable ORDER BY ycsb_key" \
    >"$output_dir/bootstrap-scan.tsv"
  docker network connect yugabytedb-ysql-connector-contract db-monitoring-dev
  jdbc_network_connected=1
  mvn --file "$script_dir/../../../benchbase/pom.xml" --batch-mode --quiet \
    --activate-profiles postgres -Dmaven.gitcommitid.skip=true -Dfmt.skip \
    -DbuildDirectory="$jdbc_build_dir" -DskipTests clean compile \
    >"$output_dir/jdbc-build.log" 2>&1
  BLACKBOX_BACKEND=yugabytedb_ysql \
  BLACKBOX_JDBC_URL="jdbc:postgresql://yugabytedb_ysql:5433/benchbase?sslmode=disable" \
  BLACKBOX_DB_USER=connector_contract_application \
  BLACKBOX_DB_PASSWORD=connector_contract_application_password \
    mvn --file "$script_dir/../../../benchbase/pom.xml" --batch-mode --quiet \
      --activate-profiles postgres -Dmaven.gitcommitid.skip=true \
      -DbuildDirectory="$jdbc_build_dir" \
      -Dexec.executable=java -Dexec.classpathScope=runtime \
      "-Dexec.args=-classpath %classpath com.oltpbenchmark.blackbox.tools.YcsbJdbcNormalizationCapture" \
      org.codehaus.mojo:exec-maven-plugin:3.4.1:exec \
      >"$output_dir/jdbc-capture.json" 2>"$output_dir/jdbc-capture.log"
  ysql_application ycsb_repeated_write_shapes \
    --file /connector-contract/workload/ycsb_repeated_write_shapes.sql \
    >"$output_dir/repeated-write-shapes.log" 2>&1
  commit_order_logs+=("$output_dir/repeated-write-shapes.log")
}

preserve_evidence() {
  compose exec -T redis redis-cli --json XRANGE cdc - + \
    >"$output_dir/redis-stream.json"
  compose exec -T redis redis-cli --raw KEYS '*' \
    | LC_ALL=C sort >"$output_dir/redis-streams.txt"
  compose exec -T redis redis-cli INFO server \
    >"$output_dir/redis-info.txt"
  compose exec -T redis redis-cli INFO memory \
    >>"$output_dir/redis-info.txt"
  compose exec -T redis redis-cli INFO stats \
    >>"$output_dir/redis-info.txt"

  ysql_admin --command \
    "SELECT version(); SELECT count(*) AS publication_count FROM pg_publication WHERE pubname = 'connector_contract_publication'; SELECT slot_name, plugin, database, yb_lsn_type, active, active_pid, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'connector_contract_slot'; SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'connector_contract_publication' ORDER BY schemaname, tablename; SELECT c.relname, c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'benchbase' AND c.relname IN ('usertable','checker_run_fence','session_txn') ORDER BY c.relname; SELECT yb_hash_code(i) FROM unnest(ARRAY[1,2,201,202,1001,1012]) AS i; SELECT count(*) AS application_rows FROM benchbase.usertable; SELECT session_id, txn_id FROM benchbase.session_txn ORDER BY session_id;" \
    >"$output_dir/database-catalog.txt"
  compose exec -T yugabytedb_ysql bin/yugabyted version \
    >"$output_dir/database-build.txt"
  compose exec -T yugabytedb_ysql bin/yb-admin --version \
    >>"$output_dir/database-build.txt"

  compose logs --no-color debezium_server >"$output_dir/debezium-server.log"
  compose logs --no-color initialize >"$output_dir/initialize.log"
  compose logs --no-color redis >"$output_dir/redis.log"
  compose logs --no-color yugabytedb_ysql >"$output_dir/yugabytedb-ysql.log"
  compose ps --all --format json >"$output_dir/compose-services.json"
  compose config >"$output_dir/docker-compose-config.yml"
  docker image inspect \
    db-monitoring/yugabytedb-ysql-connector-contract:dz.2.5.2.yb.2026.1 \
    >"$output_dir/connector-contract-image.json"
  # shellcheck disable=SC2016
  compose exec -T debezium_server sh -lc '
    set -eu
    connector_jar=$(find /debezium/lib -maxdepth 1 -type f -name "yugabytedb-source-connector-*.jar")
    test "$(printf "%s\n" "$connector_jar" | grep -c .)" -eq 1
    test "$(find /debezium/lib -maxdepth 1 -type f -name "debezium-connector-postgres-*.jar" | wc -l)" -eq 0
    printf "%s\n" "$connector_jar"
    sha256sum "$connector_jar"
    grep -a -o "io/debezium/connector/postgresql/YugabyteDBConnector.class" "$connector_jar"
    if grep -a -q "io/debezium/connector/postgresql/PostgresConnector.class" "$connector_jar"; then
      exit 1
    fi
  ' >"$output_dir/connector-classpath.txt"

  cp -- "$script_dir/provenance.lock.json" "$output_dir/provenance.lock.json"
  cp -- "$script_dir/application.properties" "$output_dir/application.properties"
  cp -- "$script_dir/docker-compose.yml" "$output_dir/docker-compose.yml"
  cp -- "$script_dir/schema.sql" "$output_dir/schema.sql"
  cp -- "$script_dir/workload/cross_tablet_marker_last.sql" "$output_dir/cross_tablet_marker_last.sql"
  cp -- "$script_dir/workload/contract_transactions.sql" "$output_dir/contract_transactions.sql"
  cp -- "$script_dir/workload/backpressure_commit_order.sql" "$output_dir/backpressure_commit_order.sql"
  cp -- "$script_dir/workload/ycsb_value_normalization.sql" "$output_dir/ycsb_value_normalization.sql"
  cp -- "$script_dir/workload/ycsb_repeated_write_shapes.sql" "$output_dir/ycsb_repeated_write_shapes.sql"
  cp -- "$script_dir/verify_ycsb_value_normalization.py" "$output_dir/verify_ycsb_value_normalization.py"
  if [[ $scenario == ycsb_value_normalization ]]; then
    cp -- "$cdc_codec_replay_bin" "$output_dir/blackbox-cdc-codec-replay"
  fi
}

mkdir -p -- "$output_dir"
declare -a commit_order_logs=()
connector_monitor_pid=
pause_started_ns=
workload_committed_ns=
connector_write_observed_ns=
unpause_ns=
stream_length_before=
stream_length_during=
jdbc_network_connected=0
jdbc_build_dir=

disconnect_jdbc_network() {
  if ((jdbc_network_connected == 1)); then
    docker network disconnect yugabytedb-ysql-connector-contract \
      db-monitoring-dev >/dev/null 2>&1 || true
    jdbc_network_connected=0
  fi
}

cleanup_capture() {
  stop_connector_output_monitor
  disconnect_jdbc_network
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  if [[ -n $jdbc_build_dir && -d $jdbc_build_dir ]]; then
    rm -rf -- "$jdbc_build_dir"
  fi
}
trap cleanup_capture EXIT

disconnect_jdbc_network
compose down --volumes --remove-orphans
compose up --detach --build
wait_for_streaming
start_connector_output_monitor

case "$scenario" in
  cross_tablet_marker_last) run_cross_tablet_marker_last ;;
  concurrent_commit_order) run_concurrent_commit_order ;;
  redis_backpressure) run_redis_backpressure ;;
  ycsb_value_normalization) run_ycsb_value_normalization ;;
esac

python3 "$script_dir/extract_commit_order.py" "${commit_order_logs[@]}" \
  >"$output_dir/commit-order.json"
expected_completed_transactions=$(jq -er '.completed_transactions | length' \
  "$output_dir/commit-order.json")
wait_for_ends "$expected_completed_transactions"
if [[ "$scenario" == redis_backpressure ]]; then
  stream_length_after=$(redis_stream_length)
  python3 "$script_dir/write_backpressure_evidence.py" \
    --pause-ms "$backpressure_ms" \
    --pause-start-ns "$pause_started_ns" \
    --workload-committed-ns "$workload_committed_ns" \
    --connector-write-observed-ns "$connector_write_observed_ns" \
    --unpause-ns "$unpause_ns" \
    --stream-length-before "$stream_length_before" \
    --stream-length-during "$stream_length_during" \
    --stream-length-after "$stream_length_after" \
    --client-list "$output_dir/redis-paused-clients.txt" \
    >"$output_dir/backpressure-evidence.json"
fi
stop_connector_output_monitor
python3 "$script_dir/extract_connector_output.py" \
  --redis-batch-size "$redis_batch_size" \
  "$output_dir/redis-monitor.log" \
  >"$output_dir/connector-output-order.json"
preserve_evidence

if [[ "$scenario" == ycsb_value_normalization ]]; then
  "$output_dir/blackbox-cdc-codec-replay" \
    --input "$output_dir/redis-stream.json" \
    --backend yugabytedb_ysql \
    --benchmark ycsb \
    --backend-source-id connector_contract \
    >"$output_dir/cpp-codec-replay.json"
  python3 "$script_dir/../../scripts/blackbox_record.py" bootstrap-manifest \
    --run-id 00000000-0000-0000-0000-000000000001 \
    --scan-file "$output_dir/bootstrap-scan.tsv" \
    >"$output_dir/bootstrap-manifest.json"
  python3 "$script_dir/verify_ycsb_value_normalization.py" \
    --redis-stream "$output_dir/redis-stream.json" \
    --bootstrap-scan "$output_dir/bootstrap-scan.tsv" \
    --jdbc-capture "$output_dir/jdbc-capture.json" \
    --cpp-codec-replay "$output_dir/cpp-codec-replay.json" \
    --report "$output_dir/normalization-report.json"
fi

python3 "$script_dir/verify_provenance.py"
python3 "$script_dir/verify_raw_contract.py" \
  --plugin "$plugin" \
  --redis-batch-size "$redis_batch_size" \
  "$output_dir/redis-stream.json"

metadata_arguments=(
  --output-dir "$output_dir"
  --scenario "$scenario"
  --redis-batch-size "$redis_batch_size"
  --source-batch-size "$source_batch_size"
)
python3 "$script_dir/write_capture_metadata.py" "${metadata_arguments[@]}"

python3 "$script_dir/verify_raw_contract.py" \
  --plugin "$plugin" \
  --redis-batch-size "$redis_batch_size" \
  "$output_dir/redis-stream.json"

checksum_file=$(mktemp /tmp/yugabytedb-ysql-capture-checksums.XXXXXX)
(
  cd -- "$output_dir"
  find . -maxdepth 1 -type f ! -name SHA256SUMS -printf '%f\0' \
    | LC_ALL=C sort -z \
    | xargs -0 sha256sum
) >"$checksum_file"
mv -- "$checksum_file" "$output_dir/SHA256SUMS"

disconnect_jdbc_network
compose down --volumes --remove-orphans
trap - EXIT
if [[ -n $jdbc_build_dir && -d $jdbc_build_dir ]]; then
  rm -rf -- "$jdbc_build_dir"
fi
echo "connector-contract evidence preserved at $output_dir"
