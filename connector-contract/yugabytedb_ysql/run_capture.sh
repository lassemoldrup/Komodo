#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
readonly scenario=${1:-cross_tablet_marker_last}
readonly redis_batch_size=${2:-2}
readonly source_batch_size=${CONNECTOR_CONTRACT_SOURCE_BATCH_SIZE:-2}
readonly backpressure_ms=750
readonly capture_timestamp=$(date -u +%Y%m%dT%H%M%SZ)
readonly output_dir=${3:-"$script_dir/fixtures/pgoutput-${scenario}-${capture_timestamp}"}
readonly plugin=pgoutput

case "$scenario" in
  cross_tablet_marker_last | concurrent_commit_order | redis_backpressure) ;;
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
    sleep 1
  done
  echo "did not observe $application_name executing $query_pattern" >&2
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
  completed_transactions=(cross_tablet:1)
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
  completed_transactions=(commit_b:1)
  wait "$commit_a_pid"
  completed_transactions+=(commit_a:1)
}

run_redis_backpressure() {
  compose exec -T redis redis-cli CLIENT PAUSE "$backpressure_ms" WRITE \
    >"$output_dir/redis-backpressure.log" 2>&1
  ysql_application contract_backpressure \
    --file /connector-contract/workload/backpressure_commit_order.sql \
    >"$output_dir/workload.log" 2>&1
  completed_transactions=()
  local txn_id
  for txn_id in $(seq 1 12); do
    completed_transactions+=("backpressure:$txn_id")
  done
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
}

mkdir -p -- "$output_dir"
declare -a completed_transactions=()

compose down --volumes --remove-orphans
compose up --detach --build
wait_for_streaming

case "$scenario" in
  cross_tablet_marker_last) run_cross_tablet_marker_last ;;
  concurrent_commit_order) run_concurrent_commit_order ;;
  redis_backpressure) run_redis_backpressure ;;
esac

wait_for_ends "${#completed_transactions[@]}"
preserve_evidence

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
if [[ "$scenario" == redis_backpressure ]]; then
  metadata_arguments+=(--backpressure-ms "$backpressure_ms")
fi
for completed in "${completed_transactions[@]}"; do
  metadata_arguments+=(--completed "$completed")
done
python3 "$script_dir/write_capture_metadata.py" "${metadata_arguments[@]}"

python3 "$script_dir/verify_raw_contract.py" \
  --plugin "$plugin" \
  --redis-batch-size "$redis_batch_size" \
  "$output_dir/redis-stream.json"

(
  cd -- "$output_dir"
  find . -maxdepth 1 -type f ! -name SHA256SUMS -printf '%f\0' \
    | LC_ALL=C sort -z \
    | xargs -0 sha256sum >SHA256SUMS
)

compose down --volumes --remove-orphans
echo "connector-contract evidence preserved at $output_dir"
