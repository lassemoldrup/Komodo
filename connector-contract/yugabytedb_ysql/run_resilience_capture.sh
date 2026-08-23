#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
readonly output_dir=${1:-"$script_dir/fixtures/pgoutput-resilience-$(date -u +%Y%m%dT%H%M%SZ)"}
[[ ! -e $output_dir ]] || { echo "resilience output already exists: $output_dir" >&2; exit 2; }
mkdir -p -- "$output_dir"

compose() {
  docker compose --file "$script_dir/docker-compose-resilience.yml" "$@"
}

ysql() {
  local application_name=$1
  shift
  compose exec -T -e PGPASSWORD=connector_contract_application_password \
    -e PGAPPNAME="$application_name" yb_node_1 bin/ysqlsh \
    --host yb_node_1 --username connector_contract_application \
    --dbname benchbase --no-psqlrc --set ON_ERROR_STOP=on "$@"
}

admin() {
  compose exec -T yb_node_1 bin/yb-admin --master_addresses yb_node_1:7100 "$@"
}

list_tablets() {
  local table=$1
  local output=$2
  admin list_tablets ysql.benchbase "$table" 0 >"$output"
}

tablet_id() {
  awk 'NR == 2 { print $1 }' "$1"
}

leader_id() {
  awk 'NR == 2 { print $NF }' "$1"
}

wait_for_tablet_count() {
  local table=$1
  local expected=$2
  local output=$3
  local deadline=$((SECONDS + 180))
  while ((SECONDS < deadline)); do
    list_tablets "$table" "$output"
    [[ $(awk 'NR > 1 && NF { count += 1 } END { print count + 0 }' "$output") == "$expected" ]] && return
    sleep 1
  done
  echo "tablet count did not reach $expected for $table" >&2
  return 1
}

wait_for_leader() {
  local table=$1
  local expected_leader=$2
  local output=$3
  local deadline=$((SECONDS + 120))
  while ((SECONDS < deadline)); do
    list_tablets "$table" "$output"
    [[ $(leader_id "$output") == "$expected_leader" ]] && return
    sleep 1
  done
  echo "leader did not move to $expected_leader for $table" >&2
  return 1
}

wait_for_leader_address_change() {
  local table=$1
  local forbidden_prefix=$2
  local output=$3
  local deadline=$((SECONDS + 180))
  while ((SECONDS < deadline)); do
    list_tablets "$table" "$output"
    local address
    address=$(awk 'NR == 2 { print $(NF - 1) }' "$output")
    [[ $address != "$forbidden_prefix"* ]] && return
    sleep 1
  done
  echo "leader remained on stopped TServer for $table" >&2
  return 1
}

wait_for_streaming() {
  local deadline=$((SECONDS + 180))
  while ((SECONDS < deadline)); do
    active=$(compose exec -T yb_node_1 bin/ysqlsh --host yb_node_1 \
      --username yugabyte --dbname benchbase --tuples-only --no-align --command \
      "SELECT count(*) FROM pg_replication_slots WHERE slot_name='connector_contract_slot' AND active AND yb_lsn_type='SEQUENCE'" 2>/dev/null | tr -d '\r') || true
    [[ $active == 1 ]] && return
    sleep 1
  done
  echo "resilience connector did not activate the SEQUENCE slot" >&2
  return 1
}

wait_for_application_query() {
  local application_name=$1
  local query_pattern=$2
  local deadline=$((SECONDS + 30))
  while ((SECONDS < deadline)); do
    count=$(compose exec -T yb_node_1 bin/ysqlsh --host yb_node_1 \
      --username yugabyte --dbname benchbase --tuples-only --no-align --command \
      "SELECT count(*) FROM pg_stat_activity WHERE application_name='$application_name' AND state='active' AND query LIKE '$query_pattern'" \
      2>/dev/null | tr -d '\r') || true
    [[ $count == 1 ]] && return
    sleep 0.05
  done
  echo "did not observe $application_name executing $query_pattern" >&2
  return 1
}

wait_for_ends() {
  local expected=$1
  local deadline=$((SECONDS + 180))
  while ((SECONDS < deadline)); do
    ends=$(compose exec -T redis redis-cli --raw XRANGE cdc - + | grep -c '"status":"END"' || true)
    ((ends == expected)) && return
    sleep 1
  done
  echo "resilience capture did not produce $expected END records" >&2
  return 1
}

run_transaction() {
  local family=$1
  local key=$2
  local txn_id=$3
  local delay_seconds=$4
  local log=$5
  ysql "resilience_$txn_id" --file /connector-contract/workload/resilience_transaction.sql \
    --set table_family="$family" --set account_id="$key" \
    --set payload="resilience-$txn_id" --set txn_id="$txn_id" \
    --set delay_seconds="$delay_seconds" >"$log" 2>&1
}

monitor_pid=
stop_monitor() {
  if [[ -n ${monitor_pid:-} ]] && kill -0 "$monitor_pid" 2>/dev/null; then
    monitor_client_id=$(compose exec -T redis redis-cli --raw CLIENT LIST 2>/dev/null \
      | awk '/(^| )cmd=monitor( |$)/ { for (i = 1; i <= NF; ++i) if ($i ~ /^id=/) { sub(/^id=/, "", $i); print $i } }')
    if [[ $monitor_client_id =~ ^[1-9][0-9]*$ ]]; then
      compose exec -T redis redis-cli --raw CLIENT KILL ID "$monitor_client_id" \
        >/dev/null 2>&1 || true
    else
      kill "$monitor_pid" 2>/dev/null || true
    fi
    wait "$monitor_pid" 2>/dev/null || true
  fi
  monitor_pid=
}

cleanup() {
  local status=$?
  stop_monitor
  if ((status != 0)); then
    compose ps --all --format json >"$output_dir/failure-compose-services.json" 2>/dev/null || true
    compose logs --no-color --timestamps >"$output_dir/failure-compose.log" 2>&1 || true
  fi
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  if ((status != 0)); then
    failure_checksums=$(mktemp /tmp/yugabytedb-resilience-failed.XXXXXX)
    (cd "$output_dir" && find . -maxdepth 1 -type f ! -name FAILED-SHA256SUMS -printf '%f\0' \
      | LC_ALL=C sort -z | xargs -0 sha256sum) >"$failure_checksums"
    mv "$failure_checksums" "$output_dir/FAILED-SHA256SUMS"
  fi
  return "$status"
}
trap cleanup EXIT

compose down --volumes --remove-orphans
compose up --detach --no-build
wait_for_streaming

compose exec -T redis redis-cli --raw MONITOR >"$output_dir/redis-monitor.log" 2>&1 &
monitor_pid=$!
deadline=$((SECONDS + 20))
while ((SECONDS < deadline)); do
  grep -qx OK "$output_dir/redis-monitor.log" && break
  sleep 0.05
done
grep -qx OK "$output_dir/redis-monitor.log"

ysql resilience_1 --file /connector-contract/workload/resilience_split_seed.sql \
  >"$output_dir/transaction-1.log" 2>&1
seed_transaction_commit_ack_ns=$(date +%s%N)

list_tablets contract_account_a "$output_dir/split-before.txt"
admin flush_table ysql.benchbase contract_account_a 60 >"$output_dir/split-flush.txt"
split_transaction_started_ns=$(date +%s%N)
run_transaction a 9000 2 5 "$output_dir/transaction-2.log" &
split_transaction_pid=$!
wait_for_application_query resilience_2 'SELECT pg_sleep%'
split_operation_started_ns=$(date +%s%N)
admin split_tablet "$(tablet_id "$output_dir/split-before.txt")" >"$output_dir/split-command.txt"
split_command_returned_ns=$(date +%s%N)
wait "$split_transaction_pid"
split_transaction_commit_ack_ns=$(date +%s%N)
wait_for_tablet_count contract_account_a 2 "$output_dir/split-after.txt"
split_completed_ns=$(date +%s%N)

list_tablets contract_account_b "$output_dir/leader-before.txt"
admin list_all_tablet_servers >"$output_dir/tablet-servers-before.txt"
node_two_ip=$(docker inspect resilience-contract-yb-2 \
  --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
node_three_ip=$(docker inspect resilience-contract-yb-3 \
  --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
jq -n --arg node_two "$node_two_ip" --arg node_three "$node_three_ip" \
  '{yb_node_2: $node_two, yb_node_3: $node_three}' >"$output_dir/node-addresses.json"
target_leader=$(awk -v address="$node_two_ip:9100" '$2 == address { print $1; exit }' \
  "$output_dir/tablet-servers-before.txt")
[[ $target_leader =~ ^[0-9a-f]{32}$ ]]
leader_transaction_started_ns=$(date +%s%N)
run_transaction b 3 3 5 "$output_dir/transaction-3.log" &
leader_transaction_pid=$!
wait_for_application_query resilience_3 'SELECT pg_sleep%'
leader_operation_started_ns=$(date +%s%N)
admin leader_stepdown "$(tablet_id "$output_dir/leader-before.txt")" "$target_leader" \
  >"$output_dir/leader-command.txt"
leader_command_returned_ns=$(date +%s%N)
wait "$leader_transaction_pid"
leader_transaction_commit_ack_ns=$(date +%s%N)
wait_for_leader contract_account_b "$target_leader" "$output_dir/leader-after.txt"
leader_completed_ns=$(date +%s%N)

list_tablets contract_account_c "$output_dir/loss-initial.txt"
target_loss_leader=$(awk -v address="$node_three_ip:9100" '$2 == address { print $1; exit }' \
  "$output_dir/tablet-servers-before.txt")
[[ $target_loss_leader =~ ^[0-9a-f]{32}$ ]]
admin leader_stepdown "$(tablet_id "$output_dir/loss-initial.txt")" "$target_loss_leader" \
  >"$output_dir/loss-leader-command.txt"
wait_for_leader contract_account_c "$target_loss_leader" "$output_dir/loss-before.txt"
loss_transaction_started_ns=$(date +%s%N)
run_transaction c 4 4 5 "$output_dir/transaction-4.log" &
loss_transaction_pid=$!
wait_for_application_query resilience_4 'SELECT pg_sleep%'
tserver_stop_started_ns=$(date +%s%N)
docker stop --time 1 resilience-contract-yb-3 >"$output_dir/stopped-container.txt"
tserver_stop_returned_ns=$(date +%s%N)
wait "$loss_transaction_pid"
loss_transaction_commit_ack_ns=$(date +%s%N)
wait_for_leader_address_change contract_account_c "$node_three_ip:" "$output_dir/loss-after.txt"
loss_failover_completed_ns=$(date +%s%N)

ysql resilience_close --file /connector-contract/workload/resilience_close.sql \
  >"$output_dir/transaction-5.log" 2>&1
closing_transaction_commit_ack_ns=$(date +%s%N)
wait_for_ends 5
closing_stream_length_before=$(compose exec -T redis redis-cli --raw XLEN cdc | tr -d '\r[:space:]')
closing_end_count_before=$(compose exec -T redis redis-cli --raw XRANGE cdc - + \
  | grep -c '"status":"END"')
stabilization_started_ns=$(date +%s%N)
sleep 5
stabilization_completed_ns=$(date +%s%N)
closing_stream_length_after=$(compose exec -T redis redis-cli --raw XLEN cdc | tr -d '\r[:space:]')
closing_end_count_after=$(compose exec -T redis redis-cli --raw XRANGE cdc - + \
  | grep -c '"status":"END"')
[[ $closing_stream_length_before == "$closing_stream_length_after" ]]
[[ $closing_end_count_before == 5 && $closing_end_count_after == 5 ]]

# shellcheck disable=SC2016
compose exec -T debezium_server sh -lc '
  set -eu
  connector_jar=$(find /debezium/lib -maxdepth 1 -type f -name "yugabytedb-source-connector-*.jar")
  test "$(printf "%s\n" "$connector_jar" | grep -c .)" -eq 1
  sha256sum "$connector_jar"
  grep -a -o "io/debezium/connector/postgresql/YugabyteDBConnector.class" "$connector_jar"
  if grep -a -q "io/debezium/connector/postgresql/PostgresConnector.class" "$connector_jar"; then
    exit 1
  fi
' >"$output_dir/connector-classpath.txt"
docker stop --time 10 resilience-contract-debezium-server \
  >"$output_dir/stopped-connector.txt"
connector_stopped_ns=$(date +%s%N)

stop_monitor

jq -n \
  --argjson seed_transaction_commit_ack_ns "$seed_transaction_commit_ack_ns" \
  --argjson split_transaction_started_ns "$split_transaction_started_ns" \
  --argjson split_operation_started_ns "$split_operation_started_ns" \
  --argjson split_command_returned_ns "$split_command_returned_ns" \
  --argjson split_transaction_commit_ack_ns "$split_transaction_commit_ack_ns" \
  --argjson split_completed_ns "$split_completed_ns" \
  --argjson leader_transaction_started_ns "$leader_transaction_started_ns" \
  --argjson leader_operation_started_ns "$leader_operation_started_ns" \
  --argjson leader_command_returned_ns "$leader_command_returned_ns" \
  --argjson leader_transaction_commit_ack_ns "$leader_transaction_commit_ack_ns" \
  --argjson leader_completed_ns "$leader_completed_ns" \
  --argjson loss_transaction_started_ns "$loss_transaction_started_ns" \
  --argjson tserver_stop_started_ns "$tserver_stop_started_ns" \
  --argjson tserver_stop_returned_ns "$tserver_stop_returned_ns" \
  --argjson loss_transaction_commit_ack_ns "$loss_transaction_commit_ack_ns" \
  --argjson loss_failover_completed_ns "$loss_failover_completed_ns" \
  '{kind:"yugabytedb_ysql_resilience_transition_timeline",
    seed_transaction_commit_ack_ns:$seed_transaction_commit_ack_ns,
    split:{transaction_started_ns:$split_transaction_started_ns,
      operation_started_ns:$split_operation_started_ns,
      command_returned_ns:$split_command_returned_ns,
      transaction_commit_ack_ns:$split_transaction_commit_ack_ns,
      topology_completed_ns:$split_completed_ns},
    leader_movement:{transaction_started_ns:$leader_transaction_started_ns,
      operation_started_ns:$leader_operation_started_ns,
      command_returned_ns:$leader_command_returned_ns,
      transaction_commit_ack_ns:$leader_transaction_commit_ack_ns,
      topology_completed_ns:$leader_completed_ns},
    tserver_loss:{transaction_started_ns:$loss_transaction_started_ns,
      operation_started_ns:$tserver_stop_started_ns,
      command_returned_ns:$tserver_stop_returned_ns,
      transaction_commit_ack_ns:$loss_transaction_commit_ack_ns,
      topology_completed_ns:$loss_failover_completed_ns}}' \
  >"$output_dir/transition-timeline.json"

jq -n \
  --argjson closing_transaction_commit_ack_ns "$closing_transaction_commit_ack_ns" \
  --argjson stabilization_started_ns "$stabilization_started_ns" \
  --argjson stabilization_completed_ns "$stabilization_completed_ns" \
  --argjson stream_length_before "$closing_stream_length_before" \
  --argjson stream_length_after "$closing_stream_length_after" \
  --argjson end_count_before "$closing_end_count_before" \
  --argjson end_count_after "$closing_end_count_after" \
  --argjson connector_stopped_ns "$connector_stopped_ns" \
  '{kind:"yugabytedb_ysql_resilience_closing_boundary",
    closing_transaction_commit_ack_ns:$closing_transaction_commit_ack_ns,
    stabilization_started_ns:$stabilization_started_ns,
    stabilization_completed_ns:$stabilization_completed_ns,
    stream_length_before:$stream_length_before,
    stream_length_after:$stream_length_after,
    end_count_before:$end_count_before,
    end_count_after:$end_count_after,
    connector_stopped_ns:$connector_stopped_ns}' \
  >"$output_dir/closing-boundary.json"

python3 "$script_dir/extract_commit_order.py" \
  "$output_dir/transaction-1.log" "$output_dir/transaction-2.log" \
  "$output_dir/transaction-3.log" "$output_dir/transaction-4.log" \
  "$output_dir/transaction-5.log" \
  >"$output_dir/commit-order.json"
python3 "$script_dir/extract_connector_output.py" --redis-batch-size 4 \
  "$output_dir/redis-monitor.log" >"$output_dir/connector-output-order.json"
compose exec -T redis redis-cli --json XRANGE cdc - + >"$output_dir/redis-stream.json"
compose exec -T redis redis-cli --raw XRANGE cdc - + >"$output_dir/redis-stream.raw.txt"
compose exec -T yb_node_1 bin/ysqlsh --host yb_node_1 --username yugabyte \
  --dbname benchbase --no-psqlrc --set ON_ERROR_STOP=on --tuples-only \
  --no-align --field-separator=$'\t' --command \
  "SELECT table_name, account_id, payload FROM (SELECT 'contract_account_a' AS table_name, account_id, payload FROM benchbase.contract_account_a UNION ALL SELECT 'contract_account_b', account_id, payload FROM benchbase.contract_account_b UNION ALL SELECT 'contract_account_c', account_id, payload FROM benchbase.contract_account_c) AS state ORDER BY table_name, account_id;" \
  >"$output_dir/database-state-after-loss.tsv"
compose exec -T yb_node_1 bin/ysqlsh --host yb_node_1 --username yugabyte \
  --dbname benchbase --no-psqlrc --set ON_ERROR_STOP=on --command \
  "SELECT 'contract_account_a' AS table_name,count(*) FROM benchbase.contract_account_a UNION ALL SELECT 'contract_account_b',count(*) FROM benchbase.contract_account_b UNION ALL SELECT 'contract_account_c',count(*) FROM benchbase.contract_account_c; SELECT * FROM benchbase.session_txn ORDER BY session_id; SELECT * FROM benchbase.checker_run_fence ORDER BY phase; SELECT slot_name,plugin,yb_lsn_type,active,confirmed_flush_lsn FROM pg_replication_slots;" \
  >"$output_dir/database-state-after-loss.txt"
admin list_all_tablet_servers >"$output_dir/tablet-servers-after-loss.txt"
compose config >"$output_dir/docker-compose-config.yml"
compose ps --all --format json >"$output_dir/compose-services.json"
for container in resilience-contract-yb-1 resilience-contract-yb-2 resilience-contract-yb-3 \
  resilience-contract-debezium-server resilience-contract-redis; do
  docker inspect "$container" >"$output_dir/container-$container.json"
  docker logs --timestamps "$container" >"$output_dir/container-$container.log" 2>&1
done
docker image inspect db-monitoring/yugabytedb-ysql-connector-contract:dz.2.5.2.yb.2026.1 \
  >"$output_dir/connector-contract-image.json"
docker image inspect "$(jq -er .artifacts.database.image "$script_dir/provenance.lock.json")" \
  >"$output_dir/database-image.json"
docker image inspect "$(jq -er .artifacts.redis.image "$script_dir/provenance.lock.json")" \
  >"$output_dir/redis-image.json"
cp "$script_dir/provenance.lock.json" "$output_dir/"
cp "$script_dir/docker-compose-resilience.yml" "$output_dir/"
cp "$script_dir/schema.sql" "$output_dir/"
cp "$script_dir/application.properties" "$output_dir/"
cp "$script_dir/workload/resilience_transaction.sql" "$output_dir/"
cp "$script_dir/workload/resilience_split_seed.sql" "$output_dir/"
cp "$script_dir/workload/resilience_close.sql" "$output_dir/"
cp "$script_dir/verify_raw_contract.py" "$output_dir/"
cp "$script_dir/verify_distributed_experiment.py" "$output_dir/"
cp "$script_dir/verify_resilience_experiment.py" "$output_dir/"

python3 "$script_dir/verify_resilience_experiment.py" \
  --directory "$output_dir" --report "$output_dir/resilience-report.json"

checksums=$(mktemp /tmp/yugabytedb-resilience.XXXXXX)
(cd "$output_dir" && find . -maxdepth 1 -type f ! -name SHA256SUMS -printf '%f\0' \
  | LC_ALL=C sort -z | xargs -0 sha256sum) >"$checksums"
mv "$checksums" "$output_dir/SHA256SUMS"
(cd "$output_dir" && sha256sum --check SHA256SUMS >/dev/null)

compose down --volumes --remove-orphans
trap - EXIT
echo "resilience evidence preserved at $output_dir"
