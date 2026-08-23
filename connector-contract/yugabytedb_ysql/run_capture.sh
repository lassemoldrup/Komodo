#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
readonly scenario=${1:-cross_tablet_marker_last}
readonly redis_batch_size=${2:-2}
readonly source_batch_size=${CONNECTOR_CONTRACT_SOURCE_BATCH_SIZE:-2}
readonly max_queue_size=${CONNECTOR_CONTRACT_MAX_QUEUE_SIZE:-32}
readonly backpressure_ms=${CONNECTOR_CONTRACT_BACKPRESSURE_MS:-30000}
capture_timestamp=$(date -u +%Y%m%dT%H%M%SZ)
readonly capture_timestamp
readonly output_dir=${3:-"$script_dir/fixtures/pgoutput-${scenario}-${capture_timestamp}"}
readonly plugin=pgoutput
readonly cdc_codec_replay_bin=${BLACKBOX_CDC_CODEC_REPLAY_BIN:-}
readonly -a evidence_tables=(
  usertable checker_run_fence session_txn
  contract_account_a contract_account_b contract_account_c contract_account_d
  warehouse district
)

case "$scenario" in
  cross_tablet_marker_last | concurrent_commit_order | commit_order_reversal | cross_table_transaction | commit_order_realtime | concurrent_tablet_commits | large_transaction | write_shape_matrix | conflict_retry_matrix | redis_backpressure | redis_backpressure_reversal | queue_saturation_order | ycsb_value_normalization) ;;
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
if [[ ! "$max_queue_size" =~ ^[1-9][0-9]*$ ]]; then
  echo "CONNECTOR_CONTRACT_MAX_QUEUE_SIZE must be a positive integer" >&2
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
  CONNECTOR_CONTRACT_MAX_QUEUE_SIZE="$max_queue_size" \
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

run_cross_table_transaction() {
  ysql_application contract_cross_table \
    --file /connector-contract/workload/cross_table_application_transactions.sql \
    >"$output_dir/workload.log" 2>&1
  commit_order_logs=("$output_dir/workload.log")
}

run_table_commit() {
  local application_name=$1
  local family=$2
  local account_id=$3
  local payload=$4
  local delay_seconds=$5
  local start_epoch=$6
  local session_id=$7
  local log_path=$8
  ysql_application "$application_name" \
    --file /connector-contract/workload/contract_table_commit.sql \
    --set table_family="$family" \
    --set account_id="$account_id" \
    --set payload="$payload" \
    --set delay_seconds="$delay_seconds" \
    --set start_epoch="$start_epoch" \
    --set session_id="$session_id" \
    --set txn_id=1 \
    >"$log_path" 2>&1
}

run_commit_order_reversal() {
  run_table_commit contract_commit_a a 201 commit-a 4 0 commit_a \
    "$output_dir/commit-a.log" &
  local commit_a_pid=$!

  wait_for_application_query contract_commit_a 'SELECT pg_sleep%'
  run_table_commit contract_commit_b b 202 commit-b 0 0 commit_b \
    "$output_dir/commit-b.log"
  wait "$commit_a_pid"
  commit_order_logs=("$output_dir/commit-b.log" "$output_dir/commit-a.log")
}

run_concurrent_commit_order() {
  run_commit_order_reversal
}

run_commit_order_realtime() {
  run_table_commit contract_realtime_a a 301 realtime-a 0 0 realtime_a \
    "$output_dir/realtime-a.log"
  run_table_commit contract_realtime_b b 302 realtime-b 0 0 realtime_b \
    "$output_dir/realtime-b.log"
  commit_order_logs=("$output_dir/realtime-a.log" "$output_dir/realtime-b.log")
}

run_concurrent_tablet_commits() {
  local release_epoch
  release_epoch=$(python3 -c 'import time; print(time.time() + 15)')
  local -a pids=()
  commit_order_logs=()
  for value in $(seq 1 24); do
    local family delay session log_path
    family=$(printf '%b' "\\$(printf '%03o' "$((97 + (value - 1) % 4))")")
    delay=$(python3 -c "print(f'{${value} * 0.10:.2f}')")
    session=$(printf 'concurrent_%02d' "$value")
    log_path=$(printf '%s/concurrent-%02d.log' "$output_dir" "$value")
    run_table_commit "contract_$session" "$family" "$((1000 + value))" \
      "$session" "$delay" "$release_epoch" "$session" "$log_path" &
    pids+=("$!")
    commit_order_logs+=("$log_path")
  done
  local pid
  for pid in "${pids[@]}"; do
    wait "$pid"
  done
}

run_large_transaction() {
  ysql_application contract_large_transaction \
    --file /connector-contract/workload/large_transaction.sql \
    >"$output_dir/workload.log" 2>&1
  commit_order_logs=("$output_dir/workload.log")
}

run_write_shape_matrix() {
  ysql_application contract_write_shapes \
    --file /connector-contract/workload/write_shape_matrix.sql \
    >"$output_dir/workload.log" 2>&1
  commit_order_logs=("$output_dir/workload.log")
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

run_redis_backpressure_reversal() {
  run_table_commit contract_backpressure_a a 701 backpressure-a 4 0 \
    backpressure_commit_a "$output_dir/commit-a.log" &
  local commit_a_pid=$!
  wait_for_application_query contract_backpressure_a 'SELECT pg_sleep%'

  stream_length_before=$(redis_stream_length)
  [[ $stream_length_before == 0 ]]
  pause_started_ns=$(date +%s%N)
  compose exec -T redis redis-cli CLIENT PAUSE "$backpressure_ms" WRITE \
    >"$output_dir/redis-backpressure.log" 2>&1

  run_table_commit contract_backpressure_b b 702 backpressure-b 0 0 \
    backpressure_commit_b "$output_dir/commit-b.log"
  wait "$commit_a_pid"
  run_table_commit contract_backpressure_realtime_a c 703 backpressure-realtime-a 0 0 \
    backpressure_realtime_a "$output_dir/realtime-a.log"
  run_table_commit contract_backpressure_realtime_b d 704 backpressure-realtime-b 0 0 \
    backpressure_realtime_b "$output_dir/realtime-b.log"
  workload_committed_ns=$(date +%s%N)
  wait_for_paused_connector_write
  stream_length_during=$(redis_stream_length)
  [[ $stream_length_during == "$stream_length_before" ]]
  compose exec -T redis redis-cli CLIENT UNPAUSE \
    >>"$output_dir/redis-backpressure.log" 2>&1
  unpause_ns=$(date +%s%N)
  commit_order_logs=(
    "$output_dir/commit-b.log" "$output_dir/commit-a.log"
    "$output_dir/realtime-a.log" "$output_dir/realtime-b.log"
  )
}

run_queue_saturation_order() {
  local workload_pid
  connect_queue_metrics_network
  queue_metrics_build_dir=$(mktemp -d /tmp/yugabytedb-queue-metrics.XXXXXX)
  javac -d "$queue_metrics_build_dir" "$script_dir/QueueMetricsCapture.java"
  java -cp "$queue_metrics_build_dir" QueueMetricsCapture \
    connector-contract-debezium-server 9010 monitor monitor123 60000 20 \
    >"$output_dir/queue-metrics.tsv" 2>"$output_dir/queue-metrics.log" &
  queue_metrics_pid=$!
  wait_for_queue_metrics

  ysql_application contract_queue_pressure \
    --file /connector-contract/workload/queue_pressure_transactions.sql \
    --set start_delay_seconds=2 \
    >"$output_dir/workload.log" 2>&1 &
  workload_pid=$!
  wait_for_application_query contract_queue_pressure 'SELECT pg_sleep%'

  stream_length_before=$(redis_stream_length)
  [[ $stream_length_before == 0 ]] || {
    echo "Redis stream was not empty before queue pressure" >&2
    return 1
  }
  pause_started_ns=$(date +%s%N)
  compose exec -T redis redis-cli CLIENT PAUSE "$backpressure_ms" WRITE \
    >"$output_dir/redis-backpressure.log" 2>&1
  wait "$workload_pid"
  workload_committed_ns=$(date +%s%N)
  wait_for_paused_connector_write
  wait_for_queue_saturation
  stream_length_during=$(redis_stream_length)
  [[ $stream_length_during == "$stream_length_before" ]] || {
    echo "Redis stream advanced while the bounded connector queue was pressured" >&2
    return 1
  }
  compose exec -T redis redis-cli CLIENT UNPAUSE \
    >>"$output_dir/redis-backpressure.log" 2>&1
  unpause_ns=$(date +%s%N)
  stop_queue_metrics
  python3 "$script_dir/verify_queue_saturation.py" \
    --metrics "$output_dir/queue-metrics.tsv" \
    --expected-capacity "$max_queue_size" \
    >"$output_dir/queue-saturation-evidence.json"
  commit_order_logs=("$output_dir/workload.log")
}

run_conflict_retry_matrix() {
  ysql_application conflict_seed \
    --file /connector-contract/workload/conflict_seed.sql \
    >"$output_dir/conflict-seed.log" 2>&1

  ysql_application serialization_holder \
    --file /connector-contract/workload/serialization_holder.sql \
    >"$output_dir/serialization-holder.log" 2>&1 &
  local holder_pid=$!
  wait_for_application_query serialization_holder 'SELECT pg_sleep%'
  local serialization_status
  set +e
  ysql_application serialization_attempt \
    --file /connector-contract/workload/serialization_attempt.sql \
    >"$output_dir/serialization-attempt.log" 2>&1
  serialization_status=$?
  set -e
  ((serialization_status != 0)) || {
    echo "serialization attempt unexpectedly committed" >&2
    return 1
  }
  wait "$holder_pid"
  ysql_application serialization_retry \
    --file /connector-contract/workload/serialization_retry.sql \
    >"$output_dir/serialization-retry.log" 2>&1

  ysql_application deadlock_left \
    --file /connector-contract/workload/deadlock_left.sql \
    >"$output_dir/deadlock-left.log" 2>&1 &
  local deadlock_left_pid=$!
  wait_for_application_query deadlock_left 'SELECT pg_sleep%'
  ysql_application deadlock_right \
    --file /connector-contract/workload/deadlock_right.sql \
    >"$output_dir/deadlock-right.log" 2>&1 &
  local deadlock_right_pid=$!

  local deadlock_left_status deadlock_right_status
  set +e
  wait "$deadlock_left_pid"
  deadlock_left_status=$?
  wait "$deadlock_right_pid"
  deadlock_right_status=$?
  set -e

  python3 "$script_dir/write_attempt_ledger.py" \
    --serialization-log "$output_dir/serialization-attempt.log" \
    --deadlock-left-status "$deadlock_left_status" \
    --deadlock-left-log "$output_dir/deadlock-left.log" \
    --deadlock-right-status "$deadlock_right_status" \
    --deadlock-right-log "$output_dir/deadlock-right.log" \
    --output "$output_dir/attempt-ledger.json" \
    >"$output_dir/attempt-ledger-summary.json"

  local deadlock_winner deadlock_loser winner_log
  deadlock_winner=$(jq -er .deadlock_winner "$output_dir/attempt-ledger.json")
  deadlock_loser=$(jq -er .deadlock_loser "$output_dir/attempt-ledger.json")
  winner_log="$output_dir/${deadlock_winner//_/-}.log"
  ysql_application deadlock_retry \
    --file /connector-contract/workload/deadlock_retry.sql \
    --set session_id="$deadlock_loser" \
    --set payload="${deadlock_loser}-retry" \
    >"$output_dir/deadlock-retry.log" 2>&1

  commit_order_logs=(
    "$output_dir/conflict-seed.log"
    "$output_dir/serialization-holder.log"
    "$output_dir/serialization-retry.log"
    "$winner_log"
    "$output_dir/deadlock-retry.log"
  )
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
    "SELECT version(); SELECT count(*) AS publication_count FROM pg_publication WHERE pubname = 'connector_contract_publication'; SELECT slot_name, plugin, database, yb_lsn_type, active, active_pid, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'connector_contract_slot'; SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'connector_contract_publication' ORDER BY schemaname, tablename; SELECT c.relname, c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'benchbase' AND c.relkind = 'r' ORDER BY c.relname; SELECT 'usertable' AS table_name, count(*) AS rows FROM benchbase.usertable UNION ALL SELECT 'checker_run_fence', count(*) FROM benchbase.checker_run_fence UNION ALL SELECT 'session_txn', count(*) FROM benchbase.session_txn UNION ALL SELECT 'contract_account_a', count(*) FROM benchbase.contract_account_a UNION ALL SELECT 'contract_account_b', count(*) FROM benchbase.contract_account_b UNION ALL SELECT 'contract_account_c', count(*) FROM benchbase.contract_account_c UNION ALL SELECT 'contract_account_d', count(*) FROM benchbase.contract_account_d UNION ALL SELECT 'warehouse', count(*) FROM benchbase.warehouse UNION ALL SELECT 'district', count(*) FROM benchbase.district ORDER BY table_name; SELECT session_id, txn_id FROM benchbase.session_txn ORDER BY session_id;" \
    >"$output_dir/database-catalog.txt"
  ysql_admin --tuples-only --no-align --field-separator=$'\t' --command \
    "SELECT 'usertable:1', yb_hash_code(1); SELECT 'usertable:2', yb_hash_code(2); SELECT 'usertable:201', yb_hash_code(201); SELECT 'usertable:202', yb_hash_code(202);" \
    >"$output_dir/key-hashes.tsv"
  local table
  for table in "${evidence_tables[@]}"; do
    compose exec -T yugabytedb_ysql bin/yb-admin \
      --master_addresses yugabytedb_ysql:7100 \
      list_tablets ysql.benchbase "$table" 0 \
      >"$output_dir/tablets-$table.txt"
  done
  local -a placement_arguments=(
    --key-hashes "$output_dir/key-hashes.tsv"
    --output "$output_dir/tablet-placement.json"
  )
  for table in "${evidence_tables[@]}"; do
    placement_arguments+=(--table "$table=$output_dir/tablets-$table.txt")
  done
  python3 "$script_dir/write_tablet_placement.py" "${placement_arguments[@]}"
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
  docker image inspect \
    "$(jq -er .artifacts.database.image "$script_dir/provenance.lock.json")" \
    >"$output_dir/database-image.json"
  docker image inspect \
    "$(jq -er .artifacts.redis.image "$script_dir/provenance.lock.json")" \
    >"$output_dir/redis-image.json"
  local container
  for container in \
    connector-contract-yugabytedb-ysql connector-contract-initialize \
    connector-contract-debezium-server connector-contract-redis; do
    docker inspect "$container" >"$output_dir/container-$container.json"
  done
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
  cp -- "$script_dir"/workload/*.sql "$output_dir/"
  cp -- "$script_dir/verify_ycsb_value_normalization.py" "$output_dir/verify_ycsb_value_normalization.py"
  cp -- "$script_dir/verify_distributed_experiment.py" "$output_dir/verify_distributed_experiment.py"
  cp -- "$script_dir/verify_raw_contract.py" "$output_dir/verify_raw_contract.py"
  cp -- "$script_dir/write_attempt_ledger.py" "$output_dir/write_attempt_ledger.py"
  cp -- "$script_dir/write_tablet_placement.py" "$output_dir/write_tablet_placement.py"
  cp -- "$script_dir/QueueMetricsCapture.java" "$output_dir/QueueMetricsCapture.java"
  cp -- "$script_dir/verify_queue_saturation.py" "$output_dir/verify_queue_saturation.py"
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
queue_metrics_network_connected=0
queue_metrics_build_dir=
queue_metrics_pid=

connect_queue_metrics_network() {
  docker network connect yugabytedb-ysql-connector-contract db-monitoring-dev
  queue_metrics_network_connected=1
}

disconnect_queue_metrics_network() {
  if ((queue_metrics_network_connected == 1)); then
    docker network disconnect yugabytedb-ysql-connector-contract \
      db-monitoring-dev >/dev/null 2>&1 || true
    queue_metrics_network_connected=0
  fi
}

wait_for_queue_metrics() {
  local deadline=$((SECONDS + 30))
  while ((SECONDS < deadline)); do
    if awk -F '\t' 'NR > 1 && $4 > 0 { found=1 } END { exit !found }' \
        "$output_dir/queue-metrics.tsv" 2>/dev/null; then
      return
    fi
    kill -0 "$queue_metrics_pid" 2>/dev/null || {
      echo "queue metrics capture exited before emitting capacity telemetry" >&2
      return 1
    }
    sleep 0.05
  done
  echo "timed out waiting for connector queue metrics" >&2
  return 1
}

wait_for_queue_saturation() {
  local deadline=$((SECONDS + 20))
  while ((SECONDS < deadline)); do
    if awk -F '\t' 'NR > 1 && $5 == 0 { found=1 } END { exit !found }' \
        "$output_dir/queue-metrics.tsv"; then
      return
    fi
    kill -0 "$queue_metrics_pid" 2>/dev/null || {
      echo "queue metrics capture exited before observing saturation" >&2
      return 1
    }
    sleep 0.05
  done
  echo "connector queue never reported zero remaining capacity" >&2
  return 1
}

stop_queue_metrics() {
  if [[ -n ${queue_metrics_pid:-} ]] && kill -0 "$queue_metrics_pid" 2>/dev/null; then
    kill "$queue_metrics_pid" 2>/dev/null || true
    wait "$queue_metrics_pid" 2>/dev/null || true
  fi
  queue_metrics_pid=
  disconnect_queue_metrics_network
}

disconnect_jdbc_network() {
  if ((jdbc_network_connected == 1)); then
    docker network disconnect yugabytedb-ysql-connector-contract \
      db-monitoring-dev >/dev/null 2>&1 || true
    jdbc_network_connected=0
  fi
}

cleanup_capture() {
  local status=$?
  stop_connector_output_monitor
  stop_queue_metrics
  disconnect_jdbc_network
  if ((status != 0)) && [[ -d $output_dir ]] && [[ ! -e $output_dir/FAILED-SHA256SUMS ]]; then
    compose ps --all --format json >"$output_dir/failure-compose-services.json" 2>/dev/null || true
    compose logs --no-color --timestamps >"$output_dir/failure-compose.log" 2>&1 || true
  fi
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  if [[ -n $jdbc_build_dir && -d $jdbc_build_dir ]]; then
    rm -rf -- "$jdbc_build_dir"
  fi
  if [[ -n $queue_metrics_build_dir && -d $queue_metrics_build_dir ]]; then
    rm -rf -- "$queue_metrics_build_dir"
  fi
  if ((status != 0)) && [[ -d $output_dir ]] && [[ ! -e $output_dir/FAILED-SHA256SUMS ]]; then
    local failure_checksum_file
    failure_checksum_file=$(mktemp /tmp/yugabytedb-ysql-failed-capture-checksums.XXXXXX)
    (
      cd -- "$output_dir"
      find . -maxdepth 1 -type f ! -name FAILED-SHA256SUMS -printf '%f\0' \
        | LC_ALL=C sort -z | xargs -0 sha256sum
    ) >"$failure_checksum_file"
    mv -- "$failure_checksum_file" "$output_dir/FAILED-SHA256SUMS"
  fi
  return "$status"
}
trap cleanup_capture EXIT

disconnect_jdbc_network
disconnect_queue_metrics_network
compose down --volumes --remove-orphans
compose up --detach --build
wait_for_streaming
start_connector_output_monitor

case "$scenario" in
  cross_tablet_marker_last) run_cross_tablet_marker_last ;;
  cross_table_transaction) run_cross_table_transaction ;;
  concurrent_commit_order | commit_order_reversal) run_commit_order_reversal ;;
  commit_order_realtime) run_commit_order_realtime ;;
  concurrent_tablet_commits) run_concurrent_tablet_commits ;;
  large_transaction) run_large_transaction ;;
  write_shape_matrix) run_write_shape_matrix ;;
  conflict_retry_matrix) run_conflict_retry_matrix ;;
  redis_backpressure) run_redis_backpressure ;;
  redis_backpressure_reversal) run_redis_backpressure_reversal ;;
  queue_saturation_order) run_queue_saturation_order ;;
  ycsb_value_normalization) run_ycsb_value_normalization ;;
esac

python3 "$script_dir/extract_commit_order.py" "${commit_order_logs[@]}" \
  >"$output_dir/commit-order.json"
expected_completed_transactions=$(jq -er '.completed_transactions | length' \
  "$output_dir/commit-order.json")
wait_for_ends "$expected_completed_transactions"
if [[ "$scenario" == redis_backpressure || "$scenario" == redis_backpressure_reversal || "$scenario" == queue_saturation_order ]]; then
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

case "$scenario" in
  cross_tablet_marker_last | cross_table_transaction | commit_order_reversal | commit_order_realtime | concurrent_tablet_commits | large_transaction | write_shape_matrix | conflict_retry_matrix | redis_backpressure_reversal | queue_saturation_order)
    experiment_arguments=(
      --scenario "$scenario"
      --capture "$output_dir/redis-stream.json"
      --placement "$output_dir/tablet-placement.json"
      --redis-batch-size "$redis_batch_size"
      --source-batch-size "$source_batch_size"
      --report "$output_dir/experiment-report.json"
    )
    if [[ $scenario == conflict_retry_matrix ]]; then
      experiment_arguments+=(--attempt-ledger "$output_dir/attempt-ledger.json")
    elif [[ $scenario == queue_saturation_order ]]; then
      experiment_arguments+=(
        --queue-evidence "$output_dir/queue-saturation-evidence.json"
      )
    fi
    python3 "$script_dir/verify_distributed_experiment.py" \
      "${experiment_arguments[@]}"
    ;;
esac

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
