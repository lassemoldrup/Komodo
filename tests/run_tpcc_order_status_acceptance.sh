#!/usr/bin/env bash
# Live one-warehouse RR acceptance run for audited TPC-C OrderStatus.

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source=scripts/blackbox_lifecycle.sh
. "$ROOT/scripts/blackbox_lifecycle.sh"

BLACKBOX_DOCKER=${BLACKBOX_DOCKER:-docker}
OUTPUT_DIR=${BLACKBOX_TPCC_ORDER_STATUS_OUTPUT_DIR:-}
BENCHBASE_DIST=${BLACKBOX_TPCC_ORDER_STATUS_BENCHBASE_DIST:-}
BENCHBASE_CONFIG=${BLACKBOX_TPCC_ORDER_STATUS_CONFIG:-}
MONITOR_BIN=${BLACKBOX_TPCC_ORDER_STATUS_MONITOR_BIN:-}
JAVA_BIN=${BLACKBOX_TPCC_ORDER_STATUS_JAVA:-java}
E2E=${BLACKBOX_TPCC_ORDER_STATUS_E2E:-$ROOT/scripts/e2e.sh}
VALIDATOR=${BLACKBOX_TPCC_ORDER_STATUS_VALIDATOR:-$ROOT/tests/verify_tpcc_order_status_acceptance.py}
WAREHOUSES=${BLACKBOX_TPCC_ORDER_STATUS_WAREHOUSES:-1}
TERMINALS=${BLACKBOX_TPCC_ORDER_STATUS_TERMINALS:-1}
WEIGHTS=${BLACKBOX_TPCC_ORDER_STATUS_WEIGHTS:-0,0,100,0,0}
RANDOM_SEED=${BLACKBOX_TPCC_ORDER_STATUS_RANDOM_SEED:-3}
WARMUP_SECONDS=${BLACKBOX_TPCC_ORDER_STATUS_WARMUP_SECONDS:-0}
DURATION_SECONDS=${BLACKBOX_TPCC_ORDER_STATUS_DURATION_SECONDS:-2}
RATE=${BLACKBOX_TPCC_ORDER_STATUS_RATE:-unlimited}
LOADER_THREADS=${BLACKBOX_TPCC_ORDER_STATUS_LOADER_THREADS:-1}
LOADER_BATCH_SIZE=${BLACKBOX_TPCC_ORDER_STATUS_LOADER_BATCH_SIZE:-128}
MINIMUM_TRANSACTIONS=${BLACKBOX_TPCC_ORDER_STATUS_MINIMUM_TRANSACTIONS:-2}
MINIMUM_ID_TRANSACTIONS=${BLACKBOX_TPCC_ORDER_STATUS_MINIMUM_ID_TRANSACTIONS:-1}
MINIMUM_LAST_NAME_TRANSACTIONS=${BLACKBOX_TPCC_ORDER_STATUS_MINIMUM_LAST_NAME_TRANSACTIONS:-1}
MINIMUM_MULTIROW_TRANSACTIONS=${BLACKBOX_TPCC_ORDER_STATUS_MINIMUM_MULTIROW_TRANSACTIONS:-1}
MONITOR_MAX_BOOTSTRAP_STATE_ROWS=${BLACKBOX_TPCC_ORDER_STATUS_MONITOR_MAX_BOOTSTRAP_STATE_ROWS:-1000000}
MONITOR_MAX_BOOTSTRAP_STATE_BYTES=${BLACKBOX_TPCC_ORDER_STATUS_MONITOR_MAX_BOOTSTRAP_STATE_BYTES:-1073741824}
STACK_TIMEOUT_SECONDS=${BLACKBOX_TPCC_ORDER_STATUS_STACK_TIMEOUT_SECONDS:-180}
LOADER_TIMEOUT_SECONDS=${BLACKBOX_TPCC_ORDER_STATUS_LOADER_TIMEOUT_SECONDS:-900}
WORKLOAD_TIMEOUT_SECONDS=${BLACKBOX_TPCC_ORDER_STATUS_WORKLOAD_TIMEOUT_SECONDS:-300}
STATUS_TIMEOUT_SECONDS=${BLACKBOX_TPCC_ORDER_STATUS_STATUS_TIMEOUT_SECONDS:-900}
CASE_TIMEOUT_SECONDS=${BLACKBOX_TPCC_ORDER_STATUS_CASE_TIMEOUT_SECONDS:-1800}
KILL_AFTER_SECONDS=${BLACKBOX_TPCC_ORDER_STATUS_KILL_AFTER_SECONDS:-15}
KEEP_STACK=${BLACKBOX_TPCC_ORDER_STATUS_KEEP_STACK:-0}
PRINT_CONFIG=0
SUCCESS=0


usage() {
  cat <<'EOF'
Usage: run_tpcc_order_status_acceptance.sh [options]

Required for a live run:
  --benchbase-dist PATH       Distribution containing benchbase.jar.
  --monitor-bin PATH          blackbox_run_monitor executable.

Controls (matching BLACKBOX_TPCC_ORDER_STATUS_* environment variables):
  --output-dir PATH
  --config PATH
  --warehouses N              Must be 1 for this acceptance run.
  --terminals N
  --weights CSV               Must be 0,0,100,0,0.
  --random-seed N
  --warmup-seconds N
  --duration-seconds N
  --rate unlimited|N
  --loader-threads N
  --loader-batch-size N
  --minimum-transactions N
  --minimum-id-transactions N
  --minimum-last-name-transactions N
  --minimum-multirow-transactions N
  --monitor-max-bootstrap-state-rows N
  --monitor-max-bootstrap-state-bytes N
  --stack-timeout-seconds N
  --loader-timeout-seconds N
  --workload-timeout-seconds N
  --status-timeout-seconds N
  --case-timeout-seconds N
  --kill-after-seconds N
  --redis-host HOST
  --redis-port PORT
  --db-host HOST
  --db-port PORT
  --java PATH
  --keep-stack
  --print-config              Validate controls and print the recorded JSON only.
EOF
}


need_value() {
  (( $# >= 2 )) || {
    printf 'missing value for %s\n' "$1" >&2
    exit 2
  }
}


while (( $# > 0 )); do
  case "$1" in
    --output-dir) need_value "$@"; OUTPUT_DIR=$2; shift 2 ;;
    --benchbase-dist) need_value "$@"; BENCHBASE_DIST=$2; shift 2 ;;
    --config) need_value "$@"; BENCHBASE_CONFIG=$2; shift 2 ;;
    --monitor-bin) need_value "$@"; MONITOR_BIN=$2; shift 2 ;;
    --java) need_value "$@"; JAVA_BIN=$2; shift 2 ;;
    --warehouses) need_value "$@"; WAREHOUSES=$2; shift 2 ;;
    --terminals) need_value "$@"; TERMINALS=$2; shift 2 ;;
    --weights) need_value "$@"; WEIGHTS=$2; shift 2 ;;
    --random-seed) need_value "$@"; RANDOM_SEED=$2; shift 2 ;;
    --warmup-seconds) need_value "$@"; WARMUP_SECONDS=$2; shift 2 ;;
    --duration-seconds) need_value "$@"; DURATION_SECONDS=$2; shift 2 ;;
    --rate) need_value "$@"; RATE=$2; shift 2 ;;
    --loader-threads) need_value "$@"; LOADER_THREADS=$2; shift 2 ;;
    --loader-batch-size) need_value "$@"; LOADER_BATCH_SIZE=$2; shift 2 ;;
    --minimum-transactions) need_value "$@"; MINIMUM_TRANSACTIONS=$2; shift 2 ;;
    --minimum-id-transactions) need_value "$@"; MINIMUM_ID_TRANSACTIONS=$2; shift 2 ;;
    --minimum-last-name-transactions) need_value "$@"; MINIMUM_LAST_NAME_TRANSACTIONS=$2; shift 2 ;;
    --minimum-multirow-transactions) need_value "$@"; MINIMUM_MULTIROW_TRANSACTIONS=$2; shift 2 ;;
    --monitor-max-bootstrap-state-rows) need_value "$@"; MONITOR_MAX_BOOTSTRAP_STATE_ROWS=$2; shift 2 ;;
    --monitor-max-bootstrap-state-bytes) need_value "$@"; MONITOR_MAX_BOOTSTRAP_STATE_BYTES=$2; shift 2 ;;
    --stack-timeout-seconds) need_value "$@"; STACK_TIMEOUT_SECONDS=$2; shift 2 ;;
    --loader-timeout-seconds) need_value "$@"; LOADER_TIMEOUT_SECONDS=$2; shift 2 ;;
    --workload-timeout-seconds) need_value "$@"; WORKLOAD_TIMEOUT_SECONDS=$2; shift 2 ;;
    --status-timeout-seconds) need_value "$@"; STATUS_TIMEOUT_SECONDS=$2; shift 2 ;;
    --case-timeout-seconds) need_value "$@"; CASE_TIMEOUT_SECONDS=$2; shift 2 ;;
    --kill-after-seconds) need_value "$@"; KILL_AFTER_SECONDS=$2; shift 2 ;;
    --redis-host) need_value "$@"; BLACKBOX_REDIS_HOST=$2; shift 2 ;;
    --redis-port) need_value "$@"; BLACKBOX_REDIS_PORT=$2; shift 2 ;;
    --db-host) need_value "$@"; BLACKBOX_DB_HOST=$2; shift 2 ;;
    --db-port) need_value "$@"; BLACKBOX_DB_PORT=$2; shift 2 ;;
    --keep-stack) KEEP_STACK=1; shift ;;
    --print-config) PRINT_CONFIG=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done


require_positive() {
  local name=$1
  local value=$2
  [[ $value =~ ^[1-9][0-9]*$ ]] || {
    printf '%s must be a positive integer, found %q\n' "$name" "$value" >&2
    exit 2
  }
}


require_nonnegative() {
  local name=$1
  local value=$2
  [[ $value =~ ^[0-9]+$ ]] || {
    printf '%s must be a nonnegative integer, found %q\n' "$name" "$value" >&2
    exit 2
  }
}


require_controls() {
  require_positive warehouses "$WAREHOUSES"
  [[ $WAREHOUSES == 1 ]] || {
    printf 'warehouses must be 1 for this acceptance run\n' >&2
    exit 2
  }
  require_positive terminals "$TERMINALS"
  [[ $WEIGHTS == 0,0,100,0,0 ]] || {
    printf 'weights must be exactly 0,0,100,0,0\n' >&2
    exit 2
  }
  [[ $RANDOM_SEED =~ ^-?[0-9]+$ ]] || {
    printf 'random-seed must be an integer\n' >&2
    exit 2
  }
  python3 -c '
import sys
value = int(sys.argv[1])
raise SystemExit(0 if -(1 << 63) <= value < (1 << 63) else 1)
' "$RANDOM_SEED" || {
    printf 'random-seed must fit a signed 64-bit integer\n' >&2
    exit 2
  }
  require_nonnegative warmup-seconds "$WARMUP_SECONDS"
  require_positive duration-seconds "$DURATION_SECONDS"
  [[ $RATE == unlimited || $RATE =~ ^[1-9][0-9]*$ ]] || {
    printf 'rate must be unlimited or a positive integer\n' >&2
    exit 2
  }
  local name
  for name in \
      LOADER_THREADS LOADER_BATCH_SIZE MINIMUM_TRANSACTIONS \
      MINIMUM_ID_TRANSACTIONS MINIMUM_LAST_NAME_TRANSACTIONS \
      MINIMUM_MULTIROW_TRANSACTIONS MONITOR_MAX_BOOTSTRAP_STATE_ROWS \
      MONITOR_MAX_BOOTSTRAP_STATE_BYTES STACK_TIMEOUT_SECONDS \
      LOADER_TIMEOUT_SECONDS WORKLOAD_TIMEOUT_SECONDS STATUS_TIMEOUT_SECONDS \
      CASE_TIMEOUT_SECONDS KILL_AFTER_SECONDS; do
    require_positive "${name,,}" "${!name}"
  done
  (( LOADER_BATCH_SIZE <= 2147483647 )) || {
    printf 'loader-batch-size exceeds the signed 32-bit range\n' >&2
    exit 2
  }
  [[ $KEEP_STACK == 0 || $KEEP_STACK == 1 ]] || {
    printf 'keep-stack must be 0 or 1\n' >&2
    exit 2
  }
}


session_roster() {
  local result=''
  local index
  for (( index = 1; index <= TERMINALS; ++index )); do
    [[ -z $result ]] || result+=,
    result+="tpcc/worker/$index"
  done
  printf '%s\n' "$result"
}


emit_config() {
  local destination=$1
  python3 - "$destination" "$WAREHOUSES" "$TERMINALS" "$RANDOM_SEED" \
    "$WARMUP_SECONDS" "$DURATION_SECONDS" "$RATE" "$LOADER_THREADS" \
    "$LOADER_BATCH_SIZE" "$MINIMUM_TRANSACTIONS" "$MINIMUM_ID_TRANSACTIONS" \
    "$MINIMUM_LAST_NAME_TRANSACTIONS" "$MINIMUM_MULTIROW_TRANSACTIONS" \
    "$MONITOR_MAX_BOOTSTRAP_STATE_ROWS" "$MONITOR_MAX_BOOTSTRAP_STATE_BYTES" \
    "$STACK_TIMEOUT_SECONDS" "$LOADER_TIMEOUT_SECONDS" "$WORKLOAD_TIMEOUT_SECONDS" \
    "$STATUS_TIMEOUT_SECONDS" "$CASE_TIMEOUT_SECONDS" "$KILL_AFTER_SECONDS" \
    "$KEEP_STACK" "$BLACKBOX_REDIS_HOST" "$BLACKBOX_REDIS_PORT" \
    "$BLACKBOX_DB_HOST" "$BLACKBOX_DB_PORT" "$(session_roster)" <<'PY'
import json
import os
import sys

(
    destination,
    warehouses,
    terminals,
    random_seed,
    warmup,
    duration,
    rate,
    loader_threads,
    loader_batch_size,
    minimum_transactions,
    minimum_id,
    minimum_last_name,
    minimum_multirow,
    bootstrap_rows,
    bootstrap_bytes,
    stack_timeout,
    loader_timeout,
    workload_timeout,
    status_timeout,
    case_timeout,
    kill_after,
    keep_stack,
    redis_host,
    redis_port,
    db_host,
    db_port,
    sessions,
) = sys.argv[1:]
document = {
    "kind": "tpcc_order_status_acceptance_config",
    "benchmark": "tpcc",
    "backend": "mariadb",
    "declared_isolation": "REPEATABLE_READ",
    "warehouses": int(warehouses),
    "terminals": int(terminals),
    "weights": [0, 0, 100, 0, 0],
    "random_seed": int(random_seed),
    "warmup_seconds": int(warmup),
    "duration_seconds": int(duration),
    "rate": rate,
    "loader_threads": int(loader_threads),
    "loader_batch_size": int(loader_batch_size),
    "minimum_transactions": int(minimum_transactions),
    "minimum_id_transactions": int(minimum_id),
    "minimum_last_name_transactions": int(minimum_last_name),
    "minimum_multirow_transactions": int(minimum_multirow),
    "monitor_max_bootstrap_state_rows": int(bootstrap_rows),
    "monitor_max_bootstrap_state_bytes": int(bootstrap_bytes),
    "stack_timeout_seconds": int(stack_timeout),
    "loader_timeout_seconds": int(loader_timeout),
    "workload_timeout_seconds": int(workload_timeout),
    "status_timeout_seconds": int(status_timeout),
    "case_timeout_seconds": int(case_timeout),
    "kill_after_seconds": int(kill_after),
    "keep_stack": keep_stack == "1",
    "redis_host": redis_host,
    "redis_port": int(redis_port),
    "db_host": db_host,
    "db_port": int(db_port),
    "expected_session_ids": sessions.split(","),
}
encoded = json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"
if destination == "-":
    sys.stdout.write(encoded)
else:
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write(encoded)
PY
}


require_controls
if (( PRINT_CONFIG == 1 )); then
  emit_config -
  exit 0
fi

: "${BENCHBASE_DIST:?set --benchbase-dist or BLACKBOX_TPCC_ORDER_STATUS_BENCHBASE_DIST}"
: "${MONITOR_BIN:?set --monitor-bin or BLACKBOX_TPCC_ORDER_STATUS_MONITOR_BIN}"
[[ -d $BENCHBASE_DIST && -f $BENCHBASE_DIST/benchbase.jar ]] || {
  printf 'BenchBase distribution lacks benchbase.jar: %s\n' "$BENCHBASE_DIST" >&2
  exit 2
}
if [[ -z $BENCHBASE_CONFIG ]]; then
  BENCHBASE_CONFIG="$BENCHBASE_DIST/config/mariadb/blackbox_tpcc_config.xml"
fi
[[ -f $BENCHBASE_CONFIG ]] || {
  printf 'BenchBase config is missing: %s\n' "$BENCHBASE_CONFIG" >&2
  exit 2
}
[[ -x $MONITOR_BIN && -x $E2E && -f $VALIDATOR ]] || {
  printf 'monitor, e2e, or validator path is unavailable\n' >&2
  exit 2
}
command -v "$JAVA_BIN" >/dev/null 2>&1 || [[ -x $JAVA_BIN ]] || {
  printf 'Java executable is unavailable: %s\n' "$JAVA_BIN" >&2
  exit 2
}
for command in python3 jq timeout "$BLACKBOX_DOCKER" "$BLACKBOX_REDIS_CLI" "$BLACKBOX_MARIADB_CLI"; do
  command -v "$command" >/dev/null 2>&1 || {
    printf 'required command is unavailable: %s\n' "$command" >&2
    exit 2
  }
done

if [[ -z $OUTPUT_DIR ]]; then
  OUTPUT_DIR=$(mktemp -d /tmp/tpcc-order-status-acceptance.XXXXXX)
else
  [[ ! -e $OUTPUT_DIR ]] || {
    printf 'output directory already exists: %s\n' "$OUTPUT_DIR" >&2
    exit 2
  }
  mkdir -p -- "$OUTPUT_DIR"
fi
OUTPUT_DIR=$(cd "$OUTPUT_DIR" && pwd -P)
umask 077
mkdir -p "$OUTPUT_DIR/loader-results" "$OUTPUT_DIR/workload-results"

COMPOSE=("$BLACKBOX_DOCKER" compose --file "$ROOT/docker-compose.yml" --profile mariadb)
cleanup() {
  local status=$?
  trap - EXIT
  if (( SUCCESS == 0 )); then
    "${COMPOSE[@]}" ps --all >"$OUTPUT_DIR/failure-compose-ps.txt" 2>&1 || true
    "${COMPOSE[@]}" logs --no-color --timestamps >"$OUTPUT_DIR/failure-compose.log" 2>&1 || true
  fi
  if (( KEEP_STACK == 0 )); then
    timeout --foreground --kill-after=10 120 \
      "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  printf 'TPC-C OrderStatus acceptance artifacts: %s\n' "$OUTPUT_DIR" >&2
  return "$status"
}
trap cleanup EXIT

BLACKBOX_LIVE_LOCK_TARGET=${BLACKBOX_LIVE_LOCK_TARGET:-$ROOT}
blackbox_acquire_live_stack_lock

RUN_CONFIG="$OUTPUT_DIR/run-config.json"
RUN_ID_FILE="$OUTPUT_DIR/run-id.txt"
MONITOR_METRICS="$OUTPUT_DIR/monitor-metrics.json"
LOADER_DECLARATION="$OUTPUT_DIR/loader-declaration.json"
LOADER_VALIDATION="$OUTPUT_DIR/loader-validation.json"
BOOTSTRAP_MANIFEST="$OUTPUT_DIR/bootstrap-manifest.json"
CLIENT_STREAM="$OUTPUT_DIR/client-stream.json"
STATUS_STREAM="$OUTPUT_DIR/status-stream.json"
CDC_TAIL="$OUTPUT_DIR/cdc-tail.json"
CUSTOMER_CATALOG="$OUTPUT_DIR/customer-catalog.jsonl"
REPORT="$OUTPUT_DIR/acceptance-report.json"
emit_config "$RUN_CONFIG"

quote_command() {
  local result
  printf -v result '%q ' "$@"
  printf '%s' "$result"
}

loader=(
  "$JAVA_BIN" -jar "$BENCHBASE_DIST/benchbase.jar"
  -b tpcc -c "$BENCHBASE_CONFIG"
  --create=false --load=true --execute=false
  --directory "$OUTPUT_DIR/loader-results"
)
workload=(
  "$JAVA_BIN" -jar "$BENCHBASE_DIST/benchbase.jar"
  -b tpcc -c "$BENCHBASE_CONFIG"
  --create=false --load=false --execute=true
  --directory "$OUTPUT_DIR/workload-results"
)
LOADER_COMMAND="cd $(printf '%q' "$BENCHBASE_DIST") && exec $(quote_command "${loader[@]}")"
WORKLOAD_COMMAND="cd $(printf '%q' "$BENCHBASE_DIST") && exec $(quote_command "${workload[@]}")"
printf '%s\n' "$LOADER_COMMAND" >"$OUTPUT_DIR/loader-command.txt"
printf '%s\n' "$WORKLOAD_COMMAND" >"$OUTPUT_DIR/workload-command.txt"

set +e
BLACKBOX_BENCHMARK=tpcc \
BLACKBOX_DECLARED_ISOLATION=REPEATABLE_READ \
BLACKBOX_DOCKER="$BLACKBOX_DOCKER" \
BLACKBOX_REDIS_CLI="$BLACKBOX_REDIS_CLI" \
BLACKBOX_MARIADB_CLI="$BLACKBOX_MARIADB_CLI" \
BLACKBOX_REDIS_HOST="$BLACKBOX_REDIS_HOST" \
BLACKBOX_REDIS_PORT="$BLACKBOX_REDIS_PORT" \
BLACKBOX_DB_HOST="$BLACKBOX_DB_HOST" \
BLACKBOX_DB_PORT="$BLACKBOX_DB_PORT" \
BLACKBOX_DB_USER="$BLACKBOX_DB_USER" \
BLACKBOX_DB_PASSWORD="$BLACKBOX_DB_PASSWORD" \
BLACKBOX_EXPECTED_SESSION_IDS="$(session_roster)" \
BLACKBOX_LOADER_COMMAND="$LOADER_COMMAND" \
BLACKBOX_WORKLOAD_COMMAND="$WORKLOAD_COMMAND" \
BLACKBOX_LOADER_DECLARED_COUNTS_FILE="$LOADER_DECLARATION" \
BLACKBOX_LOADER_VALIDATION_PATH="$LOADER_VALIDATION" \
BLACKBOX_BOOTSTRAP_MANIFEST_PATH="$BOOTSTRAP_MANIFEST" \
BLACKBOX_RUN_ID_PATH="$RUN_ID_FILE" \
BLACKBOX_MONITOR_METRICS_PATH="$MONITOR_METRICS" \
BLACKBOX_MONITOR_BIN="$MONITOR_BIN" \
BLACKBOX_MONITOR_MAX_BOOTSTRAP_STATE_ROWS="$MONITOR_MAX_BOOTSTRAP_STATE_ROWS" \
BLACKBOX_MONITOR_MAX_BOOTSTRAP_STATE_BYTES="$MONITOR_MAX_BOOTSTRAP_STATE_BYTES" \
BLACKBOX_LOADER_BATCH_SIZE="$LOADER_BATCH_SIZE" \
BLACKBOX_LOADER_SCALE_VALUE="$WAREHOUSES" \
BLACKBOX_STACK_TIMEOUT_SECONDS="$STACK_TIMEOUT_SECONDS" \
BLACKBOX_LOADER_TIMEOUT_SECONDS="$LOADER_TIMEOUT_SECONDS" \
BLACKBOX_WORKLOAD_TIMEOUT_SECONDS="$WORKLOAD_TIMEOUT_SECONDS" \
BLACKBOX_STATUS_TIMEOUT_SECONDS="$STATUS_TIMEOUT_SECONDS" \
BLACKBOX_PROCESSING_MODE=audited \
BLACKBOX_MEASUREMENT_MODE=audited \
BENCHBASE_MEASUREMENT_SCALE_FACTOR="$WAREHOUSES" \
BENCHBASE_MEASUREMENT_TERMINALS="$TERMINALS" \
BENCHBASE_MEASUREMENT_LOADER_THREADS="$LOADER_THREADS" \
BENCHBASE_MEASUREMENT_RANDOM_SEED="$RANDOM_SEED" \
BENCHBASE_MEASUREMENT_WARMUP_SECONDS="$WARMUP_SECONDS" \
BENCHBASE_MEASUREMENT_DURATION_SECONDS="$DURATION_SECONDS" \
BENCHBASE_MEASUREMENT_RATE="$RATE" \
BENCHBASE_MEASUREMENT_WEIGHTS="$WEIGHTS" \
  timeout --foreground --signal=TERM --kill-after="$KILL_AFTER_SECONDS" \
    "$CASE_TIMEOUT_SECONDS" "$E2E" >"$OUTPUT_DIR/e2e.log" 2>&1
status=$?
set -e
(( status == 0 )) || {
  printf 'e2e exited %d; see %s\n' "$status" "$OUTPUT_DIR/e2e.log" >&2
  exit "$status"
}

run_id=$(<"$RUN_ID_FILE")
blackbox_redis --json XRANGE "blackbox:client:$run_id" - + >"$CLIENT_STREAM"
blackbox_redis --json XRANGE "blackbox:status:$run_id" - + >"$STATUS_STREAM"
load_cdc_id=$(jq -er '
  [.[] | (.[1][1] | fromjson) |
   select(.kind == "status" and .lifecycle_state == "LOAD_FINISHED_SEEN") |
   .last_cdc_stream_id] |
  if length == 1 then .[0] else error("LOAD_FINISHED status count differs") end
' "$STATUS_STREAM")
blackbox_redis --json XRANGE cdc "($load_cdc_id" + >"$CDC_TAIL"
blackbox_database_query '
  SELECT JSON_OBJECT(
           "c_w_id", c_w_id,
           "c_d_id", c_d_id,
           "c_id", c_id,
           "c_last", c_last,
           "last_name_group_count",
             COUNT(*) OVER (PARTITION BY c_w_id, c_d_id, c_last))
    FROM customer
   ORDER BY c_w_id, c_d_id, c_id' >"$CUSTOMER_CATALOG"

python3 "$VALIDATOR" \
  --config "$RUN_CONFIG" \
  --run-id-file "$RUN_ID_FILE" \
  --client-stream "$CLIENT_STREAM" \
  --cdc-tail "$CDC_TAIL" \
  --status-stream "$STATUS_STREAM" \
  --customer-catalog "$CUSTOMER_CATALOG" \
  --monitor-metrics "$MONITOR_METRICS" \
  --output "$REPORT"
SUCCESS=1
