#!/usr/bin/env bash
# Shared live-run, capture, and cleanup machinery for focused TPC-C acceptance.
# shellcheck disable=SC2034 # Output variables are consumed by profile wrappers.

TPCC_ACCEPTANCE_ROOT=${TPCC_ACCEPTANCE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
# shellcheck source=scripts/blackbox_lifecycle.sh
. "$TPCC_ACCEPTANCE_ROOT/scripts/blackbox_lifecycle.sh"

BLACKBOX_DOCKER=${BLACKBOX_DOCKER:-docker}
TPCC_ACCEPTANCE_SUCCESS=0
TPCC_ACCEPTANCE_PRINT_CONFIG=0
TPCC_ACCEPTANCE_PARSED_ARGUMENTS=0
TPCC_ACCEPTANCE_VALIDATOR_EXTRA_ARGS=()


tpcc_acceptance_setting() {
  local suffix=$1
  local fallback=$2
  local variable="${TPCC_ACCEPTANCE_ENV_PREFIX}_${suffix}"
  if [[ -v $variable ]]; then
    printf '%s' "${!variable}"
  else
    printf '%s' "$fallback"
  fi
}


tpcc_acceptance_init() {
  : "${TPCC_ACCEPTANCE_ENV_PREFIX:?profile must set TPCC_ACCEPTANCE_ENV_PREFIX}"
  : "${TPCC_ACCEPTANCE_SLUG:?profile must set TPCC_ACCEPTANCE_SLUG}"
  : "${TPCC_ACCEPTANCE_CONFIG_KIND:?profile must set TPCC_ACCEPTANCE_CONFIG_KIND}"
  : "${TPCC_ACCEPTANCE_DEFAULT_WEIGHTS:?profile must set TPCC_ACCEPTANCE_DEFAULT_WEIGHTS}"
  : "${TPCC_ACCEPTANCE_DEFAULT_TERMINALS:?profile must set TPCC_ACCEPTANCE_DEFAULT_TERMINALS}"

  TPCC_OUTPUT_DIR=$(tpcc_acceptance_setting OUTPUT_DIR "")
  TPCC_BENCHBASE_DIST=$(tpcc_acceptance_setting BENCHBASE_DIST "")
  TPCC_BENCHBASE_CONFIG=$(tpcc_acceptance_setting CONFIG "")
  TPCC_MONITOR_BIN=$(tpcc_acceptance_setting MONITOR_BIN "")
  TPCC_JAVA_BIN=$(tpcc_acceptance_setting JAVA java)
  TPCC_E2E=$(tpcc_acceptance_setting E2E "$TPCC_ACCEPTANCE_ROOT/scripts/e2e.sh")
  TPCC_VALIDATOR=$(tpcc_acceptance_setting VALIDATOR "$TPCC_ACCEPTANCE_DEFAULT_VALIDATOR")
  TPCC_WAREHOUSES=$(tpcc_acceptance_setting WAREHOUSES 1)
  TPCC_TERMINALS=$(tpcc_acceptance_setting TERMINALS "$TPCC_ACCEPTANCE_DEFAULT_TERMINALS")
  TPCC_WEIGHTS=$(tpcc_acceptance_setting WEIGHTS "$TPCC_ACCEPTANCE_DEFAULT_WEIGHTS")
  TPCC_RANDOM_SEED=$(tpcc_acceptance_setting RANDOM_SEED "${TPCC_ACCEPTANCE_DEFAULT_RANDOM_SEED:-3}")
  TPCC_WARMUP_SECONDS=$(tpcc_acceptance_setting WARMUP_SECONDS 0)
  TPCC_DURATION_SECONDS=$(tpcc_acceptance_setting DURATION_SECONDS "${TPCC_ACCEPTANCE_DEFAULT_DURATION_SECONDS:-2}")
  TPCC_RATE=$(tpcc_acceptance_setting RATE unlimited)
  TPCC_LOADER_THREADS=$(tpcc_acceptance_setting LOADER_THREADS "${TPCC_ACCEPTANCE_DEFAULT_LOADER_THREADS:-1}")
  TPCC_LOADER_BATCH_SIZE=$(tpcc_acceptance_setting LOADER_BATCH_SIZE 128)
  TPCC_MONITOR_MAX_BOOTSTRAP_STATE_ROWS=$(
    tpcc_acceptance_setting MONITOR_MAX_BOOTSTRAP_STATE_ROWS 1000000
  )
  TPCC_MONITOR_MAX_BOOTSTRAP_STATE_BYTES=$(
    tpcc_acceptance_setting MONITOR_MAX_BOOTSTRAP_STATE_BYTES 1073741824
  )
  TPCC_STACK_TIMEOUT_SECONDS=$(tpcc_acceptance_setting STACK_TIMEOUT_SECONDS 180)
  TPCC_LOADER_TIMEOUT_SECONDS=$(tpcc_acceptance_setting LOADER_TIMEOUT_SECONDS 900)
  TPCC_WORKLOAD_TIMEOUT_SECONDS=$(tpcc_acceptance_setting WORKLOAD_TIMEOUT_SECONDS 300)
  TPCC_STATUS_TIMEOUT_SECONDS=$(tpcc_acceptance_setting STATUS_TIMEOUT_SECONDS 900)
  TPCC_CASE_TIMEOUT_SECONDS=$(tpcc_acceptance_setting CASE_TIMEOUT_SECONDS 1800)
  TPCC_KILL_AFTER_SECONDS=$(tpcc_acceptance_setting KILL_AFTER_SECONDS 15)
  TPCC_KEEP_STACK=$(tpcc_acceptance_setting KEEP_STACK 0)
}


tpcc_acceptance_common_usage() {
  cat <<'EOF'
  --output-dir PATH
  --benchbase-dist PATH
  --config PATH
  --monitor-bin PATH
  --java PATH
  --warehouses N
  --terminals N
  --weights CSV
  --random-seed N
  --warmup-seconds N
  --duration-seconds N
  --rate unlimited|N
  --loader-threads N
  --loader-batch-size N
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
  --keep-stack
  --print-config
EOF
}


tpcc_acceptance_need_value() {
  (( $# >= 2 )) || {
    printf 'missing value for %s\n' "$1" >&2
    exit 2
  }
}


tpcc_acceptance_parse_common_option() {
  TPCC_ACCEPTANCE_PARSED_ARGUMENTS=0
  case "$1" in
    --output-dir) tpcc_acceptance_need_value "$@"; TPCC_OUTPUT_DIR=$2 ;;
    --benchbase-dist) tpcc_acceptance_need_value "$@"; TPCC_BENCHBASE_DIST=$2 ;;
    --config) tpcc_acceptance_need_value "$@"; TPCC_BENCHBASE_CONFIG=$2 ;;
    --monitor-bin) tpcc_acceptance_need_value "$@"; TPCC_MONITOR_BIN=$2 ;;
    --java) tpcc_acceptance_need_value "$@"; TPCC_JAVA_BIN=$2 ;;
    --warehouses) tpcc_acceptance_need_value "$@"; TPCC_WAREHOUSES=$2 ;;
    --terminals) tpcc_acceptance_need_value "$@"; TPCC_TERMINALS=$2 ;;
    --weights) tpcc_acceptance_need_value "$@"; TPCC_WEIGHTS=$2 ;;
    --random-seed) tpcc_acceptance_need_value "$@"; TPCC_RANDOM_SEED=$2 ;;
    --warmup-seconds) tpcc_acceptance_need_value "$@"; TPCC_WARMUP_SECONDS=$2 ;;
    --duration-seconds) tpcc_acceptance_need_value "$@"; TPCC_DURATION_SECONDS=$2 ;;
    --rate) tpcc_acceptance_need_value "$@"; TPCC_RATE=$2 ;;
    --loader-threads) tpcc_acceptance_need_value "$@"; TPCC_LOADER_THREADS=$2 ;;
    --loader-batch-size) tpcc_acceptance_need_value "$@"; TPCC_LOADER_BATCH_SIZE=$2 ;;
    --monitor-max-bootstrap-state-rows)
      tpcc_acceptance_need_value "$@"; TPCC_MONITOR_MAX_BOOTSTRAP_STATE_ROWS=$2
      ;;
    --monitor-max-bootstrap-state-bytes)
      tpcc_acceptance_need_value "$@"; TPCC_MONITOR_MAX_BOOTSTRAP_STATE_BYTES=$2
      ;;
    --stack-timeout-seconds) tpcc_acceptance_need_value "$@"; TPCC_STACK_TIMEOUT_SECONDS=$2 ;;
    --loader-timeout-seconds) tpcc_acceptance_need_value "$@"; TPCC_LOADER_TIMEOUT_SECONDS=$2 ;;
    --workload-timeout-seconds) tpcc_acceptance_need_value "$@"; TPCC_WORKLOAD_TIMEOUT_SECONDS=$2 ;;
    --status-timeout-seconds) tpcc_acceptance_need_value "$@"; TPCC_STATUS_TIMEOUT_SECONDS=$2 ;;
    --case-timeout-seconds) tpcc_acceptance_need_value "$@"; TPCC_CASE_TIMEOUT_SECONDS=$2 ;;
    --kill-after-seconds) tpcc_acceptance_need_value "$@"; TPCC_KILL_AFTER_SECONDS=$2 ;;
    --redis-host) tpcc_acceptance_need_value "$@"; BLACKBOX_REDIS_HOST=$2 ;;
    --redis-port) tpcc_acceptance_need_value "$@"; BLACKBOX_REDIS_PORT=$2 ;;
    --db-host) tpcc_acceptance_need_value "$@"; BLACKBOX_DB_HOST=$2 ;;
    --db-port) tpcc_acceptance_need_value "$@"; BLACKBOX_DB_PORT=$2 ;;
    --keep-stack) TPCC_KEEP_STACK=1; TPCC_ACCEPTANCE_PARSED_ARGUMENTS=1; return 0 ;;
    --print-config) TPCC_ACCEPTANCE_PRINT_CONFIG=1; TPCC_ACCEPTANCE_PARSED_ARGUMENTS=1; return 0 ;;
    *) return 1 ;;
  esac
  TPCC_ACCEPTANCE_PARSED_ARGUMENTS=2
}


tpcc_acceptance_require_positive() {
  local name=$1
  local value=$2
  [[ $value =~ ^[1-9][0-9]*$ ]] || {
    printf '%s must be a positive integer, found %q\n' "$name" "$value" >&2
    exit 2
  }
}


tpcc_acceptance_require_nonnegative() {
  local name=$1
  local value=$2
  [[ $value =~ ^[0-9]+$ ]] || {
    printf '%s must be a nonnegative integer, found %q\n' "$name" "$value" >&2
    exit 2
  }
}


tpcc_acceptance_validate_common_controls() {
  tpcc_acceptance_require_positive warehouses "$TPCC_WAREHOUSES"
  tpcc_acceptance_require_positive terminals "$TPCC_TERMINALS"
  [[ $TPCC_RANDOM_SEED =~ ^-?[0-9]+$ ]] || {
    printf 'random-seed must be an integer\n' >&2
    exit 2
  }
  python3 -c '
import sys
value = int(sys.argv[1])
raise SystemExit(0 if -(1 << 31) <= value < (1 << 31) else 1)
' "$TPCC_RANDOM_SEED" || {
    printf 'random-seed must fit a signed 32-bit integer\n' >&2
    exit 2
  }
  tpcc_acceptance_require_nonnegative warmup-seconds "$TPCC_WARMUP_SECONDS"
  tpcc_acceptance_require_positive duration-seconds "$TPCC_DURATION_SECONDS"
  [[ $TPCC_RATE == unlimited || $TPCC_RATE =~ ^[1-9][0-9]*$ ]] || {
    printf 'rate must be unlimited or a positive integer\n' >&2
    exit 2
  }
  local name
  for name in \
      TPCC_LOADER_THREADS TPCC_LOADER_BATCH_SIZE \
      TPCC_MONITOR_MAX_BOOTSTRAP_STATE_ROWS TPCC_MONITOR_MAX_BOOTSTRAP_STATE_BYTES \
      TPCC_STACK_TIMEOUT_SECONDS TPCC_LOADER_TIMEOUT_SECONDS \
      TPCC_WORKLOAD_TIMEOUT_SECONDS TPCC_STATUS_TIMEOUT_SECONDS \
      TPCC_CASE_TIMEOUT_SECONDS TPCC_KILL_AFTER_SECONDS; do
    tpcc_acceptance_require_positive "${name,,}" "${!name}"
  done
  (( TPCC_LOADER_BATCH_SIZE <= 2147483647 )) || {
    printf 'loader-batch-size exceeds the signed 32-bit range\n' >&2
    exit 2
  }
  [[ $TPCC_KEEP_STACK == 0 || $TPCC_KEEP_STACK == 1 ]] || {
    printf 'keep-stack must be 0 or 1\n' >&2
    exit 2
  }
}


tpcc_acceptance_session_roster() {
  local result=''
  local index
  for (( index = 1; index <= TPCC_TERMINALS; ++index )); do
    [[ -z $result ]] || result+=,
    result+="tpcc/worker/$index"
  done
  printf '%s\n' "$result"
}


tpcc_acceptance_emit_config() {
  local destination=$1
  local extras
  extras=$(tpcc_acceptance_config_extras)
  python3 - "$destination" "$TPCC_ACCEPTANCE_CONFIG_KIND" "$TPCC_WAREHOUSES" \
    "$TPCC_TERMINALS" "$TPCC_WEIGHTS" "$TPCC_RANDOM_SEED" \
    "$TPCC_WARMUP_SECONDS" "$TPCC_DURATION_SECONDS" "$TPCC_RATE" \
    "$TPCC_LOADER_THREADS" "$TPCC_LOADER_BATCH_SIZE" \
    "$TPCC_MONITOR_MAX_BOOTSTRAP_STATE_ROWS" "$TPCC_MONITOR_MAX_BOOTSTRAP_STATE_BYTES" \
    "$TPCC_STACK_TIMEOUT_SECONDS" "$TPCC_LOADER_TIMEOUT_SECONDS" \
    "$TPCC_WORKLOAD_TIMEOUT_SECONDS" "$TPCC_STATUS_TIMEOUT_SECONDS" \
    "$TPCC_CASE_TIMEOUT_SECONDS" "$TPCC_KILL_AFTER_SECONDS" "$TPCC_KEEP_STACK" \
    "$BLACKBOX_REDIS_HOST" "$BLACKBOX_REDIS_PORT" "$BLACKBOX_DB_HOST" \
    "$BLACKBOX_DB_PORT" "$(tpcc_acceptance_session_roster)" "$extras" <<'PY'
import json
import os
import sys

(
    destination, kind, warehouses, terminals, weights, random_seed, warmup,
    duration, rate, loader_threads, loader_batch_size, bootstrap_rows,
    bootstrap_bytes, stack_timeout, loader_timeout, workload_timeout,
    status_timeout, case_timeout, kill_after, keep_stack, redis_host,
    redis_port, db_host, db_port, sessions, extras,
) = sys.argv[1:]
document = {
    "kind": kind,
    "benchmark": "tpcc",
    "backend": "mariadb",
    "declared_isolation": "REPEATABLE_READ",
    "warehouses": int(warehouses),
    "terminals": int(terminals),
    "weights": [int(value) for value in weights.split(",")],
    "random_seed": int(random_seed),
    "warmup_seconds": int(warmup),
    "duration_seconds": int(duration),
    "rate": rate,
    "loader_threads": int(loader_threads),
    "loader_batch_size": int(loader_batch_size),
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
extra_document = json.loads(extras)
if not isinstance(extra_document, dict) or set(document).intersection(extra_document):
    raise SystemExit("profile config extras are malformed or overlap common fields")
document.update(extra_document)
encoded = json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"
if destination == "-":
    sys.stdout.write(encoded)
else:
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write(encoded)
PY
}


tpcc_acceptance_quote_command() {
  local result
  printf -v result '%q ' "$@"
  printf '%s' "$result"
}


tpcc_acceptance_cleanup() {
  local status=$?
  trap - EXIT
  if (( TPCC_ACCEPTANCE_SUCCESS == 0 )); then
    "${TPCC_ACCEPTANCE_COMPOSE[@]}" ps --all \
      >"$TPCC_OUTPUT_DIR/failure-compose-ps.txt" 2>&1 || true
    "${TPCC_ACCEPTANCE_COMPOSE[@]}" logs --no-color --timestamps \
      >"$TPCC_OUTPUT_DIR/failure-compose.log" 2>&1 || true
  fi
  if (( TPCC_KEEP_STACK == 0 )); then
    timeout --foreground --kill-after=10 120 \
      "${TPCC_ACCEPTANCE_COMPOSE[@]}" down --volumes --remove-orphans \
      >/dev/null 2>&1 || true
  fi
  printf 'TPC-C %s acceptance artifacts: %s\n' \
    "$TPCC_ACCEPTANCE_SLUG" "$TPCC_OUTPUT_DIR" >&2
  return "$status"
}


tpcc_acceptance_preflight() {
  : "${TPCC_BENCHBASE_DIST:?set --benchbase-dist or the profile BENCHBASE_DIST environment variable}"
  : "${TPCC_MONITOR_BIN:?set --monitor-bin or the profile MONITOR_BIN environment variable}"
  [[ -d $TPCC_BENCHBASE_DIST && -f $TPCC_BENCHBASE_DIST/benchbase.jar ]] || {
    printf 'BenchBase distribution lacks benchbase.jar: %s\n' "$TPCC_BENCHBASE_DIST" >&2
    exit 2
  }
  if [[ -z $TPCC_BENCHBASE_CONFIG ]]; then
    TPCC_BENCHBASE_CONFIG="$TPCC_BENCHBASE_DIST/config/mariadb/blackbox_tpcc_config.xml"
  fi
  [[ -f $TPCC_BENCHBASE_CONFIG ]] || {
    printf 'BenchBase config is missing: %s\n' "$TPCC_BENCHBASE_CONFIG" >&2
    exit 2
  }
  [[ -x $TPCC_MONITOR_BIN && -x $TPCC_E2E && -f $TPCC_VALIDATOR ]] || {
    printf 'monitor, e2e, or validator path is unavailable\n' >&2
    exit 2
  }
  command -v "$TPCC_JAVA_BIN" >/dev/null 2>&1 || [[ -x $TPCC_JAVA_BIN ]] || {
    printf 'Java executable is unavailable: %s\n' "$TPCC_JAVA_BIN" >&2
    exit 2
  }
  local command
  for command in \
      python3 jq timeout "$BLACKBOX_DOCKER" "$BLACKBOX_REDIS_CLI" \
      "$BLACKBOX_MARIADB_CLI"; do
    command -v "$command" >/dev/null 2>&1 || {
      printf 'required command is unavailable: %s\n' "$command" >&2
      exit 2
    }
  done
}


tpcc_acceptance_run() {
  tpcc_acceptance_validate_common_controls
  tpcc_acceptance_validate_profile_controls
  if (( TPCC_ACCEPTANCE_PRINT_CONFIG == 1 )); then
    tpcc_acceptance_emit_config -
    return 0
  fi
  tpcc_acceptance_preflight

  if [[ -z $TPCC_OUTPUT_DIR ]]; then
    TPCC_OUTPUT_DIR=$(mktemp -d "/tmp/tpcc-${TPCC_ACCEPTANCE_SLUG}-acceptance.XXXXXX")
  else
    [[ ! -e $TPCC_OUTPUT_DIR ]] || {
      printf 'output directory already exists: %s\n' "$TPCC_OUTPUT_DIR" >&2
      exit 2
    }
    mkdir -p -- "$TPCC_OUTPUT_DIR"
  fi
  TPCC_OUTPUT_DIR=$(cd "$TPCC_OUTPUT_DIR" && pwd -P)
  umask 077
  mkdir -p "$TPCC_OUTPUT_DIR/loader-results" "$TPCC_OUTPUT_DIR/workload-results"

  TPCC_ACCEPTANCE_COMPOSE=(
    "$BLACKBOX_DOCKER" compose
    --file "$TPCC_ACCEPTANCE_ROOT/docker-compose.yml" --profile mariadb
  )
  trap tpcc_acceptance_cleanup EXIT
  BLACKBOX_LIVE_LOCK_TARGET=${BLACKBOX_LIVE_LOCK_TARGET:-$TPCC_ACCEPTANCE_ROOT}
  blackbox_acquire_live_stack_lock

  local run_config="$TPCC_OUTPUT_DIR/run-config.json"
  local run_id_file="$TPCC_OUTPUT_DIR/run-id.txt"
  local monitor_metrics="$TPCC_OUTPUT_DIR/monitor-metrics.json"
  local loader_declaration="$TPCC_OUTPUT_DIR/loader-declaration.json"
  local loader_validation="$TPCC_OUTPUT_DIR/loader-validation.json"
  local bootstrap_manifest="$TPCC_OUTPUT_DIR/bootstrap-manifest.json"
  local client_stream="$TPCC_OUTPUT_DIR/client-stream.json"
  local status_stream="$TPCC_OUTPUT_DIR/status-stream.json"
  local cdc_tail="$TPCC_OUTPUT_DIR/cdc-tail.json"
  local report="$TPCC_OUTPUT_DIR/acceptance-report.json"
  tpcc_acceptance_emit_config "$run_config"

  local loader=(
    "$TPCC_JAVA_BIN" -jar "$TPCC_BENCHBASE_DIST/benchbase.jar"
    -b tpcc -c "$TPCC_BENCHBASE_CONFIG"
    --create=false --load=true --execute=false
    --directory "$TPCC_OUTPUT_DIR/loader-results"
  )
  local workload=(
    "$TPCC_JAVA_BIN" -jar "$TPCC_BENCHBASE_DIST/benchbase.jar"
    -b tpcc -c "$TPCC_BENCHBASE_CONFIG"
    --create=false --load=false --execute=true
    --directory "$TPCC_OUTPUT_DIR/workload-results"
  )
  local loader_command workload_command
  loader_command="cd $(printf '%q' "$TPCC_BENCHBASE_DIST") && exec $(
    tpcc_acceptance_quote_command "${loader[@]}")"
  workload_command="cd $(printf '%q' "$TPCC_BENCHBASE_DIST") && exec $(
    tpcc_acceptance_quote_command "${workload[@]}")"
  printf '%s\n' "$loader_command" >"$TPCC_OUTPUT_DIR/loader-command.txt"
  printf '%s\n' "$workload_command" >"$TPCC_OUTPUT_DIR/workload-command.txt"

  local status
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
  BLACKBOX_EXPECTED_SESSION_IDS="$(tpcc_acceptance_session_roster)" \
  BLACKBOX_LOADER_COMMAND="$loader_command" \
  BLACKBOX_WORKLOAD_COMMAND="$workload_command" \
  BLACKBOX_LOADER_DECLARED_COUNTS_FILE="$loader_declaration" \
  BLACKBOX_LOADER_VALIDATION_PATH="$loader_validation" \
  BLACKBOX_BOOTSTRAP_MANIFEST_PATH="$bootstrap_manifest" \
  BLACKBOX_RUN_ID_PATH="$run_id_file" \
  BLACKBOX_MONITOR_METRICS_PATH="$monitor_metrics" \
  BLACKBOX_MONITOR_BIN="$TPCC_MONITOR_BIN" \
  BLACKBOX_MONITOR_MAX_BOOTSTRAP_STATE_ROWS="$TPCC_MONITOR_MAX_BOOTSTRAP_STATE_ROWS" \
  BLACKBOX_MONITOR_MAX_BOOTSTRAP_STATE_BYTES="$TPCC_MONITOR_MAX_BOOTSTRAP_STATE_BYTES" \
  BLACKBOX_LOADER_BATCH_SIZE="$TPCC_LOADER_BATCH_SIZE" \
  BLACKBOX_LOADER_SCALE_VALUE="$TPCC_WAREHOUSES" \
  BLACKBOX_STACK_TIMEOUT_SECONDS="$TPCC_STACK_TIMEOUT_SECONDS" \
  BLACKBOX_LOADER_TIMEOUT_SECONDS="$TPCC_LOADER_TIMEOUT_SECONDS" \
  BLACKBOX_WORKLOAD_TIMEOUT_SECONDS="$TPCC_WORKLOAD_TIMEOUT_SECONDS" \
  BLACKBOX_STATUS_TIMEOUT_SECONDS="$TPCC_STATUS_TIMEOUT_SECONDS" \
  BLACKBOX_PROCESSING_MODE=audited \
  BLACKBOX_MEASUREMENT_MODE=audited \
  BENCHBASE_MEASUREMENT_SCALE_FACTOR="$TPCC_WAREHOUSES" \
  BENCHBASE_MEASUREMENT_TERMINALS="$TPCC_TERMINALS" \
  BENCHBASE_MEASUREMENT_LOADER_THREADS="$TPCC_LOADER_THREADS" \
  BENCHBASE_MEASUREMENT_RANDOM_SEED="$TPCC_RANDOM_SEED" \
  BENCHBASE_MEASUREMENT_WARMUP_SECONDS="$TPCC_WARMUP_SECONDS" \
  BENCHBASE_MEASUREMENT_DURATION_SECONDS="$TPCC_DURATION_SECONDS" \
  BENCHBASE_MEASUREMENT_RATE="$TPCC_RATE" \
  BENCHBASE_MEASUREMENT_WEIGHTS="$TPCC_WEIGHTS" \
    timeout --foreground --signal=TERM --kill-after="$TPCC_KILL_AFTER_SECONDS" \
      "$TPCC_CASE_TIMEOUT_SECONDS" "$TPCC_E2E" \
      >"$TPCC_OUTPUT_DIR/e2e.log" 2>&1
  status=$?
  set -e
  (( status == 0 )) || {
    printf 'e2e exited %d; see %s\n' "$status" "$TPCC_OUTPUT_DIR/e2e.log" >&2
    exit "$status"
  }

  local run_id load_cdc_id
  run_id=$(<"$run_id_file")
  blackbox_redis --json XRANGE "blackbox:client:$run_id" - + >"$client_stream"
  blackbox_redis --json XRANGE "blackbox:status:$run_id" - + >"$status_stream"
  load_cdc_id=$(jq -er '
    [.[] | (.[1][1] | fromjson) |
     select(.kind == "status" and .lifecycle_state == "LOAD_FINISHED_SEEN") |
     .last_cdc_stream_id] |
    if length == 1 then .[0] else error("LOAD_FINISHED status count differs") end
  ' "$status_stream")
  blackbox_redis --json XRANGE cdc "($load_cdc_id" + >"$cdc_tail"

  TPCC_ACCEPTANCE_VALIDATOR_EXTRA_ARGS=()
  tpcc_acceptance_capture_profile_artifacts "$TPCC_OUTPUT_DIR"
  python3 "$TPCC_VALIDATOR" \
    --config "$run_config" \
    --run-id-file "$run_id_file" \
    --client-stream "$client_stream" \
    --cdc-tail "$cdc_tail" \
    --status-stream "$status_stream" \
    --monitor-metrics "$monitor_metrics" \
    --output "$report" \
    "${TPCC_ACCEPTANCE_VALIDATOR_EXTRA_ARGS[@]}"
  TPCC_ACCEPTANCE_SUCCESS=1
}
