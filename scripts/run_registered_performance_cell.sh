#!/usr/bin/env bash
# Execute one externally declared performance cell. Benchmark, backend, mode,
# workload, capture profile, and workload parameters are opaque inputs.
set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BACKEND=
BENCHMARK=
MODE=
WORKLOAD=
CAPTURE_PROFILE=
EXECUTION_PROFILE=
ARCHIVE=
ARCHIVE_SHA256=
CONFIG=
EXPECTED_SESSIONS=
OUTPUT=
MONITOR_BIN=
DATABASE_CONTAINER=
CONNECTOR_CONTAINER=
REPETITION=0
LOADER_TIMEOUT=3600
WORKLOAD_TIMEOUT=900
STACK_TIMEOUT=600
STATUS_TIMEOUT=900
DRAIN_LIMIT=120
RESOURCE_INTERVAL=1
REDIS_MAXMEMORY=8589934592
MONITOR_BOOTSTRAP_STATE_BYTES=268435456
MONITOR_PENDING_CLIENT_RECORDS=1000000
MONITOR_PENDING_CLIENT_BYTES=268435456
MONITOR_RETAINED_IDENTITY_BYTES=2147483648
MONITOR_CHECKER_STATE_BYTES=536870912
HISTORY_GC=${BLACKBOX_MONITOR_HISTORY_GC:-off}
WORKLOAD_RSS_SAMPLING=${BLACKBOX_MONITOR_WORKLOAD_RSS_SAMPLING:-off}
EVIDENCE_RETENTION=compact
RETAINED_EVIDENCE=compact
HEADLINE_TRANSACTION_TYPE=
HEADLINE_RATE_LABEL=
PARAMETERS=()
STACK_MAY_BE_RUNNING=0

while (( $# > 0 )); do
  case "$1" in
    --backend) BACKEND=$2; shift 2 ;;
    --benchmark) BENCHMARK=$2; shift 2 ;;
    --mode) MODE=$2; shift 2 ;;
    --workload) WORKLOAD=$2; shift 2 ;;
    --capture-profile) CAPTURE_PROFILE=$2; shift 2 ;;
    --execution-profile) EXECUTION_PROFILE=$2; shift 2 ;;
    --archive) ARCHIVE=$2; shift 2 ;;
    --archive-sha256) ARCHIVE_SHA256=$2; shift 2 ;;
    --config) CONFIG=$2; shift 2 ;;
    --expected-sessions) EXPECTED_SESSIONS=$2; shift 2 ;;
    --output) OUTPUT=$2; shift 2 ;;
    --monitor-bin) MONITOR_BIN=$2; shift 2 ;;
    --database-container) DATABASE_CONTAINER=$2; shift 2 ;;
    --connector-container) CONNECTOR_CONTAINER=$2; shift 2 ;;
    --repetition) REPETITION=$2; shift 2 ;;
    --parameter) PARAMETERS+=("$2"); shift 2 ;;
    --loader-timeout-seconds) LOADER_TIMEOUT=$2; shift 2 ;;
    --workload-timeout-seconds) WORKLOAD_TIMEOUT=$2; shift 2 ;;
    --stack-timeout-seconds) STACK_TIMEOUT=$2; shift 2 ;;
    --status-timeout-seconds) STATUS_TIMEOUT=$2; shift 2 ;;
    --drain-limit-seconds) DRAIN_LIMIT=$2; shift 2 ;;
    --resource-interval-seconds) RESOURCE_INTERVAL=$2; shift 2 ;;
    --redis-maxmemory-bytes) REDIS_MAXMEMORY=$2; shift 2 ;;
    --monitor-bootstrap-state-bytes) MONITOR_BOOTSTRAP_STATE_BYTES=$2; shift 2 ;;
    --monitor-pending-client-records) MONITOR_PENDING_CLIENT_RECORDS=$2; shift 2 ;;
    --monitor-pending-client-bytes) MONITOR_PENDING_CLIENT_BYTES=$2; shift 2 ;;
    --monitor-retained-identity-bytes) MONITOR_RETAINED_IDENTITY_BYTES=$2; shift 2 ;;
    --monitor-checker-state-bytes) MONITOR_CHECKER_STATE_BYTES=$2; shift 2 ;;
    --history-gc) HISTORY_GC=$2; shift 2 ;;
    --workload-rss-sampling) WORKLOAD_RSS_SAMPLING=$2; shift 2 ;;
    --evidence-retention) EVIDENCE_RETENTION=$2; shift 2 ;;
    --headline-transaction-type) HEADLINE_TRANSACTION_TYPE=$2; shift 2 ;;
    --headline-rate-label) HEADLINE_RATE_LABEL=$2; shift 2 ;;
    *) printf 'unknown registered performance-cell argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done

for name in BACKEND BENCHMARK MODE WORKLOAD CAPTURE_PROFILE EXECUTION_PROFILE; do
  value=${!name}
  [[ $value =~ ^[A-Za-z][A-Za-z0-9_]{0,63}$ ]] || {
    printf '%s is not a bounded registry name\n' "$name" >&2
    exit 2
  }
done
[[ $EXECUTION_PROFILE == direct_benchbase ||
   $EXECUTION_PROFILE == coordinated_cdc_only ||
   $EXECUTION_PROFILE == coordinated_e2e ]]
[[ $HISTORY_GC == on || $HISTORY_GC == off ]] || {
  printf 'history-gc must be on or off\n' >&2; exit 2;
}
[[ $HISTORY_GC == off || $EXECUTION_PROFILE == coordinated_e2e ]] || {
  printf 'history-gc requires coordinated_e2e\n' >&2; exit 2;
}
export BLACKBOX_MONITOR_HISTORY_GC="$HISTORY_GC"
[[ $WORKLOAD_RSS_SAMPLING == on || $WORKLOAD_RSS_SAMPLING == off ]] || {
  printf 'workload-rss-sampling must be on or off\n' >&2; exit 2;
}
[[ $WORKLOAD_RSS_SAMPLING == off || $EXECUTION_PROFILE == coordinated_e2e ]] || {
  printf 'workload-rss-sampling requires coordinated_e2e\n' >&2; exit 2;
}
export BLACKBOX_MONITOR_WORKLOAD_RSS_SAMPLING="$WORKLOAD_RSS_SAMPLING"
for value in "$REPETITION" "$LOADER_TIMEOUT" "$WORKLOAD_TIMEOUT" "$STACK_TIMEOUT" "$STATUS_TIMEOUT" "$DRAIN_LIMIT" "$REDIS_MAXMEMORY" "$MONITOR_BOOTSTRAP_STATE_BYTES" "$MONITOR_PENDING_CLIENT_RECORDS" "$MONITOR_PENDING_CLIENT_BYTES" "$MONITOR_RETAINED_IDENTITY_BYTES" "$MONITOR_CHECKER_STATE_BYTES"; do
  [[ $value =~ ^[1-9][0-9]*$ ]] || { printf 'positive integer parameter is invalid\n' >&2; exit 2; }
done
[[ $RESOURCE_INTERVAL =~ ^[0-9]+([.][0-9]+)?$ ]]
[[ $HIGH_WATER_THRESHOLD =~ ^[0-9]+([.][0-9]+)?$ ]]
[[ $EVIDENCE_RETENTION == compact || $EVIDENCE_RETENTION == full ]] || {
  printf 'evidence retention must be compact or full\n' >&2
  exit 2
}
[[ -f $ARCHIVE ]] || { printf 'build artifact is missing\n' >&2; exit 2; }
[[ -x $MONITOR_BIN && -f $ROOT/protocol/backends/$BACKEND.json &&
   -f $ROOT/protocol/adapters/$BENCHMARK.json ]] || {
  printf 'cell registration or monitor executable is missing\n' >&2
  exit 2
}
printf '%s  %s\n' "$ARCHIVE_SHA256" "$ARCHIVE" | sha256sum --check --status
[[ ! -e $OUTPUT ]] || { printf 'cell output already exists: %s\n' "$OUTPUT" >&2; exit 2; }
mkdir -p "$OUTPUT/loader-results" "$OUTPUT/workload-results"
OUTPUT=$(cd "$OUTPUT" && pwd -P)

for parameter in "${PARAMETERS[@]}"; do
  name=${parameter%%=*}
  [[ $parameter == *=* &&
     ( $name == BLACKBOX_LOADER_SCALE_VALUE || $name == BENCHBASE_MEASUREMENT_* ) &&
     $name =~ ^[A-Z][A-Z0-9_]{0,95}$ ]] || {
    printf 'forwarded workload parameter is invalid: %s\n' "$parameter" >&2
    exit 2
  }
  export "${parameter?}"
done

export BLACKBOX_BACKEND="$BACKEND" BLACKBOX_BENCHMARK="$BENCHMARK"
export BLACKBOX_CAPTURE_PROFILE="$CAPTURE_PROFILE"
# shellcheck source=scripts/blackbox_lifecycle.sh disable=SC1091
. "$ROOT/scripts/blackbox_lifecycle.sh"

DIST_PARENT=$(mktemp -d /tmp/registered-performance-cell.XXXXXX)
cleanup() {
  local status=$?
  if (( STACK_MAY_BE_RUNNING == 1 )) && declare -F teardown_stack >/dev/null; then
    teardown_stack
  fi
  rm -rf -- "$DIST_PARENT"
  return "$status"
}
trap cleanup EXIT
tar -xzf "$ARCHIVE" -C "$DIST_PARENT"
mapfile -t roots < <(find "$DIST_PARENT" -mindepth 1 -maxdepth 1 -type d -print)
(( ${#roots[@]} == 1 )) || { printf 'BenchBase archive root differs\n' >&2; exit 2; }
DIST=${roots[0]}
SOURCE_CONFIG="$DIST/$CONFIG"
[[ -f $SOURCE_CONFIG ]] || { printf 'declared BenchBase config is missing\n' >&2; exit 2; }
CONFIG_PATH=$SOURCE_CONFIG
if [[ $EXECUTION_PROFILE != coordinated_e2e ]]; then
  CONFIG_PATH="$OUTPUT/benchbase-uninstrumented.xml"
  python3 - "$SOURCE_CONFIG" "$CONFIG_PATH" <<'PY'
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

source, output = (Path(value) for value in sys.argv[1:])
tree = ET.parse(source)
root = tree.getroot()
blackbox = root.find("blackbox")
if blackbox is None:
    raise SystemExit("source configuration has no blackbox element to disable")
mode = blackbox.find("mode")
if mode is None:
    raise SystemExit("source configuration has no blackbox mode element")
mode.text = "disabled"
retained = {"mode", "backend", "declaredIsolation", "benchmark"}
for child in list(blackbox):
    if child.tag not in retained:
        blackbox.remove(child)
tree.write(output, encoding="utf-8", xml_declaration=True)
PY
fi

shell_join() {
  local output='' argument
  for argument in "$@"; do
    printf -v output '%s%q ' "$output" "$argument"
  done
  printf '%s' "$output"
}

write_checksums() {
  local directory=$1 temporary
  temporary=$(mktemp /tmp/registered-performance-checksums.XXXXXX)
  if ! (cd "$directory" &&
    find . -type f ! -name SHA256SUMS -print0 | LC_ALL=C sort -z |
      xargs -0 sha256sum >"$temporary"); then
    rm -f -- "$temporary"
    return 1
  fi
  mv "$temporary" "$directory/SHA256SUMS"
  (cd "$directory" && sha256sum --check SHA256SUMS >/dev/null)
}

LOADER_DECLARATION="$OUTPUT/loader-declaration.json"
LOADER_ARGS=(
  java -Xms1g -Xmx1g -XX:+AlwaysPreTouch -XX:+UseG1GC -jar benchbase.jar
  -b "$BENCHMARK" -c "$CONFIG_PATH" --create=false --load=true --execute=false
  --directory "$OUTPUT/loader-results"
)
MEASURED_ARGS=(
  "$ROOT/scripts/run_measured_command.sh"
  --resource-output "$OUTPUT/resource-samples.csv"
  --network-output "$OUTPUT/network-metrics.json"
  --sample-duration-seconds "$WORKLOAD_TIMEOUT"
  --sample-interval-seconds "$RESOURCE_INTERVAL"
  --process-label benchbase
  --container "$BACKEND=$DATABASE_CONTAINER"
)
if [[ $EXECUTION_PROFILE != direct_benchbase ]]; then
  MEASURED_ARGS+=(--container redis=cdc-redis --container "debezium=$CONNECTOR_CONTAINER")
fi
if declare -F blackbox_backend_performance_telemetry_sampler >/dev/null; then
  BACKEND_SAMPLER=$(blackbox_backend_performance_telemetry_sampler)
  if [[ -n $BACKEND_SAMPLER ]]; then
    MEASURED_ARGS+=(
      --backend-sampler "$BACKEND_SAMPLER"
      --backend-output "$OUTPUT/backend-telemetry.json"
    )
  fi
fi
MEASURED_ARGS+=(-- java -Xms1g -Xmx1g -XX:+AlwaysPreTouch -XX:+UseG1GC -jar benchbase.jar
  -b "$BENCHMARK" -c "$CONFIG_PATH" --create=false --load=false --execute=true
  --directory "$OUTPUT/workload-results")
LOADER_COMMAND="cd $(printf '%q' "$DIST") && exec $(shell_join "${LOADER_ARGS[@]}")"
if [[ $BACKEND == oceanbase_mysql ]]; then
  loader_url="${BLACKBOX_JDBC_URL%%\?*}?"
  IFS='&' read -r -a loader_parameters <<<"${BLACKBOX_JDBC_URL#*\?}"
  for parameter in "${loader_parameters[@]}"; do
    [[ $BLACKBOX_JDBC_URL == *\?* && -n $parameter &&
       $parameter != rewriteBatchedStatements=* ]] || continue
    loader_url+="$parameter&"
  done
  LOADER_COMMAND="export BLACKBOX_JDBC_URL=$(printf '%q' "${loader_url}rewriteBatchedStatements=true"); $LOADER_COMMAND"
fi
WORKLOAD_COMMAND="cd $(printf '%q' "$DIST") && exec $(shell_join "${MEASURED_ARGS[@]}")"
printf '%s\n' "$LOADER_COMMAND" >"$OUTPUT/loader-command.txt"
printf '%s\n' "$WORKLOAD_COMMAND" >"$OUTPUT/workload-command.txt"
printf '%s\n' "${PARAMETERS[@]}" >"$OUTPUT/workload-parameters.txt"

capture_status_and_redis_info() {
  [[ -f $OUTPUT/run-id.txt ]] || return 0
  local run_id
  run_id=$(tr -d '[:space:]' <"$OUTPUT/run-id.txt")
  redis-cli -h host.docker.internal --json XRANGE "blackbox:status:$run_id" - + \
    >"$OUTPUT/status-stream.json"
  redis-cli -h host.docker.internal --raw INFO all >"$OUTPUT/redis-info-after.txt"
}

capture_full_streams() {
  [[ -f $OUTPUT/run-id.txt ]] || return 0
  local run_id
  run_id=$(tr -d '[:space:]' <"$OUTPUT/run-id.txt")
  timeout "$STATUS_TIMEOUT" redis-cli -h host.docker.internal --json XRANGE cdc - + >"$OUTPUT/cdc-stream.json"
  timeout "$STATUS_TIMEOUT" redis-cli -h host.docker.internal --json XRANGE "blackbox:client:$run_id" - + \
    >"$OUTPUT/client-stream.json"
  capture_status_and_redis_info
  RETAINED_EVIDENCE=full
  jq -n --arg requested "$EVIDENCE_RETENTION" \
    '{kind:"performance_evidence_retention",requested:$requested,retained:"full",
      full_streams_retained:true}' >"$OUTPUT/evidence-retention.json"
}

capture_compact_streams() {
  [[ -f $OUTPUT/run-id.txt ]] || return 1
  local run_id boundaries load_finished_id end_fence_id final_status_id
  run_id=$(tr -d '[:space:]' <"$OUTPUT/run-id.txt")
  capture_status_and_redis_info
  boundaries=$(jq -er --arg run_id "$run_id" '
    [.[]
      | select(.[1][0] == "payload")
      | {stream_id: .[0], status: (.[1][1] | fromjson)}
      | select(.status.run_id == $run_id and .status.lifecycle_state == "FINAL")]
    | if length == 1 then
        [.[0].status.reconciliation.load_finished_cdc_stream_id,
         .[0].status.reconciliation.end_fence_cdc_stream_id,
         .[0].stream_id] | @tsv
      else error("run-scoped FINAL status count differs") end
  ' "$OUTPUT/status-stream.json")
  IFS=$'\t' read -r load_finished_id end_fence_id final_status_id <<<"$boundaries"
  for stream_id in "$load_finished_id" "$end_fence_id" "$final_status_id"; do
    [[ $stream_id =~ ^(0|[1-9][0-9]*)-(0|[1-9][0-9]*)$ && $stream_id != 0-0 ]] ||
      return 1
  done
  jq -s '.[0] + .[1]' \
    <(redis-cli -h host.docker.internal --json XRANGE cdc \
      "$load_finished_id" "$load_finished_id") \
    <(redis-cli -h host.docker.internal --json XRANGE cdc \
      "$end_fence_id" "$end_fence_id") \
    >"$OUTPUT/cdc-boundary-entries.json"
  jq -e --arg load "$load_finished_id" --arg end "$end_fence_id" '
    length == 2 and .[0][0] == $load and .[1][0] == $end
  ' "$OUTPUT/cdc-boundary-entries.json" >/dev/null
  redis-cli -h host.docker.internal --json XINFO STREAM cdc \
    >"$OUTPUT/cdc-stream-info.json"
  redis-cli -h host.docker.internal --json \
    XINFO STREAM "blackbox:client:$run_id" >"$OUTPUT/client-stream-info.json"
  jq -n --arg requested "$EVIDENCE_RETENTION" \
    --arg load_finished_cdc_stream_id "$load_finished_id" \
    --arg end_fence_cdc_stream_id "$end_fence_id" \
    --arg final_status_stream_id "$final_status_id" \
    '{kind:"performance_evidence_retention",requested:$requested,retained:"compact",
      full_streams_retained:false,
      load_finished_cdc_stream_id:$load_finished_cdc_stream_id,
      end_fence_cdc_stream_id:$end_fence_cdc_stream_id,
      final_status_stream_id:$final_status_stream_id}' \
    >"$OUTPUT/evidence-retention.json"
  RETAINED_EVIDENCE=compact
}

capture_failure_evidence() {
  [[ -f $OUTPUT/run-id.txt ]] || return 0
  local run_id
  run_id=$(tr -d '[:space:]' <"$OUTPUT/run-id.txt")
  # Failure diagnostics must not block cleanup or materialize the full streams.
  timeout 5 redis-cli -h host.docker.internal --json XREVRANGE cdc + - COUNT 64 \
    >"$OUTPUT/failure-cdc-tail.json" 2>"$OUTPUT/failure-cdc-tail.err" || true
  timeout 5 redis-cli -h host.docker.internal --json \
    XREVRANGE "blackbox:client:$run_id" + - COUNT 64 \
    >"$OUTPUT/failure-client-tail.json" 2>"$OUTPUT/failure-client-tail.err" || true
  timeout 5 redis-cli -h host.docker.internal --json \
    XREVRANGE "blackbox:status:$run_id" + - COUNT 64 \
    >"$OUTPUT/failure-status-tail.json" 2>"$OUTPUT/failure-status-tail.err" || true
  timeout 5 redis-cli -h host.docker.internal --raw INFO all \
    >"$OUTPUT/redis-info-after.txt" 2>"$OUTPUT/failure-redis-info.err" || true
  jq -n --arg requested "$EVIDENCE_RETENTION" \
    '{kind:"performance_evidence_retention",requested:$requested,retained:"diagnostic",
      full_streams_retained:false,tail_records_per_stream:64,command_timeout_seconds:5}' \
    >"$OUTPUT/evidence-retention.json"
}

capture_success_evidence() {
  if [[ $EVIDENCE_RETENTION == full ]]; then
    capture_full_streams
  else
    capture_compact_streams
  fi
}

compress_full_streams() {
  [[ $RETAINED_EVIDENCE == full ]] || return 0
  local stream
  for stream in cdc-stream.json client-stream.json; do
    if [[ -f $OUTPUT/$stream ]]; then
      gzip --fast "$OUTPUT/$stream"
    fi
  done
}

capture_containers() {
  if [[ $EXECUTION_PROFILE != direct_benchbase ]]; then
    docker logs "$CONNECTOR_CONTAINER" >"$OUTPUT/connector.log" 2>&1 || true
    docker inspect "$CONNECTOR_CONTAINER" "$DATABASE_CONTAINER" cdc-redis \
      >"$OUTPUT/container-inspect.json" 2>/dev/null || true
  else
    docker inspect "$DATABASE_CONTAINER" >"$OUTPUT/container-inspect.json" 2>/dev/null || true
  fi
  docker logs "$DATABASE_CONTAINER" >"$OUTPUT/database.log" 2>&1 || true
}

wait_for_connector_progress() {
  local deadline=$(( $(date +%s) + STACK_TIMEOUT )) progress_status state
  while (( $(date +%s) <= deadline )); do
    progress_status=0
    blackbox_backend_wait_for_connector_progress || progress_status=$?
    if (( progress_status == 0 )); then
      return 0
    fi
    if (( progress_status == 2 )); then
      printf 'backend reported terminal connector-progress failure\n' >&2
      return 1
    fi
    state=$(docker inspect --format '{{.State.Status}}' "$CONNECTOR_CONTAINER" 2>/dev/null || true)
    if [[ $state == exited || $state == dead ]]; then
      printf 'connector entered terminal state: %s\n' "$state" >&2
      return 1
    fi
    sleep 1
  done
  printf 'connector did not reach streaming progress\n' >&2
  return 1
}

cdc_fence_seen() {
  local phase=$1 nonce=$2
  python3 /dev/fd/3 "$BLACKBOX_ACTIVE_RUN_ID" "$phase" "$nonce" \
    3<<'PY' < <(redis-cli -h host.docker.internal --json XREVRANGE cdc + - COUNT 64)
import json
import sys

run_id, phase, nonce = sys.argv[1:]
try:
    entries = json.load(sys.stdin)
except json.JSONDecodeError:
    raise SystemExit(1)
for entry in entries:
    fields = entry[1] if isinstance(entry, list) and len(entry) == 2 else []
    for index in range(1, len(fields), 2):
        try:
            document = json.loads(fields[index])
        except (TypeError, json.JSONDecodeError):
            continue
        after = document.get("after") if isinstance(document, dict) else None
        if not isinstance(after, dict):
            continue
        values = {str(key).casefold(): value for key, value in after.items()}
        if (
            str(values.get("run_id")) == run_id
            and str(values.get("phase")) == phase
            and str(values.get("nonce")) == nonce
        ):
            raise SystemExit(0)
raise SystemExit(1)
PY
}

commit_and_wait_for_cdc_fence() {
  local phase=$1 nonce=$2 deadline
  blackbox_backend_commit_fence "$BLACKBOX_ACTIVE_RUN_ID" "$phase" "$nonce"
  deadline=$(( $(date +%s) + STATUS_TIMEOUT ))
  while (( $(date +%s) <= deadline )); do
    if cdc_fence_seen "$phase" "$nonce"; then
      return 0
    fi
    sleep 0.2
  done
  printf 'CDC did not deliver %s fence before timeout\n' "$phase" >&2
  return 1
}

teardown_stack() {
  BLACKBOX_BACKEND="$BACKEND" bash -lc \
    '. "$1/scripts/blackbox_lifecycle.sh"; blackbox_backend_teardown' _ "$ROOT" \
    >"$OUTPUT/teardown.log" 2>&1 || true
}

write_marker_storage() {
  local values rows bytes
  values=$(blackbox_backend_marker_storage)
  read -r rows bytes <<<"${values//$'\t'/ }"
  jq -n --argjson rows "$rows" --argjson bytes "$bytes" \
    '{row_count:$rows,allocated_bytes:$bytes}' >"$OUTPUT/marker-storage.json"
}

wait_for_database_container() {
  local deadline=$(( $(date +%s) + STACK_TIMEOUT ))
  while (( $(date +%s) <= deadline )); do
    local state health
    state=$(docker inspect --format '{{.State.Status}}' "$DATABASE_CONTAINER" 2>/dev/null || true)
    health=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' \
      "$DATABASE_CONTAINER" 2>/dev/null || true)
    if [[ $state == running && ( -z $health || $health == healthy ) ]]; then
      return 0
    fi
    if [[ $state == exited || $state == dead || $health == unhealthy ]]; then
      printf 'database container entered terminal state: state=%s health=%s\n' \
        "$state" "$health" >&2
      return 1
    fi
    sleep 1
  done
  printf 'database container did not become ready: %s\n' "$DATABASE_CONTAINER" >&2
  return 1
}

if [[ $EXECUTION_PROFILE == coordinated_e2e ]]; then
  STACK_MAY_BE_RUNNING=1
  set +e
  BLACKBOX_BACKEND="$BACKEND" \
  BLACKBOX_BENCHMARK="$BENCHMARK" \
  BLACKBOX_CAPTURE_PROFILE="$CAPTURE_PROFILE" \
  BLACKBOX_DECLARED_ISOLATION=REPEATABLE_READ \
  BLACKBOX_EXPECTED_SESSION_IDS="$EXPECTED_SESSIONS" \
  BLACKBOX_LOADER_COMMAND="$LOADER_COMMAND" \
  BLACKBOX_WORKLOAD_COMMAND="$WORKLOAD_COMMAND" \
  BLACKBOX_LOADER_DECLARED_COUNTS_FILE="$LOADER_DECLARATION" \
  BLACKBOX_LOADER_VALIDATION_PATH="$OUTPUT/loader-validation.json" \
  BLACKBOX_BOOTSTRAP_MANIFEST_PATH="$OUTPUT/bootstrap-manifest.json" \
  BLACKBOX_RUN_ID_PATH="$OUTPUT/run-id.txt" \
  BLACKBOX_MONITOR_METRICS_PATH="$OUTPUT/monitor-metrics.json" \
  BLACKBOX_MONITOR_BIN="$MONITOR_BIN" \
  BLACKBOX_LOADER_BATCH_SIZE="${BENCHBASE_MEASUREMENT_BATCH_SIZE:?}" \
  BLACKBOX_LOADER_SCALE_VALUE="${BLACKBOX_LOADER_SCALE_VALUE:-}" \
  BLACKBOX_STACK_TIMEOUT_SECONDS="$STACK_TIMEOUT" \
  BLACKBOX_LOADER_TIMEOUT_SECONDS="$LOADER_TIMEOUT" \
  BLACKBOX_WORKLOAD_TIMEOUT_SECONDS="$WORKLOAD_TIMEOUT" \
  BLACKBOX_STATUS_TIMEOUT_SECONDS="$STATUS_TIMEOUT" \
  BLACKBOX_QUIESCENCE_TIMEOUT_SECONDS="$DRAIN_LIMIT" \
  BLACKBOX_PROCESSING_MODE=audited \
  BLACKBOX_MEASUREMENT_MODE=audited \
  BLACKBOX_REDIS_MAXMEMORY_BYTES="$REDIS_MAXMEMORY" \
  BLACKBOX_EXPECTED_REDIS_MAXMEMORY_BYTES="$REDIS_MAXMEMORY" \
  BLACKBOX_MONITOR_MAX_BOOTSTRAP_STATE_BYTES="$MONITOR_BOOTSTRAP_STATE_BYTES" \
  BLACKBOX_MONITOR_MAX_PENDING_CLIENT_RECORDS="$MONITOR_PENDING_CLIENT_RECORDS" \
  BLACKBOX_MONITOR_MAX_PENDING_CLIENT_BYTES="$MONITOR_PENDING_CLIENT_BYTES" \
  BLACKBOX_MONITOR_MAX_RETAINED_IDENTITY_BYTES="$MONITOR_RETAINED_IDENTITY_BYTES" \
  BLACKBOX_MONITOR_MAX_CHECKER_STATE_BYTES="$MONITOR_CHECKER_STATE_BYTES" \
    timeout --foreground --signal=TERM --kill-after=60 \
      "$((LOADER_TIMEOUT + WORKLOAD_TIMEOUT + STATUS_TIMEOUT + 600))" \
      "$ROOT/scripts/e2e.sh" >"$OUTPUT/e2e.log" 2>&1
  CELL_STATUS=$?
  set -e
  printf '%s\n' "$CELL_STATUS" >"$OUTPUT/e2e-exit-status.txt"
  if (( CELL_STATUS == 0 )); then
    if ! capture_success_evidence; then
      CELL_STATUS=1
      printf '%s\n' "$CELL_STATUS" >"$OUTPUT/e2e-exit-status.txt"
    fi
  fi
  capture_containers
  (( CELL_STATUS == 0 )) || {
    capture_failure_evidence || true
    compress_full_streams || true
    teardown_stack
    STACK_MAY_BE_RUNNING=0
    write_checksums "$OUTPUT"
    exit "$CELL_STATUS"
  }
else
  export BLACKBOX_ACTIVE_RUN_ID
  BLACKBOX_ACTIVE_RUN_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
  printf '%s\n' "$BLACKBOX_ACTIVE_RUN_ID" >"$OUTPUT/run-id.txt"
  export BLACKBOX_LOADER_DECLARED_COUNTS_FILE="$LOADER_DECLARATION"
  export BLACKBOX_LOADER_BATCH_SIZE="${BENCHBASE_MEASUREMENT_BATCH_SIZE:?}"
  export BLACKBOX_STACK_TIMEOUT_SECONDS="$STACK_TIMEOUT"
  export BLACKBOX_REDIS_MAXMEMORY_BYTES="$REDIS_MAXMEMORY"
  blackbox_acquire_live_stack_lock
  BLACKBOX_ADAPTER_CONTRACT=$(blackbox_adapter_contract)
  BLACKBOX_BACKEND_CONTRACT=$(blackbox_backend_contract)
  export BLACKBOX_ADAPTER_CONTRACT BLACKBOX_BACKEND_CONTRACT
  blackbox_backend_prepare_run
  blackbox_backend_teardown >"$OUTPUT/pre-teardown.log" 2>&1
  if [[ $EXECUTION_PROFILE == coordinated_cdc_only ]]; then
    blackbox_backend_start_database_schema >"$OUTPUT/stack.log" 2>&1
  else
    blackbox_backend_start_uninstrumented_database >"$OUTPUT/stack.log" 2>&1
  fi
  STACK_MAY_BE_RUNNING=1
  wait_for_database_container
  blackbox_backend_verify_session_isolation
  if [[ $EXECUTION_PROFILE == coordinated_cdc_only ]]; then
    blackbox_backend_verify_schema
    blackbox_backend_start_connector >"$OUTPUT/connector-start.log" 2>&1
    wait_for_connector_progress
  elif declare -F blackbox_backend_verify_uninstrumented_schema >/dev/null; then
    blackbox_backend_verify_uninstrumented_schema
  else
    blackbox_backend_verify_schema
  fi
  timeout --foreground --signal=TERM --kill-after=30 "$LOADER_TIMEOUT" \
    bash -lc "$LOADER_COMMAND" >"$OUTPUT/loader.log" 2>&1
  blackbox_validate_loader_state >"$OUTPUT/loader-validation.json"
  blackbox_capture_bootstrap_scan "$OUTPUT/bootstrap-scan.txt"
  blackbox_build_bootstrap_manifest "$OUTPUT/bootstrap-scan.txt" >"$OUTPUT/bootstrap-manifest.json"
  write_marker_storage
  jq -e '.row_count == 0' "$OUTPUT/marker-storage.json" >/dev/null
  if [[ $EXECUTION_PROFILE == coordinated_cdc_only ]]; then
    commit_and_wait_for_cdc_fence LOAD_FINISHED 1
  fi
  timeout --foreground --signal=TERM --kill-after=30 "$WORKLOAD_TIMEOUT" \
    bash -lc "$WORKLOAD_COMMAND" >"$OUTPUT/workload.log" 2>&1
  write_marker_storage
  jq -e '.row_count == 0' "$OUTPUT/marker-storage.json" >/dev/null
  if [[ $EXECUTION_PROFILE == coordinated_cdc_only ]]; then
    commit_and_wait_for_cdc_fence END_FENCE 2
    capture_full_streams
    cdc_entries=$(redis-cli -h host.docker.internal --raw XLEN cdc)
    jq -n --argjson cdc_entries "$cdc_entries" \
      '{kind:"cdc_only_evidence",client_instrumentation:false,monitor_started:false,
        load_fence_seen:true,end_fence_seen:true,cdc_entries:$cdc_entries}' \
      >"$OUTPUT/cdc-only-evidence.json"
  fi
  capture_containers
fi

if [[ ! -f $OUTPUT/evidence-retention.json ]]; then
  RETAINED_EVIDENCE=compact
  jq -n --arg requested "$EVIDENCE_RETENTION" \
    '{kind:"performance_evidence_retention",requested:$requested,
      retained:"compact",full_streams_retained:false}' \
    >"$OUTPUT/evidence-retention.json"
fi

result_args=(
  --artifact-dir "$OUTPUT" --backend "$BACKEND" --benchmark "$BENCHMARK"
  --mode "$MODE" --execution-profile "$EXECUTION_PROFILE" --workload "$WORKLOAD"
  --capture-profile "$CAPTURE_PROFILE" --repetition "$REPETITION"
  --history-gc "$HISTORY_GC"
  --workload-rss-sampling "$WORKLOAD_RSS_SAMPLING"
  --drain-limit-seconds "$DRAIN_LIMIT"
)
if [[ -n $HEADLINE_TRANSACTION_TYPE ]]; then
  result_args+=(--headline-transaction-type "$HEADLINE_TRANSACTION_TYPE"
    --headline-rate-label "$HEADLINE_RATE_LABEL")
fi
run_result_reporter() {
  python3 "$ROOT/scripts/registered_performance_result.py" \
    "${result_args[@]}" --evidence-retention "$RETAINED_EVIDENCE"
}

result_status=0
set +e
run_result_reporter
result_status=$?
if (( result_status == 0 )); then
  jq -e '.accepted == true' "$OUTPUT/result.json" >/dev/null
  result_status=$?
fi
set -e
if (( result_status != 0 )); then
  capture_failure_evidence || true
  compress_full_streams || true
  teardown_stack
  STACK_MAY_BE_RUNNING=0
  write_checksums "$OUTPUT"
  exit "$result_status"
fi
compress_full_streams
sha256sum "$ARCHIVE" "$MONITOR_BIN" >"$OUTPUT/build-artifact-checksums.txt"
teardown_stack
STACK_MAY_BE_RUNNING=0
write_checksums "$OUTPUT"
