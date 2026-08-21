#!/usr/bin/env bash
# Replay captured full-mix evidence with required fail-closed mutations.

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source=scripts/blackbox_lifecycle.sh
. "$ROOT/scripts/blackbox_lifecycle.sh"
BLACKBOX_DOCKER=${BLACKBOX_DOCKER:-docker}
CORPUS_DIR=${BLACKBOX_TPCC_MUTATION_CORPUS_DIR:-}
MONITOR_BIN=${BLACKBOX_TPCC_MUTATION_MONITOR_BIN:-}
OUTPUT_DIR=${BLACKBOX_TPCC_MUTATION_OUTPUT_DIR:-}
REDIS_MAXMEMORY_BYTES=${BLACKBOX_TPCC_MUTATION_REDIS_MAXMEMORY_BYTES:-8589934592}
MONITOR_TIMEOUT_SECONDS=${BLACKBOX_TPCC_MUTATION_MONITOR_TIMEOUT_SECONDS:-1200}
REPLAY_TIMEOUT_SECONDS=${BLACKBOX_TPCC_MUTATION_REPLAY_TIMEOUT_SECONDS:-600}
MAX_BOOTSTRAP_ROWS=${BLACKBOX_TPCC_MUTATION_MAX_BOOTSTRAP_ROWS:-1000000}
MAX_BOOTSTRAP_BYTES=${BLACKBOX_TPCC_MUTATION_MAX_BOOTSTRAP_BYTES:-1073741824}
PRINT_CASES=0
MUTATIONS=(drop_client drop_workload_transaction drop_workload_row wrong_typed_client_value table_roster_mismatch)


usage() {
  cat <<'EOF'
Usage: run_tpcc_full_mix_mutations.sh --corpus-dir PATH --monitor-bin PATH --output-dir PATH

Options have matching BLACKBOX_TPCC_MUTATION_* environment variables:
  --redis-maxmemory-bytes N
  --monitor-timeout-seconds N
  --replay-timeout-seconds N
  --max-bootstrap-rows N
  --max-bootstrap-bytes N
  --print-cases
EOF
}


while (( $# > 0 )); do
  case "$1" in
    --corpus-dir) CORPUS_DIR=${2:?missing corpus path}; shift 2 ;;
    --monitor-bin) MONITOR_BIN=${2:?missing monitor path}; shift 2 ;;
    --output-dir) OUTPUT_DIR=${2:?missing output path}; shift 2 ;;
    --redis-maxmemory-bytes) REDIS_MAXMEMORY_BYTES=${2:?missing bytes}; shift 2 ;;
    --monitor-timeout-seconds) MONITOR_TIMEOUT_SECONDS=${2:?missing timeout}; shift 2 ;;
    --replay-timeout-seconds) REPLAY_TIMEOUT_SECONDS=${2:?missing timeout}; shift 2 ;;
    --max-bootstrap-rows) MAX_BOOTSTRAP_ROWS=${2:?missing rows}; shift 2 ;;
    --max-bootstrap-bytes) MAX_BOOTSTRAP_BYTES=${2:?missing bytes}; shift 2 ;;
    --print-cases) PRINT_CASES=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'unknown option: %s\n' "$1" >&2; exit 2 ;;
  esac
done

if (( PRINT_CASES == 1 )); then
  printf '%s\n' "${MUTATIONS[@]}"
  exit 0
fi

for value in "$REDIS_MAXMEMORY_BYTES" "$MONITOR_TIMEOUT_SECONDS" \
    "$REPLAY_TIMEOUT_SECONDS" "$MAX_BOOTSTRAP_ROWS" "$MAX_BOOTSTRAP_BYTES"; do
  [[ $value =~ ^[1-9][0-9]*$ ]] || { printf 'numeric controls must be positive\n' >&2; exit 2; }
done
[[ -d $CORPUS_DIR && -f $CORPUS_DIR/replay-manifest.json ]] || {
  printf 'corpus replay manifest is missing\n' >&2; exit 2;
}
[[ -x $MONITOR_BIN ]] || { printf 'monitor binary is not executable\n' >&2; exit 2; }
[[ ! -e $OUTPUT_DIR ]] || { printf 'output path already exists\n' >&2; exit 2; }
mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR=$(cd "$OUTPUT_DIR" && pwd -P)
BLACKBOX_LIVE_LOCK_TARGET=${BLACKBOX_LIVE_LOCK_TARGET:-$ROOT}
blackbox_acquire_live_stack_lock
COMPOSE=("$BLACKBOX_DOCKER" compose --file "$ROOT/docker-compose.yml" --profile mariadb)
cleanup() {
  local status=$?
  trap - EXIT
  BLACKBOX_REDIS_MAXMEMORY_BYTES="$REDIS_MAXMEMORY_BYTES" \
    "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  printf 'TPC-C mutation artifacts: %s\n' "$OUTPUT_DIR" >&2
  return "$status"
}
trap cleanup EXIT

manifest="$CORPUS_DIR/replay-manifest.json"
run_id=$(jq -er '.run_id' "$manifest")
original_cdc=$(jq -er '.streams.cdc.path' "$manifest")
original_client=$(jq -er '.streams.client.path' "$manifest")
sha256sum --check <(
  jq -r '.streams | to_entries[] | "\(.value.sha256)  \(.value.path)"' "$manifest"
) >/dev/null

wait_redis() {
  local deadline=$((SECONDS + 120))
  while (( SECONDS <= deadline )); do
    status=$("$BLACKBOX_DOCKER" inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' cdc-redis 2>/dev/null || true)
    [[ $status == healthy || $status == running ]] && return 0
    sleep 1
  done
  return 1
}

replay() {
  local input=$1 stream=$2 log=$3
  set +e
  set -o pipefail
  timeout --foreground "$REPLAY_TIMEOUT_SECONDS" \
    python3 "$ROOT/scripts/xrange_to_redis_pipe.py" --input-json "$input" --stream "$stream" |
    redis-cli -h "$BLACKBOX_REDIS_HOST" -p "$BLACKBOX_REDIS_PORT" --pipe >"$log" 2>&1
  local result=$?
  set +o pipefail
  set -e
  (( result == 0 ))
}

for mutation in "${MUTATIONS[@]}"; do
  case_dir="$OUTPUT_DIR/$mutation"
  mkdir -p "$case_dir"
  cdc=$original_cdc
  client=$original_client
  if [[ $mutation == drop_workload_transaction || $mutation == drop_workload_row ]]; then
    cdc="$case_dir/cdc.json"
    python3 "$ROOT/tests/tpcc_full_mix_mutations.py" \
      --mutation "$mutation" --input "$original_cdc" --output "$cdc"
  else
    client="$case_dir/client.json"
    python3 "$ROOT/tests/tpcc_full_mix_mutations.py" \
      --mutation "$mutation" --input "$original_client" --output "$client"
  fi
  BLACKBOX_REDIS_MAXMEMORY_BYTES="$REDIS_MAXMEMORY_BYTES" \
    "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null
  BLACKBOX_REDIS_MAXMEMORY_BYTES="$REDIS_MAXMEMORY_BYTES" \
    "${COMPOSE[@]}" up --detach --force-recreate redis >/dev/null
  wait_redis || { printf 'Redis did not become ready\n' >&2; exit 1; }
  replay "$cdc" cdc "$case_dir/replay-cdc.log"
  replay "$client" "blackbox:client:$run_id" "$case_dir/replay-client.log"
  set +e
  timeout --foreground --kill-after=10 "$MONITOR_TIMEOUT_SECONDS" \
    "$MONITOR_BIN" --redis-host "$BLACKBOX_REDIS_HOST" --redis-port "$BLACKBOX_REDIS_PORT" \
    --run-id "$run_id" --overall-timeout-ms "$((MONITOR_TIMEOUT_SECONDS * 1000))" \
    --metrics-output "$case_dir/monitor-metrics.json" \
    --max-bootstrap-state-rows "$MAX_BOOTSTRAP_ROWS" \
    --max-bootstrap-state-bytes "$MAX_BOOTSTRAP_BYTES" \
    >"$case_dir/monitor.stdout.log" 2>"$case_dir/monitor.stderr.log"
  monitor_status=$?
  set -e
  (( monitor_status == 4 )) || {
    printf '%s monitor exited %d, expected INVALID exit 4\n' "$mutation" "$monitor_status" >&2
    exit 1
  }
  redis-cli -h "$BLACKBOX_REDIS_HOST" -p "$BLACKBOX_REDIS_PORT" --json \
    XRANGE "blackbox:status:$run_id" - + >"$case_dir/status.json"
  python3 "$ROOT/tests/verify_tpcc_full_mix_mutation_result.py" \
    --mutation "$mutation" --run-id "$run_id" \
    --metrics "$case_dir/monitor-metrics.json" --status "$case_dir/status.json" \
    --output "$case_dir/result.json"
done

python3 - "$OUTPUT_DIR" "$run_id" "${MUTATIONS[@]}" <<'PY'
import json
import sys
from pathlib import Path

root, run_id, *mutations = sys.argv[1:]
root = Path(root)
results = [json.loads((root / name / "result.json").read_text()) for name in mutations]
with (root / "mutation-report.json").open("x", encoding="utf-8") as output:
    json.dump({"kind": "tpcc_full_mix_mutations", "run_id": run_id,
               "all_invalid": True, "results": results}, output,
              sort_keys=True, separators=(",", ":"))
    output.write("\n")
PY
