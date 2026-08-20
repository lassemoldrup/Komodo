#!/usr/bin/env bash
# Reproducible saturated replay benchmark for the blackbox_run_monitor process.

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORKSPACE_ROOT=$(cd "$ROOT/.." && pwd)
CO_CHECKER_ROOT="$WORKSPACE_ROOT/co-checker"
REPORTER="$ROOT/scripts/blackbox_monitor_performance_report.py"
CHECKSUM_TOOL="$ROOT/scripts/blackbox_performance_report.py"
REPLAY_TOOL="$ROOT/scripts/xrange_to_redis_pipe.py"
MEASURED_COMMAND="$ROOT/scripts/run_measured_command.sh"
ARTIFACT_PREFIX=/media/newhd/yifan/db-monitoring-builds

CANONICAL_STAGES=(assembly_join assembly_join_check)
REQUESTED_STAGES=()
STAGES=()

CORPUS_DIR=
OUTPUT_ROOT=
REPETITIONS=${BLACKBOX_MONITOR_PERFORMANCE_REPETITIONS:-3}
EXPECTED_DURATION_SECONDS=${BLACKBOX_MONITOR_PERFORMANCE_EXPECTED_DURATION_SECONDS:-120}
RANDOM_SEED=${BLACKBOX_MONITOR_PERFORMANCE_RANDOM_SEED:-20260818}
COOLDOWN_SECONDS=${BLACKBOX_MONITOR_PERFORMANCE_COOLDOWN_SECONDS:-2}
RESOURCE_INTERVAL_SECONDS=${BLACKBOX_MONITOR_PERFORMANCE_RESOURCE_INTERVAL_SECONDS:-1}
REDIS_MAXMEMORY_BYTES=${BLACKBOX_MONITOR_PERFORMANCE_REDIS_MAXMEMORY_BYTES:-8589934592}
MONITOR_MAX_PENDING_CLIENT_BYTES=${BLACKBOX_MONITOR_PERFORMANCE_MONITOR_MAX_PENDING_CLIENT_BYTES:-3221225472}
MONITOR_MAX_RETAINED_IDENTITY_BYTES=${BLACKBOX_MONITOR_PERFORMANCE_MONITOR_MAX_RETAINED_IDENTITY_BYTES:-2147483648}
MONITOR_MAX_BOOTSTRAP_STATE_ROWS=${BLACKBOX_MONITOR_PERFORMANCE_MONITOR_MAX_BOOTSTRAP_STATE_ROWS:-10000000}
MONITOR_MAX_BOOTSTRAP_STATE_BYTES=${BLACKBOX_MONITOR_PERFORMANCE_MONITOR_MAX_BOOTSTRAP_STATE_BYTES:-268435456}
OUTPUT_MAX_BYTES=${BLACKBOX_MONITOR_PERFORMANCE_OUTPUT_MAX_BYTES:-107374182400}
COMPOSE_TIMEOUT_SECONDS=${BLACKBOX_MONITOR_PERFORMANCE_COMPOSE_TIMEOUT_SECONDS:-300}
DOCKER_TIMEOUT_SECONDS=${BLACKBOX_MONITOR_PERFORMANCE_DOCKER_TIMEOUT_SECONDS:-30}
REPLAY_TIMEOUT_SECONDS=${BLACKBOX_MONITOR_PERFORMANCE_REPLAY_TIMEOUT_SECONDS:-1800}
MONITOR_TIMEOUT_SECONDS=${BLACKBOX_MONITOR_PERFORMANCE_MONITOR_TIMEOUT_SECONDS:-900}
REDIS_COMMAND_TIMEOUT_SECONDS=${BLACKBOX_MONITOR_PERFORMANCE_REDIS_COMMAND_TIMEOUT_SECONDS:-30}
PRINT_STAGE_ORDER=0

SI_MONITORING_HEAD=
CO_CHECKER_HEAD=
MONITOR_BIN=
RUN_ID=
CDC_CAPTURE=
CLIENT_CAPTURE=
CDC_SHA256=
CLIENT_SHA256=
CDC_ENTRIES=
CLIENT_ENTRIES=
WORKLOAD_TRANSACTIONS=
SOURCE_TRIAL_MANIFEST_SHA256=
LIVE_LOCK_ACQUIRED=0
STACK_MAY_BE_RUNNING=0
ACTIVE_TRIAL_DIR=
OUTPUT_GUARD_PID=
OUTPUT_LIMIT_SENTINEL=


usage() {
  cat >&2 <<'EOF'
usage: run_blackbox_monitor_performance.sh --corpus-dir PATH --output-root PATH [OPTIONS]

Options:
  --stage NAME               Run this stage; repeat to select multiple stages.
                             Default: assembly_join and assembly_join_check.
  --repetitions N            Rotated measured repetitions (default: 3).
  --expected-duration-seconds N
                             Required source-workload duration (default: 120).
  --seed N                   Fixed stage-order seed (default: 20260818).
  --cooldown-seconds N       Delay after each trial (default: 2).
  --redis-maxmemory-bytes N  Fresh replay Redis capacity (default: 8589934592).
  --monitor-pending-client-bytes N
                             Joiner pending-client capacity (default: 3221225472).
  --monitor-retained-identity-bytes N
                             Joiner retained-identity capacity (default: 2147483648).
  --monitor-bootstrap-state-rows N
                             Joiner bootstrap row capacity (default: 10000000).
  --monitor-bootstrap-state-bytes N
                             Joiner bootstrap byte capacity (default: 268435456).
  --output-max-bytes N       Artifact limit, at most 100 GiB (default: 100 GiB).
  --replay-timeout-seconds N Per-stream replay limit (default: 1800).
  --docker-timeout-seconds N Bounded Docker daemon operation limit (default: 30).
  --monitor-timeout-seconds N
                             Monitor deadline (default: 900).
  --print-stage-order        Print the selected rotated order and exit.

The source corpus is preloaded before timing. Its requested workload duration
must be 120 seconds by default, but the saturated monitor drains it as quickly
as possible; the monitor is not artificially delayed to 120 seconds.
EOF
}


log() {
  printf '[blackbox-monitor-performance] %s\n' "$*" >&2
}


die() {
  log "ERROR: $*"
  exit 1
}


require_unsigned_integer() {
  local name=$1
  local value=$2
  [[ $value =~ ^[0-9]+$ ]] || die "$name must be a nonnegative integer: $value"
}


require_positive_integer() {
  local name=$1
  local value=$2
  [[ $value =~ ^[1-9][0-9]*$ ]] || die "$name must be a positive integer: $value"
}


require_capacity_bytes() {
  local name=$1
  local value=$2
  require_positive_integer "$name" "$value"
  python3 -c \
    'import sys; value = int(sys.argv[1]); raise SystemExit(0 if value <= 9223372036854775807 else 1)' \
    "$value" || die "$name exceeds the signed 64-bit byte range: $value"
}


require_capacity_count() {
  local name=$1
  local value=$2
  require_positive_integer "$name" "$value"
  python3 -c \
    'import sys; value = int(sys.argv[1]); raise SystemExit(0 if value <= 9223372036854775807 else 1)' \
    "$value" || die "$name exceeds the signed 64-bit count range: $value"
}


configure_stages() {
  local requested canonical found
  local selected=()
  if (( ${#REQUESTED_STAGES[@]} == 0 )); then
    STAGES=("${CANONICAL_STAGES[@]}")
  else
    for requested in "${REQUESTED_STAGES[@]}"; do
      found=0
      for canonical in "${CANONICAL_STAGES[@]}"; do
        if [[ $requested == "$canonical" ]]; then
          found=1
          break
        fi
      done
      (( found == 1 )) || die "unknown monitor stage: $requested"
      for canonical in "${selected[@]}"; do
        [[ $canonical != "$requested" ]] || die "duplicate monitor stage: $requested"
      done
      selected+=("$requested")
    done
    for canonical in "${CANONICAL_STAGES[@]}"; do
      for requested in "${selected[@]}"; do
        if [[ $canonical == "$requested" ]]; then
          STAGES+=("$canonical")
          break
        fi
      done
    done
  fi
}


parse_arguments() {
  while (( $# > 0 )); do
    case "$1" in
      --corpus-dir)
        CORPUS_DIR=${2:?missing --corpus-dir value}
        shift 2
        ;;
      --output-root)
        OUTPUT_ROOT=${2:?missing --output-root value}
        shift 2
        ;;
      --stage)
        REQUESTED_STAGES+=("${2:?missing --stage value}")
        shift 2
        ;;
      --repetitions)
        REPETITIONS=${2:?missing --repetitions value}
        shift 2
        ;;
      --expected-duration-seconds)
        EXPECTED_DURATION_SECONDS=${2:?missing --expected-duration-seconds value}
        shift 2
        ;;
      --seed)
        RANDOM_SEED=${2:?missing --seed value}
        shift 2
        ;;
      --cooldown-seconds)
        COOLDOWN_SECONDS=${2:?missing --cooldown-seconds value}
        shift 2
        ;;
      --redis-maxmemory-bytes)
        REDIS_MAXMEMORY_BYTES=${2:?missing --redis-maxmemory-bytes value}
        shift 2
        ;;
      --monitor-pending-client-bytes)
        MONITOR_MAX_PENDING_CLIENT_BYTES=${2:?missing --monitor-pending-client-bytes value}
        shift 2
        ;;
      --monitor-retained-identity-bytes)
        MONITOR_MAX_RETAINED_IDENTITY_BYTES=${2:?missing --monitor-retained-identity-bytes value}
        shift 2
        ;;
      --monitor-bootstrap-state-rows)
        MONITOR_MAX_BOOTSTRAP_STATE_ROWS=${2:?missing --monitor-bootstrap-state-rows value}
        shift 2
        ;;
      --monitor-bootstrap-state-bytes)
        MONITOR_MAX_BOOTSTRAP_STATE_BYTES=${2:?missing --monitor-bootstrap-state-bytes value}
        shift 2
        ;;
      --output-max-bytes)
        OUTPUT_MAX_BYTES=${2:?missing --output-max-bytes value}
        shift 2
        ;;
      --replay-timeout-seconds)
        REPLAY_TIMEOUT_SECONDS=${2:?missing --replay-timeout-seconds value}
        shift 2
        ;;
      --docker-timeout-seconds)
        DOCKER_TIMEOUT_SECONDS=${2:?missing --docker-timeout-seconds value}
        shift 2
        ;;
      --monitor-timeout-seconds)
        MONITOR_TIMEOUT_SECONDS=${2:?missing --monitor-timeout-seconds value}
        shift 2
        ;;
      --print-stage-order)
        PRINT_STAGE_ORDER=1
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        usage
        die "unknown argument: $1"
        ;;
    esac
  done
  configure_stages
  require_positive_integer repetitions "$REPETITIONS"
  require_positive_integer expected-duration-seconds "$EXPECTED_DURATION_SECONDS"
  require_unsigned_integer seed "$RANDOM_SEED"
  require_unsigned_integer cooldown-seconds "$COOLDOWN_SECONDS"
  require_capacity_bytes redis-maxmemory-bytes "$REDIS_MAXMEMORY_BYTES"
  require_capacity_bytes monitor-pending-client-bytes "$MONITOR_MAX_PENDING_CLIENT_BYTES"
  require_capacity_bytes monitor-retained-identity-bytes "$MONITOR_MAX_RETAINED_IDENTITY_BYTES"
  require_capacity_count monitor-bootstrap-state-rows "$MONITOR_MAX_BOOTSTRAP_STATE_ROWS"
  require_capacity_bytes monitor-bootstrap-state-bytes "$MONITOR_MAX_BOOTSTRAP_STATE_BYTES"
  require_capacity_bytes output-max-bytes "$OUTPUT_MAX_BYTES"
  (( OUTPUT_MAX_BYTES <= 107374182400 )) ||
    die "output-max-bytes may not exceed 107374182400"
  require_positive_integer compose-timeout-seconds "$COMPOSE_TIMEOUT_SECONDS"
  require_positive_integer docker-timeout-seconds "$DOCKER_TIMEOUT_SECONDS"
  require_positive_integer replay-timeout-seconds "$REPLAY_TIMEOUT_SECONDS"
  require_positive_integer monitor-timeout-seconds "$MONITOR_TIMEOUT_SECONDS"
  require_positive_integer redis-command-timeout-seconds "$REDIS_COMMAND_TIMEOUT_SECONDS"
  [[ $RESOURCE_INTERVAL_SECONDS =~ ^[0-9]+([.][0-9]+)?$ ]] ||
    die "resource interval is invalid: $RESOURCE_INTERVAL_SECONDS"
  awk -v value="$RESOURCE_INTERVAL_SECONDS" 'BEGIN { exit !(value > 0) }' ||
    die "resource interval must be positive: $RESOURCE_INTERVAL_SECONDS"
}


print_stage_order() {
  printf 'repetition,stage_order_position,stage\n'
  local repetition position offset index
  for (( repetition = 1; repetition <= REPETITIONS; repetition++ )); do
    offset=$(( (RANDOM_SEED + repetition - 1) % ${#STAGES[@]} ))
    for (( position = 1; position <= ${#STAGES[@]}; position++ )); do
      index=$(( (offset + position - 1) % ${#STAGES[@]} ))
      printf '%s,%s,%s\n' "$repetition" "$position" "${STAGES[$index]}"
    done
  done
}


verify_canonical_container() {
  [[ ${CONTAINER_WORKSPACE_FOLDER:-} == "$WORKSPACE_ROOT" ]] ||
    die "run inside db-monitoring-dev (workspace marker is absent or different)"
  local expected_id actual_hostname
  expected_id=$(docker_command inspect --format '{{.Id}}' db-monitoring-dev 2>/dev/null) ||
    die "cannot inspect canonical container db-monitoring-dev"
  actual_hostname=$(hostname)
  [[ ${expected_id:0:12} == "$actual_hostname" ]] ||
    die "this process is not running inside db-monitoring-dev"
}


repo_dirty() {
  [[ -n $(git -C "$1" status --porcelain=v1 --untracked-files=all) ]]
}


verify_inputs() {
  [[ -n $CORPUS_DIR ]] || die "--corpus-dir is required"
  [[ -n $OUTPUT_ROOT ]] || die "--output-root is required"
  CORPUS_DIR=$(realpath -e "$CORPUS_DIR") || die "corpus directory does not exist"
  [[ -d $CORPUS_DIR ]] || die "corpus path is not a directory: $CORPUS_DIR"
  OUTPUT_ROOT=$(realpath -m "$OUTPUT_ROOT")
  [[ $OUTPUT_ROOT == "$ARTIFACT_PREFIX/"* ]] ||
    die "output root must be below $ARTIFACT_PREFIX: $OUTPUT_ROOT"
  if [[ -e $OUTPUT_ROOT && ! -d $OUTPUT_ROOT ]]; then
    die "output root exists and is not a directory: $OUTPUT_ROOT"
  fi
  if [[ -d $OUTPUT_ROOT && -n $(find "$OUTPUT_ROOT" -mindepth 1 -maxdepth 1 -print -quit) ]]; then
    die "refusing to reuse nonempty output root: $OUTPUT_ROOT"
  fi
  for path in "$ROOT" "$CO_CHECKER_ROOT"; do
    git -C "$path" rev-parse --is-inside-work-tree >/dev/null 2>&1 ||
      die "required repository is missing: $path"
    ! repo_dirty "$path" || die "measurement repository must be clean: $path"
  done
  SI_MONITORING_HEAD=$(git -C "$ROOT" rev-parse HEAD)
  CO_CHECKER_HEAD=$(git -C "$CO_CHECKER_ROOT" rev-parse HEAD)
  for tool in awk docker du flock jq pgrep python3 redis-cli sha256sum timeout; do
    command -v "$tool" >/dev/null 2>&1 || die "required command is unavailable: $tool"
  done
  [[ -x $REPORTER && -x $CHECKSUM_TOOL && -x $REPLAY_TOOL && -x $MEASURED_COMMAND ]] ||
    die "monitor performance helper is not executable"
}


verify_repositories_unchanged() {
  [[ $(git -C "$ROOT" rev-parse HEAD) == "$SI_MONITORING_HEAD" ]] ||
    die "si-monitoring head changed during measurement"
  [[ $(git -C "$CO_CHECKER_ROOT" rev-parse HEAD) == "$CO_CHECKER_HEAD" ]] ||
    die "co-checker head changed during measurement"
  ! repo_dirty "$ROOT" || die "si-monitoring became dirty during measurement"
  ! repo_dirty "$CO_CHECKER_ROOT" || die "co-checker became dirty during measurement"
}


compose() {
  timeout --foreground --signal=TERM --kill-after=10 "$COMPOSE_TIMEOUT_SECONDS" \
    docker compose --file "$ROOT/docker-compose.yml" --profile mariadb "$@"
}


docker_command() {
  timeout --foreground --signal=TERM --kill-after=10 "$DOCKER_TIMEOUT_SECONDS" \
    docker "$@"
}


redis_command() {
  timeout --foreground --signal=TERM --kill-after=2 "$REDIS_COMMAND_TIMEOUT_SECONDS" \
    redis-cli -h host.docker.internal -p 6379 "$@"
}


cleanup() {
  local status=$?
  stop_output_guard
  if (( LIVE_LOCK_ACQUIRED == 1 && STACK_MAY_BE_RUNNING == 1 )); then
    if [[ ( -z $OUTPUT_LIMIT_SENTINEL || ! -e $OUTPUT_LIMIT_SENTINEL ) &&
          -n $ACTIVE_TRIAL_DIR && -d $ACTIVE_TRIAL_DIR ]]; then
      compose logs --no-color --timestamps redis \
        >"$ACTIVE_TRIAL_DIR/redis-compose-failure.log" 2>&1 || true
    fi
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    STACK_MAY_BE_RUNNING=0
  fi
  return "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM


output_size_bytes() {
  du --summarize --bytes -- "$OUTPUT_ROOT" | awk '{print $1}'
}


assert_output_size_within_limit() {
  local size
  size=$(output_size_bytes)
  [[ $size =~ ^[0-9]+$ ]] || die "cannot determine output size"
  (( size <= OUTPUT_MAX_BYTES )) ||
    die "monitor performance output exceeds $OUTPUT_MAX_BYTES bytes (found $size)"
}


stop_output_guard() {
  if [[ -n $OUTPUT_GUARD_PID ]]; then
    kill "$OUTPUT_GUARD_PID" >/dev/null 2>&1 || true
    wait "$OUTPUT_GUARD_PID" >/dev/null 2>&1 || true
    OUTPUT_GUARD_PID=
  fi
}


terminate_output_descendants() {
  local parent_pid=$1
  local excluded_pid=$2
  local child_pid
  local children=()
  mapfile -t children < <(pgrep --parent "$parent_pid" 2>/dev/null || true)
  for child_pid in "${children[@]}"; do
    [[ $child_pid == "$excluded_pid" ]] && continue
    kill -STOP "$child_pid" >/dev/null 2>&1 || continue
    terminate_output_descendants "$child_pid" "$excluded_pid"
    kill -KILL "$child_pid" >/dev/null 2>&1 || true
  done
}


start_output_guard() {
  local owner_pid=$BASHPID
  OUTPUT_LIMIT_SENTINEL="$OUTPUT_ROOT/.output-size-limit-exceeded"
  (
    while true; do
      local_size=$(du --summarize --bytes -- "$OUTPUT_ROOT" 2>/dev/null | awk '{print $1}')
      if [[ $local_size =~ ^[0-9]+$ ]] && (( local_size > OUTPUT_MAX_BYTES )); then
        printf 'limit_bytes=%s observed_bytes=%s\n' \
          "$OUTPUT_MAX_BYTES" "$local_size" >"$OUTPUT_LIMIT_SENTINEL"
        kill -STOP "$owner_pid" >/dev/null 2>&1 || true
        terminate_output_descendants "$owner_pid" "$BASHPID"
        kill -TERM "$owner_pid" >/dev/null 2>&1 || true
        kill -CONT "$owner_pid" >/dev/null 2>&1 || true
        exit 1
      fi
      sleep 1
    done
  ) &
  OUTPUT_GUARD_PID=$!
}


record_command() {
  local output=$1
  shift
  printf '%q ' "$@" >>"$output"
  printf '\n' >>"$output"
}


write_provenance() {
  local directory="$OUTPUT_ROOT/provenance"
  mkdir -p "$directory"
  {
    date -u --iso-8601=seconds
    uname -a
    lscpu
    cat /proc/meminfo
  } >"$directory/host.txt"
  docker_command version >"$directory/docker-version.txt" 2>&1
  docker_command compose version >"$directory/docker-compose-version.txt" 2>&1
  docker_command info >"$directory/docker-info.txt" 2>&1
  docker_command ps --no-trunc --format json >"$directory/docker-ps-before.jsonl"
  git -C "$ROOT" status --short --branch --untracked-files=all \
    >"$directory/si-monitoring-status.txt"
  git -C "$CO_CHECKER_ROOT" status --short --branch --untracked-files=all \
    >"$directory/co-checker-status.txt"
  cmake --version >"$directory/cmake-version.txt"
  c++ --version >"$directory/cxx-version.txt" 2>&1
  compose config >"$directory/compose-config.yml"
  compose config --images >"$directory/compose-images.txt"
}


build_monitor() {
  local build="$OUTPUT_ROOT/build/monitor-release"
  mkdir -p "$build"
  record_command "$OUTPUT_ROOT/build-commands.txt" cmake -S "$ROOT/monitor" -B "$build" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release "-DCO_CHECKER_ROOT=$CO_CHECKER_ROOT"
  cmake -S "$ROOT/monitor" -B "$build" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release "-DCO_CHECKER_ROOT=$CO_CHECKER_ROOT" \
    >"$OUTPUT_ROOT/build-configure.log" 2>&1
  record_command "$OUTPUT_ROOT/build-commands.txt" cmake --build "$build"
  cmake --build "$build" >"$OUTPUT_ROOT/build.log" 2>&1
  MONITOR_BIN="$build/blackbox_run_monitor"
  [[ -x $MONITOR_BIN ]] || die "Release monitor binary is missing"
  sha256sum "$MONITOR_BIN" >"$OUTPUT_ROOT/monitor-binary.sha256"
  ldd "$MONITOR_BIN" >"$OUTPUT_ROOT/monitor-binary.ldd"
}


validate_corpus() {
  record_command "$OUTPUT_ROOT/corpus-commands.txt" sha256sum --check "$CORPUS_DIR/SHA256SUMS"
  (cd "$CORPUS_DIR" && sha256sum --check SHA256SUMS) \
    >"$OUTPUT_ROOT/corpus-checksums.log"
  python3 - "$CORPUS_DIR" "$EXPECTED_DURATION_SECONDS" \
    "$OUTPUT_ROOT/corpus_manifest.json" <<'PY'
import hashlib
import json
import re
import sys
from pathlib import Path

corpus = Path(sys.argv[1]).resolve()
expected_duration = int(sys.argv[2])
output = Path(sys.argv[3])

def fail(message):
    raise SystemExit(f"invalid monitor replay corpus: {message}")

def load_object(name):
    try:
        value = json.loads((corpus / name).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        fail(f"cannot read {name}: {error}")
    if not isinstance(value, dict):
        fail(f"{name} is not a JSON object")
    return value

def at(value, *path):
    current = value
    for component in path:
        if not isinstance(current, dict) or component not in current:
            fail(f"missing {'.'.join(path)}")
        current = current[component]
    return current

def positive(value, label):
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        fail(f"{label} is not a positive integer")
    return value

manifest_path = corpus / "SHA256SUMS"
try:
    manifest_bytes = manifest_path.read_bytes()
except OSError as error:
    fail(f"cannot read SHA256SUMS: {error}")
entries = {}
for line_number, line in enumerate(manifest_bytes.decode("utf-8").splitlines(), 1):
    digest, separator, name = line.partition("  ")
    if separator != "  " or not re.fullmatch(r"[0-9a-f]{64}", digest) or not name:
        fail(f"SHA256SUMS line {line_number} is malformed")
    relative = Path(name)
    if relative.is_absolute() or ".." in relative.parts or name in entries:
        fail(f"SHA256SUMS path is unsafe or repeated: {name}")
    entries[name] = digest
required = {
    "metadata.json", "run_id.txt", "monitor_metrics.json", "client_metrics.json",
    "redis_metrics.json", "cdc_stream.json", "client_stream.json",
}
missing = sorted(required - entries.keys())
if missing:
    fail(f"SHA256SUMS omits {', '.join(missing)}")

metadata = load_object("metadata.json")
monitor = load_object("monitor_metrics.json")
client = load_object("client_metrics.json")
redis = load_object("redis_metrics.json")
try:
    run_id = (corpus / "run_id.txt").read_text(encoding="utf-8").strip()
except (OSError, UnicodeError) as error:
    fail(f"cannot read run_id.txt: {error}")
if not re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", run_id):
    fail("run ID is not a lowercase UUID")
if metadata.get("stage") != "assembly_join_check":
    fail("source trial is not assembly_join_check")
if metadata.get("requested_duration_seconds") != expected_duration:
    fail("source requested duration differs from the configured expectation")
if metadata.get("run_id") != run_id or monitor.get("run_id") != run_id:
    fail("source run IDs differ")
if monitor.get("kind") != "performance_measurement" or monitor.get("processing_mode") != "audited":
    fail("source monitor measurement is not audited")
if monitor.get("completed") is not True or monitor.get("completion_status") != "completed":
    fail("source monitor did not complete")
if monitor.get("evidence_reconciled") is not True or monitor.get("outcome") != "PASS":
    fail("source monitor did not reconcile to PASS")
if monitor.get("diagnostics") != []:
    fail("source monitor contains diagnostics")

workload = positive(at(monitor, "checker", "workload_transactions"), "workload transaction count")
bootstrap = positive(at(monitor, "checker", "bootstrap_transactions"), "bootstrap transaction count")
anchors = positive(at(monitor, "checker", "phase_anchor_transactions"), "phase-anchor count")
if at(monitor, "checker", "consumed_transactions") != workload + bootstrap + anchors:
    fail("source checker counts do not reconcile")
if at(monitor, "joiner", "joined_workload_transactions") != workload:
    fail("source joiner/checker workload counts differ")
if at(monitor, "reconciliation", "joined_workload_transactions") != workload:
    fail("source terminal workload reconciliation differs")
if at(monitor, "reconciliation", "accepted_client_transactions") != workload:
    fail("source client reconciliation differs")
if at(monitor, "reconciliation", "checker_consumed_transactions") != workload + bootstrap + anchors:
    fail("source terminal checker reconciliation differs")
if at(monitor, "reconciliation", "completed_sessions") != anchors:
    fail("source sessions and phase anchors differ")
if at(monitor, "reconciliation", "expected_sessions") != anchors:
    fail("source expected sessions differ")
if at(monitor, "reconciliation", "unmatched_client_records") != 0 or \
        at(monitor, "reconciliation", "unmatched_workload_cdc_slots") != 0:
    fail("source corpus has unmatched evidence")
if at(monitor, "reconciliation", "ambiguous_attempts") != 0:
    fail("source corpus has ambiguous attempts")

raw_cdc = positive(at(monitor, "assembler", "accepted_raw_records"), "raw CDC count")
if redis.get("input_entry_count") != raw_cdc:
    fail("source Redis and monitor raw CDC counts differ")
client_entries = positive(client.get("entry_count"), "client entry count")
if client.get("kind_counts", {}).get("client_txn") != workload:
    fail("source ClientTxn count differs")
if client.get("kind_counts", {}).get("run_manifest") != 1 or \
        client.get("kind_counts", {}).get("bootstrap_manifest") != 1:
    fail("source client stream does not have exactly one manifest of each kind")
if client.get("kind_counts", {}).get("session_done") != anchors:
    fail("source SessionDone count differs")
if client_entries != workload + anchors + 2:
    fail("source client stream cardinality differs")

streams = {}
for label, filename, count in (
    ("cdc", "cdc_stream.json", raw_cdc),
    ("client", "client_stream.json", client_entries),
):
    path = corpus / filename
    streams[label] = {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": entries[filename],
        "entries": count,
    }

document = {
    "kind": "monitor_performance_corpus",
    "format_version": 1,
    "source_trial_dir": str(corpus),
    "source_duration_seconds": expected_duration,
    "source_trial_manifest_path": str(manifest_path),
    "source_trial_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
    "run_id": run_id,
    "workload_transactions": workload,
    "bootstrap_transactions": bootstrap,
    "phase_anchor_transactions": anchors,
    "cdc_raw_records": raw_cdc,
    "client_entries": client_entries,
    "streams": streams,
    "source_heads": {
        "si_monitoring": metadata.get("si_monitoring_head"),
        "benchbase": metadata.get("benchbase_current_head"),
        "co_checker": metadata.get("co_checker_head"),
    },
}
output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
  SOURCE_TRIAL_MANIFEST_SHA256=$(jq -er '.source_trial_manifest_sha256' "$OUTPUT_ROOT/corpus_manifest.json")
  RUN_ID=$(jq -er '.run_id' "$OUTPUT_ROOT/corpus_manifest.json")
  CDC_CAPTURE=$(jq -er '.streams.cdc.path' "$OUTPUT_ROOT/corpus_manifest.json")
  CLIENT_CAPTURE=$(jq -er '.streams.client.path' "$OUTPUT_ROOT/corpus_manifest.json")
  CDC_SHA256=$(jq -er '.streams.cdc.sha256' "$OUTPUT_ROOT/corpus_manifest.json")
  CLIENT_SHA256=$(jq -er '.streams.client.sha256' "$OUTPUT_ROOT/corpus_manifest.json")
  CDC_ENTRIES=$(jq -er '.streams.cdc.entries' "$OUTPUT_ROOT/corpus_manifest.json")
  CLIENT_ENTRIES=$(jq -er '.streams.client.entries' "$OUTPUT_ROOT/corpus_manifest.json")
  WORKLOAD_TRANSACTIONS=$(jq -er '.workload_transactions' "$OUTPUT_ROOT/corpus_manifest.json")
  [[ $CDC_ENTRIES =~ ^[1-9][0-9]*$ && $CLIENT_ENTRIES =~ ^[1-9][0-9]*$ &&
    $WORKLOAD_TRANSACTIONS =~ ^[1-9][0-9]*$ ]] || die "corpus counts are invalid"
  [[ -f $CDC_CAPTURE && -f $CLIENT_CAPTURE ]] || die "corpus stream capture is missing"
}


verify_corpus_unchanged() {
  printf '%s  %s\n%s  %s\n' \
    "$CDC_SHA256" "$CDC_CAPTURE" "$CLIENT_SHA256" "$CLIENT_CAPTURE" |
    sha256sum --check >"$OUTPUT_ROOT/corpus-streams-after.log"
  [[ $(sha256sum "$CORPUS_DIR/SHA256SUMS" | awk '{print $1}') == "$SOURCE_TRIAL_MANIFEST_SHA256" ]] ||
    die "source trial checksum manifest changed during measurement"
}


start_fresh_redis() {
  # Treat teardown as owned destructive state too: if it times out after a
  # partial removal, EXIT cleanup must retry it.
  STACK_MAY_BE_RUNNING=1
  compose down --volumes --remove-orphans
  STACK_MAY_BE_RUNNING=0
  # Mark ownership before startup so EXIT cleanup removes a container/network
  # even when Compose partially creates the stack and then fails.
  STACK_MAY_BE_RUNNING=1
  BLACKBOX_REDIS_MAXMEMORY_BYTES="$REDIS_MAXMEMORY_BYTES" \
    compose up --detach --force-recreate redis
  local deadline=$(( $(date +%s) + 180 ))
  local state
  while (( $(date +%s) <= deadline )); do
    state=$(docker_command inspect --format \
      '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
      cdc-redis 2>/dev/null || true)
    case "$state" in
      healthy)
        break
        ;;
      unhealthy|exited|dead)
        die "Redis entered terminal state: $state"
        ;;
    esac
    sleep 1
  done
  [[ $state == healthy ]] || die "timed out waiting for Redis"
  [[ $(redis_command --raw DBSIZE) == 0 ]] || die "fresh Redis is not empty"
  local maxmemory policy
  maxmemory=$(redis_command --raw CONFIG GET maxmemory | tail -n 1)
  policy=$(redis_command --raw CONFIG GET maxmemory-policy | tail -n 1)
  [[ $maxmemory == "$REDIS_MAXMEMORY_BYTES" ]] ||
    die "Redis maxmemory differs (expected $REDIS_MAXMEMORY_BYTES, found $maxmemory)"
  [[ $policy == noeviction ]] || die "Redis policy is not noeviction"
}


replay_stream() {
  local label=$1
  local capture=$2
  local stream=$3
  local trial_dir=$4
  record_command "$trial_dir/commands.txt" python3 "$REPLAY_TOOL" \
    --input-json "$capture" --stream "$stream" '|' redis-cli -h host.docker.internal \
    -p 6379 --pipe --pipe-timeout "$REPLAY_TIMEOUT_SECONDS"
  local statuses=()
  set +e
  timeout --foreground --signal=TERM --kill-after=10 "$REPLAY_TIMEOUT_SECONDS" \
    python3 "$REPLAY_TOOL" --input-json "$capture" --stream "$stream" \
    2>"$trial_dir/replay_${label}_converter.log" |
    timeout --foreground --signal=TERM --kill-after=10 "$REPLAY_TIMEOUT_SECONDS" \
      redis-cli -h host.docker.internal -p 6379 --pipe \
      --pipe-timeout "$REPLAY_TIMEOUT_SECONDS" \
      >"$trial_dir/replay_${label}.log" 2>"$trial_dir/replay_${label}_redis.log"
  statuses=("${PIPESTATUS[@]}")
  set -e
  (( statuses[0] == 0 && statuses[1] == 0 )) ||
    die "$label replay failed (converter=${statuses[0]} redis=${statuses[1]})"
}


redis_stream_id() {
  local direction=$1
  local key=$2
  if [[ $direction == first ]]; then
    redis_command --json XRANGE "$key" - + COUNT 1 | jq -er '.[0][0]'
  else
    redis_command --json XREVRANGE "$key" + - COUNT 1 | jq -er '.[0][0]'
  fi
}


write_redis_snapshot() {
  local output=$1
  local client_key="blackbox:client:$RUN_ID"
  local status_key="blackbox:status:$RUN_ID"
  local owner_key="blackbox:monitor-owner:$RUN_ID"
  local keys dbsize cdc_length client_length status_exists owner_exists
  local cdc_first cdc_last client_first client_last cdc_memory client_memory
  keys=$(redis_command --json KEYS '*' | jq -c 'sort')
  dbsize=$(redis_command --raw DBSIZE)
  cdc_length=$(redis_command --raw XLEN cdc)
  client_length=$(redis_command --raw XLEN "$client_key")
  status_exists=$(redis_command --raw EXISTS "$status_key")
  owner_exists=$(redis_command --raw EXISTS "$owner_key")
  cdc_first=$(redis_stream_id first cdc)
  cdc_last=$(redis_stream_id last cdc)
  client_first=$(redis_stream_id first "$client_key")
  client_last=$(redis_stream_id last "$client_key")
  cdc_memory=$(redis_command --raw MEMORY USAGE cdc)
  client_memory=$(redis_command --raw MEMORY USAGE "$client_key")
  python3 - "$output" "$keys" "$dbsize" "$status_exists" "$owner_exists" \
    "$cdc_length" "$cdc_first" "$cdc_last" "$cdc_memory" \
    "$client_length" "$client_first" "$client_last" "$client_memory" <<'PY'
import json
import sys
from pathlib import Path

(
    output, keys, dbsize, status_exists, owner_exists,
    cdc_length, cdc_first, cdc_last, cdc_memory,
    client_length, client_first, client_last, client_memory,
) = sys.argv[1:]
document = {
    "keys": json.loads(keys),
    "dbsize": int(dbsize),
    "status_exists": int(status_exists),
    "owner_exists": int(owner_exists),
    "streams": {
        "cdc": {
            "length": int(cdc_length),
            "first_id": cdc_first,
            "last_id": cdc_last,
            "memory_usage_bytes": int(cdc_memory),
        },
        "client": {
            "length": int(client_length),
            "first_id": client_first,
            "last_id": client_last,
            "memory_usage_bytes": int(client_memory),
        },
    },
}
Path(output).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}


write_trial_metadata() {
  local trial_dir=$1
  local stage=$2
  local repetition=$3
  local position=$4
  local roster_json
  roster_json=$(printf '%s\n' "${STAGES[@]}" | jq -Rsc 'split("\n")[:-1]')
  python3 - "$trial_dir/metadata.json" "$stage" "$repetition" "$position" \
    "$REPETITIONS" "$RANDOM_SEED" "$RUN_ID" "$EXPECTED_DURATION_SECONDS" \
    "$roster_json" "$CORPUS_DIR" "$SOURCE_TRIAL_MANIFEST_SHA256" \
    "$CDC_SHA256" "$CLIENT_SHA256" "$CDC_ENTRIES" "$CLIENT_ENTRIES" \
    "$WORKLOAD_TRANSACTIONS" "$SI_MONITORING_HEAD" "$CO_CHECKER_HEAD" \
    "$REDIS_MAXMEMORY_BYTES" "$MONITOR_MAX_PENDING_CLIENT_BYTES" \
    "$MONITOR_MAX_RETAINED_IDENTITY_BYTES" \
    "$MONITOR_MAX_BOOTSTRAP_STATE_ROWS" "$MONITOR_MAX_BOOTSTRAP_STATE_BYTES" <<'PY'
import json
import sys
from pathlib import Path

values = sys.argv[1:]
document = {
    "kind": "monitor_performance_trial",
    "stage": values[1],
    "repetition": int(values[2]),
    "stage_order_position": int(values[3]),
    "configured_repetitions": int(values[4]),
    "random_seed": int(values[5]),
    "run_id": values[6],
    "source_requested_duration_seconds": int(values[7]),
    "configured_stages": json.loads(values[8]),
    "trial_id": f"repetition-{int(values[2]):02d}-position-{int(values[3]):02d}-{values[1]}",
    "source_trial_dir": values[9],
    "source_trial_manifest_sha256": values[10],
    "source_cdc_sha256": values[11],
    "source_client_sha256": values[12],
    "source_cdc_entries": int(values[13]),
    "source_client_entries": int(values[14]),
    "source_workload_transactions": int(values[15]),
    "si_monitoring_head": values[16],
    "co_checker_head": values[17],
    "si_monitoring_dirty": False,
    "co_checker_dirty": False,
    "redis_maxmemory_bytes": int(values[18]),
    "monitor_max_pending_client_bytes": int(values[19]),
    "monitor_max_retained_identity_bytes": int(values[20]),
    "monitor_max_bootstrap_state_rows": int(values[21]),
    "monitor_max_bootstrap_state_bytes": int(values[22]),
}
Path(values[0]).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}


capture_trial_environment() {
  local trial_dir=$1
  date -u --iso-8601=seconds >"$trial_dir/host_before.txt"
  cat /proc/loadavg >>"$trial_dir/host_before.txt"
  uptime >>"$trial_dir/host_before.txt"
  docker_command ps --no-trunc --format json >"$trial_dir/docker_ps_before.jsonl"
  docker_command inspect --format '{{json .}}' cdc-redis >"$trial_dir/redis_container_inspect.json"
  redis_command --raw INFO server >"$trial_dir/redis_info_server.txt"
  redis_command --raw INFO memory >"$trial_dir/redis_info_memory_pre_monitor.txt"
}


run_monitor() {
  local stage=$1
  local trial_dir=$2
  local processing_mode=audited
  [[ $stage == assembly_join ]] && processing_mode=join-only
  local overall_timeout_ms=$(( MONITOR_TIMEOUT_SECONDS * 1000 ))
  local outer_timeout=$(( MONITOR_TIMEOUT_SECONDS + 30 ))
  local measurement_timeout=$(( outer_timeout + 60 ))
  record_command "$trial_dir/commands.txt" timeout --foreground --signal=TERM --kill-after=10 \
    "$measurement_timeout" "$MEASURED_COMMAND" \
    --resource-output "$trial_dir/resource_samples.csv" \
    --network-output "$trial_dir/network_metrics.json" \
    --sample-duration-seconds "$outer_timeout" \
    --sample-interval-seconds "$RESOURCE_INTERVAL_SECONDS" \
    --process-label monitor --container redis=cdc-redis -- \
    timeout --foreground --signal=TERM --kill-after=10 "$outer_timeout" \
    "$MONITOR_BIN" --redis-host host.docker.internal --redis-port 6379 \
    --run-id "$RUN_ID" --overall-timeout-ms "$overall_timeout_ms" \
    --processing-mode "$processing_mode" \
    --metrics-output "$trial_dir/monitor_metrics.json" \
    --max-pending-client-bytes "$MONITOR_MAX_PENDING_CLIENT_BYTES" \
    --max-retained-identity-bytes "$MONITOR_MAX_RETAINED_IDENTITY_BYTES" \
    --max-bootstrap-state-rows "$MONITOR_MAX_BOOTSTRAP_STATE_ROWS" \
    --max-bootstrap-state-bytes "$MONITOR_MAX_BOOTSTRAP_STATE_BYTES"
  local status=0
  set +e
  timeout --foreground --signal=TERM --kill-after=10 "$measurement_timeout" \
    "$MEASURED_COMMAND" \
    --resource-output "$trial_dir/resource_samples.csv" \
    --network-output "$trial_dir/network_metrics.json" \
    --sample-duration-seconds "$outer_timeout" \
    --sample-interval-seconds "$RESOURCE_INTERVAL_SECONDS" \
    --process-label monitor --container redis=cdc-redis -- \
    timeout --foreground --signal=TERM --kill-after=10 "$outer_timeout" \
    "$MONITOR_BIN" --redis-host host.docker.internal --redis-port 6379 \
    --run-id "$RUN_ID" --overall-timeout-ms "$overall_timeout_ms" \
    --processing-mode "$processing_mode" \
    --metrics-output "$trial_dir/monitor_metrics.json" \
    --max-pending-client-bytes "$MONITOR_MAX_PENDING_CLIENT_BYTES" \
    --max-retained-identity-bytes "$MONITOR_MAX_RETAINED_IDENTITY_BYTES" \
    --max-bootstrap-state-rows "$MONITOR_MAX_BOOTSTRAP_STATE_ROWS" \
    --max-bootstrap-state-bytes "$MONITOR_MAX_BOOTSTRAP_STATE_BYTES" \
    >"$trial_dir/monitor.stdout.log" 2>"$trial_dir/monitor.stderr.log"
  status=$?
  set -e
  (( status == 0 )) || die "$stage monitor exited $status"
  [[ -s $trial_dir/monitor_metrics.json ]] || die "$stage monitor metrics are missing"
}


capture_status_stream() {
  local trial_dir=$1
  redis_command --json XRANGE "blackbox:status:$RUN_ID" - + \
    >"$trial_dir/status_stream.json"
}


validate_trial_artifacts() {
  local trial_dir=$1
  python3 - "$trial_dir" "$OUTPUT_ROOT/corpus_manifest.json" <<'PY'
import json
import re
import sys
from pathlib import Path

trial = Path(sys.argv[1])
corpus_path = Path(sys.argv[2])

def fail(message):
    raise SystemExit(f"invalid saturated monitor trial: {message}")

def load_object(path):
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        fail(f"cannot read {path.name}: {error}")
    if not isinstance(value, dict):
        fail(f"{path.name} is not an object")
    return value

def at(value, *path):
    current = value
    for component in path:
        if not isinstance(current, dict) or component not in current:
            fail(f"missing {'.'.join(path)}")
        current = current[component]
    return current

def integer(value, label, minimum=0):
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        fail(f"{label} is not an integer >= {minimum}")
    return value

metadata = load_object(trial / "metadata.json")
metrics = load_object(trial / "monitor_metrics.json")
before = load_object(trial / "redis_pre_monitor.json")
after = load_object(trial / "redis_post_monitor.json")
corpus = load_object(corpus_path)
stage = metadata.get("stage")
if stage not in {"assembly_join", "assembly_join_check"}:
    fail("stage is unknown")
run_id = corpus.get("run_id")
workload = integer(corpus.get("workload_transactions"), "corpus workload", 1)
bootstrap = integer(corpus.get("bootstrap_transactions"), "corpus bootstrap", 1)
anchors = integer(corpus.get("phase_anchor_transactions"), "corpus anchors", 1)
raw_cdc = integer(corpus.get("cdc_raw_records"), "corpus raw CDC", 1)
client_entries = integer(corpus.get("client_entries"), "corpus client entries", 1)
if metadata.get("run_id") != run_id or metrics.get("run_id") != run_id:
    fail("run IDs differ")
if metadata.get("source_cdc_sha256") != at(corpus, "streams", "cdc", "sha256") or \
        metadata.get("source_client_sha256") != at(corpus, "streams", "client", "sha256"):
    fail("trial corpus hashes differ from corpus manifest")
if metrics.get("kind") != "performance_measurement":
    fail("monitor metric kind differs")
if metrics.get("completed") is not True or metrics.get("completion_status") != "completed":
    fail("monitor did not complete")
if metrics.get("evidence_reconciled") is not True or metrics.get("diagnostics") != []:
    fail("monitor evidence did not reconcile cleanly")
integer(metrics.get("monitor_wall_time_nanoseconds"), "monitor wall time", 1)
integer(at(metrics, "process", "user_cpu_nanoseconds"), "user CPU")
integer(at(metrics, "process", "system_cpu_nanoseconds"), "system CPU")
integer(at(metrics, "process", "max_rss_kib"), "max RSS", 1)
if at(metrics, "process", "user_cpu_nanoseconds") + at(metrics, "process", "system_cpu_nanoseconds") <= 0:
    fail("monitor consumed no CPU")
if at(metrics, "limits", "joiner", "max_pending_client_bytes") != metadata.get("monitor_max_pending_client_bytes") or \
        at(metrics, "limits", "joiner", "max_retained_identity_bytes") != metadata.get("monitor_max_retained_identity_bytes") or \
        at(metrics, "limits", "joiner", "max_bootstrap_state_rows") != metadata.get("monitor_max_bootstrap_state_rows") or \
        at(metrics, "limits", "joiner", "max_bootstrap_state_bytes") != metadata.get("monitor_max_bootstrap_state_bytes"):
    fail("monitor resource limits differ from metadata")
if at(metrics, "assembler", "accepted_raw_records") != raw_cdc:
    fail("monitor did not consume the exact raw CDC corpus")
if at(metrics, "assembler", "completed_bootstrap_transactions") != bootstrap or \
        at(metrics, "assembler", "completed_run_control_transactions") != 3 or \
        at(metrics, "assembler", "completed_workload_transactions") != workload:
    fail("assembled transaction counts differ from corpus")
if at(metrics, "joiner", "accepted_client_transactions") != workload or \
        at(metrics, "joiner", "accepted_workload_cdc_transactions") != workload or \
        at(metrics, "joiner", "joined_workload_transactions") != workload:
    fail("joiner workload counts differ from corpus")
if at(metrics, "joiner", "released_bootstrap_transactions") != bootstrap or \
        at(metrics, "joiner", "released_phase_anchor_transactions") != anchors:
    fail("joiner bootstrap/anchor release counts differ")
for path in (
    ("joiner", "pending_client_records"),
    ("joiner", "pending_cdc_slots"),
    ("joiner", "unmatched_client_records"),
    ("joiner", "unmatched_workload_cdc_slots"),
):
    if at(metrics, *path) != 0:
        fail(f"terminal {'.'.join(path)} is nonzero")
if at(metrics, "joiner", "bootstrap_state_validated") is not True or \
        at(metrics, "joiner", "end_fence_observed") is not True or \
        at(metrics, "joiner", "end_fence_processed") is not True:
    fail("bootstrap or END_FENCE validation is incomplete")
reconciliation = at(metrics, "reconciliation")
if not isinstance(reconciliation, dict) or reconciliation.get("evidence_reconciled") is not True:
    fail("terminal reconciliation failed")
expected_reconciliation = {
    "bootstrap_manifest_validated": True,
    "end_fence_seen": True,
    "expected_sessions": anchors,
    "completed_sessions": anchors,
    "accepted_client_transactions": workload,
    "joined_workload_transactions": workload,
    "bootstrap_cdc_transactions": bootstrap,
    "unmatched_client_records": 0,
    "unmatched_workload_cdc_slots": 0,
    "ambiguous_attempts": 0,
}
for key, expected in expected_reconciliation.items():
    if reconciliation.get(key) != expected:
        fail(f"terminal reconciliation field {key} differs")

client_key = f"blackbox:client:{run_id}"
status_key = f"blackbox:status:{run_id}"
owner_key = f"blackbox:monitor-owner:{run_id}"
if before.get("keys") != sorted(["cdc", client_key]) or before.get("dbsize") != 2:
    fail("pre-monitor Redis key set is not exactly the replay corpus")
if before.get("status_exists") != 0 or before.get("owner_exists") != 0:
    fail("status or owner existed before monitor start")
if after.get("keys") != sorted(["cdc", client_key, status_key, owner_key]) or after.get("dbsize") != 4:
    fail("post-monitor Redis key set is not exact")
if after.get("status_exists") != 1 or after.get("owner_exists") != 1:
    fail("status or owner is absent after monitor completion")
for label, expected_count in (("cdc", raw_cdc), ("client", client_entries)):
    initial = at(before, "streams", label)
    final = at(after, "streams", label)
    if initial.get("length") != expected_count or final.get("length") != expected_count:
        fail(f"{label} Redis stream length differs from corpus")
    for field in ("length", "first_id", "last_id"):
        if initial.get(field) != final.get(field):
            fail(f"monitor changed {label} stream {field}")
    if not re.fullmatch(r"(?:0|[1-9][0-9]*)-(?:0|[1-9][0-9]*)", str(initial.get("first_id", ""))) or \
            not re.fullmatch(r"(?:0|[1-9][0-9]*)-(?:0|[1-9][0-9]*)", str(initial.get("last_id", ""))):
        fail(f"{label} endpoint stream ID is invalid")

for label, count in (("cdc", raw_cdc), ("client", client_entries)):
    try:
        replay_log = (trial / f"replay_{label}.log").read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        fail(f"cannot read {label} replay log: {error}")
    match = re.search(r"errors:\s*([0-9]+),\s*replies:\s*([0-9]+)", replay_log)
    if match is None or int(match.group(1)) != 0 or int(match.group(2)) != count:
        fail(f"{label} Redis pipe acknowledgements differ")

try:
    status_capture = json.loads((trial / "status_stream.json").read_text(encoding="utf-8"))
except (OSError, UnicodeError, json.JSONDecodeError) as error:
    fail(f"cannot read status stream: {error}")
if not isinstance(status_capture, list) or not status_capture:
    fail("status stream is empty")
statuses = []
previous_id = None
for index, entry in enumerate(status_capture):
    if not isinstance(entry, list) or len(entry) != 2 or not isinstance(entry[1], list) or \
            len(entry[1]) != 2 or entry[1][0] != "payload":
        fail(f"status entry {index} violates XRANGE field shape")
    stream_id = entry[0]
    if not isinstance(stream_id, str) or not re.fullmatch(r"(?:0|[1-9][0-9]*)-(?:0|[1-9][0-9]*)", stream_id):
        fail(f"status entry {index} has invalid stream ID")
    parsed_id = tuple(int(part) for part in stream_id.split("-"))
    if previous_id is not None and parsed_id <= previous_id:
        fail("status stream IDs are not increasing")
    previous_id = parsed_id
    try:
        status = json.loads(entry[1][1])
    except (TypeError, json.JSONDecodeError) as error:
        fail(f"status entry {index} payload is malformed: {error}")
    if not isinstance(status, dict) or status.get("kind") != "status" or status.get("run_id") != run_id:
        fail(f"status entry {index} belongs to another run")
    statuses.append(status)
final_positions = [index for index, status in enumerate(statuses) if status.get("lifecycle_state") == "FINAL"]

checker = at(metrics, "checker")
if stage == "assembly_join":
    if metrics.get("processing_mode") != "join-only" or "outcome" in metrics:
        fail("join-only metrics contain audited result state")
    for field in (
        "next_commit_seq", "consumed_transactions", "bootstrap_transactions",
        "phase_anchor_transactions", "workload_transactions", "read_events",
        "write_events", "consume_total_nanoseconds", "consume_max_nanoseconds",
        "mapped_identity_records", "mapped_identity_bytes",
    ):
        if checker.get(field) != 0:
            fail(f"join-only checker metric {field} is nonzero")
    if at(checker, "engine", "retained_state_bytes") != 0 or \
            at(checker, "engine", "peak_retained_state_bytes") != 0:
        fail("join-only checker retained state")
    if reconciliation.get("checker_consumed_transactions") != 0 or \
            reconciliation.get("bootstrap_checker_transactions") != 0 or \
            reconciliation.get("phase_anchor_transactions") != 0:
        fail("join-only reconciliation contains checker work")
    if final_positions:
        fail("join-only monitor emitted FINAL status")
    if any(status.get("outcome") != "RUNNING" or status.get("checker_observation") != "NOT_STARTED" for status in statuses):
        fail("join-only status contains checker/terminal state")
else:
    if metrics.get("processing_mode") != "audited" or metrics.get("outcome") != "PASS":
        fail("checked monitor did not produce audited PASS")
    if checker.get("workload_transactions") != workload or \
            checker.get("bootstrap_transactions") != bootstrap or \
            checker.get("phase_anchor_transactions") != anchors or \
            checker.get("consumed_transactions") != workload + bootstrap + anchors:
        fail("checker transaction counts differ from corpus")
    if reconciliation.get("checker_consumed_transactions") != workload + bootstrap + anchors or \
            reconciliation.get("bootstrap_checker_transactions") != bootstrap or \
            reconciliation.get("phase_anchor_transactions") != anchors:
        fail("checked reconciliation counts differ")
    if final_positions != [len(statuses) - 1]:
        fail("checked monitor did not emit exactly one FINAL status last")
    if statuses[-1].get("outcome") != "PASS" or statuses[-1].get("checker_observation") != "PASS_SO_FAR":
        fail("checked terminal status is not PASS")
    if not any(status.get("lifecycle_state") == "RECONCILED" for status in statuses):
        fail("checked monitor omitted RECONCILED status")

validation = {
    "kind": "monitor_performance_trial_validation",
    "stage": stage,
    "run_id": run_id,
    "source_streams_unchanged": True,
    "redis_replay_acknowledged": True,
    "monitor_completed": True,
    "evidence_reconciled": True,
    "status_records": len(statuses),
    "workload_transactions": workload,
}
(trial / "artifact_validation.json").write_text(
    json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY
}


checksum_trial() {
  local trial_dir=$1
  python3 "$CHECKSUM_TOOL" checksums --root "$trial_dir" --output "$trial_dir/SHA256SUMS"
  (cd "$trial_dir" && sha256sum --check SHA256SUMS >/dev/null)
}


run_trial() {
  local repetition=$1
  local position=$2
  local stage=$3
  local trial_dir
  trial_dir="$OUTPUT_ROOT/trials/repetition-$(printf '%02d' "$repetition")/$(printf '%02d' "$position")-$stage"
  mkdir -p "$trial_dir"
  ACTIVE_TRIAL_DIR=$trial_dir
  write_trial_metadata "$trial_dir" "$stage" "$repetition" "$position"
  log "starting repetition=$repetition position=$position stage=$stage"
  start_fresh_redis
  capture_trial_environment "$trial_dir"
  replay_stream cdc "$CDC_CAPTURE" cdc "$trial_dir"
  replay_stream client "$CLIENT_CAPTURE" "blackbox:client:$RUN_ID" "$trial_dir"
  write_redis_snapshot "$trial_dir/redis_pre_monitor.json"
  run_monitor "$stage" "$trial_dir"
  write_redis_snapshot "$trial_dir/redis_post_monitor.json"
  capture_status_stream "$trial_dir"
  redis_command --raw INFO memory >"$trial_dir/redis_info_memory_post_monitor.txt"
  compose logs --no-color --timestamps redis >"$trial_dir/redis-compose.log" 2>&1
  docker_command stats --no-stream --format json cdc-redis >"$trial_dir/redis_stats_after.jsonl" 2>/dev/null || true
  date -u --iso-8601=seconds >"$trial_dir/host_after.txt"
  cat /proc/loadavg >>"$trial_dir/host_after.txt"
  uptime >>"$trial_dir/host_after.txt"
  validate_trial_artifacts "$trial_dir"
  checksum_trial "$trial_dir"
  assert_output_size_within_limit
  compose down --volumes --remove-orphans
  STACK_MAY_BE_RUNNING=0
  ACTIVE_TRIAL_DIR=
  log "completed repetition=$repetition position=$position stage=$stage"
  if (( COOLDOWN_SECONDS > 0 )); then
    sleep "$COOLDOWN_SECONDS"
  fi
}


finalize_reports() {
  record_command "$OUTPUT_ROOT/report-commands.txt" python3 "$REPORTER" --root "$OUTPUT_ROOT"
  python3 "$REPORTER" --root "$OUTPUT_ROOT"
  docker_command ps --no-trunc --format json >"$OUTPUT_ROOT/provenance/docker-ps-after.jsonl"
  python3 "$CHECKSUM_TOOL" checksums --root "$OUTPUT_ROOT" --output "$OUTPUT_ROOT/SHA256SUMS"
  (cd "$OUTPUT_ROOT" && sha256sum --check SHA256SUMS >/dev/null)
}


main() {
  parse_arguments "$@"
  if (( PRINT_STAGE_ORDER == 1 )); then
    print_stage_order
    return 0
  fi
  verify_canonical_container
  verify_inputs
  mkdir -p "$OUTPUT_ROOT"
  start_output_guard
  write_provenance
  build_monitor
  validate_corpus
  print_stage_order >"$OUTPUT_ROOT/stage_order.csv"
  assert_output_size_within_limit
  # shellcheck disable=SC1091
  . "$ROOT/scripts/blackbox_lifecycle.sh"
  blackbox_acquire_live_stack_lock
  LIVE_LOCK_ACQUIRED=1
  export BLACKBOX_REDIS_MAXMEMORY_BYTES="$REDIS_MAXMEMORY_BYTES"

  local repetition position offset index stage
  for (( repetition = 1; repetition <= REPETITIONS; repetition++ )); do
    offset=$(( (RANDOM_SEED + repetition - 1) % ${#STAGES[@]} ))
    for (( position = 1; position <= ${#STAGES[@]}; position++ )); do
      index=$(( (offset + position - 1) % ${#STAGES[@]} ))
      stage=${STAGES[$index]}
      run_trial "$repetition" "$position" "$stage"
    done
  done
  verify_corpus_unchanged
  verify_repositories_unchanged
  finalize_reports
  assert_output_size_within_limit
  stop_output_guard
  log "artifacts=$OUTPUT_ROOT"
}


if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
  main "$@"
fi
