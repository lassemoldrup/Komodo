#!/usr/bin/env bash
# Reproducible six-stage black-box performance measurement coordinator.

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORKSPACE_ROOT=$(cd "$ROOT/.." && pwd)
CURRENT_BENCHBASE_ROOT="$WORKSPACE_ROOT/benchbase"
CO_CHECKER_ROOT="$WORKSPACE_ROOT/co-checker"
ANALYZER="$ROOT/scripts/blackbox_performance.py"
REPORTER="$ROOT/scripts/blackbox_performance_report.py"
MEASURED_COMMAND="$ROOT/scripts/run_measured_command.sh"
E2E="$ROOT/scripts/e2e.sh"
EXPECTED_BASELINE_HEAD=2224eeca0d81565e762e584a9214569a28205e3d
ARTIFACT_PREFIX=/media/newhd/yifan/db-monitoring-builds

STAGES=(
  benchbase_uninstrumented
  read_capture_encoding
  read_capture_encoding_redis
  cdc_capture
  assembly_join
  assembly_join_check
)

BASELINE_ROOT=${BLACKBOX_PERFORMANCE_BASELINE_ROOT:-}
OUTPUT_ROOT=${BLACKBOX_PERFORMANCE_OUTPUT_ROOT:-}
REPETITIONS=${BLACKBOX_PERFORMANCE_REPETITIONS:-3}
TERMINALS=${BLACKBOX_PERFORMANCE_TERMINALS:-4}
SCALE_FACTOR=${BLACKBOX_PERFORMANCE_SCALE_FACTOR:-10}
FIELD_SIZE=${BLACKBOX_PERFORMANCE_FIELD_SIZE:-32}
LOADER_THREADS=${BLACKBOX_PERFORMANCE_LOADER_THREADS:-1}
WARMUP_SECONDS=${BLACKBOX_PERFORMANCE_WARMUP_SECONDS:-5}
DURATION_SECONDS=${BLACKBOX_PERFORMANCE_DURATION_SECONDS:-20}
COOLDOWN_SECONDS=${BLACKBOX_PERFORMANCE_COOLDOWN_SECONDS:-2}
RANDOM_SEED=${BLACKBOX_PERFORMANCE_RANDOM_SEED:-20260818}
RESOURCE_INTERVAL_SECONDS=${BLACKBOX_PERFORMANCE_RESOURCE_INTERVAL_SECONDS:-1}
COMPOSE_TIMEOUT_SECONDS=${BLACKBOX_PERFORMANCE_COMPOSE_TIMEOUT_SECONDS:-300}
LOADER_TIMEOUT_SECONDS=${BLACKBOX_PERFORMANCE_LOADER_TIMEOUT_SECONDS:-600}
TRIAL_TIMEOUT_SECONDS=${BLACKBOX_PERFORMANCE_TRIAL_TIMEOUT_SECONDS:-1800}
JAVA_FLAGS_TEXT=${BLACKBOX_PERFORMANCE_JAVA_FLAGS:--Xms1g -Xmx1g -XX:+AlwaysPreTouch -XX:+UseG1GC}
JAVA_FLAGS=()
SMOKE=0
PRINT_STAGE_ORDER=0
VERIFY_INPUTS_ONLY=0

BASELINE_HEAD=
CURRENT_BENCHBASE_HEAD=
SI_MONITORING_HEAD=
CO_CHECKER_HEAD=
BASELINE_DIRTY=0
CURRENT_BENCHBASE_DIRTY=0
SI_MONITORING_DIRTY=0
CO_CHECKER_DIRTY=0
BASELINE_DIST=
CURRENT_DIST=
MONITOR_BIN=
BASELINE_CONFIG=
CURRENT_CONFIG=
ACTIVE_TRIAL_DIR=
ACTIVE_TRIAL_CAPTURED=0
STACK_MAY_BE_RUNNING=0
LIVE_LOCK_ACQUIRED=0


usage() {
  cat >&2 <<'EOF'
usage: run_blackbox_performance.sh --baseline-root PATH [--output-root PATH] [OPTIONS]

Options:
  --smoke                    Run all six real stages once with the smoke configuration.
  --repetitions N            Measured repetitions (default: 3).
  --terminals N              BenchBase terminals (default: 4).
  --scale-factor N           YCSB scale factor (default: 10).
  --field-size N             Bytes per YCSB field (default: 32).
  --loader-threads N         Fixed BenchBase loader concurrency (must be 1).
  --warmup-seconds N         Warmup duration (default: 5).
  --duration-seconds N       Requested measurement duration (default: 20).
  --cooldown-seconds N       Cooldown after a trial (default: 2).
  --seed N                   Fixed BenchBase/order seed (default: 20260818).
  --print-stage-order        Print the deterministic rotated order and exit.
  --verify-inputs-only       Validate environment/worktrees/output safety and exit (smoke only).
EOF
}


log() {
  printf '[blackbox-performance] %s\n' "$*" >&2
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


parse_arguments() {
  while (( $# > 0 )); do
    case "$1" in
      --baseline-root)
        BASELINE_ROOT=${2:?missing --baseline-root value}
        shift 2
        ;;
      --output-root)
        OUTPUT_ROOT=${2:?missing --output-root value}
        shift 2
        ;;
      --repetitions)
        REPETITIONS=${2:?missing --repetitions value}
        shift 2
        ;;
      --terminals)
        TERMINALS=${2:?missing --terminals value}
        shift 2
        ;;
      --scale-factor)
        SCALE_FACTOR=${2:?missing --scale-factor value}
        shift 2
        ;;
      --field-size)
        FIELD_SIZE=${2:?missing --field-size value}
        shift 2
        ;;
      --loader-threads)
        LOADER_THREADS=${2:?missing --loader-threads value}
        shift 2
        ;;
      --warmup-seconds)
        WARMUP_SECONDS=${2:?missing --warmup-seconds value}
        shift 2
        ;;
      --duration-seconds)
        DURATION_SECONDS=${2:?missing --duration-seconds value}
        shift 2
        ;;
      --cooldown-seconds)
        COOLDOWN_SECONDS=${2:?missing --cooldown-seconds value}
        shift 2
        ;;
      --seed)
        RANDOM_SEED=${2:?missing --seed value}
        shift 2
        ;;
      --smoke)
        SMOKE=1
        shift
        ;;
      --print-stage-order)
        PRINT_STAGE_ORDER=1
        shift
        ;;
      --verify-inputs-only)
        VERIFY_INPUTS_ONLY=1
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

  if (( SMOKE == 1 )); then
    REPETITIONS=1
    TERMINALS=2
    SCALE_FACTOR=1
    FIELD_SIZE=32
    LOADER_THREADS=1
    WARMUP_SECONDS=1
    DURATION_SECONDS=2
    COOLDOWN_SECONDS=0
  fi
  require_positive_integer repetitions "$REPETITIONS"
  require_positive_integer terminals "$TERMINALS"
  require_positive_integer scale-factor "$SCALE_FACTOR"
  require_positive_integer field-size "$FIELD_SIZE"
  require_positive_integer loader-threads "$LOADER_THREADS"
  (( LOADER_THREADS == 1 )) || die "loader-threads must be 1 for deterministic loaded bytes"
  require_unsigned_integer warmup-seconds "$WARMUP_SECONDS"
  require_positive_integer duration-seconds "$DURATION_SECONDS"
  require_unsigned_integer cooldown-seconds "$COOLDOWN_SECONDS"
  require_unsigned_integer seed "$RANDOM_SEED"
  require_positive_integer loader-timeout-seconds "$LOADER_TIMEOUT_SECONDS"
  require_positive_integer trial-timeout-seconds "$TRIAL_TIMEOUT_SECONDS"
  require_positive_integer compose-timeout-seconds "$COMPOSE_TIMEOUT_SECONDS"
  [[ $RESOURCE_INTERVAL_SECONDS =~ ^[0-9]+([.][0-9]+)?$ ]] ||
    die "resource interval must be positive: $RESOURCE_INTERVAL_SECONDS"
  awk -v value="$RESOURCE_INTERVAL_SECONDS" 'BEGIN { exit !(value > 0) }' ||
    die "resource interval must be positive: $RESOURCE_INTERVAL_SECONDS"
  if (( VERIFY_INPUTS_ONLY == 1 && SMOKE == 0 )); then
    die "--verify-inputs-only is available only with --smoke"
  fi
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
  command -v docker >/dev/null 2>&1 || die "docker is unavailable"
  local expected_id actual_hostname
  expected_id=$(docker inspect --format '{{.Id}}' db-monitoring-dev 2>/dev/null) ||
    die "cannot inspect canonical container db-monitoring-dev"
  actual_hostname=$(hostname)
  [[ ${expected_id:0:12} == "$actual_hostname" ]] ||
    die "this process is not running inside db-monitoring-dev"
}


repo_dirty() {
  [[ -n $(git -C "$1" status --porcelain=v1 --untracked-files=all) ]]
}


verify_repository() {
  local label=$1
  local path=$2
  [[ -d $path ]] || die "$label root does not exist: $path"
  git -C "$path" rev-parse --is-inside-work-tree >/dev/null 2>&1 ||
    die "$label root is not a Git worktree: $path"
}


verify_inputs() {
  [[ -n $BASELINE_ROOT ]] || die "--baseline-root is required"
  BASELINE_ROOT=$(realpath -e "$BASELINE_ROOT") || die "baseline root does not exist"
  verify_repository baseline "$BASELINE_ROOT"
  verify_repository current-benchbase "$CURRENT_BENCHBASE_ROOT"
  verify_repository si-monitoring "$ROOT"
  verify_repository co-checker "$CO_CHECKER_ROOT"
  [[ $BASELINE_ROOT != "$(realpath -e "$CURRENT_BENCHBASE_ROOT")" ]] ||
    die "baseline must be a separate worktree, not the current BenchBase worktree"

  BASELINE_HEAD=$(git -C "$BASELINE_ROOT" rev-parse HEAD)
  CURRENT_BENCHBASE_HEAD=$(git -C "$CURRENT_BENCHBASE_ROOT" rev-parse HEAD)
  SI_MONITORING_HEAD=$(git -C "$ROOT" rev-parse HEAD)
  CO_CHECKER_HEAD=$(git -C "$CO_CHECKER_ROOT" rev-parse HEAD)
  [[ $BASELINE_HEAD == "$EXPECTED_BASELINE_HEAD" ]] ||
    die "baseline head must be exactly $EXPECTED_BASELINE_HEAD, found $BASELINE_HEAD"
  if git -C "$BASELINE_ROOT" symbolic-ref -q HEAD >/dev/null; then
    die "baseline worktree must be detached at $EXPECTED_BASELINE_HEAD"
  fi
  if repo_dirty "$BASELINE_ROOT"; then
    BASELINE_DIRTY=1
    die "baseline worktree must be clean"
  fi

  repo_dirty "$CURRENT_BENCHBASE_ROOT" && CURRENT_BENCHBASE_DIRTY=1
  repo_dirty "$ROOT" && SI_MONITORING_DIRTY=1
  repo_dirty "$CO_CHECKER_ROOT" && CO_CHECKER_DIRTY=1
  if (( SMOKE == 0 )); then
    (( CURRENT_BENCHBASE_DIRTY == 0 )) || die "current BenchBase worktree is dirty"
    (( SI_MONITORING_DIRTY == 0 )) || die "si-monitoring worktree is dirty"
    (( CO_CHECKER_DIRTY == 0 )) || die "co-checker worktree is dirty"
  fi

  if [[ -z $OUTPUT_ROOT ]]; then
    OUTPUT_ROOT="$ARTIFACT_PREFIX/blackbox-performance/results/$(date -u +%Y%m%dT%H%M%SZ)"
  fi
  OUTPUT_ROOT=$(realpath -m "$OUTPUT_ROOT")
  [[ $OUTPUT_ROOT == "$ARTIFACT_PREFIX/"* ]] ||
    die "output root must be below $ARTIFACT_PREFIX: $OUTPUT_ROOT"
  if [[ -e $OUTPUT_ROOT && ! -d $OUTPUT_ROOT ]]; then
    die "output root exists and is not a directory: $OUTPUT_ROOT"
  fi
  if [[ -d $OUTPUT_ROOT && -n $(find "$OUTPUT_ROOT" -mindepth 1 -maxdepth 1 -print -quit) ]]; then
    die "refusing to reuse nonempty output root: $OUTPUT_ROOT"
  fi
}


verify_repositories_unchanged() {
  [[ $(git -C "$BASELINE_ROOT" rev-parse HEAD) == "$BASELINE_HEAD" ]] ||
    die "historical BenchBase head changed during measurement"
  [[ $(git -C "$CURRENT_BENCHBASE_ROOT" rev-parse HEAD) == "$CURRENT_BENCHBASE_HEAD" ]] ||
    die "current BenchBase head changed during measurement"
  [[ $(git -C "$ROOT" rev-parse HEAD) == "$SI_MONITORING_HEAD" ]] ||
    die "si-monitoring head changed during measurement"
  [[ $(git -C "$CO_CHECKER_ROOT" rev-parse HEAD) == "$CO_CHECKER_HEAD" ]] ||
    die "co-checker head changed during measurement"
  repo_dirty "$BASELINE_ROOT" && die "historical BenchBase became dirty during measurement"
  if (( SMOKE == 0 )); then
    ! repo_dirty "$CURRENT_BENCHBASE_ROOT" || die "current BenchBase became dirty during measurement"
    ! repo_dirty "$ROOT" || die "si-monitoring became dirty during measurement"
    ! repo_dirty "$CO_CHECKER_ROOT" || die "co-checker became dirty during measurement"
  fi
}


compose() {
  timeout --foreground --signal=TERM --kill-after=10 "$COMPOSE_TIMEOUT_SECONDS" \
    docker compose --file "$ROOT/docker-compose.yml" --profile mariadb "$@"
}


capture_stack_artifacts() {
  local trial_dir=$1
  (( STACK_MAY_BE_RUNNING == 1 )) || return 0
  compose ps --all --format json >"$trial_dir/compose_ps.json" 2>"$trial_dir/compose_ps.error" || true
  compose logs --no-color --timestamps >"$trial_dir/compose.log" 2>&1 || true
  docker ps --no-trunc --format json >"$trial_dir/docker_ps_all_after.jsonl" 2>/dev/null || true
  docker stats --no-stream --format json >"$trial_dir/docker_stats_all_after.jsonl" 2>/dev/null || true
  docker inspect --format '{{json .}}' \
    cdc-mariadb cdc-redis cdc-debezium >"$trial_dir/container_inspect.jsonl" 2>/dev/null || true
  ACTIVE_TRIAL_CAPTURED=1
}


cleanup_stack() {
  (( LIVE_LOCK_ACQUIRED == 1 && STACK_MAY_BE_RUNNING == 1 )) || return 0
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  STACK_MAY_BE_RUNNING=0
}


reset_stack() {
  (( LIVE_LOCK_ACQUIRED == 1 )) || die "live stack lock is not held"
  compose down --volumes --remove-orphans
  STACK_MAY_BE_RUNNING=0
}


cleanup() {
  local status=$?
  if [[ -n $ACTIVE_TRIAL_DIR && -d $ACTIVE_TRIAL_DIR && $ACTIVE_TRIAL_CAPTURED == 0 ]]; then
    capture_stack_artifacts "$ACTIVE_TRIAL_DIR" || true
  fi
  cleanup_stack
  return "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM


record_command() {
  local output=$1
  shift
  printf '%q ' "$@" >>"$output"
  printf '\n' >>"$output"
}


write_system_provenance() {
  local directory=$1
  mkdir -p "$directory"
  {
    date -u --iso-8601=seconds
    uname -a
    printf '\n/etc/os-release\n'
    sed -n '1,200p' /etc/os-release
    printf '\nlscpu\n'
    lscpu
    printf '\n/memory\n'
    cat /proc/meminfo
  } >"$directory/host.txt"
  java -XshowSettings:vm -version >"$directory/java.txt" 2>&1
  mvn --version >"$directory/maven.txt" 2>&1
  docker version >"$directory/docker-version.txt" 2>&1
  docker compose version >"$directory/docker-compose-version.txt" 2>&1
  docker info >"$directory/docker-info.txt" 2>&1
  compose config --images >"$directory/compose-images.txt"
  for spec in \
    "baseline:$BASELINE_ROOT" \
    "current_benchbase:$CURRENT_BENCHBASE_ROOT" \
    "si_monitoring:$ROOT" \
    "co_checker:$CO_CHECKER_ROOT"; do
    local label=${spec%%:*}
    local path=${spec#*:}
    git -C "$path" status --short --branch --untracked-files=all >"$directory/${label}-status.txt"
    git -C "$path" diff --binary HEAD >"$directory/${label}-diff.patch"
    git -C "$path" ls-files --others --exclude-standard -z |
      while IFS= read -r -d '' untracked; do
        sha256sum "$path/$untracked"
      done >"$directory/${label}-untracked-sha256.txt"
  done
}


generate_configs() {
  BASELINE_CONFIG="$OUTPUT_ROOT/config/benchbase_uninstrumented.xml"
  CURRENT_CONFIG="$OUTPUT_ROOT/config/benchbase_instrumented.xml"
  mkdir -p "$OUTPUT_ROOT/config"
  python3 - \
    "$BASELINE_CONFIG" "$CURRENT_CONFIG" "$TERMINALS" "$SCALE_FACTOR" \
    "$FIELD_SIZE" "$LOADER_THREADS" "$WARMUP_SECONDS" "$DURATION_SECONDS" "$RANDOM_SEED" <<'PY'
import sys
from pathlib import Path

baseline, current = (Path(sys.argv[1]), Path(sys.argv[2]))
terminals, scale, field_size, loader_threads, warmup, duration, seed = sys.argv[3:]

def document(instrumented: bool) -> str:
    blackbox = "" if not instrumented else """
    <blackbox>
        <mode>${env:BLACKBOX_MEASUREMENT_MODE}</mode>
        <runId>${env:BLACKBOX_ACTIVE_RUN_ID}</runId>
        <redis>
            <host>${env:BLACKBOX_REDIS_HOST}</host>
            <port>${env:BLACKBOX_REDIS_PORT}</port>
        </redis>
        <declaredIsolation>REPEATABLE_READ</declaredIsolation>
        <benchmark>ycsb</benchmark>
        <sessionIdPrefix>ycsb/worker/</sessionIdPrefix>
    </blackbox>
"""
    return f"""<?xml version=\"1.0\"?>
<parameters>
    <type>MARIADB</type>
    <driver>org.mariadb.jdbc.Driver</driver>
    <url>jdbc:mariadb://host.docker.internal:3306/benchbase?useServerPrepStmts=true</url>
    <username>app</username>
    <password>app_pw</password>
    <isolation>TRANSACTION_REPEATABLE_READ</isolation>
    <batchsize>128</batchsize>
    <retries>3</retries>
    <randomSeed>{seed}</randomSeed>
{blackbox}
    <scalefactor>{scale}</scalefactor>
    <fieldSize>{field_size}</fieldSize>
    <loaderThreads>{loader_threads}</loaderThreads>
    <terminals>{terminals}</terminals>
    <works>
        <work>
            <warmup>{warmup}</warmup>
            <time>{duration}</time>
            <rate>unlimited</rate>
            <weights>50,0,0,50,0,0</weights>
        </work>
    </works>
    <transactiontypes>
        <transactiontype><name>ReadRecord</name></transactiontype>
        <transactiontype><name>InsertRecord</name></transactiontype>
        <transactiontype><name>ScanRecord</name></transactiontype>
        <transactiontype><name>UpdateRecord</name></transactiontype>
        <transactiontype><name>DeleteRecord</name></transactiontype>
        <transactiontype><name>ReadModifyWriteRecord</name></transactiontype>
    </transactiontypes>
</parameters>
"""

baseline.write_text(document(False), encoding="utf-8")
current.write_text(document(True), encoding="utf-8")
PY
}


build_artifacts() {
  local build_root="$OUTPUT_ROOT/build"
  local baseline_build="$build_root/benchbase-baseline"
  local current_build="$build_root/benchbase-current"
  local monitor_build="$build_root/monitor-release"
  mkdir -p "$build_root"
  local build_commands="$OUTPUT_ROOT/build-commands.txt"
  local common_maven=(
    --batch-mode
    --activate-profiles mariadb
    -Dmaven.gitcommitid.skip=true
    -DskipTests
    -Ddescriptors=src/main/assembly/dir.xml
    package
  )

  record_command "$build_commands" mvn "${common_maven[@]}" "-DbuildDirectory=$baseline_build"
  (
    cd "$BASELINE_ROOT"
    mvn "${common_maven[@]}" "-DbuildDirectory=$baseline_build"
  ) >"$OUTPUT_ROOT/build-baseline.log" 2>&1

  record_command "$build_commands" mvn "${common_maven[@]}" "-DbuildDirectory=$current_build"
  (
    cd "$CURRENT_BENCHBASE_ROOT"
    mvn "${common_maven[@]}" "-DbuildDirectory=$current_build"
  ) >"$OUTPUT_ROOT/build-current.log" 2>&1

  record_command "$build_commands" cmake -S "$ROOT/monitor" -B "$monitor_build" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release "-DCO_CHECKER_ROOT=$CO_CHECKER_ROOT"
  cmake -S "$ROOT/monitor" -B "$monitor_build" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release "-DCO_CHECKER_ROOT=$CO_CHECKER_ROOT" \
    >"$OUTPUT_ROOT/build-monitor-configure.log" 2>&1
  record_command "$build_commands" cmake --build "$monitor_build"
  cmake --build "$monitor_build" >"$OUTPUT_ROOT/build-monitor.log" 2>&1

  local baseline_jars current_jars
  mapfile -t baseline_jars < <(find "$baseline_build" -type f -name benchbase.jar | sort)
  mapfile -t current_jars < <(find "$current_build" -type f -name benchbase.jar | sort)
  (( ${#baseline_jars[@]} == 1 )) || die "expected one historical benchbase.jar"
  (( ${#current_jars[@]} == 1 )) || die "expected one current benchbase.jar"
  BASELINE_DIST=$(dirname "${baseline_jars[0]}")
  CURRENT_DIST=$(dirname "${current_jars[0]}")
  MONITOR_BIN="$monitor_build/blackbox_run_monitor"
  [[ -f $BASELINE_DIST/benchbase.jar ]] || die "historical BenchBase distribution is missing"
  [[ -f $CURRENT_DIST/benchbase.jar ]] || die "current BenchBase distribution is missing"
  [[ -x $MONITOR_BIN ]] || die "Release monitor is missing"
}


wait_for_container() {
  local container=$1
  local deadline=$(( $(date +%s) + 180 ))
  while (( $(date +%s) <= deadline )); do
    local state
    state=$(docker inspect --format \
      '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
      "$container" 2>/dev/null || true)
    case "$state" in
      healthy|running) return 0 ;;
      unhealthy|exited|dead) die "$container entered terminal state: $state" ;;
    esac
    sleep 1
  done
  die "timed out waiting for $container"
}


wait_for_cdc() {
  local deadline=$(( $(date +%s) + 180 ))
  while (( $(date +%s) <= deadline )); do
    if docker logs cdc-debezium 2>&1 | grep -q 'Starting streaming'; then
      return 0
    fi
    local state
    state=$(docker inspect --format '{{.State.Status}}' cdc-debezium 2>/dev/null || true)
    [[ $state != exited && $state != dead ]] || die "Debezium exited before streaming"
    sleep 1
  done
  die "timed out waiting for Debezium streaming"
}


wait_for_cdc_fence() {
  local phase=$1
  local trial_dir=$2
  local deadline=$(( $(date +%s) + 180 ))
  while (( $(date +%s) <= deadline )); do
    blackbox_redis --json XREVRANGE cdc + - COUNT 1000 >"$trial_dir/cdc_wait.json"
    if python3 - "$trial_dir/cdc_wait.json" "$phase" "$BLACKBOX_ACTIVE_RUN_ID" <<'PY'
import json
import sys
from pathlib import Path

document = list(reversed(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))))
expected_phase, expected_run = sys.argv[2:]

def contains_fence(value):
    if isinstance(value, dict):
        if value.get("phase") == expected_phase and value.get("run_id") == expected_run:
            return True
        return any(contains_fence(child) for child in value.values())
    if isinstance(value, list):
        return any(contains_fence(child) for child in value)
    if isinstance(value, str) and value.startswith(("{", "[")):
        try:
            return contains_fence(json.loads(value))
        except json.JSONDecodeError:
            return False
    return False

def values(entry):
    fields = entry[1]
    if isinstance(fields, dict):
        raw_values = fields.values()
    else:
        raw_values = fields[1::2]
    for raw in raw_values:
        try:
            value = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            yield value

transaction_id = None
fence_index = None
for index, entry in enumerate(document):
    for envelope in values(entry):
        if contains_fence(envelope):
            transaction = envelope.get("transaction")
            if isinstance(transaction, dict) and isinstance(transaction.get("id"), str):
                transaction_id = transaction["id"]
                fence_index = index
                break
    if fence_index is not None:
        break

if transaction_id is not None and fence_index is not None:
    for entry in document[fence_index + 1:]:
        for envelope in values(entry):
            if envelope.get("status") == "END" and envelope.get("id") == transaction_id:
                raise SystemExit(0)
raise SystemExit(1)
PY
    then
      return 0
    fi
    sleep 0.25
  done
  die "timed out waiting for CDC fence $phase"
}


verify_empty_schema() {
  local application_rows fence_rows marker_rows isolation tables
  tables=$(blackbox_database_query \
    "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='benchbase' ORDER BY TABLE_NAME")
  [[ $tables == $'checker_run_fence\nsession_txn\nusertable' ]] ||
    die "fresh trial schema does not have the exact expected table roster"
  application_rows=$(blackbox_database_query 'SELECT COUNT(*) FROM usertable')
  fence_rows=$(blackbox_database_query 'SELECT COUNT(*) FROM checker_run_fence')
  marker_rows=$(blackbox_database_query 'SELECT COUNT(*) FROM session_txn')
  isolation=$(blackbox_database_query 'SELECT @@innodb_snapshot_isolation')
  application_rows=${application_rows//[[:space:]]/}
  fence_rows=${fence_rows//[[:space:]]/}
  marker_rows=${marker_rows//[[:space:]]/}
  isolation=${isolation//[[:space:]]/}
  [[ $application_rows == 0 && $fence_rows == 0 && $marker_rows == 0 ]] ||
    die "fresh trial schema contains rows"
  [[ $isolation == 1 ]] || die "MariaDB snapshot isolation is disabled"
  if [[ $(blackbox_redis --raw PING 2>/dev/null || true) == PONG ]]; then
    local redis_keys
    redis_keys=$(blackbox_redis --raw DBSIZE)
    redis_keys=${redis_keys//[[:space:]]/}
    [[ $redis_keys == 0 ]] || die "fresh trial Redis contains pre-existing keys"
  fi
}


start_direct_stack() {
  local stage=$1
  record_command "$ACTIVE_TRIAL_DIR/commands.txt" compose down --volumes --remove-orphans
  reset_stack
  STACK_MAY_BE_RUNNING=1
  if [[ $stage == read_capture_encoding_redis || $stage == cdc_capture ]]; then
    record_command "$ACTIVE_TRIAL_DIR/commands.txt" compose up --detach --force-recreate redis mariadb
    compose up --detach --force-recreate redis mariadb
    wait_for_container cdc-redis
  else
    record_command "$ACTIVE_TRIAL_DIR/commands.txt" compose up --detach --force-recreate mariadb
    compose up --detach --force-recreate mariadb
  fi
  wait_for_container cdc-mariadb
  verify_empty_schema
  if [[ $stage == cdc_capture ]]; then
    record_command "$ACTIVE_TRIAL_DIR/commands.txt" compose up --detach --no-deps --force-recreate debezium
    compose up --detach --no-deps --force-recreate debezium
    wait_for_cdc
  fi
}


expected_sessions() {
  local output=
  local separator=
  local worker
  for (( worker = 1; worker <= TERMINALS; worker++ )); do
    output+="${separator}ycsb/worker/$worker"
    separator=,
  done
  printf '%s' "$output"
}


write_trial_metadata() {
  local trial_dir=$1
  local stage=$2
  local repetition=$3
  local position=$4
  local run_id=$5
  local trial_id=$6
  python3 - "$trial_dir/metadata.json" \
    "$stage" "$repetition" "$position" "$trial_id" "$run_id" \
    "$TERMINALS" "$SCALE_FACTOR" "$FIELD_SIZE" "$LOADER_THREADS" "$WARMUP_SECONDS" \
    "$DURATION_SECONDS" "$RANDOM_SEED" "$JAVA_FLAGS_TEXT" \
    "$SI_MONITORING_HEAD" "$CURRENT_BENCHBASE_HEAD" "$BASELINE_HEAD" "$CO_CHECKER_HEAD" \
    "$SI_MONITORING_DIRTY" "$CURRENT_BENCHBASE_DIRTY" "$BASELINE_DIRTY" "$CO_CHECKER_DIRTY" \
    "$SMOKE" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
values = sys.argv[2:]
document = {
    "stage": values[0],
    "repetition": int(values[1]),
    "stage_order_position": int(values[2]),
    "trial_id": values[3],
    "run_id": values[4],
    "terminals": int(values[5]),
    "scale_factor": int(values[6]),
    "field_size": int(values[7]),
    "loader_threads": int(values[8]),
    "warmup_seconds": int(values[9]),
    "requested_duration_seconds": int(values[10]),
    "random_seed": int(values[11]),
    "isolation": "REPEATABLE_READ",
    "rate": "unlimited",
    "redis_host": "host.docker.internal",
    "redis_port": 6379,
    "database_host": "host.docker.internal",
    "database_port": 3306,
    "resource_sample_window": "workload_process_window",
    "monitor_resource_window": "full_monitor_lifecycle",
    "java_flags": values[12],
    "si_monitoring_head": values[13],
    "benchbase_current_head": values[14],
    "benchbase_baseline_head": values[15],
    "co_checker_head": values[16],
    "si_monitoring_dirty": values[17] == "1",
    "benchbase_current_dirty": values[18] == "1",
    "benchbase_baseline_dirty": values[19] == "1",
    "co_checker_dirty": values[20] == "1",
    "smoke": values[21] == "1",
}
path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}


update_trial_run_id() {
  local trial_dir=$1
  local run_id=$2
  python3 - "$trial_dir/metadata.json" "$run_id" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
document = json.loads(path.read_text(encoding="utf-8"))
document["run_id"] = sys.argv[2]
path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}


capture_host_before() {
  local trial_dir=$1
  date -u --iso-8601=seconds >"$trial_dir/host_before.txt"
  cat /proc/loadavg >>"$trial_dir/host_before.txt"
  uptime >>"$trial_dir/host_before.txt"
  docker ps --no-trunc --format json >"$trial_dir/docker_ps_all_before.jsonl" 2>/dev/null || true
  docker stats --no-stream --format json >"$trial_dir/docker_stats_all_before.jsonl" 2>/dev/null || true
}


capture_versions() {
  local trial_dir=$1
  blackbox_database_query 'SELECT VERSION()' >"$trial_dir/mariadb_version.txt" || true
  blackbox_redis --raw INFO server >"$trial_dir/redis_info_server.txt" 2>/dev/null || true
  compose config --images >"$trial_dir/compose_images.txt"
  local image
  while IFS= read -r image; do
    [[ -n $image ]] || continue
    docker image inspect --format '{{json .}}' "$image" || true
  done <"$trial_dir/compose_images.txt" >"$trial_dir/image_inspect.jsonl"
}


run_loader() {
  local trial_dir=$1
  local commands="$trial_dir/commands.txt"
  local command=(
    java "${JAVA_FLAGS[@]}" -jar "$BASELINE_DIST/benchbase.jar"
    -b ycsb -c "$BASELINE_CONFIG"
    --create=false --load=true --execute=false
    --directory "$trial_dir/loader_results"
  )
  mkdir -p "$trial_dir/loader_results"
  record_command "$commands" timeout --foreground --signal=TERM --kill-after=10 \
    "$LOADER_TIMEOUT_SECONDS" "${command[@]}"
  (
    cd "$BASELINE_DIST"
    timeout --foreground --signal=TERM --kill-after=10 \
      "$LOADER_TIMEOUT_SECONDS" "${command[@]}"
  ) >"$trial_dir/loader.log" 2>&1
}


capture_bootstrap() {
  local trial_dir=$1
  blackbox_capture_bootstrap_scan "$trial_dir/bootstrap_scan.tsv"
  blackbox_build_bootstrap_manifest "$trial_dir/bootstrap_scan.tsv" \
    >"$trial_dir/bootstrap_manifest.json"
  validate_bootstrap "$trial_dir/bootstrap_manifest.json"
}


validate_bootstrap() {
  local manifest=$1
  local expected_rows=$(( SCALE_FACTOR * 1000 ))
  jq -e --argjson expected "$expected_rows" \
    '.kind == "bootstrap_manifest" and
     (.table_row_counts | length) == 1 and
     .table_row_counts[0].table == "benchbase.usertable" and
     .table_row_counts[0].row_count == $expected and
     (.canonical_state_digest | test("^[0-9a-f]{64}$"))' \
    "$manifest" >/dev/null ||
    die "bootstrap manifest does not contain the expected $expected_rows YCSB rows"
}


workload_container_arguments() {
  local include_redis=$1
  local include_cdc=$2
  WORKLOAD_CONTAINERS=(--container mariadb=cdc-mariadb)
  if (( include_redis == 1 )); then
    WORKLOAD_CONTAINERS+=(--container redis=cdc-redis)
  fi
  if (( include_cdc == 1 )); then
    WORKLOAD_CONTAINERS+=(--container debezium=cdc-debezium)
  fi
}


run_direct_workload() {
  local stage=$1
  local trial_dir=$2
  local run_id=$3
  local distribution config mode include_redis include_cdc
  distribution=$BASELINE_DIST
  config=$BASELINE_CONFIG
  mode=disabled
  include_redis=0
  include_cdc=0
  case "$stage" in
    benchbase_uninstrumented) ;;
    read_capture_encoding)
      distribution=$CURRENT_DIST
      config=$CURRENT_CONFIG
      mode=capture_encoding
      ;;
    read_capture_encoding_redis)
      distribution=$CURRENT_DIST
      config=$CURRENT_CONFIG
      mode=capture_encoding_redis
      include_redis=1
      ;;
    cdc_capture)
      include_redis=1
      include_cdc=1
      ;;
    *) die "not a direct stage: $stage" ;;
  esac
  workload_container_arguments "$include_redis" "$include_cdc"
  local sample_duration=$(( WARMUP_SECONDS + DURATION_SECONDS + 120 ))
  local command=(
    "$MEASURED_COMMAND"
    --resource-output "$trial_dir/resource_samples.csv"
    --network-output "$trial_dir/network_metrics.json"
    --sample-duration-seconds "$sample_duration"
    --sample-interval-seconds "$RESOURCE_INTERVAL_SECONDS"
    "${WORKLOAD_CONTAINERS[@]}"
    -- java "${JAVA_FLAGS[@]}" -jar "$distribution/benchbase.jar"
    -b ycsb -c "$config"
    --create=false --load=false --execute=true
    --directory "$trial_dir/benchbase_results"
  )
  mkdir -p "$trial_dir/benchbase_results"
  record_command "$trial_dir/commands.txt" env \
    "BLACKBOX_MEASUREMENT_MODE=$mode" "BLACKBOX_ACTIVE_RUN_ID=$run_id" \
    BLACKBOX_REDIS_HOST=host.docker.internal BLACKBOX_REDIS_PORT=6379 \
    timeout --foreground --signal=TERM --kill-after=10 "$TRIAL_TIMEOUT_SECONDS" \
    "${command[@]}"
  (
    cd "$distribution"
    BLACKBOX_MEASUREMENT_MODE=$mode \
    BLACKBOX_ACTIVE_RUN_ID=$run_id \
    BLACKBOX_REDIS_HOST=host.docker.internal \
    BLACKBOX_REDIS_PORT=6379 \
      timeout --foreground --signal=TERM --kill-after=10 "$TRIAL_TIMEOUT_SECONDS" \
        "${command[@]}"
  ) >"$trial_dir/workload.log" 2>&1
}


assert_no_transaction_markers() {
  local trial_dir=$1
  record_command "$trial_dir/commands.txt" mariadb \
    --skip-ssl --host=host.docker.internal --port=3306 --user=app \
    --password=app_pw --database=benchbase --batch --raw --skip-column-names \
    --execute 'SELECT COUNT(*) FROM session_txn'
  local marker_rows
  marker_rows=$(blackbox_database_query 'SELECT COUNT(*) FROM session_txn')
  marker_rows=${marker_rows//[[:space:]]/}
  [[ $marker_rows == 0 ]] ||
    die "partial measurement stage wrote $marker_rows forbidden transaction marker row(s)"
}


shell_command() {
  local output=
  printf -v output '%q ' "$@"
  printf '%s' "$output"
}


run_full_stage() {
  local stage=$1
  local trial_dir=$2
  local processing_mode=audited
  [[ $stage == assembly_join ]] && processing_mode=join-only
  workload_container_arguments 1 1
  local sample_duration=$(( WARMUP_SECONDS + DURATION_SECONDS + 120 ))
  local loader_args=(
    java "${JAVA_FLAGS[@]}" -jar "$BASELINE_DIST/benchbase.jar"
    -b ycsb -c "$BASELINE_CONFIG"
    --create=false --load=true --execute=false
    --directory "$trial_dir/loader_results"
  )
  local workload_args=(
    "$MEASURED_COMMAND"
    --resource-output "$trial_dir/resource_samples.csv"
    --network-output "$trial_dir/network_metrics.json"
    --sample-duration-seconds "$sample_duration"
    --sample-interval-seconds "$RESOURCE_INTERVAL_SECONDS"
    "${WORKLOAD_CONTAINERS[@]}"
    -- java "${JAVA_FLAGS[@]}" -jar "$CURRENT_DIST/benchbase.jar"
    -b ycsb -c "$CURRENT_CONFIG"
    --create=false --load=false --execute=true
    --directory "$trial_dir/benchbase_results"
  )
  mkdir -p "$trial_dir/loader_results" "$trial_dir/benchbase_results"
  local loader_command workload_command
  loader_command="cd $(printf '%q' "$BASELINE_DIST") && exec $(shell_command "${loader_args[@]}")"
  workload_command="cd $(printf '%q' "$CURRENT_DIST") && exec $(shell_command "${workload_args[@]}")"
  printf '%s\n' "$loader_command" >"$trial_dir/loader-command.txt"
  printf '%s\n' "$workload_command" >"$trial_dir/workload-command.txt"
  local session_roster
  session_roster=$(expected_sessions)
  record_command "$trial_dir/commands.txt" env \
    "BLACKBOX_PROCESSING_MODE=$processing_mode" \
    "BLACKBOX_MONITOR_METRICS_PATH=$trial_dir/monitor_metrics.json" \
    "BLACKBOX_RUN_ID_PATH=$trial_dir/run_id.txt" \
    "BLACKBOX_BOOTSTRAP_MANIFEST_PATH=$trial_dir/bootstrap_manifest.json" \
    "BLACKBOX_MONITOR_BIN=$MONITOR_BIN" \
    "BLACKBOX_LOADER_COMMAND=$loader_command" \
    "BLACKBOX_WORKLOAD_COMMAND=$workload_command" \
    "BLACKBOX_EXPECTED_SESSION_IDS=$session_roster" \
    BLACKBOX_DECLARED_ISOLATION=REPEATABLE_READ \
    BLACKBOX_MEASUREMENT_MODE=audited \
    BLACKBOX_REDIS_HOST=host.docker.internal BLACKBOX_REDIS_PORT=6379 \
    "BLACKBOX_LOADER_TIMEOUT_SECONDS=$LOADER_TIMEOUT_SECONDS" \
    "BLACKBOX_WORKLOAD_TIMEOUT_SECONDS=$TRIAL_TIMEOUT_SECONDS" \
    timeout --foreground --signal=TERM --kill-after=10 "$TRIAL_TIMEOUT_SECONDS" "$E2E"

  local status=0
  set +e
  BLACKBOX_PROCESSING_MODE=$processing_mode \
  BLACKBOX_MONITOR_METRICS_PATH="$trial_dir/monitor_metrics.json" \
  BLACKBOX_RUN_ID_PATH="$trial_dir/run_id.txt" \
  BLACKBOX_BOOTSTRAP_MANIFEST_PATH="$trial_dir/bootstrap_manifest.json" \
  BLACKBOX_MONITOR_BIN="$MONITOR_BIN" \
  BLACKBOX_LOADER_COMMAND="$loader_command" \
  BLACKBOX_WORKLOAD_COMMAND="$workload_command" \
  BLACKBOX_EXPECTED_SESSION_IDS="$session_roster" \
  BLACKBOX_DECLARED_ISOLATION=REPEATABLE_READ \
  BLACKBOX_MEASUREMENT_MODE=audited \
  BLACKBOX_REDIS_HOST=host.docker.internal \
  BLACKBOX_REDIS_PORT=6379 \
  BLACKBOX_LOADER_TIMEOUT_SECONDS="$LOADER_TIMEOUT_SECONDS" \
  BLACKBOX_WORKLOAD_TIMEOUT_SECONDS="$TRIAL_TIMEOUT_SECONDS" \
    timeout --foreground --signal=TERM --kill-after=10 "$TRIAL_TIMEOUT_SECONDS" "$E2E" \
      >"$trial_dir/workload.log" 2>&1
  status=$?
  set -e
  if [[ $stage == assembly_join ]]; then
    (( status == 0 )) || die "join-only stage exited $status"
  else
    (( status == 0 || status == 3 )) || die "audited stage exited $status"
  fi
  [[ -s $trial_dir/run_id.txt ]] || die "e2e did not record its run ID"
  [[ -s $trial_dir/bootstrap_manifest.json ]] || die "e2e did not record its bootstrap manifest"
  validate_bootstrap "$trial_dir/bootstrap_manifest.json"
  local run_id
  run_id=$(tr -d '[:space:]' <"$trial_dir/run_id.txt")
  update_trial_run_id "$trial_dir" "$run_id"
}


capture_redis_streams() {
  local stage=$1
  local trial_dir=$2
  local run_id=$3
  case "$stage" in
    read_capture_encoding_redis)
      record_command "$trial_dir/commands.txt" redis-cli -h host.docker.internal -p 6379 --json XRANGE \
        "blackbox:measurement:client:$run_id" - +
      blackbox_redis --json XRANGE "blackbox:measurement:client:$run_id" - + \
        >"$trial_dir/measurement_stream.json"
      ;;
    cdc_capture)
      record_command "$trial_dir/commands.txt" redis-cli -h host.docker.internal -p 6379 --json XRANGE cdc - +
      blackbox_redis --json XRANGE cdc - + >"$trial_dir/cdc_stream.json"
      ;;
    assembly_join|assembly_join_check)
      record_command "$trial_dir/commands.txt" redis-cli -h host.docker.internal -p 6379 --json XRANGE cdc - +
      record_command "$trial_dir/commands.txt" redis-cli -h host.docker.internal -p 6379 --json XRANGE \
        "blackbox:client:$run_id" - +
      record_command "$trial_dir/commands.txt" redis-cli -h host.docker.internal -p 6379 --json XRANGE \
        "blackbox:status:$run_id" - +
      blackbox_redis --json XRANGE cdc - + >"$trial_dir/cdc_stream.json"
      blackbox_redis --json XRANGE "blackbox:client:$run_id" - + \
        >"$trial_dir/client_stream.json"
      blackbox_redis --json XRANGE "blackbox:status:$run_id" - + \
        >"$trial_dir/status_stream.json"
      ;;
  esac
  blackbox_redis --raw INFO all >"$trial_dir/redis_info_after.txt" 2>/dev/null || true
}


capture_redis_usage() {
  local trial_dir=$1
  local run_id=$2
  local values=()
  local label key value
  local available=false
  if [[ $(blackbox_redis --raw PING 2>/dev/null || true) == PONG ]]; then
    available=true
  fi
  for label in cdc measurement_client client status monitor_owner; do
    case "$label" in
      cdc) key=cdc ;;
      measurement_client) key="blackbox:measurement:client:$run_id" ;;
      client) key="blackbox:client:$run_id" ;;
      status) key="blackbox:status:$run_id" ;;
      monitor_owner) key="blackbox:monitor-owner:$run_id" ;;
    esac
    value=
    if [[ $available == true ]]; then
      value=$(blackbox_redis --raw MEMORY USAGE "$key" 2>/dev/null || true)
      [[ $value =~ ^[0-9]+$ ]] || value=0
    fi
    values+=("$label" "$value")
  done
  python3 - "$trial_dir/redis_usage.json" "$available" "${values[@]}" <<'PY'
import json
import sys
from pathlib import Path

available = sys.argv[2] == "true"
keys = {}
for index in range(3, len(sys.argv), 2):
    keys[sys.argv[index]] = int(sys.argv[index + 1]) if sys.argv[index + 1] else None
document = {
    "available": available,
    "keys": keys,
    "total_bytes": sum(value for value in keys.values() if value is not None) if available else None,
}
Path(sys.argv[1]).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}


refresh_network_after() {
  local trial_dir=$1
  local path="$trial_dir/network_metrics.json"
  [[ -f $path ]] || return 0
  [[ $(blackbox_redis --raw PING 2>/dev/null || true) == PONG ]] || return 0
  local input_after output_after
  input_after=$(blackbox_redis --raw INFO stats |
    awk -F: '$1 == "total_net_input_bytes" { gsub(/\r/, "", $2); print $2 }')
  output_after=$(blackbox_redis --raw INFO stats |
    awk -F: '$1 == "total_net_output_bytes" { gsub(/\r/, "", $2); print $2 }')
  python3 - "$path" "$input_after" "$output_after" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
document = json.loads(path.read_text(encoding="utf-8"))
document["available_after"] = True
document["total_net_input_bytes_after"] = int(sys.argv[2])
document["total_net_output_bytes_after"] = int(sys.argv[3])
document["available"] = bool(document.get("available_before"))
document["window_definition"] = "workload_process_start_through_pipeline_drain"
if document["available"]:
    document["input_bytes_delta"] = (
        document["total_net_input_bytes_after"] - document["total_net_input_bytes_before"]
    )
    document["output_bytes_delta"] = (
        document["total_net_output_bytes_after"] - document["total_net_output_bytes_before"]
    )
path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}


analyze_trial() {
  local stage=$1
  local trial_dir=$2
  local run_id=$3
  local raw_files summary_files
  mapfile -t raw_files < <(find "$trial_dir/benchbase_results" -maxdepth 1 -name '*.raw.csv' -type f | sort)
  mapfile -t summary_files < <(find "$trial_dir/benchbase_results" -maxdepth 1 -name '*.summary.json' -type f | sort)
  (( ${#raw_files[@]} == 1 )) || die "expected one BenchBase raw CSV in $trial_dir"
  (( ${#summary_files[@]} == 1 )) || die "expected one BenchBase summary JSON in $trial_dir"
  record_command "$trial_dir/commands.txt" python3 "$ANALYZER" analyze-benchbase \
    --raw-csv "${raw_files[0]}" --summary-json "${summary_files[0]}" \
    --output-json "$trial_dir/benchbase_metrics.json" \
    --output-csv "$trial_dir/benchbase_metrics.csv"
  python3 "$ANALYZER" analyze-benchbase \
    --raw-csv "${raw_files[0]}" --summary-json "${summary_files[0]}" \
    --output-json "$trial_dir/benchbase_metrics.json" \
    --output-csv "$trial_dir/benchbase_metrics.csv"
  local measured_start measured_end redis_end window_definition
  measured_start=$(jq -er '.measured_start_epoch_ms' "$trial_dir/benchbase_metrics.json")
  measured_end=$(jq -er '.measured_end_epoch_ms' "$trial_dir/benchbase_metrics.json")
  redis_end=$measured_end
  window_definition=benchbase_measured_request_window

  local redis_input=
  case "$stage" in
    read_capture_encoding_redis) redis_input="$trial_dir/measurement_stream.json" ;;
    cdc_capture|assembly_join|assembly_join_check)
      redis_input="$trial_dir/cdc_stream.json"
      redis_end=$(python3 "$REPORTER" fence-stream-time \
        --input "$redis_input" --run-id "$run_id" --phase END_FENCE --transaction-end)
      window_definition=benchbase_measured_start_through_end_fence_transaction_end_cdc_arrival
      ;;
  esac
  python3 - "$trial_dir/analysis_window.json" "$measured_start" "$redis_end" \
    "$window_definition" <<'PY'
import json
import sys
from pathlib import Path

Path(sys.argv[1]).write_text(
    json.dumps(
        {
            "start_epoch_ms": int(sys.argv[2]),
            "end_epoch_ms": int(sys.argv[3]),
            "definition": sys.argv[4],
        },
        indent=2,
        sort_keys=True,
    ) + "\n",
    encoding="utf-8",
)
PY
  if [[ -n $redis_input ]]; then
    record_command "$trial_dir/commands.txt" python3 "$ANALYZER" analyze-redis \
      --input-json "$redis_input" \
      --measured-start-epoch-ms "$measured_start" \
      --measured-end-epoch-ms "$redis_end" \
      --output-json "$trial_dir/redis_metrics.json" \
      --output-csv "$trial_dir/redis_metrics.csv"
    python3 "$ANALYZER" analyze-redis \
      --input-json "$redis_input" \
      --measured-start-epoch-ms "$measured_start" \
      --measured-end-epoch-ms "$redis_end" \
      --output-json "$trial_dir/redis_metrics.json" \
      --output-csv "$trial_dir/redis_metrics.csv"
  fi
  if [[ -f $trial_dir/client_stream.json ]]; then
    record_command "$trial_dir/commands.txt" python3 "$ANALYZER" analyze-client \
      --input-json "$trial_dir/client_stream.json" \
      --output-json "$trial_dir/client_metrics.json" \
      --output-csv "$trial_dir/client_metrics.csv"
    python3 "$ANALYZER" analyze-client \
      --input-json "$trial_dir/client_stream.json" \
      --output-json "$trial_dir/client_metrics.json" \
      --output-csv "$trial_dir/client_metrics.csv"
  elif [[ $stage == read_capture_encoding_redis ]]; then
    record_command "$trial_dir/commands.txt" python3 "$ANALYZER" analyze-client \
      --input-json "$trial_dir/measurement_stream.json" \
      --output-json "$trial_dir/client_metrics.json" \
      --output-csv "$trial_dir/client_metrics.csv"
    python3 "$ANALYZER" analyze-client \
      --input-json "$trial_dir/measurement_stream.json" \
      --output-json "$trial_dir/client_metrics.json" \
      --output-csv "$trial_dir/client_metrics.csv"
  fi
  if [[ $stage == read_capture_encoding || $stage == read_capture_encoding_redis ]]; then
    grep -q 'BLACKBOX_MEASUREMENT ' "$trial_dir/workload.log" ||
      die "$stage did not emit required capture summaries"
    local capture_args=(
      capture-summaries
      --log "$trial_dir/workload.log"
      --output "$trial_dir/capture_metrics.json"
      --run-id "$run_id"
    )
    local worker
    for (( worker = 1; worker <= TERMINALS; worker++ )); do
      capture_args+=(--expected-session "ycsb/worker/$worker")
    done
    record_command "$trial_dir/commands.txt" python3 "$REPORTER" "${capture_args[@]}"
    python3 "$REPORTER" "${capture_args[@]}"
    if [[ $stage == read_capture_encoding_redis ]]; then
      local capture_records stream_records
      capture_records=$(jq -er '.record_count' "$trial_dir/capture_metrics.json")
      stream_records=$(jq -er '.entry_count' "$trial_dir/client_metrics.json")
      [[ $capture_records == "$stream_records" ]] ||
        die "measurement Redis stream count differs from acknowledged capture records"
    fi
  fi
  capture_redis_usage "$trial_dir" "$run_id"
}


checksum_trial() {
  local trial_dir=$1
  python3 "$REPORTER" checksums --root "$trial_dir" --output "$trial_dir/SHA256SUMS"
  (cd "$trial_dir" && sha256sum --check SHA256SUMS >/dev/null)
}


run_trial() {
  local repetition=$1
  local position=$2
  local stage=$3
  local trial_id
  printf -v trial_id 'repetition-%02d-position-%02d-%s' "$repetition" "$position" "$stage"
  local trial_dir
  trial_dir="$OUTPUT_ROOT/trials/repetition-$(printf '%02d' "$repetition")/$(printf '%02d' "$position")-$stage"
  if [[ -d $trial_dir && -n $(find "$trial_dir" -mindepth 1 -maxdepth 1 -print -quit) ]]; then
    die "refusing to reuse nonempty trial directory: $trial_dir"
  fi
  mkdir -p "$trial_dir"
  ACTIVE_TRIAL_DIR=$trial_dir
  ACTIVE_TRIAL_CAPTURED=0
  local run_id
  run_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
  write_trial_metadata "$trial_dir" "$stage" "$repetition" "$position" "$run_id" "$trial_id"
  cp "$BASELINE_CONFIG" "$CURRENT_CONFIG" "$trial_dir/"

  log "starting repetition=$repetition position=$position stage=$stage trial_id=$trial_id"
  if [[ $stage == assembly_join || $stage == assembly_join_check ]]; then
    STACK_MAY_BE_RUNNING=1
    capture_host_before "$trial_dir"
    run_full_stage "$stage" "$trial_dir"
    run_id=$(tr -d '[:space:]' <"$trial_dir/run_id.txt")
  else
    start_direct_stack "$stage"
    capture_host_before "$trial_dir"
    capture_versions "$trial_dir"
    export BLACKBOX_ACTIVE_RUN_ID=$run_id
    run_loader "$trial_dir"
    capture_bootstrap "$trial_dir"
    run_direct_workload "$stage" "$trial_dir" "$run_id"
    assert_no_transaction_markers "$trial_dir"
  fi

  refresh_network_after "$trial_dir"
  capture_redis_streams "$stage" "$trial_dir" "$run_id"
  analyze_trial "$stage" "$trial_dir" "$run_id"
  capture_stack_artifacts "$trial_dir"
  capture_versions "$trial_dir"
  date -u --iso-8601=seconds >"$trial_dir/host_after.txt"
  cat /proc/loadavg >>"$trial_dir/host_after.txt"
  uptime >>"$trial_dir/host_after.txt"
  record_command "$trial_dir/commands.txt" compose down --volumes --remove-orphans
  checksum_trial "$trial_dir"
  cleanup_stack
  ACTIVE_TRIAL_DIR=
  ACTIVE_TRIAL_CAPTURED=0
  log "completed $trial_id"
  if (( COOLDOWN_SECONDS > 0 )); then
    sleep "$COOLDOWN_SECONDS"
  fi
}


run_cdc_trial() {
  local repetition=$1
  local position=$2
  local stage=cdc_capture
  local trial_id
  printf -v trial_id 'repetition-%02d-position-%02d-%s' "$repetition" "$position" "$stage"
  local trial_dir
  trial_dir="$OUTPUT_ROOT/trials/repetition-$(printf '%02d' "$repetition")/$(printf '%02d' "$position")-$stage"
  if [[ -d $trial_dir && -n $(find "$trial_dir" -mindepth 1 -maxdepth 1 -print -quit) ]]; then
    die "refusing to reuse nonempty trial directory: $trial_dir"
  fi
  mkdir -p "$trial_dir"
  ACTIVE_TRIAL_DIR=$trial_dir
  ACTIVE_TRIAL_CAPTURED=0
  local run_id
  run_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
  write_trial_metadata "$trial_dir" "$stage" "$repetition" "$position" "$run_id" "$trial_id"
  cp "$BASELINE_CONFIG" "$CURRENT_CONFIG" "$trial_dir/"
  log "starting repetition=$repetition position=$position stage=$stage trial_id=$trial_id"

  start_direct_stack "$stage"
  capture_host_before "$trial_dir"
  capture_versions "$trial_dir"
  export BLACKBOX_ACTIVE_RUN_ID=$run_id
  record_command "$trial_dir/commands.txt" blackbox_commit_fence RUN_START
  blackbox_commit_fence RUN_START
  wait_for_cdc_fence RUN_START "$trial_dir"
  run_loader "$trial_dir"
  capture_bootstrap "$trial_dir"
  record_command "$trial_dir/commands.txt" blackbox_commit_fence LOAD_FINISHED
  blackbox_commit_fence LOAD_FINISHED
  wait_for_cdc_fence LOAD_FINISHED "$trial_dir"
  run_direct_workload "$stage" "$trial_dir" "$run_id"
  assert_no_transaction_markers "$trial_dir"
  record_command "$trial_dir/commands.txt" blackbox_commit_fence END_FENCE
  blackbox_commit_fence END_FENCE
  wait_for_cdc_fence END_FENCE "$trial_dir"

  refresh_network_after "$trial_dir"
  capture_redis_streams "$stage" "$trial_dir" "$run_id"
  analyze_trial "$stage" "$trial_dir" "$run_id"
  capture_stack_artifacts "$trial_dir"
  capture_versions "$trial_dir"
  date -u --iso-8601=seconds >"$trial_dir/host_after.txt"
  cat /proc/loadavg >>"$trial_dir/host_after.txt"
  uptime >>"$trial_dir/host_after.txt"
  record_command "$trial_dir/commands.txt" compose down --volumes --remove-orphans
  checksum_trial "$trial_dir"
  cleanup_stack
  ACTIVE_TRIAL_DIR=
  ACTIVE_TRIAL_CAPTURED=0
  log "completed $trial_id"
  if (( COOLDOWN_SECONDS > 0 )); then
    sleep "$COOLDOWN_SECONDS"
  fi
}


finalize_reports() {
  local raw="$OUTPUT_ROOT/raw_trials.csv"
  local summary="$OUTPUT_ROOT/summary_statistics.csv"
  local overhead="$OUTPUT_ROOT/overhead.csv"
  local artifact_index="$OUTPUT_ROOT/artifact_index.csv"
  local commands="$OUTPUT_ROOT/report-commands.txt"
  record_command "$commands" python3 "$REPORTER" aggregate \
    --root "$OUTPUT_ROOT" --raw-output "$raw" --overhead-output "$overhead" \
    --artifact-index-output "$artifact_index"
  python3 "$REPORTER" aggregate \
    --root "$OUTPUT_ROOT" \
    --raw-output "$raw" \
    --overhead-output "$overhead" \
    --artifact-index-output "$artifact_index"
  record_command "$commands" python3 "$ANALYZER" summarize-trials \
    --input-csv "$raw" --baseline-stage benchbase_uninstrumented \
    --output-csv "$summary" --output-json "$OUTPUT_ROOT/summary_statistics.json"
  python3 "$ANALYZER" summarize-trials \
    --input-csv "$raw" \
    --baseline-stage benchbase_uninstrumented \
    --output-csv "$summary" \
    --output-json "$OUTPUT_ROOT/summary_statistics.json"
  record_command "$commands" python3 "$REPORTER" checksums \
    --root "$OUTPUT_ROOT" --output "$OUTPUT_ROOT/SHA256SUMS"
  python3 "$REPORTER" checksums \
    --root "$OUTPUT_ROOT" --output "$OUTPUT_ROOT/SHA256SUMS"
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
  if (( VERIFY_INPUTS_ONLY == 1 )); then
    printf 'verified baseline=%s output=%s\n' "$BASELINE_HEAD" "$OUTPUT_ROOT"
    return 0
  fi

  mkdir -p "$OUTPUT_ROOT"
  write_system_provenance "$OUTPUT_ROOT/provenance"
  generate_configs
  print_stage_order >"$OUTPUT_ROOT/stage_order.csv"
  build_artifacts
  # shellcheck source=scripts/blackbox_lifecycle.sh
  . "$ROOT/scripts/blackbox_lifecycle.sh"
  blackbox_acquire_live_stack_lock
  LIVE_LOCK_ACQUIRED=1
  read -r -a JAVA_FLAGS <<<"$JAVA_FLAGS_TEXT"
  (( ${#JAVA_FLAGS[@]} > 0 )) || die "at least one explicit JVM flag is required"

  local repetition position offset index stage
  for (( repetition = 1; repetition <= REPETITIONS; repetition++ )); do
    offset=$(( (RANDOM_SEED + repetition - 1) % ${#STAGES[@]} ))
    for (( position = 1; position <= ${#STAGES[@]}; position++ )); do
      index=$(( (offset + position - 1) % ${#STAGES[@]} ))
      stage=${STAGES[$index]}
      if [[ $stage == cdc_capture ]]; then
        run_cdc_trial "$repetition" "$position"
      else
        run_trial "$repetition" "$position" "$stage"
      fi
    done
  done
  verify_repositories_unchanged
  finalize_reports
  log "artifacts=$OUTPUT_ROOT"
}


main "$@"
