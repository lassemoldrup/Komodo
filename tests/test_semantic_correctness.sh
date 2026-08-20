#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
SCRIPT="$ROOT/tests/test_semantic_correctness.sh"
# shellcheck source=scripts/blackbox_lifecycle.sh
. "$ROOT/scripts/blackbox_lifecycle.sh"

BLACKBOX_DOCKER=${BLACKBOX_DOCKER:-docker}
COMPOSE=("$BLACKBOX_DOCKER" compose --file "$ROOT/docker-compose.yml" --profile mariadb)
CLIENT="$ROOT/tests/semantic_client.sh"
OUTPUT_ROOT=${BLACKBOX_CORRECTNESS_OUTPUT_ROOT:-/tmp}
CASE_TIMEOUT_SECONDS=${BLACKBOX_CORRECTNESS_CASE_TIMEOUT_SECONDS:-600}
CASE_KILL_AFTER_SECONDS=${BLACKBOX_CORRECTNESS_CASE_KILL_AFTER_SECONDS:-10}
TERMINATION_GRACE_SECONDS=${BLACKBOX_CORRECTNESS_TERMINATION_GRACE_SECONDS:-3}
DIAGNOSTIC_TIMEOUT_SECONDS=${BLACKBOX_CORRECTNESS_DIAGNOSTIC_TIMEOUT_SECONDS:-30}
DIAGNOSTIC_KILL_AFTER_SECONDS=${BLACKBOX_CORRECTNESS_DIAGNOSTIC_KILL_AFTER_SECONDS:-5}
CLEANUP_TIMEOUT_SECONDS=${BLACKBOX_CORRECTNESS_CLEANUP_TIMEOUT_SECONDS:-60}
CLEANUP_KILL_AFTER_SECONDS=${BLACKBOX_CORRECTNESS_CLEANUP_KILL_AFTER_SECONDS:-10}
SUCCESS=0
RUN_OUTPUT=
CURRENT_CASE=
CURRENT_LOG=
ACTIVE_CASE_SESSION=


require_positive_integer() {
  local name=$1
  local value=$2
  [[ $value =~ ^[1-9][0-9]*$ ]] || {
    printf '%s must be a positive integer, found %q\n' "$name" "$value" >&2
    return 1
  }
}


require_harness_configuration() {
  : "${BLACKBOX_MONITOR_BIN:?set BLACKBOX_MONITOR_BIN to blackbox_run_monitor}"
  : "${BLACKBOX_BENCHBASE_BUILD_DIR:?set BLACKBOX_BENCHBASE_BUILD_DIR}"
  : "${BLACKBOX_BENCHBASE_ROOT:?set BLACKBOX_BENCHBASE_ROOT}"
  : "${BLACKBOX_MAVEN:?set BLACKBOX_MAVEN}"
  [[ -x $BLACKBOX_MONITOR_BIN ]]
  [[ -x $CLIENT ]]
  [[ -f $BLACKBOX_BENCHBASE_ROOT/pom.xml ]]
  command -v timeout >/dev/null 2>&1
  command -v "$BLACKBOX_DOCKER" >/dev/null 2>&1
  command -v "$BLACKBOX_REDIS_CLI" >/dev/null 2>&1
  command -v "$BLACKBOX_MARIADB_CLI" >/dev/null 2>&1
  command -v jq >/dev/null 2>&1
  command -v python3 >/dev/null 2>&1
  command -v setsid >/dev/null 2>&1
  require_positive_integer BLACKBOX_CORRECTNESS_CASE_TIMEOUT_SECONDS \
    "$CASE_TIMEOUT_SECONDS"
  require_positive_integer BLACKBOX_CORRECTNESS_CASE_KILL_AFTER_SECONDS \
    "$CASE_KILL_AFTER_SECONDS"
  require_positive_integer BLACKBOX_CORRECTNESS_TERMINATION_GRACE_SECONDS \
    "$TERMINATION_GRACE_SECONDS"
  require_positive_integer BLACKBOX_CORRECTNESS_DIAGNOSTIC_TIMEOUT_SECONDS \
    "$DIAGNOSTIC_TIMEOUT_SECONDS"
  require_positive_integer BLACKBOX_CORRECTNESS_DIAGNOSTIC_KILL_AFTER_SECONDS \
    "$DIAGNOSTIC_KILL_AFTER_SECONDS"
  require_positive_integer BLACKBOX_CORRECTNESS_CLEANUP_TIMEOUT_SECONDS \
    "$CLEANUP_TIMEOUT_SECONDS"
  require_positive_integer BLACKBOX_CORRECTNESS_CLEANUP_KILL_AFTER_SECONDS \
    "$CLEANUP_KILL_AFTER_SECONDS"
  (( CASE_KILL_AFTER_SECONDS > TERMINATION_GRACE_SECONDS )) || {
    printf '%s must exceed %s so process-group cleanup can escalate\n' \
      BLACKBOX_CORRECTNESS_CASE_KILL_AFTER_SECONDS \
      BLACKBOX_CORRECTNESS_TERMINATION_GRACE_SECONDS >&2
    return 1
  }
}


run_bounded_diagnostic() {
  timeout --foreground --kill-after="${DIAGNOSTIC_KILL_AFTER_SECONDS}s" \
    "${DIAGNOSTIC_TIMEOUT_SECONDS}s" "$@"
}


run_bounded_cleanup() {
  timeout --foreground --kill-after="${CLEANUP_KILL_AFTER_SECONDS}s" \
    "${CLEANUP_TIMEOUT_SECONDS}s" "$@"
}


redis_command() {
  local command=(
    "$BLACKBOX_REDIS_CLI"
    -h "$BLACKBOX_REDIS_HOST"
    -p "$BLACKBOX_REDIS_PORT"
  )
  if [[ -n ${BLACKBOX_REDIS_PASSWORD:-} ]]; then
    command+=(--pass "$BLACKBOX_REDIS_PASSWORD")
  fi
  run_bounded_diagnostic "${command[@]}" "$@"
}


database_query() {
  local query=$1
  run_bounded_diagnostic "$BLACKBOX_MARIADB_CLI" \
    --skip-ssl \
    --host="$BLACKBOX_DB_HOST" \
    --port="$BLACKBOX_DB_PORT" \
    --user="$BLACKBOX_DB_USER" \
    --password="$BLACKBOX_DB_PASSWORD" \
    --database=benchbase \
    --default-character-set=utf8mb4 \
    --batch \
    --raw \
    --skip-column-names \
    --execute "$query"
}


capture_redis_stream() {
  local stream=$1
  local output_file=$2
  redis_command --json XRANGE "$stream" - + >"$output_file"
}


extract_run_id() {
  local log_file=$1
  local line run_id=
  [[ -f $log_file ]] || return 1
  while IFS= read -r line; do
    if [[ $line == '[blackbox] run_id='* ]]; then
      run_id=${line#'[blackbox] run_id='}
    fi
  done <"$log_file"
  [[ $run_id =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] ||
    return 1
  printf '%s\n' "$run_id"
}


capture_failure_artifacts() {
  [[ -n $RUN_OUTPUT && -d $RUN_OUTPUT ]] || return 0
  local case_name=${CURRENT_CASE:-setup}
  local prefix="$RUN_OUTPUT/$case_name-failure"
  local errors="$prefix-capture-errors.log"
  local run_id=

  if ! capture_redis_stream cdc "$prefix-cdc.json" 2>>"$errors"; then
    printf 'could not capture the CDC stream\n' >>"$errors"
  fi
  if [[ -n $CURRENT_LOG ]]; then
    run_id=$(extract_run_id "$CURRENT_LOG" 2>>"$errors" || true)
  fi
  if [[ -n $run_id ]]; then
    if ! capture_redis_stream "blackbox:client:$run_id" \
      "$prefix-client.json" 2>>"$errors"; then
      printf 'could not capture the client stream for %s\n' "$run_id" >>"$errors"
    fi
    if ! capture_redis_stream "blackbox:status:$run_id" \
      "$prefix-status.json" 2>>"$errors"; then
      printf 'could not capture the status stream for %s\n' "$run_id" >>"$errors"
    fi
  else
    printf 'could not recover a run ID from %s\n' "${CURRENT_LOG:-no case log}" >>"$errors"
  fi
  if ! run_bounded_diagnostic "${COMPOSE[@]}" ps --all \
    >"$prefix-compose-ps.txt" 2>>"$errors"; then
    printf 'could not capture Compose process state\n' >>"$errors"
  fi
  if ! run_bounded_diagnostic "${COMPOSE[@]}" logs --no-color --timestamps \
    >"$prefix-compose.log" 2>>"$errors"; then
    printf 'could not capture Compose logs\n' >>"$errors"
  fi
  if [[ ! -s $errors ]]; then
    rm -f -- "$errors"
  fi
}


cleanup() {
  local status=$?
  trap - EXIT
  if (( SUCCESS != 1 )); then
    capture_failure_artifacts || true
  fi
  if ! run_bounded_cleanup "${COMPOSE[@]}" down --volumes --remove-orphans \
    >/dev/null 2>&1; then
    printf 'bounded Compose cleanup did not complete successfully\n' >&2
  fi
  if (( SUCCESS == 1 )) && [[ ${BLACKBOX_KEEP_CORRECTNESS_OUTPUT:-0} != 1 ]]; then
    rm -rf -- "$RUN_OUTPUT"
  else
    printf 'semantic run output retained at %s\n' "$RUN_OUTPUT" >&2
  fi
  return "$status"
}


extract_final_status() {
  local log_file=$1
  python3 - "$log_file" <<'PY'
import json
import sys

records = []
with open(sys.argv[1], encoding="utf-8") as source:
    for line in source:
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if value.get("kind") == "status" and value.get("lifecycle_state") == "FINAL":
            records.append(value)
if len(records) != 1:
    raise SystemExit(f"expected exactly one final status, found {len(records)}")
print(json.dumps(records[0], separators=(",", ":"), sort_keys=True))
PY
}


assert_client_evidence() {
  local input_file=$1
  local mode=$2
  local expected_first=$3
  local expected_second=$4
  local dropped=$5
  python3 - "$input_file" "$mode" "$expected_first" "$expected_second" "$dropped" <<'PY'
import base64
import json
import struct
import sys

entries = json.load(open(sys.argv[1], encoding="utf-8"))
mode = sys.argv[2]
expected_values = [sys.argv[3], sys.argv[4]]
dropped = sys.argv[5] == "true"
records = []
for _, fields in entries:
    if len(fields) != 2 or fields[0] != "payload":
        raise SystemExit("client stream entry does not contain one payload field")
    records.append(json.loads(fields[1]))

clients = [record for record in records if record.get("kind") == "client_txn"]
by_session = {}
for record in clients:
    by_session.setdefault(record.get("session_id"), []).append(record)
done = [record for record in records if record.get("kind") == "session_done"]
done_by_session = {record.get("session_id"): record for record in done}
expected_sessions = {"ycsb/worker/1", "ycsb/worker/2"}
if len(done) != 2 or set(done_by_session) != expected_sessions:
    raise SystemExit("both probe sessions must publish exactly one SessionDone")

def one_client(session_id):
    matches = by_session.get(session_id, [])
    if len(matches) != 1:
        raise SystemExit(f"expected one ClientTxn for {session_id}, found {len(matches)}")
    record = matches[0]
    if record.get("txn_type") != "si_fractured_read_probe":
        raise SystemExit(f"{session_id} has the wrong transaction type")
    return record

def canonical_key(key):
    def lp(value):
        return struct.pack(">I", len(value)) + value
    return lp(b"benchbase") + lp(b"usertable") + struct.pack(">i", key)

def first_field(encoded):
    raw = base64.b64decode(encoded, validate=True)
    if not raw or raw[0] != 1 or len(raw) < 5:
        raise SystemExit("expected a present canonical first field")
    length = struct.unpack(">I", raw[1:5])[0]
    end = 5 + length
    if end > len(raw):
        raise SystemExit("truncated canonical first field")
    return raw[5:end].decode("utf-8")

def assert_reader(record, session_id):
    reads = record.get("reads")
    if record.get("read_count") != 2 or not isinstance(reads, list) or len(reads) != 2:
        raise SystemExit(f"{session_id} must contain exactly two reads")
    for event_seq, (read, key) in enumerate(zip(reads, (1, 2), strict=True)):
        if read.get("event_seq") != event_seq:
            raise SystemExit(f"{session_id} read sequence is not zero-based and dense")
        if read.get("source") != "snapshot" or read.get("value_state") != "present":
            raise SystemExit(f"{session_id} reads must be present snapshot observations")
        observed_key = base64.b64decode(read["key_bytes_base64"], validate=True)
        if observed_key != canonical_key(key):
            raise SystemExit(f"{session_id} read {event_seq} did not capture key {key}")
    observed = [first_field(read["value_bytes_base64"]) for read in reads]
    if observed != expected_values:
        raise SystemExit(
            f"{session_id} values {observed!r} differ from {expected_values!r}"
        )

if dropped:
    if set(by_session) != {"ycsb/worker/2"} or len(clients) != 1:
        raise SystemExit("the dropped case must retain only worker 2's ClientTxn")
    if done_by_session["ycsb/worker/1"].get("published_transactions") != 0:
        raise SystemExit("the omitted ClientTxn must have a zero-publication closure ledger")
    assert_reader(one_client("ycsb/worker/2"), "ycsb/worker/2")
elif mode == "clean":
    if set(by_session) != expected_sessions or len(clients) != 2:
        raise SystemExit("clean mode must publish one ClientTxn for each reader")
    assert_reader(one_client("ycsb/worker/1"), "ycsb/worker/1")
    assert_reader(one_client("ycsb/worker/2"), "ycsb/worker/2")
elif mode == "fractured":
    if set(by_session) != expected_sessions or len(clients) != 2:
        raise SystemExit("fractured mode must publish one ClientTxn for each session")
    assert_reader(one_client("ycsb/worker/1"), "ycsb/worker/1")
    writer = one_client("ycsb/worker/2")
    if writer.get("read_count") != 0 or writer.get("reads") != []:
        raise SystemExit("fractured mode worker 2 must be the zero-read writer")
else:
    raise SystemExit(f"unknown probe mode {mode!r}")
PY
}


assert_database_state() {
  local mode=$1
  local value
  case "$mode" in
    clean) value=old ;;
    fractured) value=new ;;
    *) return 1 ;;
  esac
  local expected observed
  printf -v expected '1\t%s\n2\t%s' "$value" "$value"
  observed=$(database_query \
    'SELECT ycsb_key, field1 FROM usertable WHERE ycsb_key IN (1, 2) ORDER BY ycsb_key')
  if [[ $observed != "$expected" ]]; then
    printf 'final database state %q differs from expected %q\n' \
      "$observed" "$expected" >&2
    return 1
  fi
}


assert_terminal_status() {
  local final_status=$1
  local expected_outcome=$2
  jq -e --arg outcome "$expected_outcome" '
    .outcome == $outcome and
    .lifecycle_state == "FINAL" and
    .assembled_cdc_transactions == 6 and
    .reconciliation.bootstrap_manifest_validated == true and
    .reconciliation.expected_sessions == 2 and
    .reconciliation.completed_sessions == 2 and
    .reconciliation.bootstrap_cdc_transactions == 1 and
    .reconciliation.bootstrap_checker_transactions == 1 and
    .reconciliation.phase_anchor_transactions == 2
  ' <<<"$final_status" >/dev/null

  case "$expected_outcome" in
    PASS)
      jq -e '
        .checker_observation == "PASS_SO_FAR" and
        (.diagnostics | length) == 0 and
        .reconciliation.evidence_reconciled == true and
        .reconciliation.end_fence_seen == true and
        .reconciliation.accepted_client_transactions == 2 and
        .reconciliation.joined_workload_transactions == 2 and
        .reconciliation.checker_consumed_transactions == 5 and
        .reconciliation.unmatched_client_records == 0 and
        .reconciliation.unmatched_workload_cdc_slots == 0
      ' <<<"$final_status" >/dev/null
      ;;
    VIOLATION)
      jq -e '
        .checker_observation == "VIOLATION_OBSERVED" and
        .reconciliation.evidence_reconciled == true and
        .reconciliation.end_fence_seen == true and
        .reconciliation.accepted_client_transactions == 2 and
        .reconciliation.joined_workload_transactions == 2 and
        .reconciliation.checker_consumed_transactions == 5 and
        any(.diagnostics[];
          .category == "CHECKER" and
          .code == "VIOLATION_OBSERVED" and
          (.message | contains("session_id=ycsb/worker/1")))
      ' <<<"$final_status" >/dev/null
      ;;
    INVALID)
      jq -e '
        .checker_observation != "VIOLATION_OBSERVED" and
        .reconciliation.evidence_reconciled == false and
        .reconciliation.end_fence_seen == true and
        .reconciliation.accepted_client_transactions == 1 and
        .reconciliation.joined_workload_transactions == 0 and
        .reconciliation.checker_consumed_transactions == 3 and
        .reconciliation.unmatched_client_records == 0 and
        .reconciliation.unmatched_workload_cdc_slots == 1 and
        all(.diagnostics[]; .code != "CDC_ORDER_INVALID") and
        any(.diagnostics[];
          .category == "PROTOCOL" and
          .code == "CLIENT_HALF_MISSING")
      ' <<<"$final_status" >/dev/null
      ;;
    *) return 1 ;;
  esac
}


terminate_case_session() {
  local signal=$1
  local status=$2
  trap - HUP INT TERM
  if [[ -n $ACTIVE_CASE_SESSION ]] && kill -0 -- "-$ACTIVE_CASE_SESSION" 2>/dev/null; then
    kill -s "$signal" -- "-$ACTIVE_CASE_SESSION" 2>/dev/null || true
    local deadline=$(( SECONDS + TERMINATION_GRACE_SECONDS ))
    while kill -0 -- "-$ACTIVE_CASE_SESSION" 2>/dev/null &&
        (( SECONDS < deadline )); do
      sleep 0.1
    done
    if kill -0 -- "-$ACTIVE_CASE_SESSION" 2>/dev/null; then
      kill -KILL -- "-$ACTIVE_CASE_SESSION" 2>/dev/null || true
    fi
    wait "$ACTIVE_CASE_SESSION" 2>/dev/null || true
    ACTIVE_CASE_SESSION=
  fi
  exit "$status"
}


run_case_body() {
  (( $# == 8 )) || {
    echo 'internal semantic case invocation requires eight arguments' >&2
    return 2
  }
  local case_name=$1
  local isolation=$2
  local mode=$3
  local drop_session=$4
  local expected_outcome=$5
  local expected_exit=$6
  local expected_first=$7
  local expected_second=$8
  [[ $case_name =~ ^[a-z0-9_]+$ ]]
  local log_file="$RUN_OUTPUT/$case_name.log"
  local client_file="$RUN_OUTPUT/$case_name-client.json"
  local status_file="$RUN_OUTPUT/$case_name-status.json"
  local loader_command workload_command
  printf -v loader_command '%q load' "$CLIENT"
  printf -v workload_command '%q run' "$CLIENT"

  local status
  trap 'terminate_case_session HUP 129' HUP
  trap 'terminate_case_session INT 130' INT
  trap 'terminate_case_session TERM 143' TERM
  set +e
  BLACKBOX_DECLARED_ISOLATION=$isolation \
  BLACKBOX_EXPECTED_SESSION_IDS=ycsb/worker/1,ycsb/worker/2 \
  BLACKBOX_LOADER_COMMAND=$loader_command \
  BLACKBOX_LOADER_DECLARED_COUNTS_FILE="$RUN_OUTPUT/$case_name-loader-declaration.json" \
  BLACKBOX_WORKLOAD_COMMAND=$workload_command \
  BLACKBOX_BENCHBASE_BUILD_DIR=$BLACKBOX_BENCHBASE_BUILD_DIR \
  BLACKBOX_BENCHBASE_ROOT=$BLACKBOX_BENCHBASE_ROOT \
  BLACKBOX_MAVEN=$BLACKBOX_MAVEN \
  BLACKBOX_PROBE_MODE=$mode \
  BLACKBOX_DROP_CLIENT_SESSION=$drop_session \
  BLACKBOX_PROBE_TIMEOUT_SECONDS=30 \
  BLACKBOX_STACK_TIMEOUT_SECONDS=180 \
  BLACKBOX_LOADER_TIMEOUT_SECONDS=30 \
  BLACKBOX_WORKLOAD_TIMEOUT_SECONDS=60 \
  BLACKBOX_STATUS_TIMEOUT_SECONDS=120 \
  BLACKBOX_MONITOR_BIN=$BLACKBOX_MONITOR_BIN \
    setsid "$ROOT/scripts/e2e.sh" >"$log_file" 2>&1 &
  ACTIVE_CASE_SESSION=$!
  wait "$ACTIVE_CASE_SESSION"
  status=$?
  ACTIVE_CASE_SESSION=
  set -e
  trap - HUP INT TERM

  if (( status != expected_exit )); then
    echo "$case_name exited $status, expected $expected_exit" >&2
    return 1
  fi

  local final_status run_id status_run_id
  run_id=$(extract_run_id "$log_file")
  capture_redis_stream "blackbox:client:$run_id" "$client_file"
  capture_redis_stream "blackbox:status:$run_id" "$status_file"
  final_status=$(extract_final_status "$log_file")
  assert_terminal_status "$final_status" "$expected_outcome"
  status_run_id=$(jq -er '.run_id' <<<"$final_status")
  [[ $status_run_id == "$run_id" ]]
  if [[ -n $drop_session ]]; then
    assert_client_evidence \
      "$client_file" "$mode" "$expected_first" "$expected_second" true
  else
    assert_client_evidence \
      "$client_file" "$mode" "$expected_first" "$expected_second" false
  fi
  assert_database_state "$mode"

  local final_count database_value
  final_count=$(jq \
    '[.[] | (.[1][1] | fromjson) | select(.lifecycle_state == "FINAL")] | length' \
    "$status_file")
  [[ $final_count == 1 ]]
  if [[ $mode == clean ]]; then
    database_value=old
  else
    database_value=new
  fi

  printf 'semantic case %s: outcome=%s run_id=%s reader=%s/%s database=%s/%s\n' \
    "$case_name" "$expected_outcome" "$run_id" "$expected_first" \
    "$expected_second" "$database_value" "$database_value"
}


run_case() {
  local case_name=$1
  CURRENT_CASE=$case_name
  CURRENT_LOG="$RUN_OUTPUT/$case_name.log"
  printf 'running semantic case %s (isolation=%s mode=%s)\n' "$1" "$2" "$3"

  local status
  set +e
  BLACKBOX_CORRECTNESS_RUN_OUTPUT=$RUN_OUTPUT \
  BLACKBOX_MONITOR_BIN=$BLACKBOX_MONITOR_BIN \
  BLACKBOX_BENCHBASE_BUILD_DIR=$BLACKBOX_BENCHBASE_BUILD_DIR \
  BLACKBOX_BENCHBASE_ROOT=$BLACKBOX_BENCHBASE_ROOT \
  BLACKBOX_MAVEN=$BLACKBOX_MAVEN \
    timeout --foreground --kill-after="${CASE_KILL_AFTER_SECONDS}s" \
      "${CASE_TIMEOUT_SECONDS}s" "$SCRIPT" --run-case "$@"
  status=$?
  set -e
  if (( status != 0 )); then
    if [[ -f $CURRENT_LOG ]]; then
      sed -n '1,260p' "$CURRENT_LOG" >&2
    fi
    if (( status == 124 || status == 137 )); then
      printf '%s exceeded its %ss wall-clock limit (status %s)\n' \
        "$case_name" "$CASE_TIMEOUT_SECONDS" "$status" >&2
    else
      printf '%s semantic assertions failed (status %s)\n' "$case_name" "$status" >&2
    fi
    return 1
  fi
}


run_all_cases() {
  require_harness_configuration
  blackbox_acquire_live_stack_lock
  mkdir -p "$OUTPUT_ROOT"
  RUN_OUTPUT=$(mktemp -d "$OUTPUT_ROOT/blackbox-correctness.XXXXXX")
  trap cleanup EXIT

  run_case clean_repeatable_read REPEATABLE_READ clean '' PASS 0 old old
  run_case fractured_read_committed READ_COMMITTED fractured '' VIOLATION 3 old new
  run_case fractured_repeatable_read REPEATABLE_READ fractured '' PASS 0 old old
  run_case dropped_client_record REPEATABLE_READ clean ycsb/worker/1 INVALID 4 old old

  SUCCESS=1
  printf 'all semantic correctness cases passed\n'
}


case "${1:-}" in
  --run-case)
    shift
    require_harness_configuration
    : "${BLACKBOX_CORRECTNESS_RUN_OUTPUT:?missing internal semantic output directory}"
    RUN_OUTPUT=$BLACKBOX_CORRECTNESS_RUN_OUTPUT
    [[ -d $RUN_OUTPUT ]]
    run_case_body "$@"
    ;;
  '') run_all_cases ;;
  *)
    echo "usage: $0" >&2
    exit 2
    ;;
esac
