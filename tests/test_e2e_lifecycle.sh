#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
TEST_ROOT=$(mktemp -d /tmp/blackbox-lifecycle-test.XXXXXX)
trap 'rm -rf -- "$TEST_ROOT"' EXIT


fail() {
  printf 'lifecycle test failed: %s\n' "$*" >&2
  exit 1
}


write_test_programs() {
  local case_root=$1
  mkdir -p "$case_root/bin"

  cat >"$case_root/bin/docker" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
if [[ $1 == compose ]]; then
  if [[ " $* " == *" up "* && " $* " == *" redis mariadb "* ]]; then
    echo 'stack:database' >>"$EVENT_LOG"
  elif [[ " $* " == *" up "* && " $* " == *" debezium "* ]]; then
    echo 'stack:cdc' >>"$EVENT_LOG"
  fi
  exit 0
fi
if [[ $1 == inspect ]]; then
  if [[ $* == *Health* ]]; then
    echo healthy
  else
    echo running
  fi
  exit 0
fi
if [[ $1 == logs ]]; then
  echo 'Starting streaming'
  exit 0
fi
exit 2
SCRIPT

  cat >"$case_root/bin/mariadb" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
query=
while (( $# > 0 )); do
  if [[ $1 == --execute ]]; then
    query=$2
    break
  fi
  shift
done
case "$query" in
  *"'SETTING'"*)
    cat <<'METADATA'
SETTING	server.character_set	utf8mb4
SETTING	server.collation	utf8mb4_bin
SETTING	server.time_zone	+00:00
SETTING	server.sql_mode	STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,PAD_CHAR_TO_FULL_LENGTH
SETTING	server.transaction_isolation	REPEATABLE-READ
SETTING	server.snapshot_isolation	1
SETTING	session.character_set_client	utf8mb4
SETTING	session.character_set_connection	utf8mb4
SETTING	session.character_set_results	utf8mb4
SETTING	session.collation_connection	utf8mb4_bin
SETTING	session.time_zone	+00:00
SETTING	session.sql_mode	STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,PAD_CHAR_TO_FULL_LENGTH
SETTING	session.transaction_isolation	REPEATABLE-READ
SETTING	session.snapshot_isolation	1
METADATA
    ;;
  *"information_schema.SCHEMATA"*)
    printf 'DATABASE\tbenchbase\tutf8mb4\tutf8mb4_bin\n'
    ;;
  *"information_schema.TABLES"*)
    printf 'TABLE\tbenchbase.checker_run_fence\tInnoDB\tutf8mb4_bin\n'
    printf 'TABLE\tbenchbase.session_txn\tInnoDB\tutf8mb4_bin\n'
    printf 'TABLE\tbenchbase.usertable\tInnoDB\tutf8mb4_bin\n'
    ;;
  *"information_schema.COLUMNS"*)
    cat <<'METADATA'
COLUMN	benchbase.checker_run_fence	run_id	1	char	NO	-	-	36	-	ascii	ascii_bin	-
COLUMN	benchbase.checker_run_fence	phase	2	varchar	NO	-	-	32	-	ascii	ascii_bin	-
COLUMN	benchbase.checker_run_fence	nonce	3	char	NO	-	-	36	-	ascii	ascii_bin	-
COLUMN	benchbase.session_txn	session_id	1	varchar	NO	-	-	64	-	ascii	ascii_bin	-
COLUMN	benchbase.session_txn	txn_id	2	bigint	NO	19	0	-	-	-	-	-
COLUMN	benchbase.usertable	ycsb_key	1	int	NO	10	0	-	-	-	-	-
COLUMN	benchbase.usertable	field1	2	varchar	YES	-	-	100	-	utf8mb4	utf8mb4_bin	-
COLUMN	benchbase.usertable	field2	3	varchar	YES	-	-	100	-	utf8mb4	utf8mb4_bin	-
COLUMN	benchbase.usertable	field3	4	varchar	YES	-	-	100	-	utf8mb4	utf8mb4_bin	-
COLUMN	benchbase.usertable	field4	5	varchar	YES	-	-	100	-	utf8mb4	utf8mb4_bin	-
COLUMN	benchbase.usertable	field5	6	varchar	YES	-	-	100	-	utf8mb4	utf8mb4_bin	-
COLUMN	benchbase.usertable	field6	7	varchar	YES	-	-	100	-	utf8mb4	utf8mb4_bin	-
COLUMN	benchbase.usertable	field7	8	varchar	YES	-	-	100	-	utf8mb4	utf8mb4_bin	-
COLUMN	benchbase.usertable	field8	9	varchar	YES	-	-	100	-	utf8mb4	utf8mb4_bin	-
COLUMN	benchbase.usertable	field9	10	varchar	YES	-	-	100	-	utf8mb4	utf8mb4_bin	-
COLUMN	benchbase.usertable	field10	11	varchar	YES	-	-	100	-	utf8mb4	utf8mb4_bin	-
METADATA
    ;;
  *"information_schema.KEY_COLUMN_USAGE"*)
    printf 'PRIMARY\tbenchbase.checker_run_fence\trun_id\t1\n'
    printf 'PRIMARY\tbenchbase.checker_run_fence\tphase\t2\n'
    printf 'PRIMARY\tbenchbase.session_txn\tsession_id\t1\n'
    printf 'PRIMARY\tbenchbase.usertable\tycsb_key\t1\n'
    ;;
  *"COUNT(*) FROM session_txn"*)
    cat "$MARKER_ROWS_FILE"
    ;;
  *"COUNT(*) FROM "*"benchbase"*"usertable"*)
    echo "${OBSERVED_LOADER_ROWS:-0}"
    ;;
  *"COUNT(*) FROM "*"usertable"*)
    echo 0
    ;;
  *"COUNT(*) FROM checker_run_fence"*)
    echo 0
    ;;
  *"@@innodb_snapshot_isolation"*)
    echo 1
    ;;
  *"information_schema.PROCESSLIST"*)
    cat "$ACTIVE_CONNECTIONS_FILE"
    ;;
  *"SELECT ycsb_key,"*)
    : # Empty bootstrap state.
    ;;
  *"INSERT INTO checker_run_fence"*)
    phase=$(sed -n "s/.*VALUES ('[^']*', '\([^']*\)',.*/\1/p" <<<"$query")
    [[ -n $phase ]]
    if [[ $phase == ${FAIL_FENCE_PHASE:-} ]]; then
      echo "fence_failed:$phase" >>"$EVENT_LOG"
      exit 1
    fi
    echo "fence:$phase" >>"$EVENT_LOG"
    ;;
  *)
    printf 'unexpected fake MariaDB query: %s\n' "$query" >&2
    exit 2
    ;;
esac
SCRIPT

  cat >"$case_root/bin/redis-cli" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
json_output=0
while [[ ${1:-} == -* ]]; do
  case "$1" in
    -h|-p|--pass) shift 2 ;;
    --raw) shift ;;
    --json) json_output=1; shift ;;
    *) printf 'unexpected redis option: %s\n' "$1" >&2; exit 2 ;;
  esac
done
command=$1
shift
case "$command" in
  CONFIG)
    [[ $1 == GET ]]
    case "$2" in
      maxmemory) printf 'maxmemory\n%s\n' "${EXPECTED_REDIS_MAXMEMORY_BYTES:-2147483648}" ;;
      maxmemory-policy) printf 'maxmemory-policy\nnoeviction\n' ;;
      *) exit 2 ;;
    esac
    ;;
  EXISTS|DBSIZE)
    echo 0
    ;;
  XADD)
    stream=$1
    field=$3
    payload=$4
    [[ $field == payload ]]
    kind=$(python3 -c '
import json, sys
record = json.loads(sys.argv[1])
if record["kind"] == "run_manifest":
    assert record["benchmark"] == "ycsb"
    assert record["application_table_allowlist"] == ["benchbase.usertable"]
    assert record["transaction_marker_table"] == "benchbase.session_txn"
    assert record["result_scope"] == "point_transaction_soww"
print(record["kind"])
' "$payload")
    echo "publish:$kind" >>"$EVENT_LOG"
    counter=1
    if [[ -f $REDIS_COUNTER_FILE ]]; then
      counter=$(( $(cat "$REDIS_COUNTER_FILE") + 1 ))
    fi
    echo "$counter" >"$REDIS_COUNTER_FILE"
    echo "$counter-0"
    ;;
  XREAD)
    (( json_output == 1 ))
    stream=${@: -2:1}
    cursor=${@: -1}
    run_id=${stream##*:}
    lifecycle=
    entry_id=
    if [[ $cursor != 3-0 ]] && grep -q '^publish:run_failed$' "$EVENT_LOG"; then
      lifecycle=FINAL
      entry_id=3-0
    elif [[ $cursor == 0-0 && ${SUPPRESS_START_ACK:-0} != 1 ]] && grep -q '^fence:RUN_START$' "$EVENT_LOG"; then
      lifecycle=START_FENCE_SEEN
      entry_id=1-0
    elif [[ $cursor == 1-0 && ${EMIT_LIFECYCLE_REGRESSION:-0} == 1 ]]; then
      lifecycle=STARTING
      entry_id=1-1
    elif [[ $cursor == 1-0 ]] && grep -q '^fence:LOAD_FINISHED$' "$EVENT_LOG"; then
      lifecycle=LOAD_FINISHED_SEEN
      entry_id=2-0
    elif [[ $cursor == 2-0 ]] && { grep -q '^fence:END_FENCE$' "$EVENT_LOG" || grep -q '^publish:run_failed$' "$EVENT_LOG"; }; then
      lifecycle=FINAL
      entry_id=3-0
    fi
    if [[ -z $lifecycle ]]; then
      sleep 0.05
      echo null
      exit 0
    fi
    echo "status:$lifecycle" >>"$EVENT_LOG"
    python3 - "$stream" "$run_id" "$lifecycle" "$entry_id" <<'PY'
import json
import sys

stream, run_id, lifecycle, entry_id = sys.argv[1:]
with open(__import__("os").environ["EVENT_LOG"], encoding="utf-8") as event_file:
    event_lines = {line.rstrip("\n") for line in event_file}
failed = lifecycle == "FINAL" and "publish:run_failed" in event_lines
end_seen = "fence:END_FENCE" in event_lines
record = {
    "kind": "status",
    "run_id": run_id,
    "lifecycle_state": lifecycle,
    "outcome": ("INVALID" if failed else "PASS") if lifecycle == "FINAL" else "RUNNING",
    "checker_observation": "PASS_SO_FAR" if lifecycle == "FINAL" else "NOT_STARTED",
    "last_cdc_stream_id": "0-0",
    "last_client_stream_id": "0-0",
    "assembled_cdc_transactions": 3,
    "joined_transactions": 0,
    "checked_transactions": 1,
    "pending_cdc_slots": 0,
    "unmatched_client_records": 0,
    "diagnostics": [],
}
if lifecycle == "FINAL":
    record["reconciliation"] = {
        "evidence_reconciled": not failed,
        "bootstrap_manifest_validated": True,
        "end_fence_seen": end_seen,
        "expected_sessions": 1,
        "completed_sessions": 1,
        "accepted_client_transactions": 0,
        "joined_workload_transactions": 0,
        "bootstrap_cdc_transactions": 0,
        "bootstrap_checker_transactions": 0,
        "phase_anchor_transactions": 1,
        "checker_consumed_transactions": 1,
        "unmatched_client_records": 0,
        "unmatched_workload_cdc_slots": 0,
        "ambiguous_attempts": 0,
    }
payload = json.dumps(record, separators=(",", ":"))
print(json.dumps({stream: [[entry_id, ["payload", payload]]]}, separators=(",", ":")))
PY
    ;;
  *)
    printf 'unexpected fake Redis command: %s\n' "$command" >&2
    exit 2
    ;;
esac
SCRIPT

  cat >"$case_root/bin/monitor" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
trap 'echo "monitor:stop" >>"$EVENT_LOG"; exit 0' TERM INT
processing_mode=audited
metrics_output=
pending_client_bytes=268435456
retained_identity_bytes=536870912
while (( $# > 0 )); do
  case "$1" in
    --processing-mode) processing_mode=$2; shift 2 ;;
    --metrics-output) metrics_output=$2; shift 2 ;;
    --max-pending-client-bytes) pending_client_bytes=$2; shift 2 ;;
    --max-retained-identity-bytes) retained_identity_bytes=$2; shift 2 ;;
    *) echo "unexpected monitor argument: $1" >&2; exit 2 ;;
  esac
done
echo 'monitor:start' >>"$EVENT_LOG"
echo "monitor:mode:$processing_mode" >>"$EVENT_LOG"
echo "monitor:pending-client-bytes:$pending_client_bytes" >>"$EVENT_LOG"
echo "monitor:retained-identity-bytes:$retained_identity_bytes" >>"$EVENT_LOG"
while ! grep -q '^fence:END_FENCE$' "$EVENT_LOG"; do
  sleep 0.05
done
if [[ $processing_mode == join-only ]]; then
  [[ -n $metrics_output ]]
  completion_status=completed
  completed=true
  reconciled=true
  exit_status=0
  if grep -q '^publish:run_failed$' "$EVENT_LOG"; then
    completion_status=failed
    completed=false
    reconciled=false
    exit_status=1
  fi
  python3 - "$metrics_output" "$completion_status" "$completed" "$reconciled" \
    "$pending_client_bytes" "$retained_identity_bytes" <<'PY'
import json
import os
import sys

path, completion_status, completed, reconciled, pending_client_bytes, retained_identity_bytes = sys.argv[1:]
completed = completed == "true"
reconciled = reconciled == "true"
measurement = {
    "kind": "performance_measurement",
    "run_id": os.environ["BLACKBOX_ACTIVE_RUN_ID"],
    "processing_mode": "join-only",
    "completed": completed,
    "completion_status": completion_status,
    "evidence_reconciled": reconciled,
    "started_at_utc": "2026-08-18T00:00:00.000Z",
    "finished_at_utc": "2026-08-18T00:00:01.000Z",
    "monitor_wall_time_nanoseconds": 1_000_000_000,
    "limits": {"joiner": {
        "max_pending_client_bytes": int(pending_client_bytes),
        "max_retained_identity_bytes": int(retained_identity_bytes),
    }},
    "process": {"user_cpu_nanoseconds": 1, "system_cpu_nanoseconds": 1, "max_rss_kib": 1},
    "assembler": {"completed_transactions": 3},
    "joiner": {"released_phase_anchor_transactions": 1},
    "checker": {"consumed_transactions": 0},
    "reconciliation": {"evidence_reconciled": reconciled},
    "diagnostics": [] if completed else [{"category": "OPERATIONAL", "code": "INTERNAL_ERROR", "message": "injected failure"}],
}
with open(path, "x", encoding="utf-8") as output:
    json.dump(measurement, output, separators=(",", ":"))
    output.write("\n")
PY
  echo "monitor:measurement:$completion_status" >>"$EVENT_LOG"
  exit "$exit_status"
fi
if [[ -n $metrics_output ]]; then
  while ! grep -q '^status:FINAL$' "$EVENT_LOG"; do
    sleep 0.05
  done
  completion_status=completed
  completed=true
  reconciled=true
  outcome=PASS
  exit_status=0
  if grep -q '^publish:run_failed$' "$EVENT_LOG"; then
    completion_status=failed
    completed=false
    reconciled=false
    outcome=INVALID
    exit_status=4
  fi
  python3 - "$metrics_output" "$completion_status" "$completed" "$reconciled" \
    "$outcome" "$pending_client_bytes" "$retained_identity_bytes" <<'PY'
import json
import os
import sys

path, completion_status, completed, reconciled, outcome, pending_client_bytes, retained_identity_bytes = sys.argv[1:]
completed = completed == "true"
reconciled = reconciled == "true"
measurement = {
    "kind": "performance_measurement",
    "run_id": os.environ["BLACKBOX_ACTIVE_RUN_ID"],
    "processing_mode": "audited",
    "completed": completed,
    "completion_status": completion_status,
    "evidence_reconciled": reconciled,
    "outcome": outcome,
    "started_at_utc": "2026-08-18T00:00:00.000Z",
    "finished_at_utc": "2026-08-18T00:00:01.000Z",
    "monitor_wall_time_nanoseconds": 1_000_000_000,
    "limits": {"joiner": {
        "max_pending_client_bytes": int(pending_client_bytes),
        "max_retained_identity_bytes": int(retained_identity_bytes),
    }},
    "process": {"user_cpu_nanoseconds": 1, "system_cpu_nanoseconds": 1, "max_rss_kib": 1},
    "assembler": {"completed_transactions": 3},
    "joiner": {"released_phase_anchor_transactions": 1},
    "checker": {"consumed_transactions": 1},
    "reconciliation": {"evidence_reconciled": reconciled},
    "diagnostics": [],
}
with open(path, "x", encoding="utf-8") as output:
    json.dump(measurement, output, separators=(",", ":"))
    output.write("\n")
PY
  echo "monitor:measurement:$completion_status" >>"$EVENT_LOG"
  exit "$exit_status"
fi
SCRIPT

  cat >"$case_root/bin/loader" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
echo 'phase:loader' >>"$EVENT_LOG"
if [[ ${CONTAMINATE_MARKER:-0} == 1 ]]; then
  echo 1 >"$MARKER_ROWS_FILE"
fi
if [[ ${HANG_LOADER:-0} == 1 ]]; then
  sleep 30
fi
status=${LOADER_EXIT_STATUS:-0}
if [[ $status == 0 ]]; then
  python3 - <<'PY'
import json
import os

path = os.environ["BLACKBOX_LOADER_DECLARED_COUNTS_FILE"]
temporary = path + ".tmp"
document = {
    "kind": "loader_declaration",
    "benchmark": "ycsb",
    "loader_batch_size": int(os.environ["BLACKBOX_LOADER_BATCH_SIZE"]),
    "scale_parameter": None,
    "declared_table_row_counts": [
        {
            "table": "benchbase.usertable",
            "row_count": int(os.environ["LOADER_DECLARED_ROWS"]),
        }
    ],
}
with open(temporary, "x", encoding="utf-8") as output:
    json.dump(document, output, separators=(",", ":"))
    output.write("\n")
os.link(temporary, path)
os.unlink(temporary)
PY
fi
exit "$status"
SCRIPT

  cat >"$case_root/bin/workload" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
echo 'phase:workload' >>"$EVENT_LOG"
if [[ ${LEAVE_ACTIVE_CONNECTION:-0} == 1 ]]; then
  echo 1 >"$ACTIVE_CONNECTIONS_FILE"
fi
SCRIPT

  chmod +x "$case_root/bin/"*
}


line_of() {
  local pattern=$1
  local log=$2
  grep -n -m1 "^${pattern}$" "$log" | cut -d: -f1
}


run_case() {
  local name=$1
  local leave_active=$2
  local suppress_start_ack=${3:-0}
  local loader_exit_status=${4:-0}
  local emit_lifecycle_regression=${5:-0}
  local hang_loader=${6:-0}
  local contaminate_marker=${7:-0}
  local fail_fence_phase=${8:-}
  local processing_mode=${9:-audited}
  local capture_metrics=${10:-0}
  local retained_identity_bytes=${11:-}
  local pending_client_bytes=${12:-}
  local benchmark=${13:-ycsb}
  local loader_batch_size=${14:-128}
  local observed_loader_rows=${15:-0}
  local declared_loader_rows=${16:-0}
  local case_root="$TEST_ROOT/$name"
  mkdir -p "$case_root"
  write_test_programs "$case_root"
  : >"$case_root/events"
  echo 0 >"$case_root/active-connections"
  echo 0 >"$case_root/marker-rows"

  export EVENT_LOG="$case_root/events"
  export ACTIVE_CONNECTIONS_FILE="$case_root/active-connections"
  export REDIS_COUNTER_FILE="$case_root/redis-counter"
  export MARKER_ROWS_FILE="$case_root/marker-rows"
  export LEAVE_ACTIVE_CONNECTION=$leave_active
  export SUPPRESS_START_ACK=$suppress_start_ack
  export LOADER_EXIT_STATUS=$loader_exit_status
  export EMIT_LIFECYCLE_REGRESSION=$emit_lifecycle_regression
  export HANG_LOADER=$hang_loader
  export CONTAMINATE_MARKER=$contaminate_marker
  export FAIL_FENCE_PHASE=$fail_fence_phase
  export OBSERVED_LOADER_ROWS=$observed_loader_rows
  export LOADER_DECLARED_ROWS=$declared_loader_rows

  local metrics_path=
  local run_id_path=
  local bootstrap_manifest_path=
  if [[ $processing_mode == join-only || $capture_metrics == 1 ]]; then
    metrics_path="$case_root/monitor-metrics.json"
  fi
  if [[ $processing_mode == join-only ]]; then
    run_id_path="$case_root/run-id.txt"
    bootstrap_manifest_path="$case_root/bootstrap-manifest.json"
  fi

  local status
  if BLACKBOX_DOCKER="$case_root/bin/docker" \
      BLACKBOX_LIVE_LOCK_TARGET="$case_root" \
      BLACKBOX_REDIS_CLI="$case_root/bin/redis-cli" \
      BLACKBOX_MARIADB_CLI="$case_root/bin/mariadb" \
      BLACKBOX_REDIS_HOST=fake \
      BLACKBOX_REDIS_PORT=0 \
      BLACKBOX_DB_HOST=fake \
      BLACKBOX_DB_PORT=0 \
      BLACKBOX_MONITOR_COMMAND="$case_root/bin/monitor" \
      BLACKBOX_PROCESSING_MODE="$processing_mode" \
      BLACKBOX_MONITOR_METRICS_PATH="$metrics_path" \
      BLACKBOX_MONITOR_MAX_PENDING_CLIENT_BYTES="$pending_client_bytes" \
      BLACKBOX_MONITOR_MAX_RETAINED_IDENTITY_BYTES="$retained_identity_bytes" \
      BLACKBOX_BENCHMARK="$benchmark" \
      BLACKBOX_EXPECTED_REDIS_MAXMEMORY_BYTES="${EXPECTED_REDIS_MAXMEMORY_BYTES:-}" \
      BLACKBOX_RUN_ID_PATH="$run_id_path" \
      BLACKBOX_BOOTSTRAP_MANIFEST_PATH="$bootstrap_manifest_path" \
      BLACKBOX_LOADER_VALIDATION_PATH="$case_root/loader-validation.json" \
      BLACKBOX_LOADER_DECLARED_COUNTS_FILE="$case_root/loader-declaration.json" \
      BLACKBOX_LOADER_BATCH_SIZE="$loader_batch_size" \
      BLACKBOX_LOADER_COMMAND="$case_root/bin/loader" \
      BLACKBOX_WORKLOAD_COMMAND="$case_root/bin/workload" \
      BLACKBOX_EXPECTED_SESSION_IDS=ycsb/worker/1 \
      BLACKBOX_STACK_TIMEOUT_SECONDS=2 \
      BLACKBOX_STATUS_TIMEOUT_SECONDS=2 \
      BLACKBOX_QUIESCENCE_TIMEOUT_SECONDS=1 \
      BLACKBOX_LOADER_TIMEOUT_SECONDS=1 \
      BLACKBOX_WORKLOAD_TIMEOUT_SECONDS=2 \
      BLACKBOX_TERMINATION_GRACE_SECONDS=1 \
      "$ROOT/scripts/e2e.sh" >"$case_root/stdout" 2>"$case_root/stderr"; then
    status=0
  else
    status=$?
  fi

  CASE_STATUS=$status
  CASE_LOG="$case_root/events"
  CASE_ROOT=$case_root
}


run_case clean 0
[[ $CASE_STATUS == 0 ]] || fail "clean lifecycle exited $CASE_STATUS: $(cat "$TEST_ROOT/clean/stderr")"

manifest_line=$(line_of 'publish:run_manifest' "$CASE_LOG")
start_fence_line=$(line_of 'fence:RUN_START' "$CASE_LOG")
start_ack_line=$(line_of 'status:START_FENCE_SEEN' "$CASE_LOG")
loader_line=$(line_of 'phase:loader' "$CASE_LOG")
bootstrap_line=$(line_of 'publish:bootstrap_manifest' "$CASE_LOG")
load_fence_line=$(line_of 'fence:LOAD_FINISHED' "$CASE_LOG")
load_ack_line=$(line_of 'status:LOAD_FINISHED_SEEN' "$CASE_LOG")
workload_line=$(line_of 'phase:workload' "$CASE_LOG")
end_fence_line=$(line_of 'fence:END_FENCE' "$CASE_LOG")
final_line=$(line_of 'status:FINAL' "$CASE_LOG")

(( manifest_line < start_fence_line )) || fail 'RunManifest was not published before RUN_START'
(( start_fence_line < start_ack_line && start_ack_line < loader_line )) || fail 'loader bypassed START acknowledgement'
(( loader_line < bootstrap_line && bootstrap_line < load_fence_line )) || fail 'bootstrap manifest/fence order is wrong'
(( load_fence_line < load_ack_line && load_ack_line < workload_line )) || fail 'workload bypassed LOAD acknowledgement'
(( workload_line < end_fence_line && end_fence_line < final_line )) || fail 'END/final order is wrong'
[[ $(jq -r '.loader_batch_size' "$CASE_ROOT/loader-validation.json") == 128 ]] ||
  fail 'loader validation did not record the default batch size'
[[ $(jq -r '.benchmark' "$CASE_ROOT/loader-declaration.json") == ycsb ]] ||
  fail 'loader did not publish its declaration'
[[ $(jq -r '.validated' "$CASE_ROOT/loader-validation.json") == true ]] ||
  fail 'loader validation report is not validated'

run_case custom_loader_batch 0 0 0 0 0 0 '' audited 0 '' '' ycsb 64
[[ $CASE_STATUS == 0 ]] || fail 'custom loader batch lifecycle failed'
[[ $(jq -r '.loader_batch_size' "$CASE_ROOT/loader-validation.json") == 64 ]] ||
  fail 'loader validation did not record the selected batch size'
[[ $(jq -r '.loader_batch_size' "$CASE_ROOT/loader-declaration.json") == 64 ]] ||
  fail 'loader declaration did not record the selected batch size'

run_case loader_count_mismatch 0 0 0 0 0 0 '' audited 0 '' '' ycsb 128 1 0
[[ $CASE_STATUS == 1 ]] || fail 'loader SQL/declaration mismatch unexpectedly succeeded'
grep -q '^publish:run_failed$' "$CASE_LOG" ||
  fail 'loader SQL/declaration mismatch lacked a failure record'
if grep -q '^fence:LOAD_FINISHED$' "$CASE_LOG"; then
  fail 'loader SQL/declaration mismatch advanced to LOAD_FINISHED'
fi

run_case canonical_scan_count_mismatch 0 0 0 0 0 0 '' audited 0 '' '' ycsb 128 1 1
[[ $CASE_STATUS == 1 ]] || fail 'canonical scan/count mismatch unexpectedly succeeded'
grep -q '^publish:run_failed$' "$CASE_LOG" ||
  fail 'canonical scan/count mismatch lacked a failure record'
if grep -q '^fence:LOAD_FINISHED$' "$CASE_LOG"; then
  fail 'canonical scan/count mismatch advanced to LOAD_FINISHED'
fi

run_case tpcc_runtime_gate 0 0 0 0 0 0 '' audited 0 '' '' tpcc
[[ $CASE_STATUS != 0 ]] || fail 'incomplete TPC-C runtime unexpectedly entered the lifecycle'
if grep -q '^stack:database$' "$CASE_LOG"; then
  fail 'incomplete TPC-C runtime performed destructive stack startup'
fi
grep -q 'adapter tpcc runtime is incomplete' "$CASE_ROOT/stderr" ||
  fail 'incomplete TPC-C runtime did not report its capability gate'

run_case audited_metrics 0 0 0 0 0 0 '' audited 1
[[ $CASE_STATUS == 0 ]] ||
  fail "audited metrics lifecycle exited $CASE_STATUS: $(cat "$CASE_ROOT/stderr")"
grep -q '^status:FINAL$' "$CASE_LOG" ||
  fail 'audited metrics lifecycle did not observe FINAL'
grep -q '^monitor:measurement:completed$' "$CASE_LOG" ||
  fail 'audited metrics lifecycle did not wait for monitor measurement output'
audited_final_line=$(line_of 'status:FINAL' "$CASE_LOG")
audited_measurement_line=$(line_of 'monitor:measurement:completed' "$CASE_LOG")
(( audited_final_line < audited_measurement_line )) ||
  fail 'audited measurement was not written after FINAL publication'
[[ $(jq -r '.outcome' "$CASE_ROOT/monitor-metrics.json") == PASS ]] ||
  fail 'audited measurement did not preserve the terminal outcome'

run_case join_only_clean 0 0 0 0 0 0 '' join-only
[[ $CASE_STATUS == 0 ]] ||
  fail "clean join-only lifecycle exited $CASE_STATUS: $(cat "$CASE_ROOT/stderr")"
grep -q '^monitor:mode:join-only$' "$CASE_LOG" ||
  fail 'join-only coordinator did not pass the monitor processing mode'
grep -q '^monitor:measurement:completed$' "$CASE_LOG" ||
  fail 'join-only coordinator did not wait for the completed measurement'
if grep -q '^status:FINAL$' "$CASE_LOG"; then
  fail 'join-only coordinator waited for a terminal status record'
fi
join_end_line=$(line_of 'fence:END_FENCE' "$CASE_LOG")
join_measurement_line=$(line_of 'monitor:measurement:completed' "$CASE_LOG")
(( join_end_line < join_measurement_line )) ||
  fail 'join-only monitor completed before END_FENCE'
join_run_id=$(cat "$CASE_ROOT/run-id.txt")
[[ $(jq -r '.run_id' "$CASE_ROOT/monitor-metrics.json") == "$join_run_id" ]] ||
  fail 'run-id provenance does not match the measurement'
[[ $(jq -r '.run_id' "$CASE_ROOT/bootstrap-manifest.json") == "$join_run_id" ]] ||
  fail 'BootstrapManifest provenance does not match the run ID'
if grep -Eq 'PASS|VIOLATION|INVALID' "$CASE_ROOT/stdout" "$CASE_ROOT/stderr"; then
  fail 'join-only lifecycle output implied an audited result'
fi

export EXPECTED_REDIS_MAXMEMORY_BYTES=8589934592
run_case capacity_override 0 0 0 0 0 0 '' join-only 0 2147483648 1073741824
unset EXPECTED_REDIS_MAXMEMORY_BYTES
[[ $CASE_STATUS == 0 ]] ||
  fail "capacity override lifecycle exited $CASE_STATUS: $(cat "$CASE_ROOT/stderr")"
grep -q '^monitor:pending-client-bytes:1073741824$' "$CASE_LOG" ||
  fail 'coordinator did not pass the pending-client byte override'
grep -q '^monitor:retained-identity-bytes:2147483648$' "$CASE_LOG" ||
  fail 'coordinator did not pass the retained-identity byte override'
[[ $(jq -r '.limits.joiner.max_pending_client_bytes' \
  "$CASE_ROOT/monitor-metrics.json") == 1073741824 ]] ||
  fail 'pending-client capacity override is absent from monitor provenance'
[[ $(jq -r '.limits.joiner.max_retained_identity_bytes' \
  "$CASE_ROOT/monitor-metrics.json") == 2147483648 ]] ||
  fail 'capacity override is absent from monitor provenance'

run_case join_only_loader_failure 0 0 9 0 0 0 '' join-only
[[ $CASE_STATUS == 1 ]] ||
  fail 'join-only loader failure did not fail operationally'
grep -q '^monitor:measurement:failed$' "$CASE_LOG" ||
  fail 'join-only failure did not produce a failed measurement'
if grep -q '^status:FINAL$' "$CASE_LOG"; then
  fail 'join-only failure waited for a terminal status record'
fi
[[ $(jq -r '.completion_status' "$CASE_ROOT/monitor-metrics.json") == failed ]] ||
  fail 'join-only failure artifact has the wrong completion status'
if grep -Eq 'PASS|VIOLATION|INVALID' "$CASE_ROOT/stdout" "$CASE_ROOT/stderr"; then
  fail 'failed join-only lifecycle output implied an audited result'
fi

run_case active_connection 1
[[ $CASE_STATUS != 0 ]] || fail 'unproven quiescence unexpectedly succeeded'
if grep -q '^fence:END_FENCE$' "$CASE_LOG"; then
  fail 'END_FENCE was written without proven client quiescence'
fi
grep -q '^publish:run_failed$' "$CASE_LOG" || fail 'quiescence failure did not publish a diagnostic'
grep -q '^monitor:stop$' "$CASE_LOG" || fail 'quiescence failure did not stop the monitor'
if grep -q '^status:FINAL$' "$CASE_LOG"; then
  fail 'quiescence failure incorrectly awaited a terminal monitor result'
fi
grep -q 'timeout-invalid outcome=INVALID' "$TEST_ROOT/active_connection/stderr" ||
  fail 'quiescence failure did not report timeout-invalid'

run_case ambiguous_run_start 0 0 0 0 0 0 RUN_START
[[ $CASE_STATUS == 1 ]] || fail 'ambiguous RUN_START did not fail operationally'
grep -q '^fence_failed:RUN_START$' "$CASE_LOG" || fail 'RUN_START failure was not injected'
grep -q '^publish:run_failed$' "$CASE_LOG" || fail 'ambiguous RUN_START lacked a diagnostic'
grep -q '^monitor:stop$' "$CASE_LOG" || fail 'ambiguous RUN_START did not stop the monitor'
if grep -q '^status:FINAL$\|^fence:END_FENCE$' "$CASE_LOG"; then
  fail 'ambiguous RUN_START attempted audited closure'
fi

run_case ambiguous_load_fence 0 0 0 0 0 0 LOAD_FINISHED
[[ $CASE_STATUS == 1 ]] || fail 'ambiguous LOAD_FINISHED did not fail operationally'
grep -q '^fence_failed:LOAD_FINISHED$' "$CASE_LOG" || fail 'LOAD_FINISHED failure was not injected'
grep -q '^monitor:stop$' "$CASE_LOG" || fail 'ambiguous LOAD_FINISHED did not stop the monitor'
if grep -q '^status:FINAL$\|^fence:END_FENCE$' "$CASE_LOG"; then
  fail 'ambiguous LOAD_FINISHED attempted audited closure'
fi

run_case ambiguous_end_fence 0 0 0 0 0 0 END_FENCE
[[ $CASE_STATUS == 1 ]] || fail 'ambiguous END_FENCE did not fail operationally'
grep -q '^fence_failed:END_FENCE$' "$CASE_LOG" || fail 'END_FENCE failure was not injected'
grep -q '^monitor:stop$' "$CASE_LOG" || fail 'ambiguous END_FENCE did not stop the monitor'
if grep -q '^status:FINAL$' "$CASE_LOG"; then
  fail 'ambiguous END_FENCE incorrectly awaited a terminal monitor result'
fi

run_case missing_start_ack 0 1
[[ $CASE_STATUS != 0 ]] || fail 'missing START acknowledgement unexpectedly succeeded'
if grep -q '^phase:loader$' "$CASE_LOG"; then
  fail 'loader ran without START_FENCE_SEEN acknowledgement'
fi
grep -q '^publish:run_failed$' "$CASE_LOG" || fail 'missing START acknowledgement did not publish a diagnostic'
grep -q '^fence:END_FENCE$' "$CASE_LOG" || fail 'proven-quiescent START failure did not close with END_FENCE'
grep -q '^status:FINAL$' "$CASE_LOG" || fail 'missing START acknowledgement did not await final INVALID'

run_case loader_failure 0 0 9
[[ $CASE_STATUS == 1 ]] || fail "loader failure did not map child exit 9 to operational exit 1"
grep -q '^publish:run_failed$' "$CASE_LOG" || fail 'loader failure did not publish a diagnostic'
grep -q '^fence:END_FENCE$' "$CASE_LOG" || fail 'quiescent loader failure did not close with END_FENCE'
grep -q '^status:FINAL$' "$CASE_LOG" || fail 'loader failure did not await final INVALID'
if grep -q '^fence:LOAD_FINISHED$\|^phase:workload$' "$CASE_LOG"; then
  fail 'loader failure advanced into LOAD_FINISHED/workload'
fi
[[ ! -e $CASE_ROOT/loader-declaration.json ]] ||
  fail 'failed loader published a declaration'

run_case lifecycle_regression 0 0 0 1
[[ $CASE_STATUS == 1 ]] || fail 'lifecycle regression did not fail operationally'
grep -q '^publish:run_failed$' "$CASE_LOG" || fail 'lifecycle regression did not publish a diagnostic'
if grep -q '^phase:workload$' "$CASE_LOG"; then
  fail 'workload ran after a monitor lifecycle regression'
fi

run_case hung_loader 0 0 0 0 1
[[ $CASE_STATUS == 1 ]] || fail 'hung loader did not fail operationally'
grep -q '^publish:run_failed$' "$CASE_LOG" || fail 'hung loader did not publish a diagnostic'
grep -q '^fence:END_FENCE$' "$CASE_LOG" || fail 'terminated/quiescent loader did not close with END_FENCE'
grep -q '^status:FINAL$' "$CASE_LOG" || fail 'hung loader did not await final INVALID'

run_case loader_marker_contamination 0 0 0 0 0 1
[[ $CASE_STATUS == 1 ]] || fail 'loader marker contamination unexpectedly succeeded'
grep -q '^publish:run_failed$' "$CASE_LOG" || fail 'loader marker contamination lacked a diagnostic'
grep -q '^fence:END_FENCE$' "$CASE_LOG" || fail 'quiescent marker contamination did not close with END_FENCE'
if grep -q '^fence:LOAD_FINISHED$\|^phase:workload$' "$CASE_LOG"; then
  fail 'loader marker contamination advanced into LOAD_FINISHED/workload'
fi

echo 'lifecycle tests passed'
