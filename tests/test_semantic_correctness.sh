#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
COMPOSE=(docker compose --file "$ROOT/docker-compose.yml" --profile mariadb)
CLIENT="$ROOT/tests/semantic_client.sh"
OUTPUT_ROOT=${BLACKBOX_CORRECTNESS_OUTPUT_ROOT:-/tmp}
SUCCESS=0

: "${BLACKBOX_MONITOR_BIN:?set BLACKBOX_MONITOR_BIN to blackbox_run_monitor}"
: "${BLACKBOX_BENCHBASE_BUILD_DIR:?set BLACKBOX_BENCHBASE_BUILD_DIR}"
[[ -x $BLACKBOX_MONITOR_BIN ]]
[[ -x $CLIENT ]]
mkdir -p "$OUTPUT_ROOT"
RUN_OUTPUT=$(mktemp -d "$OUTPUT_ROOT/blackbox-correctness.XXXXXX")


cleanup() {
  "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  if (( SUCCESS == 1 )) && [[ ${BLACKBOX_KEEP_CORRECTNESS_OUTPUT:-0} != 1 ]]; then
    rm -rf -- "$RUN_OUTPUT"
  else
    printf 'semantic run output retained at %s\n' "$RUN_OUTPUT" >&2
  fi
}
trap cleanup EXIT


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
        if (value.get("kind") == "status" and value.get("lifecycle_state") == "FINAL"):
            records.append(value)
if len(records) != 1:
    raise SystemExit(f"expected exactly one final status, found {len(records)}")
print(json.dumps(records[0], separators=(",", ":"), sort_keys=True))
PY
}


capture_client_stream() {
  local run_id=$1
  local output_file=$2
  redis-cli \
    -h "${BLACKBOX_REDIS_HOST:-host.docker.internal}" \
    -p "${BLACKBOX_REDIS_PORT:-6379}" \
    --json XRANGE "blackbox:client:$run_id" - + >"$output_file"
}


assert_client_reads() {
  local input_file=$1
  local expected_first=$2
  local expected_second=$3
  local dropped=$4
  python3 - "$input_file" "$expected_first" "$expected_second" "$dropped" <<'PY'
import base64
import json
import struct
import sys

entries = json.load(open(sys.argv[1], encoding="utf-8"))
records = []
for _, fields in entries:
    if len(fields) != 2 or fields[0] != "payload":
        raise SystemExit("client stream entry does not contain one payload field")
    records.append(json.loads(fields[1]))

clients = [record for record in records if record.get("kind") == "client_txn"]
reader = [record for record in clients if record.get("session_id") == "ycsb/worker/1"]
done = [record for record in records if record.get("kind") == "session_done"]
if sorted(record["session_id"] for record in done) != ["ycsb/worker/1", "ycsb/worker/2"]:
    raise SystemExit("both probe sessions must publish SessionDone")

if sys.argv[4] == "true":
    if reader:
        raise SystemExit("the injected dropped reader ClientTxn is still present")
    if len(clients) != 1:
        raise SystemExit(f"expected one retained ClientTxn, found {len(clients)}")
    raise SystemExit(0)

if len(reader) != 1:
    raise SystemExit(f"expected one reader ClientTxn, found {len(reader)}")
record = reader[0]
if record.get("txn_type") != "si_fractured_read_probe" or record.get("read_count") != 2:
    raise SystemExit("reader transaction has the wrong type or read count")
reads = record["reads"]

def canonical_key(key):
    def lp(value):
        return struct.pack(">I", len(value)) + value
    return lp(b"benchbase") + lp(b"usertable") + struct.pack(">i", key)

for event_seq, (read, key) in enumerate(zip(reads, (1, 2), strict=True)):
    if read.get("event_seq") != event_seq:
        raise SystemExit("probe read event sequence is not zero-based and dense")
    if read.get("source") != "snapshot" or read.get("value_state") != "present":
        raise SystemExit("probe reads must be present snapshot observations")
    observed_key = base64.b64decode(read["key_bytes_base64"], validate=True)
    if observed_key != canonical_key(key):
        raise SystemExit(f"read {event_seq} did not capture canonical key {key}")

def first_field(encoded):
    raw = base64.b64decode(encoded, validate=True)
    if not raw or raw[0] != 1 or len(raw) < 5:
        raise SystemExit("expected a present canonical first field")
    length = struct.unpack(">I", raw[1:5])[0]
    end = 5 + length
    if end > len(raw):
        raise SystemExit("truncated canonical first field")
    return raw[5:end].decode("utf-8")

observed = [first_field(read["value_bytes_base64"]) for read in reads]
expected = [sys.argv[2], sys.argv[3]]
if observed != expected:
    raise SystemExit(f"reader values {observed!r} differ from expected {expected!r}")
PY
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
        all(.diagnostics[]; .code != "CDC_ORDER_INVALID") and
        any(.diagnostics[];
          .category == "PROTOCOL" and
          (.code == "CLIENT_HALF_MISSING" or .code == "SESSION_DONE_MISMATCH"))
      ' <<<"$final_status" >/dev/null
      ;;
    *) return 1 ;;
  esac
}


run_case() {
  local case_name=$1
  local isolation=$2
  local mode=$3
  local drop_session=$4
  local expected_outcome=$5
  local expected_exit=$6
  local expected_first=$7
  local expected_second=$8
  local log_file="$RUN_OUTPUT/$case_name.log"
  local client_file="$RUN_OUTPUT/$case_name-client.json"
  local loader_command workload_command
  printf -v loader_command '%q load' "$CLIENT"
  printf -v workload_command '%q run' "$CLIENT"

  printf 'running semantic case %s (isolation=%s mode=%s)\n' \
    "$case_name" "$isolation" "$mode"
  local status
  set +e
  BLACKBOX_DECLARED_ISOLATION=$isolation \
  BLACKBOX_EXPECTED_SESSION_IDS=ycsb/worker/1,ycsb/worker/2 \
  BLACKBOX_LOADER_COMMAND=$loader_command \
  BLACKBOX_WORKLOAD_COMMAND=$workload_command \
  BLACKBOX_BENCHBASE_BUILD_DIR=$BLACKBOX_BENCHBASE_BUILD_DIR \
  BLACKBOX_PROBE_MODE=$mode \
  BLACKBOX_DROP_CLIENT_SESSION=$drop_session \
  BLACKBOX_PROBE_TIMEOUT_SECONDS=30 \
  BLACKBOX_STACK_TIMEOUT_SECONDS=180 \
  BLACKBOX_LOADER_TIMEOUT_SECONDS=30 \
  BLACKBOX_WORKLOAD_TIMEOUT_SECONDS=60 \
  BLACKBOX_STATUS_TIMEOUT_SECONDS=120 \
  BLACKBOX_MONITOR_BIN=$BLACKBOX_MONITOR_BIN \
    "$ROOT/scripts/e2e.sh" >"$log_file" 2>&1
  status=$?
  set -e

  if (( status != expected_exit )); then
    sed -n '1,260p' "$log_file" >&2
    echo "$case_name exited $status, expected $expected_exit" >&2
    return 1
  fi

  local final_status run_id
  final_status=$(extract_final_status "$log_file")
  assert_terminal_status "$final_status" "$expected_outcome"
  run_id=$(jq -er '.run_id' <<<"$final_status")
  capture_client_stream "$run_id" "$client_file"
  if [[ -n $drop_session ]]; then
    assert_client_reads "$client_file" "$expected_first" "$expected_second" true
  else
    assert_client_reads "$client_file" "$expected_first" "$expected_second" false
  fi

  local final_count
  final_count=$(redis-cli \
    -h "${BLACKBOX_REDIS_HOST:-host.docker.internal}" \
    -p "${BLACKBOX_REDIS_PORT:-6379}" \
    --json XRANGE "blackbox:status:$run_id" - + |
    jq '[.[] | (.[1][1] | fromjson) | select(.lifecycle_state == "FINAL")] | length')
  [[ $final_count == 1 ]]

  printf 'semantic case %s: outcome=%s run_id=%s reader=%s/%s\n' \
    "$case_name" "$expected_outcome" "$run_id" "$expected_first" "$expected_second"
}


run_case clean_repeatable_read REPEATABLE_READ clean '' PASS 0 old old
run_case fractured_read_committed READ_COMMITTED fractured '' VIOLATION 3 old new
run_case fractured_repeatable_read REPEATABLE_READ fractured '' PASS 0 old old
run_case dropped_client_record REPEATABLE_READ clean ycsb/worker/1 INVALID 4 old old

SUCCESS=1
printf 'all semantic correctness cases passed\n'
