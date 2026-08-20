#!/usr/bin/env bash
# Fast option/rotation checks for the saturated monitor performance runner.

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
RUNNER="$ROOT/scripts/run_blackbox_monitor_performance.sh"
PLACEHOLDER_CORPUS=/does/not/need/to/exist-for-order-output
PLACEHOLDER_OUTPUT=/media/newhd/yifan/db-monitoring-builds/monitor-order-test
TEST_COMMAND_FILE=$(mktemp /media/newhd/yifan/db-monitoring-builds/monitor-order-command.XXXXXX)
trap 'rm -f -- "$TEST_COMMAND_FILE"' EXIT

fail() {
  printf 'monitor performance harness test failed: %s\n' "$*" >&2
  exit 1
}

actual=$(
  "$RUNNER" --corpus-dir "$PLACEHOLDER_CORPUS" --output-root "$PLACEHOLDER_OUTPUT" \
    --repetitions 3 --seed 0 --print-stage-order
)
expected='repetition,stage_order_position,stage
1,1,assembly_join
1,2,assembly_join_check
2,1,assembly_join_check
2,2,assembly_join
3,1,assembly_join
3,2,assembly_join_check'
[[ $actual == "$expected" ]] || fail "default two-stage rotation differs"

actual=$(
  "$RUNNER" --corpus-dir "$PLACEHOLDER_CORPUS" --output-root "$PLACEHOLDER_OUTPUT" \
    --repetitions 2 --seed 9 --stage assembly_join_check --print-stage-order
)
expected='repetition,stage_order_position,stage
1,1,assembly_join_check
2,1,assembly_join_check'
[[ $actual == "$expected" ]] || fail "single-stage selection differs"

actual=$(
  "$RUNNER" --corpus-dir "$PLACEHOLDER_CORPUS" --output-root "$PLACEHOLDER_OUTPUT" \
    --repetitions 1 --seed 0 --stage assembly_join_check --stage assembly_join \
    --print-stage-order
)
expected='repetition,stage_order_position,stage
1,1,assembly_join
1,2,assembly_join_check'
[[ $actual == "$expected" ]] || fail "selected stages were not canonicalized"

if "$RUNNER" --corpus-dir "$PLACEHOLDER_CORPUS" --output-root "$PLACEHOLDER_OUTPUT" \
  --stage unknown --print-stage-order >/dev/null 2>&1; then
  fail "unknown stage was accepted"
fi
if "$RUNNER" --corpus-dir "$PLACEHOLDER_CORPUS" --output-root "$PLACEHOLDER_OUTPUT" \
  --stage assembly_join --stage assembly_join --print-stage-order >/dev/null 2>&1; then
  fail "duplicate stage was accepted"
fi
if "$RUNNER" --corpus-dir "$PLACEHOLDER_CORPUS" --output-root "$PLACEHOLDER_OUTPUT" \
  --repetitions 0 --print-stage-order >/dev/null 2>&1; then
  fail "zero repetitions were accepted"
fi
if "$RUNNER" --corpus-dir "$PLACEHOLDER_CORPUS" --output-root "$PLACEHOLDER_OUTPUT" \
  --output-max-bytes 107374182401 --print-stage-order >/dev/null 2>&1; then
  fail "output limit above 100 GiB was accepted"
fi

"$RUNNER" --help 2>&1 | grep -q -- '--expected-duration-seconds' ||
  fail "help omits the source-duration contract"

# shellcheck source=scripts/run_blackbox_monitor_performance.sh
. "$RUNNER"
trap 'rm -f -- "$TEST_COMMAND_FILE"' EXIT
redis_command() {
  printf '%s\n' "$*" >"$TEST_COMMAND_FILE"
  printf '[["20-1",["payload","value"]]]\n'
}
last_id=$(redis_stream_id last example)
[[ $last_id == 20-1 ]] || fail "last stream ID parsing differs"
[[ $(<"$TEST_COMMAND_FILE") == '--json XREVRANGE example + - COUNT 1' ]] ||
  fail "XREVRANGE did not use the required + - bounds"

GUARD_ROOT=$(mktemp -d /media/newhd/yifan/db-monitoring-builds/monitor-output-guard.XXXXXX)
set +e
# shellcheck disable=SC2016
timeout --foreground --signal=TERM --kill-after=2 10 bash -c '
  set -Eeuo pipefail
  source "$1"
  OUTPUT_ROOT=$2
  OUTPUT_MAX_BYTES=1
  mkdir -p "$OUTPUT_ROOT"
  start_output_guard
  printf oversized >"$OUTPUT_ROOT/generated"
  sleep 10
' _ "$RUNNER" "$GUARD_ROOT" >/dev/null 2>&1
guard_status=$?
set -e
[[ $guard_status == 143 ]] || fail "output guard exited $guard_status instead of 143"
[[ -s $GUARD_ROOT/.output-size-limit-exceeded ]] ||
  fail "output guard did not preserve limit evidence"
rm -rf -- "$GUARD_ROOT"

STARTUP_LOG=$(mktemp /media/newhd/yifan/db-monitoring-builds/monitor-startup-cleanup.XXXXXX)
set +e
# shellcheck disable=SC2016
bash -c '
  set -Eeuo pipefail
  source "$1"
  STARTUP_LOG_PATH=$2
  LIVE_LOCK_ACQUIRED=1
  STACK_MAY_BE_RUNNING=0
  OUTPUT_ROOT=$(dirname "$2")
  ACTIVE_TRIAL_DIR=
  compose() {
    printf "%s\n" "$*" >>"$STARTUP_LOG_PATH"
    [[ $1 != up ]]
  }
  start_fresh_redis
' _ "$RUNNER" "$STARTUP_LOG"
startup_status=$?
set -e
[[ $startup_status != 0 ]] || fail "partial Redis startup unexpectedly succeeded"
[[ $(grep -c '^down --volumes --remove-orphans$' "$STARTUP_LOG") == 2 ]] ||
  fail "partial Redis startup was not removed by EXIT cleanup"
grep -q '^up --detach --force-recreate redis$' "$STARTUP_LOG" ||
  fail "partial Redis startup path was not exercised"
rm -f -- "$STARTUP_LOG"

printf 'monitor performance harness option tests passed\n'
