#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORKSPACE=$(cd "$ROOT/.." && pwd)
RUNNER="$ROOT/scripts/run_blackbox_performance.sh"
EXPECTED_HEAD=2224eeca0d81565e762e584a9214569a28205e3d
TEST_ROOT=$(mktemp -d /media/newhd/yifan/db-monitoring-builds/blackbox-performance-harness-test.XXXXXX)
BASELINE="$TEST_ROOT/baseline"


cleanup() {
  git -C "$WORKSPACE/benchbase" worktree remove --force "$BASELINE" >/dev/null 2>&1 || true
  rm -rf -- "$TEST_ROOT"
}
trap cleanup EXIT


fail() {
  printf 'performance harness test failed: %s\n' "$*" >&2
  exit 1
}


git -C "$WORKSPACE/benchbase" worktree add --detach "$BASELINE" "$EXPECTED_HEAD" >/dev/null

mkdir -p "$TEST_ROOT/mock-bin"
cat >"$TEST_ROOT/mock-bin/docker" <<'SCRIPT'
#!/usr/bin/env bash
: >"$UNEXPECTED_DOCKER_CALL"
exit 99
SCRIPT
chmod +x "$TEST_ROOT/mock-bin/docker"
UNEXPECTED_DOCKER_CALL="$TEST_ROOT/unexpected-docker-call" \
PATH="$TEST_ROOT/mock-bin:$PATH" \
  "$RUNNER" --print-stage-order --repetitions 1 >/dev/null
[[ ! -e $TEST_ROOT/unexpected-docker-call ]] ||
  fail "non-running validation path attempted Compose cleanup without the live lock"

actual_order=$(
  "$RUNNER" --print-stage-order --repetitions 3 --seed 20260818
)
expected_order=$(cat <<'EOF'
repetition,stage_order_position,stage
1,1,benchbase_uninstrumented
1,2,read_capture_encoding
1,3,read_capture_encoding_redis
1,4,cdc_capture
1,5,assembly_join
1,6,assembly_join_check
2,1,read_capture_encoding
2,2,read_capture_encoding_redis
2,3,cdc_capture
2,4,assembly_join
2,5,assembly_join_check
2,6,benchbase_uninstrumented
3,1,read_capture_encoding_redis
3,2,cdc_capture
3,3,assembly_join
3,4,assembly_join_check
3,5,benchbase_uninstrumented
3,6,read_capture_encoding
EOF
)
[[ $actual_order == "$expected_order" ]] || fail "deterministic rotated stage order differs"

"$RUNNER" --smoke --verify-inputs-only \
  --baseline-root "$BASELINE" --output-root "$TEST_ROOT/empty-output" >/dev/null

mkdir -p "$TEST_ROOT/nonempty-output"
: >"$TEST_ROOT/nonempty-output/existing"
if "$RUNNER" --smoke --verify-inputs-only \
    --baseline-root "$BASELINE" --output-root "$TEST_ROOT/nonempty-output" \
    >/dev/null 2>&1; then
  fail "nonempty output root was accepted"
fi

: >"$BASELINE/untracked"
if "$RUNNER" --smoke --verify-inputs-only \
    --baseline-root "$BASELINE" --output-root "$TEST_ROOT/another-output" \
    >/dev/null 2>&1; then
  fail "dirty baseline worktree was accepted"
fi
rm -- "$BASELINE/untracked"

if "$RUNNER" --smoke --verify-inputs-only \
    --baseline-root "$WORKSPACE/benchbase" --output-root "$TEST_ROOT/current-output" \
    >/dev/null 2>&1; then
  fail "current BenchBase worktree was accepted as the baseline"
fi

BLACKBOX_REDIS_PORT=6399 "$ROOT/scripts/run_measured_command.sh" \
  --resource-output "$TEST_ROOT/resources.csv" \
  --network-output "$TEST_ROOT/network.json" \
  --sample-duration-seconds 1 \
  --sample-interval-seconds 0.1 \
  -- bash -c 'sleep 0.2'
jq -e \
  '.available == false and .input_bytes_delta == null and .output_bytes_delta == null' \
  "$TEST_ROOT/network.json" >/dev/null ||
  fail "absent Redis was represented as a zero-byte measurement"

printf 'performance harness validation tests passed\n'
