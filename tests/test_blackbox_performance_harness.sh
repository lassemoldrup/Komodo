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

default_redis_command=$(docker compose --file "$ROOT/docker-compose.yml" \
  --profile mariadb config --format json | jq -c '.services.redis.command')
[[ $default_redis_command == *'"--maxmemory","2147483648"'* ]] ||
  fail 'ordinary Compose Redis capacity is not the 2 GiB default'
override_redis_command=$(BLACKBOX_REDIS_MAXMEMORY_BYTES=8589934592 \
  docker compose --file "$ROOT/docker-compose.yml" --profile mariadb \
    config --format json | jq -c '.services.redis.command')
[[ $override_redis_command == *'"--maxmemory","8589934592"'* ]] ||
  fail 'Compose Redis capacity override was not resolved'

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

single_stage_order=$(
  "$RUNNER" --print-stage-order --repetitions 2 --seed 20260818 \
    --stage cdc_capture
)
expected_single_stage_order=$(cat <<'EOF'
repetition,stage_order_position,stage
1,1,cdc_capture
2,1,cdc_capture
EOF
)
[[ $single_stage_order == "$expected_single_stage_order" ]] ||
  fail "single-stage selection differs"

two_stage_order=$(
  "$RUNNER" --print-stage-order --repetitions 2 --seed 20260818 \
    --stage assembly_join_check --stage assembly_join
)
expected_two_stage_order=$(cat <<'EOF'
repetition,stage_order_position,stage
1,1,assembly_join
1,2,assembly_join_check
2,1,assembly_join_check
2,2,assembly_join
EOF
)
[[ $two_stage_order == "$expected_two_stage_order" ]] ||
  fail "two-stage selection did not use canonical-order rotation"

tpcc_order=$(
  "$RUNNER" --benchmark tpcc --print-stage-order --repetitions 1 --seed 3
)
[[ $(grep -c '^1,' <<<"$tpcc_order") == 6 ]] ||
  fail 'TPC-C stage order did not retain all six canonical profiles'

config_generator="$TEST_ROOT/generate-configs.py"
awk '
  /^generate_configs\(\)/ { in_function=1 }
  in_function && /<<'\''PY'\''$/ { capture=1; next }
  capture && /^PY$/ { exit }
  capture { print }
' "$RUNNER" >"$config_generator"
python3 "$config_generator" \
  "$TEST_ROOT/tpcc-disabled.xml" "$TEST_ROOT/tpcc-audited.xml" \
  4 1 32 1 128 0 2 3 tpcc 45,43,4,4,4 unlimited 2000-01-01T00:00:00Z
grep -F "<mode>\${env:BLACKBOX_MEASUREMENT_MODE}</mode>" \
  "$TEST_ROOT/tpcc-audited.xml" >/dev/null ||
  fail 'generated TPC-C configuration lost the measurement-mode placeholder'
grep -F "<runId>\${env:BLACKBOX_ACTIVE_RUN_ID}</runId>" \
  "$TEST_ROOT/tpcc-disabled.xml" >/dev/null ||
  fail 'generated compatibility configuration lost the run-id placeholder'
grep -F '<benchmark>tpcc</benchmark>' "$TEST_ROOT/tpcc-disabled.xml" >/dev/null ||
  fail 'generated compatibility configuration selected the wrong adapter'
for generated_config in "$TEST_ROOT/tpcc-disabled.xml" "$TEST_ROOT/tpcc-audited.xml"; do
  grep -F '<loaderTimestamp>2000-01-01T00:00:00Z</loaderTimestamp>' \
    "$generated_config" >/dev/null ||
    fail "generated configuration did not record the fixed loader timestamp: $generated_config"
done

for seed in -2147483648 2147483647; do
  "$RUNNER" --benchmark tpcc --print-stage-order --repetitions 1 --seed "$seed" \
    >/dev/null || fail "signed 32-bit seed $seed was rejected"
done
for seed in -2147483649 2147483648 01; do
  if "$RUNNER" --benchmark tpcc --print-stage-order --seed "$seed" >/dev/null 2>&1; then
    fail "invalid BenchBase seed $seed was accepted"
  fi
done
if "$RUNNER" --benchmark tpcc --print-stage-order --mix 45,43,4,4 \
    >/dev/null 2>&1; then
  fail 'TPC-C mix with four weights was accepted'
fi
if "$RUNNER" --benchmark tpcc --print-stage-order --mix 45,43,4,4,5 \
    >/dev/null 2>&1; then
  fail 'TPC-C mix not summing to 100 was accepted'
fi
if "$RUNNER" --benchmark unknown --print-stage-order >/dev/null 2>&1; then
  fail 'unknown benchmark was accepted'
fi
if "$RUNNER" --benchmark tpcc --print-stage-order --scale-factor 1 >/dev/null 2>&1; then
  fail 'TPC-C accepted the YCSB scale-factor option'
fi
if "$RUNNER" --benchmark ycsb --print-stage-order --warehouses 1 >/dev/null 2>&1; then
  fail 'YCSB accepted the TPC-C warehouse option'
fi
"$RUNNER" --print-stage-order \
  --loader-timestamp-utc 2024-02-29T12:34:56Z >/dev/null ||
  fail 'valid canonical loader timestamp was rejected'
for loader_timestamp in \
  2024-02-29T12:34:56+00:00 \
  2024-02-29T12:34:56.000Z \
  2024-2-29T12:34:56Z \
  2023-02-29T12:34:56Z \
  '2024-02-29T12:34:56Z '; do
  if "$RUNNER" --print-stage-order --loader-timestamp-utc "$loader_timestamp" \
      >/dev/null 2>&1; then
    fail "noncanonical loader timestamp was accepted: $loader_timestamp"
  fi
done

if "$RUNNER" --print-stage-order --stage unknown_stage \
    >"$TEST_ROOT/unknown-stage.stdout" 2>"$TEST_ROOT/unknown-stage.stderr"; then
  fail "unknown stage was accepted"
fi
grep -F 'unknown stage: unknown_stage' "$TEST_ROOT/unknown-stage.stderr" >/dev/null ||
  fail "unknown stage diagnostic is unclear"

if "$RUNNER" --print-stage-order --stage cdc_capture --stage cdc_capture \
    >"$TEST_ROOT/duplicate-stage.stdout" 2>"$TEST_ROOT/duplicate-stage.stderr"; then
  fail "duplicate stage was accepted"
fi
grep -F 'duplicate stage: cdc_capture' "$TEST_ROOT/duplicate-stage.stderr" >/dev/null ||
  fail "duplicate stage diagnostic is unclear"

if "$RUNNER" --print-stage-order --stage \
    >"$TEST_ROOT/missing-stage.stdout" 2>"$TEST_ROOT/missing-stage.stderr"; then
  fail "missing stage value was accepted"
fi
grep -F 'missing --stage value' "$TEST_ROOT/missing-stage.stderr" >/dev/null ||
  fail "missing stage value diagnostic is unclear"

verified=$(
  "$RUNNER" --smoke --verify-inputs-only \
    --baseline-root "$BASELINE" --output-root "$TEST_ROOT/empty-output"
)
[[ $verified == *'redis_maxmemory_bytes=8589934592'* &&
      $verified == *'loader_batch_size=128'* &&
      $verified == *'loader_timestamp_utc=2000-01-01T00:00:00Z'* &&
      $verified == *'monitor_pending_client_bytes=1073741824'* &&
      $verified == *'monitor_retained_identity_bytes=2147483648'* &&
      $verified == *'monitor_bootstrap_state_rows=10000000'* &&
      $verified == *'monitor_bootstrap_state_bytes=268435456'* &&
      $verified == *'output_max_bytes=107374182400'* ]] ||
  fail 'performance capacity defaults were not reported'
[[ $verified == *'stages=benchbase_uninstrumented,read_capture_encoding,read_capture_encoding_redis,cdc_capture,assembly_join,assembly_join_check'* ]] ||
  fail 'default stage roster was not reported'

verified=$(
  "$RUNNER" --benchmark tpcc --smoke --verify-inputs-only \
    --output-root "$TEST_ROOT/tpcc-output"
)
  [[ $verified == *'benchmark=tpcc'* &&
      $verified == *'adapter_schema_file='*'/mariadb/initdb/adapters/tpcc.sql'* &&
      $verified == *'mix=45,43,4,4,4'* &&
      $verified == *'status_timeout_seconds=900'* &&
      $verified == *'high_water_threshold_percent=70'* &&
      $verified == *'bootstrap_scan_max_rows=1000000'* &&
      $verified == *'bootstrap_scan_max_input_bytes=4294967296'* &&
      $verified == *'bootstrap_scan_max_canonical_bytes=2147483648'* &&
      $verified == *'monitor_assembler_active_transactions=1024'* &&
      $verified == *'monitor_assembler_events_per_transaction=100000'* &&
      $verified == *'monitor_assembler_active_bytes=268435456'* &&
      $verified == *'monitor_assembler_completed_transactions=10000000'* &&
      $verified == *'monitor_assembler_retained_identity_bytes=268435456'* &&
      $verified == *'monitor_pending_client_records=1000000'* &&
      $verified == *'monitor_pending_cdc_slots=1000000'* &&
      $verified == *'monitor_pending_cdc_bytes=268435456'* &&
      $verified == *'monitor_retained_identity_records=10000000'* &&
      $verified == *'monitor_events_per_transaction=101000'* &&
      $verified == *'monitor_bootstrap_state_bytes=1073741824'* &&
      $verified == *'monitor_checker_state_bytes=536870912'* ]] ||
  fail 'TPC-C defaults and complete capacity roster were not reported'

# These are literal source mappings; expansion here would inspect this test's
# unset variables instead of the runner's recorded environment expressions.
# shellcheck disable=SC2016
for mapping in \
  'BLACKBOX_BOOTSTRAP_MAX_ROWS=$BLACKBOX_BOOTSTRAP_MAX_ROWS' \
  'BLACKBOX_BOOTSTRAP_MAX_INPUT_BYTES=$BLACKBOX_BOOTSTRAP_MAX_INPUT_BYTES' \
  'BLACKBOX_BOOTSTRAP_MAX_CANONICAL_BYTES=$BLACKBOX_BOOTSTRAP_MAX_CANONICAL_BYTES' \
  'BLACKBOX_MONITOR_MAX_ASSEMBLER_ACTIVE_TRANSACTIONS=$MONITOR_MAX_ASSEMBLER_ACTIVE_TRANSACTIONS' \
  'BLACKBOX_MONITOR_MAX_ASSEMBLER_EVENTS_PER_TRANSACTION=$MONITOR_MAX_ASSEMBLER_EVENTS_PER_TRANSACTION' \
  'BLACKBOX_MONITOR_MAX_ASSEMBLER_ACTIVE_BYTES=$MONITOR_MAX_ASSEMBLER_ACTIVE_BYTES' \
  'BLACKBOX_MONITOR_MAX_ASSEMBLER_COMPLETED_TRANSACTIONS=$MONITOR_MAX_ASSEMBLER_COMPLETED_TRANSACTIONS' \
  'BLACKBOX_MONITOR_MAX_ASSEMBLER_RETAINED_IDENTITY_BYTES=$MONITOR_MAX_ASSEMBLER_RETAINED_IDENTITY_BYTES' \
  'BLACKBOX_MONITOR_MAX_PENDING_CLIENT_RECORDS=$MONITOR_MAX_PENDING_CLIENT_RECORDS' \
  'BLACKBOX_MONITOR_MAX_PENDING_CDC_SLOTS=$MONITOR_MAX_PENDING_CDC_SLOTS' \
  'BLACKBOX_MONITOR_MAX_PENDING_CDC_BYTES=$MONITOR_MAX_PENDING_CDC_BYTES' \
  'BLACKBOX_MONITOR_MAX_RETAINED_IDENTITY_RECORDS=$MONITOR_MAX_RETAINED_IDENTITY_RECORDS' \
  'BLACKBOX_MONITOR_MAX_EVENTS_PER_TRANSACTION=$MONITOR_MAX_EVENTS_PER_TRANSACTION' \
  'BLACKBOX_MONITOR_MAX_CHECKER_STATE_BYTES=$MONITOR_MAX_CHECKER_STATE_BYTES'; do
  grep -F "$mapping" "$RUNNER" >/dev/null ||
    fail "main staged runner is missing monitor mapping $mapping"
done
for transaction_name in NewOrder Payment OrderStatus Delivery StockLevel; do
  grep -F "<name>$transaction_name</name>" "$RUNNER" >/dev/null ||
    fail "TPC-C generated config is missing $transaction_name"
done

verified=$(
  "$RUNNER" --smoke --verify-inputs-only \
    --baseline-root "$BASELINE" --output-root "$TEST_ROOT/selected-output" \
    --stage assembly_join_check --stage assembly_join
)
[[ $verified == *'stages=assembly_join,assembly_join_check'* &&
      $verified == *'redis_maxmemory_bytes=8589934592'* ]] ||
  fail 'selected stage roster was not reported in canonical order'

verified=$(
  "$RUNNER" --smoke --verify-inputs-only \
    --baseline-root "$BASELINE" --output-root "$TEST_ROOT/override-output" \
    --redis-maxmemory-bytes 4294967296 \
    --monitor-pending-client-bytes 805306368 \
    --monitor-retained-identity-bytes 1073741824 \
    --monitor-bootstrap-state-rows 20000000 \
    --monitor-bootstrap-state-bytes 1073741824 \
    --output-max-bytes 53687091200
)
[[ $verified == *'redis_maxmemory_bytes=4294967296'* &&
      $verified == *'monitor_pending_client_bytes=805306368'* &&
      $verified == *'monitor_retained_identity_bytes=1073741824'* &&
      $verified == *'monitor_bootstrap_state_rows=20000000'* &&
      $verified == *'monitor_bootstrap_state_bytes=1073741824'* &&
      $verified == *'output_max_bytes=53687091200'* ]] ||
  fail 'performance capacity overrides were not reported'

verified=$(
  "$RUNNER" --smoke --verify-inputs-only \
    --baseline-root "$BASELINE" --output-root "$TEST_ROOT/batch-output" \
    --loader-batch-size 64
)
[[ $verified == *'loader_batch_size=64'* ]] ||
  fail 'selected loader batch size was not reported'

verified=$(
  BLACKBOX_PERFORMANCE_LOADER_TIMESTAMP_UTC=2001-02-03T04:05:06Z \
    "$RUNNER" --smoke --verify-inputs-only \
      --baseline-root "$BASELINE" --output-root "$TEST_ROOT/timestamp-env-output"
)
[[ $verified == *'loader_timestamp_utc=2001-02-03T04:05:06Z'* ]] ||
  fail 'loader timestamp environment override was not reported'

verified=$(
  BLACKBOX_PERFORMANCE_LOADER_TIMESTAMP_UTC=2001-02-03T04:05:06Z \
    "$RUNNER" --smoke --verify-inputs-only \
      --baseline-root "$BASELINE" --output-root "$TEST_ROOT/timestamp-cli-output" \
      --loader-timestamp-utc 2007-08-09T10:11:12Z
)
[[ $verified == *'loader_timestamp_utc=2007-08-09T10:11:12Z'* ]] ||
  fail 'loader timestamp command-line override did not take precedence'

if "$RUNNER" --print-stage-order --loader-batch-size 0 >/dev/null 2>&1; then
  fail '--loader-batch-size accepted zero'
fi
if "$RUNNER" --print-stage-order --loader-batch-size 2147483648 >/dev/null 2>&1; then
  fail '--loader-batch-size accepted signed 32-bit overflow'
fi

for option in \
  --redis-maxmemory-bytes \
  --bootstrap-scan-max-rows \
  --bootstrap-scan-max-input-bytes \
  --bootstrap-scan-max-canonical-bytes \
  --monitor-assembler-active-transactions \
  --monitor-assembler-events-per-transaction \
  --monitor-assembler-active-bytes \
  --monitor-assembler-completed-transactions \
  --monitor-assembler-retained-identity-bytes \
  --monitor-pending-client-records \
  --monitor-pending-client-bytes \
  --monitor-pending-cdc-slots \
  --monitor-pending-cdc-bytes \
  --monitor-retained-identity-records \
  --monitor-retained-identity-bytes \
  --monitor-events-per-transaction \
  --monitor-bootstrap-state-rows \
  --monitor-bootstrap-state-bytes \
  --monitor-checker-state-bytes \
  --output-max-bytes; do
  if "$RUNNER" --print-stage-order "$option" 0 >/dev/null 2>&1; then
    fail "$option accepted zero"
  fi
  if "$RUNNER" --print-stage-order "$option" 9223372036854775808 >/dev/null 2>&1; then
    fail "$option accepted signed 64-bit overflow"
  fi
done
if "$RUNNER" --print-stage-order --output-max-bytes 107374182401 \
    >/dev/null 2>&1; then
  fail '--output-max-bytes accepted a value above the canonical 100 GiB cap'
fi

guard_output="$TEST_ROOT/guard-output"
set +e
timeout --foreground --signal=TERM --kill-after=2 10 \
  "$RUNNER" --smoke --baseline-root "$BASELINE" \
    --output-root "$guard_output" --output-max-bytes 1 \
    >"$TEST_ROOT/guard.stdout" 2>"$TEST_ROOT/guard.stderr"
guard_status=$?
set -e
[[ $guard_status == 143 ]] ||
  fail "output-size guard did not terminate the coordinator (status $guard_status)"
[[ -s $guard_output/.output-size-limit-exceeded ]] ||
  fail 'output-size guard did not retain its limit evidence'
if pgrep --full -- "$guard_output" >/dev/null; then
  fail 'output-size guard left a generated-artifact process running'
fi

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

printf '%s\n' \
  'stage,throughput_requests_per_second,latency_mean_microseconds,latency_p95_microseconds' \
  'benchbase_uninstrumented,1000,100,150' \
  'cdc_capture,900,110,160' \
  >"$TEST_ROOT/headline-input.csv"
python3 "$ROOT/scripts/blackbox_performance_report.py" headline \
  --input-csv "$TEST_ROOT/headline-input.csv" \
  --csv-output "$TEST_ROOT/headline_results.csv" \
  --markdown-output "$TEST_ROOT/headline_results.md"
grep -Fx 'cdc_capture,900,110,160,10.0' \
  "$TEST_ROOT/headline_results.csv" >/dev/null ||
  fail 'headline CSV is missing exact CDC medians and baseline loss'
grep -Fx '| CDC only | 900.0 req/s | 110.0 µs | 160 µs | 10.00% |' \
  "$TEST_ROOT/headline_results.md" >/dev/null ||
  fail 'headline Markdown is missing the readable CDC row'
grep -Fx '| Stage | BenchBase throughput | Mean latency | p95 | Loss vs baseline |' \
  "$TEST_ROOT/headline_results.md" >/dev/null ||
  fail 'headline Markdown does not identify BenchBase throughput'

printf 'performance harness validation tests passed\n'
