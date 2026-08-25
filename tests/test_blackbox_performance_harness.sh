#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
RUNNER="$ROOT/scripts/run_blackbox_performance.sh"
TEST_ROOT=$(mktemp -d /media/newhd/yifan/db-monitoring-builds/blackbox-performance-harness-test.XXXXXX)


cleanup() {
  rm -rf -- "$TEST_ROOT"
}
trap cleanup EXIT


fail() {
  printf 'performance harness test failed: %s\n' "$*" >&2
  exit 1
}


default_redis_command=$(docker compose --file "$ROOT/docker-compose.yml" \
  --profile mariadb config --format json | jq -c '.services.redis.command')
[[ $default_redis_command == *'"--maxmemory","2147483648"'* ]] ||
  fail 'ordinary Compose Redis capacity is not the 2 GiB default'
override_redis_command=$(BLACKBOX_REDIS_MAXMEMORY_BYTES=8589934592 \
  docker compose --file "$ROOT/docker-compose.yml" --profile mariadb \
    config --format json | jq -c '.services.redis.command')
[[ $override_redis_command == *'"--maxmemory","8589934592"'* ]] ||
  fail 'Compose Redis capacity override was not resolved'
if grep -E 'PERFORMANCE_BASELINE_ROOT|--baseline-root' "$ROOT/Makefile" >/dev/null; then
  fail 'formal performance Make targets still require a historical baseline worktree'
fi

mkdir -p "$TEST_ROOT/mock-bin"
cat >"$TEST_ROOT/mock-bin/docker" <<'SCRIPT'
#!/usr/bin/env bash
: >"$UNEXPECTED_DOCKER_CALL"
exit 99
SCRIPT
chmod +x "$TEST_ROOT/mock-bin/docker"
UNEXPECTED_DOCKER_CALL="$TEST_ROOT/unexpected-docker-call" \
PATH="$TEST_ROOT/mock-bin:$PATH" \
  "$RUNNER" --print-mode-order --repetitions 1 >/dev/null
[[ ! -e $TEST_ROOT/unexpected-docker-call ]] ||
  fail "non-running validation path attempted Compose cleanup without the live lock"

actual_order=$(
  "$RUNNER" --print-mode-order --repetitions 3 --seed 20260818
)
expected_order=$(cat <<'EOF'
repetition,mode_order_position,mode
1,1,benchbase_uninstrumented
1,2,full_e2e
2,1,full_e2e
2,2,benchbase_uninstrumented
3,1,benchbase_uninstrumented
3,2,full_e2e
EOF
)
[[ $actual_order == "$expected_order" ]] || fail "deterministic rotated mode order differs"

single_mode_order=$(
  "$RUNNER" --print-mode-order --repetitions 2 --seed 20260818 \
    --mode full_e2e
)
expected_single_mode_order=$(cat <<'EOF'
repetition,mode_order_position,mode
1,1,full_e2e
2,1,full_e2e
EOF
)
[[ $single_mode_order == "$expected_single_mode_order" ]] ||
  fail "single-mode selection differs"

two_mode_order=$(
  "$RUNNER" --print-mode-order --repetitions 2 --seed 20260818 \
    --mode full_e2e --mode benchbase_uninstrumented
)
expected_two_mode_order=$(cat <<'EOF'
repetition,mode_order_position,mode
1,1,benchbase_uninstrumented
1,2,full_e2e
2,1,full_e2e
2,2,benchbase_uninstrumented
EOF
)
[[ $two_mode_order == "$expected_two_mode_order" ]] ||
  fail "two-mode selection did not use canonical-order rotation"

tpcc_order=$(
  "$RUNNER" --benchmark tpcc --print-mode-order --repetitions 1 --seed 3
)
[[ $(grep -c '^1,' <<<"$tpcc_order") == 2 ]] ||
  fail 'TPC-C mode order did not retain the two formal modes'

config_generator="$TEST_ROOT/generate-configs.py"
awk '
  /^generate_configs\(\)/ { in_function=1 }
  in_function && /<<'\''PY'\''$/ { capture=1; next }
  capture && /^PY$/ { exit }
  capture { print }
' "$RUNNER" >"$config_generator"
python3 "$config_generator" \
  "$TEST_ROOT/tpcc-disabled.xml" "$TEST_ROOT/tpcc-audited.xml" \
  4 1 32 1 128 0 2 3 tpcc 45,43,4,4,4 unlimited 2000-01-01T00:00:00Z mariadb
grep -F "<mode>\${env:BLACKBOX_MEASUREMENT_MODE}</mode>" \
  "$TEST_ROOT/tpcc-audited.xml" >/dev/null ||
  fail 'generated TPC-C configuration lost the measurement-mode placeholder'
if grep -F '<blackbox>' "$TEST_ROOT/tpcc-disabled.xml" >/dev/null; then
  fail 'generated uninstrumented configuration contains black-box instrumentation'
fi
if grep -F '<runId>' "$TEST_ROOT/tpcc-disabled.xml" >/dev/null; then
  fail 'generated uninstrumented configuration contains a black-box run ID'
fi
grep -F '<benchmark>tpcc</benchmark>' "$TEST_ROOT/tpcc-audited.xml" >/dev/null ||
  fail 'generated full-E2E configuration selected the wrong adapter'
grep -F '<backend>mariadb</backend>' "$TEST_ROOT/tpcc-audited.xml" >/dev/null ||
  fail 'generated full-E2E configuration lost the backend selection'
python3 "$config_generator" \
  "$TEST_ROOT/ysql-disabled.xml" "$TEST_ROOT/ysql-audited.xml" \
  4 1 32 1 128 0 2 3 tpcc 45,43,4,4,4 unlimited 2000-01-01T00:00:00Z yugabytedb_ysql
grep -F '<type>POSTGRES</type>' "$TEST_ROOT/ysql-audited.xml" >/dev/null ||
  fail 'generated YugabyteDB configuration lost the POSTGRES dialect'
grep -F '<backend>yugabytedb_ysql</backend>' "$TEST_ROOT/ysql-audited.xml" >/dev/null ||
  fail 'generated YugabyteDB configuration lost the backend selection'
if grep -F '<blackbox>' "$TEST_ROOT/ysql-disabled.xml" >/dev/null; then
  fail 'generated YugabyteDB uninstrumented configuration is instrumented'
fi
for generated_config in "$TEST_ROOT/tpcc-disabled.xml" "$TEST_ROOT/tpcc-audited.xml"; do
  grep -F '<loaderTimestamp>2000-01-01T00:00:00Z</loaderTimestamp>' \
    "$generated_config" >/dev/null ||
    fail "generated configuration did not record the fixed loader timestamp: $generated_config"
  # The generated XML intentionally retains this environment expression.
  # shellcheck disable=SC2016
  grep -F '<loaderDeclaredCountsFile>${env:BLACKBOX_LOADER_DECLARED_COUNTS_FILE}</loaderDeclaredCountsFile>' \
    "$generated_config" >/dev/null ||
    fail "generated configuration did not preserve loader-state proof: $generated_config"
done

for seed in -2147483648 2147483647; do
  "$RUNNER" --benchmark tpcc --print-mode-order --repetitions 1 --seed "$seed" \
    >/dev/null || fail "signed 32-bit seed $seed was rejected"
done
for seed in -2147483649 2147483648 01; do
  if "$RUNNER" --benchmark tpcc --print-mode-order --seed "$seed" >/dev/null 2>&1; then
    fail "invalid BenchBase seed $seed was accepted"
  fi
done
if "$RUNNER" --benchmark tpcc --print-mode-order --mix 45,43,4,4 \
    >/dev/null 2>&1; then
  fail 'TPC-C mix with four weights was accepted'
fi
if "$RUNNER" --benchmark tpcc --print-mode-order --mix 45,43,4,4,5 \
    >/dev/null 2>&1; then
  fail 'TPC-C mix not summing to 100 was accepted'
fi
if "$RUNNER" --benchmark unknown --print-mode-order >/dev/null 2>&1; then
  fail 'unknown benchmark was accepted'
fi
if "$RUNNER" --benchmark tpcc --print-mode-order --scale-factor 1 >/dev/null 2>&1; then
  fail 'TPC-C accepted the YCSB scale-factor option'
fi
if "$RUNNER" --benchmark ycsb --print-mode-order --warehouses 1 >/dev/null 2>&1; then
  fail 'YCSB accepted the TPC-C warehouse option'
fi
"$RUNNER" --print-mode-order \
  --loader-timestamp-utc 2024-02-29T12:34:56Z >/dev/null ||
  fail 'valid canonical loader timestamp was rejected'
for loader_timestamp in \
  2024-02-29T12:34:56+00:00 \
  2024-02-29T12:34:56.000Z \
  2024-2-29T12:34:56Z \
  2023-02-29T12:34:56Z \
  '2024-02-29T12:34:56Z '; do
  if "$RUNNER" --print-mode-order --loader-timestamp-utc "$loader_timestamp" \
      >/dev/null 2>&1; then
    fail "noncanonical loader timestamp was accepted: $loader_timestamp"
  fi
done

if "$RUNNER" --print-mode-order --mode unknown_mode \
    >"$TEST_ROOT/unknown-mode.stdout" 2>"$TEST_ROOT/unknown-mode.stderr"; then
  fail "unknown mode was accepted"
fi
grep -F 'unknown mode: unknown_mode' "$TEST_ROOT/unknown-mode.stderr" >/dev/null ||
  fail "unknown mode diagnostic is unclear"

if "$RUNNER" --print-mode-order --mode full_e2e --mode full_e2e \
    >"$TEST_ROOT/duplicate-mode.stdout" 2>"$TEST_ROOT/duplicate-mode.stderr"; then
  fail "duplicate mode was accepted"
fi
grep -F 'duplicate mode: full_e2e' "$TEST_ROOT/duplicate-mode.stderr" >/dev/null ||
  fail "duplicate mode diagnostic is unclear"

if "$RUNNER" --print-mode-order --mode \
    >"$TEST_ROOT/missing-mode.stdout" 2>"$TEST_ROOT/missing-mode.stderr"; then
  fail "missing mode value was accepted"
fi
grep -F 'missing --mode value' "$TEST_ROOT/missing-mode.stderr" >/dev/null ||
  fail "missing mode value diagnostic is unclear"

if "$RUNNER" --print-mode-order --stage full_e2e >/dev/null 2>&1; then
  fail "legacy --stage option was accepted"
fi
if "$RUNNER" --print-stage-order >/dev/null 2>&1; then
  fail "legacy --print-stage-order option was accepted"
fi

verified=$(
  "$RUNNER" --smoke --verify-inputs-only \
    --output-root "$TEST_ROOT/empty-output"
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
[[ $verified == *'modes=benchbase_uninstrumented,full_e2e'* ]] ||
  fail 'default mode roster was not reported'

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
    fail "main two-mode runner is missing monitor mapping $mapping"
done
for transaction_name in NewOrder Payment OrderStatus Delivery StockLevel; do
  grep -F "<name>$transaction_name</name>" "$RUNNER" >/dev/null ||
    fail "TPC-C generated config is missing $transaction_name"
done

# These are literal source mappings in the runner, not shell expressions here.
# shellcheck disable=SC2016
for window_contract in \
  '"client_measurement_window"' \
  '"component_workload_window"' \
  '"after_stream_id"' \
  '"through_stream_id"' \
  '"start_exclusive": True' \
  '"end_inclusive": True' \
  '"final_verdict"' \
  'fence-window' \
  'final-status-boundary' \
  '--window-after-stream-id "$component_after_stream_id"' \
  '--window-through-stream-id "$component_through_stream_id"'; do
  grep -F -- "$window_contract" "$RUNNER" >/dev/null ||
    fail "two-mode runner is missing window contract: $window_contract"
done
if grep -E -- '--(measured|window)-(start|end)-epoch-ms' "$RUNNER" >/dev/null; then
  fail 'CDC component selection still uses ambiguous millisecond-only bounds'
fi

analysis_window_writer="$TEST_ROOT/write-analysis-window.py"
awk '
  /python3 - "\$trial_dir\/analysis_window.json"/ { found=1 }
  found && /<<'\''PY'\''$/ { capture=1; next }
  capture && /^PY$/ { exit }
  capture { print }
' "$RUNNER" >"$analysis_window_writer"
python3 "$analysis_window_writer" "$TEST_ROOT/analysis-window.json" \
  1100 2100 1000-2 2200-4 1000 2200 2300-0 2300
jq -e '
  .client_measurement_window == {
    "definition":"benchbase_measured_request_window",
    "start_epoch_ms":1100,
    "end_epoch_ms":2100
  } and
  .component_workload_window.after_stream_id == "1000-2" and
  .component_workload_window.through_stream_id == "2200-4" and
  .component_workload_window.start_epoch_ms == 1000 and
  .component_workload_window.end_epoch_ms == 2200 and
  .component_workload_window.start_exclusive == true and
  .component_workload_window.end_inclusive == true and
  .final_verdict == {"stream_id":"2300-0","epoch_ms":2300}
' "$TEST_ROOT/analysis-window.json" >/dev/null ||
  fail 'analysis window writer lost exact component or FINAL status boundaries'
python3 "$analysis_window_writer" "$TEST_ROOT/baseline-analysis-window.json" \
  1100 2100 '' '' '' '' '' ''
jq -e '
  .client_measurement_window.start_epoch_ms == 1100 and
  .client_measurement_window.end_epoch_ms == 2100 and
  .component_workload_window == null and .final_verdict == null
' "$TEST_ROOT/baseline-analysis-window.json" >/dev/null ||
  fail 'baseline analysis window unexpectedly contains component boundaries'

verified=$(
  "$RUNNER" --smoke --verify-inputs-only \
    --output-root "$TEST_ROOT/selected-output" \
    --mode full_e2e --mode benchbase_uninstrumented
)
[[ $verified == *'modes=benchbase_uninstrumented,full_e2e'* &&
      $verified == *'redis_maxmemory_bytes=8589934592'* ]] ||
  fail 'selected mode roster was not reported in canonical order'

verified=$(
  "$RUNNER" --smoke --verify-inputs-only \
    --output-root "$TEST_ROOT/override-output" \
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
    --output-root "$TEST_ROOT/batch-output" \
    --loader-batch-size 64
)
[[ $verified == *'loader_batch_size=64'* ]] ||
  fail 'selected loader batch size was not reported'

verified=$(
  BLACKBOX_PERFORMANCE_LOADER_TIMESTAMP_UTC=2001-02-03T04:05:06Z \
    "$RUNNER" --smoke --verify-inputs-only \
      --output-root "$TEST_ROOT/timestamp-env-output"
)
[[ $verified == *'loader_timestamp_utc=2001-02-03T04:05:06Z'* ]] ||
  fail 'loader timestamp environment override was not reported'

verified=$(
  BLACKBOX_PERFORMANCE_LOADER_TIMESTAMP_UTC=2001-02-03T04:05:06Z \
    "$RUNNER" --smoke --verify-inputs-only \
      --output-root "$TEST_ROOT/timestamp-cli-output" \
      --loader-timestamp-utc 2007-08-09T10:11:12Z
)
[[ $verified == *'loader_timestamp_utc=2007-08-09T10:11:12Z'* ]] ||
  fail 'loader timestamp command-line override did not take precedence'

if "$RUNNER" --print-mode-order --loader-batch-size 0 >/dev/null 2>&1; then
  fail '--loader-batch-size accepted zero'
fi
if "$RUNNER" --print-mode-order --loader-batch-size 2147483648 >/dev/null 2>&1; then
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
  if "$RUNNER" --print-mode-order "$option" 0 >/dev/null 2>&1; then
    fail "$option accepted zero"
  fi
  if "$RUNNER" --print-mode-order "$option" 9223372036854775808 >/dev/null 2>&1; then
    fail "$option accepted signed 64-bit overflow"
  fi
done
if "$RUNNER" --print-mode-order --output-max-bytes 107374182401 \
    >/dev/null 2>&1; then
  fail '--output-max-bytes accepted a value above the canonical 100 GiB cap'
fi

guard_output="$TEST_ROOT/guard-output"
set +e
timeout --foreground --signal=TERM --kill-after=2 10 \
  "$RUNNER" --smoke \
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
    --output-root "$TEST_ROOT/nonempty-output" \
    >/dev/null 2>&1; then
  fail "nonempty output root was accepted"
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

cat >"$TEST_ROOT/backend-sampler" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
output=
while (( $# > 0 )); do
  case "$1" in
    --output) output=$2; shift 2 ;;
    --stop-file|--interval-seconds|--duration-seconds) shift 2 ;;
    *) exit 2 ;;
  esac
done
printf '{"kind":"backend_performance_telemetry","sample_count":1}\n' >"$output"
SCRIPT
chmod +x "$TEST_ROOT/backend-sampler"
BLACKBOX_REDIS_PORT=6399 "$ROOT/scripts/run_measured_command.sh" \
  --resource-output "$TEST_ROOT/backend-resources.csv" \
  --network-output "$TEST_ROOT/backend-network.json" \
  --sample-duration-seconds 1 \
  --sample-interval-seconds 0.1 \
  --backend-sampler "$TEST_ROOT/backend-sampler" \
  --backend-output "$TEST_ROOT/backend-telemetry.json" \
  -- bash -c 'sleep 0.2'
jq -e \
  '.kind == "backend_performance_telemetry" and .sample_count == 1' \
  "$TEST_ROOT/backend-telemetry.json" >/dev/null ||
  fail "generic measured-command boundary did not retain backend telemetry"

printf '%s\n' \
  'mode,logical_transactions_per_second,successful_transactions_per_second,latency_mean_microseconds,latency_p95_microseconds' \
  'benchbase_uninstrumented,1000,990,100,150' \
  'full_e2e,900,895,110,160' \
  >"$TEST_ROOT/headline-input.csv"
python3 "$ROOT/scripts/blackbox_performance_report.py" headline \
  --input-csv "$TEST_ROOT/headline-input.csv" \
  --csv-output "$TEST_ROOT/headline_results.csv" \
  --markdown-output "$TEST_ROOT/headline_results.md"
grep -Fx 'full_e2e,900,895,110,160,10.0' \
  "$TEST_ROOT/headline_results.csv" >/dev/null ||
  fail 'headline CSV is missing exact full-E2E medians and baseline loss'
grep -Fx '| Full black-box E2E | 900.0 tx/s | 895.0 tx/s | 110.0 µs | 160 µs | 10.00% |' \
  "$TEST_ROOT/headline_results.md" >/dev/null ||
  fail 'headline Markdown is missing the readable full-E2E row'
grep -Fx '| Mode | Logical TPS | Successful TPS | Mean latency | p95 | Logical TPS loss vs baseline |' \
  "$TEST_ROOT/headline_results.md" >/dev/null ||
  fail 'headline Markdown does not identify BenchBase throughput'

printf 'performance harness validation tests passed\n'
