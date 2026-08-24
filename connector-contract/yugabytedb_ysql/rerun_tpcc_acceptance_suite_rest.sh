#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
OUTPUT_ROOT=${1:?}
MONITOR_BIN=${2:?}
DIST="$OUTPUT_ROOT/benchbase-postgres"
step() { printf '=== %s %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
common=(
  --benchbase-dist "$DIST"
  --monitor-bin "$MONITOR_BIN"
  --stack-timeout-seconds 300
  --loader-timeout-seconds 5400
  --status-timeout-seconds 2400
  --case-timeout-seconds 10800
)
run() {
  local name=$1 script=$2
  shift 2
  step "starting $name"
  TPCC_ACCEPTANCE_BACKEND=yugabytedb_ysql \
    "$ROOT/tests/$script" "${common[@]}" \
    --output-dir "$OUTPUT_ROOT/$name" "$@"
  step "finished $name"
}
run payment-acceptance run_tpcc_payment_acceptance.sh
BLACKBOX_REDIS_MAXMEMORY_BYTES=4294967296 \
  run new-order-remote-acceptance run_tpcc_new_order_acceptance.sh \
    --warehouses 2 --duration-seconds 30
BLACKBOX_REDIS_MAXMEMORY_BYTES=4294967296 \
  run payment-remote-acceptance run_tpcc_payment_acceptance.sh \
    --warehouses 2 --duration-seconds 30
run delivery-acceptance run_tpcc_delivery_acceptance.sh --duration-seconds 90
step "starting resource-exhaustion reference probe (expected INVALID)"
set +e
TPCC_ACCEPTANCE_BACKEND=yugabytedb_ysql \
  "$ROOT/tests/run_tpcc_stock_level_acceptance.sh" "${common[@]}" \
  --output-dir "$OUTPUT_ROOT/resource-exhaustion-probe" \
  --monitor-max-bootstrap-state-bytes 268435456
probe_status=$?
set -e
step "probe exited $probe_status (nonzero expected)"
python3 - "$OUTPUT_ROOT/resource-exhaustion-probe/monitor-metrics.json" <<'CHECK'
import json
import sys

metrics = json.load(open(sys.argv[1]))
assert metrics["outcome"] == "INVALID" and metrics["completed"] is False
assert any(item["code"] == "RESOURCE_EXHAUSTED" for item in metrics["diagnostics"])
print("probe bootstrap_state_bytes:", metrics["joiner"]["bootstrap_state_bytes"])
CHECK
run full-mix-acceptance run_tpcc_full_mix_acceptance.sh \
  --resource-exhaustion-metrics "$OUTPUT_ROOT/resource-exhaustion-probe/monitor-metrics.json"
step "suite complete"
