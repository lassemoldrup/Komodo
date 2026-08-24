#!/usr/bin/env bash
# Sequential live YugabyteDB TPC-C acceptance suite against one freshly built
# BenchBase distribution, including the W>=2 remote profiles, the
# resource-exhaustion reference probe, and the standard full mix.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
OUTPUT_ROOT=${1:?usage: rerun_tpcc_acceptance_suite.sh OUTPUT_ROOT MONITOR_BIN}
MONITOR_BIN=${2:?usage: rerun_tpcc_acceptance_suite.sh OUTPUT_ROOT MONITOR_BIN}
mkdir -p "$OUTPUT_ROOT"
DIST="$OUTPUT_ROOT/benchbase-postgres"

step() {
  printf '=== %s %s\n' "$(date -u +%H:%M:%SZ)" "$*"
}

step "building BenchBase distribution from HEAD"
(
  cd "$ROOT/../benchbase"
  mvn --batch-mode --quiet --activate-profiles postgres \
    -Dmaven.gitcommitid.skip=true -Dfmt.skip -DskipTests \
    -DbuildDirectory="$OUTPUT_ROOT/benchbase-build" clean package
)
rm -rf "$DIST"
tar -xzf "$OUTPUT_ROOT/benchbase-build/benchbase-postgres.tgz" -C "$OUTPUT_ROOT"
sha256sum "$DIST/benchbase.jar" | tee "$OUTPUT_ROOT/benchbase-jar.sha256"

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

run stock-level-acceptance run_tpcc_stock_level_acceptance.sh
run order-status-acceptance run_tpcc_order_status_acceptance.sh --duration-seconds 10
run new-order-acceptance run_tpcc_new_order_acceptance.sh
run payment-acceptance run_tpcc_payment_acceptance.sh

step "starting remote runs at two warehouses"
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
