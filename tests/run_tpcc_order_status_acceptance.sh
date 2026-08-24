#!/usr/bin/env bash
# Live one-warehouse RR acceptance run for audited TPC-C OrderStatus.

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
TPCC_ACCEPTANCE_ROOT=$ROOT
TPCC_ACCEPTANCE_ENV_PREFIX=BLACKBOX_TPCC_ORDER_STATUS
TPCC_ACCEPTANCE_SLUG=order-status
TPCC_ACCEPTANCE_CONFIG_KIND=tpcc_order_status_acceptance_config
TPCC_ACCEPTANCE_DEFAULT_VALIDATOR="$ROOT/tests/verify_tpcc_order_status_acceptance.py"
TPCC_ACCEPTANCE_DEFAULT_WEIGHTS=0,0,100,0,0
TPCC_ACCEPTANCE_DEFAULT_TERMINALS=1
TPCC_ACCEPTANCE_DEFAULT_RANDOM_SEED=3
TPCC_ACCEPTANCE_DEFAULT_DURATION_SECONDS=2
TPCC_ACCEPTANCE_DEFAULT_LOADER_THREADS=1
# shellcheck source=tests/tpcc_live_acceptance.sh
. "$ROOT/tests/tpcc_live_acceptance.sh"
tpcc_acceptance_init

MINIMUM_TRANSACTIONS=$(tpcc_acceptance_setting MINIMUM_TRANSACTIONS 2)
MINIMUM_ID_TRANSACTIONS=$(tpcc_acceptance_setting MINIMUM_ID_TRANSACTIONS 1)
MINIMUM_LAST_NAME_TRANSACTIONS=$(tpcc_acceptance_setting MINIMUM_LAST_NAME_TRANSACTIONS 1)
MINIMUM_MULTIROW_TRANSACTIONS=$(tpcc_acceptance_setting MINIMUM_MULTIROW_TRANSACTIONS 1)


usage() {
  cat <<'EOF'
Usage: run_tpcc_order_status_acceptance.sh [options]

Required for a live run:
  --benchbase-dist PATH       Distribution containing benchbase.jar.
  --monitor-bin PATH          blackbox_run_monitor executable.

Profile controls:
  --minimum-transactions N
  --minimum-id-transactions N
  --minimum-last-name-transactions N
  --minimum-multirow-transactions N

Common controls (matching BLACKBOX_TPCC_ORDER_STATUS_* environment variables):
EOF
  tpcc_acceptance_common_usage
  printf '  --help\n'
}


while (( $# > 0 )); do
  if tpcc_acceptance_parse_common_option "$@"; then
    shift "$TPCC_ACCEPTANCE_PARSED_ARGUMENTS"
    continue
  fi
  case "$1" in
    --minimum-transactions)
      tpcc_acceptance_need_value "$@"; MINIMUM_TRANSACTIONS=$2; shift 2
      ;;
    --minimum-id-transactions)
      tpcc_acceptance_need_value "$@"; MINIMUM_ID_TRANSACTIONS=$2; shift 2
      ;;
    --minimum-last-name-transactions)
      tpcc_acceptance_need_value "$@"; MINIMUM_LAST_NAME_TRANSACTIONS=$2; shift 2
      ;;
    --minimum-multirow-transactions)
      tpcc_acceptance_need_value "$@"; MINIMUM_MULTIROW_TRANSACTIONS=$2; shift 2
      ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done


tpcc_acceptance_validate_profile_controls() {
  [[ $TPCC_WAREHOUSES == 1 ]] || {
    printf 'warehouses must be 1 for this acceptance run\n' >&2
    exit 2
  }
  [[ $TPCC_WEIGHTS == 0,0,100,0,0 ]] || {
    printf 'weights must be exactly 0,0,100,0,0\n' >&2
    exit 2
  }
  tpcc_acceptance_require_positive minimum-transactions "$MINIMUM_TRANSACTIONS"
  tpcc_acceptance_require_positive minimum-id-transactions "$MINIMUM_ID_TRANSACTIONS"
  tpcc_acceptance_require_positive \
    minimum-last-name-transactions "$MINIMUM_LAST_NAME_TRANSACTIONS"
  tpcc_acceptance_require_positive \
    minimum-multirow-transactions "$MINIMUM_MULTIROW_TRANSACTIONS"
}


tpcc_acceptance_config_extras() {
  printf '{"minimum_transactions":%s,"minimum_id_transactions":%s,"minimum_last_name_transactions":%s,"minimum_multirow_transactions":%s}\n' \
    "$MINIMUM_TRANSACTIONS" "$MINIMUM_ID_TRANSACTIONS" \
    "$MINIMUM_LAST_NAME_TRANSACTIONS" "$MINIMUM_MULTIROW_TRANSACTIONS"
}


tpcc_acceptance_capture_profile_artifacts() {
  local output_dir=$1
  local catalog="$output_dir/customer-catalog.jsonl"
  # Backend-neutral SQL; the tab-separated rows become JSONL records here.
  blackbox_database_query '
    SELECT c_w_id, c_d_id, c_id, c_last,
           COUNT(*) OVER (PARTITION BY c_w_id, c_d_id, c_last)
      FROM customer
     ORDER BY c_w_id, c_d_id, c_id' |
    python3 -c '
import json
import sys

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue
    w_id, d_id, c_id, c_last, group = line.split("\t")
    print(json.dumps({
        "c_w_id": int(w_id), "c_d_id": int(d_id), "c_id": int(c_id),
        "c_last": c_last, "last_name_group_count": int(group),
    }, separators=(",", ":")))
' >"$catalog"
  TPCC_ACCEPTANCE_VALIDATOR_EXTRA_ARGS=(--customer-catalog "$catalog")
}


tpcc_acceptance_run
