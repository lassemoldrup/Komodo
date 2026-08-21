#!/usr/bin/env bash
# Live one-warehouse RR acceptance run for audited TPC-C StockLevel.

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
TPCC_ACCEPTANCE_ROOT=$ROOT
TPCC_ACCEPTANCE_ENV_PREFIX=BLACKBOX_TPCC_STOCK_LEVEL
TPCC_ACCEPTANCE_SLUG=stock-level
TPCC_ACCEPTANCE_CONFIG_KIND=tpcc_stock_level_acceptance_config
TPCC_ACCEPTANCE_DEFAULT_VALIDATOR="$ROOT/tests/verify_tpcc_stock_level_acceptance.py"
TPCC_ACCEPTANCE_DEFAULT_WEIGHTS=0,0,0,0,100
TPCC_ACCEPTANCE_DEFAULT_TERMINALS=1
TPCC_ACCEPTANCE_DEFAULT_RANDOM_SEED=3
TPCC_ACCEPTANCE_DEFAULT_DURATION_SECONDS=5
TPCC_ACCEPTANCE_DEFAULT_LOADER_THREADS=1
# shellcheck source=tests/tpcc_live_acceptance.sh
. "$ROOT/tests/tpcc_live_acceptance.sh"
tpcc_acceptance_init

MINIMUM_TRANSACTIONS=$(tpcc_acceptance_setting MINIMUM_TRANSACTIONS 10)


usage() {
  cat <<'EOF'
Usage: run_tpcc_stock_level_acceptance.sh [options]

Required for a live run:
  --benchbase-dist PATH
  --monitor-bin PATH

Profile controls:
  --minimum-transactions N

Common controls (matching BLACKBOX_TPCC_STOCK_LEVEL_* environment variables):
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
      tpcc_acceptance_need_value "$@"; MINIMUM_TRANSACTIONS=$2; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done


tpcc_acceptance_validate_profile_controls() {
  [[ $TPCC_WAREHOUSES == 1 ]] || {
    printf 'warehouses must be 1 for this acceptance run\n' >&2
    exit 2
  }
  (( TPCC_TERMINALS >= 1 && TPCC_TERMINALS <= 10 )) || {
    printf 'terminals must be between 1 and 10\n' >&2
    exit 2
  }
  [[ $TPCC_WEIGHTS == 0,0,0,0,100 ]] || {
    printf 'weights must be exactly 0,0,0,0,100\n' >&2
    exit 2
  }
  tpcc_acceptance_require_positive minimum-transactions "$MINIMUM_TRANSACTIONS"
}


tpcc_acceptance_config_extras() {
  printf '{"minimum_transactions":%s}\n' "$MINIMUM_TRANSACTIONS"
}


tpcc_acceptance_capture_profile_artifacts() {
  local output_dir=$1
  TPCC_ACCEPTANCE_VALIDATOR_EXTRA_ARGS=(--benchbase-log "$output_dir/e2e.log")
}


tpcc_acceptance_run
