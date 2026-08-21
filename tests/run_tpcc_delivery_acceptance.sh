#!/usr/bin/env bash
# Live one-warehouse RR acceptance run for audited TPC-C Delivery.

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
TPCC_ACCEPTANCE_ROOT=$ROOT
TPCC_ACCEPTANCE_ENV_PREFIX=BLACKBOX_TPCC_DELIVERY
TPCC_ACCEPTANCE_SLUG=delivery
TPCC_ACCEPTANCE_CONFIG_KIND=tpcc_delivery_acceptance_config
TPCC_ACCEPTANCE_DEFAULT_VALIDATOR="$ROOT/tests/verify_tpcc_delivery_acceptance.py"
TPCC_ACCEPTANCE_DEFAULT_WEIGHTS=0,0,0,100,0
TPCC_ACCEPTANCE_DEFAULT_TERMINALS=2
TPCC_ACCEPTANCE_DEFAULT_RANDOM_SEED=3
TPCC_ACCEPTANCE_DEFAULT_DURATION_SECONDS=10
TPCC_ACCEPTANCE_DEFAULT_LOADER_THREADS=1
# shellcheck source=tests/tpcc_live_acceptance.sh
. "$ROOT/tests/tpcc_live_acceptance.sh"
tpcc_acceptance_init

MINIMUM_TRANSACTIONS=$(tpcc_acceptance_setting MINIMUM_TRANSACTIONS 10)
MINIMUM_EMPTY_TRANSACTIONS=$(tpcc_acceptance_setting MINIMUM_EMPTY_TRANSACTIONS 1)
MINIMUM_ACTIVE_TRANSACTIONS=$(tpcc_acceptance_setting MINIMUM_ACTIVE_TRANSACTIONS 1)
MINIMUM_MULTI_DISTRICT_TRANSACTIONS=$(
  tpcc_acceptance_setting MINIMUM_MULTI_DISTRICT_TRANSACTIONS 1
)
MINIMUM_MULTI_LINE_DISTRICTS=$(tpcc_acceptance_setting MINIMUM_MULTI_LINE_DISTRICTS 1)
MINIMUM_RETRIES=$(tpcc_acceptance_setting MINIMUM_RETRIES 0)


usage() {
  cat <<'EOF'
Usage: run_tpcc_delivery_acceptance.sh [options]

Required for a live run:
  --benchbase-dist PATH
  --monitor-bin PATH

Profile controls:
  --minimum-transactions N
  --minimum-empty-transactions N
  --minimum-active-transactions N
  --minimum-multi-district-transactions N
  --minimum-multi-line-districts N
  --minimum-retries N

Common controls (matching BLACKBOX_TPCC_DELIVERY_* environment variables):
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
    --minimum-empty-transactions)
      tpcc_acceptance_need_value "$@"; MINIMUM_EMPTY_TRANSACTIONS=$2; shift 2 ;;
    --minimum-active-transactions)
      tpcc_acceptance_need_value "$@"; MINIMUM_ACTIVE_TRANSACTIONS=$2; shift 2 ;;
    --minimum-multi-district-transactions)
      tpcc_acceptance_need_value "$@"; MINIMUM_MULTI_DISTRICT_TRANSACTIONS=$2; shift 2 ;;
    --minimum-multi-line-districts)
      tpcc_acceptance_need_value "$@"; MINIMUM_MULTI_LINE_DISTRICTS=$2; shift 2 ;;
    --minimum-retries)
      tpcc_acceptance_need_value "$@"; MINIMUM_RETRIES=$2; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done


tpcc_acceptance_validate_profile_controls() {
  [[ $TPCC_WAREHOUSES == 1 ]] || {
    printf 'warehouses must be 1 for this acceptance run\n' >&2
    exit 2
  }
  (( TPCC_TERMINALS >= 2 && TPCC_TERMINALS <= 10 )) || {
    printf 'terminals must be between 2 and 10 for the Delivery race gate\n' >&2
    exit 2
  }
  [[ $TPCC_WEIGHTS == 0,0,0,100,0 ]] || {
    printf 'weights must be exactly 0,0,0,100,0\n' >&2
    exit 2
  }
  tpcc_acceptance_require_positive minimum-transactions "$MINIMUM_TRANSACTIONS"
  tpcc_acceptance_require_positive minimum-empty-transactions "$MINIMUM_EMPTY_TRANSACTIONS"
  tpcc_acceptance_require_positive minimum-active-transactions "$MINIMUM_ACTIVE_TRANSACTIONS"
  tpcc_acceptance_require_positive \
    minimum-multi-district-transactions "$MINIMUM_MULTI_DISTRICT_TRANSACTIONS"
  tpcc_acceptance_require_positive \
    minimum-multi-line-districts "$MINIMUM_MULTI_LINE_DISTRICTS"
  tpcc_acceptance_require_nonnegative minimum-retries "$MINIMUM_RETRIES"
}


tpcc_acceptance_config_extras() {
  printf '{"minimum_transactions":%s,"minimum_empty_transactions":%s,"minimum_active_transactions":%s,"minimum_multi_district_transactions":%s,"minimum_multi_line_districts":%s,"minimum_retries":%s}\n' \
    "$MINIMUM_TRANSACTIONS" "$MINIMUM_EMPTY_TRANSACTIONS" \
    "$MINIMUM_ACTIVE_TRANSACTIONS" "$MINIMUM_MULTI_DISTRICT_TRANSACTIONS" \
    "$MINIMUM_MULTI_LINE_DISTRICTS" "$MINIMUM_RETRIES"
}


tpcc_acceptance_capture_profile_artifacts() {
  local output_dir=$1
  TPCC_ACCEPTANCE_VALIDATOR_EXTRA_ARGS=(--benchbase-log "$output_dir/e2e.log")
}


tpcc_acceptance_run
