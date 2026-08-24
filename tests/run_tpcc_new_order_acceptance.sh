#!/usr/bin/env bash
# Live one-warehouse RR acceptance run for audited TPC-C NewOrder.

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
TPCC_ACCEPTANCE_ROOT=$ROOT
TPCC_ACCEPTANCE_ENV_PREFIX=BLACKBOX_TPCC_NEW_ORDER
TPCC_ACCEPTANCE_SLUG=new-order
TPCC_ACCEPTANCE_CONFIG_KIND=tpcc_new_order_acceptance_config
TPCC_ACCEPTANCE_DEFAULT_VALIDATOR="$ROOT/tests/verify_tpcc_new_order_acceptance.py"
TPCC_ACCEPTANCE_DEFAULT_WEIGHTS=100,0,0,0,0
TPCC_ACCEPTANCE_DEFAULT_TERMINALS=2
TPCC_ACCEPTANCE_DEFAULT_RANDOM_SEED=144
TPCC_ACCEPTANCE_DEFAULT_DURATION_SECONDS=10
TPCC_ACCEPTANCE_DEFAULT_LOADER_THREADS=1
# shellcheck source=tests/tpcc_live_acceptance.sh
. "$ROOT/tests/tpcc_live_acceptance.sh"
tpcc_acceptance_init

MINIMUM_TRANSACTIONS=$(tpcc_acceptance_setting MINIMUM_TRANSACTIONS 10)
MINIMUM_INVALID_ITEM_ABORTS=$(tpcc_acceptance_setting MINIMUM_INVALID_ITEM_ABORTS 1)
MINIMUM_REPEATED_KEY_TRANSACTIONS=$(
  tpcc_acceptance_setting MINIMUM_REPEATED_KEY_TRANSACTIONS 1
)
MINIMUM_RETRIES=$(tpcc_acceptance_setting MINIMUM_RETRIES 0)
MINIMUM_REMOTE_ITEM_TRANSACTIONS=$(
  tpcc_acceptance_setting MINIMUM_REMOTE_ITEM_TRANSACTIONS ""
)


usage() {
  cat <<'EOF'
Usage: run_tpcc_new_order_acceptance.sh [options]

Required for a live run:
  --benchbase-dist PATH       Distribution containing benchbase.jar.
  --monitor-bin PATH          blackbox_run_monitor executable.

Profile controls:
  --minimum-transactions N
  --minimum-invalid-item-aborts N
  --minimum-repeated-key-transactions N
  --minimum-remote-item-transactions N
  --minimum-retries N

Common controls (matching BLACKBOX_TPCC_NEW_ORDER_* environment variables):
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
    --minimum-invalid-item-aborts)
      tpcc_acceptance_need_value "$@"; MINIMUM_INVALID_ITEM_ABORTS=$2; shift 2
      ;;
    --minimum-repeated-key-transactions)
      tpcc_acceptance_need_value "$@"; MINIMUM_REPEATED_KEY_TRANSACTIONS=$2; shift 2
      ;;
    --minimum-remote-item-transactions)
      tpcc_acceptance_need_value "$@"; MINIMUM_REMOTE_ITEM_TRANSACTIONS=$2; shift 2
      ;;
    --minimum-retries)
      tpcc_acceptance_need_value "$@"; MINIMUM_RETRIES=$2; shift 2
      ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done


tpcc_acceptance_validate_profile_controls() {
  tpcc_acceptance_require_positive warehouses "$TPCC_WAREHOUSES"
  [[ $TPCC_WEIGHTS == 100,0,0,0,0 ]] || {
    printf 'weights must be exactly 100,0,0,0,0\n' >&2
    exit 2
  }
  # A remote item cannot be selected at one warehouse; at two or more the
  # live run must positively prove the remote branch.
  if (( TPCC_WAREHOUSES >= 2 )); then
    REMOTE_ITEM_LIVE_APPLICABLE=true
    [[ -n $MINIMUM_REMOTE_ITEM_TRANSACTIONS ]] ||
      MINIMUM_REMOTE_ITEM_TRANSACTIONS=1
    tpcc_acceptance_require_positive \
      minimum-remote-item-transactions "$MINIMUM_REMOTE_ITEM_TRANSACTIONS"
  else
    REMOTE_ITEM_LIVE_APPLICABLE=false
    [[ -z $MINIMUM_REMOTE_ITEM_TRANSACTIONS \
       || $MINIMUM_REMOTE_ITEM_TRANSACTIONS == 0 ]] || {
      printf 'remote-item transactions are impossible at one warehouse\n' >&2
      exit 2
    }
    MINIMUM_REMOTE_ITEM_TRANSACTIONS=0
  fi
  tpcc_acceptance_require_positive minimum-transactions "$MINIMUM_TRANSACTIONS"
  tpcc_acceptance_require_positive \
    minimum-invalid-item-aborts "$MINIMUM_INVALID_ITEM_ABORTS"
  tpcc_acceptance_require_positive \
    minimum-repeated-key-transactions "$MINIMUM_REPEATED_KEY_TRANSACTIONS"
  tpcc_acceptance_require_nonnegative minimum-retries "$MINIMUM_RETRIES"
}


tpcc_acceptance_config_extras() {
  printf '{"minimum_transactions":%s,"minimum_invalid_item_aborts":%s,"minimum_repeated_key_transactions":%s,"minimum_remote_item_transactions":%s,"minimum_retries":%s,"remote_item_live_applicable":%s}\n' \
    "$MINIMUM_TRANSACTIONS" "$MINIMUM_INVALID_ITEM_ABORTS" \
    "$MINIMUM_REPEATED_KEY_TRANSACTIONS" "$MINIMUM_REMOTE_ITEM_TRANSACTIONS" \
    "$MINIMUM_RETRIES" "$REMOTE_ITEM_LIVE_APPLICABLE"
}


tpcc_acceptance_capture_profile_artifacts() {
  local output_dir=$1
  TPCC_ACCEPTANCE_VALIDATOR_EXTRA_ARGS=(--benchbase-log "$output_dir/e2e.log")
}


tpcc_acceptance_run
