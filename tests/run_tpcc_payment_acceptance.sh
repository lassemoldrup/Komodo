#!/usr/bin/env bash
# Live one-warehouse RR acceptance run for audited TPC-C Payment.

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
TPCC_ACCEPTANCE_ROOT=$ROOT
TPCC_ACCEPTANCE_ENV_PREFIX=BLACKBOX_TPCC_PAYMENT
TPCC_ACCEPTANCE_SLUG=payment
TPCC_ACCEPTANCE_CONFIG_KIND=tpcc_payment_acceptance_config
TPCC_ACCEPTANCE_DEFAULT_VALIDATOR="$ROOT/tests/verify_tpcc_payment_acceptance.py"
TPCC_ACCEPTANCE_DEFAULT_WEIGHTS=0,100,0,0,0
TPCC_ACCEPTANCE_DEFAULT_TERMINALS=2
TPCC_ACCEPTANCE_DEFAULT_RANDOM_SEED=0
TPCC_ACCEPTANCE_DEFAULT_DURATION_SECONDS=10
TPCC_ACCEPTANCE_DEFAULT_LOADER_THREADS=1
# shellcheck source=tests/tpcc_live_acceptance.sh
. "$ROOT/tests/tpcc_live_acceptance.sh"
tpcc_acceptance_init

MINIMUM_TRANSACTIONS=$(tpcc_acceptance_setting MINIMUM_TRANSACTIONS 10)
MINIMUM_ID_TRANSACTIONS=$(tpcc_acceptance_setting MINIMUM_ID_TRANSACTIONS 1)
MINIMUM_LAST_NAME_TRANSACTIONS=$(tpcc_acceptance_setting MINIMUM_LAST_NAME_TRANSACTIONS 1)
MINIMUM_GOOD_CREDIT_TRANSACTIONS=$(
  tpcc_acceptance_setting MINIMUM_GOOD_CREDIT_TRANSACTIONS 1
)
MINIMUM_BAD_CREDIT_TRANSACTIONS=$(
  tpcc_acceptance_setting MINIMUM_BAD_CREDIT_TRANSACTIONS 1
)
MINIMUM_RETRIES=$(tpcc_acceptance_setting MINIMUM_RETRIES 0)


usage() {
  cat <<'EOF'
Usage: run_tpcc_payment_acceptance.sh [options]

Required for a live run:
  --benchbase-dist PATH       Distribution containing benchbase.jar.
  --monitor-bin PATH          blackbox_run_monitor executable.

Profile controls:
  --minimum-transactions N
  --minimum-id-transactions N
  --minimum-last-name-transactions N
  --minimum-good-credit-transactions N
  --minimum-bad-credit-transactions N
  --minimum-retries N

Common controls (matching BLACKBOX_TPCC_PAYMENT_* environment variables):
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
    --minimum-good-credit-transactions)
      tpcc_acceptance_need_value "$@"; MINIMUM_GOOD_CREDIT_TRANSACTIONS=$2; shift 2
      ;;
    --minimum-bad-credit-transactions)
      tpcc_acceptance_need_value "$@"; MINIMUM_BAD_CREDIT_TRANSACTIONS=$2; shift 2
      ;;
    --minimum-retries)
      tpcc_acceptance_need_value "$@"; MINIMUM_RETRIES=$2; shift 2
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
  [[ $TPCC_WEIGHTS == 0,100,0,0,0 ]] || {
    printf 'weights must be exactly 0,100,0,0,0\n' >&2
    exit 2
  }
  tpcc_acceptance_require_positive minimum-transactions "$MINIMUM_TRANSACTIONS"
  tpcc_acceptance_require_positive minimum-id-transactions "$MINIMUM_ID_TRANSACTIONS"
  tpcc_acceptance_require_positive \
    minimum-last-name-transactions "$MINIMUM_LAST_NAME_TRANSACTIONS"
  tpcc_acceptance_require_positive \
    minimum-good-credit-transactions "$MINIMUM_GOOD_CREDIT_TRANSACTIONS"
  tpcc_acceptance_require_positive \
    minimum-bad-credit-transactions "$MINIMUM_BAD_CREDIT_TRANSACTIONS"
  tpcc_acceptance_require_nonnegative minimum-retries "$MINIMUM_RETRIES"
}


tpcc_acceptance_config_extras() {
  printf '{"minimum_transactions":%s,"minimum_id_transactions":%s,"minimum_last_name_transactions":%s,"minimum_good_credit_transactions":%s,"minimum_bad_credit_transactions":%s,"minimum_retries":%s,"remote_customer_live_applicable":false}\n' \
    "$MINIMUM_TRANSACTIONS" "$MINIMUM_ID_TRANSACTIONS" \
    "$MINIMUM_LAST_NAME_TRANSACTIONS" "$MINIMUM_GOOD_CREDIT_TRANSACTIONS" \
    "$MINIMUM_BAD_CREDIT_TRANSACTIONS" "$MINIMUM_RETRIES"
}


tpcc_acceptance_capture_profile_artifacts() {
  local output_dir=$1
  local customer_catalog="$output_dir/customer-catalog.jsonl"
  local history_catalog="$output_dir/history-catalog.jsonl"
  blackbox_database_query '
    SELECT JSON_OBJECT(
             "c_w_id", c_w_id,
             "c_d_id", c_d_id,
             "c_id", c_id,
             "c_last", c_last,
             "c_first", c_first,
             "c_credit", c_credit,
             "last_name_group_count",
               COUNT(*) OVER (PARTITION BY c_w_id, c_d_id, c_last))
      FROM customer
     ORDER BY c_w_id, c_d_id, c_id' >"$customer_catalog"
  blackbox_database_query '
    SELECT JSON_OBJECT("h_id", h_id)
      FROM history
     ORDER BY h_id' >"$history_catalog"
  TPCC_ACCEPTANCE_VALIDATOR_EXTRA_ARGS=(
    --customer-catalog "$customer_catalog"
    --history-catalog "$history_catalog"
    --loader-validation "$output_dir/loader-validation.json"
    --benchbase-log "$output_dir/e2e.log"
  )
}


tpcc_acceptance_run
