#!/usr/bin/env bash
set -Eeuo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
output=${BLACKBOX_OCEANBASE_MYSQL_SEMANTIC_OUTPUT_DIR:?}
monitor_bin=${BLACKBOX_MONITOR_BIN:?}
benchbase_archive=${BLACKBOX_BENCHBASE_ARCHIVE:?}
compose=(docker compose --file "$root/docker-compose.yml" --profile oceanbase_mysql)

[[ ! -e $output ]]
mkdir -p "$output"
distribution_parent=$(mktemp -d /tmp/oceanbase-mysql-semantic-dist.XXXXXX)
tar -xzf "$benchbase_archive" -C "$distribution_parent"
distribution="$distribution_parent/benchbase-oceanbase"

cleanup() {
  local status=$?
  trap - EXIT
  timeout --foreground --kill-after=10s 180s \
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf -- "$distribution_parent"
  return "$status"
}
trap cleanup EXIT

run_case() {
  local name=$1 isolation=$2 mode=$3 drop_session=$4
  local expected_outcome=$5 expected_exit=$6 second_value=$7
  set +e
  BLACKBOX_BACKEND=oceanbase_mysql \
  BLACKBOX_CORRECTNESS_RUN_OUTPUT="$output" \
  BLACKBOX_MONITOR_BIN="$monitor_bin" \
  BLACKBOX_BENCHBASE_DIST="$distribution" \
  BLACKBOX_STACK_TIMEOUT_SECONDS=600 \
    bash "$root/tests/test_semantic_correctness.sh" --run-case \
      "$name" "$isolation" "$mode" "$drop_session" "$expected_outcome" "$expected_exit" \
      old "$second_value" ycsb_fractured_read_probe
  local status=$?
  set -e
  (( status == 0 ))
  "${compose[@]}" down --volumes --remove-orphans \
    >"$output/$name-teardown.log" 2>&1
}

run_case fractured_repeatable_read REPEATABLE_READ fractured '' PASS 0 old
run_case fractured_read_committed READ_COMMITTED fractured '' VIOLATION 3 new
run_case dropped_client_record REPEATABLE_READ clean ycsb/worker/1 INVALID 4 old

cp "$root/connector-contract/oceanbase_mysql/provenance.lock.json" \
  "$output/provenance.lock.json"
(
  cd "$output"
  checksum_file=$(mktemp /tmp/oceanbase-mysql-semantic-checksums.XXXXXX)
  find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum \
    >"$checksum_file"
  mv "$checksum_file" SHA256SUMS
  sha256sum --check SHA256SUMS >/dev/null
)
trap - EXIT
rm -rf -- "$distribution_parent"
