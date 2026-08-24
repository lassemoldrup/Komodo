#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
TEST_ROOT=$(mktemp -d /tmp/blackbox-backend-driver-test.XXXXXX)
trap 'rm -r -- "$TEST_ROOT"' EXIT
mkdir -p "$TEST_ROOT/drivers"

cat >"$TEST_ROOT/drivers/template.sh" <<'DRIVER'
blackbox_backend_initialize() {
  BLACKBOX_COMPOSE_PROFILE=$BLACKBOX_BACKEND
  BLACKBOX_DATABASE_SERVICE=database
  BLACKBOX_DATABASE_CONTAINER=database-container
  BLACKBOX_CONNECTOR_SERVICE=connector
  BLACKBOX_CONNECTOR_CONTAINER=connector-container
  BLACKBOX_DB_PORT=1
  BLACKBOX_DB_USER=application
  BLACKBOX_DB_PASSWORD=password
  BLACKBOX_BACKEND_SOURCE_ID=${BLACKBOX_BACKEND_SOURCE_ID:-$MOCK_SOURCE_ID}
}
mock_driver_gate() { [[ ${FAIL_DRIVER_OPERATION:-} != "$1" ]]; }
blackbox_backend_prepare_run() { mock_driver_gate prepare_run; }
blackbox_backend_require_commands() { :; }
blackbox_backend_database() { :; }
blackbox_backend_database_query() { mock_driver_gate database_query && printf '0\n'; }
blackbox_backend_capture_schema_metadata() { printf 'mock\n'; }
blackbox_backend_verify_schema() { mock_driver_gate verify_schema; }
blackbox_backend_count_table() { mock_driver_gate count_table && printf '0\n'; }
blackbox_backend_commit_fence() {
  mock_driver_gate commit_fence || return 1
  printf 'fence:%s\n' "$2" >>"$MOCK_EVENT_LOG"
}
blackbox_backend_active_external_connections() {
  mock_driver_gate active_external_connections && printf '0\n'
}
blackbox_backend_wait_for_connector_progress() { mock_driver_gate connector_progress; }
blackbox_backend_capture_attestation_inputs() {
  mock_driver_gate capture_attestation || return 1
  python3 "$BLACKBOX_RECORD_TOOL" attestation-tables \
    --benchmark "$BLACKBOX_BENCHMARK" >"$1"
  jq '.backend_facts' "$MOCK_ATTESTATION_EXAMPLE" >"$2"
}
blackbox_backend_start_database_schema() {
  mock_driver_gate start_database_schema && printf 'stack:database\n' >>"$MOCK_EVENT_LOG"
}
blackbox_backend_start_uninstrumented_database() {
  mock_driver_gate start_uninstrumented_database &&
    printf 'stack:uninstrumented-database\n' >>"$MOCK_EVENT_LOG"
}
blackbox_backend_verify_session_isolation() {
  mock_driver_gate verify_session_isolation
}
blackbox_backend_start_connector() {
  mock_driver_gate start_connector && printf 'stack:connector\n' >>"$MOCK_EVENT_LOG"
}
blackbox_backend_teardown() {
  mock_driver_gate teardown && printf 'stack:teardown\n' >>"$MOCK_EVENT_LOG"
}
blackbox_backend_status() { mock_driver_gate status && printf 'status:healthy\n'; }
DRIVER
cp "$TEST_ROOT/drivers/template.sh" "$TEST_ROOT/drivers/mariadb.sh"
cp "$TEST_ROOT/drivers/template.sh" "$TEST_ROOT/drivers/yugabytedb_ysql.sh"

run_backend() {
  local backend=$1 source_id=$2 example=$3 output=$4
  BLACKBOX_BACKEND="$backend" \
  BLACKBOX_BACKEND_SOURCE_ID='' \
  BLACKBOX_BACKEND_DRIVER_DIR="$TEST_ROOT/drivers" \
  MOCK_SOURCE_ID="$source_id" \
  MOCK_ATTESTATION_EXAMPLE="$example" \
  MOCK_EVENT_LOG="$output" \
  ROOT="$ROOT" \
    bash -eu -o pipefail <<'SCRIPT'
. "$ROOT/scripts/blackbox_lifecycle.sh"

[[ $BLACKBOX_DOCKER == docker ]]
BLACKBOX_DOCKER=true
BLACKBOX_ACTIVE_RUN_ID=00112233-4455-6677-8899-aabbccddeeff
BLACKBOX_SESSION_IDS=(ycsb/worker/1)
BLACKBOX_ADAPTER_CONTRACT=$(blackbox_adapter_contract)
BLACKBOX_BACKEND_CONTRACT=$(blackbox_backend_contract)
blackbox_backend_prepare_run
blackbox_backend_teardown
blackbox_backend_start_database_schema
blackbox_backend_verify_schema
blackbox_backend_start_connector
blackbox_backend_wait_for_connector_progress
blackbox_build_run_manifest >/dev/null
printf 'record:run_manifest\n' >>"$MOCK_EVENT_LOG"
startup=$(blackbox_build_schema_attestation STARTUP)
printf 'record:schema_attestation:STARTUP\n' >>"$MOCK_EVENT_LOG"
blackbox_backend_commit_fence "$BLACKBOX_ACTIVE_RUN_ID" RUN_START nonce
printf 'state:START_FENCE_SEEN\nphase:loader\nrecord:bootstrap_manifest\n' >>"$MOCK_EVENT_LOG"
blackbox_backend_commit_fence "$BLACKBOX_ACTIVE_RUN_ID" LOAD_FINISHED nonce
printf 'state:LOAD_FINISHED_SEEN\nphase:workload\nrecord:session_done\n' >>"$MOCK_EVENT_LOG"
blackbox_backend_commit_fence "$BLACKBOX_ACTIVE_RUN_ID" END_FENCE nonce
final=$(blackbox_build_schema_attestation FINAL)
jq -en --argjson startup "$startup" --argjson final "$final" '
  $startup.tables == $final.tables and
  $startup.backend_facts == $final.backend_facts and
  $startup.canonical_attestation_digest == $final.canonical_attestation_digest
' >/dev/null
printf 'record:schema_attestation:FINAL\nstate:RECONCILED\nstate:FINAL\n' >>"$MOCK_EVENT_LOG"
SCRIPT
}

run_backend mariadb mariadb-main \
  "$ROOT/protocol/examples/schema-attestation-startup.json" \
  "$TEST_ROOT/mariadb.events"
run_backend yugabytedb_ysql yugabytedb-ysql-main \
  "$ROOT/protocol/examples/backends/yugabytedb-ysql-schema-attestation.json" \
  "$TEST_ROOT/yugabyte.events"

diff -u "$TEST_ROOT/mariadb.events" "$TEST_ROOT/yugabyte.events"

export ROOT
MOCK_FAILURE_EXAMPLE="$ROOT/protocol/examples/schema-attestation-startup.json"
export MOCK_FAILURE_EXAMPLE
for operation in prepare_run database_query verify_schema count_table commit_fence \
  active_external_connections connector_progress capture_attestation start_database_schema \
  start_connector teardown status; do
  if BLACKBOX_BACKEND=mariadb BLACKBOX_BACKEND_DRIVER_DIR="$TEST_ROOT/drivers" \
      MOCK_SOURCE_ID=mariadb-main \
      MOCK_ATTESTATION_EXAMPLE="$MOCK_FAILURE_EXAMPLE" \
      MOCK_EVENT_LOG="$TEST_ROOT/failure.events" FAIL_DRIVER_OPERATION="$operation" \
      TEST_OPERATION="$operation" bash -eu -o pipefail <<'SCRIPT'
. "$ROOT/scripts/blackbox_lifecycle.sh"
case "$TEST_OPERATION" in
  prepare_run) blackbox_backend_prepare_run ;;
  database_query) blackbox_backend_database_query 'SELECT 1' ;;
  verify_schema) blackbox_backend_verify_schema ;;
  count_table) blackbox_backend_count_table benchbase.usertable ;;
  commit_fence) blackbox_backend_commit_fence run RUN_START nonce ;;
  active_external_connections) blackbox_backend_active_external_connections ;;
  connector_progress) blackbox_backend_wait_for_connector_progress ;;
  capture_attestation)
    blackbox_backend_capture_attestation_inputs /tmp/unused-tables /tmp/unused-facts ;;
  start_database_schema) blackbox_backend_start_database_schema ;;
  start_connector) blackbox_backend_start_connector ;;
  teardown) blackbox_backend_teardown ;;
  status) blackbox_backend_status ;;
esac
SCRIPT
  then
    echo "backend driver failure did not propagate: $operation" >&2
    exit 1
  fi
done

echo "backend driver contract tests passed"
