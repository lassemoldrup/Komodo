#!/usr/bin/env bash
# Live integration against the pinned MariaDB/Debezium/Redis stack.
set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source=scripts/blackbox_lifecycle.sh
. "$ROOT/scripts/blackbox_lifecycle.sh"

STACK_TIMEOUT_SECONDS=${STACK_TIMEOUT_SECONDS:-180}
OWN_CAPTURE=0
if [[ -n ${BLACKBOX_CAPTURE_PATH:-} ]]; then
  CAPTURE_PATH=$BLACKBOX_CAPTURE_PATH
else
  CAPTURE_PATH=$(mktemp /tmp/mariadb-lifecycle-stream.XXXXXX.json)
  OWN_CAPTURE=1
fi
SCAN_PATH=$(mktemp /tmp/mariadb-bootstrap-scan.XXXXXX)


compose() {
  docker compose --file "$ROOT/docker-compose.yml" --profile mariadb "$@"
}


cleanup() {
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -f -- "$SCAN_PATH"
  if (( OWN_CAPTURE == 1 )); then
    rm -f -- "$CAPTURE_PATH"
  fi
}
trap cleanup EXIT


wait_healthy() {
  local container=$1
  local deadline=$(( $(date +%s) + STACK_TIMEOUT_SECONDS ))
  while (( $(date +%s) <= deadline )); do
    local status
    status=$(docker inspect --format '{{.State.Health.Status}}' "$container" 2>/dev/null || true)
    [[ $status == healthy ]] && return 0
    [[ $status == unhealthy ]] && return 1
    sleep 1
  done
  return 1
}


wait_streaming() {
  local deadline=$(( $(date +%s) + STACK_TIMEOUT_SECONDS ))
  while (( $(date +%s) <= deadline )); do
    local logs
    logs=$(docker logs cdc-debezium 2>&1 || true)
    [[ $(grep -c 'Starting streaming' <<<"$logs") -gt 0 ]] && return 0
    sleep 1
  done
  return 1
}


wait_end_fence() {
  local deadline=$(( $(date +%s) + STACK_TIMEOUT_SECONDS ))
  while (( $(date +%s) <= deadline )); do
    if blackbox_redis --raw XRANGE cdc - + | grep -Fq "$BLACKBOX_ACTIVE_RUN_ID"; then
      if blackbox_redis --raw XRANGE cdc - + | grep -Fq END_FENCE; then
        return 0
      fi
    fi
    sleep 0.2
  done
  return 1
}


main() {
  blackbox_require_command docker
  blackbox_require_command "$BLACKBOX_REDIS_CLI"
  blackbox_require_command "$BLACKBOX_MARIADB_CLI"
  BLACKBOX_ACTIVE_RUN_ID=$(blackbox_new_uuid)
  BLACKBOX_SESSION_IDS=(ycsb/worker/1)
  export BLACKBOX_ACTIVE_RUN_ID

  compose down --volumes --remove-orphans
  compose up --detach --force-recreate redis mariadb
  wait_healthy cdc-redis
  wait_healthy cdc-mariadb

  [[ $(blackbox_database_query "SELECT COUNT(*) FROM usertable" | tr -d '[:space:]') == 0 ]]
  [[ $(blackbox_database_query "SELECT COUNT(*) FROM checker_run_fence" | tr -d '[:space:]') == 0 ]]
  [[ $(blackbox_database_query "SELECT COUNT(*) FROM session_txn" | tr -d '[:space:]') == 0 ]]
  [[ $(blackbox_redis --raw DBSIZE | tr -d '[:space:]') == 0 ]]
  if blackbox_database_query "CREATE TABLE forbidden_after_init (id INT)" >/dev/null 2>&1; then
    echo "app user unexpectedly retained DDL privilege" >&2
    return 1
  fi

  blackbox_database_query "SELECT SLEEP(2)" >/dev/null &
  local sleeper_pid=$!
  sleep 0.2
  [[ $(blackbox_active_application_connections) -ge 1 ]]
  wait "$sleeper_pid"

  compose up --detach --no-deps --force-recreate debezium
  wait_streaming

  local run_manifest
  run_manifest=$(blackbox_build_run_manifest)
  blackbox_publish_record "$(blackbox_client_stream)" "$run_manifest" >/dev/null
  blackbox_commit_fence RUN_START

  blackbox_database_query "
    START TRANSACTION;
    INSERT INTO usertable
      (ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10)
    VALUES
      (42, NULL, '', 'A', 'é', '中', '0', 'hello', '🙂', CONCAT('a', CHAR(0), 'b'), 'last'),
      (-1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    COMMIT;
  " >/dev/null

  blackbox_capture_bootstrap_scan "$SCAN_PATH"
  local bootstrap_manifest bootstrap_digest bootstrap_count
  bootstrap_manifest=$(blackbox_build_bootstrap_manifest "$SCAN_PATH")
  bootstrap_digest=$(jq -r '.canonical_state_digest' <<<"$bootstrap_manifest")
  bootstrap_count=$(jq -r '.table_row_counts[0].row_count' <<<"$bootstrap_manifest")
  [[ $bootstrap_digest == 71749720e062cf361dbad1e139c88d7d39b10debeddf18562d35cd80ae53afa8 ]]
  [[ $bootstrap_count == 2 ]]
  blackbox_publish_record "$(blackbox_client_stream)" "$bootstrap_manifest" >/dev/null
  blackbox_commit_fence LOAD_FINISHED

  # Loader/bootstrap activity must not create workload marker state.
  [[ $(blackbox_database_query "SELECT COUNT(*) FROM session_txn" | tr -d '[:space:]') == 0 ]]

  blackbox_database_query "
    START TRANSACTION;
    UPDATE usertable SET field1 = 'changed' WHERE ycsb_key = 42;
    INSERT INTO session_txn (session_id, txn_id) VALUES ('ycsb/worker/1', 1)
      ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
    COMMIT;
  " >/dev/null
  blackbox_commit_fence END_FENCE

  wait_end_fence
  blackbox_redis --json XRANGE cdc - + >"$CAPTURE_PATH"
  python3 "$ROOT/tests/verify_lifecycle_stream.py" \
    --input "$CAPTURE_PATH" --run-id "$BLACKBOX_ACTIVE_RUN_ID" >/dev/null

  printf 'live MariaDB lifecycle test passed (run_id=%s, capture=%s)\n' \
    "$BLACKBOX_ACTIVE_RUN_ID" "$CAPTURE_PATH"
}


main "$@"
