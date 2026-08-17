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
REPLAY_PATH=$(mktemp /tmp/mariadb-cdc-replay.XXXXXX.json)
INGEST_PATH=$(mktemp /tmp/mariadb-cdc-ingest.XXXXXX.json)


compose() {
  docker compose --file "$ROOT/docker-compose.yml" --profile mariadb "$@"
}


cleanup() {
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -f -- "$SCAN_PATH"
  rm -f -- "$REPLAY_PATH"
  rm -f -- "$INGEST_PATH"
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
  if [[ -z ${BLACKBOX_CDC_REPLAY_BIN:-} || ! -x $BLACKBOX_CDC_REPLAY_BIN ]]; then
    echo "BLACKBOX_CDC_REPLAY_BIN must name the built C++ CDC replay tool" >&2
    return 1
  fi
  if [[ -z ${BLACKBOX_CDC_INGEST_BIN:-} || ! -x $BLACKBOX_CDC_INGEST_BIN ]]; then
    echo "BLACKBOX_CDC_INGEST_BIN must name the built C++ CDC ingestion tool" >&2
    return 1
  fi
  BLACKBOX_ACTIVE_RUN_ID=${BLACKBOX_ACTIVE_RUN_ID:-$(blackbox_new_uuid)}
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

  # Marker-only commit: successful no-application-write transactions must
  # still occupy a CDC slot after marker filtering.
  blackbox_database_query "
    START TRANSACTION;
    INSERT INTO session_txn (session_id, txn_id) VALUES ('ycsb/worker/1', 2)
      ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
    COMMIT;
  " >/dev/null

  # Preserve touched keys even when repeated writes collapse to Absent.
  blackbox_database_query "
    START TRANSACTION;
    INSERT INTO usertable
      (ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10)
    VALUES (100, 'first', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    UPDATE usertable SET field1 = 'second' WHERE ycsb_key = 100;
    UPDATE usertable SET field1 = 'first' WHERE ycsb_key = 100;
    DELETE FROM usertable WHERE ycsb_key = 100;
    INSERT INTO session_txn (session_id, txn_id) VALUES ('ycsb/worker/1', 3)
      ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
    COMMIT;
  " >/dev/null

  blackbox_database_query "
    START TRANSACTION;
    INSERT INTO usertable
      (ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10)
    VALUES (7, 'original', '', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'tail');
    INSERT INTO session_txn (session_id, txn_id) VALUES ('ycsb/worker/1', 4)
      ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
    COMMIT;
  " >/dev/null

  # Same-key updates that return to the original value remain a touched write.
  blackbox_database_query "
    START TRANSACTION;
    UPDATE usertable SET field1 = 'temporary' WHERE ycsb_key = 7;
    UPDATE usertable SET field1 = 'original' WHERE ycsb_key = 7;
    INSERT INTO session_txn (session_id, txn_id) VALUES ('ycsb/worker/1', 5)
      ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
    COMMIT;
  " >/dev/null

  blackbox_database_query "
    START TRANSACTION;
    DELETE FROM usertable WHERE ycsb_key = 7;
    INSERT INTO session_txn (session_id, txn_id) VALUES ('ycsb/worker/1', 6)
      ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
    COMMIT;
  " >/dev/null

  # Exercise the explicit primary-key-change normalization rule.
  blackbox_database_query "
    START TRANSACTION;
    UPDATE usertable SET ycsb_key = -2 WHERE ycsb_key = -1;
    INSERT INTO session_txn (session_id, txn_id) VALUES ('ycsb/worker/1', 7)
      ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
    COMMIT;
  " >/dev/null
  blackbox_commit_fence END_FENCE

  wait_end_fence
  "$BLACKBOX_CDC_INGEST_BIN" \
    --redis-host "$BLACKBOX_REDIS_HOST" \
    --redis-port "$BLACKBOX_REDIS_PORT" \
    --run-id "$BLACKBOX_ACTIVE_RUN_ID" >"$INGEST_PATH"
  jq -e '
    .completed_transactions == 11 and
    .client_records_seen == 2 and
    [.transactions[].cdc_seq] == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10] and
    [.transactions[] | select(.kind == "RUN_CONTROL") | .fence_phase] ==
      ["RUN_START", "LOAD_FINISHED", "END_FENCE"] and
    [.transactions[3:10][].correlation_id.txn_id] == [1, 2, 3, 4, 5, 6, 7] and
    [.transactions[3:10][].write_count] == [1, 0, 1, 1, 1, 1, 2]
  ' "$INGEST_PATH" >/dev/null
  blackbox_redis --json XRANGE cdc - + >"$CAPTURE_PATH"
  python3 "$ROOT/tests/verify_lifecycle_stream.py" \
    --input "$CAPTURE_PATH" --run-id "$BLACKBOX_ACTIVE_RUN_ID" >/dev/null
  "$BLACKBOX_CDC_REPLAY_BIN" \
    --input "$CAPTURE_PATH" --run-id "$BLACKBOX_ACTIVE_RUN_ID" >"$REPLAY_PATH"
  jq -e '
    .completed_transactions == 11 and
    [.transactions[].kind] == [
      "RUN_CONTROL", "BOOTSTRAP_APP", "RUN_CONTROL",
      "WORKLOAD_MARKED", "WORKLOAD_MARKED", "WORKLOAD_MARKED",
      "WORKLOAD_MARKED", "WORKLOAD_MARKED", "WORKLOAD_MARKED",
      "WORKLOAD_MARKED", "RUN_CONTROL"
    ] and
    [.transactions[3:10][].correlation_id.txn_id] == [1, 2, 3, 4, 5, 6, 7] and
    [.transactions[3:10][].writes | length] == [1, 0, 1, 1, 1, 1, 2] and
    .transactions[4].writes == [] and
    .transactions[5].writes[0].value_state == "absent" and
    .transactions[7].writes[0].value_hex == .transactions[6].writes[0].value_hex and
    [.transactions[9].writes[].value_state] == ["present", "absent"]
  ' "$REPLAY_PATH" >/dev/null

  printf 'live MariaDB lifecycle test passed (run_id=%s, capture=%s)\n' \
    "$BLACKBOX_ACTIVE_RUN_ID" "$CAPTURE_PATH"
}


main "$@"
