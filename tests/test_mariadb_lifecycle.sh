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
INGEST_ERROR_PATH=$(mktemp /tmp/mariadb-cdc-ingest.XXXXXX.log)
PREFIX_INGEST_PATH=$(mktemp /tmp/mariadb-prefix-ingest.XXXXXX.json)
PREFIX_ERROR_PATH=$(mktemp /tmp/mariadb-prefix-ingest.XXXXXX.log)
BAD_PREFIX_ERROR_PATH=$(mktemp /tmp/mariadb-prefix-invalid.XXXXXX.log)
UNKNOWN_INGEST_PATH=$(mktemp /tmp/mariadb-cdc-unknown.XXXXXX.json)
UNKNOWN_ERROR_PATH=$(mktemp /tmp/mariadb-cdc-unknown.XXXXXX.log)
DDL_INGEST_PATH=$(mktemp /tmp/mariadb-cdc-ddl.XXXXXX.json)
DDL_ERROR_PATH=$(mktemp /tmp/mariadb-cdc-ddl.XXXXXX.log)
INGEST_PID=
PREFIX_PID=


compose() {
  docker compose --file "$ROOT/docker-compose.yml" --profile mariadb "$@"
}


cleanup() {
  if [[ -n $INGEST_PID ]] && kill -0 "$INGEST_PID" >/dev/null 2>&1; then
    kill "$INGEST_PID" >/dev/null 2>&1 || true
    wait "$INGEST_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n $PREFIX_PID ]] && kill -0 "$PREFIX_PID" >/dev/null 2>&1; then
    kill "$PREFIX_PID" >/dev/null 2>&1 || true
    wait "$PREFIX_PID" >/dev/null 2>&1 || true
  fi
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -f -- "$SCAN_PATH"
  rm -f -- "$REPLAY_PATH"
  rm -f -- "$INGEST_PATH"
  rm -f -- "$INGEST_ERROR_PATH"
  rm -f -- "$PREFIX_INGEST_PATH"
  rm -f -- "$PREFIX_ERROR_PATH"
  rm -f -- "$BAD_PREFIX_ERROR_PATH"
  rm -f -- "$UNKNOWN_INGEST_PATH"
  rm -f -- "$UNKNOWN_ERROR_PATH"
  rm -f -- "$DDL_INGEST_PATH"
  rm -f -- "$DDL_ERROR_PATH"
  if (( OWN_CAPTURE == 1 )); then
    rm -f -- "$CAPTURE_PATH"
  fi
}
trap cleanup EXIT


root_database_query() {
  docker exec cdc-mariadb mariadb \
    --skip-ssl \
    --user=root \
    --password="${MARIADB_ROOT_PASSWORD:-rootpw}" \
    --database=benchbase \
    --default-character-set=utf8mb4 \
    --batch \
    --raw \
    --skip-column-names \
    --execute "$1"
}


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


reset_database_and_redis() {
  compose down --volumes --remove-orphans
  compose up --detach --force-recreate redis mariadb
  wait_healthy cdc-redis
  wait_healthy cdc-mariadb
}


start_connector_with_snapshot_schema() {
  local expected_table=${1:-}
  compose up --detach --no-deps --force-recreate debezium
  wait_streaming

  # include.schema.changes=true emits a schema-only connector snapshot even
  # with snapshot.mode=no_data. Retain it in cdc and prove it contains only the
  # pinned pre-run snapshot shape; the assembler validates the same boundary.
  local deadline=$(( $(date +%s) + STACK_TIMEOUT_SECONDS ))
  local startup_capture=
  while (( $(date +%s) <= deadline )); do
    startup_capture=$(blackbox_redis --json XRANGE cdc - +)
    if jq -e '
      any(.[];
        ((.[1][1] | fromjson?) // {}) as $event |
        $event.source.snapshot? == "last")
    ' <<<"$startup_capture" >/dev/null; then
      break
    fi
    sleep 0.1
  done
  if [[ -z $startup_capture ]] ||
    ! jq -e \
    --arg expected_table "$expected_table" '
      [ .[] | {
          key: ((.[1][0] | fromjson?) // {}),
          event: ((.[1][1] | fromjson?) // {})
        }
      ] as $records |
      ($records | length) > 0 and
      all($records[];
        (.key | keys) == ["databaseName"] and
        (.event | keys | sort) ==
          (["databaseName", "ddl", "schemaName", "source", "tableChanges", "ts_ms"] | sort) and
        .key.databaseName == .event.databaseName and
        (.event.source.snapshot == "true" or
          .event.source.snapshot == "last") and
        .event.source.version == "3.6.1.Final" and
        .event.source.connector == "mariadb" and
        .event.source.name == "mariadb-main" and
        (.event.ddl | type) == "string" and
        (.event.tableChanges | type) == "array") and
      ([ $records[].event.source.snapshot ] | map(select(. == "last")) | length) == 1 and
      $records[-1].event.source.snapshot == "last" and
      any($records[].event.tableChanges[]?;
        .id == "\"benchbase\".\"usertable\"") and
      any($records[].event.tableChanges[]?;
        .id == "\"benchbase\".\"checker_run_fence\"") and
      any($records[].event.tableChanges[]?;
        .id == "\"benchbase\".\"session_txn\"") and
      ($expected_table == "" or
        any($records[].event.tableChanges[]?;
          .id == ("\"benchbase\".\"" + $expected_table + "\"")))
    ' <<<"$startup_capture" >/dev/null; then
    echo "Debezium startup evidence was not the pinned schema snapshot" >&2
    blackbox_redis --json XRANGE cdc - + >&2 || true
    return 1
  fi
}


wait_ingest_blocked() {
  local deadline=$(( $(date +%s) + 10 ))
  while (( $(date +%s) <= deadline )); do
    if blackbox_redis --raw CLIENT LIST | awk '
      {
        blocked = 0
        xread = 0
        for (field = 1; field <= NF; ++field) {
          if ($field ~ /^flags=[^ ]*b/) blocked = 1
          if ($field == "cmd=xread") xread = 1
        }
        if (blocked && xread) found = 1
      }
      END { exit(found ? 0 : 1) }
    '; then
      return 0
    fi
    if [[ -n $INGEST_PID ]] && ! kill -0 "$INGEST_PID" >/dev/null 2>&1; then
      return 1
    fi
    sleep 0.05
  done
  return 1
}


start_live_ingest() {
  local output_path=$1
  local error_path=$2
  "$BLACKBOX_CDC_INGEST_BIN" \
    --redis-host "$BLACKBOX_REDIS_HOST" \
    --redis-port "$BLACKBOX_REDIS_PORT" \
    --run-id "$BLACKBOX_ACTIVE_RUN_ID" \
    --overall-timeout-ms "$(( STACK_TIMEOUT_SECONDS * 1000 ))" \
    >"$output_path" 2>"$error_path" &
  INGEST_PID=$!
  if ! wait_ingest_blocked; then
    cat "$error_path" >&2
    echo "live CDC ingestion did not enter blocking XREAD" >&2
    return 1
  fi
}


wait_ingest_exit() {
  local expected_status=$1
  local error_path=$2
  local status
  if wait "$INGEST_PID"; then
    status=0
  else
    status=$?
  fi
  INGEST_PID=
  if (( status != expected_status )); then
    cat "$error_path" >&2
    echo "live CDC ingestion exited with $status, expected $expected_status" >&2
    return 1
  fi
}


wait_prefix_blocked() {
  local deadline=$(( $(date +%s) + 10 ))
  while (( $(date +%s) <= deadline )); do
    local blocked_readers
    blocked_readers=$(blackbox_redis --raw CLIENT LIST | awk '
      {
        blocked = 0
        xread = 0
        for (field = 1; field <= NF; ++field) {
          if ($field ~ /^flags=[^ ]*b/) blocked = 1
          if ($field == "cmd=xread") xread = 1
        }
        if (blocked && xread) count += 1
      }
      END { print count + 0 }
    ')
    if (( blocked_readers >= 2 )); then
      return 0
    fi
    if [[ -n $PREFIX_PID ]] && ! kill -0 "$PREFIX_PID" >/dev/null 2>&1; then
      return 1
    fi
    sleep 0.05
  done
  return 1
}


start_live_prefix() {
  local client_cursor=$1
  local bootstrap_count=$2
  local bootstrap_digest=$3
  "$BLACKBOX_PREFIX_INGEST_BIN" \
    --redis-host "$BLACKBOX_REDIS_HOST" \
    --redis-port "$BLACKBOX_REDIS_PORT" \
    --run-id "$BLACKBOX_ACTIVE_RUN_ID" \
    --client-cursor "$client_cursor" \
    --expected-session-id ycsb/worker/1 \
    --approved-load-cdc-seq 2 \
    --approved-bootstrap-transactions 1 \
    --approved-bootstrap-rows "$bootstrap_count" \
    --approved-bootstrap-digest "$bootstrap_digest" \
    --overall-timeout-ms "$(( STACK_TIMEOUT_SECONDS * 1000 ))" \
    >"$PREFIX_INGEST_PATH" 2>"$PREFIX_ERROR_PATH" &
  PREFIX_PID=$!
  if ! wait_prefix_blocked; then
    cat "$PREFIX_ERROR_PATH" >&2
    echo "live prefix ingestion did not enter blocking XREAD" >&2
    return 1
  fi
}


wait_prefix_exit() {
  local expected_status=$1
  local status
  if wait "$PREFIX_PID"; then
    status=0
  else
    status=$?
  fi
  PREFIX_PID=
  if (( status != expected_status )); then
    cat "$PREFIX_ERROR_PATH" >&2
    echo "live prefix ingestion exited with $status, expected $expected_status" >&2
    return 1
  fi
}


wait_cdc_fence() {
  local phase=$1
  local deadline=$(( $(date +%s) + STACK_TIMEOUT_SECONDS ))
  while (( $(date +%s) <= deadline )); do
    if blackbox_redis --json XRANGE cdc - + | jq -e \
      --arg run_id "$BLACKBOX_ACTIVE_RUN_ID" --arg phase "$phase" '
        any(.[];
          ((.[1][1] | fromjson?) // {}) as $event |
          $event.op? == "c" and
          $event.source.table? == "checker_run_fence" and
          $event.after.run_id? == $run_id and
          $event.after.phase? == $phase)
      ' >/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}


wait_end_fence() {
  wait_cdc_fence END_FENCE
}


wait_unknown_table_row() {
  local deadline=$(( $(date +%s) + STACK_TIMEOUT_SECONDS ))
  while (( $(date +%s) <= deadline )); do
    if blackbox_redis --json XRANGE cdc - + | jq -e '
      [ .[] | ((.[1][1] | fromjson?) // {}) ] as $events |
      first(range(0; $events | length) | select(
        $events[.].op? != null and
        $events[.].source.table? == "session_txn")) as $marker_index |
      first(range(0; $events | length) | select(
        $events[.].op? != null and
        $events[.].source.table? == "unmodeled_probe")) as $unknown_index |
      $marker_index < $unknown_index and
      $events[$marker_index].transaction.id ==
        $events[$unknown_index].transaction.id
    ' >/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}


wait_ddl_record() {
  local deadline=$(( $(date +%s) + STACK_TIMEOUT_SECONDS ))
  while (( $(date +%s) <= deadline )); do
    if blackbox_redis --json XRANGE cdc - + | jq -e '
      any(.[];
        ((.[1][1] | fromjson?) // {}) as $event |
        ($event.ddl? // "") | contains("ddl_probe"))
    ' >/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}


publish_client_txn() {
  local session_id=$1
  local session_seq=$2
  local txn_id=$3
  local read_mode=${4:-empty}
  local logical_txn_id attempt_id payload read_digest reads
  logical_txn_id=$(blackbox_new_uuid)
  attempt_id=$(blackbox_new_uuid)
  read_digest=3f251669e36904bc1a3bc472991c9ff89f7f23f6b98bad9dd33f0e66884a06ce
  reads='[]'
  if [[ $read_mode == mixed42 ]]; then
    read_digest=29d6e7dd6ef4be6c23d571132831b39d8aaff04ce0e003dd4eb37940c6d64a8f
    reads='[
      {
        "event_seq": 0,
        "key_bytes_base64": "AAAACWJlbmNoYmFzZQAAAAl1c2VydGFibGUAAAAq",
        "value_state": "present",
        "value_bytes_base64": "AAEAAAAAAQAAAAFBAQAAAALDqQEAAAAD5LitAQAAAAEwAQAAAAVoZWxsbwEAAAAE8J+ZggEAAAADYQBiAQAAAARsYXN0",
        "source": "snapshot"
      }
    ]'
  elif [[ $read_mode != empty ]]; then
    echo "unknown client read mode: $read_mode" >&2
    return 1
  fi
  payload=$(jq -cn \
    --arg run_id "$BLACKBOX_ACTIVE_RUN_ID" \
    --arg session_id "$session_id" \
    --arg logical_txn_id "$logical_txn_id" \
    --arg attempt_id "$attempt_id" \
    --arg read_digest "$read_digest" \
    --argjson reads "$reads" \
    --argjson session_seq "$session_seq" \
    --argjson txn_id "$txn_id" '
      {
        kind: "client_txn",
        run_id: $run_id,
        session_id: $session_id,
        session_seq: $session_seq,
        logical_txn_id: $logical_txn_id,
        attempt_id: $attempt_id,
        txn_type: "ycsb_update",
        correlation_kind: "session_txn",
        correlation_id: {
          run_id: $run_id,
          session_id: $session_id,
          txn_id: $txn_id
        },
        read_count: ($reads | length),
        read_digest: $read_digest,
        reads: $reads
      }
    ')
  blackbox_publish_record "$(blackbox_client_stream)" "$payload" >/dev/null
}


run_unknown_table_negative() {
  reset_database_and_redis
  root_database_query \
    "CREATE TABLE unmodeled_probe (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB" \
    >/dev/null
  start_connector_with_snapshot_schema unmodeled_probe

  BLACKBOX_ACTIVE_RUN_ID=$(blackbox_new_uuid)
  BLACKBOX_SESSION_IDS=(ycsb/worker/unknown-table)
  export BLACKBOX_ACTIVE_RUN_ID
  start_live_ingest "$UNKNOWN_INGEST_PATH" "$UNKNOWN_ERROR_PATH"
  sleep 0.2

  blackbox_publish_record \
    "$(blackbox_client_stream)" "$(blackbox_build_run_manifest)" >/dev/null
  wait_ingest_blocked
  blackbox_commit_fence RUN_START
  wait_cdc_fence RUN_START
  wait_ingest_blocked
  blackbox_commit_fence LOAD_FINISHED
  wait_cdc_fence LOAD_FINISHED
  wait_ingest_blocked

  # The unknown-table row deliberately follows the marker. With connector-side
  # table filtering this would make the marker falsely appear final.
  blackbox_database_query "
    START TRANSACTION;
    INSERT INTO session_txn (session_id, txn_id)
      VALUES ('ycsb/worker/unknown-table', 1);
    INSERT INTO unmodeled_probe (id) VALUES (1);
    COMMIT;
  " >/dev/null
  wait_unknown_table_row
  wait_ingest_exit 2 "$UNKNOWN_ERROR_PATH"
  grep -Fq UNEXPECTED_TABLE "$UNKNOWN_ERROR_PATH"
}


run_ddl_negative() {
  reset_database_and_redis
  start_connector_with_snapshot_schema

  BLACKBOX_ACTIVE_RUN_ID=$(blackbox_new_uuid)
  BLACKBOX_SESSION_IDS=(ycsb/worker/ddl)
  export BLACKBOX_ACTIVE_RUN_ID
  start_live_ingest "$DDL_INGEST_PATH" "$DDL_ERROR_PATH"
  sleep 0.2

  blackbox_publish_record \
    "$(blackbox_client_stream)" "$(blackbox_build_run_manifest)" >/dev/null
  wait_ingest_blocked
  blackbox_commit_fence RUN_START
  wait_cdc_fence RUN_START
  wait_ingest_blocked

  root_database_query \
    "CREATE TABLE ddl_probe (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB" \
    >/dev/null
  wait_ddl_record
  wait_ingest_exit 2 "$DDL_ERROR_PATH"
  grep -Fq DDL_OBSERVED "$DDL_ERROR_PATH"
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
  if [[ -z ${BLACKBOX_PREFIX_INGEST_BIN:-} || ! -x $BLACKBOX_PREFIX_INGEST_BIN ]]; then
    echo "BLACKBOX_PREFIX_INGEST_BIN must name the built C++ prefix ingestion tool" >&2
    return 1
  fi
  BLACKBOX_ACTIVE_RUN_ID=${BLACKBOX_ACTIVE_RUN_ID:-$(blackbox_new_uuid)}
  BLACKBOX_SESSION_IDS=(ycsb/worker/1)
  export BLACKBOX_ACTIVE_RUN_ID
  local positive_run_id=$BLACKBOX_ACTIVE_RUN_ID

  reset_database_and_redis

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

  start_connector_with_snapshot_schema
  start_live_ingest "$INGEST_PATH" "$INGEST_ERROR_PATH"
  # Let at least one finite blocking read time out before the first wake-up.
  sleep 0.2

  local run_manifest
  run_manifest=$(blackbox_build_run_manifest)
  blackbox_publish_record "$(blackbox_client_stream)" "$run_manifest" >/dev/null
  wait_ingest_blocked
  blackbox_commit_fence RUN_START
  wait_cdc_fence RUN_START
  wait_ingest_blocked

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
  local bootstrap_manifest bootstrap_digest bootstrap_count bootstrap_client_cursor
  bootstrap_manifest=$(blackbox_build_bootstrap_manifest "$SCAN_PATH")
  bootstrap_digest=$(jq -r '.canonical_state_digest' <<<"$bootstrap_manifest")
  bootstrap_count=$(jq -r '.table_row_counts[0].row_count' <<<"$bootstrap_manifest")
  [[ $bootstrap_digest == 71749720e062cf361dbad1e139c88d7d39b10debeddf18562d35cd80ae53afa8 ]]
  [[ $bootstrap_count == 2 ]]
  bootstrap_client_cursor=$(
    blackbox_publish_record "$(blackbox_client_stream)" "$bootstrap_manifest"
  )
  start_live_prefix "$bootstrap_client_cursor" "$bootstrap_count" "$bootstrap_digest"
  blackbox_commit_fence LOAD_FINISHED
  wait_cdc_fence LOAD_FINISHED
  wait_ingest_blocked

  # Loader/bootstrap activity must not create workload marker state.
  [[ $(blackbox_database_query "SELECT COUNT(*) FROM session_txn" | tr -d '[:space:]') == 0 ]]

  blackbox_database_query "
    START TRANSACTION;
    SELECT ycsb_key, field1, field2, field3, field4, field5,
           field6, field7, field8, field9, field10
      FROM usertable WHERE ycsb_key = 42;
    UPDATE usertable SET field1 = 'changed' WHERE ycsb_key = 42;
    INSERT INTO session_txn (session_id, txn_id) VALUES ('ycsb/worker/1', 1)
      ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
    COMMIT;
  " >/dev/null
  publish_client_txn ycsb/worker/1 1 1 mixed42

  # Marker-only commit: successful no-application-write transactions must
  # still occupy a CDC slot after marker filtering.
  blackbox_database_query "
    START TRANSACTION;
    INSERT INTO session_txn (session_id, txn_id) VALUES ('ycsb/worker/1', 2)
      ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
    COMMIT;
  " >/dev/null
  publish_client_txn ycsb/worker/1 2 2

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
  publish_client_txn ycsb/worker/1 3 3

  blackbox_database_query "
    START TRANSACTION;
    INSERT INTO usertable
      (ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10)
    VALUES (7, 'original', '', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'tail');
    INSERT INTO session_txn (session_id, txn_id) VALUES ('ycsb/worker/1', 4)
      ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
    COMMIT;
  " >/dev/null
  publish_client_txn ycsb/worker/1 4 4

  # Same-key updates that return to the original value remain a touched write.
  blackbox_database_query "
    START TRANSACTION;
    UPDATE usertable SET field1 = 'temporary' WHERE ycsb_key = 7;
    UPDATE usertable SET field1 = 'original' WHERE ycsb_key = 7;
    INSERT INTO session_txn (session_id, txn_id) VALUES ('ycsb/worker/1', 5)
      ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
    COMMIT;
  " >/dev/null
  publish_client_txn ycsb/worker/1 5 5

  blackbox_database_query "
    START TRANSACTION;
    DELETE FROM usertable WHERE ycsb_key = 7;
    INSERT INTO session_txn (session_id, txn_id) VALUES ('ycsb/worker/1', 6)
      ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
    COMMIT;
  " >/dev/null
  publish_client_txn ycsb/worker/1 6 6

  # Exercise the explicit primary-key-change normalization rule.
  blackbox_database_query "
    START TRANSACTION;
    UPDATE usertable SET ycsb_key = -2 WHERE ycsb_key = -1;
    INSERT INTO session_txn (session_id, txn_id) VALUES ('ycsb/worker/1', 7)
      ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
    COMMIT;
  " >/dev/null
  publish_client_txn ycsb/worker/1 7 7
  blackbox_commit_fence END_FENCE

  wait_end_fence
  wait_ingest_exit 0 "$INGEST_ERROR_PATH"
  wait_prefix_exit 0
  jq -e '
    .completed_transactions == 11 and
    .client_records_seen == 9 and
    .xread_calls > .nonempty_xread_calls and
    .nonempty_xread_calls >= 3 and
    [.transactions[].cdc_seq] == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10] and
    [.transactions[] | select(.kind == "RUN_CONTROL") | .fence_phase] ==
      ["RUN_START", "LOAD_FINISHED", "END_FENCE"] and
    [.transactions[3:10][].correlation_id.txn_id] == [1, 2, 3, 4, 5, 6, 7] and
    [.transactions[3:10][].write_count] == [1, 0, 1, 1, 1, 1, 2]
  ' "$INGEST_PATH" >/dev/null

  jq -e '
    .completed_cdc_transactions == 11 and
    .accepted_cdc_transactions == 11 and
    .accepted_client_transactions == 7 and
    .joined_workload_transactions == 7 and
    .checker_consumed_transactions == 9 and
    .checker_next_commit_seq == 9 and
    .bootstrap_checker_transactions == 1 and
    .phase_anchor_transactions == 1 and
    .workload_checker_transactions == 7 and
    .bootstrap_state_rows == 2 and
    .bootstrap_state_bytes > 0 and
    .pending_client_records == 0 and
    .pending_cdc_slots == 1 and
    (.client_first_matches + .cdc_first_matches) == 7 and
    .end_fence_observed == true and
    .checker_observation == "PASS_SO_FAR" and
    .head_block_reason == "END_FENCE_DEFERRED" and
    .client_records_read == 7 and
    .xread_calls > 1
  ' "$PREFIX_INGEST_PATH" >/dev/null

  local bad_prefix_status
  if "$BLACKBOX_PREFIX_INGEST_BIN" \
    --redis-host "$BLACKBOX_REDIS_HOST" \
    --redis-port "$BLACKBOX_REDIS_PORT" \
    --run-id "$BLACKBOX_ACTIVE_RUN_ID" \
    --client-cursor "$bootstrap_client_cursor" \
    --expected-session-id ycsb/worker/1 \
    --approved-load-cdc-seq 2 \
    --approved-bootstrap-transactions 1 \
    --approved-bootstrap-rows "$bootstrap_count" \
    --approved-bootstrap-digest "0000000000000000000000000000000000000000000000000000000000000000" \
    --overall-timeout-ms "$(( STACK_TIMEOUT_SECONDS * 1000 ))" \
    >/dev/null 2>"$BAD_PREFIX_ERROR_PATH"; then
    bad_prefix_status=0
  else
    bad_prefix_status=$?
  fi
  [[ $bad_prefix_status == 2 ]]
  grep -Fq BOOTSTRAP_STATE_MISMATCH "$BAD_PREFIX_ERROR_PATH"

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

  run_unknown_table_negative
  run_ddl_negative

  printf 'live MariaDB lifecycle test passed (run_id=%s, capture=%s)\n' \
    "$positive_run_id" "$CAPTURE_PATH"
}


main "$@"
