#!/usr/bin/env bash
# Sound MariaDB black-box lifecycle coordinator.
#
# The built one-run monitor is the default. Tests may inject a controlled
# command to drive the same acknowledgement gates; this script never
# synthesizes a monitor status.

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source=scripts/blackbox_lifecycle.sh
. "$ROOT/scripts/blackbox_lifecycle.sh"
blackbox_acquire_live_stack_lock

BLACKBOX_DOCKER=${BLACKBOX_DOCKER:-docker}
BLACKBOX_STACK_TIMEOUT_SECONDS=${BLACKBOX_STACK_TIMEOUT_SECONDS:-120}
BLACKBOX_LOADER_TIMEOUT_SECONDS=${BLACKBOX_LOADER_TIMEOUT_SECONDS:-600}
BLACKBOX_WORKLOAD_TIMEOUT_SECONDS=${BLACKBOX_WORKLOAD_TIMEOUT_SECONDS:-900}
BLACKBOX_TERMINATION_GRACE_SECONDS=${BLACKBOX_TERMINATION_GRACE_SECONDS:-5}
BLACKBOX_COORDINATOR_MARGIN_SECONDS=${BLACKBOX_COORDINATOR_MARGIN_SECONDS:-1800}
BLACKBOX_PROCESSING_MODE=${BLACKBOX_PROCESSING_MODE:-audited}
BLACKBOX_MONITOR_METRICS_PATH=${BLACKBOX_MONITOR_METRICS_PATH:-}
BLACKBOX_RUN_ID_PATH=${BLACKBOX_RUN_ID_PATH:-}
BLACKBOX_BOOTSTRAP_MANIFEST_PATH=${BLACKBOX_BOOTSTRAP_MANIFEST_PATH:-}
BLACKBOX_EXPECTED_REDIS_MAXMEMORY_BYTES=${BLACKBOX_EXPECTED_REDIS_MAXMEMORY_BYTES:-}
BLACKBOX_MONITOR_MAX_PENDING_CLIENT_BYTES=${BLACKBOX_MONITOR_MAX_PENDING_CLIENT_BYTES:-}
BLACKBOX_MONITOR_MAX_RETAINED_IDENTITY_BYTES=${BLACKBOX_MONITOR_MAX_RETAINED_IDENTITY_BYTES:-}
BLACKBOX_MONITOR_TIMEOUT_SECONDS=${BLACKBOX_MONITOR_TIMEOUT_SECONDS:-$((
  BLACKBOX_LOADER_TIMEOUT_SECONDS + BLACKBOX_WORKLOAD_TIMEOUT_SECONDS +
    2 * (BLACKBOX_TERMINATION_GRACE_SECONDS +
      BLACKBOX_QUIESCENCE_TIMEOUT_SECONDS) +
    3 * BLACKBOX_STATUS_TIMEOUT_SECONDS + BLACKBOX_COORDINATOR_MARGIN_SECONDS
))}

MONITOR_SESSION_ID=
PHASE_SESSION_ID=
PHASE_COMMAND_STATUS=0
PHASE_QUIESCENCE_PROVEN=0
BOOTSTRAP_SCAN_FILE=
END_FENCE_COMMITTED=0
MONITOR_EXIT_STATUS=0


write_provenance_file() {
  local path=$1
  local content=$2
  local temporary="${path}.tmp.$$"
  if ! (umask 077 && printf '%s\n' "$content" >"$temporary"); then
    rm -f -- "$temporary"
    blackbox_die "cannot write provenance file: $path"
    return 1
  fi
  if ! mv -f -- "$temporary" "$path"; then
    rm -f -- "$temporary"
    blackbox_die "cannot install provenance file: $path"
    return 1
  fi
}


cleanup_owned_processes() {
  local session_id
  for session_id in "$PHASE_SESSION_ID" "$MONITOR_SESSION_ID"; do
    if [[ -n $session_id ]] && blackbox_session_has_processes "$session_id"; then
      pkill --session "$session_id" >/dev/null 2>&1 || true
    fi
  done
  if [[ -n $BOOTSTRAP_SCAN_FILE && -f $BOOTSTRAP_SCAN_FILE ]]; then
    rm -f -- "$BOOTSTRAP_SCAN_FILE"
  fi
}
trap cleanup_owned_processes EXIT


require_configuration() {
  blackbox_require_command "$BLACKBOX_DOCKER"
  blackbox_require_command "$BLACKBOX_REDIS_CLI"
  blackbox_require_command "$BLACKBOX_MARIADB_CLI"
  blackbox_require_command python3
  blackbox_require_command jq
  blackbox_require_command base64
  blackbox_require_command setsid
  blackbox_require_command pgrep
  blackbox_require_command pkill

  : "${BLACKBOX_LOADER_COMMAND:?set BLACKBOX_LOADER_COMMAND to the non-DDL BenchBase loader command}"
  : "${BLACKBOX_WORKLOAD_COMMAND:?set BLACKBOX_WORKLOAD_COMMAND to the BenchBase workload command}"
  : "${BLACKBOX_EXPECTED_SESSION_IDS:?set BLACKBOX_EXPECTED_SESSION_IDS to a comma-separated worker roster}"

  [[ $BLACKBOX_STACK_TIMEOUT_SECONDS =~ ^[1-9][0-9]*$ ]]
  [[ $BLACKBOX_STATUS_TIMEOUT_SECONDS =~ ^[1-9][0-9]*$ ]]
  [[ $BLACKBOX_QUIESCENCE_TIMEOUT_SECONDS =~ ^[1-9][0-9]*$ ]]
  [[ $BLACKBOX_LOADER_TIMEOUT_SECONDS =~ ^[1-9][0-9]*$ ]]
  [[ $BLACKBOX_WORKLOAD_TIMEOUT_SECONDS =~ ^[1-9][0-9]*$ ]]
  [[ $BLACKBOX_TERMINATION_GRACE_SECONDS =~ ^[1-9][0-9]*$ ]]
  [[ $BLACKBOX_COORDINATOR_MARGIN_SECONDS =~ ^[1-9][0-9]*$ ]]
  [[ $BLACKBOX_MONITOR_TIMEOUT_SECONDS =~ ^[1-9][0-9]*$ ]]
  if [[ -n $BLACKBOX_EXPECTED_REDIS_MAXMEMORY_BYTES &&
        ! $BLACKBOX_EXPECTED_REDIS_MAXMEMORY_BYTES =~ ^[1-9][0-9]*$ ]]; then
    blackbox_die \
      "BLACKBOX_EXPECTED_REDIS_MAXMEMORY_BYTES must be a positive decimal byte count"
  fi
  if [[ -n $BLACKBOX_MONITOR_MAX_PENDING_CLIENT_BYTES &&
        ! $BLACKBOX_MONITOR_MAX_PENDING_CLIENT_BYTES =~ ^[1-9][0-9]*$ ]]; then
    blackbox_die \
      "BLACKBOX_MONITOR_MAX_PENDING_CLIENT_BYTES must be a positive decimal byte count"
  fi
  if [[ -n $BLACKBOX_MONITOR_MAX_RETAINED_IDENTITY_BYTES &&
        ! $BLACKBOX_MONITOR_MAX_RETAINED_IDENTITY_BYTES =~ ^[1-9][0-9]*$ ]]; then
    blackbox_die \
      "BLACKBOX_MONITOR_MAX_RETAINED_IDENTITY_BYTES must be a positive decimal byte count"
  fi

  case "$BLACKBOX_PROCESSING_MODE" in
    audited|join-only) ;;
    *) blackbox_die "BLACKBOX_PROCESSING_MODE must be audited or join-only" ;;
  esac
  if [[ $BLACKBOX_PROCESSING_MODE == join-only && -z $BLACKBOX_MONITOR_METRICS_PATH ]]; then
    blackbox_die "join-only processing requires BLACKBOX_MONITOR_METRICS_PATH"
  fi
  if [[ -n $BLACKBOX_MONITOR_METRICS_PATH && -e $BLACKBOX_MONITOR_METRICS_PATH ]]; then
    blackbox_die "monitor metrics path already exists: $BLACKBOX_MONITOR_METRICS_PATH"
  fi
  if [[ -n $BLACKBOX_MONITOR_METRICS_PATH &&
        ( $BLACKBOX_MONITOR_METRICS_PATH == "$BLACKBOX_RUN_ID_PATH" ||
          $BLACKBOX_MONITOR_METRICS_PATH == "$BLACKBOX_BOOTSTRAP_MANIFEST_PATH" ) ]]; then
    blackbox_die "monitor metrics and provenance paths must be distinct"
  fi
  if [[ -n $BLACKBOX_RUN_ID_PATH &&
        $BLACKBOX_RUN_ID_PATH == "$BLACKBOX_BOOTSTRAP_MANIFEST_PATH" ]]; then
    blackbox_die "run-id and BootstrapManifest provenance paths must be distinct"
  fi

  IFS=',' read -r -a BLACKBOX_SESSION_IDS <<<"$BLACKBOX_EXPECTED_SESSION_IDS"
  (( ${#BLACKBOX_SESSION_IDS[@]} > 0 ))

  # Runs never accept a caller-supplied epoch. Recreating the stack and minting
  # the UUID here makes semantic run reuse unavailable by construction.
  BLACKBOX_ACTIVE_RUN_ID=$(blackbox_new_uuid)
  export BLACKBOX_ACTIVE_RUN_ID
  export BLACKBOX_REDIS_HOST BLACKBOX_REDIS_PORT BLACKBOX_DB_HOST BLACKBOX_DB_PORT
  export BLACKBOX_BACKEND_SOURCE_ID BLACKBOX_DECLARED_ISOLATION
  export BLACKBOX_EXPECTED_SESSION_IDS
  export BLACKBOX_PROCESSING_MODE BLACKBOX_MONITOR_METRICS_PATH
  export BLACKBOX_RUN_ID_PATH BLACKBOX_BOOTSTRAP_MANIFEST_PATH
  export BLACKBOX_CLIENT_STREAM="blackbox:client:$BLACKBOX_ACTIVE_RUN_ID"
  export BLACKBOX_STATUS_STREAM="blackbox:status:$BLACKBOX_ACTIVE_RUN_ID"
  if [[ -n $BLACKBOX_RUN_ID_PATH ]]; then
    write_provenance_file "$BLACKBOX_RUN_ID_PATH" "$BLACKBOX_ACTIVE_RUN_ID"
  fi

  local -a monitor_measurement_arguments=()
  if [[ $BLACKBOX_PROCESSING_MODE == join-only ]]; then
    monitor_measurement_arguments+=(--processing-mode join-only)
  fi
  if [[ -n $BLACKBOX_MONITOR_METRICS_PATH ]]; then
    monitor_measurement_arguments+=(--metrics-output "$BLACKBOX_MONITOR_METRICS_PATH")
  fi
  if [[ -n $BLACKBOX_MONITOR_MAX_PENDING_CLIENT_BYTES ]]; then
    monitor_measurement_arguments+=(
      --max-pending-client-bytes "$BLACKBOX_MONITOR_MAX_PENDING_CLIENT_BYTES"
    )
  fi
  if [[ -n $BLACKBOX_MONITOR_MAX_RETAINED_IDENTITY_BYTES ]]; then
    monitor_measurement_arguments+=(
      --max-retained-identity-bytes "$BLACKBOX_MONITOR_MAX_RETAINED_IDENTITY_BYTES"
    )
  fi

  if [[ -z ${BLACKBOX_MONITOR_COMMAND:-} ]]; then
    : "${BLACKBOX_MONITOR_BIN:?set BLACKBOX_MONITOR_BIN or BLACKBOX_MONITOR_COMMAND}"
    [[ -x $BLACKBOX_MONITOR_BIN ]] ||
      blackbox_die "one-run monitor is not executable: $BLACKBOX_MONITOR_BIN"
    local monitor_timeout_ms=$(( BLACKBOX_MONITOR_TIMEOUT_SECONDS * 1000 ))
    printf -v BLACKBOX_MONITOR_COMMAND '%q ' \
      "$BLACKBOX_MONITOR_BIN" \
      --redis-host "$BLACKBOX_REDIS_HOST" \
      --redis-port "$BLACKBOX_REDIS_PORT" \
      --run-id "$BLACKBOX_ACTIVE_RUN_ID" \
      --overall-timeout-ms "$monitor_timeout_ms" \
      "${monitor_measurement_arguments[@]}"
  elif (( ${#monitor_measurement_arguments[@]} > 0 )); then
    local quoted_measurement_arguments
    printf -v quoted_measurement_arguments '%q ' \
      "${monitor_measurement_arguments[@]}"
    BLACKBOX_MONITOR_COMMAND+=" $quoted_measurement_arguments"
  fi

  # Building the manifest is also the strict validation pass for the UUID,
  # source identity, isolation, and expected-session roster.
  blackbox_build_run_manifest >/dev/null
}


compose() {
  "$BLACKBOX_DOCKER" compose --file "$ROOT/docker-compose.yml" --profile mariadb "$@"
}


wait_for_healthy_container() {
  local container=$1
  local deadline=$(( $(date +%s) + BLACKBOX_STACK_TIMEOUT_SECONDS ))
  while (( $(date +%s) <= deadline )); do
    local status
    status=$("$BLACKBOX_DOCKER" inspect --format \
      '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
      "$container" 2>/dev/null || true)
    if [[ $status == healthy || $status == running ]]; then
      return 0
    fi
    if [[ $status == unhealthy || $status == exited || $status == dead ]]; then
      blackbox_die "$container entered terminal state: $status"
      return 1
    fi
    sleep 1
  done
  blackbox_die "timed out waiting for $container readiness"
}


wait_for_cdc_streaming() {
  local deadline=$(( $(date +%s) + BLACKBOX_STACK_TIMEOUT_SECONDS ))
  while (( $(date +%s) <= deadline )); do
    local logs
    logs=$("$BLACKBOX_DOCKER" logs cdc-debezium 2>&1 || true)
    if [[ $(grep -c 'Starting streaming' <<<"$logs") -gt 0 ]]; then
      return 0
    fi
    local state
    state=$("$BLACKBOX_DOCKER" inspect --format '{{.State.Status}}' cdc-debezium 2>/dev/null || true)
    if [[ $state == exited || $state == dead ]]; then
      blackbox_die "Debezium exited before streaming"
      return 1
    fi
    sleep 1
  done
  blackbox_die "timed out waiting for Debezium streaming"
}


verify_fresh_schema() {
  local tables
  tables=$(blackbox_database_query \
    "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'benchbase' ORDER BY TABLE_NAME")
  if [[ $tables != $'checker_run_fence\nsession_txn\nusertable' ]]; then
    blackbox_die "benchbase schema is not the exact application/control schema"
    return 1
  fi

  local application_rows fence_rows marker_rows snapshot_isolation
  application_rows=$(blackbox_database_query "SELECT COUNT(*) FROM usertable")
  fence_rows=$(blackbox_database_query "SELECT COUNT(*) FROM checker_run_fence")
  marker_rows=$(blackbox_database_query "SELECT COUNT(*) FROM session_txn")
  snapshot_isolation=$(blackbox_database_query "SELECT @@innodb_snapshot_isolation")
  application_rows=${application_rows//[[:space:]]/}
  fence_rows=${fence_rows//[[:space:]]/}
  marker_rows=${marker_rows//[[:space:]]/}
  snapshot_isolation=${snapshot_isolation//[[:space:]]/}
  if [[ $application_rows != 0 || $fence_rows != 0 || $marker_rows != 0 ]]; then
    blackbox_die "fresh-stack enforcement found pre-CDC rows"
    return 1
  fi
  if [[ $snapshot_isolation != 1 ]]; then
    blackbox_die "MariaDB innodb_snapshot_isolation is not enabled"
    return 1
  fi

  local redis_keys
  redis_keys=$(blackbox_redis --raw DBSIZE)
  redis_keys=${redis_keys//[[:space:]]/}
  if [[ $redis_keys != 0 ]]; then
    blackbox_die "fresh-stack enforcement found pre-existing Redis keys"
    return 1
  fi
}


start_fresh_database_and_schema() {
  blackbox_log "phase database/schema: recreating Redis and MariaDB"
  compose down --volumes --remove-orphans
  compose up --detach --force-recreate redis mariadb
  wait_for_healthy_container cdc-redis
  wait_for_healthy_container cdc-mariadb
  verify_fresh_schema
  if [[ -n $BLACKBOX_EXPECTED_REDIS_MAXMEMORY_BYTES ]]; then
    blackbox_verify_redis_configuration "$BLACKBOX_EXPECTED_REDIS_MAXMEMORY_BYTES"
  fi
}


start_cdc() {
  blackbox_log "phase CDC: starting Debezium after the empty schema exists"
  compose up --detach --no-deps --force-recreate debezium
  wait_for_cdc_streaming
}


start_monitor() {
  blackbox_log "phase monitor: starting the run-scoped monitor"
  setsid bash -lc "$BLACKBOX_MONITOR_COMMAND" &
  MONITOR_SESSION_ID=$!
  sleep 0.1
  if ! kill -0 "$MONITOR_SESSION_ID" >/dev/null 2>&1; then
    wait "$MONITOR_SESSION_ID" || true
    blackbox_die "monitor exited before RunManifest publication"
    return 1
  fi
}


run_client_phase() {
  local label=$1
  local command=$2
  local timeout_seconds=$3
  blackbox_log "phase $label: starting"
  setsid bash -lc "$command" &
  PHASE_SESSION_ID=$!
  PHASE_COMMAND_STATUS=0
  PHASE_QUIESCENCE_PROVEN=0

  local deadline=$(( $(date +%s) + timeout_seconds ))
  local timed_out=0
  while kill -0 "$PHASE_SESSION_ID" >/dev/null 2>&1; do
    if (( $(date +%s) > deadline )); then
      timed_out=1
      break
    fi
    sleep 0.1
  done

  local status
  if (( timed_out == 1 )); then
    blackbox_log "phase $label exceeded ${timeout_seconds}s; terminating its owned process session"
    pkill --signal TERM --session "$PHASE_SESSION_ID" >/dev/null 2>&1 || true
    local termination_deadline=$(( $(date +%s) + BLACKBOX_TERMINATION_GRACE_SECONDS ))
    while blackbox_session_has_processes "$PHASE_SESSION_ID" \
        && (( $(date +%s) <= termination_deadline )); do
      sleep 0.1
    done
    if blackbox_session_has_processes "$PHASE_SESSION_ID"; then
      pkill --signal KILL --session "$PHASE_SESSION_ID" >/dev/null 2>&1 || true
    fi
    wait "$PHASE_SESSION_ID" >/dev/null 2>&1 || true
    status=124
  elif wait "$PHASE_SESSION_ID"; then
    status=0
  else
    status=$?
  fi
  PHASE_COMMAND_STATUS=$status
  if ! blackbox_prove_quiescence "$PHASE_SESSION_ID"; then
    PHASE_QUIESCENCE_PROVEN=0
    return 1
  fi
  PHASE_QUIESCENCE_PROVEN=1
  PHASE_SESSION_ID=
  return "$status"
}


wait_for_owned_monitor_exit() {
  if [[ -z $MONITOR_SESSION_ID ]]; then
    blackbox_die "owned monitor process is unavailable"
    return 1
  fi
  local deadline=$(( $(date +%s) + BLACKBOX_STATUS_TIMEOUT_SECONDS ))
  while blackbox_session_has_processes "$MONITOR_SESSION_ID"; do
    if (( $(date +%s) > deadline )); then
      blackbox_die "timed out waiting for the owned monitor process to exit"
      return 1
    fi
    sleep 0.1
  done

  MONITOR_EXIT_STATUS=0
  if wait "$MONITOR_SESSION_ID"; then
    MONITOR_EXIT_STATUS=0
  else
    MONITOR_EXIT_STATUS=$?
  fi
  MONITOR_SESSION_ID=
}


validate_join_only_measurement() {
  local expected_completion=$1
  [[ -s $BLACKBOX_MONITOR_METRICS_PATH ]] || {
    blackbox_die "join-only monitor did not write its measurement artifact"
    return 1
  }

  local expected_completed=false
  local expected_status=failed
  local expected_reconciled=false
  if [[ $expected_completion == completed ]]; then
    expected_completed=true
    expected_status=completed
    expected_reconciled=true
  fi
  jq -e \
    --arg run_id "$BLACKBOX_ACTIVE_RUN_ID" \
    --arg status "$expected_status" \
    --argjson completed "$expected_completed" \
    --argjson reconciled "$expected_reconciled" '
      .kind == "performance_measurement" and
      .run_id == $run_id and
      .processing_mode == "join-only" and
      .completed == $completed and
      .completion_status == $status and
      .evidence_reconciled == $reconciled and
      (.assembler | type == "object") and
      (.joiner | type == "object") and
      (.checker | type == "object") and
      (.process | type == "object") and
      ([.. | objects | has("outcome") or has("verdict")] | any | not) and
      ([.. | strings | select(. == "PASS" or . == "VIOLATION" or . == "INVALID")] | length == 0) and
      (if $reconciled then .reconciliation.evidence_reconciled == true else true end)
    ' "$BLACKBOX_MONITOR_METRICS_PATH" >/dev/null || {
      blackbox_die "join-only monitor measurement failed validation"
      return 1
    }
}


validate_audited_measurement() {
  local expected_outcome=$1
  [[ -s $BLACKBOX_MONITOR_METRICS_PATH ]] || {
    blackbox_die "audited monitor did not write its measurement artifact"
    return 1
  }

  local expected_completed=true
  local expected_status=completed
  local expected_reconciled=true
  if [[ $expected_outcome == INVALID ]]; then
    expected_completed=false
    expected_status=failed
    expected_reconciled=false
  fi
  jq -e \
    --arg run_id "$BLACKBOX_ACTIVE_RUN_ID" \
    --arg outcome "$expected_outcome" \
    --arg status "$expected_status" \
    --argjson completed "$expected_completed" \
    --argjson reconciled "$expected_reconciled" '
      .kind == "performance_measurement" and
      .run_id == $run_id and
      .processing_mode == "audited" and
      .completed == $completed and
      .completion_status == $status and
      .evidence_reconciled == $reconciled and
      .outcome == $outcome and
      (.assembler | type == "object") and
      (.joiner | type == "object") and
      (.checker | type == "object") and
      (.process | type == "object") and
      .reconciliation.evidence_reconciled == $reconciled
    ' "$BLACKBOX_MONITOR_METRICS_PATH" >/dev/null || {
      blackbox_die "audited monitor measurement failed validation"
      return 1
    }
}


wait_for_failed_final() {
  if [[ $BLACKBOX_PROCESSING_MODE == join-only ]]; then
    blackbox_log "waiting for failed join-only measurement"
    wait_for_owned_monitor_exit || return 1
    validate_join_only_measurement failed
    return
  fi
  blackbox_log "waiting for final INVALID monitor result"
  if ! blackbox_wait_for_status FINAL; then
    return 1
  fi
  local outcome
  outcome=$(jq -r '.outcome' <<<"$MATCHED_STATUS")
  printf '%s\n' "$MATCHED_STATUS"
  if [[ $outcome != INVALID ]]; then
    blackbox_die "failed run ended with non-INVALID monitor outcome: $outcome"
    return 1
  fi
  if [[ -n $BLACKBOX_MONITOR_METRICS_PATH ]]; then
    wait_for_owned_monitor_exit || return 1
    if (( MONITOR_EXIT_STATUS != 4 )); then
      blackbox_die "audited monitor exited $MONITOR_EXIT_STATUS after INVALID"
      return 1
    fi
    validate_audited_measurement INVALID
  fi
}


stop_monitor_without_fence() {
  local reason_code=$1
  local diagnostic=$2
  if [[ $BLACKBOX_PROCESSING_MODE == join-only ]]; then
    blackbox_log "completion_status=failed reason_code=$reason_code diagnostic=$diagnostic"
  else
    blackbox_log "timeout-invalid outcome=INVALID reason_code=$reason_code diagnostic=$diagnostic"
  fi

  if [[ -z $MONITOR_SESSION_ID ]]; then
    return 0
  fi
  if blackbox_session_has_processes "$MONITOR_SESSION_ID"; then
    pkill --signal TERM --session "$MONITOR_SESSION_ID" >/dev/null 2>&1 || true
    local deadline=$(( $(date +%s) + BLACKBOX_TERMINATION_GRACE_SECONDS ))
    while blackbox_session_has_processes "$MONITOR_SESSION_ID" \
        && (( $(date +%s) <= deadline )); do
      sleep 0.1
    done
    if blackbox_session_has_processes "$MONITOR_SESSION_ID"; then
      pkill --signal KILL --session "$MONITOR_SESSION_ID" >/dev/null 2>&1 || true
    fi
  fi
  wait "$MONITOR_SESSION_ID" >/dev/null 2>&1 || true
  MONITOR_SESSION_ID=
}


close_failed_run() {
  local reason_code=$1
  local diagnostic=$2
  local write_end_fence=$3
  if (( STATUS_TERMINAL_SEEN == 1 )); then
    blackbox_log "monitor is already final; no later failure record or fence will be written"
    return 0
  fi
  blackbox_publish_run_failed "$reason_code" "$diagnostic" || true

  if (( write_end_fence == 0 )); then
    stop_monitor_without_fence "$reason_code" "$diagnostic"
    return 0
  fi

  if (( write_end_fence == 1 && END_FENCE_COMMITTED == 0 )); then
    if blackbox_commit_fence END_FENCE; then
      END_FENCE_COMMITTED=1
    else
      blackbox_log "END_FENCE commit failed during best-effort failure closure"
      stop_monitor_without_fence COMMIT_OUTCOME_AMBIGUOUS \
        "$diagnostic; END_FENCE commit did not complete with a known outcome"
      return 0
    fi
  fi
  wait_for_failed_final || true
}


close_coordinator_failure_after_quiescence_check() {
  local reason_code=$1
  local diagnostic=$2
  if blackbox_prove_quiescence ""; then
    close_failed_run "$reason_code" "$diagnostic" 1
  else
    close_failed_run QUIESCENCE_UNPROVEN \
      "$diagnostic; database quiescence was not proven and END_FENCE was not written" 0
  fi
}


publish_manifest_and_start_fence() {
  local manifest
  manifest=$(blackbox_build_run_manifest)
  blackbox_publish_record "$(blackbox_client_stream)" "$manifest" >/dev/null
  if ! blackbox_commit_fence RUN_START; then
    close_failed_run COMMIT_OUTCOME_AMBIGUOUS \
      "RUN_START commit did not complete with a known outcome; END_FENCE is forbidden" 0
    return 1
  fi
  blackbox_log "waiting for START_FENCE_SEEN"
  if ! blackbox_wait_for_status START_FENCE_SEEN; then
    close_coordinator_failure_after_quiescence_check INTERNAL_ERROR \
      "monitor did not acknowledge RUN_START"
    return 1
  fi
}


run_loader_and_close_bootstrap() {
  if run_client_phase loader "$BLACKBOX_LOADER_COMMAND" "$BLACKBOX_LOADER_TIMEOUT_SECONDS"; then
    :
  else
    :
  fi
  if (( PHASE_QUIESCENCE_PROVEN == 0 )); then
    close_failed_run QUIESCENCE_UNPROVEN \
      "loader process or database connections remained active; END_FENCE was not written" 0
    return 70
  fi
  if (( PHASE_COMMAND_STATUS != 0 )); then
    close_failed_run INTERNAL_ERROR "loader command exited $PHASE_COMMAND_STATUS" 1
    return 1
  fi

  local marker_rows
  marker_rows=$(blackbox_database_query "SELECT COUNT(*) FROM session_txn") || {
    close_failed_run INTERNAL_ERROR "could not verify marker state after loader quiescence" 1
    return 1
  }
  marker_rows=${marker_rows//[[:space:]]/}
  if [[ $marker_rows != 0 ]]; then
    close_failed_run MARKER_MALFORMED \
      "loader/bootstrap phase contaminated session_txn with $marker_rows row(s)" 1
    return 1
  fi

  if ! BOOTSTRAP_SCAN_FILE=$(mktemp /tmp/blackbox-bootstrap.XXXXXX); then
    close_failed_run RESOURCE_EXHAUSTED "cannot allocate bootstrap scan file" 1
    return 1
  fi
  if ! blackbox_capture_bootstrap_scan "$BOOTSTRAP_SCAN_FILE"; then
    close_failed_run CANONICAL_ENCODING_FAILED "post-loader canonical state scan failed" 1
    return 1
  fi
  local manifest
  if ! manifest=$(blackbox_build_bootstrap_manifest "$BOOTSTRAP_SCAN_FILE"); then
    close_failed_run CANONICAL_ENCODING_FAILED "post-loader canonical state encoding failed" 1
    return 1
  fi
  if [[ -n $BLACKBOX_BOOTSTRAP_MANIFEST_PATH ]]; then
    if ! write_provenance_file "$BLACKBOX_BOOTSTRAP_MANIFEST_PATH" "$manifest"; then
      close_failed_run RESOURCE_EXHAUSTED \
        "could not persist BootstrapManifest provenance" 1
      return 1
    fi
  fi
  if ! blackbox_publish_record "$(blackbox_client_stream)" "$manifest" >/dev/null; then
    close_failed_run REDIS_PUBLICATION_FAILED "BootstrapManifest publication failed" 1
    return 1
  fi
  if ! blackbox_commit_fence LOAD_FINISHED; then
    close_failed_run COMMIT_OUTCOME_AMBIGUOUS \
      "LOAD_FINISHED commit did not complete with a known outcome" 0
    return 1
  fi
  blackbox_log "waiting for LOAD_FINISHED_SEEN"
  if ! blackbox_wait_for_status LOAD_FINISHED_SEEN; then
    close_failed_run INTERNAL_ERROR "monitor did not acknowledge LOAD_FINISHED" 1
    return 1
  fi
  rm -f -- "$BOOTSTRAP_SCAN_FILE" || true
  BOOTSTRAP_SCAN_FILE=
}


run_workload_and_close() {
  if run_client_phase workload "$BLACKBOX_WORKLOAD_COMMAND" "$BLACKBOX_WORKLOAD_TIMEOUT_SECONDS"; then
    :
  else
    :
  fi

  if (( PHASE_QUIESCENCE_PROVEN == 0 )); then
    close_failed_run QUIESCENCE_UNPROVEN \
      "workload process or database connections remained active; END_FENCE was not written" 0
    return 1
  fi
  if (( PHASE_COMMAND_STATUS != 0 )); then
    close_failed_run INTERNAL_ERROR "workload command exited $PHASE_COMMAND_STATUS" 1
    return 1
  fi

  # This call is intentionally reachable only after process-session and
  # database-connection quiescence have both been observed twice.
  if ! blackbox_commit_fence END_FENCE; then
    close_failed_run COMMIT_OUTCOME_AMBIGUOUS \
      "END_FENCE commit did not complete with a known outcome" 0
    return 1
  fi
  END_FENCE_COMMITTED=1
  if [[ $BLACKBOX_PROCESSING_MODE == join-only ]]; then
    blackbox_log "waiting for completed join-only measurement"
    wait_for_owned_monitor_exit || return 1
    if (( MONITOR_EXIT_STATUS != 0 )); then
      blackbox_die "join-only monitor exited $MONITOR_EXIT_STATUS"
      return 1
    fi
    validate_join_only_measurement completed
    return
  fi
  blackbox_log "waiting for final monitor result"
  blackbox_wait_for_status FINAL

  local outcome
  outcome=$(jq -r '.outcome' <<<"$MATCHED_STATUS")
  printf '%s\n' "$MATCHED_STATUS"
  local result
  case "$outcome" in
    PASS) result=0 ;;
    VIOLATION) result=3 ;;
    INVALID) result=4 ;;
    *)
      blackbox_die "unexpected final outcome: $outcome"
      return 1
      ;;
  esac
  if [[ -n $BLACKBOX_MONITOR_METRICS_PATH ]]; then
    wait_for_owned_monitor_exit || return 1
    if (( MONITOR_EXIT_STATUS != result )); then
      blackbox_die \
        "audited monitor exited $MONITOR_EXIT_STATUS after $outcome"
      return 1
    fi
    validate_audited_measurement "$outcome" || return 1
  fi
  return "$result"
}


main() {
  require_configuration
  blackbox_log "run_id=$BLACKBOX_ACTIVE_RUN_ID"
  start_fresh_database_and_schema
  start_cdc
  start_monitor
  publish_manifest_and_start_fence
  run_loader_and_close_bootstrap
  run_workload_and_close
}


main "$@"
