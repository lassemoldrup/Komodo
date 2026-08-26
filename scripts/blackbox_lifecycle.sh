#!/usr/bin/env bash
# Reusable coordinator helpers for one branch-local black-box run.
# MATCHED_STATUS is an output variable consumed by scripts that source this file.
# shellcheck disable=SC2034

BLACKBOX_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BLACKBOX_RECORD_TOOL="$BLACKBOX_ROOT/scripts/blackbox_record.py"

BLACKBOX_DOCKER=${BLACKBOX_DOCKER:-docker}
BLACKBOX_REDIS_HOST=${BLACKBOX_REDIS_HOST:-host.docker.internal}
BLACKBOX_REDIS_PORT=${BLACKBOX_REDIS_PORT:-6379}
BLACKBOX_DB_HOST=${BLACKBOX_DB_HOST:-host.docker.internal}
BLACKBOX_DB_PORT=${BLACKBOX_DB_PORT:-}
BLACKBOX_DB_USER=${BLACKBOX_DB_USER:-}
BLACKBOX_DB_PASSWORD=${BLACKBOX_DB_PASSWORD:-}
BLACKBOX_BACKEND_SOURCE_ID=${BLACKBOX_BACKEND_SOURCE_ID:-}
BLACKBOX_BACKEND=${BLACKBOX_BACKEND:-mariadb}
BLACKBOX_BENCHMARK=${BLACKBOX_BENCHMARK:-ycsb}
BLACKBOX_DECLARED_ISOLATION=${BLACKBOX_DECLARED_ISOLATION:-REPEATABLE_READ}
BLACKBOX_LOADER_BATCH_SIZE=${BLACKBOX_LOADER_BATCH_SIZE:-128}
BLACKBOX_LOADER_SCALE_VALUE=${BLACKBOX_LOADER_SCALE_VALUE:-}
BLACKBOX_LOADER_DECLARED_COUNTS_FILE=${BLACKBOX_LOADER_DECLARED_COUNTS_FILE:-}
BLACKBOX_BOOTSTRAP_MAX_ROWS=${BLACKBOX_BOOTSTRAP_MAX_ROWS:-1000000}
BLACKBOX_BOOTSTRAP_MAX_INPUT_BYTES=${BLACKBOX_BOOTSTRAP_MAX_INPUT_BYTES:-4294967296}
BLACKBOX_BOOTSTRAP_MAX_CANONICAL_BYTES=${BLACKBOX_BOOTSTRAP_MAX_CANONICAL_BYTES:-2147483648}
BLACKBOX_STATUS_TIMEOUT_SECONDS=${BLACKBOX_STATUS_TIMEOUT_SECONDS:-120}
BLACKBOX_QUIESCENCE_TIMEOUT_SECONDS=${BLACKBOX_QUIESCENCE_TIMEOUT_SECONDS:-30}

BLACKBOX_REDIS_CLI=${BLACKBOX_REDIS_CLI:-redis-cli}
STATUS_CURSOR=0-0
MATCHED_STATUS=
STATUS_TERMINAL_SEEN=0
STATUS_LAST_LIFECYCLE=


blackbox_log() {
  printf '[blackbox] %s\n' "$*" >&2
}


blackbox_die() {
  blackbox_log "ERROR: $*"
  return 1
}


blackbox_require_command() {
  command -v "$1" >/dev/null 2>&1 || blackbox_die "required command not found: $1"
}


blackbox_load_backend_driver() {
  [[ $BLACKBOX_BACKEND =~ ^[A-Za-z][A-Za-z0-9_]{0,63}$ ]] || {
    blackbox_die "backend name is not a bounded registry identifier"
    return 1
  }
  local driver_root=${BLACKBOX_BACKEND_DRIVER_DIR:-$BLACKBOX_ROOT/scripts/backends}
  local driver="$driver_root/$BLACKBOX_BACKEND.sh"
  [[ -f $driver ]] || {
    blackbox_die "registered coordinator backend driver is missing: $driver"
    return 1
  }
  # shellcheck disable=SC1090
  . "$driver"
  local operation
  for operation in initialize prepare_run require_commands database \
    database_query capture_schema_metadata verify_schema count_table marker_storage commit_fence \
    active_external_connections wait_for_connector_progress \
    capture_attestation_inputs start_database_schema \
    start_uninstrumented_database verify_session_isolation start_connector \
    teardown status; do
    declare -F "blackbox_backend_$operation" >/dev/null || {
      blackbox_die "backend driver lacks operation: $operation"
      return 1
    }
  done
  blackbox_backend_initialize
}


blackbox_load_backend_driver


blackbox_acquire_live_stack_lock() {
  local requested_target=${BLACKBOX_LIVE_LOCK_TARGET:-$BLACKBOX_ROOT}
  if [[ ! -d $requested_target ]]; then
    blackbox_die \
      "live stack lock target is not an existing directory: $requested_target"
    return 1
  fi
  local lock_target
  lock_target=$(cd "$requested_target" && pwd -P)
  BLACKBOX_LIVE_LOCK_TARGET=$lock_target
  export BLACKBOX_LIVE_LOCK_TARGET
  blackbox_require_command flock || return 1

  if [[ -n ${BLACKBOX_LIVE_LOCK_FD:-} ]]; then
    local descriptor_path=/proc/$BASHPID/fd/$BLACKBOX_LIVE_LOCK_FD
    if [[ ! $BLACKBOX_LIVE_LOCK_FD =~ ^[0-9]+$ ]] ||
        [[ ! -e $descriptor_path ]] ||
        [[ ! $descriptor_path -ef $lock_target ]]; then
      blackbox_die \
        "inherited live stack lock descriptor is invalid for: $lock_target"
      return 1
    fi
  else
    exec {BLACKBOX_LIVE_LOCK_FD}<"$lock_target"
    export BLACKBOX_LIVE_LOCK_FD
  fi

  if ! flock --nonblock "$BLACKBOX_LIVE_LOCK_FD"; then
    blackbox_die \
      "another destructive live suite owns the repository stack lock: $lock_target"
    return 1
  fi
}


blackbox_new_uuid() {
  python3 -c 'import uuid; print(uuid.uuid4())'
}


blackbox_redis() {
  local args=("$BLACKBOX_REDIS_CLI" -h "$BLACKBOX_REDIS_HOST" -p "$BLACKBOX_REDIS_PORT")
  if [[ -n ${BLACKBOX_REDIS_PASSWORD:-} ]]; then
    args+=(--pass "$BLACKBOX_REDIS_PASSWORD")
  fi
  "${args[@]}" "$@"
}


blackbox_redis_config_value() {
  local setting=$1
  local reply
  reply=$(blackbox_redis --raw CONFIG GET "$setting") || {
    blackbox_die "cannot read Redis configuration setting: $setting"
    return 1
  }
  local lines=()
  mapfile -t lines <<<"$reply"
  if (( ${#lines[@]} != 2 )) || [[ ${lines[0]} != "$setting" || -z ${lines[1]} ]]; then
    blackbox_die "Redis returned malformed CONFIG GET output for: $setting"
    return 1
  fi
  printf '%s\n' "${lines[1]}"
}


blackbox_verify_redis_configuration() {
  local expected_maxmemory_bytes=$1
  [[ $expected_maxmemory_bytes =~ ^[1-9][0-9]*$ ]] || {
    blackbox_die "expected Redis maxmemory must be a positive decimal byte count"
    return 1
  }
  local actual_maxmemory_bytes policy
  actual_maxmemory_bytes=$(blackbox_redis_config_value maxmemory) || return 1
  policy=$(blackbox_redis_config_value maxmemory-policy) || return 1
  [[ $actual_maxmemory_bytes =~ ^[1-9][0-9]*$ ]] || {
    blackbox_die "Redis maxmemory is not a positive decimal byte count"
    return 1
  }
  if [[ $actual_maxmemory_bytes != "$expected_maxmemory_bytes" ]]; then
    blackbox_die \
      "Redis maxmemory differs from the requested performance capacity: expected $expected_maxmemory_bytes, found $actual_maxmemory_bytes"
    return 1
  fi
  if [[ $policy != noeviction ]]; then
    blackbox_die "Redis maxmemory-policy must be noeviction, found: $policy"
    return 1
  fi
  BLACKBOX_REDIS_EFFECTIVE_MAXMEMORY_BYTES=$actual_maxmemory_bytes
  BLACKBOX_REDIS_EFFECTIVE_MAXMEMORY_POLICY=$policy
}


blackbox_database() {
  blackbox_backend_database "$@"
}


blackbox_database_query() {
  blackbox_backend_database_query "$1"
}


blackbox_compose() {
  "$BLACKBOX_DOCKER" compose --file "$BLACKBOX_ROOT/docker-compose.yml" \
    --profile "$BLACKBOX_COMPOSE_PROFILE" "$@"
}


blackbox_compose_all_profiles() {
  local profile profile_arguments=()
  while IFS= read -r profile; do
    [[ -n $profile ]] && profile_arguments+=(--profile "$profile")
  done < <(
    "$BLACKBOX_DOCKER" compose --file "$BLACKBOX_ROOT/docker-compose.yml" \
      config --profiles
  )
  if (( ${#profile_arguments[@]} == 0 )); then
    # Test doubles and older Compose implementations may not expose profile
    # discovery; retain the selected-profile contract in that environment.
    blackbox_compose "$@"
    return
  fi
  "$BLACKBOX_DOCKER" compose --file "$BLACKBOX_ROOT/docker-compose.yml" \
    "${profile_arguments[@]}" "$@"
}


blackbox_adapter_contract() {
  python3 "$BLACKBOX_RECORD_TOOL" adapter-contract --benchmark "$BLACKBOX_BENCHMARK"
}


blackbox_backend_contract() {
  python3 "$BLACKBOX_RECORD_TOOL" backend-contract --backend "$BLACKBOX_BACKEND"
}


blackbox_capture_schema_metadata() {
  blackbox_backend_capture_schema_metadata
}


blackbox_verify_schema_metadata() {
  blackbox_backend_verify_schema
}


blackbox_append_loader_arguments() {
  local array_name=$1
  local -n target_arguments=$array_name
  target_arguments+=(
    --benchmark "$BLACKBOX_BENCHMARK"
    --loader-declaration-file "$BLACKBOX_LOADER_DECLARED_COUNTS_FILE"
    --expected-loader-batch-size "$BLACKBOX_LOADER_BATCH_SIZE"
  )
  if [[ -n $BLACKBOX_LOADER_SCALE_VALUE ]]; then
    target_arguments+=(--expected-scale-value "$BLACKBOX_LOADER_SCALE_VALUE")
  fi
}


blackbox_build_loader_plan() {
  local arguments=(loader-plan)
  blackbox_append_loader_arguments arguments || return 1
  python3 "$BLACKBOX_RECORD_TOOL" "${arguments[@]}"
}


blackbox_capture_loader_observations() {
  local query_document
  query_document=$(python3 "$BLACKBOX_RECORD_TOOL" loader-queries \
    --backend "$BLACKBOX_BACKEND" --benchmark "$BLACKBOX_BENCHMARK") || return 1
  local encoded_queries
  encoded_queries=$(jq -er '.queries[] | @base64' <<<"$query_document") || {
    blackbox_die "adapter loader query document is malformed"
    return 1
  }
  local query_specs=()
  mapfile -t query_specs <<<"$encoded_queries"
  local encoded_query query observation identity result
  for encoded_query in "${query_specs[@]}"; do
    query=$(printf '%s' "$encoded_query" | base64 --decode) || return 1
    observation=$(jq -er '.observation' <<<"$query") || return 1
    result=$(blackbox_database_query "$(jq -er '.sql' <<<"$query")") || return 1
    result=${result//[[:space:]]/}
    [[ $result =~ ^[0-9]+$ ]] || {
      blackbox_die "loader validation query did not return a nonnegative integer: $result"
      return 1
    }
    case "$observation" in
      count)
        identity=$(jq -er '.table' <<<"$query") || return 1
        printf 'COUNT\t%s\t%s\n' "$identity" "$result"
        ;;
      sum_column)
        identity=$(jq -er '.table' <<<"$query") || return 1
        printf 'SUM\t%s\t%s\n' "$identity" "$result"
        ;;
      relationship_violations)
        identity=$(jq -er '.relationship_index' <<<"$query") || return 1
        printf 'RELATIONSHIP\t%s\t%s\n' "$identity" "$result"
        ;;
      *)
        blackbox_die "unknown adapter loader observation: $observation"
        return 1
        ;;
    esac
  done
}


blackbox_validate_loader_state() {
  local arguments=(loader-check)
  blackbox_append_loader_arguments arguments || return 1
  blackbox_capture_loader_observations |
    python3 "$BLACKBOX_RECORD_TOOL" "${arguments[@]}"
}


blackbox_validate_historical_loader_state() {
  local loader_batch_size=$1
  shift
  local arguments=(
    loader-independent-check
    --benchmark "$BLACKBOX_BENCHMARK"
    --loader-batch-size "$loader_batch_size"
  )
  if [[ -n $BLACKBOX_LOADER_SCALE_VALUE ]]; then
    arguments+=(--scale-value "$BLACKBOX_LOADER_SCALE_VALUE")
  fi
  local expected_count
  for expected_count in "$@"; do
    arguments+=(--expected-count "$expected_count")
  done
  blackbox_capture_loader_observations |
    python3 "$BLACKBOX_RECORD_TOOL" "${arguments[@]}"
}


blackbox_client_stream() {
  printf 'blackbox:client:%s' "$BLACKBOX_ACTIVE_RUN_ID"
}


blackbox_status_stream() {
  printf 'blackbox:status:%s' "$BLACKBOX_ACTIVE_RUN_ID"
}


blackbox_publish_record() {
  local stream=$1
  local payload=$2
  local entry_id
  entry_id=$(blackbox_redis --raw XADD "$stream" '*' payload "$payload") || {
    blackbox_die "Redis rejected required publication to $stream"
    return 1
  }
  if [[ ! $entry_id =~ ^(0|[1-9][0-9]*)-(0|[1-9][0-9]*)$ ]]; then
    blackbox_die "Redis returned an invalid stream ID for $stream: $entry_id"
    return 1
  fi
  printf '%s\n' "$entry_id"
}


blackbox_build_run_manifest() {
  local args=(
    run-manifest
    --run-id "$BLACKBOX_ACTIVE_RUN_ID"
    --backend "$BLACKBOX_BACKEND"
    --backend-source-id "$BLACKBOX_BACKEND_SOURCE_ID"
    --declared-isolation "$BLACKBOX_DECLARED_ISOLATION"
    --benchmark "$BLACKBOX_BENCHMARK"
  )
  local session_id
  for session_id in "${BLACKBOX_SESSION_IDS[@]}"; do
    args+=(--session-id "$session_id")
  done
  python3 "$BLACKBOX_RECORD_TOOL" "${args[@]}"
}


blackbox_build_schema_attestation() {
  local phase=$1
  [[ $phase == STARTUP || $phase == FINAL ]] || return 2
  local tables_file facts_file
  tables_file=$(mktemp /tmp/blackbox-attestation-tables.XXXXXX) || return 1
  facts_file=$(mktemp /tmp/blackbox-attestation-facts.XXXXXX) || {
    rm -f -- "$tables_file"
    return 1
  }
  if ! blackbox_backend_capture_attestation_inputs "$tables_file" "$facts_file"; then
    rm -f -- "$tables_file" "$facts_file"
    return 1
  fi
  local record_status=0
  python3 "$BLACKBOX_RECORD_TOOL" schema-attestation \
    --run-id "$BLACKBOX_ACTIVE_RUN_ID" --phase "$phase" \
    --backend "$BLACKBOX_BACKEND" \
    --backend-source-id "$BLACKBOX_BACKEND_SOURCE_ID" \
    --benchmark "$BLACKBOX_BENCHMARK" \
    --tables-file "$tables_file" --facts-file "$facts_file" || record_status=$?
  rm -f -- "$tables_file" "$facts_file"
  return "$record_status"
}


blackbox_commit_fence() {
  local phase=$1
  case "$phase" in
    RUN_START|LOAD_FINISHED|END_FENCE) ;;
    *)
      blackbox_die "invalid fence phase: $phase"
      return 1
      ;;
  esac

  local nonce
  nonce=$(blackbox_new_uuid)
  blackbox_backend_commit_fence "$BLACKBOX_ACTIVE_RUN_ID" "$phase" "$nonce"
}


blackbox_capture_bootstrap_scan() {
  local output_path=$1
  local query_document
  query_document=$(python3 "$BLACKBOX_RECORD_TOOL" bootstrap-queries \
    --backend "$BLACKBOX_BACKEND" --benchmark "$BLACKBOX_BENCHMARK") || return 1
  local encoded_queries
  encoded_queries=$(jq -er '.queries[] | @base64' <<<"$query_document") || {
    blackbox_die "adapter bootstrap query document is malformed"
    return 1
  }
  : >"$output_path"
  local query_specs=()
  mapfile -t query_specs <<<"$encoded_queries"
  local encoded_query query input_bytes
  for encoded_query in "${query_specs[@]}"; do
    query=$(printf '%s' "$encoded_query" | base64 --decode) || return 1
    blackbox_database_query "$(jq -er '.sql' <<<"$query")" >>"$output_path" || return 1
    input_bytes=$(wc -c <"$output_path") || return 1
    if (( input_bytes > BLACKBOX_BOOTSTRAP_MAX_INPUT_BYTES )); then
      blackbox_die "bootstrap scan exceeded input byte limit"
      return 1
    fi
  done
}


blackbox_build_bootstrap_manifest() {
  local scan_file=$1
  python3 "$BLACKBOX_RECORD_TOOL" bootstrap-manifest \
    --run-id "$BLACKBOX_ACTIVE_RUN_ID" \
    --benchmark "$BLACKBOX_BENCHMARK" \
    --scan-file "$scan_file" \
    --max-rows "$BLACKBOX_BOOTSTRAP_MAX_ROWS" \
    --max-input-bytes "$BLACKBOX_BOOTSTRAP_MAX_INPUT_BYTES" \
    --max-canonical-bytes "$BLACKBOX_BOOTSTRAP_MAX_CANONICAL_BYTES"
}


blackbox_publish_run_failed() {
  local reason_code=$1
  local diagnostic=$2
  local payload
  payload=$(python3 "$BLACKBOX_RECORD_TOOL" run-failed \
    --run-id "$BLACKBOX_ACTIVE_RUN_ID" \
    --phase COORDINATOR \
    --reason-code "$reason_code" \
    --diagnostic "$diagnostic") || return 1
  blackbox_publish_record "$(blackbox_client_stream)" "$payload" >/dev/null
}


blackbox_wait_for_status() {
  local lifecycle=$1
  local deadline=$(( $(date +%s) + BLACKBOX_STATUS_TIMEOUT_SECONDS ))
  local stream
  stream=$(blackbox_status_stream)

  while (( $(date +%s) <= deadline )); do
    local reply
    reply=$(blackbox_redis --json XREAD BLOCK 1000 COUNT 100 STREAMS "$stream" "$STATUS_CURSOR") || {
      blackbox_die "cannot read monitor status stream"
      return 1
    }
    if [[ -z $reply || $reply == null ]]; then
      continue
    fi

    local rendered_entries
    rendered_entries=$(jq -er \
      'to_entries[] as $stream | $stream.value[] | [$stream.key, .[0], (.[1] | length | tostring), .[1][0], (.[1][1] | @base64)] | @tsv' \
      <<<"$reply") || {
      blackbox_die "Redis returned malformed XREAD JSON"
      return 1
    }
    local entries=()
    mapfile -t entries <<<"$rendered_entries"
    if (( ${#entries[@]} == 0 )); then
      blackbox_die "Redis returned an empty non-null XREAD reply"
      return 1
    fi

    local entry
    for entry in "${entries[@]}"; do
      local returned_stream entry_id field_count field payload_base64 payload action observed_lifecycle
      IFS=$'\t' read -r returned_stream entry_id field_count field payload_base64 <<<"$entry"
      if [[ $returned_stream != "$stream" || ! $entry_id =~ ^(0|[1-9][0-9]*)-(0|[1-9][0-9]*)$ || $field_count != 2 || $field != payload ]]; then
        blackbox_die "malformed monitor stream entry at ${entry_id:-unknown}"
        return 1
      fi
      payload=$(printf '%s' "$payload_base64" | base64 --decode) || {
        blackbox_die "invalid encoded monitor payload at $entry_id"
        return 1
      }
      STATUS_CURSOR=$entry_id
      local status_args=(
        status-action
        --run-id "$BLACKBOX_ACTIVE_RUN_ID"
        --expected-sessions "${#BLACKBOX_SESSION_IDS[@]}"
        --lifecycle "$lifecycle"
      )
      if [[ -n $STATUS_LAST_LIFECYCLE ]]; then
        status_args+=(--previous-lifecycle "$STATUS_LAST_LIFECYCLE")
      fi
      IFS=$'\t' read -r action observed_lifecycle < <(
        printf '%s' "$payload" | python3 "$BLACKBOX_RECORD_TOOL" "${status_args[@]}"
      ) || return 1
      [[ -n $action && -n $observed_lifecycle ]] || return 1
      STATUS_LAST_LIFECYCLE=$observed_lifecycle
      case "$action" in
        match)
          MATCHED_STATUS=$payload
          return 0
          ;;
        terminal)
          MATCHED_STATUS=$payload
          STATUS_TERMINAL_SEEN=1
          blackbox_die "monitor finalized before lifecycle state $lifecycle"
          return 1
          ;;
        continue) ;;
        *)
          blackbox_die "unexpected status classification: $action"
          return 1
          ;;
      esac
    done
  done

  blackbox_die "timed out waiting for monitor lifecycle state $lifecycle"
}


blackbox_session_has_processes() {
  local session_id=$1
  [[ -n $session_id ]] && pgrep --session "$session_id" >/dev/null 2>&1
}


blackbox_active_external_connections() {
  local count
  count=$(blackbox_backend_active_external_connections) || return 1
  count=${count//[[:space:]]/}
  [[ $count =~ ^[0-9]+$ ]] || {
    blackbox_die "invalid active-connection count: $count"
    return 1
  }
  printf '%s\n' "$count"
}


blackbox_prove_quiescence() {
  local process_session_id=$1
  local deadline=$(( $(date +%s) + BLACKBOX_QUIESCENCE_TIMEOUT_SECONDS ))
  local consecutive_clean_checks=0

  while (( $(date +%s) <= deadline )); do
    local active_connections
    active_connections=$(blackbox_active_external_connections) || return 1
    if ! blackbox_session_has_processes "$process_session_id" && (( active_connections == 0 )); then
      ((consecutive_clean_checks += 1))
      if (( consecutive_clean_checks >= 2 )); then
        return 0
      fi
    else
      consecutive_clean_checks=0
    fi
    sleep 0.2
  done

  blackbox_die "client quiescence could not be proven; END_FENCE is forbidden"
}
