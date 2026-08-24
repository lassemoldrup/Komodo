#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source=scripts/blackbox_lifecycle.sh
. "$ROOT/scripts/blackbox_lifecycle.sh"

case "${1:-}" in
  restore_redis)
    lifecycle_log=${2:?lifecycle log is required}
    evidence_file=${3:?OOM evidence file is required}
    payload_file=${4:?RunFailed payload file is required}
    expected_maxmemory=${5:?restored maxmemory is required}
    deadline=$((SECONDS + 30))
    while ! grep -Eq \
        'Redis (rejected required publication|returned an invalid stream ID)' \
        "$lifecycle_log" 2>/dev/null; do
      (( SECONDS < deadline )) || {
        printf 'restore_error=lifecycle Redis OOM was not observed\n' >>"$evidence_file"
        exit 1
      }
      sleep 0.05
    done
    printf 'lifecycle_oom_observed_ns=%s\n' "$(date +%s%N)" >>"$evidence_file"
    blackbox_redis --raw CONFIG SET maxmemory "$expected_maxmemory" \
      >>"$evidence_file" 2>&1
    printf 'restored_maxmemory=%s\n' \
      "$(blackbox_redis_config_value maxmemory)" >>"$evidence_file"
    run_failed_payload=$(<"$payload_file")
    printf 'run_failed_stream_id=%s\n' \
      "$(blackbox_publish_record "$(blackbox_client_stream)" "$run_failed_payload")" \
      >>"$evidence_file"
    printf 'restore_completed_ns=%s\n' "$(date +%s%N)" >>"$evidence_file"
    ;;
  load)
    exec "$ROOT/tests/semantic_client.sh" load
    ;;
  run)
    failure=${2:?operational failure kind is required}
    "$ROOT/tests/semantic_client.sh" run
    case "$failure" in
      external_writer)
        blackbox_backend_database_query "
          BEGIN ISOLATION LEVEL REPEATABLE READ;
          INSERT INTO benchbase.usertable (ycsb_key, field1)
          VALUES (9001, 'external-writer');
          COMMIT;
        " >/dev/null
        ;;
      live_ddl)
        [[ $BLACKBOX_BACKEND == yugabytedb_ysql ]]
        blackbox_backend_admin_query \
          "ALTER TABLE benchbase.usertable ADD COLUMN injected_ddl_value INTEGER" \
          >/dev/null
        blackbox_backend_database_query "
          BEGIN ISOLATION LEVEL REPEATABLE READ;
          UPDATE benchbase.usertable
             SET injected_ddl_value = 1
           WHERE ycsb_key = 1;
          INSERT INTO benchbase.session_txn (session_id, txn_id)
          VALUES ('ycsb/worker/1', 2);
          COMMIT;
        " >/dev/null
        ;;
      connector_stall)
        [[ $BLACKBOX_BACKEND == yugabytedb_ysql ]]
        : "${BLACKBOX_CORRECTNESS_RUN_OUTPUT:?missing correctness output directory}"
        stall_evidence="$BLACKBOX_CORRECTNESS_RUN_OUTPUT/connector_stall_invalid-operation.txt"
        {
          printf 'admitted_slot_active=%s\n' \
            "$(blackbox_backend_admin_query "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$BLACKBOX_YSQL_SLOT_NAME' AND active AND active_pid IS NOT NULL" | tr -d '[:space:]')"
          printf 'pause_requested_ns=%s\n' "$(date +%s%N)"
        } >"$stall_evidence"
        printf 'paused_container=%s\n' \
          "$("$BLACKBOX_DOCKER" pause "$BLACKBOX_CONNECTOR_CONTAINER")" \
          >>"$stall_evidence"
        printf 'paused_state=%s\n' \
          "$("$BLACKBOX_DOCKER" inspect --format '{{.State.Paused}}' "$BLACKBOX_CONNECTOR_CONTAINER")" \
          >>"$stall_evidence"
        printf 'pause_completed_ns=%s\n' "$(date +%s%N)" >>"$stall_evidence"
        ;;
      redis_oom)
        [[ $BLACKBOX_BACKEND == yugabytedb_ysql ]]
        : "${BLACKBOX_CORRECTNESS_RUN_OUTPUT:?missing correctness output directory}"
        : "${BLACKBOX_ACTIVE_RUN_ID:?missing active run ID}"
        expected_maxmemory=268435456
        evidence_file="$BLACKBOX_CORRECTNESS_RUN_OUTPUT/redis_oom_invalid-operation.txt"
        payload_file="$BLACKBOX_CORRECTNESS_RUN_OUTPUT/redis_oom_invalid-run-failed.json"
        lifecycle_log="$BLACKBOX_CORRECTNESS_RUN_OUTPUT/redis_oom_invalid.log"
        policy=$(blackbox_redis_config_value maxmemory-policy)
        [[ $policy == noeviction ]]
        used_memory=$(blackbox_redis --raw INFO memory | tr -d '\r' |
          awk -F: '$1 == "used_memory" {print $2}')
        [[ $used_memory =~ ^[1-9][0-9]*$ ]]
        python3 "$BLACKBOX_RECORD_TOOL" run-failed \
          --run-id "$BLACKBOX_ACTIVE_RUN_ID" \
          --phase COORDINATOR \
          --reason-code RESOURCE_EXHAUSTED \
          --diagnostic "Redis server rejected an audited lifecycle XADD under noeviction" \
          >"$payload_file"
        {
          printf 'maxmemory_policy=%s\n' "$policy"
          printf 'used_memory_before_oom=%s\n' "$used_memory"
          printf 'oom_maxmemory=1\n'
          printf 'oom_injected_ns=%s\n' "$(date +%s%N)"
        } >"$evidence_file"
        blackbox_redis --raw CONFIG SET maxmemory 1 >>"$evidence_file" 2>&1
        set +e
        blackbox_redis --raw XADD "blackbox:oom-probe:$BLACKBOX_ACTIVE_RUN_ID" \
          '*' payload audited-lifecycle-oom \
          >>"$evidence_file" 2>&1
        probe_status=$?
        set -e
        printf 'probe_exit_status=%s\n' "$probe_status" >>"$evidence_file"
        grep -q 'OOM command not allowed' "$evidence_file"
        setsid bash "$ROOT/tests/operational_failure_client.sh" restore_redis \
          "$lifecycle_log" "$evidence_file" "$payload_file" "$expected_maxmemory" \
          >"$BLACKBOX_CORRECTNESS_RUN_OUTPUT/redis_oom_invalid-restore.log" 2>&1 &
        printf 'restore_helper_pid=%s\n' "$!" >>"$evidence_file"
        ;;
      *)
        echo "unsupported operational failure kind: $failure" >&2
        exit 2
        ;;
    esac
    ;;
  *)
    echo "usage: $0 load|run FAILURE_KIND" >&2
    exit 2
    ;;
esac
