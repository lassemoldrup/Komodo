#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source=scripts/blackbox_lifecycle.sh
. "$ROOT/scripts/blackbox_lifecycle.sh"

case "${1:-}" in
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
          VALUES ('ycsb/worker/1', 2)
          ON CONFLICT (session_id) DO UPDATE SET txn_id = EXCLUDED.txn_id;
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
