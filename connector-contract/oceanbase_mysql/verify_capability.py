#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def require_text(path: Path, needle: str, failures: list[str]) -> None:
    if not path.is_file():
        failures.append(f"missing evidence file: {path.name}")
        return
    if needle not in path.read_text(encoding="utf-8", errors="replace"):
        failures.append(f"{path.name} lacks expected text: {needle}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    evidence = args.evidence.resolve()
    failures: list[str] = []

    require_text(evidence / "show-master-status.txt", "data_row_count=1", failures)
    require_text(evidence / "show-master-status.txt", "mysql-bin.", failures)
    require_text(evidence / "show-binary-log-status.txt", "exit_code=1", failures)
    require_text(evidence / "show-binary-log-status.txt", "ERROR 1064", failures)
    require_text(
        evidence / "debezium-server.log",
        "falling back to 'SHOW MASTER STATUS'",
        failures,
    )
    require_text(
        evidence / "debezium-server.log",
        "Successfully tested connection",
        failures,
    )
    require_text(
        evidence / "debezium-server.log",
        "Snapshot completed",
        failures,
    )
    require_text(evidence / "debezium-server.log", "Starting streaming", failures)
    require_text(evidence / "redis-cdc-info.txt", "length=4", failures)
    require_text(evidence / "runtime-versions.txt", "OceanBase_CE 4.4.2.1", failures)
    require_text(evidence / "runtime-versions.txt", "version: 4.3.6.4-", failures)
    require_text(evidence / "runtime-versions.txt", "debezium-server-dist 3.6.1.Final", failures)

    provenance_path = evidence / "configuration" / "provenance.lock.json"
    if not provenance_path.is_file():
        failures.append("missing provenance lock")
    else:
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        if provenance.get("contract") != (
            "oceanbase_mysql_official_obbinlog_to_stock_debezium_to_redis"
        ):
            failures.append("unexpected provenance contract")

    raw_path = evidence / "raw-redis-cdc.json"
    if not raw_path.is_file():
        failures.append("missing raw Redis CDC evidence")
    else:
        raw_entries = json.loads(raw_path.read_text(encoding="utf-8"))
        if len(raw_entries) != 4:
            failures.append(f"expected four Redis CDC entries, got {len(raw_entries)}")
        else:
            values = [json.loads(entry[1][1]) for entry in raw_entries]
            if values[0].get("status") != "BEGIN":
                failures.append("first CDC entry is not BEGIN")
            if values[-1].get("status") != "END":
                failures.append("last CDC entry is not END")
            if values[-1].get("event_count") != 2:
                failures.append("END event_count is not two")
            if values[1].get("source", {}).get("table") != "usertable":
                failures.append("first row is not the application row")
            if values[2].get("source", {}).get("table") != "session_txn":
                failures.append("marker row is not final")
            transaction_ids = {
                values[0].get("id"),
                values[1].get("transaction", {}).get("id"),
                values[2].get("transaction", {}).get("id"),
                values[3].get("id"),
            }
            if None in transaction_ids or len(transaction_ids) != 1:
                failures.append("transaction IDs disagree within the raw frame")

    evidence_valid = not failures
    result = {
        "kind": "oceanbase_mysql_capability_verification",
        "evidence_valid": evidence_valid,
        "stock_connector_gate_passed": evidence_valid,
        "capability_gate_passed": False,
        "verdict_capable": False,
        "connector_path": {
            "jdbc_and_replication_endpoint": "odp:2883",
            "binlog_management_endpoint": "obbinlog:2983",
            "binlog_instance_endpoint": "diagnostic_only:8100",
            "redis_cdc_entries": 4,
        },
        "remaining_gates": [
            "rr_snapshot_isolation_litmus",
            "complete_final_write_corpus",
            "primary_key_delete_create",
            "commit_order_and_marker_last",
            "large_transaction_and_backpressure",
            "end_fence_drain",
        ],
        "evidence_failures": failures,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if evidence_valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
