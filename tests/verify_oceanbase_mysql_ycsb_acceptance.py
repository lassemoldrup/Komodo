#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def stream_payloads(path: Path) -> list[dict]:
    entries = json.loads(path.read_text(encoding="utf-8"))
    return [json.loads(entry[1][1]) for entry in entries]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--cdc", type=Path, required=True)
    parser.add_argument("--connector", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    failures: list[str] = []

    statuses = stream_payloads(args.status)
    if not statuses:
        failures.append("missing monitor statuses")
        final = {}
    else:
        final = statuses[-1]
        if final.get("lifecycle_state") != "FINAL":
            failures.append("monitor did not reach FINAL")
        if final.get("outcome") not in {"PASS", "VIOLATION"}:
            failures.append("monitor did not produce an audited verdict")
        reconciliation = final.get("reconciliation") or {}
        if not reconciliation.get("evidence_reconciled"):
            failures.append("evidence did not reconcile")
        if reconciliation.get("unmatched_client_records") != 0:
            failures.append("unmatched client records remain")
        if reconciliation.get("unmatched_workload_cdc_slots") != 0:
            failures.append("unmatched CDC slots remain")
        if reconciliation.get("completed_sessions") != 2:
            failures.append("session roster did not complete")

    client_records = stream_payloads(args.client)
    if len([record for record in client_records if record.get("kind") == "session_done"]) != 2:
        failures.append("client stream lacks two session manifests")

    cdc_entries = json.loads(args.cdc.read_text(encoding="utf-8"))
    row_values = [
        json.loads(entry[1][1])
        for entry in cdc_entries
        if json.loads(entry[1][1]).get("op") in {"c", "u", "d"}
    ]
    if not row_values:
        failures.append("CDC stream has no row events")
    for value in row_values:
        source = value.get("source") or {}
        if source.get("connector") != "mysql" or source.get("version") != "3.6.1.Final":
            failures.append("CDC source identity differs")
            break

    connector = json.loads(args.connector.read_text(encoding="utf-8"))[0]
    if connector["RestartCount"] != 0 or connector["State"]["Status"] != "running":
        failures.append("connector restarted or stopped")

    result = {
        "kind": "oceanbase_mysql_ycsb_acceptance",
        "passed": not failures,
        "outcome": final.get("outcome"),
        "client_records": len(client_records),
        "cdc_entries": len(cdc_entries),
        "failures": failures,
    }
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
