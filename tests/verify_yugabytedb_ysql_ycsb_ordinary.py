#!/usr/bin/env python3
"""Verify the ordinary YugabyteDB YSQL YCSB live-acceptance evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from yugabytedb_ysql_acceptance_evidence import (
    cdc_frames,
    final_status,
    validate_client_cdc_correlations,
    validate_runtime_provenance,
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--monitor", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--cdc", type=Path, required=True)
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--outcomes", type=Path, required=True)
    parser.add_argument("--bootstrap", type=Path, required=True)
    parser.add_argument("--loader-validation", type=Path, required=True)
    parser.add_argument("--tablet-count", type=Path, required=True)
    parser.add_argument("--provenance", type=Path, required=True)
    parser.add_argument("--connector-container", type=Path, required=True)
    parser.add_argument("--connector-image", type=Path, required=True)
    parser.add_argument("--connector-identity", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    monitor = load_json(args.monitor)
    reconciliation = monitor.get("reconciliation", {})
    joiner = monitor.get("joiner", {})
    checker = monitor.get("checker", {})
    assembler = monitor.get("assembler", {})
    require(monitor.get("kind") == "performance_measurement", "wrong monitor artifact kind")
    require(monitor.get("processing_mode") == "audited", "monitor was not audited")
    require(monitor.get("outcome") in {"PASS", "VIOLATION"}, "ordinary outcome is invalid")
    require(monitor.get("completed") is True, "monitor did not complete")
    require(monitor.get("evidence_reconciled") is True, "evidence did not reconcile")
    require(reconciliation.get("expected_sessions") == 2, "expected-session count differs")
    require(reconciliation.get("completed_sessions") == 2, "SessionDone count differs")
    require(reconciliation.get("bootstrap_manifest_validated") is True, "bootstrap not validated")
    require(reconciliation.get("startup_schema_attested") is True, "startup attestation missing")
    require(reconciliation.get("final_schema_attested") is True, "final attestation missing")
    require(reconciliation.get("phase_anchor_transactions") == 2, "phase anchors differ")
    require(reconciliation.get("end_fence_seen") is True, "end fence missing")
    require(reconciliation.get("unmatched_client_records") == 0, "unmatched client records")
    require(reconciliation.get("unmatched_workload_cdc_slots") == 0, "unmatched CDC slots")
    require(joiner.get("pending_client_records") == 0, "pending client records")
    require(joiner.get("pending_cdc_slots") == 0, "pending CDC slots")
    require(joiner.get("marker_only_workload_transactions", 0) > 0, "marker-only coverage missing")
    require(assembler.get("workload_normalized_write_events", 0) > 0, "normalized writes missing")
    require(checker.get("workload_read_events", 0) > 0, "checker read coverage missing")
    require(checker.get("workload_write_events", 0) > 0, "checker write coverage missing")
    require(
        reconciliation.get("accepted_client_transactions")
        == reconciliation.get("joined_workload_transactions"),
        "client/joined transaction totals differ",
    )

    connector_image_id = validate_runtime_provenance(
        args.provenance,
        [args.connector_container],
        connector_image_path=args.connector_image,
        connector_identity_paths=[args.connector_identity],
    )
    frames = cdc_frames(args.cdc)
    require(
        len(frames) == assembler.get("completed_transactions"),
        "raw CDC/assembler transaction totals differ",
    )
    status = final_status(args.status, monitor["run_id"])
    require(status.get("outcome") == monitor.get("outcome"), "status/monitor outcome differs")
    require(
        status.get("reconciliation") == reconciliation,
        "status/monitor reconciliation differs",
    )
    client_stream, marker_frames = validate_client_cdc_correlations(
        monitor["run_id"], args.client, args.cdc
    )
    require(
        marker_frames == reconciliation["accepted_client_transactions"],
        "raw CDC marker total differs",
    )
    for frame in frames:
        for row in frame:
            source = row.get("source")
            if isinstance(source, dict):
                require(
                    source.get("version") == "dz.2.5.2.yb.2026.1",
                    "raw CDC connector version differs",
                )

    records = client_stream
    clients = [record for record in records if record.get("kind") == "client_txn"]
    done = [record for record in records if record.get("kind") == "session_done"]
    require(len(done) == 2, "expected exactly two SessionDone records")
    require(
        {record.get("session_id") for record in done}
        == {"ycsb/worker/1", "ycsb/worker/2"},
        "SessionDone roster differs",
    )
    published = sum(record["published_transactions"] for record in done)
    require(published == len(clients), "SessionDone/client publication total differs")
    require(published == reconciliation["accepted_client_transactions"], "monitor/client total differs")
    for record in done:
        require(record["ambiguous_attempts"] == 0, "ordinary run has an ambiguous attempt")
        require(
            record["physical_attempts_started"]
            == record["successful_transactions"] + record["definitively_aborted_attempts"],
            "physical-attempt ledger differs",
        )

    transaction_types = {record.get("txn_type") for record in clients}
    require(
        {"ycsb_read", "ycsb_insert", "ycsb_update", "ycsb_delete"}
        <= transaction_types,
        "ordinary transaction-type coverage differs",
    )
    value_states = {
        read.get("value_state") for record in clients for read in record.get("reads", [])
    }
    require({"present", "absent"} <= value_states, "point hit/miss coverage differs")

    outcomes = load_json(args.outcomes)
    for transaction_name in ("ReadRecord", "InsertRecord", "UpdateRecord", "DeleteRecord"):
        require(
            outcomes["transaction_types"][transaction_name]["successful_transactions"] > 0,
            f"{transaction_name} success coverage missing",
        )
    require(outcomes["totals"]["ambiguous_attempts"] == 0, "BenchBase ambiguity differs")

    bootstrap = load_json(args.bootstrap)
    loader = load_json(args.loader_validation)
    require(
        bootstrap["table_row_counts"] == [{"table": "benchbase.usertable", "row_count": 1000}],
        "bootstrap count differs",
    )
    require(
        loader["observed_table_row_counts"] == bootstrap["table_row_counts"],
        "loader/bootstrap counts differ",
    )
    tablet_count = int(args.tablet_count.read_text(encoding="utf-8").strip())
    require(tablet_count >= 2, "usertable has fewer than two tablets")

    report = {
        "kind": "yugabytedb_ysql_ycsb_ordinary_acceptance",
        "run_id": monitor["run_id"],
        "outcome": monitor["outcome"],
        "accepted_client_transactions": reconciliation["accepted_client_transactions"],
        "assembled_cdc_transactions": assembler["completed_transactions"],
        "marker_only_workload_transactions": joiner["marker_only_workload_transactions"],
        "workload_read_events": checker["workload_read_events"],
        "workload_write_events": checker["workload_write_events"],
        "usertable_tablets": tablet_count,
        "transaction_types": sorted(transaction_types),
        "point_value_states": sorted(value_states),
        "raw_cdc_transactions": len(frames),
        "raw_marker_frames": marker_frames,
        "runtime_connector_image_id": connector_image_id,
        "evidence_reconciled": True,
    }
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except ValueError as error:
        raise SystemExit(str(error)) from error
