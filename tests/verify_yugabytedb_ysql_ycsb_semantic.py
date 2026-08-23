#!/usr/bin/env python3
"""Verify YugabyteDB YSQL retry and fractured-read semantic evidence."""

from __future__ import annotations

import argparse
import base64
import json
import struct
from pathlib import Path
from typing import Any

from yugabytedb_ysql_acceptance_evidence import (
    validate_client_cdc_correlations,
    validate_runtime_provenance,
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def records(path: Path, *, named_payload: bool = True) -> list[dict[str, Any]]:
    entries = json.loads(path.read_text(encoding="utf-8"))
    result = []
    for entry in entries:
        require(isinstance(entry, list) and len(entry) == 2, f"malformed Redis entry in {path}")
        fields = entry[1]
        require(len(fields) == 2, f"malformed field pair in {path}")
        if named_payload:
            require(fields[0] == "payload", f"malformed payload in {path}")
        result.append(json.loads(fields[1]))
    return result


def case_final_status(directory: Path, case: str, outcome: str) -> dict[str, Any]:
    finals = [
        record
        for record in records(directory / f"{case}-status.json")
        if record.get("lifecycle_state") == "FINAL"
    ]
    require(len(finals) == 1, f"{case} final-status count differs")
    final = finals[0]
    require(final.get("outcome") == outcome, f"{case} outcome differs")
    reconciliation = final.get("reconciliation", {})
    require(reconciliation.get("evidence_reconciled") is True, f"{case} did not reconcile")
    require(reconciliation.get("bootstrap_manifest_validated") is True, f"{case} bootstrap differs")
    require(reconciliation.get("startup_schema_attested") is True, f"{case} startup attestation missing")
    require(reconciliation.get("final_schema_attested") is True, f"{case} final attestation missing")
    require(reconciliation.get("expected_sessions") == 2, f"{case} expected sessions differ")
    require(reconciliation.get("completed_sessions") == 2, f"{case} completed sessions differ")
    require(reconciliation.get("accepted_client_transactions") == 2, f"{case} clients differ")
    require(reconciliation.get("joined_workload_transactions") == 2, f"{case} joins differ")
    require(reconciliation.get("unmatched_client_records") == 0, f"{case} unmatched clients")
    require(reconciliation.get("unmatched_workload_cdc_slots") == 0, f"{case} unmatched CDC")
    return final


def first_field(record: dict[str, Any]) -> str:
    raw = base64.b64decode(record["value_bytes_base64"], validate=True)
    require(raw[:1] == b"\x01" and len(raw) >= 5, "canonical value is malformed")
    length = struct.unpack(">I", raw[1:5])[0]
    require(5 + length <= len(raw), "canonical first field is truncated")
    return raw[5 : 5 + length].decode("utf-8")


def reader_values(directory: Path, case: str) -> list[str]:
    clients = [
        record
        for record in records(directory / f"{case}-client.json")
        if record.get("kind") == "client_txn" and record.get("session_id") == "ycsb/worker/1"
    ]
    require(len(clients) == 1, f"{case} reader ClientTxn count differs")
    return [first_field(read) for read in clients[0]["reads"]]


def verify_retry(directory: Path) -> dict[str, Any]:
    case = "definitive_retry"
    final = case_final_status(directory, case, "PASS")
    client_records = records(directory / f"{case}-client.json")
    clients = [record for record in client_records if record.get("kind") == "client_txn"]
    require(len(clients) == 2, "retry ClientTxn count differs")
    require(
        {(client["session_id"], client["correlation_id"]["txn_id"]) for client in clients}
        == {("ycsb/worker/1", 1), ("ycsb/worker/2", 2)},
        "retry ClientTxn correlations differ",
    )
    done = {
        record["session_id"]: record
        for record in client_records
        if record.get("kind") == "session_done"
    }
    require(set(done) == {"ycsb/worker/1", "ycsb/worker/2"}, "retry SessionDone roster differs")
    require(done["ycsb/worker/1"]["physical_attempts_started"] == 1, "retry winner attempts differ")
    retried = done["ycsb/worker/2"]
    require(retried["logical_transactions_started"] == 1, "retry logical count differs")
    require(retried["physical_attempts_started"] == 2, "retry physical count differs")
    require(retried["definitively_aborted_attempts"] == 1, "definitive abort count differs")
    require(retried["retry_attempts"] == 1, "retry count differs")
    require(retried["published_transactions"] == 1, "retry publication count differs")
    require(retried["ambiguous_attempts"] == 0, "retry ambiguity differs")

    cdc = records(directory / f"{case}-cdc.json", named_payload=False)
    row_images = []
    marker_pairs = []
    for record in cdc:
        source = record.get("source")
        after = record.get("after")
        if not isinstance(source, dict) or not isinstance(after, dict):
            continue
        if source.get("table") == "usertable":
            row_images.append(after)
        elif source.get("table") == "session_txn":
            marker_pairs.append((after.get("session_id"), after.get("txn_id")))
    require(row_images, "retry usertable CDC images are missing")
    require(
        any(
            row.get("field3") is None
            and row.get("field4") == ""
            and row.get("field5") == "é"
            and row.get("field6") == "中"
            and row.get("field9") == "🙂"
            for row in row_images
        ),
        "null/empty/UTF-8 CDC coverage differs",
    )
    require(
        marker_pairs.count(("ycsb/worker/1", 1)) == 1
        and marker_pairs.count(("ycsb/worker/2", 2)) == 1
        and ("ycsb/worker/2", 1) not in marker_pairs,
        "aborted retry attempt emitted a marker or committed markers differ",
    )
    _, marker_frames = validate_client_cdc_correlations(
        final["run_id"],
        directory / f"{case}-client.json",
        directory / f"{case}-cdc.json",
    )
    require(marker_frames == 2, "retry marker-bearing frame count differs")
    return {"run_id": final["run_id"], "outcome": final["outcome"], "session_done": list(done.values())}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--provenance", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    directory = args.directory

    cases = (
        "definitive_retry",
        "fractured_repeatable_read",
        "fractured_read_committed",
    )
    connector_image_id = validate_runtime_provenance(
        args.provenance,
        [directory / f"{case}-connector-container.json" for case in cases],
        connector_identity_paths=[
            directory / f"{case}-connector-identity.txt" for case in cases
        ],
    )

    retry = verify_retry(directory)
    repeatable = case_final_status(directory, "fractured_repeatable_read", "PASS")
    committed = case_final_status(directory, "fractured_read_committed", "VIOLATION")
    for case, final, benchmark, isolation in (
        ("definitive_retry", retry, "ycsb", "REPEATABLE_READ"),
        (
            "fractured_repeatable_read",
            repeatable,
            "ycsb_fractured_read_probe",
            "REPEATABLE_READ",
        ),
        (
            "fractured_read_committed",
            committed,
            "ycsb_fractured_read_probe",
            "READ_COMMITTED",
        ),
    ):
        stream = records(directory / f"{case}-client.json")
        manifests = [record for record in stream if record.get("kind") == "run_manifest"]
        require(len(manifests) == 1, f"{case} RunManifest count differs")
        require(
            manifests[0].get("benchmark") == benchmark
            and manifests[0].get("declared_isolation") == isolation,
            f"{case} RunManifest admission differs",
        )
        _, marker_frames = validate_client_cdc_correlations(
            final["run_id"],
            directory / f"{case}-client.json",
            directory / f"{case}-cdc.json",
        )
        require(marker_frames == 2, f"{case} raw marker frame count differs")
    require(
        reader_values(directory, "fractured_repeatable_read") == ["old", "old", "old"],
        "RR/SI fractured reader did not retain its snapshot",
    )
    require(
        reader_values(directory, "fractured_read_committed") == ["old", "old", "new"],
        "RC fractured reader did not expose old/old/new",
    )
    require(
        committed.get("checker_observation") == "VIOLATION_OBSERVED",
        "RC expected negative is not checker-attributed",
    )
    for case in ("definitive_retry", "fractured_repeatable_read", "fractured_read_committed"):
        for suffix in ("cdc.raw.txt", "client.raw.txt", "status.raw.txt", "compose.log"):
            path = directory / f"{case}-{suffix}"
            require(path.is_file() and path.stat().st_size > 0, f"missing raw evidence: {path.name}")

    report = {
        "kind": "yugabytedb_ysql_ycsb_semantic_acceptance",
        "definitive_retry": retry,
        "fractured_repeatable_read": {"run_id": repeatable["run_id"], "outcome": "PASS"},
        "fractured_read_committed": {
            "run_id": committed["run_id"],
            "outcome": "VIOLATION",
            "checker_observation": committed["checker_observation"],
        },
        "repeatable_read_values": ["old", "old", "old"],
        "read_committed_values": ["old", "old", "new"],
        "null_empty_utf8_validated": True,
        "aborted_attempt_emitted_no_marker": True,
        "runtime_connector_image_id": connector_image_id,
        "evidence_reconciled": True,
    }
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except ValueError as error:
        raise SystemExit(str(error)) from error
