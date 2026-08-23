#!/usr/bin/env python3
"""Verify YugabyteDB YSQL retry and fractured-read semantic evidence."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import struct
import subprocess
import sys
from pathlib import Path
from typing import Any

TEST_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(TEST_ROOT.parent / "protocol" / "tools"))
sys.path.insert(0, str(TEST_ROOT.parent / "scripts"))

from blackbox_record import bootstrap_digest, canonical_attestation_digest  # noqa: E402
from tpcc_client_fixture import read_digest, rolling_digest  # noqa: E402
from yugabytedb_ysql_acceptance_evidence import (
    cdc_frames,
    marker_identity,
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


def canonical_ycsb_key(key: int) -> bytes:
    def length_prefixed(value: bytes) -> bytes:
        return struct.pack(">I", len(value)) + value

    return (
        length_prefixed(b"benchbase")
        + length_prefixed(b"usertable")
        + struct.pack(">i", key)
    )


def canonical_ycsb_value(image: dict[str, Any]) -> bytes:
    expected = {"ycsb_key", *(f"field{index}" for index in range(1, 11))}
    require(set(image) == expected, "bootstrap YCSB row shape differs")
    result = bytearray()
    for index in range(1, 11):
        value = image[f"field{index}"]
        if value is None:
            result.append(0)
            continue
        require(isinstance(value, str) and len(value) <= 100,
                "bootstrap YCSB field differs")
        encoded = value.encode("utf-8", "strict")
        result.append(1)
        result.extend(struct.pack(">I", len(encoded)))
        result.extend(encoded)
    return bytes(result)


def validate_bootstrap_and_sessions(
    case: str,
    run_id: str,
    client_records: list[dict[str, Any]],
    raw_frames: list[list[dict[str, Any]]],
    expected_sessions: list[str],
) -> dict[str, Any]:
    bootstrap = [
        record for record in client_records if record.get("kind") == "bootstrap_manifest"
    ]
    require(len(bootstrap) == 1 and bootstrap[0].get("run_id") == run_id,
            f"{case} BootstrapManifest identity differs")
    snapshot_rows: list[dict[str, Any]] = []
    bootstrap_open = False
    for frame in raw_frames:
        fence_phases = [
            row.get("after", {}).get("phase")
            for row in frame
            if row.get("source", {}).get("table") == "checker_run_fence"
            and isinstance(row.get("after"), dict)
        ]
        if fence_phases == ["RUN_START"]:
            bootstrap_open = True
            continue
        if fence_phases == ["LOAD_FINISHED"]:
            bootstrap_open = False
            continue
        if bootstrap_open:
            require(
                all(
                    row.get("source", {}).get("schema") == "benchbase"
                    and row.get("source", {}).get("table") == "usertable"
                    and row.get("op") in {"c", "r"}
                    for row in frame
                ),
                f"{case} bootstrap CDC transaction differs",
            )
            snapshot_rows.extend(frame)
    canonical_rows: list[tuple[bytes, bytes]] = []
    for row in snapshot_rows:
        after = row.get("after")
        require(isinstance(after, dict) and type(after.get("ycsb_key")) is int,
                f"{case} bootstrap row image differs")
        canonical_rows.append(
            (canonical_ycsb_key(after["ycsb_key"]), canonical_ycsb_value(after))
        )
    row_count, state_digest = bootstrap_digest(canonical_rows)
    require(
        row_count > 0
        and bootstrap[0].get("table_row_counts")
        == [{"table": "benchbase.usertable", "row_count": row_count}]
        and bootstrap[0].get("canonical_state_digest") == state_digest,
        f"{case} BootstrapManifest count or digest differs from raw CDC",
    )

    clients = [record for record in client_records if record.get("kind") == "client_txn"]
    done_records = [
        record for record in client_records if record.get("kind") == "session_done"
    ]
    require(len(done_records) == len(expected_sessions), f"{case} SessionDone count differs")
    done_by_session = {record.get("session_id"): record for record in done_records}
    require(len(done_by_session) == len(done_records)
            and set(done_by_session) == set(expected_sessions),
            f"{case} SessionDone roster differs")
    for session in expected_sessions:
        done = done_by_session[session]
        session_clients = [
            record for record in clients if record.get("session_id") == session
        ]
        client_indexes = [
            index for index, record in enumerate(client_records)
            if record.get("kind") == "client_txn" and record.get("session_id") == session
        ]
        done_index = next(
            index for index, record in enumerate(client_records)
            if record.get("kind") == "session_done" and record.get("session_id") == session
        )
        require(all(index < done_index for index in client_indexes),
                f"{case} ClientTxn occurs after SessionDone for {session}")
        sequences = [record.get("session_seq") for record in session_clients]
        require(
            all(type(sequence) is int and sequence > 0 for sequence in sequences)
            and sequences == sorted(set(sequences))
            and all(sequence <= done.get("final_session_seq", 0) for sequence in sequences)
            and done.get("run_id") == run_id
            and done.get("final_session_seq") == done.get("logical_transactions_started")
            and done.get("published_transactions") == len(session_clients)
            and done.get("published_transactions") == done.get("successful_transactions")
            and done.get("logical_transactions_started")
            == done.get("successful_transactions") + done.get("terminal_aborted_transactions")
            and done.get("physical_attempts_started")
            == done.get("successful_transactions")
            + done.get("definitively_aborted_attempts")
            + done.get("ambiguous_attempts")
            and done.get("definitively_aborted_attempts")
            == done.get("retry_attempts") + done.get("terminal_aborted_transactions")
            and done.get("retry_attempts")
            == done.get("physical_attempts_started")
            - done.get("logical_transactions_started")
            - done.get("ambiguous_attempts")
            and done.get("ambiguous_attempts") == 0
            and rolling_digest(run_id, session, session_clients)
            == done.get("rolling_correlation_digest"),
            f"{case} complete SessionDone ledger differs for {session}",
        )
    return {
        "bootstrap_row_count": row_count,
        "bootstrap_state_digest": state_digest,
        "completed_sessions": len(done_records),
    }


def production_replay(
    replay_bin: Path,
    directory: Path,
    case: str,
    captured_final: dict[str, Any],
    *,
    cdc_name: str | None = None,
) -> dict[str, Any]:
    client_path = directory / f"{case}-client.json"
    cdc_path = directory / (cdc_name or f"{case}-cdc.json")
    client_records = records(client_path)
    manifests = [record for record in client_records if record.get("kind") == "run_manifest"]
    require(len(manifests) == 1, f"{case} RunManifest count differs before replay")
    manifest = manifests[0]
    run_id = manifest.get("run_id")
    require(isinstance(run_id, str), f"{case} RunManifest run ID differs before replay")
    tables = sorted(
        [*manifest.get("application_table_allowlist", []),
         *manifest.get("control_table_allowlist", []),
         manifest.get("transaction_marker_table")]
    )
    membership_digest = hashlib.sha256(
        "".join(f"{table}\n" for table in tables).encode("ascii")
    ).hexdigest()
    replica_digest = hashlib.sha256(
        "".join(f"{table}\tf\n" for table in tables).encode("ascii")
    ).hexdigest()
    compact_run_id = run_id.replace("-", "")
    command = [
        str(replay_bin),
        "--client-input", str(client_path),
        "--cdc-input", str(cdc_path),
        "--expected-backend-fact",
        f"publication_name=blackbox_{compact_run_id}_publication",
        "--expected-backend-fact",
        f"slot_name=blackbox_{compact_run_id}_slot",
        "--expected-backend-fact",
        f"publication_membership_sha256={membership_digest}",
        "--expected-backend-fact",
        f"replica_identity_full_tables_sha256={replica_digest}",
    ]
    result = subprocess.run(command, text=True, capture_output=True)
    require(result.returncode == 0,
            f"{case} production evidence replay failed: {result.stderr.strip()}")
    replay = json.loads(result.stdout)
    require(
        replay.get("kind") == "production_evidence_replay"
        and replay.get("run_id") == run_id
        and replay.get("final_status") == captured_final,
        f"{case} captured FINAL differs from production decoder/checker replay",
    )
    return {
        "outcome": replay["final_status"]["outcome"],
        "checker_observation": replay["final_status"]["checker_observation"],
        "reconciliation": replay["final_status"]["reconciliation"],
        "completed_transactions": len(replay["transactions"]),
    }


def independently_reconcile_case(
    directory: Path,
    case: str,
    final: dict[str, Any],
    *,
    expected_client_count: int,
    expect_reconciled: bool,
) -> dict[str, Any]:
    client_records = records(directory / f"{case}-client.json")
    manifests = [record for record in client_records if record.get("kind") == "run_manifest"]
    require(len(manifests) == 1, f"{case} RunManifest count differs")
    manifest = manifests[0]
    run_id = final["run_id"]
    require(manifest.get("run_id") == run_id, f"{case} RunManifest run ID differs")
    expected_sessions = manifest.get("expected_session_ids")
    require(expected_sessions == ["ycsb/worker/1", "ycsb/worker/2"],
            f"{case} expected-session roster differs")

    attestations = [
        record for record in client_records if record.get("kind") == "schema_attestation"
    ]
    require(
        len(attestations) == 2
        and [record.get("phase") for record in attestations] == ["STARTUP", "FINAL"],
        f"{case} schema-attestation phase roster differs",
    )
    for attestation in attestations:
        require(
            attestation.get("run_id") == run_id
            and attestation.get("backend") == manifest.get("backend")
            and attestation.get("backend_source_id") == manifest.get("backend_source_id")
            and canonical_attestation_digest(attestation)
            == attestation.get("canonical_attestation_digest"),
            f"{case} schema-attestation identity or digest differs",
        )
    startup, final_attestation = attestations
    require(
        startup["tables"] == final_attestation["tables"]
        and startup["backend_facts"] == final_attestation["backend_facts"]
        and startup["canonical_attestation_digest"]
        == final_attestation["canonical_attestation_digest"],
        f"{case} STARTUP/FINAL schema or capture evidence differs",
    )
    bootstrap = [
        record for record in client_records if record.get("kind") == "bootstrap_manifest"
    ]
    require(len(bootstrap) == 1 and bootstrap[0].get("run_id") == run_id,
            f"{case} BootstrapManifest differs")

    clients = [record for record in client_records if record.get("kind") == "client_txn"]
    require(len(clients) == expected_client_count, f"{case} ClientTxn count differs")
    clients_by_session: dict[str, list[dict[str, Any]]] = {
        session: [] for session in expected_sessions
    }
    client_correlations: list[tuple[str, int]] = []
    for client in clients:
        correlation = client.get("correlation_id")
        require(
            client.get("run_id") == run_id
            and client.get("session_id") in clients_by_session
            and isinstance(correlation, dict)
            and correlation.get("run_id") == run_id
            and correlation.get("session_id") == client.get("session_id")
            and read_digest(client.get("reads", [])) == client.get("read_digest")
            and client.get("read_count") == len(client.get("reads", [])),
            f"{case} ClientTxn identity or read digest differs",
        )
        clients_by_session[client["session_id"]].append(client)
        client_correlations.append((correlation["session_id"], correlation["txn_id"]))

    done_records = [
        record for record in client_records if record.get("kind") == "session_done"
    ]
    require(len(done_records) == 2, f"{case} SessionDone count differs")
    done_by_session = {record.get("session_id"): record for record in done_records}
    require(set(done_by_session) == set(expected_sessions), f"{case} SessionDone roster differs")
    for session in expected_sessions:
        done = done_by_session[session]
        session_clients = sorted(
            clients_by_session[session], key=lambda record: record["session_seq"]
        )
        require(
            done.get("run_id") == run_id
            and done.get("published_transactions") == len(session_clients)
            and done.get("logical_transactions_started")
            == done.get("successful_transactions") + done.get("terminal_aborted_transactions")
            and done.get("physical_attempts_started")
            == done.get("successful_transactions")
            + done.get("definitively_aborted_attempts")
            + done.get("ambiguous_attempts")
            and done.get("ambiguous_attempts") == 0
            and rolling_digest(run_id, session, session_clients)
            == done.get("rolling_correlation_digest"),
            f"{case} SessionDone counters or digest differ for {session}",
        )

    raw_frames = cdc_frames(directory / f"{case}-cdc.json")
    ledger_audit = validate_bootstrap_and_sessions(
        case, run_id, client_records, raw_frames, expected_sessions
    )
    fence_phases: list[str] = []
    marker_correlations: list[tuple[str, int]] = []
    for frame in raw_frames:
        if len(frame) == 1 and frame[0].get("source", {}).get("table") == "checker_run_fence":
            after = frame[0].get("after")
            require(isinstance(after, dict) and after.get("run_id") == run_id,
                    f"{case} fence run ID differs")
            fence_phases.append(after.get("phase"))
        if frame and frame[-1].get("source", {}).get("table") == "session_txn":
            marker_correlations.append(marker_identity(frame[-1]))
    require(fence_phases == ["RUN_START", "LOAD_FINISHED", "END_FENCE"],
            f"{case} fence order differs")
    require(len(raw_frames) == final.get("assembled_cdc_transactions") == 6,
            f"{case} raw/assembled CDC transaction count differs")
    require(
        len(marker_correlations) == 2
        and {session for session, _ in marker_correlations} == set(expected_sessions),
        f"{case} marker correlation roster differs",
    )

    reconciliation = final.get("reconciliation", {})
    require(
        reconciliation.get("expected_sessions") == len(expected_sessions)
        and reconciliation.get("completed_sessions") == len(done_records)
        and reconciliation.get("accepted_client_transactions") == len(clients)
        and reconciliation.get("phase_anchor_transactions") == len(expected_sessions)
        and reconciliation.get("bootstrap_cdc_transactions") == 1
        and reconciliation.get("bootstrap_checker_transactions") == 1
        and reconciliation.get("bootstrap_manifest_validated") is True
        and reconciliation.get("startup_schema_attested") is True
        and reconciliation.get("final_schema_attested") is True
        and reconciliation.get("schema_attestation_digest")
        == startup["canonical_attestation_digest"]
        and reconciliation.get("end_fence_seen") is True
        and reconciliation.get("evidence_reconciled") is expect_reconciled
        and reconciliation.get("ambiguous_attempts") == 0,
        f"{case} independent/final reconciliation differs",
    )
    if expect_reconciled:
        require(
            sorted(client_correlations) == sorted(marker_correlations)
            and reconciliation.get("joined_workload_transactions") == len(clients)
            and reconciliation.get("unmatched_client_records") == 0
            and reconciliation.get("unmatched_workload_cdc_slots") == 0
            and reconciliation.get("checker_consumed_transactions")
            == 1 + len(expected_sessions) + len(clients),
            f"{case} complete correlation reconciliation differs",
        )
    return {
        "run_id": run_id,
        "client_correlations": client_correlations,
        "marker_correlations": marker_correlations,
        "schema_attestation_digest": startup["canonical_attestation_digest"],
        "bootstrap_and_session_ledger": ledger_audit,
    }


def case_final_status(directory: Path, case: str, outcome: str) -> dict[str, Any]:
    finals = [
        record
        for record in records(directory / f"{case}-status.json")
        if record.get("lifecycle_state") == "FINAL"
    ]
    require(len(finals) == 1, f"{case} final-status count differs")
    final = finals[0]
    require(final.get("outcome") == outcome, f"{case} outcome differs")
    expected_diagnostics = set() if outcome == "PASS" else {"VIOLATION_OBSERVED"}
    require(
        {item.get("code") for item in final.get("diagnostics", [])}
        == expected_diagnostics,
        f"{case} final diagnostic priority differs",
    )
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
    require(reconciliation.get("ambiguous_attempts") == 0, f"{case} ambiguity differs")
    return final


def dropped_client_final_status(directory: Path) -> dict[str, Any]:
    case = "dropped_client_record"
    finals = [
        record
        for record in records(directory / f"{case}-status.json")
        if record.get("lifecycle_state") == "FINAL"
    ]
    require(len(finals) == 1, "dropped-client final-status count differs")
    final = finals[0]
    reconciliation = final.get("reconciliation", {})
    require(final.get("outcome") == "INVALID", "dropped-client outcome differs")
    require(final.get("checker_observation") != "VIOLATION_OBSERVED",
            "dropped-client run reported a checker violation")
    require(reconciliation.get("evidence_reconciled") is False,
            "dropped-client evidence unexpectedly reconciled")
    require(reconciliation.get("accepted_client_transactions") == 1,
            "dropped-client accepted-client count differs")
    require(reconciliation.get("joined_workload_transactions") == 0,
            "dropped-client joined count differs")
    require(reconciliation.get("unmatched_client_records") == 0,
            "dropped-client unmatched-client count differs")
    require(reconciliation.get("unmatched_workload_cdc_slots") == 1,
            "dropped-client unmatched-CDC count differs")
    diagnostics = final.get("diagnostics", [])
    require(any(item.get("code") == "CLIENT_HALF_MISSING" for item in diagnostics),
            "dropped-client diagnostic is absent")
    require(all(item.get("code") != "CDC_ORDER_INVALID" for item in diagnostics),
            "dropped-client run invented a CDC-order failure")
    require(
        {item.get("code") for item in diagnostics}
        == {"CLIENT_HALF_MISSING", "RECONCILIATION_MISMATCH"},
        "dropped-client run contains an unrelated operational diagnostic",
    )

    client_records = records(directory / f"{case}-client.json")
    manifests = [record for record in client_records if record.get("kind") == "run_manifest"]
    require(
        len(manifests) == 1
        and manifests[0].get("benchmark") == "ycsb_fractured_read_probe"
        and manifests[0].get("declared_isolation") == "REPEATABLE_READ",
        "dropped-client RunManifest admission differs",
    )
    clients = [record for record in client_records if record.get("kind") == "client_txn"]
    done = [record for record in client_records if record.get("kind") == "session_done"]
    require(len(clients) == 1 and len(done) == 2,
            "dropped-client stream roster differs")
    frames = cdc_frames(directory / f"{case}-cdc.json")
    marker_frames = [
        frame for frame in frames
        if frame and frame[-1].get("source", {}).get("table") == "session_txn"
    ]
    require(len(marker_frames) == 2,
            "dropped-client raw marker-bearing frame count differs")
    audit = independently_reconcile_case(
        directory,
        case,
        final,
        expected_client_count=1,
        expect_reconciled=False,
    )
    surviving = set(audit["client_correlations"])
    committed = set(audit["marker_correlations"])
    require(
        surviving == {("ycsb/worker/2", 1)}
        and committed == {("ycsb/worker/1", 1), ("ycsb/worker/2", 1)}
        and committed - surviving == {("ycsb/worker/1", 1)},
        "dropped-client missing correlation is not the injected transaction",
    )
    return final


def operational_invalid_status(
    directory: Path, case: str, expected_code: str
) -> dict[str, Any]:
    finals = [
        record
        for record in records(directory / f"{case}-status.json")
        if record.get("lifecycle_state") == "FINAL"
    ]
    require(len(finals) == 1, f"{case} final-status count differs")
    final = finals[0]
    reconciliation = final.get("reconciliation", {})
    require(
        final.get("outcome") == "INVALID"
        and final.get("checker_observation") != "VIOLATION_OBSERVED"
        and reconciliation.get("evidence_reconciled") is False
        and reconciliation.get("startup_schema_attested") is True
        and reconciliation.get("final_schema_attested") is False
        and reconciliation.get("accepted_client_transactions") == 2
        and reconciliation.get("joined_workload_transactions") == 2
        and reconciliation.get("unmatched_client_records") == 0
        and reconciliation.get("unmatched_workload_cdc_slots") == 0,
        f"{case} final invalid reconciliation differs",
    )
    require(
        {item.get("code") for item in final.get("diagnostics", [])}
        == {expected_code, "SCHEMA_ATTESTATION_MISSING", "RECONCILIATION_MISMATCH"},
        f"{case} diagnostic roster differs",
    )

    client_records = records(directory / f"{case}-client.json")
    manifests = [record for record in client_records if record.get("kind") == "run_manifest"]
    startup = [
        record
        for record in client_records
        if record.get("kind") == "schema_attestation" and record.get("phase") == "STARTUP"
    ]
    clients = [record for record in client_records if record.get("kind") == "client_txn"]
    done = [record for record in client_records if record.get("kind") == "session_done"]
    require(
        len(manifests) == len(startup) == 1
        and len(clients) == len(done) == 2
        and manifests[0].get("run_id") == startup[0].get("run_id") == final.get("run_id")
        and canonical_attestation_digest(startup[0])
        == startup[0].get("canonical_attestation_digest")
        and reconciliation.get("schema_attestation_digest")
        == startup[0].get("canonical_attestation_digest"),
        f"{case} startup/client evidence differs",
    )
    raw_frames = cdc_frames(directory / f"{case}-cdc.json")
    if expected_code == "UNMARKED_APPLICATION_TRANSACTION":
        injected = [
            row
            for frame in raw_frames
            for row in frame
            if row.get("source", {}).get("table") == "usertable"
            and (row.get("after") or {}).get("ycsb_key") == 9001
        ]
        require(
            len(injected) == 1
            and all(row.get("source", {}).get("table") != "session_txn"
                    for row in next(frame for frame in raw_frames if injected[0] in frame)),
            "external-writer raw transaction is absent or marked",
        )
    else:
        injected = [
            row
            for frame in raw_frames
            for row in frame
            if row.get("source", {}).get("table") == "usertable"
            and "injected_ddl_value" in (row.get("after") or {})
        ]
        require(len(injected) == 1, "live-DDL changed row shape is absent")
    return final


def connector_stall_status(directory: Path) -> dict[str, Any]:
    case = "connector_stall_invalid"
    status_records = records(directory / f"{case}-stalled-status.json")
    captured_status_records = records(directory / f"{case}-status.json")
    finals = [
        record for record in status_records if record.get("lifecycle_state") == "FINAL"
    ]
    captured_finals = [
        record
        for record in captured_status_records
        if record.get("lifecycle_state") == "FINAL"
    ]
    require(
        len(finals) == len(captured_finals) == 1 and finals[0] == captured_finals[0],
        "connector-stall case lacks one authoritative captured FINAL",
    )
    final = finals[0]
    reconciliation = final.get("reconciliation", {})
    diagnostic_codes = {item.get("code") for item in final.get("diagnostics", [])}
    require(
        final.get("outcome") == "INVALID"
        and final.get("checker_observation") != "VIOLATION_OBSERVED"
        and reconciliation.get("evidence_reconciled") is False
        and reconciliation.get("startup_schema_attested") is True
        and reconciliation.get("final_schema_attested") is True
        and reconciliation.get("end_fence_seen") is False
        and reconciliation.get("accepted_client_transactions") == 2
        and reconciliation.get("joined_workload_transactions") == 2
        and reconciliation.get("ambiguous_attempts") == 0
        and "FENCE_MISSING" in diagnostic_codes
        and "RECONCILIATION_MISMATCH" in diagnostic_codes,
        "connector-stall authoritative INVALID differs",
    )
    lifecycle_states = [record.get("lifecycle_state") for record in status_records]
    require(
        "START_FENCE_SEEN" in lifecycle_states and "LOAD_FINISHED_SEEN" in lifecycle_states,
        "connector stall did not occur after connector admission and bootstrap",
    )

    operation: dict[str, str] = {}
    for line in (directory / f"{case}-operation.txt").read_text(encoding="utf-8").splitlines():
        name, separator, value = line.partition("=")
        require(bool(separator) and name not in operation, "connector-stall evidence is malformed")
        operation[name] = value
    require(
        operation.get("admitted_slot_active") == "1"
        and operation.get("paused_container") == "cdc-debezium-yugabytedb-ysql"
        and operation.get("paused_state") == "true"
        and operation.get("unpaused_container") == "cdc-debezium-yugabytedb-ysql"
        and operation.get("pause_requested_ns", "").isdigit()
        and operation.get("pause_completed_ns", "").isdigit()
        and operation.get("unpause_completed_ns", "").isdigit()
        and int(operation["pause_requested_ns"])
        <= int(operation["pause_completed_ns"])
        < int(operation["unpause_completed_ns"]),
        "connector-stall pause provenance differs",
    )
    log = (directory / f"{case}.log").read_text(encoding="utf-8")
    require(
        "waiting for final monitor result" in log
        and '"lifecycle_state":"FINAL"' in log
        and '"outcome":"INVALID"' in log,
        "connector-stall run did not emit its authoritative FINAL INVALID",
    )

    client_records = records(directory / f"{case}-client.json")
    manifests = [record for record in client_records if record.get("kind") == "run_manifest"]
    attestations = [
        record for record in client_records if record.get("kind") == "schema_attestation"
    ]
    done = [record for record in client_records if record.get("kind") == "session_done"]
    clients = [record for record in client_records if record.get("kind") == "client_txn"]
    require(
        len(manifests) == 1
        and [record.get("phase") for record in attestations] == ["STARTUP", "FINAL"]
        and len(done) == len(clients) == 2,
        "connector-stall client evidence roster differs",
    )
    run_id = manifests[0].get("run_id")
    require(
        isinstance(run_id, str)
        and all(record.get("run_id") == run_id for record in status_records)
        and all(record.get("run_id") == run_id for record in attestations + done + clients),
        "connector-stall run identity differs",
    )
    for attestation in attestations:
        require(
            canonical_attestation_digest(attestation)
            == attestation.get("canonical_attestation_digest"),
            "connector-stall attestation digest differs",
        )
    stalled_container = json.loads(
        (directory / f"{case}-stalled-connector-container.json").read_text(
            encoding="utf-8"
        )
    )
    require(
        isinstance(stalled_container, list)
        and len(stalled_container) == 1
        and stalled_container[0].get("State", {}).get("Paused") is True,
        "connector was not captured in its paused state",
    )
    frames = cdc_frames(directory / f"{case}-stalled-cdc.json")
    fence_phases = [
        frame[0].get("after", {}).get("phase")
        for frame in frames
        if len(frame) == 1
        and frame[0].get("source", {}).get("table") == "checker_run_fence"
    ]
    require(
        fence_phases == ["RUN_START", "LOAD_FINISHED"],
        "connector-stall capture did not stop before the committed END fence",
    )
    return final


def redis_oom_status(directory: Path) -> dict[str, Any]:
    case = "redis_oom_invalid"
    finals = [
        record
        for record in records(directory / f"{case}-status.json")
        if record.get("lifecycle_state") == "FINAL"
    ]
    require(len(finals) == 1, "Redis OOM case lacks one authoritative FINAL")
    final = finals[0]
    reconciliation = final.get("reconciliation", {})
    diagnostic_codes = {item.get("code") for item in final.get("diagnostics", [])}
    require(
        final.get("outcome") == "INVALID"
        and final.get("checker_observation") != "VIOLATION_OBSERVED"
        and reconciliation.get("evidence_reconciled") is False
        and reconciliation.get("startup_schema_attested") is True
        and reconciliation.get("final_schema_attested") is False
        and reconciliation.get("accepted_client_transactions") == 2
        and reconciliation.get("joined_workload_transactions") == 2
        and reconciliation.get("ambiguous_attempts") == 0
        and {"RESOURCE_EXHAUSTED", "SCHEMA_ATTESTATION_MISSING",
             "RECONCILIATION_MISMATCH"} <= diagnostic_codes,
        "Redis OOM authoritative INVALID differs",
    )
    operation = (directory / f"{case}-operation.txt").read_text(encoding="utf-8")
    require(
        "maxmemory_policy=noeviction\n" in operation
        and "oom_maxmemory=1\n" in operation
        and "OOM command not allowed when used memory > 'maxmemory'." in operation
        and "lifecycle_oom_observed_ns=" in operation
        and "restored_maxmemory=268435456\n" in operation
        and "run_failed_stream_id=" in operation
        and "restore_completed_ns=" in operation,
        "Redis OOM server/lifecycle evidence differs",
    )
    failures = [
        record
        for record in records(directory / f"{case}-client.json")
        if record.get("kind") == "run_failed"
    ]
    require(
        len(failures) == 2
        and all(record.get("run_id") == final.get("run_id") for record in failures)
        and {record.get("reason_code") for record in failures}
        == {"RESOURCE_EXHAUSTED", "SCHEMA_ATTESTATION_MISSING"},
        "Redis OOM RunFailed evidence differs",
    )
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


def validate_probe_reads(directory: Path, case: str) -> None:
    clients = {
        record["session_id"]: record
        for record in records(directory / f"{case}-client.json")
        if record.get("kind") == "client_txn"
    }
    if case == "definitive_retry":
        expected = {
            "ycsb/worker/1": ([1], ["old"], "ycsb_update"),
            "ycsb/worker/2": ([1], ["winner"], "ycsb_update"),
        }
    elif case == "fractured_repeatable_read":
        expected = {
            "ycsb/worker/1": ([1, 2, 2], ["old", "old", "old"],
                              "si_fractured_read_probe"),
            "ycsb/worker/2": ([], [], "si_fractured_read_probe"),
        }
    elif case == "fractured_read_committed":
        expected = {
            "ycsb/worker/1": ([1, 2, 2], ["old", "old", "new"],
                              "si_fractured_read_probe"),
            "ycsb/worker/2": ([], [], "si_fractured_read_probe"),
        }
    elif case == "dropped_client_record":
        expected = {
            "ycsb/worker/2": ([1, 2], ["old", "old"],
                              "si_fractured_read_probe"),
        }
    else:
        expected = {
            "ycsb/worker/1": ([1, 2], ["old", "old"],
                              "si_fractured_read_probe"),
            "ycsb/worker/2": ([1, 2], ["old", "old"],
                              "si_fractured_read_probe"),
        }
    require(set(clients) == set(expected), f"{case} probe client roster differs")
    for session, (expected_keys, expected_values, expected_type) in expected.items():
        client = clients[session]
        reads = client.get("reads")
        require(
            client.get("txn_type") == expected_type
            and isinstance(reads, list)
            and client.get("read_count") == len(reads) == len(expected_keys),
            f"{case} probe read roster differs for {session}",
        )
        observed_values: list[str] = []
        for event_seq, (read, key) in enumerate(zip(reads, expected_keys, strict=True)):
            require(
                read.get("event_seq") == event_seq
                and read.get("source") == "snapshot"
                and read.get("value_state") == "present"
                and base64.b64decode(read.get("key_bytes_base64", ""), validate=True)
                == canonical_ycsb_key(key),
                f"{case} read source or canonical key differs for {session}",
            )
            observed_values.append(first_field(read))
        require(observed_values == expected_values,
                f"{case} probe values differ for {session}")


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
    parser.add_argument("--evidence-replay-bin", type=Path, required=True)
    args = parser.parse_args()
    directory = args.directory
    require(args.evidence_replay_bin.is_file(), "production evidence replay binary is missing")

    cases = (
        "definitive_retry",
        "fractured_repeatable_read",
        "fractured_read_committed",
        "dropped_client_record",
        "external_writer_invalid",
        "live_ddl_invalid",
        "connector_stall_invalid",
        "redis_oom_invalid",
    )
    connector_image_id = validate_runtime_provenance(
        args.provenance,
        [directory / f"{case}-connector-container.json" for case in cases],
        connector_identity_paths=[
            directory / f"{case}-connector-identity.txt" for case in cases
        ],
    )

    retry_final = case_final_status(directory, "definitive_retry", "PASS")
    retry = verify_retry(directory)
    repeatable = case_final_status(directory, "fractured_repeatable_read", "PASS")
    committed = case_final_status(directory, "fractured_read_committed", "VIOLATION")
    dropped = dropped_client_final_status(directory)
    external_writer = operational_invalid_status(
        directory, "external_writer_invalid", "UNMARKED_APPLICATION_TRANSACTION"
    )
    live_ddl = operational_invalid_status(
        directory, "live_ddl_invalid", "CDC_RECORD_MALFORMED"
    )
    connector_stall = connector_stall_status(directory)
    redis_oom = redis_oom_status(directory)
    finals_by_case = {
        "definitive_retry": retry_final,
        "fractured_repeatable_read": repeatable,
        "fractured_read_committed": committed,
        "dropped_client_record": dropped,
        "external_writer_invalid": external_writer,
        "live_ddl_invalid": live_ddl,
        "connector_stall_invalid": connector_stall,
        "redis_oom_invalid": redis_oom,
    }
    production_replays: dict[str, dict[str, Any]] = {}
    for case, final in finals_by_case.items():
        validate_probe_reads(directory, case)
        raw_name = (
            f"{case}-stalled-cdc.json"
            if case == "connector_stall_invalid"
            else f"{case}-cdc.json"
        )
        if case not in {
            "definitive_retry",
            "fractured_repeatable_read",
            "fractured_read_committed",
            "dropped_client_record",
        }:
            validate_bootstrap_and_sessions(
                case,
                final["run_id"],
                records(directory / f"{case}-client.json"),
                cdc_frames(directory / raw_name),
                ["ycsb/worker/1", "ycsb/worker/2"],
            )
        production_replays[case] = production_replay(
            args.evidence_replay_bin,
            directory,
            case,
            final,
            cdc_name=raw_name,
        )
    reconciled_audits = {
        case: independently_reconcile_case(
            directory,
            case,
            final,
            expected_client_count=2,
            expect_reconciled=True,
        )
        for case, final in (
            ("definitive_retry", retry_final),
            ("fractured_repeatable_read", repeatable),
            ("fractured_read_committed", committed),
        )
    }
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
    for suffix in ("cdc.raw.txt", "client.raw.txt", "status.raw.txt", "compose.log"):
        path = directory / f"dropped_client_record-{suffix}"
        require(path.is_file() and path.stat().st_size > 0,
                f"missing dropped-client raw evidence: {path.name}")
    for case in (
        "external_writer_invalid",
        "live_ddl_invalid",
        "connector_stall_invalid",
        "redis_oom_invalid",
    ):
        for suffix in ("cdc.raw.txt", "client.raw.txt", "status.raw.txt", "compose.log"):
            path = directory / f"{case}-{suffix}"
            require(path.is_file() and path.stat().st_size > 0,
                    f"missing operational raw evidence: {path.name}")
    for suffix in ("stalled-cdc.raw.txt", "stalled-status.raw.txt"):
        path = directory / f"connector_stall_invalid-{suffix}"
        require(path.is_file() and path.stat().st_size > 0,
                f"missing stalled-boundary raw evidence: {path.name}")

    report = {
        "kind": "yugabytedb_ysql_ycsb_semantic_acceptance",
        "definitive_retry": retry,
        "fractured_repeatable_read": {"run_id": repeatable["run_id"], "outcome": "PASS"},
        "fractured_read_committed": {
            "run_id": committed["run_id"],
            "outcome": "VIOLATION",
            "checker_observation": committed["checker_observation"],
        },
        "dropped_client_record": {
            "run_id": dropped["run_id"],
            "outcome": "INVALID",
            "diagnostic": "CLIENT_HALF_MISSING",
            "evidence_reconciled": False,
        },
        "external_writer_invalid": {
            "run_id": external_writer["run_id"],
            "outcome": "INVALID",
            "diagnostic": "UNMARKED_APPLICATION_TRANSACTION",
        },
        "live_ddl_invalid": {
            "run_id": live_ddl["run_id"],
            "outcome": "INVALID",
            "diagnostic": "CDC_RECORD_MALFORMED",
        },
        "connector_stall_invalid": connector_stall,
        "redis_oom_invalid": {
            "run_id": redis_oom["run_id"],
            "outcome": "INVALID",
            "diagnostic": "RESOURCE_EXHAUSTED",
            "authoritative_final": True,
            "actual_server_xadd_oom": True,
        },
        "repeatable_read_values": ["old", "old", "old"],
        "read_committed_values": ["old", "old", "new"],
        "null_empty_utf8_validated": True,
        "aborted_attempt_emitted_no_marker": True,
        "runtime_connector_image_id": connector_image_id,
        "independently_reconciled_verdict_cases": reconciled_audits,
        "production_decoder_checker_replays": production_replays,
        "all_expected_outcomes_validated": True,
    }
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except ValueError as error:
        raise SystemExit(str(error)) from error
