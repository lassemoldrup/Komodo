#!/usr/bin/env python3
"""Validate a live standard-mix TPC-C run and its bounded replay corpus."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))

import tpcc_acceptance as common  # noqa: E402


AcceptanceError = common.AcceptanceError
require = common.require
TRANSACTION_TYPES = ("NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel")
EXPECTED_TABLES = common.EXPECTED_TABLES


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
    except OSError as error:
        raise AcceptanceError(f"cannot hash {path}: {error}") from error
    return digest.hexdigest()


def load_config(path: Path) -> dict[str, Any]:
    config = common.load_json(path, "run configuration")
    base = {
        "kind", "benchmark", "backend", "declared_isolation", "warehouses",
        "terminals", "weights", "random_seed", "warmup_seconds", "duration_seconds",
        "rate", "loader_threads", "loader_batch_size", "monitor_max_bootstrap_state_rows",
        "monitor_max_bootstrap_state_bytes", "stack_timeout_seconds", "loader_timeout_seconds",
        "workload_timeout_seconds", "status_timeout_seconds", "case_timeout_seconds",
        "kill_after_seconds", "keep_stack", "redis_host", "redis_port", "db_host", "db_port",
        "expected_session_ids", "minimum_per_type", "accepted_outcomes",
        "replay_capture_max_bytes", "resource_exhaustion_metrics",
    }
    require(isinstance(config, dict) and set(config) == base, "run configuration fields differ")
    require(config["kind"] == "tpcc_full_mix_acceptance_config", "config kind")
    require(config["benchmark"] == "tpcc" and config["backend"] == "mariadb", "config identity")
    require(config["declared_isolation"] == "REPEATABLE_READ", "config isolation")
    require(config["warehouses"] == 1 and config["terminals"] == 4, "standard topology differs")
    require(config["weights"] == [45, 43, 4, 4, 4], "standard weights differ")
    require(config["accepted_outcomes"] == ["PASS", "VIOLATION"], "accepted outcomes differ")
    for field in (
        "minimum_per_type", "replay_capture_max_bytes", "duration_seconds",
        "loader_threads", "loader_batch_size", "monitor_max_bootstrap_state_rows",
        "monitor_max_bootstrap_state_bytes", "stack_timeout_seconds", "loader_timeout_seconds",
        "workload_timeout_seconds", "status_timeout_seconds", "case_timeout_seconds",
        "kill_after_seconds",
    ):
        require(isinstance(config[field], int) and config[field] > 0, f"{field} is not positive")
    expected_sessions = [f"tpcc/worker/{index}" for index in range(1, 5)]
    require(config["expected_session_ids"] == expected_sessions, "session roster differs")
    resource = config["resource_exhaustion_metrics"]
    require(
        isinstance(resource, dict) and set(resource) == {"path", "sha256"}
        and isinstance(resource["path"], str)
        and re.fullmatch(r"[0-9a-f]{64}", str(resource["sha256"])),
        "resource-exhaustion reference differs",
    )
    return config


def validate_resource_exhaustion(reference: dict[str, Any]) -> dict[str, Any]:
    path = Path(reference["path"])
    require(path.is_file(), "resource-exhaustion metrics path is missing")
    require(sha256_file(path) == reference["sha256"], "resource-exhaustion checksum differs")
    metrics = common.load_json(path, "resource-exhaustion metrics")
    require(
        metrics.get("kind") == "performance_measurement"
        and metrics.get("completed") is False
        and metrics.get("completion_status") == "failed"
        and metrics.get("evidence_reconciled") is False
        and metrics.get("outcome") == "INVALID"
        and any(item.get("code") == "RESOURCE_EXHAUSTED" for item in metrics.get("diagnostics", [])),
        "recorded resource-exhaustion outcome differs",
    )
    require(
        metrics.get("joiner", {}).get("bootstrap_state_bytes") == 268435104
        and metrics.get("joiner", {}).get("bootstrap_state_validated") is False,
        "recorded 256MiB bootstrap exhaustion boundary differs",
    )
    return {"path": str(path), "sha256": reference["sha256"],
            "outcome": "INVALID", "diagnostic": "RESOURCE_EXHAUSTED",
            "bootstrap_state_bytes": 268435104, "limit_bytes": 268435456}


def validate_replay_manifest(path: Path, config: dict[str, Any], run_id: str) -> dict[str, Any]:
    document = common.load_json(path, "replay manifest")
    require(
        isinstance(document, dict)
        and set(document) == {"kind", "run_id", "capture_max_bytes", "streams"}
        and document["kind"] == "tpcc_full_mix_replay"
        and document["run_id"] == run_id
        and document["capture_max_bytes"] == config["replay_capture_max_bytes"],
        "replay manifest identity differs",
    )
    require(set(document["streams"]) == {"cdc", "client"}, "replay stream roster differs")
    for label, metadata in document["streams"].items():
        require(
            isinstance(metadata, dict) and set(metadata) == {"path", "sha256", "bytes"}
            and isinstance(metadata["bytes"], int) and 0 < metadata["bytes"] <= document["capture_max_bytes"]
            and re.fullmatch(r"[0-9a-f]{64}", str(metadata["sha256"])),
            f"replay {label} metadata differs",
        )
        stream = Path(metadata["path"])
        require(stream.is_file() and stream.stat().st_size == metadata["bytes"], f"replay {label} path/size")
        require(sha256_file(stream) == metadata["sha256"], f"replay {label} checksum")
    return document


def decode_client_evidence(config: dict, run_id: str, records: list[dict]):
    common.validate_record_schemas(records)
    manifests = [row for row in records if row.get("kind") == "run_manifest"]
    bootstraps = [row for row in records if row.get("kind") == "bootstrap_manifest"]
    clients = [row for row in records if row.get("kind") == "client_txn"]
    done_records = [row for row in records if row.get("kind") == "session_done"]
    require(len(manifests) == len(bootstraps) == 1, "manifest count differs")
    require(not any(row.get("kind") == "run_failed" for row in records), "RunFailed present")
    manifest, bootstrap = manifests[0], bootstraps[0]
    require(
        manifest["run_id"] == run_id and manifest["benchmark"] == "tpcc"
        and manifest["backend"] == "mariadb"
        and manifest["declared_isolation"] == "REPEATABLE_READ"
        and manifest["expected_session_ids"] == config["expected_session_ids"]
        and manifest["application_table_allowlist"] == EXPECTED_TABLES,
        "RunManifest differs",
    )
    counts = bootstrap["table_row_counts"]
    require(
        bootstrap["run_id"] == run_id
        and [row["table"] for row in counts] == sorted(EXPECTED_TABLES)
        and all(row["row_count"] > 0 for row in counts),
        "all-nine BootstrapManifest roster/counts differ",
    )
    by_type = Counter()
    correlations = {}
    by_session: dict[str, list[dict]] = defaultdict(list)
    for index, client in enumerate(clients):
        label = f"ClientTxn[{index}]"
        require(client["run_id"] == run_id and client["txn_type"] in TRANSACTION_TYPES, f"{label} identity")
        require(client["read_count"] == len(client["reads"])
                and common.read_digest(client["reads"]) == client["read_digest"], f"{label} digest/count")
        identity = (client["session_id"], client["correlation_id"]["txn_id"])
        require(client["correlation_id"]["run_id"] == run_id and identity not in correlations, f"{label} correlation")
        correlations[identity] = client["txn_type"]
        by_type[client["txn_type"]] += 1
        by_session[client["session_id"]].append(client)
    require(set(by_type) == set(TRANSACTION_TYPES), "full mix lacks a transaction type")
    require(all(by_type[name] >= config["minimum_per_type"] for name in TRANSACTION_TYPES), "per-type minimum differs")
    done_by_session = {row["session_id"]: row for row in done_records}
    require(set(done_by_session) == set(config["expected_session_ids"])
            and set(by_session) == set(config["expected_session_ids"]), "session roster differs")
    terminal_aborts = retries = definitive = 0
    for session_id in config["expected_session_ids"]:
        session_clients = sorted(by_session[session_id], key=lambda row: row["session_seq"])
        sequences = [row["session_seq"] for row in session_clients]
        txn_ids = [row["correlation_id"]["txn_id"] for row in session_clients]
        require(sequences == sorted(set(sequences)) and txn_ids == sorted(set(txn_ids)), f"{session_id} sequences")
        done = done_by_session[session_id]
        logical, successful = done["logical_transactions_started"], done["successful_transactions"]
        aborted, retry_count = done["terminal_aborted_transactions"], done["retry_attempts"]
        require(
            done["run_id"] == run_id and done["final_session_seq"] == logical
            and logical == successful + aborted
            and successful == done["published_transactions"] == len(session_clients)
            and done["physical_attempts_started"] == successful + done["definitively_aborted_attempts"]
            and retry_count == done["physical_attempts_started"] - logical
            and done["definitively_aborted_attempts"] == aborted + retry_count
            and done["ambiguous_attempts"] == 0
            and len(set(range(1, logical + 1)) - set(sequences)) == aborted,
            f"{session_id} SessionDone accounting differs",
        )
        require(done["rolling_correlation_digest"] == common.rolling_digest(run_id, session_id, session_clients), f"{session_id} rolling digest")
        terminal_aborts += aborted; retries += retry_count; definitive += done["definitively_aborted_attempts"]
    return clients, correlations, dict(by_type), {
        "terminal_aborts": terminal_aborts, "retries": retries,
        "definitive_aborts": definitive,
    }


def benchbase_mix_outcomes(path: Path, per_type: dict[str, int], attempts: dict[str, int]):
    lines = [
        common.ANSI_ESCAPE.sub("", line).strip()
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    positions = {}
    for heading in common.BENCHBASE_SECTIONS:
        matches = [index for index, line in enumerate(lines) if line == heading]
        require(len(matches) == 1, f"BenchBase log must contain one {heading} section")
        positions[heading] = matches[0]
    parsed, empty = {}, {}
    for index, heading in enumerate(common.BENCHBASE_SECTIONS):
        start = positions[heading] + 1
        end = (
            positions[common.BENCHBASE_SECTIONS[index + 1]]
            if index + 1 < len(common.BENCHBASE_SECTIONS)
            else len(lines)
        )
        rows, saw_empty = {}, False
        for line in lines[start:end]:
            if line == "<EMPTY>":
                saw_empty = True
                continue
            match = common.BENCHBASE_TRANSACTION.match(line)
            if match:
                name, count = match.groups()
                require(name not in rows, f"BenchBase {heading} repeats type")
                rows[name] = int(count)
        require(not (rows and saw_empty), f"BenchBase {heading} mixes EMPTY and rows")
        parsed[heading], empty[heading] = rows, saw_empty
    completed = {name: parsed["Completed Transactions:"].get(name, 0) for name in TRANSACTION_TYPES}
    require(completed == per_type, "BenchBase/per-type ClientTxn counts differ")
    aborted = sum(parsed["Aborted Transactions:"].values())
    server_rejections = sum(parsed["Rejected Transactions (Server Retry):"].values())
    require(aborted == attempts["terminal_aborts"], "BenchBase aborted total differs")
    require(
        attempts["retries"] <= server_rejections
        <= attempts["retries"] + attempts["terminal_aborts"],
        "BenchBase server-retry total differs",
    )
    for heading in (
        "Rejected Transactions (Retry Different):",
        "Unexpected SQL Errors:",
        "Unknown Status Transactions:",
    ):
        require(empty[heading] and not parsed[heading], f"BenchBase {heading} is not empty")
    return {
        "completed_by_type": completed,
        "aborted_by_type": parsed["Aborted Transactions:"],
        "server_retries_by_type": parsed["Rejected Transactions (Server Retry):"],
        "retry_different": 0,
        "unexpected_sql_errors": 0,
        "unknown_status": 0,
    }


def validate_cdc_tail(path: Path, run_id: str, load_id: str, correlations: dict):
    entries = common.raw_cdc_records(path)
    require(entries and common.parse_stream_id(entries[0][0], "CDC first")
            > common.parse_stream_id(load_id, "LOAD cursor"), "CDC tail boundary differs")
    transactions, current = [], None
    for entry in entries:
        status = entry[2].get("status")
        if status == "BEGIN": require(current is None, "nested BEGIN"); current = [entry]
        elif status == "END": require(current is not None, "END without BEGIN"); current.append(entry); transactions.append(current); current = None
        else: require(current is not None, "row outside transaction"); current.append(entry)
    require(current is None, "incomplete CDC transaction")
    markers, by_type, fences = set(), Counter(), 0
    for index, transaction in enumerate(transactions):
        label = f"CDC transaction {index}"; begin, end = transaction[0][2], transaction[-1][2]
        rows, txid = transaction[1:-1], begin.get("id")
        require(end.get("id") == txid and end.get("event_count") == len(rows), f"{label} framing")
        marker_rows = [entry for entry in rows if entry[2].get("source", {}).get("table") == "session_txn"]
        fence_rows = [entry for entry in rows if entry[2].get("source", {}).get("table") == "checker_run_fence"]
        if fence_rows:
            require(len(rows) == len(fence_rows) == 1 and index == len(transactions) - 1
                    and fence_rows[0][2].get("after", {}).get("run_id") == run_id
                    and fence_rows[0][2]["after"].get("phase") == "END_FENCE", "END_FENCE differs")
            fences += 1; continue
        require(len(marker_rows) == 1 and rows[-1] is marker_rows[0], f"{label} marker absent/not final")
        after = marker_rows[0][2].get("after")
        identity = (after["session_id"], after["txn_id"])
        require(identity in correlations and identity not in markers, f"{label} marker/client identity")
        markers.add(identity); by_type[correlations[identity]] += 1
    require(fences == 1 and markers == set(correlations), "CDC marker reconciliation differs")
    return {"transactions": len(transactions), "markers": len(markers),
            "end_fences": 1, "markers_by_type": dict(by_type)}


def validate_terminal(config: dict, run_id: str, statuses: list, metrics: dict,
                      clients: int, reads: int):
    common.validate_record_schemas(statuses)
    for lifecycle in ("START_FENCE_SEEN", "LOAD_FINISHED_SEEN", "FINAL"):
        common.one_status(statuses, lifecycle)
    final = common.one_status(statuses, "FINAL")
    outcome = final["outcome"]
    require(outcome in config["accepted_outcomes"], "final outcome differs")
    require(final["pending_cdc_slots"] == final["unmatched_client_records"] == 0, "final unmatched differs")
    reconciliation = final["reconciliation"]
    require(
        reconciliation["evidence_reconciled"] is True
        and reconciliation["bootstrap_manifest_validated"] is True
        and reconciliation["end_fence_seen"] is True
        and reconciliation["expected_sessions"] == reconciliation["completed_sessions"] == 4
        and reconciliation["accepted_client_transactions"] == clients
        and reconciliation["joined_workload_transactions"] == clients
        and reconciliation["unmatched_client_records"] == 0
        and reconciliation["unmatched_workload_cdc_slots"] == 0
        and reconciliation["ambiguous_attempts"] == 0,
        "final reconciliation differs",
    )
    require(
        final["assembled_cdc_transactions"]
        == reconciliation["bootstrap_cdc_transactions"] + clients + 3
        and final["checked_transactions"]
        == reconciliation["checker_consumed_transactions"]
        == reconciliation["bootstrap_checker_transactions"]
        + reconciliation["phase_anchor_transactions"]
        + clients,
        "final CDC/checker counter algebra differs",
    )
    if outcome == "PASS":
        require(final["checker_observation"] == "PASS_SO_FAR" and final["diagnostics"] == [], "PASS attribution differs")
    else:
        require(final["checker_observation"] == "VIOLATION_OBSERVED"
                and any(item.get("category") == "CHECKER" and item.get("code") == "VIOLATION_OBSERVED"
                        for item in final["diagnostics"]), "VIOLATION attribution differs")
    require(
        metrics.get("kind") == "performance_measurement" and metrics.get("run_id") == run_id
        and metrics.get("completed") is True and metrics.get("completion_status") == "completed"
        and metrics.get("evidence_reconciled") is True and metrics.get("outcome") == outcome,
        "monitor metrics outcome differs",
    )
    joiner = metrics["joiner"]
    require(joiner["accepted_client_transactions"] == joiner["accepted_workload_cdc_transactions"]
            == joiner["joined_workload_transactions"] == clients
            and joiner["joined_read_events"] == reads
            and joiner["pending_client_records"] == joiner["pending_cdc_slots"] == 0
            and joiner["unmatched_client_records"] == joiner["unmatched_workload_cdc_slots"] == 0,
            "joiner totals differ")
    measured_reconciliation = metrics.get("reconciliation")
    require(
        isinstance(measured_reconciliation, dict)
        and all(measured_reconciliation.get(key) == value for key, value in reconciliation.items()),
        "metrics/status reconciliation differs",
    )
    return final


def validate_acceptance(config_path: Path, run_path: Path, client_path: Path,
                        cdc_tail_path: Path, status_path: Path, monitor_path: Path,
                        benchbase_log: Path, loader_path: Path, replay_path: Path):
    config = load_config(config_path); run_id = run_path.read_text(encoding="ascii").strip()
    require(re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", run_id) is not None, "run ID")
    resource = validate_resource_exhaustion(config["resource_exhaustion_metrics"])
    replay = validate_replay_manifest(replay_path, config, run_id)
    records = [row for _, row in common.payload_records(client_path, "client stream")]
    clients, correlations, per_type, attempts = decode_client_evidence(config, run_id, records)
    benchbase = benchbase_mix_outcomes(benchbase_log, per_type, attempts)
    loader = common.load_json(loader_path, "loader validation")
    loader_counts = {row["table"]: row["row_count"] for row in loader["observed_table_row_counts"]}
    bootstrap = next(row for row in records if row.get("kind") == "bootstrap_manifest")
    require(loader_counts == {row["table"]: row["row_count"] for row in bootstrap["table_row_counts"]},
            "loader/bootstrap all-nine counts differ")
    statuses = [row for _, row in common.payload_records(status_path, "status stream")]
    load = common.one_status(statuses, "LOAD_FINISHED_SEEN")
    cdc = validate_cdc_tail(cdc_tail_path, run_id, load["last_cdc_stream_id"], correlations)
    require(cdc["markers_by_type"] == per_type, "per-type marker/join counts differ")
    reads = sum(row["read_count"] for row in clients)
    final = validate_terminal(config, run_id, statuses, common.load_json(monitor_path, "monitor metrics"), len(clients), reads)
    return {"kind": "tpcc_full_mix_acceptance", "passed": True, "run_id": run_id,
            "configuration": config, "outcome": final["outcome"],
            "per_type": {name: {"clients": per_type[name], "markers": per_type[name],
                                "joins": per_type[name]} for name in TRANSACTION_TYPES},
            "attempts": attempts, "benchbase_outcomes": benchbase,
            "cdc": cdc, "resource_exhaustion": resource,
            "replay": replay, "sessions": {"expected": 4, "completed": 4},
            "reconciliation": {"accepted_client_transactions": len(clients),
                "joined_workload_transactions": len(clients), "unmatched_client_records": 0,
                "unmatched_workload_cdc_slots": 0, "ambiguous_attempts": 0}}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("config", "run-id-file", "client-stream", "cdc-tail", "status-stream",
                 "monitor-metrics", "benchbase-log", "loader-validation", "replay-manifest", "output"):
        parser.add_argument(f"--{flag}", required=True, type=Path)
    args = parser.parse_args()
    try:
        report = validate_acceptance(args.config, args.run_id_file, args.client_stream,
            args.cdc_tail, args.status_stream, args.monitor_metrics, args.benchbase_log,
            args.loader_validation, args.replay_manifest)
        common.write_report(args.output, report)
    except AcceptanceError as error:
        print(f"TPC-C full-mix acceptance failed: {error}", file=sys.stderr); return 1
    print(json.dumps(report, sort_keys=True, separators=(",", ":"))); return 0


if __name__ == "__main__":
    raise SystemExit(main())
