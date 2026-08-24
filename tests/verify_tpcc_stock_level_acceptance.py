#!/usr/bin/env python3
"""Validate a focused live TPC-C StockLevel black-box acceptance run."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "protocol" / "tools"))

import tpcc_acceptance as common  # noqa: E402
from verify_protocol import load_adapter_contracts, load_schema_validator  # noqa: E402


AcceptanceError = common.AcceptanceError
require = common.require
EXPECTED_TABLES = common.EXPECTED_TABLES
EXPECTED_WEIGHTS = [0, 0, 0, 0, 100]


def load_config(path: Path) -> dict[str, Any]:
    config = common.load_json(path, "run configuration")
    required = {
        "kind", "benchmark", "backend", "declared_isolation", "warehouses",
        "terminals", "weights", "random_seed", "warmup_seconds", "duration_seconds",
        "rate", "loader_threads", "loader_batch_size", "minimum_transactions",
        "monitor_max_bootstrap_state_rows", "monitor_max_bootstrap_state_bytes",
        "stack_timeout_seconds", "loader_timeout_seconds", "workload_timeout_seconds",
        "status_timeout_seconds", "case_timeout_seconds", "kill_after_seconds",
        "keep_stack", "redis_host", "redis_port", "db_host", "db_port",
        "expected_session_ids",
    }
    require(isinstance(config, dict) and set(config) == required, "run configuration fields differ")
    require(config["kind"] == "tpcc_stock_level_acceptance_config", "run configuration kind")
    require(config["benchmark"] == "tpcc", "run benchmark")
    common.expected_source_id(config)
    require(config["declared_isolation"] == "REPEATABLE_READ", "run isolation")
    require(config["warehouses"] == 1 and 1 <= config["terminals"] <= 10, "live topology")
    require(config["weights"] == EXPECTED_WEIGHTS, "weights are not StockLevel-only")
    for field in (
        "terminals", "duration_seconds", "loader_threads", "loader_batch_size",
        "minimum_transactions", "monitor_max_bootstrap_state_rows",
        "monitor_max_bootstrap_state_bytes", "stack_timeout_seconds",
        "loader_timeout_seconds", "workload_timeout_seconds", "status_timeout_seconds",
        "case_timeout_seconds", "kill_after_seconds", "redis_port", "db_port",
    ):
        require(isinstance(config[field], int) and config[field] > 0, f"{field} is not positive")
    require(isinstance(config["warmup_seconds"], int) and config["warmup_seconds"] >= 0, "warmup")
    require(
        isinstance(config["random_seed"], int)
        and -(1 << 31) <= config["random_seed"] < (1 << 31),
        "random_seed is not signed 32-bit",
    )
    expected = [f"tpcc/worker/{index}" for index in range(1, config["terminals"] + 1)]
    require(config["expected_session_ids"] == expected, "configured session roster differs")
    return config


def worker_district_range(session_id: str, terminals: int) -> tuple[int, int]:
    match = re.fullmatch(r"tpcc/worker/([1-9][0-9]*)", session_id)
    require(match is not None, f"invalid StockLevel session {session_id}")
    worker = int(match.group(1))
    require(worker <= terminals, f"StockLevel worker exceeds terminal count")
    per_terminal = 10 / terminals
    return int((worker - 1) * per_terminal) + 1, (10 if worker == terminals else int(worker * per_terminal))


def decode_clients(config: dict, run_id: str, records: list, decoder: common.CanonicalDecoder):
    common.validate_record_schemas(records)
    manifests = [row for row in records if row.get("kind") == "run_manifest"]
    bootstraps = [row for row in records if row.get("kind") == "bootstrap_manifest"]
    clients = [row for row in records if row.get("kind") == "client_txn"]
    done_records = [row for row in records if row.get("kind") == "session_done"]
    require(len(manifests) == len(bootstraps) == 1, "manifest count differs")
    require(not any(row.get("kind") == "run_failed" for row in records), "RunFailed present")
    manifest = manifests[0]
    require(
        manifest["run_id"] == run_id and manifest["benchmark"] == "tpcc"
        and manifest["backend"] == config["backend"]
        and manifest["declared_isolation"] == "REPEATABLE_READ",
        "RunManifest identity differs",
    )
    require(manifest["expected_session_ids"] == config["expected_session_ids"], "RunManifest roster")
    require(manifest["application_table_allowlist"] == EXPECTED_TABLES, "RunManifest tables")
    require(bootstraps[0]["run_id"] == run_id, "BootstrapManifest run ID")
    require(len(clients) >= config["minimum_transactions"], "too few successful StockLevel transactions")
    correlations, by_session = set(), {}
    for index, client in enumerate(clients):
        label = f"ClientTxn[{index}]"
        require(client["run_id"] == run_id and client["txn_type"] == "StockLevel", f"{label} identity")
        require(client["read_count"] == len(client["reads"]) == 1, f"{label} must have exactly one read")
        read = client["reads"][0]
        require(
            read["event_seq"] == 0 and read["source"] == "snapshot"
            and read["value_state"] == "present"
            and common.read_digest(client["reads"]) == client["read_digest"],
            f"{label} snapshot read/digest differs",
        )
        key_bytes = common.decode_base64(read["key_bytes_base64"], f"{label} key")
        value_bytes = common.decode_base64(read["value_bytes_base64"], f"{label} value")
        table, key = decoder.decode_key(key_bytes, f"{label} key")
        decoder.decode_value(table, value_bytes, f"{label} value")
        require(table == "benchbase.district" and key["d_w_id"] == 1, f"{label} includes stock/aggregate evidence")
        lower, upper = worker_district_range(client["session_id"], config["terminals"])
        require(lower <= key["d_id"] <= upper, f"{label} district differs from worker range")
        identity = (client["session_id"], client["correlation_id"]["txn_id"])
        require(client["correlation_id"]["run_id"] == run_id and identity not in correlations, f"{label} correlation")
        correlations.add(identity)
        by_session.setdefault(client["session_id"], []).append(client)
    require(set(by_session) == set(config["expected_session_ids"]), "ClientTxn roster")
    done_by_session = {row["session_id"]: row for row in done_records}
    require(set(done_by_session) == set(config["expected_session_ids"]), "SessionDone roster")
    for session_id in config["expected_session_ids"]:
        session_clients = sorted(by_session[session_id], key=lambda row: row["session_seq"])
        count = len(session_clients)
        require([row["session_seq"] for row in session_clients] == list(range(1, count + 1)), f"{session_id} sequence")
        require([row["correlation_id"]["txn_id"] for row in session_clients] == list(range(1, count + 1)), f"{session_id} marker sequence")
        done = done_by_session[session_id]
        require(
            done["run_id"] == run_id and done["final_session_seq"] == count
            and done["logical_transactions_started"] == count
            and done["successful_transactions"] == done["published_transactions"] == count
            and done["terminal_aborted_transactions"] == 0
            and done["physical_attempts_started"] == count
            and done["definitively_aborted_attempts"] == done["retry_attempts"] == 0
            and done["ambiguous_attempts"] == 0,
            f"{session_id} SessionDone accounting differs",
        )
        require(done["rolling_correlation_digest"] == common.rolling_digest(run_id, session_id, session_clients), f"{session_id} rolling digest")
    return clients, correlations


def validate_marker_cdc(path: Path, run_id: str, load_id: str, correlations: set,
                        profile: dict):
    entries = common.raw_cdc_records(path)
    require(entries and common.parse_stream_id(entries[0][0], "CDC first")
            > common.parse_stream_id(load_id, "LOAD cursor"), "CDC tail boundary")
    transactions, current = [], None
    for entry in entries:
        status = entry[2].get("status")
        if status == "BEGIN": require(current is None, "nested BEGIN"); current = [entry]
        elif status == "END": require(current is not None, "END without BEGIN"); current.append(entry); transactions.append(current); current = None
        else: require(current is not None, "row outside transaction"); current.append(entry)
    require(current is None, "incomplete transaction")
    markers, fences = set(), 0
    for index, transaction in enumerate(transactions):
        label = f"CDC transaction {index}"; begin, end = transaction[0][2], transaction[-1][2]
        rows, txid = transaction[1:-1], begin.get("id")
        common.require_connector_transaction_id(end.get("id"), txid, profile, f"{label} END")
        require(end.get("event_count") == len(rows) == 1, f"{label} framing/count")
        _, key, envelope = rows[0]; source, meta = envelope.get("source"), envelope.get("transaction")
        require(isinstance(source, dict) and source.get("name") == profile["source_id"]
                and source.get("db") == "benchbase" and isinstance(meta, dict)
                and meta.get("total_order") == 1
                and meta.get("data_collection_order") == 1, f"{label} metadata")
        common.require_connector_transaction_id(meta.get("id"), txid, profile, f"{label} row")
        table = source.get("table")
        require(end.get("data_collections") == [{"data_collection": f"benchbase.{table}", "event_count": 1}], f"{label} collection")
        if table == "checker_run_fence":
            require(index == len(transactions) - 1 and envelope.get("after", {}).get("run_id") == run_id
                    and envelope["after"].get("phase") == "END_FENCE", "END_FENCE differs")
            fences += 1; continue
        require(table == "session_txn", f"{label} contains application/aggregate-derived CDC evidence")
        after, before = envelope.get("after"), envelope.get("before")
        identity = (after["session_id"], after["txn_id"])
        require(envelope.get("op") == "c" and before is None
                and key == {"session_id": after["session_id"], "txn_id": after["txn_id"]}
                and identity in correlations and identity not in markers,
                f"{label} marker identity")
        markers.add(identity)
    require(fences == 1 and markers == correlations, "CDC marker reconciliation differs")
    return {"transactions": len(transactions), "markers": len(markers), "end_fences": 1}


def validate_acceptance(config_path: Path, run_path: Path, client_path: Path,
                        cdc_path: Path, status_path: Path, monitor_path: Path,
                        benchbase_log: Path) -> dict[str, Any]:
    config = load_config(config_path); run_id = run_path.read_text(encoding="ascii").strip()
    require(re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", run_id) is not None, "run ID")
    _, schemas = load_schema_validator(); contracts = load_adapter_contracts(schemas)
    clients, correlations = decode_clients(
        config, run_id, [row for _, row in common.payload_records(client_path, "client stream")],
        common.CanonicalDecoder(contracts["tpcc"]))
    outcomes = common.benchbase_outcomes(benchbase_log, "StockLevel", len(clients), 0, 0)
    statuses = [row for _, row in common.payload_records(status_path, "status stream")]
    load = common.one_status(statuses, "LOAD_FINISHED_SEEN")
    cdc = validate_marker_cdc(cdc_path, run_id, load["last_cdc_stream_id"], correlations,
                              common.backend_profile(config))
    final, _ = common.validate_audited_status_and_metrics(
        config, run_id, statuses, common.load_json(monitor_path, "monitor metrics"),
        len(clients), len(clients), marker_only_count=len(clients), write_count=0)
    return {"kind": "tpcc_stock_level_acceptance", "passed": True, "run_id": run_id,
            "configuration": config, "coverage": {"transactions": len(clients), "reads": len(clients)},
            "benchbase_outcomes": outcomes, "cdc": cdc,
            "sessions": {"expected": config["terminals"], "completed": final["reconciliation"]["completed_sessions"]},
            "reconciliation": {"accepted_client_transactions": len(clients),
                "joined_workload_transactions": final["reconciliation"]["joined_workload_transactions"],
                "unmatched_client_records": 0, "unmatched_workload_cdc_slots": 0},
            "final_outcome": "PASS"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("config", "run-id-file", "client-stream", "cdc-tail", "status-stream",
                 "monitor-metrics", "benchbase-log", "output"):
        parser.add_argument(f"--{flag}", required=True, type=Path)
    args = parser.parse_args()
    try:
        report = validate_acceptance(args.config, args.run_id_file, args.client_stream,
            args.cdc_tail, args.status_stream, args.monitor_metrics, args.benchbase_log)
        common.write_report(args.output, report)
    except AcceptanceError as error:
        print(f"TPC-C StockLevel acceptance failed: {error}", file=sys.stderr); return 1
    print(json.dumps(report, sort_keys=True, separators=(",", ":"))); return 0


if __name__ == "__main__":
    raise SystemExit(main())
