#!/usr/bin/env python3
"""Validate a focused live TPC-C Delivery black-box acceptance run."""

from __future__ import annotations

import argparse
import json
import re
import struct
import sys
from collections import Counter, defaultdict
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "protocol" / "tools"))

import tpcc_acceptance as common  # noqa: E402
from tpcc_codec import CodecError, TpccCodec  # noqa: E402
from verify_protocol import load_adapter_contracts, load_schema_validator  # noqa: E402


AcceptanceError = common.AcceptanceError
require = common.require
EXPECTED_WEIGHTS = [0, 0, 0, 100, 0]
EXPECTED_TABLES = common.EXPECTED_TABLES
EXPECTED_READ_TABLES = {"benchbase.new_order", "benchbase.oorder"}
EXPECTED_WRITE_TABLES = {
    "benchbase.new_order",
    "benchbase.oorder",
    "benchbase.order_line",
    "benchbase.customer",
}


def load_config(path: Path) -> dict[str, Any]:
    config = common.load_json(path, "run configuration")
    required = {
        "kind", "benchmark", "backend", "declared_isolation", "warehouses",
        "terminals", "weights", "random_seed", "warmup_seconds",
        "duration_seconds", "rate", "loader_threads", "loader_batch_size",
        "minimum_transactions", "minimum_empty_transactions",
        "minimum_active_transactions", "minimum_multi_district_transactions",
        "minimum_multi_line_districts", "minimum_retries",
        "monitor_max_bootstrap_state_rows", "monitor_max_bootstrap_state_bytes",
        "stack_timeout_seconds", "loader_timeout_seconds", "workload_timeout_seconds",
        "status_timeout_seconds", "case_timeout_seconds", "kill_after_seconds",
        "keep_stack", "redis_host", "redis_port", "db_host", "db_port",
        "expected_session_ids",
    }
    require(isinstance(config, dict) and set(config) == required, "run configuration fields differ")
    require(config["kind"] == "tpcc_delivery_acceptance_config", "run configuration kind")
    require(config["benchmark"] == "tpcc", "run benchmark")
    common.expected_source_id(config)
    require(config["declared_isolation"] == "REPEATABLE_READ", "run isolation is not RR")
    require(
        config["warehouses"] == 1 and 2 <= config["terminals"] <= 10,
        "Delivery live topology differs",
    )
    require(config["weights"] == EXPECTED_WEIGHTS, "acceptance weights are not Delivery-only")
    for field in (
        "terminals", "duration_seconds", "loader_threads", "loader_batch_size",
        "minimum_transactions", "minimum_empty_transactions",
        "minimum_active_transactions", "minimum_multi_district_transactions",
        "minimum_multi_line_districts", "monitor_max_bootstrap_state_rows",
        "monitor_max_bootstrap_state_bytes", "stack_timeout_seconds",
        "loader_timeout_seconds", "workload_timeout_seconds", "status_timeout_seconds",
        "case_timeout_seconds", "kill_after_seconds", "redis_port", "db_port",
    ):
        require(isinstance(config[field], int) and config[field] > 0, f"{field} is not positive")
    require(isinstance(config["minimum_retries"], int) and config["minimum_retries"] >= 0, "minimum_retries")
    require(isinstance(config["warmup_seconds"], int) and config["warmup_seconds"] >= 0, "warmup")
    require(
        isinstance(config["random_seed"], int)
        and -(1 << 31) <= config["random_seed"] < (1 << 31),
        "random_seed is not signed 32-bit",
    )
    require(
        config["rate"] == "unlimited"
        or (isinstance(config["rate"], str) and re.fullmatch(r"[1-9][0-9]*", config["rate"])),
        "rate differs",
    )
    expected_sessions = [f"tpcc/worker/{index}" for index in range(1, config["terminals"] + 1)]
    require(config["expected_session_ids"] == expected_sessions, "configured session roster differs")
    return config


def worker_district_range(session_id: str, terminals: int) -> tuple[int, int]:
    match = re.fullmatch(r"tpcc/worker/([1-9][0-9]*)", session_id)
    require(match is not None, f"invalid Delivery session ID {session_id}")
    worker = int(match.group(1))
    require(worker <= terminals, f"Delivery session worker exceeds terminal count: {session_id}")
    # Delivery intentionally ignores TPCCWorker's lower bound and starts at
    # district 1. Later workers therefore overlap earlier workers, which is the
    # production race exercised by the live gate.
    districts_per_terminal = 10 / terminals
    lower = 1
    upper = 10 if worker == terminals else int(worker * districts_per_terminal)
    return lower, upper


def decode_clients(
    config: dict[str, Any], run_id: str, records: list[dict[str, Any]], decoder: common.CanonicalDecoder
) -> tuple[list[dict[str, Any]], dict[tuple[str, int], dict[str, Any]], dict[str, int]]:
    common.validate_record_schemas(records)
    manifests = [record for record in records if record.get("kind") == "run_manifest"]
    bootstraps = [record for record in records if record.get("kind") == "bootstrap_manifest"]
    clients = [record for record in records if record.get("kind") == "client_txn"]
    done_records = [record for record in records if record.get("kind") == "session_done"]
    require(len(manifests) == len(bootstraps) == 1, "client stream manifest count differs")
    require(not any(record.get("kind") == "run_failed" for record in records), "client stream has RunFailed")
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
    require(len(clients) >= config["minimum_transactions"], "too few successful Delivery transactions")
    shapes = {}
    by_session: dict[str, list[dict[str, Any]]] = defaultdict(list)
    empty_transactions = active_transactions = multi_district = total_reads = 0
    for client_index, client in enumerate(clients):
        label = f"ClientTxn[{client_index}]"
        require(client["run_id"] == run_id and client["txn_type"] == "Delivery", f"{label} identity")
        require(client["correlation_kind"] == "session_txn", f"{label} correlation kind")
        require(client["read_count"] == len(client["reads"]), f"{label} read count")
        require(common.read_digest(client["reads"]) == client["read_digest"], f"{label} digest")
        identity = (client["session_id"], client["correlation_id"]["txn_id"])
        require(client["correlation_id"]["run_id"] == run_id and identity not in shapes, f"{label} correlation")
        by_session[client["session_id"]].append(client)
        decoded = []
        for read_index, read in enumerate(client["reads"]):
            require(
                read["event_seq"] == read_index and read["source"] == "snapshot"
                and read["value_state"] == "present",
                f"{label} read sequence/source/state differs",
            )
            key_bytes = common.decode_base64(read["key_bytes_base64"], f"{label} key")
            value_bytes = common.decode_base64(read["value_bytes_base64"], f"{label} value")
            table, key = decoder.decode_key(key_bytes, f"{label} key")
            value = decoder.decode_value(table, value_bytes, f"{label} value")
            decoded.append({"table": table, "key": key, "row": {**key, **value},
                            "key_bytes": key_bytes, "value_bytes": value_bytes})
        total_reads += len(decoded)
        require(len(decoded) % 2 == 0, f"{label} does not contain new_order/oorder pairs")
        pairs = []
        for position in range(0, len(decoded), 2):
            new_order, order = decoded[position : position + 2]
            require(
                new_order["table"] == "benchbase.new_order"
                and order["table"] == "benchbase.oorder",
                f"{label} includes aggregate/other read or wrong pair order",
            )
            require(
                new_order["row"]["no_w_id"] == order["row"]["o_w_id"] == 1
                and new_order["row"]["no_d_id"] == order["row"]["o_d_id"]
                and new_order["row"]["no_o_id"] == order["row"]["o_id"],
                f"{label} new_order/oorder keys differ",
            )
            pairs.append({"new_order": new_order, "order": order})
        districts = [pair["new_order"]["row"]["no_d_id"] for pair in pairs]
        lower, upper = worker_district_range(client["session_id"], config["terminals"])
        require(
            len(districts) == len(set(districts))
            and all(lower <= district <= upper for district in districts),
            f"{label} active districts differ from worker range",
        )
        empty_districts = (upper - lower + 1) - len(districts)
        empty_transactions += int(not pairs)
        active_transactions += int(bool(pairs))
        multi_district += int(len(pairs) >= 2)
        shapes[identity] = {"session_seq": client["session_seq"], "pairs": pairs,
                            "empty_districts": empty_districts}
    require(empty_transactions >= config["minimum_empty_transactions"], "empty Delivery lacks proof")
    require(active_transactions >= config["minimum_active_transactions"], "active Delivery lacks proof")
    require(multi_district >= config["minimum_multi_district_transactions"], "multi-district lacks proof")
    require(set(by_session) == set(config["expected_session_ids"]), "ClientTxn roster differs")
    done_by_session = {record["session_id"]: record for record in done_records}
    require(set(done_by_session) == set(config["expected_session_ids"]), "SessionDone roster differs")
    terminal_aborts = retries = definitive_aborts = retry_increases = 0
    for session_id in config["expected_session_ids"]:
        session_clients = sorted(by_session[session_id], key=lambda item: item["session_seq"])
        sequences = [client["session_seq"] for client in session_clients]
        txn_ids = [client["correlation_id"]["txn_id"] for client in session_clients]
        require(sequences == sorted(set(sequences)) and txn_ids == sorted(set(txn_ids)), f"{session_id} sequence")
        previous_surplus = 0
        for txn_id, sequence in zip(txn_ids, sequences, strict=True):
            surplus = txn_id - sequence
            require(surplus >= previous_surplus, f"{session_id} retry surplus decreases")
            retry_increases += int(surplus > previous_surplus)
            previous_surplus = surplus
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
            and done["ambiguous_attempts"] == 0,
            f"{session_id} SessionDone accounting differs",
        )
        require(len(set(range(1, logical + 1)) - set(sequences)) == aborted, f"{session_id} abort gaps")
        require(
            done["rolling_correlation_digest"]
            == common.rolling_digest(run_id, session_id, session_clients),
            f"{session_id} rolling digest differs",
        )
        terminal_aborts += aborted
        retries += retry_count
        definitive_aborts += done["definitively_aborted_attempts"]
    require(retries >= config["minimum_retries"], "retry accounting below minimum")
    if retries:
        require(retry_increases > 0, "retries produce no published attempt-identity increase")
    return clients, shapes, {
        "transactions": len(clients), "reads": total_reads,
        "empty_transactions": empty_transactions, "active_transactions": active_transactions,
        "multi_district_transactions": multi_district,
        "terminal_aborts": terminal_aborts, "retries": retries,
        "definitive_aborts": definitive_aborts,
        "retry_surplus_increases": retry_increases,
    }


def decimal(value: Any, label: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise AcceptanceError(f"{label} decimal differs: {error}") from error


def unchanged_except(before: dict[str, Any], after: dict[str, Any], changed: set[str], label: str):
    require(set(before) == set(after), f"{label} before/after columns differ")
    require(all(before[key] == after[key] for key in before if key not in changed), f"{label} other column changed")


def canonical_row(codec: TpccCodec, table: str, row: dict, label: str):
    try:
        return codec.encode_row(table, row)
    except CodecError as error:
        raise AcceptanceError(f"{label} canonical encoding failed: {error.code}: {error}") from error


def validate_delivery_rows(label: str, shape: dict, rows: list, codec: TpccCodec) -> tuple[int, int]:
    by_table: dict[str, list] = defaultdict(list)
    for key, envelope in rows:
        table = f"benchbase.{envelope['source']['table']}"
        require(table in EXPECTED_WRITE_TABLES, f"{label} writes forbidden table {table}")
        by_table[table].append((key, envelope))
    active = len(shape["pairs"])
    require(
        len(by_table["benchbase.new_order"]) == active
        and len(by_table["benchbase.oorder"]) == active
        and len(by_table["benchbase.customer"]) == active,
        f"{label} delete/order/customer counts differ from active districts",
    )
    require(set(by_table) <= EXPECTED_WRITE_TABLES, f"{label} write roster differs")
    lines_by_order: dict[tuple[int, int, int], list] = defaultdict(list)
    for key, envelope in by_table["benchbase.order_line"]:
        after = envelope.get("after")
        before = envelope.get("before")
        require(
            envelope.get("op") == "u" and isinstance(before, dict) and isinstance(after, dict)
            and key == {"ol_w_id": after["ol_w_id"], "ol_d_id": after["ol_d_id"],
                        "ol_o_id": after["ol_o_id"], "ol_number": after["ol_number"]}
            and after["ol_w_id"] == 1 and before["ol_delivery_d"] is None
            and after["ol_delivery_d"] is not None,
            f"{label} order_line delivery update differs",
        )
        unchanged_except(before, after, {"ol_delivery_d"}, f"{label} order_line")
        lines_by_order[(after["ol_w_id"], after["ol_d_id"], after["ol_o_id"])].append(after)
    carrier_ids = set()
    for pair in shape["pairs"]:
        no = pair["new_order"]["row"]
        order_snapshot = pair["order"]
        order_key = (1, no["no_d_id"], no["no_o_id"])
        deletes = [entry for entry in by_table["benchbase.new_order"] if entry[0] == {
            "no_w_id": 1, "no_d_id": no["no_d_id"], "no_o_id": no["no_o_id"]}]
        orders = [entry for entry in by_table["benchbase.oorder"] if entry[0] == {
            "o_w_id": 1, "o_d_id": no["no_d_id"], "o_id": no["no_o_id"]}]
        require(len(deletes) == len(orders) == 1, f"{label} active district write identity differs")
        _, delete = deletes[0]
        _, order = orders[0]
        require(
            delete.get("op") == "d" and delete.get("after") is None
            and isinstance(delete.get("before"), dict),
            f"{label} new_order delete differs",
        )
        require(
            canonical_row(codec, "benchbase.new_order", delete["before"], label)
            == (pair["new_order"]["key_bytes"], pair["new_order"]["value_bytes"]),
            f"{label} new_order snapshot differs from delete before-image",
        )
        before_o, after_o = order.get("before"), order.get("after")
        require(order.get("op") == "u" and isinstance(before_o, dict) and isinstance(after_o, dict), f"{label} oorder update")
        require(
            canonical_row(codec, "benchbase.oorder", before_o, label)
            == (order_snapshot["key_bytes"], order_snapshot["value_bytes"]),
            f"{label} oorder snapshot differs from before-image",
        )
        require(before_o["o_carrier_id"] is None and isinstance(after_o["o_carrier_id"], int)
                and 1 <= after_o["o_carrier_id"] <= 10, f"{label} carrier update differs")
        unchanged_except(before_o, after_o, {"o_carrier_id"}, f"{label} oorder")
        carrier_ids.add(after_o["o_carrier_id"])
        order_lines = lines_by_order.get(order_key, [])
        require(
            len(order_lines) == order_snapshot["row"]["o_ol_cnt"] >= 1,
            f"{label} order_line update count differs from captured o_ol_cnt",
        )
        delivery_timestamps = {line["ol_delivery_d"] for line in order_lines}
        require(
            len(delivery_timestamps) == 1 and None not in delivery_timestamps,
            f"{label} order_line delivery timestamps differ within one order",
        )
        customers = [entry for entry in by_table["benchbase.customer"] if entry[0] == {
            "c_w_id": 1, "c_d_id": no["no_d_id"], "c_id": before_o["o_c_id"]}]
        require(len(customers) == 1, f"{label} customer update identity differs")
        _, customer = customers[0]
        before_c, after_c = customer.get("before"), customer.get("after")
        require(customer.get("op") == "u" and isinstance(before_c, dict) and isinstance(after_c, dict), f"{label} customer update")
        exact_total = sum(
            (decimal(line["ol_amount"], label) for line in order_lines), Decimal("0.00")
        )
        float_total = struct.unpack(">f", struct.pack(">f", float(exact_total)))[0]
        expected_balance = (
            decimal(before_c["c_balance"], label) + Decimal(str(float_total))
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        require(
            after_c["c_delivery_cnt"] == before_c["c_delivery_cnt"] + 1
            and decimal(after_c["c_balance"], label) == expected_balance,
            f"{label} customer delivery update differs",
        )
        unchanged_except(before_c, after_c, {"c_balance", "c_delivery_cnt"}, f"{label} customer")
    require(len(carrier_ids) <= 1, f"{label} uses multiple carrier IDs in one Delivery")
    require(len(lines_by_order) == active, f"{label} order_line updates include unselected order")
    multi_line = sum(len(lines) >= 2 for lines in lines_by_order.values())
    return 3 * active + sum(len(lines) for lines in lines_by_order.values()), multi_line


def validate_cdc_tail(path: Path, run_id: str, load_id: str, shapes: dict, codec: TpccCodec,
                      profile: dict):
    entries = common.raw_cdc_records(path)
    require(entries and common.parse_stream_id(entries[0][0], "CDC first")
            > common.parse_stream_id(load_id, "LOAD cursor"), "CDC tail boundary differs")
    transactions, current = [], None
    for entry in entries:
        if entry[2].get("status") == "BEGIN":
            require(current is None, "nested BEGIN")
            current = [entry]
        elif entry[2].get("status") == "END":
            require(current is not None, "END without BEGIN")
            current.append(entry); transactions.append(current); current = None
        else:
            require(current is not None, "row outside transaction"); current.append(entry)
    require(current is None, "incomplete CDC transaction")
    correlations = set()
    end_fences = normalized_writes = multi_line = marker_only = 0
    for index, transaction in enumerate(transactions):
        label = f"CDC transaction {index}"
        begin, end, rows = transaction[0][2], transaction[-1][2], transaction[1:-1]
        txid = begin.get("id")
        require(begin.get("status") == "BEGIN" and end.get("status") == "END"
                and end.get("event_count") == len(rows), f"{label} framing")
        common.require_connector_transaction_id(end.get("id"), txid, profile, f"{label} END")
        counts, orders, parsed = Counter(), Counter(), []
        for total, (_, key, envelope) in enumerate(rows, 1):
            source, meta = envelope.get("source"), envelope.get("transaction")
            require(isinstance(source, dict) and source.get("name") == profile["source_id"]
                    and source.get("db") == "benchbase" and isinstance(meta, dict)
                    and meta.get("total_order") == total, f"{label} metadata")
            common.require_connector_transaction_id(meta.get("id"), txid, profile, f"{label} row")
            collection = f"benchbase.{source['table']}"; counts[collection] += 1; orders[collection] += 1
            require(meta.get("data_collection_order") == orders[collection], f"{label} collection order")
            parsed.append((key, envelope))
        declared = Counter()
        for item in end.get("data_collections", []): declared[item["data_collection"]] += item["event_count"]
        require(declared == counts, f"{label} collection counts")
        fence = [row for row in parsed if row[1]["source"]["table"] == "checker_run_fence"]
        marker = [row for row in parsed if row[1]["source"]["table"] == "session_txn"]
        if fence:
            require(len(fence) == len(parsed) == 1 and index == len(transactions) - 1, "END_FENCE mixed/not final")
            require(fence[0][1].get("after", {}).get("run_id") == run_id
                    and fence[0][1]["after"].get("phase") == "END_FENCE", "END_FENCE identity")
            end_fences += 1; continue
        require(len(marker) == 1 and parsed[-1] is marker[0], f"{label} marker absent/not final")
        key, envelope = marker[0]; after, before = envelope.get("after"), envelope.get("before")
        identity = (after["session_id"], after["txn_id"])
        require(envelope.get("op") == "c" and before is None
                and key == {"session_id": after["session_id"], "txn_id": after["txn_id"]}
                and identity in shapes
                and identity not in correlations, f"{label} marker identity")
        correlations.add(identity)
        app = [row for row in parsed if row is not marker[0]]
        writes, multiple = validate_delivery_rows(label, shapes[identity], app, codec)
        normalized_writes += writes; multi_line += multiple; marker_only += int(not app)
    require(end_fences == 1 and correlations == set(shapes), "CDC reconciliation differs")
    return {"transactions": len(transactions), "markers": len(correlations), "end_fences": 1,
            "normalized_writes": normalized_writes, "multi_line_districts": multi_line,
            "marker_only_transactions": marker_only}


def validate_acceptance(config_path: Path, run_id_path: Path, client_path: Path,
                        cdc_path: Path, status_path: Path, monitor_path: Path,
                        benchbase_log_path: Path) -> dict[str, Any]:
    config = load_config(config_path)
    run_id = run_id_path.read_text(encoding="ascii").strip()
    require(re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", run_id) is not None, "run ID")
    _, schemas = load_schema_validator(); contracts = load_adapter_contracts(schemas)
    decoder, codec = common.CanonicalDecoder(contracts["tpcc"]), TpccCodec(contracts["tpcc"])
    clients, shapes, coverage = decode_clients(
        config, run_id, [record for _, record in common.payload_records(client_path, "client stream")], decoder)
    outcomes = common.benchbase_outcomes(
        benchbase_log_path,
        "Delivery",
        len(clients),
        coverage["terminal_aborts"],
        coverage["retries"],
        server_rejections=coverage["definitive_aborts"],
    )
    status_records = [record for _, record in common.payload_records(status_path, "status stream")]
    load_status = common.one_status(status_records, "LOAD_FINISHED_SEEN")
    cdc = validate_cdc_tail(cdc_path, run_id, load_status["last_cdc_stream_id"], shapes, codec,
                            common.backend_profile(config))
    require(cdc["markers"] == len(clients), "marker/client count differs")
    require(cdc["multi_line_districts"] >= config["minimum_multi_line_districts"], "multi-line lacks proof")
    metrics = common.load_json(monitor_path, "monitor metrics")
    final, _ = common.validate_audited_status_and_metrics(
        config, run_id, status_records, metrics, len(clients), coverage["reads"],
        marker_only_count=cdc["marker_only_transactions"], write_count=cdc["normalized_writes"])
    return {
        "kind": "tpcc_delivery_acceptance", "passed": True, "run_id": run_id,
        "configuration": config, "coverage": coverage, "benchbase_outcomes": outcomes,
        "cdc": cdc,
        "sessions": {"expected": config["terminals"],
                     "completed": final["reconciliation"]["completed_sessions"]},
        "reconciliation": {"accepted_client_transactions": len(clients),
                           "joined_workload_transactions": final["reconciliation"]["joined_workload_transactions"],
                           "unmatched_client_records": 0, "unmatched_workload_cdc_slots": 0},
        "final_outcome": "PASS",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("config", "run-id-file", "client-stream", "cdc-tail", "status-stream",
                 "monitor-metrics", "benchbase-log", "output"):
        parser.add_argument(f"--{flag}", required=True, type=Path)
    args = parser.parse_args()
    try:
        report = validate_acceptance(args.config, args.run_id_file, args.client_stream,
                                     args.cdc_tail, args.status_stream, args.monitor_metrics,
                                     args.benchbase_log)
        common.write_report(args.output, report)
    except AcceptanceError as error:
        print(f"TPC-C Delivery acceptance failed: {error}", file=sys.stderr); return 1
    print(json.dumps(report, sort_keys=True, separators=(",", ":"))); return 0


if __name__ == "__main__":
    raise SystemExit(main())
