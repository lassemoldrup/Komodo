#!/usr/bin/env python3
"""Validate a focused live TPC-C Payment black-box acceptance run."""

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
EXPECTED_WEIGHTS = [0, 100, 0, 0, 0]
EXPECTED_TABLES = common.EXPECTED_TABLES
EXPECTED_READ_TABLES = {
    "benchbase.warehouse",
    "benchbase.district",
    "benchbase.customer",
}
EXPECTED_WRITE_TABLES = {
    "benchbase.warehouse",
    "benchbase.district",
    "benchbase.customer",
    "benchbase.history",
}


def load_config(path: Path) -> dict[str, Any]:
    config = common.load_json(path, "run configuration")
    required = {
        "kind",
        "benchmark",
        "backend",
        "declared_isolation",
        "warehouses",
        "terminals",
        "weights",
        "random_seed",
        "warmup_seconds",
        "duration_seconds",
        "rate",
        "loader_threads",
        "loader_batch_size",
        "minimum_transactions",
        "minimum_id_transactions",
        "minimum_last_name_transactions",
        "minimum_good_credit_transactions",
        "minimum_bad_credit_transactions",
        "minimum_retries",
        "remote_customer_live_applicable",
        "monitor_max_bootstrap_state_rows",
        "monitor_max_bootstrap_state_bytes",
        "stack_timeout_seconds",
        "loader_timeout_seconds",
        "workload_timeout_seconds",
        "status_timeout_seconds",
        "case_timeout_seconds",
        "kill_after_seconds",
        "keep_stack",
        "redis_host",
        "redis_port",
        "db_host",
        "db_port",
        "expected_session_ids",
    }
    require(isinstance(config, dict) and set(config) == required, "run configuration fields differ")
    require(config["kind"] == "tpcc_payment_acceptance_config", "run configuration kind")
    require(config["benchmark"] == "tpcc", "run benchmark")
    common.expected_source_id(config)
    require(config["declared_isolation"] == "REPEATABLE_READ", "run isolation is not RR")
    require(config["warehouses"] == 1, "acceptance run must use one warehouse")
    require(config["weights"] == EXPECTED_WEIGHTS, "acceptance weights are not Payment-only")
    require(config["remote_customer_live_applicable"] is False, "remote applicability differs at W=1")
    for field in (
        "terminals",
        "duration_seconds",
        "loader_threads",
        "loader_batch_size",
        "minimum_transactions",
        "minimum_id_transactions",
        "minimum_last_name_transactions",
        "minimum_good_credit_transactions",
        "minimum_bad_credit_transactions",
        "monitor_max_bootstrap_state_rows",
        "monitor_max_bootstrap_state_bytes",
        "stack_timeout_seconds",
        "loader_timeout_seconds",
        "workload_timeout_seconds",
        "status_timeout_seconds",
        "case_timeout_seconds",
        "kill_after_seconds",
        "redis_port",
        "db_port",
    ):
        require(isinstance(config[field], int) and config[field] > 0, f"{field} is not positive")
    require(
        isinstance(config["minimum_retries"], int) and config["minimum_retries"] >= 0,
        "minimum_retries is negative",
    )
    require(
        isinstance(config["warmup_seconds"], int) and config["warmup_seconds"] >= 0,
        "warmup_seconds is negative",
    )
    require(
        isinstance(config["random_seed"], int)
        and -(1 << 31) <= config["random_seed"] < (1 << 31),
        "random_seed is not a signed 32-bit integer",
    )
    require(
        config["rate"] == "unlimited"
        or (
            isinstance(config["rate"], str)
            and re.fullmatch(r"[1-9][0-9]*", config["rate"]) is not None
        ),
        "rate is not unlimited or a positive integer",
    )
    require(isinstance(config["keep_stack"], bool), "keep_stack is not boolean")
    expected_sessions = [f"tpcc/worker/{index}" for index in range(1, config["terminals"] + 1)]
    require(config["expected_session_ids"] == expected_sessions, "configured session roster differs")
    return config


def load_customer_catalog(path: Path) -> tuple[
    dict[tuple[int, int, int], dict[str, Any]],
    dict[tuple[int, int, str], set[tuple[int, int, int]]],
]:
    rows: dict[tuple[int, int, int], dict[str, Any]] = {}
    groups: dict[tuple[int, int, str], set[tuple[int, int, int]]] = defaultdict(set)
    try:
        lines = path.read_text(encoding="utf-8", errors="strict").splitlines()
    except (OSError, UnicodeError) as error:
        raise AcceptanceError(f"cannot read customer catalog: {error}") from error
    require(lines, "customer catalog is empty")
    expected = {
        "c_w_id",
        "c_d_id",
        "c_id",
        "c_last",
        "c_first",
        "c_credit",
        "last_name_group_count",
    }
    for index, line in enumerate(lines):
        row = common.strict_json(line, f"customer catalog line {index + 1}")
        require(isinstance(row, dict) and set(row) == expected, "customer catalog row fields differ")
        key = (row["c_w_id"], row["c_d_id"], row["c_id"])
        group = (row["c_w_id"], row["c_d_id"], row["c_last"])
        require(
            all(isinstance(value, int) and value > 0 for value in key)
            and row["c_w_id"] == 1
            and isinstance(row["c_last"], str)
            and isinstance(row["c_first"], str)
            and row["c_credit"] in {"GC", "BC"}
            and isinstance(row["last_name_group_count"], int)
            and row["last_name_group_count"] > 0,
            "customer catalog row types differ",
        )
        require(key not in rows, "customer catalog has duplicate primary key")
        rows[key] = row
        groups[group].add(key)
    for group, members in groups.items():
        require(
            all(rows[key]["last_name_group_count"] == len(members) for key in members),
            f"customer catalog SQL cardinality differs for {group}",
        )
    return rows, groups


def decode_clients(
    config: dict[str, Any],
    run_id: str,
    records: list[dict[str, Any]],
    decoder: common.CanonicalDecoder,
    catalog: dict[tuple[int, int, int], dict[str, Any]],
    groups: dict[tuple[int, int, str], set[tuple[int, int, int]]],
) -> tuple[list[dict[str, Any]], dict[tuple[str, int], dict[str, Any]], dict[str, int]]:
    common.validate_record_schemas(records)
    manifests = [record for record in records if record.get("kind") == "run_manifest"]
    bootstraps = [record for record in records if record.get("kind") == "bootstrap_manifest"]
    clients = [record for record in records if record.get("kind") == "client_txn"]
    done_records = [record for record in records if record.get("kind") == "session_done"]
    require(len(manifests) == 1 and len(bootstraps) == 1, "client stream manifest count differs")
    require(not any(record.get("kind") == "run_failed" for record in records), "client stream has RunFailed")
    manifest = manifests[0]
    require(
        manifest["run_id"] == run_id
        and manifest["benchmark"] == "tpcc"
        and manifest["backend"] == config["backend"]
        and manifest["declared_isolation"] == "REPEATABLE_READ",
        "RunManifest identity differs",
    )
    require(manifest["expected_session_ids"] == config["expected_session_ids"], "RunManifest session roster")
    require(manifest["application_table_allowlist"] == EXPECTED_TABLES, "RunManifest table roster")
    require(bootstraps[0]["run_id"] == run_id, "BootstrapManifest run ID differs")
    require(len(clients) >= config["minimum_transactions"], "too few successful Payment transactions")

    shapes: dict[tuple[str, int], dict[str, Any]] = {}
    by_session: dict[str, list[dict[str, Any]]] = defaultdict(list)
    logical_ids: set[str] = set()
    attempt_ids: set[str] = set()
    confirmed_id = 0
    confirmed_name = 0
    ambiguous_single = 0
    total_reads = 0
    for client_index, client in enumerate(clients):
        label = f"ClientTxn[{client_index}]"
        require(client["run_id"] == run_id and client["txn_type"] == "Payment", f"{label} identity")
        require(client["correlation_kind"] == "session_txn", f"{label} correlation kind")
        require(client["read_count"] == len(client["reads"]), f"{label} read count")
        require(common.read_digest(client["reads"]) == client["read_digest"], f"{label} read digest")
        require(client["logical_txn_id"] not in logical_ids, f"{label} duplicate logical ID")
        require(client["attempt_id"] not in attempt_ids, f"{label} duplicate attempt ID")
        logical_ids.add(client["logical_txn_id"])
        attempt_ids.add(client["attempt_id"])
        correlation = client["correlation_id"]
        require(
            correlation["run_id"] == run_id
            and correlation["session_id"] == client["session_id"],
            f"{label} nested correlation",
        )
        identity = (client["session_id"], correlation["txn_id"])
        require(identity not in shapes, f"{label} duplicate marker correlation")
        by_session[client["session_id"]].append(client)

        decoded: list[dict[str, Any]] = []
        for read_index, read in enumerate(client["reads"]):
            key_bytes = common.decode_base64(read["key_bytes_base64"], f"{label} key {read_index}")
            value_bytes = common.decode_base64(read["value_bytes_base64"], f"{label} value {read_index}")
            table, key = decoder.decode_key(key_bytes, f"{label} key {read_index}")
            value = decoder.decode_value(table, value_bytes, f"{label} value {read_index}")
            decoded.append(
                {
                    "table": table,
                    "key": key,
                    "row": {**key, **value},
                    "key_bytes": key_bytes,
                    "value_bytes": value_bytes,
                    "source": read["source"],
                    "state": read["value_state"],
                }
            )
            require(read["event_seq"] == read_index and read["value_state"] == "present", f"{label} read sequence/state")
        total_reads += len(decoded)
        require(
            len(decoded) >= 3
            and decoded[0]["table"] == "benchbase.warehouse"
            and decoded[0]["source"] == "local"
            and decoded[1]["table"] == "benchbase.district"
            and decoded[1]["source"] == "local"
            and all(
                read["table"] == "benchbase.customer" and read["source"] == "snapshot"
                for read in decoded[2:]
            ),
            f"{label} local/snapshot read sequence differs",
        )
        require({read["table"] for read in decoded} == EXPECTED_READ_TABLES, f"{label} read table roster differs")
        customer_reads = decoded[2:]
        cdata_reread = False
        selection_reads = customer_reads
        if len(customer_reads) >= 2 and customer_reads[-1]["key_bytes"] in {
            read["key_bytes"] for read in customer_reads[:-1]
        }:
            cdata_reread = True
            original = next(
                read
                for read in customer_reads[:-1]
                if read["key_bytes"] == customer_reads[-1]["key_bytes"]
            )
            require(
                original["value_bytes"] == customer_reads[-1]["value_bytes"],
                f"{label} bad-credit c_data reread differs from pre-update snapshot",
            )
            selection_reads = customer_reads[:-1]
        selection_keys = {
            (read["row"]["c_w_id"], read["row"]["c_d_id"], read["row"]["c_id"])
            for read in selection_reads
        }
        require(len(selection_keys) == len(selection_reads), f"{label} repeats selection customer")
        group_ids: set[tuple[int, int, str]] = set()
        for read in selection_reads:
            key = (read["row"]["c_w_id"], read["row"]["c_d_id"], read["row"]["c_id"])
            require(key in catalog, f"{label} customer is absent from independent SQL catalog")
            catalog_row = catalog[key]
            require(
                read["row"]["c_last"] == catalog_row["c_last"]
                and read["row"]["c_first"] == catalog_row["c_first"]
                and read["row"]["c_credit"] == catalog_row["c_credit"],
                f"{label} customer identity differs from independent SQL catalog",
            )
            group_ids.add((catalog_row["c_w_id"], catalog_row["c_d_id"], catalog_row["c_last"]))
        if len(selection_reads) > 1:
            require(len(group_ids) == 1, f"{label} name rows span customer groups")
            group = next(iter(group_ids))
            require(selection_keys == groups[group], f"{label} does not capture complete name group")
            branch = "last_name"
            confirmed_name += 1
        else:
            group = next(iter(group_ids))
            if len(groups[group]) > 1:
                branch = "id"
                confirmed_id += 1
            else:
                branch = "ambiguous_single"
                ambiguous_single += 1
        shapes[identity] = {
            "session_seq": client["session_seq"],
            "warehouse": decoded[0],
            "district": decoded[1],
            "customer_selection": selection_reads,
            "cdata_reread": cdata_reread,
            "branch": branch,
        }

    require(confirmed_id >= config["minimum_id_transactions"], "ID branch lacks positive proof")
    require(confirmed_name >= config["minimum_last_name_transactions"], "name branch lacks positive proof")
    require(set(by_session) == set(config["expected_session_ids"]), "ClientTxn session roster differs")
    done_by_session = {record["session_id"]: record for record in done_records}
    require(set(done_by_session) == set(config["expected_session_ids"]), "SessionDone roster differs")
    retries = 0
    retry_surplus_increases = 0
    for session_id in config["expected_session_ids"]:
        session_clients = sorted(by_session[session_id], key=lambda item: item["session_seq"])
        count = len(session_clients)
        sequences = [client["session_seq"] for client in session_clients]
        txn_ids = [client["correlation_id"]["txn_id"] for client in session_clients]
        require(sequences == list(range(1, count + 1)), f"{session_id} logical sequence has abort gap")
        require(txn_ids == sorted(set(txn_ids)), f"{session_id} marker IDs are not increasing/unique")
        previous_surplus = 0
        for txn_id, sequence in zip(txn_ids, sequences, strict=True):
            surplus = txn_id - sequence
            require(surplus >= previous_surplus, f"{session_id} retry surplus decreases")
            retry_surplus_increases += int(surplus > previous_surplus)
            previous_surplus = surplus
        done = done_by_session[session_id]
        retry_count = done["retry_attempts"]
        require(
            done["run_id"] == run_id
            and done["final_session_seq"] == count
            and done["logical_transactions_started"] == count
            and done["successful_transactions"] == count
            and done["terminal_aborted_transactions"] == 0
            and done["published_transactions"] == count
            and done["physical_attempts_started"] == count + retry_count
            and done["definitively_aborted_attempts"] == retry_count
            and done["ambiguous_attempts"] == 0,
            f"{session_id} SessionDone accounting differs",
        )
        require(
            done["rolling_correlation_digest"]
            == common.rolling_digest(run_id, session_id, session_clients),
            f"{session_id} rolling correlation digest differs",
        )
        retries += retry_count
    require(retries >= config["minimum_retries"], "retry accounting is below configured minimum")
    if retries > 0:
        require(retry_surplus_increases > 0, "retries produce no published marker-surplus increase")
    return clients, shapes, {
        "transactions": len(clients),
        "reads": total_reads,
        "confirmed_id": confirmed_id,
        "confirmed_last_name": confirmed_name,
        "ambiguous_single": ambiguous_single,
        "retries": retries,
        "retry_surplus_increases": retry_surplus_increases,
    }


def decimal(value: Any, label: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise AcceptanceError(f"{label} is not decimal: {error}") from error


def unchanged_except(before: dict[str, Any], after: dict[str, Any], changed: set[str], label: str) -> None:
    require(set(before) == set(after), f"{label} before/after columns differ")
    require(
        all(before[name] == after[name] for name in before if name not in changed),
        f"{label} changed an undeclared column",
    )


def canonical_row(codec: TpccCodec, table: str, row: dict[str, Any], label: str) -> tuple[bytes, bytes]:
    try:
        return codec.encode_row(table, row)
    except CodecError as error:
        raise AcceptanceError(f"{label} canonical encoding failed: {error.code}: {error}") from error


def binary32(value: Any) -> float:
    try:
        return struct.unpack(">f", struct.pack(">f", float(value)))[0]
    except (OverflowError, TypeError, ValueError) as error:
        raise AcceptanceError(f"value is not finite binary32: {error}") from error


def binary32_bits(value: Any) -> bytes:
    return struct.pack(">f", binary32(value))


def validate_payment_rows(
    label: str,
    shape: dict[str, Any],
    rows: list[tuple[dict[str, Any], dict[str, Any]]],
    codec: TpccCodec,
) -> tuple[int, str]:
    by_table: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    for key, envelope in rows:
        table = f"benchbase.{envelope['source']['table']}"
        require(table in EXPECTED_WRITE_TABLES, f"{label} writes forbidden table {table}")
        by_table[table].append((key, envelope))
    require(
        {table: len(values) for table, values in by_table.items()}
        == {table: 1 for table in EXPECTED_WRITE_TABLES},
        f"{label} CDC table/event counts differ",
    )
    warehouse_key, warehouse = by_table["benchbase.warehouse"][0]
    district_key, district = by_table["benchbase.district"][0]
    customer_key, customer = by_table["benchbase.customer"][0]
    history_key, history = by_table["benchbase.history"][0]
    for name, key, envelope in (
        ("warehouse", warehouse_key, warehouse),
        ("district", district_key, district),
        ("customer", customer_key, customer),
    ):
        require(
            envelope.get("op") == "u"
            and isinstance(envelope.get("before"), dict)
            and isinstance(envelope.get("after"), dict),
            f"{label} {name} update shape differs",
        )
    require(
        history.get("op") == "c"
        and history.get("before") is None
        and isinstance(history.get("after"), dict),
        f"{label} history insert shape differs",
    )
    before_w, after_w = warehouse["before"], warehouse["after"]
    before_d, after_d = district["before"], district["after"]
    before_c, after_c = customer["before"], customer["after"]
    after_h = history["after"]
    require(
        warehouse_key == {"w_id": 1}
        and before_w["w_id"] == after_w["w_id"] == 1,
        f"{label} warehouse key differs",
    )
    require(
        district_key == {"d_w_id": 1, "d_id": after_d["d_id"]}
        and before_d["d_w_id"] == after_d["d_w_id"] == 1,
        f"{label} district key differs",
    )
    require(customer_key == {
        "c_w_id": after_c["c_w_id"],
        "c_d_id": after_c["c_d_id"],
        "c_id": after_c["c_id"],
    }, f"{label} customer key differs")
    require(after_c["c_w_id"] == 1, f"{label} unexpectedly uses remote customer warehouse")
    amount_w = decimal(after_w["w_ytd"], label) - decimal(before_w["w_ytd"], label)
    amount_d = decimal(after_d["d_ytd"], label) - decimal(before_d["d_ytd"], label)
    amount_h = decimal(after_h["h_amount"], label)
    require(amount_w == amount_d == amount_h and amount_h > 0, f"{label} payment amounts differ")
    unchanged_except(before_w, after_w, {"w_ytd"}, f"{label} warehouse")
    unchanged_except(before_d, after_d, {"d_ytd"}, f"{label} district")
    changed_customer = {"c_balance", "c_ytd_payment", "c_payment_cnt"}
    credit = str(before_c["c_credit"]).strip()
    require(credit in {"GC", "BC"}, f"{label} customer credit domain differs")
    if credit == "BC":
        changed_customer.add("c_data")
        require(after_c["c_data"] != before_c["c_data"], f"{label} bad-credit c_data did not change")
    else:
        require(after_c["c_data"] == before_c["c_data"], f"{label} good-credit c_data changed")
    unchanged_except(before_c, after_c, changed_customer, f"{label} customer")
    payment_float = binary32(amount_h)
    expected_balance_float = binary32(
        binary32(decimal(before_c["c_balance"], label)) - payment_float
    )
    expected_balance = Decimal(str(expected_balance_float)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    expected_ytd = binary32(binary32(before_c["c_ytd_payment"]) + payment_float)
    require(
        after_c["c_payment_cnt"] == before_c["c_payment_cnt"] + 1
        and decimal(after_c["c_balance"], label) == expected_balance
        and binary32_bits(after_c["c_ytd_payment"]) == binary32_bits(expected_ytd),
        f"{label} customer payment arithmetic differs",
    )
    require(
        history_key == {"h_id": after_h["h_id"]}
        and isinstance(after_h["h_id"], int)
        and after_h["h_id"] > 0
        and after_h["h_c_w_id"] == after_c["c_w_id"]
        and after_h["h_c_d_id"] == after_c["c_d_id"]
        and after_h["h_c_id"] == after_c["c_id"]
        and after_h["h_w_id"] == 1
        and after_h["h_d_id"] == after_d["d_id"],
        f"{label} generated history identity/relationships differ",
    )
    require(
        (shape["warehouse"]["key_bytes"], shape["warehouse"]["value_bytes"])
        == canonical_row(codec, "benchbase.warehouse", after_w, label),
        f"{label} warehouse local read is not post-update row",
    )
    require(
        (shape["district"]["key_bytes"], shape["district"]["value_bytes"])
        == canonical_row(codec, "benchbase.district", after_d, label),
        f"{label} district local read is not post-update row",
    )
    selected_bytes = canonical_row(codec, "benchbase.customer", before_c, label)
    matching = [
        read
        for read in shape["customer_selection"]
        if (read["key_bytes"], read["value_bytes"]) == selected_bytes
    ]
    require(len(matching) == 1, f"{label} selected customer snapshot differs from CDC before image")
    require(
        shape["cdata_reread"] == (credit == "BC"),
        f"{label} bad-credit c_data reread presence differs",
    )
    return after_h["h_id"], credit


def validate_cdc_tail(
    path: Path,
    run_id: str,
    load_finished_stream_id: str,
    shapes: dict[tuple[str, int], dict[str, Any]],
    codec: TpccCodec,
    profile: dict[str, Any],
) -> dict[str, Any]:
    entries = common.raw_cdc_records(path)
    require(entries, "CDC tail is empty")
    require(
        common.parse_stream_id(entries[0][0], "first CDC tail entry")
        > common.parse_stream_id(load_finished_stream_id, "LOAD_FINISHED CDC cursor"),
        "CDC tail is not exclusive of LOAD_FINISHED cursor",
    )
    transactions: list[list[tuple[str, dict[str, Any], dict[str, Any]]]] = []
    current = None
    for entry in entries:
        envelope = entry[2]
        if envelope.get("status") == "BEGIN":
            require(current is None, "CDC tail has nested BEGIN")
            current = [entry]
        elif envelope.get("status") == "END":
            require(current is not None, "CDC tail has END without BEGIN")
            current.append(entry)
            transactions.append(current)
            current = None
        else:
            require(current is not None, "CDC tail row is outside transaction framing")
            current.append(entry)
    require(current is None, "CDC tail ends inside transaction")
    correlations: set[tuple[str, int]] = set()
    marker_progress: dict[str, int] = {}
    history_ids: set[int] = set()
    good_credit = 0
    bad_credit = 0
    end_fences = 0
    for transaction_index, transaction in enumerate(transactions):
        label = f"CDC transaction {transaction_index}"
        begin, end = transaction[0][2], transaction[-1][2]
        rows = transaction[1:-1]
        transaction_id = begin.get("id")
        require(begin.get("status") == "BEGIN" and isinstance(transaction_id, str), f"{label} BEGIN")
        require(end.get("status") == "END", f"{label} END")
        common.require_connector_transaction_id(
            end.get("id"), transaction_id, profile, f"{label} END")
        require(end.get("event_count") == len(rows), f"{label} END event_count differs")
        collection_counts: Counter[str] = Counter()
        collection_orders: Counter[str] = Counter()
        parsed_rows = []
        for total_order, (_, key, envelope) in enumerate(rows, start=1):
            source, metadata = envelope.get("source"), envelope.get("transaction")
            require(
                isinstance(source, dict)
                and source.get("name") == profile["source_id"]
                and source.get("db") == "benchbase"
                and isinstance(source.get("table"), str)
                and isinstance(metadata, dict)
                and metadata.get("total_order") == total_order,
                f"{label} row metadata differs",
            )
            common.require_connector_transaction_id(
                metadata.get("id"), transaction_id, profile, f"{label} row")
            collection = f"benchbase.{source['table']}"
            collection_counts[collection] += 1
            collection_orders[collection] += 1
            require(metadata.get("data_collection_order") == collection_orders[collection], f"{label} collection order")
            parsed_rows.append((key, envelope))
        declared = Counter()
        for collection in end.get("data_collections", []):
            declared[collection["data_collection"]] += collection["event_count"]
        require(declared == collection_counts, f"{label} END collection counts differ")
        fence_rows = [row for row in parsed_rows if row[1]["source"]["table"] == "checker_run_fence"]
        marker_rows = [row for row in parsed_rows if row[1]["source"]["table"] == "session_txn"]
        if fence_rows:
            require(len(fence_rows) == len(parsed_rows) == 1 and transaction_index == len(transactions) - 1, "END_FENCE mixed/not final")
            key, fence = fence_rows[0]
            after = fence.get("after")
            require(
                isinstance(after, dict)
                and after.get("run_id") == run_id
                and after.get("phase") == "END_FENCE"
                and key == {"run_id": run_id, "phase": "END_FENCE"},
                "CDC tail fence differs",
            )
            end_fences += 1
            continue
        require(len(marker_rows) == 1 and parsed_rows[-1] is marker_rows[0], f"{label} marker absent/not final")
        marker_key, marker = marker_rows[0]
        after, before = marker.get("after"), marker.get("before")
        require(isinstance(after, dict) and marker_key == {"session_id": after.get("session_id")}, f"{label} marker shape")
        identity = (after["session_id"], after["txn_id"])
        require(identity in shapes and identity not in correlations, f"{label} marker/client identity")
        previous = marker_progress.get(after["session_id"])
        require(
            (previous is None and before is None)
            or before == {"session_id": after["session_id"], "txn_id": previous},
            f"{label} marker before-image chain",
        )
        marker_progress[after["session_id"]] = after["txn_id"]
        correlations.add(identity)
        application_rows = [row for row in parsed_rows if row is not marker_rows[0]]
        history_id, credit = validate_payment_rows(label, shapes[identity], application_rows, codec)
        require(history_id not in history_ids, f"{label} repeats generated history h_id")
        history_ids.add(history_id)
        good_credit += int(credit == "GC")
        bad_credit += int(credit == "BC")
    require(end_fences == 1, "CDC tail must contain exactly one END_FENCE")
    require(correlations == set(shapes), "CDC marker/client correlation sets differ")
    return {
        "transactions": len(transactions),
        "markers": len(correlations),
        "end_fences": 1,
        "normalized_writes": 4 * len(correlations),
        "history_ids": history_ids,
        "good_credit_transactions": good_credit,
        "bad_credit_transactions": bad_credit,
    }


def load_history_ids(path: Path) -> set[int]:
    try:
        lines = path.read_text(encoding="utf-8", errors="strict").splitlines()
    except (OSError, UnicodeError) as error:
        raise AcceptanceError(f"cannot read history catalog: {error}") from error
    result = set()
    for index, line in enumerate(lines):
        row = common.strict_json(line, f"history catalog line {index + 1}")
        require(isinstance(row, dict) and set(row) == {"h_id"} and isinstance(row["h_id"], int), "history catalog row")
        require(row["h_id"] not in result, "history catalog repeats h_id")
        result.add(row["h_id"])
    return result


def loader_history_count(path: Path) -> int:
    document = common.load_json(path, "loader validation")
    require(document.get("kind") == "loader_validation" and document.get("benchmark") == "tpcc", "loader validation identity")
    matches = [
        row["row_count"]
        for row in document.get("observed_table_row_counts", [])
        if row.get("table") == "benchbase.history"
    ]
    require(len(matches) == 1 and isinstance(matches[0], int), "loader history count is absent")
    return matches[0]


def validate_acceptance(
    config_path: Path,
    run_id_path: Path,
    client_path: Path,
    cdc_path: Path,
    status_path: Path,
    customer_catalog_path: Path,
    history_catalog_path: Path,
    loader_validation_path: Path,
    monitor_metrics_path: Path,
    benchbase_log_path: Path,
) -> dict[str, Any]:
    config = load_config(config_path)
    try:
        run_id = run_id_path.read_text(encoding="ascii", errors="strict").strip()
    except (OSError, UnicodeError) as error:
        raise AcceptanceError(f"cannot read run ID: {error}") from error
    require(
        re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", run_id)
        is not None,
        "run ID file is malformed",
    )
    _, schemas = load_schema_validator()
    contracts = load_adapter_contracts(schemas)
    decoder = common.CanonicalDecoder(contracts["tpcc"])
    codec = TpccCodec(contracts["tpcc"])
    catalog, groups = load_customer_catalog(customer_catalog_path)
    client_entries = common.payload_records(client_path, "client stream")
    clients, shapes, coverage = decode_clients(
        config, run_id, [record for _, record in client_entries], decoder, catalog, groups
    )
    outcomes = common.benchbase_outcomes(
        benchbase_log_path, "Payment", len(clients), 0, coverage["retries"]
    )
    status_entries = common.payload_records(status_path, "status stream")
    status_records = [record for _, record in status_entries]
    load_status = common.one_status(status_records, "LOAD_FINISHED_SEEN")
    cdc = validate_cdc_tail(cdc_path, run_id, load_status["last_cdc_stream_id"], shapes, codec,
                            common.backend_profile(config))
    require(cdc["markers"] == len(clients), "marker count differs from successful clients")
    require(
        cdc["good_credit_transactions"] >= config["minimum_good_credit_transactions"],
        "good-credit path lacks positive proof",
    )
    require(
        cdc["bad_credit_transactions"] >= config["minimum_bad_credit_transactions"],
        "bad-credit path lacks positive proof",
    )
    final_history = load_history_ids(history_catalog_path)
    workload_history = cdc["history_ids"]
    bootstrap_history = final_history - workload_history
    expected_bootstrap = loader_history_count(loader_validation_path)
    require(
        workload_history <= final_history
        and len(final_history) == expected_bootstrap + len(clients)
        and len(bootstrap_history) == expected_bootstrap
        and max(bootstrap_history) < min(workload_history),
        "bootstrap/workload generated history identities collide or are not ordered",
    )
    metrics = common.load_json(monitor_metrics_path, "monitor metrics")
    final, _ = common.validate_audited_status_and_metrics(
        config,
        run_id,
        status_records,
        metrics,
        len(clients),
        coverage["reads"],
        marker_only_count=0,
        write_count=cdc["normalized_writes"],
    )
    cdc_report = {**cdc, "history_ids": len(workload_history)}
    return {
        "kind": "tpcc_payment_acceptance",
        "passed": True,
        "run_id": run_id,
        "configuration": config,
        "coverage": coverage,
        "benchbase_outcomes": outcomes,
        "cdc": cdc_report,
        "history_identity": {
            "bootstrap": len(bootstrap_history),
            "workload": len(workload_history),
            "final": len(final_history),
            "maximum_bootstrap_h_id": max(bootstrap_history),
            "minimum_workload_h_id": min(workload_history),
        },
        "sessions": {
            "expected": config["terminals"],
            "completed": final["reconciliation"]["completed_sessions"],
        },
        "reconciliation": {
            "accepted_client_transactions": len(clients),
            "joined_workload_transactions": final["reconciliation"]["joined_workload_transactions"],
            "unmatched_client_records": 0,
            "unmatched_workload_cdc_slots": 0,
        },
        "remote_customer_live_applicable": False,
        "final_outcome": "PASS",
    }


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--run-id-file", required=True, type=Path)
    parser.add_argument("--client-stream", required=True, type=Path)
    parser.add_argument("--cdc-tail", required=True, type=Path)
    parser.add_argument("--status-stream", required=True, type=Path)
    parser.add_argument("--customer-catalog", required=True, type=Path)
    parser.add_argument("--history-catalog", required=True, type=Path)
    parser.add_argument("--loader-validation", required=True, type=Path)
    parser.add_argument("--monitor-metrics", required=True, type=Path)
    parser.add_argument("--benchbase-log", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_arguments()
    try:
        report = validate_acceptance(
            args.config,
            args.run_id_file,
            args.client_stream,
            args.cdc_tail,
            args.status_stream,
            args.customer_catalog,
            args.history_catalog,
            args.loader_validation,
            args.monitor_metrics,
            args.benchbase_log,
        )
        common.write_report(args.output, report)
    except AcceptanceError as error:
        print(f"TPC-C Payment acceptance failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
