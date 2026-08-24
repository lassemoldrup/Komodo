#!/usr/bin/env python3
"""Validate a focused live TPC-C NewOrder black-box acceptance run."""

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
from verify_protocol import load_adapter_contracts, load_schema_validator  # noqa: E402


AcceptanceError = common.AcceptanceError
require = common.require
EXPECTED_WEIGHTS = [100, 0, 0, 0, 0]
EXPECTED_READ_TABLES = {
    "benchbase.customer",
    "benchbase.warehouse",
    "benchbase.item",
}
EXPECTED_WRITE_TABLES = {
    "benchbase.district",
    "benchbase.stock",
    "benchbase.oorder",
    "benchbase.new_order",
    "benchbase.order_line",
}
EXPECTED_TABLES = common.EXPECTED_TABLES


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
        "minimum_invalid_item_aborts",
        "minimum_repeated_key_transactions",
        "minimum_remote_item_transactions",
        "minimum_retries",
        "remote_item_live_applicable",
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
    require(config["kind"] == "tpcc_new_order_acceptance_config", "run configuration kind")
    require(config["benchmark"] == "tpcc", "run benchmark")
    common.expected_source_id(config)
    require(config["declared_isolation"] == "REPEATABLE_READ", "run isolation is not RR")
    require(config["warehouses"] >= 1, "acceptance run needs a positive warehouse count")
    require(config["weights"] == EXPECTED_WEIGHTS, "acceptance weights are not NewOrder-only")
    require(
        config["remote_item_live_applicable"] is (config["warehouses"] >= 2),
        "remote-item applicability differs from the warehouse count",
    )
    require(
        isinstance(config["minimum_remote_item_transactions"], int)
        and config["minimum_remote_item_transactions"] >= 0
        and (
            not config["remote_item_live_applicable"]
            or config["minimum_remote_item_transactions"] >= 1
        ),
        "remote-item minimum differs from applicability",
    )
    positive_fields = (
        "terminals",
        "duration_seconds",
        "loader_threads",
        "loader_batch_size",
        "minimum_transactions",
        "minimum_invalid_item_aborts",
        "minimum_repeated_key_transactions",
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
    )
    for field in positive_fields:
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
    require(
        isinstance(config["redis_host"], str)
        and config["redis_host"]
        and isinstance(config["db_host"], str)
        and config["db_host"],
        "database/Redis host control is empty",
    )
    expected_sessions = [f"tpcc/worker/{index}" for index in range(1, config["terminals"] + 1)]
    require(config["expected_session_ids"] == expected_sessions, "configured session roster differs")
    return config


def decode_clients(
    config: dict[str, Any],
    run_id: str,
    records: list[dict[str, Any]],
    decoder: common.CanonicalDecoder,
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
    require(len(clients) >= config["minimum_transactions"], "too few successful NewOrder transactions")
    require(len(done_records) == config["terminals"], "SessionDone count differs")

    shapes: dict[tuple[str, int], dict[str, Any]] = {}
    home_by_session: dict[str, int] = {}
    by_session: dict[str, list[dict[str, Any]]] = defaultdict(list)
    logical_ids: set[str] = set()
    attempt_ids: set[str] = set()
    repeated_key_transactions = 0
    total_reads = 0

    for client_index, client in enumerate(clients):
        label = f"ClientTxn[{client_index}]"
        require(client["run_id"] == run_id and client["txn_type"] == "NewOrder", f"{label} identity")
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

        decoded: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
        for read_index, read in enumerate(client["reads"]):
            require(
                read["event_seq"] == read_index
                and read["source"] == "snapshot"
                and read["value_state"] == "present",
                f"{label} read {read_index} is not a dense snapshot Present read",
            )
            key_bytes = common.decode_base64(read["key_bytes_base64"], f"{label} key {read_index}")
            value_bytes = common.decode_base64(read["value_bytes_base64"], f"{label} value {read_index}")
            table, key = decoder.decode_key(key_bytes, f"{label} key {read_index}")
            value = decoder.decode_value(table, value_bytes, f"{label} value {read_index}")
            decoded.append((table, key, {**key, **value}))
        total_reads += len(decoded)
        tables = [table for table, _, _ in decoded]
        require(
            len(tables) >= 7
            and tables[0] == "benchbase.customer"
            and tables[1] == "benchbase.warehouse"
            and all(table == "benchbase.item" for table in tables[2:]),
            f"{label} read tables/order differ from customer, warehouse, item-only capture",
        )
        require(set(tables) == EXPECTED_READ_TABLES, f"{label} includes district/stock or lacks a read table")
        customer = decoded[0][2]
        warehouse = decoded[1][2]
        items = [row for _, _, row in decoded[2:]]
        require(5 <= len(items) <= 15, f"{label} item read count is outside [5,15]")
        home_warehouse = warehouse["w_id"]
        require(
            1 <= home_warehouse <= config["warehouses"]
            and customer["c_w_id"] == home_warehouse,
            f"{label} customer/warehouse key differs",
        )
        session_home = home_by_session.setdefault(client["session_id"], home_warehouse)
        require(session_home == home_warehouse, f"{label} session home warehouse differs")
        item_ids = [row["i_id"] for row in items]
        if len(set(item_ids)) < len(item_ids):
            repeated_key_transactions += 1
        shapes[identity] = {
            "session_seq": client["session_seq"],
            "customer": customer,
            "warehouse": warehouse,
            "item_ids": item_ids,
            "items": items,
        }

    require(
        repeated_key_transactions >= config["minimum_repeated_key_transactions"],
        "repeated item/stock-key execution lacks positive proof",
    )
    require(set(by_session) == set(config["expected_session_ids"]), "ClientTxn session roster differs")
    done_by_session = {record["session_id"]: record for record in done_records}
    require(set(done_by_session) == set(config["expected_session_ids"]), "SessionDone roster differs")

    terminal_aborts = 0
    retries = 0
    retry_surplus_increases = 0
    for session_id in config["expected_session_ids"]:
        session_clients = sorted(by_session[session_id], key=lambda item: item["session_seq"])
        session_sequences = [client["session_seq"] for client in session_clients]
        marker_ids = [client["correlation_id"]["txn_id"] for client in session_clients]
        require(
            session_sequences == sorted(set(session_sequences))
            and marker_ids == sorted(set(marker_ids)),
            f"{session_id} published sequences are not increasing/unique",
        )
        surplus = [txn_id - sequence for txn_id, sequence in zip(marker_ids, session_sequences, strict=True)]
        require(all(value >= 0 for value in surplus), f"{session_id} marker precedes logical sequence")
        require(surplus == sorted(surplus), f"{session_id} retry surplus decreases")
        previous_surplus = 0
        for value in surplus:
            if value > previous_surplus:
                retry_surplus_increases += 1
            previous_surplus = value
        done = done_by_session[session_id]
        logical = done["logical_transactions_started"]
        successful = done["successful_transactions"]
        aborted = done["terminal_aborted_transactions"]
        physical = done["physical_attempts_started"]
        definitive = done["definitively_aborted_attempts"]
        retry_count = done["retry_attempts"]
        require(done["run_id"] == run_id, f"{session_id} SessionDone run ID")
        require(
            done["final_session_seq"] == logical
            and logical == successful + aborted
            and successful == done["published_transactions"] == len(session_clients)
            and physical == successful + definitive
            and retry_count == physical - logical
            and definitive == aborted + retry_count
            and done["ambiguous_attempts"] == 0,
            f"{session_id} SessionDone attempt/logical algebra differs",
        )
        require(
            len(set(range(1, logical + 1)) - set(session_sequences)) == aborted,
            f"{session_id} aborted logical gaps differ from SessionDone",
        )
        require(not marker_ids or marker_ids[-1] <= physical, f"{session_id} marker exceeds attempt count")
        require(
            done["rolling_correlation_digest"]
            == common.rolling_digest(run_id, session_id, session_clients),
            f"{session_id} rolling correlation digest differs",
        )
        terminal_aborts += aborted
        retries += retry_count

    require(
        terminal_aborts >= config["minimum_invalid_item_aborts"],
        "expected invalid-item abort accounting lacks positive proof",
    )
    require(retries >= config["minimum_retries"], "retry accounting is below configured minimum")
    if retries > 0:
        require(
            retry_surplus_increases > 0,
            "retries produced no published increase in new-attempt marker identity",
        )

    return clients, shapes, {
        "transactions": len(clients),
        "reads": total_reads,
        "repeated_key_transactions": repeated_key_transactions,
        "terminal_invalid_item_aborts": terminal_aborts,
        "retries": retries,
        "retry_surplus_increases": retry_surplus_increases,
    }


def benchbase_outcomes(
    path: Path, successful: int, terminal_aborts: int, retries: int
) -> dict[str, int]:
    result = common.benchbase_outcomes(
        path, "NewOrder", successful, terminal_aborts, retries
    )
    result["expected_user_aborts"] = result.pop("terminal_aborts")
    return result


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


def item_amount(item: dict[str, Any], quantity: int, label: str) -> Decimal:
    price = item["i_price"]
    require(
        isinstance(price, dict)
        and isinstance(price.get("unscaled"), int)
        and isinstance(price.get("scale"), int),
        f"{label} captured item price is not fixed decimal",
    )
    exact = Decimal(price["unscaled"]).scaleb(-price["scale"])
    numeric = struct.unpack(">f", struct.pack(">f", float(exact)))[0]
    multiplied = struct.unpack(">f", struct.pack(">f", numeric * quantity))[0]
    return Decimal(str(multiplied)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def validate_workload_rows(
    label: str,
    shape: dict[str, Any],
    rows: list[tuple[dict[str, Any], dict[str, Any]]],
    warehouse_count: int,
) -> tuple[int, bool, bool]:
    by_table: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    for key, envelope in rows:
        source = envelope["source"]
        table = f"benchbase.{source['table']}"
        require(table in EXPECTED_WRITE_TABLES, f"{label} writes forbidden table {table}")
        by_table[table].append((key, envelope))
    item_ids = shape["item_ids"]
    item_count = len(item_ids)
    expected_counts = {
        "benchbase.district": 1,
        "benchbase.stock": item_count,
        "benchbase.oorder": 1,
        "benchbase.new_order": 1,
        "benchbase.order_line": item_count,
    }
    require({table: len(values) for table, values in by_table.items()} == expected_counts, f"{label} CDC table/event counts differ")
    home_warehouse = shape["warehouse"]["w_id"]

    district_key, district = by_table["benchbase.district"][0]
    before_district = district.get("before")
    after_district = district.get("after")
    require(
        district.get("op") == "u"
        and isinstance(before_district, dict)
        and isinstance(after_district, dict)
        and district_key == {"d_w_id": home_warehouse, "d_id": after_district["d_id"]}
        and after_district["d_w_id"] == home_warehouse
        and after_district["d_id"] == shape["customer"]["c_d_id"]
        and after_district["d_next_o_id"] == before_district["d_next_o_id"] + 1,
        f"{label} district update differs",
    )
    unchanged_except(before_district, after_district, {"d_next_o_id"}, f"{label} district")
    order_id = before_district["d_next_o_id"]

    order_key, order = by_table["benchbase.oorder"][0]
    order_after = order.get("after")
    require(
        order.get("op") == "c"
        and order.get("before") is None
        and isinstance(order_after, dict)
        and order_key
        == {"o_w_id": home_warehouse, "o_d_id": after_district["d_id"], "o_id": order_id}
        and order_after["o_w_id"] == order_key["o_w_id"]
        and order_after["o_d_id"] == order_key["o_d_id"]
        and order_after["o_id"] == order_key["o_id"]
        and order_after["o_c_id"] == shape["customer"]["c_id"]
        and order_after["o_ol_cnt"] == item_count
        and order_after["o_carrier_id"] is None,
        f"{label} oorder insert differs",
    )

    new_order_key, new_order = by_table["benchbase.new_order"][0]
    require(
        new_order.get("op") == "c"
        and new_order.get("before") is None
        and new_order.get("after")
        == {"no_w_id": home_warehouse, "no_d_id": after_district["d_id"], "no_o_id": order_id}
        and new_order_key == new_order["after"],
        f"{label} new_order insert differs",
    )

    lines_by_number: dict[int, dict[str, Any]] = {}
    for line_key, line in by_table["benchbase.order_line"]:
        after = line.get("after")
        require(
            line.get("op") == "c"
            and line.get("before") is None
            and isinstance(after, dict)
            and line_key
            == {
                "ol_w_id": home_warehouse,
                "ol_d_id": after_district["d_id"],
                "ol_o_id": order_id,
                "ol_number": after["ol_number"],
            }
            and after["ol_w_id"] == line_key["ol_w_id"]
            and after["ol_d_id"] == line_key["ol_d_id"]
            and after["ol_o_id"] == line_key["ol_o_id"]
            and 1 <= after["ol_supply_w_id"] <= warehouse_count
            and after["ol_delivery_d"] is None,
            f"{label} order_line insert differs",
        )
        require(after["ol_number"] not in lines_by_number, f"{label} repeats order-line number")
        lines_by_number[after["ol_number"]] = after
    require(sorted(lines_by_number) == list(range(1, item_count + 1)), f"{label} line sequence differs")
    require(
        [lines_by_number[number]["ol_i_id"] for number in range(1, item_count + 1)]
        == item_ids,
        f"{label} item reads/order-line item IDs differ",
    )
    supply_pairs = [
        (lines_by_number[number]["ol_supply_w_id"], lines_by_number[number]["ol_i_id"])
        for number in range(1, item_count + 1)
    ]
    remote_transaction = any(supply != home_warehouse for supply, _ in supply_pairs)
    # BenchBase records the generator's remote intent: a sampled remote item
    # degenerates to the home warehouse at W=1 yet still clears o_all_local,
    # so an all-local supply set admits either flag value. A genuinely remote
    # supply warehouse must never claim an all-local order.
    require(
        order_after["o_all_local"] in {0, 1}
        and not (remote_transaction and order_after["o_all_local"] == 1),
        f"{label} o_all_local contradicts the supply warehouses",
    )
    for number, item in enumerate(shape["items"], start=1):
        line = lines_by_number[number]
        require(
            decimal(line["ol_amount"], f"{label} order-line amount")
            == item_amount(item, line["ol_quantity"], label),
            f"{label} order-line amount differs from captured item price and quantity",
        )

    stock_by_item: dict[tuple[int, int], list[tuple[dict[str, Any], dict[str, Any]]]] = (
        defaultdict(list)
    )
    for stock_key, stock in by_table["benchbase.stock"]:
        after = stock.get("after")
        require(
            stock.get("op") == "u"
            and isinstance(stock.get("before"), dict)
            and isinstance(after, dict)
            and stock_key == {"s_w_id": after["s_w_id"], "s_i_id": after["s_i_id"]}
            and 1 <= after["s_w_id"] <= warehouse_count,
            f"{label} stock update key/shape differs",
        )
        stock_by_item[(after["s_w_id"], after["s_i_id"])].append((stock_key, stock))
    require(
        Counter(supply_pairs)
        == Counter({pair: len(events) for pair, events in stock_by_item.items()}),
        f"{label} stock update multiset differs from supply warehouses/items",
    )

    quantities_by_item: dict[tuple[int, int], list[int]] = defaultdict(list)
    for number in range(1, item_count + 1):
        line = lines_by_number[number]
        quantities_by_item[(line["ol_supply_w_id"], line["ol_i_id"])].append(
            line["ol_quantity"]
        )
    for item_id, events in stock_by_item.items():
        previous_after: dict[str, Any] | None = None
        initial_quantity = events[0][1]["before"]["s_quantity"]
        for event_index, ((_, stock), quantity) in enumerate(
            zip(events, quantities_by_item[item_id], strict=True)
        ):
            before = stock["before"]
            after = stock["after"]
            if previous_after is not None:
                require(before == previous_after, f"{label} repeated stock before-image chain differs")
            expected_quantity = (
                initial_quantity - quantity
                if initial_quantity - quantity >= 10
                else initial_quantity - quantity + 91
            )
            expected_remote = before["s_remote_cnt"] + (
                1 if item_id[0] != home_warehouse else 0
            )
            require(
                after["s_quantity"] == expected_quantity
                and decimal(after["s_ytd"], f"{label} stock ytd")
                == decimal(before["s_ytd"], f"{label} stock ytd") + quantity
                and after["s_order_cnt"] == before["s_order_cnt"] + 1
                and after["s_remote_cnt"] == expected_remote,
                f"{label} stock arithmetic differs at item {item_id} event {event_index}",
            )
            unchanged_except(
                before,
                after,
                {"s_quantity", "s_ytd", "s_order_cnt", "s_remote_cnt"},
                f"{label} stock",
            )
            previous_after = after

    normalized_writes = 3 + item_count + len(set(supply_pairs))
    return normalized_writes, len(set(item_ids)) < item_count, remote_transaction


def validate_cdc_tail(
    path: Path,
    run_id: str,
    load_finished_stream_id: str,
    shapes: dict[tuple[str, int], dict[str, Any]],
    profile: dict[str, Any],
    warehouse_count: int,
) -> dict[str, int]:
    entries = common.raw_cdc_records(path)
    require(entries, "CDC tail is empty")
    require(
        common.parse_stream_id(entries[0][0], "first CDC tail entry")
        > common.parse_stream_id(load_finished_stream_id, "LOAD_FINISHED CDC cursor"),
        "CDC tail is not exclusive of LOAD_FINISHED cursor",
    )
    transactions: list[list[tuple[str, dict[str, Any], dict[str, Any]]]] = []
    current: list[tuple[str, dict[str, Any], dict[str, Any]]] | None = None
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
    require(current is None, "CDC tail ends inside a transaction")

    correlations: set[tuple[str, int]] = set()
    end_fences = 0
    normalized_writes = 0
    repeated_transactions = 0
    remote_item_transactions = 0
    for transaction_index, transaction in enumerate(transactions):
        label = f"CDC transaction {transaction_index}"
        begin = transaction[0][2]
        end = transaction[-1][2]
        rows = transaction[1:-1]
        transaction_id = begin.get("id")
        require(begin.get("status") == "BEGIN" and isinstance(transaction_id, str), f"{label} BEGIN")
        require(end.get("status") == "END", f"{label} END")
        common.require_connector_transaction_id(
            end.get("id"), transaction_id, profile, f"{label} END")
        require(end.get("event_count") == len(rows), f"{label} END event_count differs")
        collection_counts: Counter[str] = Counter()
        collection_orders: Counter[str] = Counter()
        parsed_rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for total_order, (_, key, envelope) in enumerate(rows, start=1):
            source = envelope.get("source")
            metadata = envelope.get("transaction")
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
            require(
                metadata.get("data_collection_order") == collection_orders[collection],
                f"{label} data-collection order differs",
            )
            parsed_rows.append((key, envelope))
        declared = Counter()
        for collection in end.get("data_collections", []):
            require(
                isinstance(collection, dict)
                and set(collection) == {"data_collection", "event_count"},
                f"{label} END collection shape differs",
            )
            declared[collection["data_collection"]] += collection["event_count"]
        require(declared == collection_counts, f"{label} END collection counts differ")

        fence_rows = [row for row in parsed_rows if row[1]["source"]["table"] == "checker_run_fence"]
        marker_rows = [row for row in parsed_rows if row[1]["source"]["table"] == "session_txn"]
        if fence_rows:
            require(
                len(fence_rows) == len(parsed_rows) == 1
                and transaction_index == len(transactions) - 1,
                "END_FENCE is mixed or not final",
            )
            key, fence = fence_rows[0]
            after = fence.get("after")
            require(
                isinstance(after, dict)
                and after.get("run_id") == run_id
                and after.get("phase") == "END_FENCE"
                and key == {"run_id": run_id, "phase": "END_FENCE"},
                "CDC tail fence differs from run END_FENCE",
            )
            end_fences += 1
            continue
        require(len(marker_rows) == 1 and parsed_rows[-1] is marker_rows[0], f"{label} marker is absent/not final")
        marker_key, marker = marker_rows[0]
        marker_after = marker.get("after")
        marker_before = marker.get("before")
        require(
            isinstance(marker_after, dict)
            and marker.get("op") == "c"
            and marker_before is None
            and marker_key
            == {
                "session_id": marker_after.get("session_id"),
                "txn_id": marker_after.get("txn_id"),
            },
            f"{label} marker shape differs",
        )
        identity = (marker_after["session_id"], marker_after["txn_id"])
        require(identity in shapes and identity not in correlations, f"{label} marker/client identity differs")
        correlations.add(identity)
        application_rows = [row for row in parsed_rows if row is not marker_rows[0]]
        write_count, repeated, remote = validate_workload_rows(
            label, shapes[identity], application_rows, warehouse_count
        )
        normalized_writes += write_count
        repeated_transactions += int(repeated)
        remote_item_transactions += int(remote)

    require(end_fences == 1, "CDC tail must contain exactly one END_FENCE")
    require(correlations == set(shapes), "CDC marker/client correlation sets differ")
    return {
        "transactions": len(transactions),
        "markers": len(correlations),
        "end_fences": 1,
        "normalized_writes": normalized_writes,
        "repeated_key_transactions": repeated_transactions,
        "remote_item_transactions": remote_item_transactions,
    }


def validate_status_and_metrics(
    config: dict[str, Any],
    run_id: str,
    status_records: list[dict[str, Any]],
    metrics: dict[str, Any],
    client_count: int,
    read_count: int,
    write_count: int,
) -> tuple[dict[str, Any], str]:
    return common.validate_audited_status_and_metrics(
        config,
        run_id,
        status_records,
        metrics,
        client_count,
        read_count,
        marker_only_count=0,
        write_count=write_count,
    )


def validate_acceptance(
    config_path: Path,
    run_id_path: Path,
    client_path: Path,
    cdc_path: Path,
    status_path: Path,
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
    client_entries = common.payload_records(client_path, "client stream")
    clients, shapes, coverage = decode_clients(
        config, run_id, [record for _, record in client_entries], decoder
    )
    outcomes = benchbase_outcomes(
        benchbase_log_path,
        len(clients),
        coverage["terminal_invalid_item_aborts"],
        coverage["retries"],
    )
    status_entries = common.payload_records(status_path, "status stream")
    metrics = common.load_json(monitor_metrics_path, "monitor metrics")
    status_records = [record for _, record in status_entries]
    load_status = common.one_status(status_records, "LOAD_FINISHED_SEEN")
    cdc = validate_cdc_tail(
        cdc_path, run_id, load_status["last_cdc_stream_id"], shapes,
        common.backend_profile(config), config["warehouses"],
    )
    require(cdc["markers"] == len(clients), "marker count differs from successful clients")
    require(
        cdc["repeated_key_transactions"] >= config["minimum_repeated_key_transactions"],
        "raw repeated stock-key execution lacks positive proof",
    )
    if config["remote_item_live_applicable"]:
        require(
            cdc["remote_item_transactions"] >= config["minimum_remote_item_transactions"],
            "remote-item execution lacks positive live proof",
        )
    else:
        require(
            cdc["remote_item_transactions"] == 0,
            "remote-item transactions are impossible at one warehouse",
        )
    final, _ = validate_status_and_metrics(
        config,
        run_id,
        status_records,
        metrics,
        len(clients),
        coverage["reads"],
        cdc["normalized_writes"],
    )
    return {
        "kind": "tpcc_new_order_acceptance",
        "passed": True,
        "run_id": run_id,
        "configuration": config,
        "coverage": coverage,
        "benchbase_outcomes": outcomes,
        "cdc": cdc,
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
        "remote_item_live_applicable": config["remote_item_live_applicable"],
        "final_outcome": "PASS",
    }


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--run-id-file", required=True, type=Path)
    parser.add_argument("--client-stream", required=True, type=Path)
    parser.add_argument("--cdc-tail", required=True, type=Path)
    parser.add_argument("--status-stream", required=True, type=Path)
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
            args.monitor_metrics,
            args.benchbase_log,
        )
        common.write_report(args.output, report)
    except AcceptanceError as error:
        print(f"TPC-C NewOrder acceptance failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
