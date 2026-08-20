#!/usr/bin/env python3
"""Validate a focused live TPC-C OrderStatus black-box acceptance run."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import struct
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PROTOCOL_ROOT = ROOT / "protocol"
sys.path.insert(0, str(PROTOCOL_ROOT / "tools"))

from tpcc_client_fixture import read_digest, rolling_digest  # noqa: E402
from tpcc_codec import reject_duplicate_keys  # noqa: E402
from verify_protocol import (  # noqa: E402
    VerificationError,
    load_adapter_contracts,
    load_schema_validator,
    verify_record_schema,
)


STREAM_ID = re.compile(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)\Z")
DECIMAL = re.compile(r"DECIMAL\(([1-9][0-9]*),([0-9]+)\)\Z")
TEXT = re.compile(r"(VARCHAR|CHAR)\(([1-9][0-9]*)\)\Z")
EXPECTED_WEIGHTS = [0, 0, 100, 0, 0]
EXPECTED_TABLES = [
    "benchbase.warehouse",
    "benchbase.item",
    "benchbase.stock",
    "benchbase.district",
    "benchbase.customer",
    "benchbase.history",
    "benchbase.oorder",
    "benchbase.new_order",
    "benchbase.order_line",
]


class AcceptanceError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AcceptanceError(message)


def strict_json(text: str, label: str) -> Any:
    try:
        return json.loads(text, object_pairs_hook=reject_duplicate_keys)
    except (json.JSONDecodeError, ValueError) as error:
        raise AcceptanceError(f"{label} is malformed JSON: {error}") from error


def load_json(path: Path, label: str) -> Any:
    try:
        return strict_json(path.read_text(encoding="utf-8", errors="strict"), label)
    except (OSError, UnicodeError) as error:
        raise AcceptanceError(f"cannot read {label}: {error}") from error


def parse_stream_id(value: Any, label: str) -> tuple[int, int]:
    require(isinstance(value, str), f"{label} stream ID is not a string")
    match = STREAM_ID.fullmatch(value)
    require(match is not None, f"{label} stream ID is malformed")
    return int(match.group(1)), int(match.group(2))


def parse_xrange(path: Path, label: str) -> list[tuple[str, list[Any]]]:
    document = load_json(path, label)
    require(isinstance(document, list), f"{label} root is not an array")
    parsed: list[tuple[str, list[Any]]] = []
    previous: tuple[int, int] | None = None
    for index, entry in enumerate(document):
        item_label = f"{label}[{index}]"
        require(
            isinstance(entry, list)
            and len(entry) == 2
            and isinstance(entry[1], list),
            f"{item_label} has wrong XRANGE shape",
        )
        numeric = parse_stream_id(entry[0], item_label)
        require(previous is None or numeric > previous, f"{label} IDs are not increasing")
        previous = numeric
        parsed.append((entry[0], entry[1]))
    return parsed


def payload_records(path: Path, label: str) -> list[tuple[str, dict[str, Any]]]:
    records: list[tuple[str, dict[str, Any]]] = []
    for index, (entry_id, fields) in enumerate(parse_xrange(path, label)):
        require(
            fields[:1] == ["payload"] and len(fields) == 2 and isinstance(fields[1], str),
            f"{label}[{index}] must contain exactly one payload field",
        )
        record = strict_json(fields[1], f"{label}[{index}].payload")
        require(isinstance(record, dict), f"{label}[{index}] payload is not an object")
        records.append((entry_id, record))
    return records


def decode_base64(value: Any, label: str) -> bytes:
    require(isinstance(value, str), f"{label} is not Base64 text")
    try:
        decoded = base64.b64decode(value, validate=True)
    except ValueError as error:
        raise AcceptanceError(f"{label} is malformed Base64: {error}") from error
    require(base64.b64encode(decoded).decode("ascii") == value, f"{label} is noncanonical")
    return decoded


class ByteCursor:
    def __init__(self, data: bytes, label: str):
        self.data = data
        self.label = label
        self.offset = 0

    def take(self, count: int) -> bytes:
        require(count >= 0 and self.offset + count <= len(self.data), f"{self.label} is truncated")
        result = self.data[self.offset : self.offset + count]
        self.offset += count
        return result

    def u8(self) -> int:
        return self.take(1)[0]

    def u32(self) -> int:
        return struct.unpack(">I", self.take(4))[0]

    def lp_text(self) -> str:
        raw = self.take(self.u32())
        try:
            return raw.decode("utf-8", "strict")
        except UnicodeDecodeError as error:
            raise AcceptanceError(f"{self.label} contains malformed UTF-8: {error}") from error

    def finish(self) -> None:
        require(self.offset == len(self.data), f"{self.label} has trailing bytes")


class CanonicalDecoder:
    def __init__(self, contract: dict[str, Any]):
        self.tables = {table["name"]: table for table in contract["tables"]}

    @staticmethod
    def decode_nonnull(cursor: ByteCursor, column: dict[str, Any]) -> Any:
        sql_type = column["sql_type"]
        if sql_type == "INT":
            return struct.unpack(">i", cursor.take(4))[0]
        if sql_type in {"BIGINT", "TIMESTAMP"}:
            return struct.unpack(">q", cursor.take(8))[0]
        if sql_type == "FLOAT":
            return {"raw_bits_hex": cursor.take(4).hex()}
        decimal = DECIMAL.fullmatch(sql_type)
        if decimal is not None:
            return {
                "unscaled": struct.unpack(">q", cursor.take(8))[0],
                "scale": int(decimal.group(2)),
            }
        text = TEXT.fullmatch(sql_type)
        require(text is not None, f"unsupported canonical SQL type {sql_type}")
        value = cursor.lp_text()
        limit = int(text.group(2))
        require(len(value) <= limit, f"canonical {column['name']} exceeds declared length")
        if text.group(1) == "CHAR":
            require(len(value) == limit, f"canonical {column['name']} is not CHAR-padded")
        return value

    def decode_column(self, cursor: ByteCursor, column: dict[str, Any]) -> Any:
        if column["nullable"]:
            tag = cursor.u8()
            require(tag in {0, 1}, f"canonical {column['name']} has invalid nullable tag")
            if tag == 0:
                return None
        return self.decode_nonnull(cursor, column)

    def decode_key(self, encoded: bytes, label: str) -> tuple[str, dict[str, Any]]:
        cursor = ByteCursor(encoded, label)
        schema = cursor.lp_text()
        table_name = cursor.lp_text()
        qualified = f"{schema}.{table_name}"
        table = self.tables.get(qualified)
        require(table is not None, f"{label} names unknown table {qualified}")
        columns = {column["name"]: column for column in table["columns"]}
        key = {
            name: self.decode_nonnull(cursor, columns[name])
            for name in table["key_columns"]
        }
        cursor.finish()
        return qualified, key

    def decode_value(self, table_name: str, encoded: bytes, label: str) -> dict[str, Any]:
        table = self.tables[table_name]
        columns = {column["name"]: column for column in table["columns"]}
        cursor = ByteCursor(encoded, label)
        value = {
            name: self.decode_column(cursor, columns[name])
            for name in table["value_columns"]
        }
        cursor.finish()
        return value


def load_config(path: Path) -> dict[str, Any]:
    config = load_json(path, "run configuration")
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
        "minimum_multirow_transactions",
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
    require(config["kind"] == "tpcc_order_status_acceptance_config", "run configuration kind")
    require(config["benchmark"] == "tpcc" and config["backend"] == "mariadb", "run benchmark/backend")
    require(config["declared_isolation"] == "REPEATABLE_READ", "run isolation is not RR")
    require(config["warehouses"] == 1, "acceptance run must use one warehouse")
    require(config["weights"] == EXPECTED_WEIGHTS, "acceptance weights are not OrderStatus-only")
    for field in (
        "terminals",
        "duration_seconds",
        "loader_threads",
        "loader_batch_size",
        "minimum_transactions",
        "minimum_id_transactions",
        "minimum_last_name_transactions",
        "minimum_multirow_transactions",
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
        isinstance(config["warmup_seconds"], int) and config["warmup_seconds"] >= 0,
        "warmup_seconds is negative",
    )
    require(
        isinstance(config["random_seed"], int)
        and -(1 << 63) <= config["random_seed"] < (1 << 63),
        "random_seed is not a signed 64-bit integer",
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
    for index, line in enumerate(lines):
        row = strict_json(line, f"customer catalog line {index + 1}")
        require(
            isinstance(row, dict)
            and set(row)
            == {"c_w_id", "c_d_id", "c_id", "c_last", "last_name_group_count"},
            "customer catalog row fields differ",
        )
        key = (row["c_w_id"], row["c_d_id"], row["c_id"])
        group = (row["c_w_id"], row["c_d_id"], row["c_last"])
        require(
            all(isinstance(value, int) and value > 0 for value in key)
            and isinstance(row["c_last"], str)
            and row["c_last"]
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


def validate_record_schemas(records: list[dict[str, Any]]) -> None:
    validator, _ = load_schema_validator()
    for index, record in enumerate(records):
        try:
            verify_record_schema(validator, record, f"acceptance record {index}")
        except VerificationError as error:
            raise AcceptanceError(str(error)) from error


def validate_clients(
    config: dict[str, Any],
    run_id: str,
    records: list[dict[str, Any]],
    decoder: CanonicalDecoder,
    catalog: dict[tuple[int, int, int], dict[str, Any]],
    groups: dict[tuple[int, int, str], set[tuple[int, int, int]]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    validate_record_schemas(records)
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
        and manifest["backend"] == "mariadb"
        and manifest["declared_isolation"] == "REPEATABLE_READ",
        "RunManifest identity differs",
    )
    require(manifest["expected_session_ids"] == config["expected_session_ids"], "RunManifest session roster")
    require(manifest["application_table_allowlist"] == EXPECTED_TABLES, "RunManifest table roster")
    require(bootstraps[0]["run_id"] == run_id, "BootstrapManifest run ID differs")
    require(len(clients) >= config["minimum_transactions"], "too few successful OrderStatus transactions")
    require(len(done_records) == config["terminals"], "SessionDone count differs")

    by_session: dict[str, list[dict[str, Any]]] = defaultdict(list)
    confirmed_id = 0
    confirmed_last_name = 0
    ambiguous_single = 0
    multirow = 0
    carrier_null = 0
    carrier_present = 0
    delivery_null = 0
    delivery_present = 0
    total_reads = 0
    correlations: set[tuple[str, int]] = set()
    logical_ids: set[str] = set()
    attempt_ids: set[str] = set()

    for client_index, client in enumerate(clients):
        label = f"ClientTxn[{client_index}]"
        require(client["run_id"] == run_id and client["txn_type"] == "OrderStatus", f"{label} identity")
        require(client["correlation_kind"] == "session_txn", f"{label} correlation kind")
        require(client["read_count"] == len(client["reads"]), f"{label} read count")
        require(read_digest(client["reads"]) == client["read_digest"], f"{label} read digest")
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
        correlation_key = (client["session_id"], correlation["txn_id"])
        require(correlation_key not in correlations, f"{label} duplicate marker correlation")
        correlations.add(correlation_key)
        by_session[client["session_id"]].append(client)

        decoded_reads: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
        for read_index, read in enumerate(client["reads"]):
            require(
                read["event_seq"] == read_index
                and read["source"] == "snapshot"
                and read["value_state"] == "present",
                f"{label} read {read_index} is not a dense snapshot Present read",
            )
            key_bytes = decode_base64(read["key_bytes_base64"], f"{label} key {read_index}")
            value_bytes = decode_base64(read["value_bytes_base64"], f"{label} value {read_index}")
            table, key = decoder.decode_key(key_bytes, f"{label} key {read_index}")
            value = decoder.decode_value(table, value_bytes, f"{label} value {read_index}")
            decoded_reads.append((table, key, {**key, **value}))
        total_reads += len(decoded_reads)

        customer_rows: list[dict[str, Any]] = []
        position = 0
        while position < len(decoded_reads) and decoded_reads[position][0] == "benchbase.customer":
            customer_rows.append(decoded_reads[position][2])
            position += 1
        require(customer_rows, f"{label} has no customer row")
        require(
            position < len(decoded_reads) and decoded_reads[position][0] == "benchbase.oorder",
            f"{label} lacks exactly positioned newest-order row",
        )
        order = decoded_reads[position][2]
        position += 1
        lines = decoded_reads[position:]
        require(lines and all(table == "benchbase.order_line" for table, _, _ in lines), f"{label} order-line shape")

        customer_keys = {
            (row["c_w_id"], row["c_d_id"], row["c_id"])
            for row in customer_rows
        }
        require(len(customer_keys) == len(customer_rows), f"{label} repeats customer row")
        catalog_rows = []
        for key in customer_keys:
            require(key in catalog, f"{label} customer is absent from independent SQL catalog")
            catalog_row = catalog[key]
            row = next(item for item in customer_rows if (item["c_w_id"], item["c_d_id"], item["c_id"]) == key)
            require(row["c_last"] == catalog_row["c_last"], f"{label} customer last name differs from SQL")
            catalog_rows.append(catalog_row)
        group_identities = {
            (row["c_w_id"], row["c_d_id"], row["c_last"])
            for row in catalog_rows
        }
        if len(customer_rows) > 1:
            require(len(group_identities) == 1, f"{label} by-name rows span last-name groups")
            group = next(iter(group_identities))
            require(customer_keys == groups[group], f"{label} does not capture the complete last-name group")
            confirmed_last_name += 1
        else:
            group = next(iter(group_identities))
            if len(groups[group]) > 1:
                confirmed_id += 1
            else:
                ambiguous_single += 1

        require(
            order["o_w_id"] == customer_rows[0]["c_w_id"]
            and order["o_d_id"] == customer_rows[0]["c_d_id"]
            and order["o_c_id"] in {row["c_id"] for row in customer_rows},
            f"{label} newest-order/customer keys differ",
        )
        line_rows = [row for _, _, row in lines]
        require(
            len(line_rows) == order["o_ol_cnt"]
            and sorted(row["ol_number"] for row in line_rows)
            == list(range(1, len(line_rows) + 1)),
            f"{label} order-line count/sequence differs",
        )
        require(
            all(
                row["ol_w_id"] == order["o_w_id"]
                and row["ol_d_id"] == order["o_d_id"]
                and row["ol_o_id"] == order["o_id"]
                for row in line_rows
            ),
            f"{label} order-line keys differ from newest order",
        )
        if len(line_rows) > 1:
            multirow += 1
        if order["o_carrier_id"] is None:
            carrier_null += 1
        else:
            carrier_present += 1
        delivery_null += sum(row["ol_delivery_d"] is None for row in line_rows)
        delivery_present += sum(row["ol_delivery_d"] is not None for row in line_rows)

    require(confirmed_id >= config["minimum_id_transactions"], "ID branch lacks positive proof")
    require(
        confirmed_last_name >= config["minimum_last_name_transactions"],
        "last-name branch lacks positive proof",
    )
    require(multirow >= config["minimum_multirow_transactions"], "multi-row order lines lack proof")
    require(carrier_null > 0 and carrier_present > 0, "nullable carrier lacks NULL/present coverage")
    require(delivery_null > 0 and delivery_present > 0, "nullable delivery lacks NULL/present coverage")
    require(set(by_session) == set(config["expected_session_ids"]), "ClientTxn session roster differs")

    done_by_session = {record["session_id"]: record for record in done_records}
    require(set(done_by_session) == set(config["expected_session_ids"]), "SessionDone roster differs")
    for session_id in config["expected_session_ids"]:
        session_clients = sorted(by_session[session_id], key=lambda item: item["session_seq"])
        count = len(session_clients)
        require(
            [client["session_seq"] for client in session_clients] == list(range(1, count + 1)),
            f"{session_id} session sequence is not dense",
        )
        require(
            [client["correlation_id"]["txn_id"] for client in session_clients]
            == list(range(1, count + 1)),
            f"{session_id} marker sequence is not dense",
        )
        done = done_by_session[session_id]
        require(done["run_id"] == run_id, f"{session_id} SessionDone run ID")
        require(
            done["final_session_seq"] == count
            and done["logical_transactions_started"] == count
            and done["successful_transactions"] == count
            and done["published_transactions"] == count
            and done["physical_attempts_started"] == count
            and done["terminal_aborted_transactions"] == 0
            and done["definitively_aborted_attempts"] == 0
            and done["retry_attempts"] == 0
            and done["ambiguous_attempts"] == 0,
            f"{session_id} SessionDone counters do not exactly match accepted clients",
        )
        require(
            done["rolling_correlation_digest"] == rolling_digest(run_id, session_id, session_clients),
            f"{session_id} rolling correlation digest differs",
        )

    return clients, {
        "transactions": len(clients),
        "reads": total_reads,
        "confirmed_id": confirmed_id,
        "confirmed_last_name": confirmed_last_name,
        "ambiguous_single": ambiguous_single,
        "multirow": multirow,
        "carrier_null": carrier_null,
        "carrier_present": carrier_present,
        "delivery_null": delivery_null,
        "delivery_present": delivery_present,
    }


def raw_cdc_records(path: Path) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    parsed: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    for index, (entry_id, fields) in enumerate(parse_xrange(path, "CDC tail")):
        require(len(fields) == 2 and all(isinstance(item, str) for item in fields), f"CDC tail[{index}] fields")
        key = strict_json(fields[0], f"CDC tail[{index}].key")
        envelope = strict_json(fields[1], f"CDC tail[{index}].envelope")
        require(isinstance(key, dict) and isinstance(envelope, dict), f"CDC tail[{index}] JSON shape")
        parsed.append((entry_id, key, envelope))
    return parsed


def validate_cdc_tail(
    path: Path,
    run_id: str,
    load_finished_stream_id: str,
    client_correlations: set[tuple[str, int]],
) -> dict[str, int]:
    entries = raw_cdc_records(path)
    require(entries, "CDC tail is empty")
    require(
        parse_stream_id(entries[0][0], "first CDC tail entry")
        > parse_stream_id(load_finished_stream_id, "LOAD_FINISHED CDC cursor"),
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

    marker_correlations: set[tuple[str, int]] = set()
    end_fence_count = 0
    marker_progress: dict[str, int] = {}
    for index, transaction in enumerate(transactions):
        begin = transaction[0][2]
        end = transaction[-1][2]
        rows = transaction[1:-1]
        transaction_id = begin.get("id")
        require(begin.get("status") == "BEGIN" and isinstance(transaction_id, str), f"CDC txn {index} BEGIN")
        require(end.get("status") == "END" and end.get("id") == transaction_id, f"CDC txn {index} END")
        require(end.get("event_count") == len(rows) == 1, f"CDC txn {index} is not one-row marker/control")
        _, key, envelope = rows[0]
        source = envelope.get("source")
        metadata = envelope.get("transaction")
        require(
            isinstance(source, dict)
            and source.get("name") == "mariadb-main"
            and source.get("db") == "benchbase"
            and isinstance(metadata, dict)
            and metadata.get("id") == transaction_id
            and metadata.get("total_order") == 1
            and metadata.get("data_collection_order") == 1,
            f"CDC txn {index} row metadata differs",
        )
        table = source.get("table")
        expected_collection = f"benchbase.{table}"
        require(
            end.get("data_collections")
            == [{"data_collection": expected_collection, "event_count": 1}],
            f"CDC txn {index} END collection differs",
        )
        after = envelope.get("after")
        before = envelope.get("before")
        if table == "session_txn":
            require(
                isinstance(after, dict)
                and set(after) == {"session_id", "txn_id"}
                and key == {"session_id": after["session_id"]},
                f"CDC txn {index} marker shape differs",
            )
            correlation = (after["session_id"], after["txn_id"])
            require(correlation not in marker_correlations, f"CDC txn {index} duplicate marker")
            previous = marker_progress.get(after["session_id"])
            if previous is None:
                require(before is None, f"CDC txn {index} first marker has before image")
            else:
                require(
                    before == {"session_id": after["session_id"], "txn_id": previous},
                    f"CDC txn {index} marker before-image chain differs",
                )
            marker_progress[after["session_id"]] = after["txn_id"]
            marker_correlations.add(correlation)
        elif table == "checker_run_fence":
            require(
                isinstance(after, dict)
                and after.get("run_id") == run_id
                and after.get("phase") == "END_FENCE"
                and key == {"run_id": run_id, "phase": "END_FENCE"},
                f"CDC txn {index} is not the run END_FENCE",
            )
            end_fence_count += 1
            require(index == len(transactions) - 1, "END_FENCE is not the final CDC transaction")
        else:
            raise AcceptanceError(
                f"workload CDC transaction contains application table {table}; OrderStatus must be marker-only"
            )
    require(end_fence_count == 1, "CDC tail must contain exactly one END_FENCE")
    require(marker_correlations == client_correlations, "CDC marker/client correlation sets differ")
    return {"transactions": len(transactions), "markers": len(marker_correlations), "end_fences": 1}


def one_status(records: list[dict[str, Any]], lifecycle: str) -> dict[str, Any]:
    matches = [record for record in records if record.get("lifecycle_state") == lifecycle]
    require(len(matches) == 1, f"status stream must contain one {lifecycle} record")
    return matches[0]


def validate_status_and_metrics(
    config: dict[str, Any],
    run_id: str,
    status_records: list[dict[str, Any]],
    metrics: dict[str, Any],
    client_count: int,
    read_count: int,
) -> tuple[dict[str, Any], str]:
    validate_record_schemas(status_records)
    require(all(record["run_id"] == run_id for record in status_records), "status run IDs differ")
    load_finished = one_status(status_records, "LOAD_FINISHED_SEEN")
    final = one_status(status_records, "FINAL")
    reconciliation = final["reconciliation"]
    require(
        final["outcome"] == "PASS"
        and final["checker_observation"] == "PASS_SO_FAR"
        and final["diagnostics"] == [],
        "final status is not a clean PASS",
    )
    require(
        final["joined_transactions"] == client_count
        and final["pending_cdc_slots"] == 0
        and final["unmatched_client_records"] == 0,
        "final top-level join counters differ",
    )
    require(
        reconciliation["evidence_reconciled"] is True
        and reconciliation["bootstrap_manifest_validated"] is True
        and reconciliation["end_fence_seen"] is True
        and reconciliation["expected_sessions"] == config["terminals"]
        and reconciliation["completed_sessions"] == config["terminals"]
        and reconciliation["accepted_client_transactions"] == client_count
        and reconciliation["joined_workload_transactions"] == client_count
        and reconciliation["unmatched_client_records"] == 0
        and reconciliation["unmatched_workload_cdc_slots"] == 0
        and reconciliation["ambiguous_attempts"] == 0,
        "final reconciliation counters differ",
    )
    require(
        reconciliation["checker_consumed_transactions"]
        == reconciliation["bootstrap_checker_transactions"]
        + reconciliation["phase_anchor_transactions"]
        + client_count
        and final["checked_transactions"] == reconciliation["checker_consumed_transactions"],
        "final checker counter algebra differs",
    )
    require(
        final["assembled_cdc_transactions"]
        == reconciliation["bootstrap_cdc_transactions"] + client_count + 3,
        "final assembled CDC counter algebra differs",
    )

    require(isinstance(metrics, dict), "monitor metrics root is not an object")
    require(
        metrics.get("kind") == "performance_measurement"
        and metrics.get("run_id") == run_id
        and metrics.get("processing_mode") == "audited"
        and metrics.get("completed") is True
        and metrics.get("completion_status") == "completed"
        and metrics.get("evidence_reconciled") is True
        and metrics.get("outcome") == "PASS"
        and metrics.get("diagnostics") == [],
        "monitor measurement is not a clean audited PASS",
    )
    limits = metrics.get("limits")
    require(
        isinstance(limits, dict)
        and isinstance(limits.get("joiner"), dict)
        and limits["joiner"].get("max_bootstrap_state_rows")
        == config["monitor_max_bootstrap_state_rows"]
        and limits["joiner"].get("max_bootstrap_state_bytes")
        == config["monitor_max_bootstrap_state_bytes"],
        "monitor bootstrap-state limit provenance differs from run configuration",
    )
    assembler = metrics.get("assembler")
    joiner = metrics.get("joiner")
    checker = metrics.get("checker")
    measured_reconciliation = metrics.get("reconciliation")
    require(
        all(
            isinstance(item, dict)
            for item in (assembler, joiner, checker, measured_reconciliation)
        ),
        "monitor metrics sections",
    )
    require(
        assembler.get("completed_run_control_transactions") == 3
        and assembler.get("completed_workload_transactions") == client_count,
        "assembler workload/control counters differ",
    )
    require(
        joiner.get("accepted_client_transactions") == client_count
        and joiner.get("accepted_workload_cdc_transactions") == client_count
        and joiner.get("joined_workload_transactions") == client_count
        and joiner.get("marker_only_workload_transactions") == client_count
        and joiner.get("joined_read_events") == read_count
        and joiner.get("joined_write_events") == 0,
        "joiner marker-only/read counters differ",
    )
    for field in (
        "pending_client_records",
        "pending_cdc_slots",
        "unmatched_client_records",
        "unmatched_workload_cdc_slots",
    ):
        require(joiner.get(field) == 0, f"joiner terminal {field} is nonzero")
    require(
        checker.get("workload_transactions") == client_count
        and checker.get("read_events") == read_count,
        "checker workload/read counters differ",
    )
    for field in (
        "evidence_reconciled",
        "bootstrap_manifest_validated",
        "end_fence_seen",
        "expected_sessions",
        "completed_sessions",
        "accepted_client_transactions",
        "joined_workload_transactions",
        "bootstrap_cdc_transactions",
        "bootstrap_checker_transactions",
        "phase_anchor_transactions",
        "checker_consumed_transactions",
        "unmatched_client_records",
        "unmatched_workload_cdc_slots",
        "ambiguous_attempts",
    ):
        require(measured_reconciliation.get(field) == reconciliation[field], f"metrics reconciliation {field}")
    return final, load_finished["last_cdc_stream_id"]


def validate_acceptance(
    config_path: Path,
    run_id_path: Path,
    client_path: Path,
    cdc_path: Path,
    status_path: Path,
    customer_catalog_path: Path,
    monitor_metrics_path: Path,
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
    decoder = CanonicalDecoder(contracts["tpcc"])
    catalog, groups = load_customer_catalog(customer_catalog_path)
    client_entries = payload_records(client_path, "client stream")
    clients, coverage = validate_clients(
        config,
        run_id,
        [record for _, record in client_entries],
        decoder,
        catalog,
        groups,
    )
    status_entries = payload_records(status_path, "status stream")
    metrics = load_json(monitor_metrics_path, "monitor metrics")
    final, load_finished_cdc_id = validate_status_and_metrics(
        config,
        run_id,
        [record for _, record in status_entries],
        metrics,
        len(clients),
        coverage["reads"],
    )
    correlations = {
        (client["session_id"], client["correlation_id"]["txn_id"])
        for client in clients
    }
    cdc = validate_cdc_tail(cdc_path, run_id, load_finished_cdc_id, correlations)
    require(cdc["markers"] == len(clients), "marker count differs from successful clients")
    return {
        "kind": "tpcc_order_status_acceptance",
        "passed": True,
        "run_id": run_id,
        "configuration": config,
        "coverage": coverage,
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
        "final_outcome": "PASS",
    }


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        with temporary.open("x", encoding="utf-8") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, path)
    except OSError as error:
        raise AcceptanceError(f"cannot publish acceptance report: {error}") from error
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--run-id-file", required=True, type=Path)
    parser.add_argument("--client-stream", required=True, type=Path)
    parser.add_argument("--cdc-tail", required=True, type=Path)
    parser.add_argument("--status-stream", required=True, type=Path)
    parser.add_argument("--customer-catalog", required=True, type=Path)
    parser.add_argument("--monitor-metrics", required=True, type=Path)
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
            args.monitor_metrics,
        )
        write_report(args.output, report)
    except AcceptanceError as error:
        print(f"TPC-C OrderStatus acceptance failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
