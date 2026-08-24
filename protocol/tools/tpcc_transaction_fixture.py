#!/usr/bin/env python3
"""Independent normalizer/verifier for captured TPC-C Debezium transactions."""

from __future__ import annotations

import hashlib
import json
import re
import struct
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from tpcc_codec import CodecError, TpccCodec, reject_duplicate_keys


STREAM_ID = re.compile(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)\Z")
HEX = re.compile(r"(?:[0-9a-f]{2})*\Z")
BOOTSTRAP_DOMAIN = b"db-monitoring/bootstrap-state-digest\x00"


class TransactionFixtureError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise TransactionFixtureError(message)


def parse_json_text(value: Any, label: str) -> dict[str, Any]:
    require(isinstance(value, str), f"{label} is not JSON text")
    try:
        parsed = json.loads(value, object_pairs_hook=reject_duplicate_keys)
    except (json.JSONDecodeError, CodecError) as error:
        raise TransactionFixtureError(f"{label} is malformed: {error}") from error
    require(isinstance(parsed, dict), f"{label} is not a JSON object")
    return parsed


def stream_id(value: Any, label: str) -> tuple[int, int]:
    require(isinstance(value, str), f"{label} stream ID is not string")
    match = STREAM_ID.fullmatch(value)
    require(match is not None, f"{label} stream ID is malformed")
    return int(match.group(1)), int(match.group(2))


def parse_raw_entry(entry: Any, label: str) -> tuple[tuple[int, int], dict[str, Any], dict[str, Any]]:
    require(
        isinstance(entry, list)
        and len(entry) == 2
        and isinstance(entry[1], list)
        and len(entry[1]) == 2,
        f"{label} raw XRANGE entry has wrong shape",
    )
    return (
        stream_id(entry[0], label),
        parse_json_text(entry[1][0], f"{label} key"),
        parse_json_text(entry[1][1], f"{label} envelope"),
    )


def table_prefix(table: str) -> bytes:
    schema, separator, name = table.partition(".")
    require(separator == ".", f"unqualified fixture table: {table}")
    schema_bytes = schema.encode("ascii")
    name_bytes = name.encode("ascii")
    return struct.pack(">I", len(schema_bytes)) + schema_bytes + struct.pack(">I", len(name_bytes)) + name_bytes


def canonical_write(table: str, key: bytes, state: str, value: bytes | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"table": table, "key_hex": key.hex(), "state": state}
    if state == "present":
        require(value is not None, "present fixture write lacks value")
        result["value_hex"] = value.hex()
    return result


def normalize_application_row(
    codec: TpccCodec,
    table: str,
    key_object: dict[str, Any],
    envelope: dict[str, Any],
    final_writes: dict[bytes, dict[str, Any]],
) -> None:
    operation = envelope.get("op")
    require(operation in {"c", "u", "d"}, f"{table}: unsupported row operation")
    before = envelope.get("before")
    after = envelope.get("after")
    require((before is None) == (operation == "c"), f"{table}: before image/operation mismatch")
    require((after is None) == (operation == "d"), f"{table}: after image/operation mismatch")
    reference_image = after if after is not None else before
    require(isinstance(reference_image, dict), f"{table}: row image is not object")
    table_contract = codec.table(table)
    require(
        set(key_object) == set(table_contract["key_columns"])
        and all(key_object[column] == reference_image[column] for column in key_object),
        f"{table}: Debezium key differs from row primary key",
    )
    try:
        before_encoded = codec.encode_row(table, before) if before is not None else None
        after_encoded = codec.encode_row(table, after) if after is not None else None
    except CodecError as error:
        raise TransactionFixtureError(f"{table}: codec failed: {error.code}: {error}") from error
    if before_encoded is not None and (
        after_encoded is None or before_encoded[0] != after_encoded[0]
    ):
        final_writes[before_encoded[0]] = canonical_write(table, before_encoded[0], "absent")
    if after_encoded is not None:
        final_writes[after_encoded[0]] = canonical_write(
            table, after_encoded[0], "present", after_encoded[1]
        )


def normalize_transaction(
    transaction: dict[str, Any],
    codec: TpccCodec,
    run_id: str,
    cdc_seq: int,
    phase: str,
    marker_identities: set[tuple[str, int]],
) -> tuple[dict[str, Any], str]:
    name = transaction.get("name")
    require(isinstance(name, str) and name, "transaction fixture lacks name")
    raw_entries = transaction.get("raw_entries")
    require(isinstance(raw_entries, list) and len(raw_entries) >= 3, f"{name}: raw entries missing")
    parsed = [parse_raw_entry(entry, f"{name}[{index}]") for index, entry in enumerate(raw_entries)]
    ids = [entry[0] for entry in parsed]
    require(ids == sorted(set(ids)), f"{name}: stream IDs are not unique/increasing")
    begin = parsed[0][2]
    end = parsed[-1][2]
    require(begin.get("status") == "BEGIN", f"{name}: first entry is not BEGIN")
    require(end.get("status") == "END", f"{name}: last entry is not END")
    transaction_id = begin.get("id")
    require(isinstance(transaction_id, str) and transaction_id, f"{name}: BEGIN id missing")
    require(end.get("id") == transaction_id, f"{name}: END id mismatch")
    row_entries = parsed[1:-1]
    require(end.get("event_count") == len(row_entries), f"{name}: END event_count mismatch")

    collection_counts: Counter[str] = Counter()
    collection_orders: dict[str, int] = defaultdict(int)
    application_rows: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    fence_rows: list[dict[str, Any]] = []
    marker_rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
    final_writes: dict[bytes, dict[str, Any]] = {}
    for total_order, (_, key_object, envelope) in enumerate(row_entries, start=1):
        source = envelope.get("source")
        transaction_meta = envelope.get("transaction")
        require(isinstance(source, dict), f"{name}: row source missing")
        require(
            source.get("version") == "3.6.1.Final"
            and source.get("connector") == "mariadb"
            and source.get("db") == "benchbase",
            f"{name}: row source metadata mismatch",
        )
        require(
            isinstance(transaction_meta, dict)
            and transaction_meta.get("id") == transaction_id
            and transaction_meta.get("total_order") == total_order,
            f"{name}: row transaction order mismatch",
        )
        table_name = source.get("table")
        require(isinstance(table_name, str), f"{name}: row table missing")
        qualified_table = f"benchbase.{table_name}"
        collection_counts[qualified_table] += 1
        collection_orders[qualified_table] += 1
        require(
            transaction_meta.get("data_collection_order")
            == collection_orders[qualified_table],
            f"{name}: data-collection order mismatch",
        )
        if qualified_table in codec.tables:
            application_rows.append((qualified_table, key_object, envelope))
        elif qualified_table == "benchbase.checker_run_fence":
            fence_rows.append(envelope)
        elif qualified_table == "benchbase.session_txn":
            marker_rows.append((key_object, envelope))
        else:
            raise TransactionFixtureError(f"{name}: unexpected table {qualified_table}")

    end_collections = end.get("data_collections")
    require(isinstance(end_collections, list), f"{name}: END collections missing")
    declared_counts: Counter[str] = Counter()
    for collection in end_collections:
        require(
            isinstance(collection, dict)
            and set(collection) == {"data_collection", "event_count"}
            and isinstance(collection["data_collection"], str)
            and isinstance(collection["event_count"], int),
            f"{name}: malformed END collection",
        )
        declared_counts[collection["data_collection"]] += collection["event_count"]
    require(declared_counts == collection_counts, f"{name}: END collection counts mismatch")

    fence_phase: str | None = None
    correlation: dict[str, Any] | None = None
    if fence_rows:
        require(
            len(fence_rows) == 1 and not application_rows and not marker_rows,
            f"{name}: fence transaction is mixed",
        )
        after = fence_rows[0].get("after")
        require(isinstance(after, dict) and after.get("run_id") == run_id, f"{name}: fence row")
        fence_phase = after.get("phase")
        require(fence_phase in {"RUN_START", "LOAD_FINISHED", "END_FENCE"}, f"{name}: fence phase")
        kind = "RUN_CONTROL"
        if fence_phase == "RUN_START":
            require(phase == "before_start", f"{name}: RUN_START phase order")
            next_phase = "bootstrap"
        elif fence_phase == "LOAD_FINISHED":
            require(phase == "bootstrap", f"{name}: LOAD_FINISHED phase order")
            next_phase = "workload"
        else:
            require(phase == "workload", f"{name}: END_FENCE phase order")
            next_phase = "closed"
    elif marker_rows:
        require(phase == "workload" and len(marker_rows) == 1, f"{name}: workload marker count")
        marker_key, marker_envelope = marker_rows[0]
        require(row_entries[-1][2] is marker_envelope, f"{name}: marker is not final row")
        marker_after = marker_envelope.get("after")
        require(isinstance(marker_after, dict), f"{name}: marker after image missing")
        correlation = {
            "run_id": run_id,
            "session_id": marker_after.get("session_id"),
            "txn_id": marker_after.get("txn_id"),
        }
        require(
            isinstance(correlation["session_id"], str)
            and isinstance(correlation["txn_id"], int)
            and correlation["txn_id"] > 0,
            f"{name}: marker correlation malformed",
        )
        identity = (correlation["session_id"], correlation["txn_id"])
        require(marker_envelope.get("op") == "c", f"{name}: marker is not an insert")
        require(marker_envelope.get("before") is None, f"{name}: marker has before image")
        require(
            marker_key
            == {"session_id": correlation["session_id"], "txn_id": correlation["txn_id"]},
            f"{name}: marker key/after identity mismatch",
        )
        require(identity not in marker_identities, f"{name}: marker identity reused")
        marker_identities.add(identity)
        kind = "WORKLOAD_MARKED"
        next_phase = phase
    else:
        require(phase == "bootstrap" and application_rows, f"{name}: unmarked transaction phase")
        kind = "BOOTSTRAP_APP"
        next_phase = phase

    for table, key_object, envelope in application_rows:
        normalize_application_row(codec, table, key_object, envelope, final_writes)
    writes = [final_writes[key] for key in sorted(final_writes)]
    actual = {
        "cdc_seq": cdc_seq,
        "correlation_id": correlation,
        "fence_phase": fence_phase,
        "kind": kind,
        "writes": writes,
    }
    require(transaction.get("expected") == actual, f"{name}: expected normalization differs")
    return actual, next_phase


def bootstrap_digest(writes: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    present = [write for write in writes if write["state"] == "present"]
    require(len(present) == len(writes), "bootstrap fixture contains absent write")
    counts: dict[str, int] = defaultdict(int)
    rows: list[tuple[bytes, bytes]] = []
    for write in present:
        counts[write["table"]] += 1
        rows.append((bytes.fromhex(write["key_hex"]), bytes.fromhex(write["value_hex"])))
    rows.sort(key=lambda item: item[0])
    preimage = bytearray(BOOTSTRAP_DOMAIN + struct.pack(">Q", len(rows)))
    for key, value in rows:
        preimage.extend(struct.pack(">I", len(key)) + key)
        preimage.extend(struct.pack(">I", len(value)) + value)
    count_array = [
        {"table": table, "row_count": counts.get(table, 0)}
        for table in sorted(counts)
    ]
    return count_array, hashlib.sha256(preimage).hexdigest()


def verify_transaction_fixture(document: dict[str, Any], codec: TpccCodec) -> list[dict[str, Any]]:
    run_id = document["run_id"]
    startup = document["startup_schema"]
    require(isinstance(startup, list) and len(startup) == 26, "startup schema must have 26 entries")
    startup_ids = [parse_raw_entry(entry, f"startup[{index}]")[0] for index, entry in enumerate(startup)]
    require(startup_ids == sorted(set(startup_ids)), "startup schema stream order mismatch")
    created_tables: set[str] = set()
    created_metadata: dict[str, dict[str, Any]] = {}
    for index, entry in enumerate(startup):
        _, _, envelope = parse_raw_entry(entry, f"startup[{index}]")
        source = envelope.get("source")
        require(
            isinstance(source, dict) and source.get("version") == "3.6.1.Final",
            f"startup[{index}] source version mismatch",
        )
        for change in envelope.get("tableChanges", []):
            if change.get("type") == "CREATE" and isinstance(change.get("id"), str):
                identifier = change["id"].replace('"', "")
                created_tables.add(identifier)
                require(isinstance(change.get("table"), dict), f"startup[{index}] table metadata")
                created_metadata[identifier] = change["table"]
    expected_tables = set(codec.contract["application_tables"]) | {
        "benchbase.checker_run_fence",
        "benchbase.session_txn",
    }
    require(created_tables == expected_tables, "startup schema table roster mismatch")
    for table_contract in codec.contract["tables"]:
        table_name = table_contract["name"]
        metadata = created_metadata[table_name]
        require(metadata.get("defaultCharsetName") == "utf8mb4", f"{table_name}: startup charset")
        require(
            metadata.get("primaryKeyColumnNames") == table_contract["key_columns"],
            f"{table_name}: startup primary key mismatch",
        )
        metadata_columns = metadata.get("columns")
        require(
            isinstance(metadata_columns, list)
            and len(metadata_columns) == len(table_contract["columns"]),
            f"{table_name}: startup column count mismatch",
        )
        for expected_column, actual_column in zip(
            table_contract["columns"], metadata_columns, strict=True
        ):
            sql_type = expected_column["sql_type"]
            type_name = sql_type.split("(", maxsplit=1)[0]
            require(
                actual_column.get("name") == expected_column["name"]
                and actual_column.get("typeName") == type_name
                and actual_column.get("optional") == expected_column["nullable"],
                f"{table_name}.{expected_column['name']}: startup type/nullability mismatch",
            )
            generated = expected_column.get("generated") == "auto_increment"
            require(
                actual_column.get("autoIncremented") == generated
                and actual_column.get("generated") == generated,
                f"{table_name}.{expected_column['name']}: startup generated metadata mismatch",
            )
            if sql_type.startswith(("VARCHAR(", "CHAR(")):
                expected_length = int(sql_type.split("(")[1][:-1])
                require(
                    actual_column.get("length") == expected_length
                    and actual_column.get("charsetName") == "utf8mb4",
                    f"{table_name}.{expected_column['name']}: startup text metadata mismatch",
                )
            elif sql_type.startswith("DECIMAL("):
                precision, scale = map(int, sql_type.split("(")[1][:-1].split(","))
                require(
                    actual_column.get("length") == precision
                    and actual_column.get("scale") == scale,
                    f"{table_name}.{expected_column['name']}: startup decimal metadata mismatch",
                )

    transactions = document["transactions"]
    require(isinstance(transactions, list) and len(transactions) == 12, "fixture transaction count")
    expected_names = [
        "run_start",
        "bootstrap_rows",
        "load_finished",
        "payment_shape",
        "new_order_shape",
        "delivery_shape",
        "order_status_marker_only",
        "primary_key_change",
        "insert_then_delete",
        "net_no_change",
        "stock_level_marker_only",
        "end_fence",
    ]
    require([item.get("name") for item in transactions] == expected_names, "fixture transaction names")
    phase = "before_start"
    marker_identities: set[tuple[str, int]] = set()
    normalized: list[dict[str, Any]] = []
    for cdc_seq, transaction in enumerate(transactions):
        require(
            isinstance(transaction, dict) and set(transaction) == {"name", "raw_entries", "expected"},
            f"transaction {cdc_seq} shape mismatch",
        )
        actual, phase = normalize_transaction(
            transaction, codec, run_id, cdc_seq, phase, marker_identities
        )
        normalized.append(actual)
    require(phase == "closed", "fixture did not reach END_FENCE")

    bootstrap = normalized[1]["writes"]
    counts, digest = bootstrap_digest(bootstrap)
    manifest = document["bootstrap_manifest"]
    require(manifest["kind"] == "bootstrap_manifest", "fixture bootstrap kind")
    require(manifest["run_id"] == run_id, "fixture bootstrap run ID")
    require(manifest["table_row_counts"] == counts, "fixture bootstrap counts mismatch")
    require(manifest["canonical_state_digest"] == digest, "fixture bootstrap digest mismatch")
    return normalized
