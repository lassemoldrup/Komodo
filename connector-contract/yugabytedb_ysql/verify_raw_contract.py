#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


SOURCE_VERSION = "dz.2.5.2.yb.2026.1"
SOURCE_NAME = "connector_contract"
CANONICAL_SCHEMA = "benchbase"
MAX_DIAGNOSTIC_ID_BYTES = 256

TABLES: dict[str, dict[str, tuple[str, ...]]] = {
    "usertable": {
        "columns": (
            "ycsb_key",
            "field1",
            "field2",
            "field3",
            "field4",
            "field5",
            "field6",
            "field7",
            "field8",
            "field9",
            "field10",
        ),
        "primary_key": ("ycsb_key",),
    },
    "checker_run_fence": {
        "columns": ("run_id", "phase", "nonce"),
        "primary_key": ("run_id", "phase"),
    },
    "session_txn": {
        "columns": ("session_id", "txn_id"),
        "primary_key": ("session_id",),
    },
}


class ContractViolation(RuntimeError):
    def __init__(self, code: str, diagnostic: str) -> None:
        super().__init__(diagnostic)
        self.code = code
        self.diagnostic = diagnostic


def require(condition: bool, code: str, diagnostic: str) -> None:
    if not condition:
        raise ContractViolation(code, diagnostic)


def require_exact_keys(
    value: dict[str, Any], expected: set[str], label: str, code: str = "RAW_ENVELOPE_PROFILE_MISMATCH"
) -> None:
    actual = set(value)
    require(
        actual == expected,
        code,
        f"{label} fields differ: expected={sorted(expected)} actual={sorted(actual)}",
    )


def require_bounded_text(value: Any, code: str, label: str, maximum_bytes: int) -> str:
    require(isinstance(value, str) and bool(value), code, f"{label} must be nonempty text")
    encoded = value.encode("utf-8")
    require(len(encoded) <= maximum_bytes, code, f"{label} exceeds {maximum_bytes} UTF-8 bytes")
    return value


def stream_id(value: str) -> tuple[int, int]:
    match = re.fullmatch(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)", value)
    require(match is not None, "REDIS_STREAM_ID_INVALID", f"invalid Redis stream ID: {value!r}")
    assert match is not None
    return int(match.group(1)), int(match.group(2))


def load_entries(path: Path) -> list[dict[str, Any]]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ContractViolation("REDIS_CAPTURE_INVALID", f"cannot decode Redis capture: {error}") from error
    require(isinstance(raw, list), "REDIS_CAPTURE_INVALID", "Redis capture must be an array")
    result: list[dict[str, Any]] = []
    previous: tuple[int, int] | None = None
    for index, entry in enumerate(raw):
        require(
            isinstance(entry, list) and len(entry) == 2,
            "REDIS_CAPTURE_INVALID",
            f"entry {index} must be [stream_id, fields]",
        )
        redis_id, fields = entry
        require(isinstance(redis_id, str), "REDIS_CAPTURE_INVALID", f"entry {index} ID is not text")
        parsed_id = stream_id(redis_id)
        require(
            previous is None or parsed_id > previous,
            "REDIS_STREAM_ORDER_INVALID",
            f"Redis stream ID did not increase at {redis_id}",
        )
        previous = parsed_id
        require(
            isinstance(fields, list) and len(fields) == 2,
            "REDIS_CAPTURE_INVALID",
            f"entry {redis_id} must contain exactly one key/value field pair",
        )
        message_key, message_value = fields
        require(
            isinstance(message_key, str) and isinstance(message_value, str),
            "REDIS_CAPTURE_INVALID",
            f"entry {redis_id} key/value must be JSON text",
        )
        try:
            decoded_key = json.loads(message_key)
            decoded_value = json.loads(message_value)
        except json.JSONDecodeError as error:
            raise ContractViolation(
                "REDIS_CAPTURE_INVALID", f"entry {redis_id} contains invalid JSON: {error}"
            ) from error
        require(
            isinstance(decoded_key, dict) and isinstance(decoded_value, dict),
            "REDIS_CAPTURE_INVALID",
            f"entry {redis_id} key/value must decode to objects",
        )
        result.append({"redis_id": redis_id, "key": decoded_key, "value": decoded_value})
    return result


def load_capture_metadata(capture_path: Path, plugin: str, redis_batch_size: int) -> dict[str, Any] | None:
    metadata_path = capture_path.with_name("capture-metadata.json")
    if not metadata_path.exists():
        return None
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ContractViolation("FIXTURE_METADATA_INVALID", f"cannot decode capture metadata: {error}") from error
    require(isinstance(metadata, dict), "FIXTURE_METADATA_INVALID", "capture metadata must be an object")

    required_fields = {
        "capture_id",
        "captured_at_utc",
        "scenario",
        "platform",
        "plugin",
        "slot_lsn_type",
        "streaming_mode",
        "logical_slots",
        "publication_count",
        "publication_name",
        "connector_tasks",
        "redis_streams",
        "source_max_batch_size",
        "redis_sink_batch_size",
        "database_image",
        "database_binary_version",
        "database_build_hash",
        "connector_version",
        "connector_sha256",
        "debezium_server_version",
        "java_runtime_version",
        "redis_version",
        "locally_built_image_id",
        "expected_completed_transactions",
        "gate",
        "file_sha256",
    }
    optional_fields = {"transaction", "backpressure"}
    actual_fields = set(metadata)
    require(
        required_fields <= actual_fields and actual_fields <= required_fields | optional_fields,
        "FIXTURE_METADATA_INVALID",
        "capture metadata fields differ: "
        f"missing={sorted(required_fields - actual_fields)} extra={sorted(actual_fields - required_fields - optional_fields)}",
    )
    require(metadata["platform"] == "linux/amd64", "FIXTURE_METADATA_INVALID", "capture platform differs")
    require(metadata["plugin"] == plugin == "pgoutput", "PLUGIN_NOT_ADMITTED", "capture plugin differs")
    require(metadata["slot_lsn_type"] == "SEQUENCE", "CAPTURE_TOPOLOGY_INVALID", "slot is not SEQUENCE")
    require(metadata["streaming_mode"] == "default", "CAPTURE_TOPOLOGY_INVALID", "streaming mode is not default")
    require(metadata["logical_slots"] == 1, "CAPTURE_TOPOLOGY_INVALID", "capture did not use one slot")
    require(metadata["publication_count"] == 1, "CAPTURE_TOPOLOGY_INVALID", "capture did not use one publication")
    require(
        metadata["publication_name"] == "connector_contract_publication",
        "CAPTURE_TOPOLOGY_INVALID",
        "publication name differs",
    )
    require(metadata["connector_tasks"] == 1, "CAPTURE_TOPOLOGY_INVALID", "capture did not use one connector task")
    require(metadata["redis_streams"] == ["cdc"], "CAPTURE_TOPOLOGY_INVALID", "capture did not use only Redis stream cdc")
    require(
        type(metadata["source_max_batch_size"]) is int and metadata["source_max_batch_size"] > 0,
        "FIXTURE_METADATA_INVALID",
        "source batch size is invalid",
    )
    require(
        metadata["redis_sink_batch_size"] == redis_batch_size,
        "FIXTURE_METADATA_INVALID",
        f"Redis batch size differs: metadata={metadata['redis_sink_batch_size']} command={redis_batch_size}",
    )
    require(metadata["connector_version"] == SOURCE_VERSION, "SOURCE_VERSION_MISMATCH", "metadata connector version differs")
    if metadata["scenario"] == "redis_backpressure":
        backpressure = metadata.get("backpressure")
        require(
            isinstance(backpressure, dict)
            and set(backpressure)
            == {
                "mechanism",
                "requested_pause_ms",
                "pause_started_monotonic_ns",
                "workload_commits_completed_monotonic_ns",
                "connector_xadd_observed_monotonic_ns",
                "explicit_unpause_monotonic_ns",
                "connector_xadd_observed_during_pause",
                "stream_length_before_pause",
                "stream_length_while_connector_blocked",
                "stream_length_after_unpause",
                "transport_errors_observed",
            },
            "BACKPRESSURE_EVIDENCE_INVALID",
            "Redis backpressure evidence fields differ",
        )
        assert isinstance(backpressure, dict)
        ordered_times = [
            backpressure["pause_started_monotonic_ns"],
            backpressure["workload_commits_completed_monotonic_ns"],
            backpressure["connector_xadd_observed_monotonic_ns"],
            backpressure["explicit_unpause_monotonic_ns"],
        ]
        require(
            backpressure["mechanism"] == "redis_client_pause_write"
            and type(backpressure["requested_pause_ms"]) is int
            and backpressure["requested_pause_ms"] > 0
            and all(type(value) is int and value > 0 for value in ordered_times)
            and ordered_times == sorted(ordered_times)
            and backpressure["connector_xadd_observed_during_pause"] is True
            and backpressure["stream_length_before_pause"] == 0
            and backpressure["stream_length_while_connector_blocked"] == 0
            and type(backpressure["stream_length_after_unpause"]) is int
            and backpressure["stream_length_after_unpause"] > 0
            and backpressure["transport_errors_observed"] is False,
            "BACKPRESSURE_EVIDENCE_INVALID",
            "Redis WRITE pause did not demonstrably overlap connector output",
        )
    else:
        require("backpressure" not in metadata, "FIXTURE_METADATA_INVALID", "unexpected backpressure evidence")
    require(
        metadata["gate"]
        == {
            "name": "raw_connector_redis_contract",
            "status": "PASSED",
            "decision_boundary": (
                "Connector IDs, source.txId, LSN, and HybridTime are diagnostic metadata only; "
                "the verifier frames only the unique contiguous BEGIN/rows/END interval and "
                "correlates completed frames only by the SQL-last client marker."
            ),
        },
        "FIXTURE_METADATA_INVALID",
        "capture report does not record the frozen passing gate",
    )

    checksums = metadata["file_sha256"]
    require(isinstance(checksums, dict) and bool(checksums), "FIXTURE_METADATA_INVALID", "file_sha256 is absent")
    require("redis-stream.json" in checksums, "FIXTURE_METADATA_INVALID", "Redis capture checksum is absent")
    for independent_name in (
        "commit-order.json",
        "connector-output-order.json",
        "redis-monitor.log",
    ):
        require(
            independent_name in checksums,
            "INDEPENDENT_ORDER_EVIDENCE_MISSING",
            f"independent ordering artifact is absent: {independent_name}",
        )
    if metadata["scenario"] == "redis_backpressure":
        for evidence_name in (
            "backpressure-evidence.json",
            "redis-paused-clients.txt",
        ):
            require(
                evidence_name in checksums,
                "BACKPRESSURE_EVIDENCE_INVALID",
                f"backpressure artifact is absent: {evidence_name}",
            )
    for name, expected in checksums.items():
        require(
            isinstance(name, str) and Path(name).name == name,
            "FIXTURE_METADATA_INVALID",
            f"invalid fixture file name: {name!r}",
        )
        require(
            isinstance(expected, str) and re.fullmatch(r"[0-9a-f]{64}", expected) is not None,
            "FIXTURE_METADATA_INVALID",
            f"invalid checksum for {name}",
        )
        file_path = capture_path.parent / name
        require(file_path.is_file(), "FIXTURE_FILE_MISSING", f"fixture file is missing: {name}")
        actual = hashlib.sha256(file_path.read_bytes()).hexdigest()
        require(
            actual == expected,
            "FIXTURE_CHECKSUM_MISMATCH",
            f"fixture checksum differs for {name}: expected={expected} actual={actual}",
        )

    if "debezium-server.log" in checksums:
        log_text = (capture_path.parent / "debezium-server.log").read_text(encoding="utf-8")
        require(
            re.search(r"\sERROR\s|\bException\b|\bCaused by:\b", log_text) is None,
            "TRANSPORT_ERROR",
            "Debezium Server log contains an error or exception",
        )
        require(
            f"redis.batch.size={metadata['redis_sink_batch_size']}" in log_text,
            "CAPTURE_TOPOLOGY_INVALID",
            "Debezium Server log does not attest the requested Redis batch size",
        )
        require(
            f"max.batch.size = {metadata['source_max_batch_size']}" in log_text,
            "CAPTURE_TOPOLOGY_INVALID",
            "Debezium Server log does not attest the requested connector batch size",
        )
        for expected_log_fact in (
            "connector.class = io.debezium.connector.postgresql.YugabyteDBConnector",
            "publication.name = connector_contract_publication",
            "slot.lsn.type = SEQUENCE",
            "task.id = 0",
            "Connector config provide.transaction.metadata is set to true",
        ):
            require(
                expected_log_fact in log_text,
                "CAPTURE_TOPOLOGY_INVALID",
                f"Debezium Server log does not attest: {expected_log_fact}",
            )
    if "commit-order.json" in checksums:
        order_evidence = json.loads((capture_path.parent / "commit-order.json").read_text(encoding="utf-8"))
        require(
            isinstance(order_evidence, dict)
            and order_evidence.get("source") == "observed_ysqlsh_commit_acknowledgements"
            and order_evidence.get("completed_transactions") == metadata["expected_completed_transactions"],
            "FIXTURE_METADATA_INVALID",
            "commit acknowledgement evidence differs from the capture report",
        )
    connector_evidence = json.loads(
        (capture_path.parent / "connector-output-order.json").read_text(encoding="utf-8")
    )
    require(
        isinstance(connector_evidence, dict)
        and connector_evidence.get("source") == "redis_monitor_connector_xadd_order"
        and connector_evidence.get("completed_transactions")
        == metadata["expected_completed_transactions"]
        and type(connector_evidence.get("xadd_commands")) is int
        and connector_evidence["xadd_commands"] > 0,
        "COMPLETED_TRANSACTION_ORDER_MISMATCH",
        "independently captured connector output order differs from the capture report",
    )
    if "redis-streams.txt" in checksums:
        streams = (capture_path.parent / "redis-streams.txt").read_text(encoding="utf-8").splitlines()
        require(streams == ["cdc"], "CAPTURE_TOPOLOGY_INVALID", f"Redis stream roster differs: {streams}")
    if "database-catalog.txt" in checksums:
        catalog = (capture_path.parent / "database-catalog.txt").read_text(encoding="utf-8")
        require(
            re.search(
                r"connector_contract_slot\s*\|\s*pgoutput\s*\|\s*benchbase\s*\|\s*SEQUENCE\s*\|\s*t",
                catalog,
            )
            is not None,
            "CAPTURE_TOPOLOGY_INVALID",
            "database catalog does not attest the one active pgoutput SEQUENCE slot",
        )
    return metadata


def validate_metadata_record(entry: dict[str, Any], expected_status: str) -> None:
    redis_id = entry["redis_id"]
    key = entry["key"]
    value = entry["value"]
    require_exact_keys(key, {"id"}, f"{expected_status} key at {redis_id}")
    require_exact_keys(value, {"status", "id", "event_count", "data_collections", "ts_ms"}, expected_status)
    require(value["status"] == expected_status, "TRANSACTION_FRAMING_INVALID", f"metadata status is not {expected_status}")
    key_id = require_bounded_text(key["id"], "OPAQUE_TRANSACTION_ID_INVALID", f"{expected_status} key id", MAX_DIAGNOSTIC_ID_BYTES)
    envelope_id = require_bounded_text(
        value["id"], "OPAQUE_TRANSACTION_ID_INVALID", f"{expected_status} envelope id", MAX_DIAGNOSTIC_ID_BYTES
    )
    require(
        key_id == envelope_id,
        "SAME_RECORD_TRANSACTION_ID_MISMATCH",
        f"{expected_status} key/envelope IDs differ within Redis record {redis_id}",
    )
    require(type(value["ts_ms"]) is int and value["ts_ms"] >= 0, "RAW_ENVELOPE_PROFILE_MISMATCH", f"{expected_status} ts_ms is invalid")
    if expected_status == "BEGIN":
        require(value["event_count"] is None, "RAW_ENVELOPE_PROFILE_MISMATCH", "BEGIN event_count is not null")
        require(value["data_collections"] is None, "RAW_ENVELOPE_PROFILE_MISMATCH", "BEGIN collections are not null")


def validate_column(table: str, column: str, value: Any, label: str) -> None:
    if column in {"ycsb_key", "txn_id"}:
        require(type(value) is int, "FULL_IMAGE_INVALID", f"{label}.{column} is not an integer")
        return
    nullable = table == "usertable" and column != "ycsb_key"
    if value is None:
        require(nullable, "FULL_IMAGE_INVALID", f"{label}.{column} is unexpectedly null")
        return
    limits = {"run_id": 36, "phase": 32, "nonce": 36, "session_id": 64}
    limit = 100 if column.startswith("field") else limits[column]
    require(isinstance(value, str), "FULL_IMAGE_INVALID", f"{label}.{column} is not text")
    require(len(value) <= limit, "FULL_IMAGE_INVALID", f"{label}.{column} exceeds {limit} characters")


def validate_image(image: Any, table: str, label: str) -> dict[str, Any]:
    require(isinstance(image, dict), "FULL_IMAGE_INVALID", f"{label} is absent or is not an object")
    columns = set(TABLES[table]["columns"])
    require_exact_keys(image, columns, label, "FULL_IMAGE_INVALID")
    for column, value in image.items():
        validate_column(table, column, value, label)
    return image


def validate_row(entry: dict[str, Any], row_number: int) -> dict[str, Any]:
    redis_id = entry["redis_id"]
    row = entry["value"]
    require_exact_keys(row, {"before", "after", "source", "op", "ts_ms", "transaction"}, f"row {row_number}")
    require(row["op"] in {"c", "u", "d"}, "ROW_OPERATION_INVALID", f"row {row_number} operation is invalid")
    require(type(row["ts_ms"]) is int and row["ts_ms"] >= 0, "RAW_ENVELOPE_PROFILE_MISMATCH", f"row {row_number} ts_ms is invalid")
    require(isinstance(row["source"], dict), "RAW_ENVELOPE_PROFILE_MISMATCH", f"row {row_number} source is invalid")
    require(isinstance(row["transaction"], dict), "RAW_ENVELOPE_PROFILE_MISMATCH", f"row {row_number} transaction is invalid")

    source_fields = {
        "version",
        "connector",
        "name",
        "ts_ms",
        "snapshot",
        "db",
        "sequence",
        "schema",
        "table",
        "txId",
        "lsn",
        "xmin",
        "origin",
        "origin_lsn",
    }
    source = row["source"]
    require_exact_keys(source, source_fields, f"row {row_number} source")
    require(source["version"] == SOURCE_VERSION, "SOURCE_VERSION_MISMATCH", f"row {row_number} source version differs")
    require(source["connector"] == "postgresql", "SOURCE_IDENTITY_MISMATCH", f"row {row_number} connector differs")
    require(source["name"] == SOURCE_NAME, "SOURCE_IDENTITY_MISMATCH", f"row {row_number} source name differs")
    require(source["db"] == CANONICAL_SCHEMA, "SOURCE_IDENTITY_MISMATCH", f"row {row_number} database differs")
    require(source["schema"] == CANONICAL_SCHEMA, "SOURCE_IDENTITY_MISMATCH", f"row {row_number} schema differs")
    require(source["snapshot"] == "false", "SNAPSHOT_ROW_REJECTED", f"row {row_number} is not a live record")
    require(type(source["ts_ms"]) is int and source["ts_ms"] >= 0, "RAW_ENVELOPE_PROFILE_MISMATCH", f"row {row_number} source ts_ms is invalid")
    require_bounded_text(source["sequence"], "RAW_ENVELOPE_PROFILE_MISMATCH", f"row {row_number} source sequence", 128)
    require(type(source["txId"]) is int, "RAW_ENVELOPE_PROFILE_MISMATCH", f"row {row_number} source txId is invalid")
    require(type(source["lsn"]) is int, "RAW_ENVELOPE_PROFILE_MISMATCH", f"row {row_number} source lsn is invalid")
    require(source["xmin"] is None or type(source["xmin"]) is int, "RAW_ENVELOPE_PROFILE_MISMATCH", f"row {row_number} xmin is invalid")
    require(source["origin"] is None or isinstance(source["origin"], str), "RAW_ENVELOPE_PROFILE_MISMATCH", f"row {row_number} origin is invalid")
    require(source["origin_lsn"] is None or type(source["origin_lsn"]) is int, "RAW_ENVELOPE_PROFILE_MISMATCH", f"row {row_number} origin_lsn is invalid")

    table = source["table"]
    require(table in TABLES, "UNKNOWN_TABLE", f"row {row_number} table is unknown: {table!r}")
    transaction = row["transaction"]
    require_exact_keys(transaction, {"id", "total_order", "data_collection_order"}, f"row {row_number} transaction")
    require_bounded_text(
        transaction["id"], "OPAQUE_TRANSACTION_ID_INVALID", f"row {row_number} transaction id", MAX_DIAGNOSTIC_ID_BYTES
    )
    require(
        type(transaction["total_order"]) is int and transaction["total_order"] > 0,
        "ROW_ORDINAL_INVALID",
        f"row {row_number} total_order is invalid",
    )
    require(
        type(transaction["data_collection_order"]) is int and transaction["data_collection_order"] > 0,
        "ROW_ORDINAL_INVALID",
        f"row {row_number} collection order is invalid",
    )

    before: dict[str, Any] | None
    after: dict[str, Any] | None
    if row["op"] == "c":
        require(row["before"] is None, "FULL_IMAGE_INVALID", f"insert row {row_number} has a before image")
        before = None
        after = validate_image(row["after"], table, f"row {row_number} after")
    elif row["op"] == "u":
        before = validate_image(row["before"], table, f"row {row_number} before")
        after = validate_image(row["after"], table, f"row {row_number} after")
    else:
        before = validate_image(row["before"], table, f"row {row_number} before")
        require(row["after"] is None, "FULL_IMAGE_INVALID", f"delete row {row_number} has an after image")
        after = None

    image_for_key = after if after is not None else before
    assert image_for_key is not None
    primary_key = TABLES[table]["primary_key"]
    require_exact_keys(entry["key"], set(primary_key), f"row {row_number} key")
    for column in primary_key:
        require(
            entry["key"][column] == image_for_key[column],
            "FULL_IMAGE_INVALID",
            f"row {row_number} key differs from the complete row image at {redis_id}",
        )
    return {"entry": entry, "row": row, "table": table, "before": before, "after": after}


def validate_completed_frame(
    frame: dict[str, Any], cdc_seq: int, marker_state: dict[str, int]
) -> dict[str, Any]:
    begin_entry = frame["begin"]
    end_entry = frame["end"]
    rows = [validate_row(entry, index) for index, entry in enumerate(frame["rows"], start=1)]
    require(bool(rows), "TRANSACTION_FRAMING_INVALID", f"frame {begin_entry['redis_id']} contains no rows")

    total_orders = [item["row"]["transaction"]["total_order"] for item in rows]
    expected_total_orders = list(range(1, len(rows) + 1))
    require(
        total_orders == expected_total_orders,
        "ROW_ORDINAL_INVALID",
        f"frame {begin_entry['redis_id']} total_order is not contiguous: {total_orders}",
    )
    collection_orders: dict[str, list[int]] = defaultdict(list)
    for item in rows:
        collection_orders[item["table"]].append(item["row"]["transaction"]["data_collection_order"])
    for table, orders in collection_orders.items():
        require(
            orders == list(range(1, len(orders) + 1)),
            "ROW_ORDINAL_INVALID",
            f"frame {begin_entry['redis_id']} {table} collection order is not contiguous: {orders}",
        )

    end = end_entry["value"]
    require(type(end["event_count"]) is int, "END_COUNT_MISMATCH", "END event_count is not an integer")
    require(
        end["event_count"] == len(rows),
        "END_COUNT_MISMATCH",
        f"frame {begin_entry['redis_id']} END event_count={end['event_count']} rows={len(rows)}",
    )
    require(isinstance(end["data_collections"], list), "END_COUNT_MISMATCH", "END collections are invalid")
    end_counts: dict[str, int] = {}
    for index, item in enumerate(end["data_collections"]):
        require(isinstance(item, dict), "END_COUNT_MISMATCH", f"END collection {index} is not an object")
        require_exact_keys(item, {"data_collection", "event_count"}, f"END collection {index}", "END_COUNT_MISMATCH")
        collection = item["data_collection"]
        count = item["event_count"]
        require(isinstance(collection, str) and collection, "END_COUNT_MISMATCH", f"END collection {index} name is invalid")
        require(type(count) is int and count > 0, "END_COUNT_MISMATCH", f"END collection {index} count is invalid")
        require(collection not in end_counts, "END_COUNT_MISMATCH", f"duplicate END collection: {collection}")
        end_counts[collection] = count
    actual_counts = Counter(f"{CANONICAL_SCHEMA}.{item['table']}" for item in rows)
    require(
        end_counts == dict(actual_counts),
        "END_COUNT_MISMATCH",
        f"frame {begin_entry['redis_id']} END collection counts differ: expected={dict(actual_counts)} actual={end_counts}",
    )

    marker = rows[-1]
    require(marker["table"] == "session_txn", "MARKER_NOT_LAST", f"frame {begin_entry['redis_id']} final row is not session_txn")
    require(marker["row"]["op"] in {"c", "u"}, "MARKER_INVALID", "final marker is not an insert/update")
    marker_after = marker["after"]
    assert marker_after is not None
    session_id = marker_after["session_id"]
    txn_id = marker_after["txn_id"]
    require(txn_id > 0, "MARKER_INVALID", "marker txn_id is not positive")
    if marker["row"]["op"] == "c":
        require(session_id not in marker_state, "MARKER_CHAIN_INVALID", f"duplicate marker insert for {session_id}")
    else:
        marker_before = marker["before"]
        assert marker_before is not None
        require(marker_before["session_id"] == session_id, "MARKER_CHAIN_INVALID", "marker session changed on update")
        require(
            marker_state.get(session_id) == marker_before["txn_id"],
            "MARKER_CHAIN_INVALID",
            f"marker before-image does not match prior value for {session_id}",
        )
        require(txn_id > marker_before["txn_id"], "MARKER_CHAIN_INVALID", "marker txn_id did not increase")
    marker_state[session_id] = txn_id

    return {
        "frame_identity": begin_entry["redis_id"],
        "cdc_seq": cdc_seq,
        "end_redis_id": end_entry["redis_id"],
        "rows": len(rows),
        "marker": {"session_id": session_id, "txn_id": txn_id},
    }


def verify_contract(capture_path: Path, plugin: str, redis_batch_size: int = 1000) -> dict[str, Any]:
    require(plugin == "pgoutput", "PLUGIN_NOT_ADMITTED", f"primary contract requires pgoutput, got {plugin}")
    require(redis_batch_size > 0, "CONFIG_INVALID", "Redis batch size must be positive")
    metadata = load_capture_metadata(capture_path, plugin, redis_batch_size)
    entries = load_entries(capture_path)
    require(bool(entries), "TRANSACTION_FRAMING_INVALID", "Redis capture is empty")

    completed: list[dict[str, Any]] = []
    active: dict[str, Any] | None = None
    marker_state: dict[str, int] = {}
    for entry in entries:
        value = entry["value"]
        status = value.get("status")
        if status is not None:
            require(status in {"BEGIN", "END"}, "TRANSACTION_FRAMING_INVALID", f"unknown metadata status: {status!r}")
            validate_metadata_record(entry, status)
            if status == "BEGIN":
                require(
                    active is None,
                    "TRANSACTION_INTERLEAVING",
                    f"nested BEGIN at {entry['redis_id']} while frame {active['begin']['redis_id'] if active else '?'} is active",
                )
                active = {"begin": entry, "rows": []}
            else:
                require(active is not None, "TRANSACTION_FRAMING_INVALID", f"END outside a frame at {entry['redis_id']}")
                active["end"] = entry
                completed.append(validate_completed_frame(active, len(completed), marker_state))
                active = None
        else:
            require(active is not None, "TRANSACTION_FRAMING_INVALID", f"row outside a frame at {entry['redis_id']}")
            active["rows"].append(entry)
    require(active is None, "PARTIAL_DELIVERY", f"capture ended inside frame {active['begin']['redis_id'] if active else '?'}")

    markers = [frame["marker"] for frame in completed]
    if metadata is not None:
        expected = metadata["expected_completed_transactions"]
        require(
            isinstance(expected, list)
            and all(isinstance(item, dict) and set(item) == {"session_id", "txn_id"} for item in expected),
            "FIXTURE_METADATA_INVALID",
            "expected completed transaction order is malformed",
        )
        require(
            markers == expected,
            "COMPLETED_TRANSACTION_ORDER_MISMATCH",
            f"Redis completed transaction order differs: expected={expected} actual={markers}",
        )

    return {
        "entries": len(entries),
        "completed_transactions": len(completed),
        "rows": sum(frame["rows"] for frame in completed),
        "marker_last": True,
        "frames": completed,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("capture", type=Path)
    parser.add_argument("--plugin", default="pgoutput")
    parser.add_argument("--redis-batch-size", type=int, default=1000)
    parser.add_argument("--expect-violation")
    args = parser.parse_args()
    try:
        summary = verify_contract(args.capture, args.plugin, args.redis_batch_size)
    except ContractViolation as error:
        print(f"{error.code}: {error.diagnostic}", file=sys.stderr)
        if args.expect_violation == error.code:
            return 0
        return 1
    if args.expect_violation:
        print(f"expected {args.expect_violation}, but fixture passed", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
