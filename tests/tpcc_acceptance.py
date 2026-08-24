#!/usr/bin/env python3
"""Benchmark-neutral helpers for focused live TPC-C acceptance validators."""

from __future__ import annotations

import base64
import json
import os
import re
import struct
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PROTOCOL_ROOT = ROOT / "protocol"
sys.path.insert(0, str(PROTOCOL_ROOT / "tools"))

from tpcc_client_fixture import read_digest, rolling_digest  # noqa: E402,F401
from tpcc_codec import reject_duplicate_keys  # noqa: E402
from verify_protocol import (  # noqa: E402
    VerificationError,
    load_schema_validator,
    verify_record_schema,
)


STREAM_ID = re.compile(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)\Z")
DECIMAL = re.compile(r"DECIMAL\(([1-9][0-9]*),([0-9]+)\)\Z")
TEXT = re.compile(r"(VARCHAR|CHAR)\(([1-9][0-9]*)\)\Z")
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
# Registered acceptance backends mapped to their pinned connector contracts.
# Selection is data-driven from the run configuration; validators never branch
# on a backend name. MariaDB's connector repeats one transaction ID on BEGIN,
# rows, and END; the pinned YugabyteDB connector intentionally emits a
# different composite ID on every record, so IDs are only bounded diagnostics.
BACKEND_PROFILES = {
    "mariadb": {
        "source_id": "mariadb-main",
        "stable_connector_transaction_ids": True,
        # Exact recorded bootstrap-state bytes when the 256MiB limit trips on
        # the deterministic one-warehouse loader. The current reproducible
        # loader replaced the earlier wall-clock timestamp corpus.
        "bootstrap_exhaustion_bytes": 268435152,
    },
    "yugabytedb_ysql": {
        "source_id": "yugabytedb_ysql_main",
        "stable_connector_transaction_ids": False,
        "bootstrap_exhaustion_bytes": 268435152,
    },
}
BACKEND_SOURCE_IDS = {
    name: profile["source_id"] for name, profile in BACKEND_PROFILES.items()
}
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")
BENCHBASE_SECTIONS = (
    "Completed Transactions:",
    "Aborted Transactions:",
    "Rejected Transactions (Server Retry):",
    "Rejected Transactions (Retry Different):",
    "Unexpected SQL Errors:",
    "Unknown Status Transactions:",
)
BENCHBASE_TRANSACTION = re.compile(
    r"^com\.oltpbenchmark\.benchmarks\.tpcc\.procedures\.([A-Za-z]+)/[0-9]+"
    r"\s+\[\s*([0-9]+)\]"
)


class AcceptanceError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AcceptanceError(message)


def backend_profile(config: dict[str, Any]) -> dict[str, Any]:
    backend = config.get("backend")
    require(
        isinstance(backend, str) and backend in BACKEND_PROFILES,
        "config backend is not a registered acceptance backend",
    )
    return BACKEND_PROFILES[backend]


def expected_source_id(config: dict[str, Any]) -> str:
    return backend_profile(config)["source_id"]


def require_connector_transaction_id(
    actual: Any, expected: Any, profile: dict[str, Any], label: str
) -> None:
    if profile["stable_connector_transaction_ids"]:
        require(actual == expected, f"{label} connector transaction ID differs")
    else:
        require(
            isinstance(actual, str) and 0 < len(actual) <= 512,
            f"{label} connector transaction ID is not a bounded diagnostic",
        )


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


def validate_record_schemas(records: list[dict[str, Any]]) -> None:
    validator, _ = load_schema_validator()
    for index, record in enumerate(records):
        try:
            verify_record_schema(validator, record, f"acceptance record {index}")
        except VerificationError as error:
            raise AcceptanceError(str(error)) from error


def raw_cdc_records(path: Path) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    parsed: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    for index, (entry_id, fields) in enumerate(parse_xrange(path, "CDC tail")):
        require(len(fields) == 2 and all(isinstance(item, str) for item in fields), f"CDC tail[{index}] fields")
        key = strict_json(fields[0], f"CDC tail[{index}].key")
        envelope = strict_json(fields[1], f"CDC tail[{index}].envelope")
        require(isinstance(key, dict) and isinstance(envelope, dict), f"CDC tail[{index}] JSON shape")
        parsed.append((entry_id, key, envelope))
    return parsed


def one_status(records: list[dict[str, Any]], lifecycle: str) -> dict[str, Any]:
    matches = [record for record in records if record.get("lifecycle_state") == lifecycle]
    require(len(matches) == 1, f"status stream must contain one {lifecycle} record")
    return matches[0]


def validate_audited_status_and_metrics(
    config: dict[str, Any],
    run_id: str,
    status_records: list[dict[str, Any]],
    metrics: dict[str, Any],
    client_count: int,
    read_count: int,
    marker_only_count: int,
    write_count: int,
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
        and reconciliation["startup_schema_attested"] is True
        and reconciliation["final_schema_attested"] is True
        and isinstance(reconciliation["schema_attestation_digest"], str)
        and re.fullmatch(r"[0-9a-f]{64}", reconciliation["schema_attestation_digest"])
        is not None
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
        and joiner.get("marker_only_workload_transactions") == marker_only_count
        and joiner.get("joined_read_events") == read_count
        and joiner.get("joined_write_events") == write_count,
        "joiner read/write counters differ",
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
    for field, value in reconciliation.items():
        require(measured_reconciliation.get(field) == value, f"metrics reconciliation {field}")
    return final, load_finished["last_cdc_stream_id"]


def benchbase_outcomes(
    path: Path,
    transaction_type: str,
    successful: int,
    terminal_aborts: int,
    retries: int,
    server_rejections: int | None = None,
) -> dict[str, int]:
    try:
        text = path.read_text(encoding="utf-8", errors="strict")
    except (OSError, UnicodeError) as error:
        raise AcceptanceError(f"cannot read BenchBase log: {error}") from error
    lines = [ANSI_ESCAPE.sub("", line).strip() for line in text.splitlines()]
    positions: dict[str, int] = {}
    for heading in BENCHBASE_SECTIONS:
        matches = [index for index, line in enumerate(lines) if line == heading]
        require(len(matches) == 1, f"BenchBase log must contain one {heading} section")
        positions[heading] = matches[0]
    require(
        [positions[heading] for heading in BENCHBASE_SECTIONS]
        == sorted(positions.values()),
        "BenchBase outcome sections are out of order",
    )
    parsed: dict[str, dict[str, int]] = {}
    empty: dict[str, bool] = {}
    for section_index, heading in enumerate(BENCHBASE_SECTIONS):
        start = positions[heading] + 1
        end = (
            positions[BENCHBASE_SECTIONS[section_index + 1]]
            if section_index + 1 < len(BENCHBASE_SECTIONS)
            else len(lines)
        )
        rows: dict[str, int] = {}
        saw_empty = False
        for line in lines[start:end]:
            if line == "<EMPTY>":
                saw_empty = True
                continue
            match = BENCHBASE_TRANSACTION.match(line)
            if match is not None:
                name, count = match.groups()
                require(name not in rows, f"BenchBase {heading} repeats transaction type")
                rows[name] = int(count)
        require(not (saw_empty and rows), f"BenchBase {heading} mixes EMPTY and rows")
        parsed[heading] = rows
        empty[heading] = saw_empty
    completed = parsed["Completed Transactions:"]
    require(
        completed.get(transaction_type) == successful
        and all(count == 0 for name, count in completed.items() if name != transaction_type),
        f"BenchBase completed counts differ from published {transaction_type} clients",
    )
    expected_aborts = {} if terminal_aborts == 0 else {transaction_type: terminal_aborts}
    require(
        parsed["Aborted Transactions:"] == expected_aborts
        and empty["Aborted Transactions:"] == (terminal_aborts == 0),
        "BenchBase aborted count differs from SessionDone terminal aborts",
    )
    expected_server_rejections = retries if server_rejections is None else server_rejections
    expected_retries = (
        {}
        if expected_server_rejections == 0
        else {transaction_type: expected_server_rejections}
    )
    require(
        parsed["Rejected Transactions (Server Retry):"] == expected_retries
        and empty["Rejected Transactions (Server Retry):"]
        == (expected_server_rejections == 0),
        "BenchBase server-retry count differs from SessionDone retries",
    )
    for heading in (
        "Rejected Transactions (Retry Different):",
        "Unexpected SQL Errors:",
        "Unknown Status Transactions:",
    ):
        require(empty[heading] and not parsed[heading], f"BenchBase {heading} is not empty")
    return {
        "completed": successful,
        "terminal_aborts": terminal_aborts,
        "server_retries": expected_server_rejections,
        "retry_different": 0,
        "unexpected_sql_errors": 0,
        "unknown_status": 0,
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
