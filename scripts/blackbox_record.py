#!/usr/bin/env python3
"""Build and validate coordinator-owned black-box protocol records."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import struct
import sys
from pathlib import Path
from typing import Any, Iterable

from jsonschema import Draft202012Validator, RefResolver


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_ROOT = ROOT / "protocol" / "schema"
ADAPTER_ROOT = ROOT / "protocol" / "adapters"
BACKEND_ROOT = ROOT / "protocol" / "backends"

UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z"
)
SESSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]*\Z")
CANONICAL_IDENTIFIER_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
ERROR_CODE_PATTERN = re.compile(r"[A-Z][A-Z0-9_]{0,95}\Z")
ADAPTER_NAME_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,63}\Z")

BOOTSTRAP_DOMAIN = b"db-monitoring/bootstrap-state-digest\x00"
MAX_SAFE_INTEGER = 9_007_199_254_740_991
LIFECYCLE_ORDER = (
    "STARTING",
    "START_FENCE_SEEN",
    "BOOTSTRAP",
    "LOAD_FINISHED_SEEN",
    "RUNNING",
    "DRAINING",
    "RECONCILED",
    "FINAL",
)


class RecordError(ValueError):
    """The requested record or scan is outside the frozen contract."""


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RecordError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RecordError(message)


def validate_uuid(value: str, label: str = "run_id") -> str:
    require(UUID_PATTERN.fullmatch(value) is not None, f"{label} is not a lowercase UUID")
    return value


def validate_session_id(value: str) -> str:
    require(1 <= len(value) <= 256, "session_id length is outside 1..256")
    require(SESSION_PATTERN.fullmatch(value) is not None, f"invalid session_id: {value}")
    return value


def validate_identifier(value: str, label: str) -> str:
    require(
        CANONICAL_IDENTIFIER_PATTERN.fullmatch(value) is not None,
        f"{label} is not a canonical lowercase identifier: {value}",
    )
    return value


def validate_adapter_name(value: str, label: str) -> str:
    require(
        ADAPTER_NAME_PATTERN.fullmatch(value) is not None,
        f"{label} must match [A-Za-z][A-Za-z0-9_]{{0,63}}: {value}",
    )
    return value


def load_schema_documents() -> list[dict[str, Any]]:
    return [
        json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicate_keys)
        for path in sorted(SCHEMA_ROOT.glob("*.schema.json"))
    ]


def document_validator(schema_name: str) -> Draft202012Validator:
    schemas = load_schema_documents()
    schema = next(
        candidate
        for candidate in schemas
        if candidate["$id"].endswith(f"/{schema_name}")
    )
    resolver = RefResolver.from_schema(
        schema, store={candidate["$id"]: candidate for candidate in schemas}
    )
    return Draft202012Validator(schema, resolver=resolver)


def validate_document(
    validator: Draft202012Validator, document: dict[str, Any], label: str
) -> None:
    errors = sorted(
        validator.iter_errors(document), key=lambda error: list(error.absolute_path)
    )
    if errors:
        rendered = "; ".join(
            f"{'/'.join(map(str, error.absolute_path)) or '<root>'}: {error.message}"
            for error in errors
        )
        raise RecordError(f"{label} fails schema validation: {rendered}")


def load_adapter_registry() -> dict[str, dict[str, Any]]:
    validator = document_validator("adapter-contract.schema.json")
    registry: dict[str, dict[str, Any]] = {}
    for path in sorted(ADAPTER_ROOT.glob("*.json")):
        contract = json.loads(
            path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicate_keys
        )
        validate_document(validator, contract, f"adapter contract {path.name}")
        benchmark = validate_adapter_name(contract["benchmark"], "benchmark")
        require(path.stem == benchmark, f"adapter filename does not match benchmark: {path.name}")
        require(benchmark not in registry, f"duplicate adapter contract: {benchmark}")
        table_names = [table["name"] for table in contract["tables"]]
        require(
            table_names == contract["application_tables"],
            f"adapter {benchmark} table definitions disagree with its roster",
        )
        require(
            len(table_names) == len(set(table_names)),
            f"adapter {benchmark} has duplicate application tables",
        )
        registry[benchmark] = contract
    return registry


def load_backend_registry() -> dict[str, dict[str, Any]]:
    validator = document_validator("backend-contract.schema.json")
    registry: dict[str, dict[str, Any]] = {}
    for path in sorted(BACKEND_ROOT.glob("*.json")):
        contract = json.loads(
            path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicate_keys
        )
        validate_document(validator, contract, f"backend contract {path.name}")
        backend = validate_adapter_name(contract["backend"], "backend")
        require(path.stem == backend, f"backend filename does not match name: {path.name}")
        require(backend not in registry, f"duplicate backend contract: {backend}")
        registry[backend] = contract
    return registry


def select_adapter(benchmark: str) -> dict[str, Any]:
    validate_adapter_name(benchmark, "benchmark")
    adapter = load_adapter_registry().get(benchmark)
    require(adapter is not None, f"unregistered benchmark adapter: {benchmark}")
    return adapter


def select_backend(backend: str) -> dict[str, Any]:
    validate_adapter_name(backend, "backend")
    contract = load_backend_registry().get(backend)
    require(contract is not None, f"unregistered backend contract: {backend}")
    return contract


def validate_application_roster(
    adapter: dict[str, Any], supplied_tables: list[str] | None
) -> list[str]:
    contract_tables = adapter["application_tables"]
    if not supplied_tables:
        return contract_tables
    require(
        len(supplied_tables) == len(contract_tables)
        and set(supplied_tables) == set(contract_tables),
        f"application table roster differs from adapter {adapter['benchmark']} contract",
    )
    return contract_tables


def validate_transaction_membership(adapter: dict[str, Any], transaction_type: str) -> str:
    validate_adapter_name(transaction_type, "transaction type")
    require(
        transaction_type in adapter["transaction_types"],
        f"transaction type {transaction_type} is not owned by adapter {adapter['benchmark']}",
    )
    return transaction_type


def common_table_contracts() -> list[dict[str, Any]]:
    return [
        {
            "name": "benchbase.checker_run_fence",
            "key_columns": ["run_id", "phase"],
            "columns": [
                {
                    "name": "run_id",
                    "sql_type": "CHAR(36)",
                    "nullable": False,
                    "character_set": "ascii",
                    "collation": "ascii_bin",
                },
                {
                    "name": "phase",
                    "sql_type": "VARCHAR(32)",
                    "nullable": False,
                    "character_set": "ascii",
                    "collation": "ascii_bin",
                },
                {
                    "name": "nonce",
                    "sql_type": "CHAR(36)",
                    "nullable": False,
                    "character_set": "ascii",
                    "collation": "ascii_bin",
                },
            ],
        },
        {
            "name": "benchbase.session_txn",
            "key_columns": ["session_id"],
            "columns": [
                {
                    "name": "session_id",
                    "sql_type": "VARCHAR(64)",
                    "nullable": False,
                    "character_set": "ascii",
                    "collation": "ascii_bin",
                },
                {
                    "name": "txn_id",
                    "sql_type": "BIGINT",
                    "nullable": False,
                },
            ],
        },
    ]


def parse_schema_metadata(text: str) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "settings": {},
        "database": None,
        "tables": {},
    }
    for line_number, line in enumerate(text.splitlines(), start=1):
        require(line != "", f"schema metadata contains empty line {line_number}")
        fields = line.split("\t")
        record_type = fields[0]
        if record_type == "SETTING":
            require(len(fields) == 3, f"schema SETTING line {line_number} has wrong arity")
            key, value = fields[1:]
            require(key not in metadata["settings"], f"duplicate schema setting: {key}")
            metadata["settings"][key] = value
        elif record_type == "DATABASE":
            require(len(fields) == 4, f"schema DATABASE line {line_number} has wrong arity")
            require(metadata["database"] is None, "duplicate schema database record")
            metadata["database"] = {
                "name": fields[1],
                "character_set": fields[2],
                "collation": fields[3],
            }
        elif record_type == "TABLE":
            require(len(fields) == 4, f"schema TABLE line {line_number} has wrong arity")
            name = fields[1]
            require(name not in metadata["tables"], f"duplicate schema table: {name}")
            metadata["tables"][name] = {
                "engine": fields[2],
                "collation": fields[3],
                "columns": [],
                "primary_key": [],
            }
        elif record_type == "COLUMN":
            require(len(fields) == 13, f"schema COLUMN line {line_number} has wrong arity")
            table_name = fields[1]
            table = metadata["tables"].get(table_name)
            require(table is not None, f"column precedes or names unknown table: {table_name}")
            try:
                ordinal = int(fields[3], 10)
            except ValueError as error:
                raise RecordError(f"column ordinal is not an integer on line {line_number}") from error
            require(ordinal > 0, f"column ordinal is not positive on line {line_number}")
            table["columns"].append(
                {
                    "name": fields[2],
                    "ordinal": ordinal,
                    "data_type": fields[4].lower(),
                    "nullable": fields[5],
                    "numeric_precision": fields[6],
                    "numeric_scale": fields[7],
                    "character_length": fields[8],
                    "datetime_precision": fields[9],
                    "character_set": fields[10],
                    "collation": fields[11],
                    "extra": fields[12],
                }
            )
        elif record_type == "PRIMARY":
            require(len(fields) == 4, f"schema PRIMARY line {line_number} has wrong arity")
            table_name = fields[1]
            table = metadata["tables"].get(table_name)
            require(table is not None, f"primary key names unknown table: {table_name}")
            try:
                ordinal = int(fields[3], 10)
            except ValueError as error:
                raise RecordError(f"primary ordinal is not an integer on line {line_number}") from error
            require(ordinal > 0, f"primary ordinal is not positive on line {line_number}")
            table["primary_key"].append((ordinal, fields[2]))
        else:
            raise RecordError(f"unknown schema metadata record on line {line_number}: {record_type}")
    require(metadata["database"] is not None, "schema metadata lacks database record")
    return metadata


def normalized_isolation(value: str) -> str:
    return value.upper().replace("-", "_").replace(" ", "_")


def require_schema_settings(metadata: dict[str, Any], backend: dict[str, Any]) -> None:
    server = backend["server_defaults"]
    session = backend["session_requirements"]
    expected = {
        "server.character_set": server["character_set"],
        "server.collation": server["collation"],
        "server.time_zone": server["time_zone"],
        "server.sql_mode": server["sql_mode"],
        "server.transaction_isolation": server["transaction_isolation"],
        "server.snapshot_isolation": "1" if server["snapshot_isolation"] else "0",
        "session.character_set_client": session["character_set_client"],
        "session.character_set_connection": session["character_set_connection"],
        "session.character_set_results": session["character_set_results"],
        "session.collation_connection": session["collation_connection"],
        "session.time_zone": session["time_zone"],
        "session.sql_mode": session["sql_mode"],
        "session.transaction_isolation": session["transaction_isolation"],
        "session.snapshot_isolation": "1" if session["snapshot_isolation"] else "0",
    }
    actual = dict(metadata["settings"])
    for isolation_key in (
        "server.transaction_isolation",
        "session.transaction_isolation",
    ):
        if isolation_key in actual:
            actual[isolation_key] = normalized_isolation(actual[isolation_key])
    require(actual == expected, "MariaDB server/session settings differ from backend contract")


SQL_TYPE_PATTERN = re.compile(
    r"(?:(INT|BIGINT|FLOAT|TIMESTAMP)|(DECIMAL)\(([1-9][0-9]*),([0-9]+)\)|"
    r"(VARCHAR|CHAR)\(([1-9][0-9]*)\))\Z"
)


def require_column_contract(
    actual: dict[str, Any],
    expected: dict[str, Any],
    ordinal: int,
    default_character_set: str,
    default_collation: str,
    label: str,
) -> None:
    require(actual["name"] == expected["name"], f"{label}: column name/order mismatch")
    require(actual["ordinal"] == ordinal, f"{label}: column ordinal mismatch")
    require(
        actual["nullable"] == ("YES" if expected["nullable"] else "NO"),
        f"{label}: column nullability mismatch",
    )
    match = SQL_TYPE_PATTERN.fullmatch(expected["sql_type"])
    require(match is not None, f"{label}: unsupported contract SQL type")
    simple_type, decimal_type, precision, scale, string_type, length = match.groups()
    expected_data_type = (simple_type or decimal_type or string_type).lower()
    require(actual["data_type"] == expected_data_type, f"{label}: data type mismatch")
    if decimal_type is not None:
        require(
            actual["numeric_precision"] == precision
            and actual["numeric_scale"] == scale,
            f"{label}: decimal precision/scale mismatch",
        )
    if string_type is not None:
        require(actual["character_length"] == length, f"{label}: character length mismatch")
        expected_character_set = expected.get("character_set", default_character_set)
        expected_collation = expected.get("collation", default_collation)
        require(
            actual["character_set"] == expected_character_set
            and actual["collation"] == expected_collation,
            f"{label}: character set/collation mismatch",
        )
    expected_auto_increment = expected.get("generated") == "auto_increment"
    actual_auto_increment = "auto_increment" in actual["extra"].split(",")
    require(
        actual_auto_increment == expected_auto_increment,
        f"{label}: auto-increment metadata mismatch",
    )


def validate_schema_metadata(
    adapter: dict[str, Any], backend: dict[str, Any], metadata_text: str
) -> None:
    metadata = parse_schema_metadata(metadata_text)
    require_schema_settings(metadata, backend)
    database = metadata["database"]
    require(database["name"] == "benchbase", "schema metadata names wrong database")
    require(
        database["character_set"] == backend["database_defaults"]["character_set"]
        and database["collation"] == backend["database_defaults"]["collation"],
        "benchbase database character set/collation differs from backend contract",
    )

    common_tables = common_table_contracts()
    expected_tables = adapter["tables"] + common_tables
    expected_by_name = {table["name"]: table for table in expected_tables}
    require(
        set(metadata["tables"]) == set(expected_by_name),
        "database table roster differs from selected adapter plus common tables",
    )
    table_requirements = backend["application_table_requirements"]
    for table_name, expected_table in expected_by_name.items():
        actual_table = metadata["tables"][table_name]
        require(
            actual_table["engine"].lower() == table_requirements["engine"].lower(),
            f"{table_name}: storage engine mismatch",
        )
        require(
            actual_table["collation"] == table_requirements["collation"],
            f"{table_name}: table collation mismatch",
        )
        actual_columns = sorted(actual_table["columns"], key=lambda column: column["ordinal"])
        expected_columns = expected_table["columns"]
        require(
            len(actual_columns) == len(expected_columns),
            f"{table_name}: column count mismatch",
        )
        for ordinal, (actual_column, expected_column) in enumerate(
            zip(actual_columns, expected_columns, strict=True), start=1
        ):
            require_column_contract(
                actual_column,
                expected_column,
                ordinal,
                table_requirements["character_set"],
                table_requirements["collation"],
                f"{table_name}.{expected_column['name']}",
            )
        primary_key = [
            name for _, name in sorted(actual_table["primary_key"], key=lambda item: item[0])
        ]
        require(
            primary_key == expected_table["key_columns"],
            f"{table_name}: primary-key order mismatch",
        )


def length_prefix(value: bytes) -> bytes:
    require(len(value) <= 0xFFFFFFFF, "length-prefixed value exceeds U32")
    return struct.pack(">I", len(value)) + value


def canonical_ycsb_key(schema: str, table: str, key: int) -> bytes:
    validate_identifier(schema, "schema")
    validate_identifier(table, "table")
    require(-0x80000000 <= key <= 0x7FFFFFFF, f"YCSB key is outside signed I32: {key}")
    return length_prefix(schema.encode("ascii")) + length_prefix(table.encode("ascii")) + struct.pack(">i", key)


def decode_present_field(value: str, line_number: int, field_number: int) -> bytes:
    encoded = value[1:]
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (ValueError, UnicodeEncodeError) as error:
        raise RecordError(
            f"scan line {line_number} FIELD{field_number} has invalid Base64: {error}"
        ) from error
    require(
        base64.b64encode(decoded).decode("ascii") == encoded,
        f"scan line {line_number} FIELD{field_number} has noncanonical Base64",
    )
    try:
        decoded.decode("utf-8", "strict")
    except UnicodeDecodeError as error:
        raise RecordError(
            f"scan line {line_number} FIELD{field_number} is not strict UTF-8"
        ) from error
    return decoded


def canonical_ycsb_value(tokens: list[str], line_number: int) -> bytes:
    require(len(tokens) == 10, f"scan line {line_number} does not contain ten fields")
    result = bytearray()
    for field_number, token in enumerate(tokens, start=1):
        if token == "N":
            result.append(0)
            continue
        require(
            token.startswith("P"),
            f"scan line {line_number} FIELD{field_number} lacks N/P state tag",
        )
        decoded = decode_present_field(token, line_number, field_number)
        result.append(1)
        result.extend(length_prefix(decoded))
    return bytes(result)


def scan_rows(path: Path, schema: str, table: str) -> list[tuple[bytes, bytes]]:
    try:
        text = path.read_text(encoding="utf-8", errors="strict")
    except (OSError, UnicodeError) as error:
        raise RecordError(f"cannot read bootstrap scan {path}: {error}") from error

    rows: list[tuple[bytes, bytes]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        require(line != "", f"bootstrap scan contains an empty line at {line_number}")
        columns = line.split("\t")
        require(len(columns) == 11, f"scan line {line_number} has {len(columns)} columns, expected 11")
        try:
            key_number = int(columns[0], 10)
        except ValueError as error:
            raise RecordError(f"scan line {line_number} has a non-integer YCSB key") from error
        rows.append(
            (
                canonical_ycsb_key(schema, table, key_number),
                canonical_ycsb_value(columns[1:], line_number),
            )
        )

    keys = [key for key, _ in rows]
    require(len(keys) == len(set(keys)), "bootstrap scan contains a duplicate canonical key")
    rows.sort(key=lambda row: row[0])
    return rows


def bootstrap_digest(rows: Iterable[tuple[bytes, bytes]]) -> tuple[int, str]:
    materialized = list(rows)
    require(len(materialized) <= MAX_SAFE_INTEGER, "bootstrap row count exceeds safe JSON integer")
    preimage = bytearray(BOOTSTRAP_DOMAIN)
    preimage.extend(struct.pack(">Q", len(materialized)))
    for key, value in materialized:
        preimage.extend(length_prefix(key))
        preimage.extend(length_prefix(value))
    return len(materialized), hashlib.sha256(preimage).hexdigest()


def dump_record(record: dict[str, Any]) -> None:
    print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


def build_run_manifest(args: argparse.Namespace) -> dict[str, Any]:
    run_id = validate_uuid(args.run_id)
    adapter = select_adapter(args.benchmark)
    missing_capabilities = sorted(
        name
        for name, available in adapter["runtime_capabilities"].items()
        if not available
    )
    require(
        not missing_capabilities,
        f"adapter {args.benchmark} runtime is incomplete: {','.join(missing_capabilities)}",
    )
    require(
        args.declared_isolation in adapter["declared_isolations"],
        f"declared isolation is unsupported by adapter {args.benchmark}",
    )
    require(1 <= len(args.backend_source_id) <= 256, "backend_source_id length is outside 1..256")
    sessions = [validate_session_id(value) for value in args.session_id]
    require(sessions, "at least one expected session is required")
    require(len(sessions) == len(set(sessions)), "expected session IDs must be unique")
    application_tables = validate_application_roster(adapter, args.application_table)
    return {
        "kind": "run_manifest",
        "run_id": run_id,
        "backend": "mariadb",
        "backend_source_id": args.backend_source_id,
        "declared_isolation": args.declared_isolation,
        "benchmark": adapter["benchmark"],
        "expected_session_ids": sessions,
        "application_table_allowlist": application_tables,
        "transaction_marker_table": "benchbase.session_txn",
        "control_table_allowlist": ["benchbase.checker_run_fence"],
        "result_scope": "point_transaction_soww",
    }


def build_bootstrap_manifest(args: argparse.Namespace) -> dict[str, Any]:
    run_id = validate_uuid(args.run_id)
    adapter = select_adapter(args.benchmark)
    require(
        adapter["runtime_capabilities"]["bootstrap_scan"],
        f"adapter {args.benchmark} bootstrap scanner is not implemented",
    )
    validate_identifier(args.schema, "schema")
    validate_identifier(args.table, "table")
    table_name = f"{args.schema}.{args.table}"
    validate_application_roster(adapter, [table_name])
    rows = scan_rows(Path(args.scan_file), args.schema, args.table)
    row_count, digest = bootstrap_digest(rows)
    return {
        "kind": "bootstrap_manifest",
        "run_id": run_id,
        "table_row_counts": [{"table": table_name, "row_count": row_count}],
        "canonical_state_digest": digest,
    }


def inspect_adapter_contract(args: argparse.Namespace) -> dict[str, Any]:
    adapter = select_adapter(args.benchmark)
    if args.transaction_type is not None:
        validate_transaction_membership(adapter, args.transaction_type)
    validate_application_roster(adapter, args.application_table)
    return adapter


def inspect_backend_contract(args: argparse.Namespace) -> dict[str, Any]:
    return select_backend(args.backend)


def check_schema_contract(args: argparse.Namespace) -> None:
    adapter = select_adapter(args.benchmark)
    backend = select_backend(args.backend)
    validate_schema_metadata(adapter, backend, sys.stdin.read())


def build_run_failed(args: argparse.Namespace) -> dict[str, Any]:
    record: dict[str, Any] = {
        "kind": "run_failed",
        "run_id": validate_uuid(args.run_id),
        "phase": args.phase,
        "reason_code": args.reason_code,
        "diagnostic": args.diagnostic,
    }
    require(ERROR_CODE_PATTERN.fullmatch(args.reason_code) is not None, "invalid reason_code")
    result_code_document = (ROOT / "protocol" / "result_error_codes.md").read_text(
        encoding="utf-8"
    )
    require("## Error codes" in result_code_document, "result error-code section is missing")
    error_code_section = result_code_document.split("## Error codes", maxsplit=1)[1]
    documented_codes = set(
        re.findall(
            r"^\| `([A-Z][A-Z0-9_]*)` \|",
            error_code_section,
            re.MULTILINE,
        )
    )
    require(args.reason_code in documented_codes, "reason_code is not in result_error_codes.md")
    require(1 <= len(args.diagnostic) <= 4096, "diagnostic length is outside 1..4096")
    return record


def status_validator() -> Draft202012Validator:
    return document_validator("monitor-status.schema.json")


def validate_final_verdict(record: dict[str, Any], expected_sessions: int) -> None:
    if record["lifecycle_state"] != "FINAL" or record["outcome"] == "INVALID":
        return
    reconciliation = record["reconciliation"]
    require(reconciliation["evidence_reconciled"], "final verdict has unreconciled evidence")
    require(
        reconciliation["bootstrap_manifest_validated"],
        "final verdict lacks validated bootstrap state",
    )
    require(reconciliation["end_fence_seen"], "final verdict lacks END_FENCE")
    require(
        reconciliation["expected_sessions"] == expected_sessions,
        "final verdict expected-session count differs from coordinator roster",
    )
    require(
        reconciliation["completed_sessions"] == expected_sessions,
        "final verdict has incomplete sessions",
    )
    require(
        reconciliation["accepted_client_transactions"]
        == reconciliation["joined_workload_transactions"],
        "final verdict client/joined transaction counts differ",
    )
    require(
        reconciliation["bootstrap_cdc_transactions"]
        == reconciliation["bootstrap_checker_transactions"],
        "final verdict bootstrap transaction counts differ",
    )
    require(
        reconciliation["phase_anchor_transactions"] == expected_sessions,
        "final verdict phase-anchor count differs from expected sessions",
    )
    expected_checker_transactions = (
        reconciliation["joined_workload_transactions"]
        + reconciliation["bootstrap_checker_transactions"]
        + reconciliation["phase_anchor_transactions"]
    )
    require(
        reconciliation["checker_consumed_transactions"] == expected_checker_transactions,
        "final verdict checker transaction algebra differs",
    )
    require(
        reconciliation["unmatched_client_records"] == 0
        and reconciliation["unmatched_workload_cdc_slots"] == 0
        and reconciliation["ambiguous_attempts"] == 0,
        "final verdict retains incomplete or ambiguous evidence",
    )
    require(record["pending_cdc_slots"] == 0, "final verdict retains pending CDC slots")
    require(
        record["assembled_cdc_transactions"]
        == reconciliation["bootstrap_cdc_transactions"]
        + reconciliation["joined_workload_transactions"]
        + 3,
        "final verdict assembled CDC counter differs from bootstrap/workload/fences",
    )
    require(
        record["joined_transactions"] == reconciliation["joined_workload_transactions"],
        "final verdict joined counters differ",
    )
    require(
        record["checked_transactions"] == reconciliation["checker_consumed_transactions"],
        "final verdict checked counters differ",
    )
    require(
        record["unmatched_client_records"] == reconciliation["unmatched_client_records"],
        "final verdict unmatched-client counters differ",
    )


def classify_status(args: argparse.Namespace) -> tuple[str, str]:
    require(args.expected_sessions > 0, "expected session count must be positive")
    try:
        record = json.loads(sys.stdin.read(), object_pairs_hook=reject_duplicate_keys)
    except json.JSONDecodeError as error:
        raise RecordError(f"monitor status is not valid JSON: {error}") from error

    errors = sorted(status_validator().iter_errors(record), key=lambda error: list(error.absolute_path))
    if errors:
        rendered = "; ".join(
            f"{'/'.join(map(str, error.absolute_path)) or '<root>'}: {error.message}"
            for error in errors
        )
        raise RecordError(f"monitor status fails schema validation: {rendered}")
    require(record["run_id"] == validate_uuid(args.run_id), "monitor status run_id mismatch")
    validate_final_verdict(record, args.expected_sessions)
    current_lifecycle = record["lifecycle_state"]
    current_rank = LIFECYCLE_ORDER.index(current_lifecycle)
    if args.previous_lifecycle:
        previous_rank = LIFECYCLE_ORDER.index(args.previous_lifecycle)
        require(current_rank >= previous_rank, "monitor lifecycle state regressed")
    if current_lifecycle == args.lifecycle:
        return "match", current_lifecycle
    if current_lifecycle == "FINAL":
        return "terminal", current_lifecycle
    required_rank = LIFECYCLE_ORDER.index(args.lifecycle)
    require(current_rank < required_rank, f"monitor skipped required lifecycle state {args.lifecycle}")
    return "continue", current_lifecycle


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    manifest = subparsers.add_parser("run-manifest", help="emit a RunManifest")
    manifest.add_argument("--run-id", required=True)
    manifest.add_argument("--backend-source-id", required=True)
    manifest.add_argument(
        "--declared-isolation", choices=("READ_COMMITTED", "REPEATABLE_READ"), required=True
    )
    manifest.add_argument("--benchmark", default="ycsb")
    manifest.add_argument("--session-id", action="append", default=[])
    manifest.add_argument("--application-table", action="append", default=[])

    bootstrap = subparsers.add_parser("bootstrap-manifest", help="emit a BootstrapManifest")
    bootstrap.add_argument("--run-id", required=True)
    bootstrap.add_argument("--benchmark", default="ycsb")
    bootstrap.add_argument("--schema", default="benchbase")
    bootstrap.add_argument("--table", default="usertable")
    bootstrap.add_argument("--scan-file", required=True)

    adapter = subparsers.add_parser(
        "adapter-contract", help="validate and emit one shared adapter contract"
    )
    adapter.add_argument("--benchmark", required=True)
    adapter.add_argument("--transaction-type")
    adapter.add_argument("--application-table", action="append", default=[])

    backend = subparsers.add_parser(
        "backend-contract", help="validate and emit one shared backend contract"
    )
    backend.add_argument("--backend", default="mariadb")

    schema_check = subparsers.add_parser(
        "schema-check", help="validate server/session/database/table metadata"
    )
    schema_check.add_argument("--backend", default="mariadb")
    schema_check.add_argument("--benchmark", required=True)

    failed = subparsers.add_parser("run-failed", help="emit a coordinator RunFailed record")
    failed.add_argument("--run-id", required=True)
    failed.add_argument(
        "--phase",
        choices=(
            "CONFIGURATION",
            "CONNECTION_SETUP",
            "ATTEMPT_BODY",
            "ROLLBACK",
            "COMMIT",
            "MARKER_WRITE",
            "REDIS_PUBLICATION",
            "SESSION_SHUTDOWN",
            "COORDINATOR",
        ),
        required=True,
    )
    failed.add_argument("--reason-code", required=True)
    failed.add_argument("--diagnostic", required=True)

    status = subparsers.add_parser("status-action", help="validate and classify one MonitorStatus")
    status.add_argument("--run-id", required=True)
    status.add_argument("--expected-sessions", type=int, required=True)
    status.add_argument("--previous-lifecycle", choices=LIFECYCLE_ORDER)
    status.add_argument(
        "--lifecycle",
        choices=(
            "STARTING",
            "START_FENCE_SEEN",
            "BOOTSTRAP",
            "LOAD_FINISHED_SEEN",
            "RUNNING",
            "DRAINING",
            "RECONCILED",
            "FINAL",
        ),
        required=True,
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "run-manifest":
            dump_record(build_run_manifest(args))
        elif args.command == "bootstrap-manifest":
            dump_record(build_bootstrap_manifest(args))
        elif args.command == "adapter-contract":
            dump_record(inspect_adapter_contract(args))
        elif args.command == "backend-contract":
            dump_record(inspect_backend_contract(args))
        elif args.command == "schema-check":
            check_schema_contract(args)
        elif args.command == "run-failed":
            dump_record(build_run_failed(args))
        elif args.command == "status-action":
            action, lifecycle = classify_status(args)
            print(f"{action}\t{lifecycle}")
        else:  # pragma: no cover - argparse owns this invariant
            raise RecordError(f"unsupported command: {args.command}")
    except (OSError, RecordError, UnicodeError, json.JSONDecodeError) as error:
        print(f"black-box record error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
