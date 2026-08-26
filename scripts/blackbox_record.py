#!/usr/bin/env python3
"""Build and validate coordinator-owned black-box protocol records."""

from __future__ import annotations

import argparse
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
sys.path.insert(0, str(ROOT / "protocol" / "tools"))
from bootstrap_adapter_registry import (  # noqa: E402
    BootstrapAdapterError,
    BootstrapAdapterRegistry,
)
from backend_sql_renderer import (  # noqa: E402
    BackendSqlRendererError,
    BackendSqlRendererRegistry,
)

UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z"
)
SESSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]*\Z")
CANONICAL_IDENTIFIER_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
ERROR_CODE_PATTERN = re.compile(r"[A-Z][A-Z0-9_]{0,95}\Z")
ADAPTER_NAME_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,63}\Z")

BOOTSTRAP_DOMAIN = b"db-monitoring/bootstrap-state-digest\x00"
SCHEMA_ATTESTATION_DOMAIN = b"db-monitoring/schema-attestation\x00"
MAX_SAFE_INTEGER = 9_007_199_254_740_991
MAX_LOADER_BATCH_SIZE = 2_147_483_647
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


def adapter_runtime_readiness(adapter: dict[str, Any]) -> dict[str, Any]:
    capabilities = adapter["runtime_capabilities"]
    monitor_requirements = ("cdc_normalization", "bootstrap_scan")
    end_to_end_requirements = ("client_trace", *monitor_requirements)
    missing_for_monitor = [name for name in monitor_requirements if not capabilities[name]]
    missing_for_end_to_end = [name for name in end_to_end_requirements if not capabilities[name]]
    return {
        "benchmark": adapter["benchmark"],
        "bootstrap_ready": capabilities["bootstrap_scan"],
        "monitor_ready": not missing_for_monitor,
        "client_ready": capabilities["client_trace"],
        "end_to_end_ready": not missing_for_end_to_end,
        "missing_for_monitor": missing_for_monitor,
        "missing_for_end_to_end": missing_for_end_to_end,
    }


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
            "key_columns": ["session_id", "txn_id"],
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
    exact_facts = {
        item["name"]: item["expected_value"]
        for item in backend["attestation_fact_contract"]
        if item["value_rule"] == "exact"
    }
    session = {
        item["name"]: item["expected_value"]
        for item in backend["session_contract"]["required_observations"]
    }
    required_fact_names = {
        "server_character_set",
        "server_collation",
        "server_time_zone",
        "server_sql_mode",
        "server_transaction_isolation",
        "server_snapshot_isolation",
    }
    require(
        required_fact_names <= set(exact_facts),
        "selected backend does not admit the legacy tabular metadata profile",
    )
    expected = {
        "server.character_set": exact_facts["server_character_set"],
        "server.collation": exact_facts["server_collation"],
        "server.time_zone": exact_facts["server_time_zone"],
        "server.sql_mode": exact_facts["server_sql_mode"],
        "server.transaction_isolation": exact_facts["server_transaction_isolation"],
        "server.snapshot_isolation": "1"
        if exact_facts["server_snapshot_isolation"] == "true"
        else "0",
        "session.character_set_client": session["character_set_client"],
        "session.character_set_connection": session["character_set_connection"],
        "session.character_set_results": session["character_set_results"],
        "session.collation_connection": session["collation_connection"],
        "session.time_zone": session["time_zone"],
        "session.sql_mode": session["sql_mode"],
        "session.transaction_isolation": session["transaction_isolation"],
        "session.snapshot_isolation": "1"
        if session["snapshot_isolation"] == "true"
        else "0",
    }
    actual = dict(metadata["settings"])
    for isolation_key in (
        "server.transaction_isolation",
        "session.transaction_isolation",
    ):
        if isolation_key in actual:
            actual[isolation_key] = normalized_isolation(actual[isolation_key])
    require(actual == expected, "server/session settings differ from backend contract")


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
    exact_facts = {
        item["name"]: item["expected_value"]
        for item in backend["attestation_fact_contract"]
        if item["value_rule"] == "exact"
    }
    database = metadata["database"]
    require(
        database["name"] == backend["physical_namespace"]["database"],
        "schema metadata names wrong database",
    )
    require(
        database["character_set"] == exact_facts["database_character_set"]
        and database["collation"] == exact_facts["database_collation"],
        "benchbase database character set/collation differs from backend contract",
    )

    common_tables = common_table_contracts()
    expected_tables = adapter["tables"] + common_tables
    expected_by_name = {table["name"]: table for table in expected_tables}
    require(
        set(metadata["tables"]) == set(expected_by_name),
        "database table roster differs from selected adapter plus common tables",
    )
    for table_name, expected_table in expected_by_name.items():
        actual_table = metadata["tables"][table_name]
        require(
            actual_table["engine"].lower()
            == exact_facts["application_table_engine"].lower(),
            f"{table_name}: storage engine mismatch",
        )
        require(
            actual_table["collation"] == exact_facts["application_table_collation"],
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
                exact_facts["application_table_character_set"],
                exact_facts["application_table_collation"],
                f"{table_name}.{expected_column['name']}",
            )
        primary_key = [
            name for _, name in sorted(actual_table["primary_key"], key=lambda item: item[0])
        ]
        require(
            primary_key == expected_table["key_columns"],
            f"{table_name}: primary-key order mismatch",
        )


def parse_count_specs(specifications: list[str], label: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for specification in specifications:
        table, separator, value_text = specification.partition("=")
        require(separator == "=" and table and value_text, f"invalid {label} count: {specification}")
        schema, dot, table_name = table.partition(".")
        require(dot == "." and schema and table_name, f"invalid declared table: {table}")
        validate_identifier(schema, f"{label}-count schema")
        validate_identifier(table_name, f"{label}-count table")
        try:
            value = int(value_text, 10)
        except ValueError as error:
            raise RecordError(f"{label} count is not an integer: {specification}") from error
        require(0 <= value <= MAX_SAFE_INTEGER, f"{label} count is out of range: {specification}")
        require(table not in counts, f"duplicate {label} table count: {table}")
        counts[table] = value
    return counts


def validate_loader_count_contract(
    adapter: dict[str, Any], counts: dict[str, int], scale_value: int | None, label: str
) -> dict[str, Any] | None:
    roster = adapter["application_tables"]
    require(
        len(counts) == len(roster) and set(counts) == set(roster),
        f"{label} count roster differs from adapter application tables",
    )
    scale_contract = adapter["loader"]["scale_parameter"]
    if scale_contract is None:
        require(scale_value is None, "adapter does not accept a loader scale value")
        scale = None
    else:
        require(scale_value is not None, "adapter requires a loader scale value")
        require(
            scale_contract["minimum"] <= scale_value <= MAX_SAFE_INTEGER,
            "loader scale value is outside the adapter contract",
        )
        scale = {"name": scale_contract["name"], "value": scale_value}
    for cardinality in adapter["loader"]["cardinalities"]:
        table = cardinality["table"]
        expected: int | None = None
        if cardinality["kind"] == "constant":
            expected = cardinality["value"]
        elif cardinality["kind"] == "scale_multiplier":
            require(scale is not None, f"{table}: scale multiplier lacks scale value")
            expected = cardinality["multiplier"] * scale["value"]
            require(expected <= MAX_SAFE_INTEGER, f"{table}: expected count exceeds safe integer")
        elif cardinality["kind"] in {"configuration", "sum_column"}:
            pass
        else:  # pragma: no cover - adapter schema owns this enumeration
            raise RecordError(f"{table}: unsupported cardinality kind")
        if expected is not None:
            require(
                counts[table] == expected,
                f"{table}: {label} count {counts[table]} differs from contract {expected}",
            )
    return scale


def load_loader_declaration(path: Path, adapter: dict[str, Any]) -> dict[str, Any]:
    try:
        declaration = json.loads(
            path.read_text(encoding="utf-8", errors="strict"),
            object_pairs_hook=reject_duplicate_keys,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RecordError(f"cannot read loader declaration {path}: {error}") from error
    require(isinstance(declaration, dict), "loader declaration is not an object")
    require(
        set(declaration)
        == {
            "kind",
            "benchmark",
            "loader_batch_size",
            "scale_parameter",
            "declared_table_row_counts",
        },
        "loader declaration fields differ from contract",
    )
    require(declaration["kind"] == "loader_declaration", "loader declaration kind mismatch")
    require(
        declaration["benchmark"] == adapter["benchmark"],
        "loader declaration benchmark mismatch",
    )
    batch_size = declaration["loader_batch_size"]
    require(
        isinstance(batch_size, int)
        and not isinstance(batch_size, bool)
        and 1 <= batch_size <= MAX_LOADER_BATCH_SIZE,
        "loader declaration batch size is not a positive signed 32-bit integer",
    )
    scale_document = declaration["scale_parameter"]
    if scale_document is None:
        scale_value = None
    else:
        require(
            isinstance(scale_document, dict) and set(scale_document) == {"name", "value"},
            "loader declaration scale parameter is malformed",
        )
        require(isinstance(scale_document["name"], str), "loader scale name is not string")
        scale_value = scale_document["value"]
        require(
            isinstance(scale_value, int) and not isinstance(scale_value, bool),
            "loader scale value is not integer",
        )
        scale_contract = adapter["loader"]["scale_parameter"]
        require(
            scale_contract is not None and scale_document["name"] == scale_contract["name"],
            "loader declaration scale name differs from adapter contract",
        )
    row_counts = declaration["declared_table_row_counts"]
    require(isinstance(row_counts, list), "loader declared table counts are not an array")
    declared: dict[str, int] = {}
    for index, entry in enumerate(row_counts):
        require(
            isinstance(entry, dict) and set(entry) == {"table", "row_count"},
            f"loader declared table count {index} is malformed",
        )
        table = entry["table"]
        row_count = entry["row_count"]
        require(isinstance(table, str), f"loader declared table {index} is not string")
        require(
            isinstance(row_count, int)
            and not isinstance(row_count, bool)
            and 0 <= row_count <= MAX_SAFE_INTEGER,
            f"loader declared row count {index} is out of range",
        )
        require(table not in declared, f"duplicate loader-declared table count: {table}")
        declared[table] = row_count
    scale = validate_loader_count_contract(adapter, declared, scale_value, "loader-declared")
    require(scale_document == scale, "loader declaration scale parameter differs from contract")
    return {
        "kind": "loader_declaration",
        "benchmark": adapter["benchmark"],
        "loader_batch_size": batch_size,
        "scale_parameter": scale,
        "declared_table_row_counts": [
            {"table": table, "row_count": declared[table]}
            for table in adapter["application_tables"]
        ],
    }


def build_loader_plan(args: argparse.Namespace) -> dict[str, Any]:
    adapter = select_adapter(args.benchmark)
    plan = load_loader_declaration(Path(args.loader_declaration_file), adapter)
    require(
        1 <= args.expected_loader_batch_size <= MAX_LOADER_BATCH_SIZE,
        "expected loader batch size must be a positive signed 32-bit integer",
    )
    require(
        plan["loader_batch_size"] == args.expected_loader_batch_size,
        "loader-declared batch size differs from coordinator configuration",
    )
    scale_contract = adapter["loader"]["scale_parameter"]
    if scale_contract is None:
        require(
            args.expected_scale_value is None,
            "adapter does not accept an expected loader scale value",
        )
    else:
        require(args.expected_scale_value is not None, "adapter requires expected loader scale")
        require(
            plan["scale_parameter"] == {
                "name": scale_contract["name"],
                "value": args.expected_scale_value,
            },
            "loader-declared scale differs from coordinator configuration",
        )
    return plan


def build_independent_loader_plan(args: argparse.Namespace) -> dict[str, Any]:
    adapter = select_adapter(args.benchmark)
    require(
        1 <= args.loader_batch_size <= MAX_LOADER_BATCH_SIZE,
        "loader batch size must be a positive signed 32-bit integer",
    )
    expected_counts = parse_count_specs(args.expected_count, "expected")
    scale = validate_loader_count_contract(adapter, expected_counts, args.scale_value, "expected")
    return {
        "kind": "loader_expectation",
        "benchmark": adapter["benchmark"],
        "loader_batch_size": args.loader_batch_size,
        "scale_parameter": scale,
        "expected_table_row_counts": [
            {"table": table, "row_count": expected_counts[table]}
            for table in adapter["application_tables"]
        ],
    }


def build_loader_queries(args: argparse.Namespace) -> dict[str, Any]:
    adapter = select_adapter(args.benchmark)
    queries = backend_sql_renderer_registry().require(args.backend).loader_queries(adapter)
    return {"backend": args.backend, "benchmark": adapter["benchmark"], "queries": queries}


def parse_loader_observations(text: str) -> dict[str, Any]:
    result: dict[str, Any] = {"counts": {}, "sums": {}, "relationships": {}}
    for line_number, line in enumerate(text.splitlines(), start=1):
        require(line != "", f"loader observation contains empty line {line_number}")
        fields = line.split("\t")
        require(len(fields) == 3, f"loader observation line {line_number} has wrong arity")
        observation, identity, value_text = fields
        try:
            value = int(value_text, 10)
        except ValueError as error:
            raise RecordError(f"loader observation line {line_number} is not integer") from error
        require(0 <= value <= MAX_SAFE_INTEGER, f"loader observation line {line_number} is out of range")
        if observation == "COUNT":
            target = result["counts"]
        elif observation == "SUM":
            target = result["sums"]
        elif observation == "RELATIONSHIP":
            try:
                identity = int(identity, 10)
            except ValueError as error:
                raise RecordError(f"relationship identity on line {line_number} is not integer") from error
            target = result["relationships"]
        else:
            raise RecordError(f"unknown loader observation on line {line_number}: {observation}")
        require(identity not in target, f"duplicate loader observation: {observation} {identity}")
        target[identity] = value
    return result


def validate_loader_observations(
    adapter: dict[str, Any], reference_counts: dict[str, int], observations: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    require(
        set(observations["counts"]) == set(adapter["application_tables"]),
        "independent SQL count roster differs from adapter application tables",
    )
    for table in adapter["application_tables"]:
        require(
            observations["counts"][table] == reference_counts[table],
            f"{table}: independent SQL count {observations['counts'][table]} "
            f"differs from reference count {reference_counts[table]}",
        )

    sum_contracts = [
        item for item in adapter["loader"]["cardinalities"] if item["kind"] == "sum_column"
    ]
    require(
        set(observations["sums"]) == {item["table"] for item in sum_contracts},
        "independent SQL sum observations differ from adapter contract",
    )
    derived_checks = []
    for cardinality in sum_contracts:
        table = cardinality["table"]
        source_sum = observations["sums"][table]
        require(
            observations["counts"][table] == source_sum,
            f"{table}: row count differs from SUM({cardinality['source_table']}."
            f"{cardinality['source_column']})",
        )
        derived_checks.append(
            {
                "table": table,
                "kind": "sum_column",
                "source_table": cardinality["source_table"],
                "source_column": cardinality["source_column"],
                "observed_source_sum": source_sum,
            }
        )

    relationships = adapter["loader"]["relationships"]
    require(
        set(observations["relationships"]) == set(range(len(relationships))),
        "independent SQL relationship observations differ from adapter contract",
    )
    relationship_checks = []
    for index, relationship in enumerate(relationships):
        violation_count = observations["relationships"][index]
        require(
            violation_count == 0,
            f"loader relationship {index} has {violation_count} violating parent rows",
        )
        relationship_checks.append(
            {
                "relationship_index": index,
                "kind": relationship["kind"],
                "parent_table": relationship["parent_table"],
                "child_table": relationship["child_table"],
                "violation_count": violation_count,
            }
        )
    return derived_checks, relationship_checks


def check_loader_state(args: argparse.Namespace) -> dict[str, Any]:
    adapter = select_adapter(args.benchmark)
    plan = build_loader_plan(args)
    observations = parse_loader_observations(sys.stdin.read())
    declared = {
        entry["table"]: entry["row_count"] for entry in plan["declared_table_row_counts"]
    }
    derived_checks, relationship_checks = validate_loader_observations(
        adapter, declared, observations
    )
    return {
        "kind": "loader_validation",
        "benchmark": adapter["benchmark"],
        "declaration_available": True,
        "loader_batch_size": plan["loader_batch_size"],
        "scale_parameter": plan["scale_parameter"],
        "declared_table_row_counts": plan["declared_table_row_counts"],
        "expected_table_row_counts": plan["declared_table_row_counts"],
        "observed_table_row_counts": [
            {"table": table, "row_count": observations["counts"][table]}
            for table in adapter["application_tables"]
        ],
        "derived_cardinality_checks": derived_checks,
        "relationship_checks": relationship_checks,
        "validated": True,
    }


def check_independent_loader_state(args: argparse.Namespace) -> dict[str, Any]:
    adapter = select_adapter(args.benchmark)
    plan = build_independent_loader_plan(args)
    observations = parse_loader_observations(sys.stdin.read())
    expected = {
        entry["table"]: entry["row_count"] for entry in plan["expected_table_row_counts"]
    }
    derived_checks, relationship_checks = validate_loader_observations(
        adapter, expected, observations
    )
    return {
        "kind": "loader_validation",
        "benchmark": adapter["benchmark"],
        "declaration_available": False,
        "loader_batch_size": plan["loader_batch_size"],
        "scale_parameter": plan["scale_parameter"],
        "declared_table_row_counts": None,
        "expected_table_row_counts": plan["expected_table_row_counts"],
        "observed_table_row_counts": [
            {"table": table, "row_count": observations["counts"][table]}
            for table in adapter["application_tables"]
        ],
        "derived_cardinality_checks": derived_checks,
        "relationship_checks": relationship_checks,
        "validated": True,
    }


def length_prefix(value: bytes) -> bytes:
    require(len(value) <= 0xFFFFFFFF, "length-prefixed value exceeds U32")
    return struct.pack(">I", len(value)) + value


def bootstrap_digest(rows: Iterable[tuple[bytes, bytes]]) -> tuple[int, str]:
    materialized = list(rows)
    require(len(materialized) <= MAX_SAFE_INTEGER, "bootstrap row count exceeds safe JSON integer")
    keys = [key for key, _ in materialized]
    require(len(keys) == len(set(keys)), "bootstrap state contains duplicate canonical key")
    materialized.sort(key=lambda item: item[0])
    preimage = bytearray(BOOTSTRAP_DOMAIN)
    preimage.extend(struct.pack(">Q", len(materialized)))
    for key, value in materialized:
        preimage.extend(length_prefix(key))
        preimage.extend(length_prefix(value))
    return len(materialized), hashlib.sha256(preimage).hexdigest()


def bootstrap_adapter_registry() -> BootstrapAdapterRegistry:
    return BootstrapAdapterRegistry.discover(ROOT / "protocol" / "tools" / "bootstrap_adapters")


def backend_sql_renderer_registry() -> BackendSqlRendererRegistry:
    return BackendSqlRendererRegistry.discover(
        ROOT / "protocol" / "tools" / "backend_sql_renderers"
    )


def build_bootstrap_queries(args: argparse.Namespace) -> dict[str, Any]:
    adapter_contract = select_adapter(args.benchmark)
    require(
        adapter_contract["runtime_capabilities"]["bootstrap_scan"],
        f"adapter {args.benchmark} bootstrap scanner is not implemented",
    )
    logical_plan = bootstrap_adapter_registry().require(args.benchmark).build_queries(
        adapter_contract
    )
    queries = backend_sql_renderer_registry().require(args.backend).bootstrap_queries(
        adapter_contract, logical_plan
    )
    return {
        "backend": args.backend,
        "benchmark": logical_plan["benchmark"],
        "format": logical_plan["format"],
        "queries": queries,
    }


def dump_record(record: dict[str, Any]) -> None:
    print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


def build_run_manifest(args: argparse.Namespace) -> dict[str, Any]:
    run_id = validate_uuid(args.run_id)
    adapter = select_adapter(args.benchmark)
    backend = select_backend(args.backend)
    readiness = adapter_runtime_readiness(adapter)
    require(
        readiness["end_to_end_ready"],
        f"adapter {args.benchmark} runtime is incomplete:"
        f"{','.join(readiness['missing_for_end_to_end'])}",
    )
    require(
        args.declared_isolation in adapter["declared_isolations"],
        f"declared isolation is unsupported by adapter {args.benchmark}",
    )
    require(
        args.declared_isolation in backend["session_contract"]["declared_isolations"],
        f"declared isolation is unsupported by backend {args.backend}",
    )
    require(1 <= len(args.backend_source_id) <= 256, "backend_source_id length is outside 1..256")
    sessions = [validate_session_id(value) for value in args.session_id]
    require(sessions, "at least one expected session is required")
    require(len(sessions) == len(set(sessions)), "expected session IDs must be unique")
    application_tables = validate_application_roster(adapter, args.application_table)
    return {
        "kind": "run_manifest",
        "run_id": run_id,
        "backend": backend["backend"],
        "backend_source_id": args.backend_source_id,
        "declared_isolation": args.declared_isolation,
        "benchmark": adapter["benchmark"],
        "expected_session_ids": sessions,
        "application_table_allowlist": application_tables,
        "transaction_marker_table": "benchbase.session_txn",
        "control_table_allowlist": ["benchbase.checker_run_fence"],
        "result_scope": "point_transaction_soww",
    }


def normalized_attestation_tables(adapter: dict[str, Any]) -> list[dict[str, Any]]:
    tables = adapter["tables"] + common_table_contracts()
    return sorted(
        [
            {
                "canonical_table_name": table["name"],
                "primary_key_columns": table["key_columns"],
                "columns": [
                    {
                        "name": column["name"],
                        "logical_sql_type": column["sql_type"],
                        "nullable": column["nullable"],
                        **({"generated": True} if "generated" in column else {}),
                    }
                    for column in table["columns"]
                ],
            }
            for table in tables
        ],
        key=lambda table: table["canonical_table_name"],
    )


def canonical_attestation_digest(record: dict[str, Any]) -> str:
    body = {
        "backend": record["backend"],
        "backend_source_id": record["backend_source_id"],
        "tables": record["tables"],
        "backend_facts": record["backend_facts"],
    }
    canonical = json.dumps(
        body, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8", "strict")
    return hashlib.sha256(SCHEMA_ATTESTATION_DOMAIN + canonical).hexdigest()


def load_json_file(path: str, label: str) -> Any:
    try:
        return json.loads(
            Path(path).read_text(encoding="utf-8"), object_pairs_hook=reject_duplicate_keys
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RecordError(f"cannot read {label}: {error}") from error


def validate_backend_facts(
    facts: Any, backend: dict[str, Any], label: str = "backend facts"
) -> list[dict[str, str]]:
    require(isinstance(facts, list), f"{label} must be an array")
    rules = backend["attestation_fact_contract"]
    require(
        len(facts) == len(rules),
        f"{label} count differs from backend contract",
    )
    result: list[dict[str, str]] = []
    for index, (fact, rule) in enumerate(zip(facts, rules, strict=True)):
        require(
            isinstance(fact, dict) and set(fact) == {"name", "canonical_value"},
            f"{label}[{index}] has wrong fields",
        )
        require(fact["name"] == rule["name"], f"{label}[{index}] name/order differs")
        value = fact["canonical_value"]
        require(isinstance(value, str) and len(value) <= 4096, f"{label}[{index}] value is invalid")
        value_rule = rule["value_rule"]
        if value_rule == "exact":
            require(value == rule["expected_value"], f"{label} {fact['name']} differs")
        elif value_rule == "sha256":
            require(re.fullmatch(r"[0-9a-f]{64}", value) is not None, f"{label} {fact['name']} is not SHA-256")
        elif value_rule == "canonical_identifier":
            require(CANONICAL_IDENTIFIER_PATTERN.fullmatch(value) is not None, f"{label} {fact['name']} is not canonical")
        elif value_rule == "positive_integer":
            require(value.isdigit() and int(value) > 0 and str(int(value)) == value, f"{label} {fact['name']} is not a positive integer")
        else:  # pragma: no cover - schema owns the enumeration
            raise RecordError(f"unsupported backend fact rule: {value_rule}")
        result.append({"name": fact["name"], "canonical_value": value})
    return result


def build_schema_attestation(args: argparse.Namespace) -> dict[str, Any]:
    backend = select_backend(args.backend)
    adapter = select_adapter(args.benchmark)
    tables = load_json_file(args.tables_file, "normalized attestation tables")
    expected_tables = normalized_attestation_tables(adapter)
    require(tables == expected_tables, "normalized attestation tables differ from adapter contract")
    facts = validate_backend_facts(
        load_json_file(args.facts_file, "backend attestation facts"), backend
    )
    record = {
        "kind": "schema_attestation",
        "run_id": validate_uuid(args.run_id),
        "phase": args.phase,
        "backend": backend["backend"],
        "backend_source_id": args.backend_source_id,
        "tables": tables,
        "backend_facts": facts,
    }
    require(
        1 <= len(args.backend_source_id) <= 256,
        "backend_source_id length is outside 1..256",
    )
    record["canonical_attestation_digest"] = canonical_attestation_digest(record)
    validate_document(
        document_validator("schema-attestation.schema.json"), record, "SchemaAttestation"
    )
    return record


def build_bootstrap_manifest(args: argparse.Namespace) -> dict[str, Any]:
    run_id = validate_uuid(args.run_id)
    adapter = select_adapter(args.benchmark)
    require(
        adapter["runtime_capabilities"]["bootstrap_scan"],
        f"adapter {args.benchmark} bootstrap scanner is not implemented",
    )
    require(args.max_rows > 0, "bootstrap max rows must be positive")
    require(args.max_input_bytes > 0, "bootstrap max input bytes must be positive")
    require(args.max_canonical_bytes > 0, "bootstrap max canonical bytes must be positive")
    if args.schema is not None:
        validate_identifier(args.schema, "legacy bootstrap schema")
    if args.table is not None:
        validate_identifier(args.table, "legacy bootstrap table")
    require(
        (args.schema is None) == (args.table is None),
        "legacy schema/table options must be supplied together",
    )
    bootstrap_adapter = bootstrap_adapter_registry().require(adapter["benchmark"])
    rows, counts = bootstrap_adapter.encode_scan(
        adapter,
        Path(args.scan_file),
        max_rows=args.max_rows,
        max_input_bytes=args.max_input_bytes,
        max_canonical_bytes=args.max_canonical_bytes,
        options={},
    )
    row_count, digest = bootstrap_digest(rows)
    require(row_count == sum(counts.values()), "bootstrap row-count algebra mismatch")
    require(
        set(counts) == set(adapter["application_tables"]),
        "bootstrap scanner count roster differs from adapter contract",
    )
    require(
        all(isinstance(count, int) and not isinstance(count, bool) and count >= 0 for count in counts.values()),
        "bootstrap scanner returned an invalid table count",
    )
    return {
        "kind": "bootstrap_manifest",
        "run_id": run_id,
        "table_row_counts": [
            {"table": table, "row_count": counts[table]} for table in sorted(counts)
        ],
        "canonical_state_digest": digest,
    }


def canonical_table_prefix(table: str) -> bytes:
    schema, separator, table_name = table.partition(".")
    require(separator == ".", f"normalized state table is not qualified: {table}")
    validate_identifier(schema, "normalized-state schema")
    validate_identifier(table_name, "normalized-state table")
    return length_prefix(schema.encode("ascii")) + length_prefix(table_name.encode("ascii"))


def decode_canonical_hex(value: Any, label: str, allow_empty: bool) -> bytes:
    require(isinstance(value, str), f"{label} is not a hex string")
    require(
        re.fullmatch(r"(?:[0-9a-f]{2})*", value) is not None,
        f"{label} is not canonical lowercase hex",
    )
    require(allow_empty or value != "", f"{label} must not be empty")
    return bytes.fromhex(value)


def load_normalized_bootstrap_state(
    path: Path,
    adapter: dict[str, Any],
    max_rows: int,
    max_input_bytes: int,
    max_canonical_bytes: int,
) -> tuple[list[tuple[bytes, bytes]], dict[str, int]]:
    require(max_rows > 0, "normalized-state max rows must be positive")
    require(max_input_bytes > 0, "normalized-state max input bytes must be positive")
    require(max_canonical_bytes > 0, "normalized-state max canonical bytes must be positive")
    try:
        require(path.stat().st_size <= max_input_bytes, "normalized state exceeds input byte limit")
    except OSError as error:
        raise RecordError(f"cannot stat normalized state {path}: {error}") from error
    counts = {table: 0 for table in adapter["application_tables"]}
    rows: list[tuple[bytes, bytes]] = []
    canonical_bytes = 0
    try:
        with path.open("r", encoding="utf-8", errors="strict") as stream:
            for line_number, line in enumerate(stream, start=1):
                line = line.rstrip("\r\n")
                require(line != "", f"normalized state has empty line {line_number}")
                try:
                    record = json.loads(line, object_pairs_hook=reject_duplicate_keys)
                except json.JSONDecodeError as error:
                    raise RecordError(
                        f"normalized state line {line_number} is invalid JSON: {error}"
                    ) from error
                require(
                    isinstance(record, dict)
                    and set(record) == {"table", "key_hex", "value_hex"},
                    f"normalized state line {line_number} has wrong fields",
                )
                table = record["table"]
                require(
                    isinstance(table, str) and table in counts,
                    f"normalized state line {line_number} names unknown table",
                )
                key = decode_canonical_hex(record["key_hex"], "normalized key", False)
                value = decode_canonical_hex(record["value_hex"], "normalized value", True)
                require(
                    key.startswith(canonical_table_prefix(table)),
                    f"normalized state line {line_number} key/table prefix mismatch",
                )
                rows.append((key, value))
                counts[table] += 1
                canonical_bytes += len(key) + len(value)
                require(len(rows) <= max_rows, "normalized state exceeds row limit")
                require(
                    canonical_bytes <= max_canonical_bytes,
                    "normalized state exceeds canonical byte limit",
                )
    except (OSError, UnicodeError) as error:
        raise RecordError(f"cannot read normalized state {path}: {error}") from error
    return rows, counts


def reconcile_bootstrap_state(args: argparse.Namespace) -> dict[str, Any]:
    adapter = select_adapter(args.benchmark)
    try:
        manifest = json.loads(
            Path(args.manifest_file).read_text(encoding="utf-8", errors="strict"),
            object_pairs_hook=reject_duplicate_keys,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RecordError(f"cannot read BootstrapManifest: {error}") from error
    validate_document(
        document_validator("bootstrap-manifest.schema.json"),
        manifest,
        "BootstrapManifest",
    )
    rows, counts = load_normalized_bootstrap_state(
        Path(args.normalized_state_file),
        adapter,
        args.max_rows,
        args.max_input_bytes,
        args.max_canonical_bytes,
    )
    _, digest = bootstrap_digest(rows)
    observed_counts = [
        {"table": table, "row_count": counts[table]} for table in sorted(counts)
    ]
    require(
        observed_counts == manifest["table_row_counts"],
        "normalized state table counts differ from BootstrapManifest",
    )
    require(
        digest == manifest["canonical_state_digest"],
        "normalized state digest differs from BootstrapManifest",
    )
    return {
        "kind": "bootstrap_reconciliation",
        "benchmark": adapter["benchmark"],
        "run_id": manifest["run_id"],
        "table_row_counts": observed_counts,
        "canonical_state_digest": digest,
        "reconciled": True,
    }


def inspect_adapter_contract(args: argparse.Namespace) -> dict[str, Any]:
    adapter = select_adapter(args.benchmark)
    if args.transaction_type is not None:
        validate_transaction_membership(adapter, args.transaction_type)
    validate_application_roster(adapter, args.application_table)
    return adapter


def inspect_adapter_readiness(args: argparse.Namespace) -> dict[str, Any]:
    return adapter_runtime_readiness(select_adapter(args.benchmark))


def inspect_backend_contract(args: argparse.Namespace) -> dict[str, Any]:
    return select_backend(args.backend)


def inspect_attestation_tables(args: argparse.Namespace) -> list[dict[str, Any]]:
    return normalized_attestation_tables(select_adapter(args.benchmark))


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
        reconciliation["startup_schema_attested"]
        and reconciliation["final_schema_attested"]
        and re.fullmatch(
            r"[0-9a-f]{64}", reconciliation["schema_attestation_digest"]
        )
        is not None,
        "final verdict lacks matching schema attestations",
    )
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


def add_loader_declaration_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--benchmark", required=True)
    parser.add_argument("--loader-declaration-file", required=True)
    parser.add_argument("--expected-loader-batch-size", type=int, required=True)
    parser.add_argument("--expected-scale-value", type=int)


def add_independent_loader_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--benchmark", required=True)
    parser.add_argument("--scale-value", type=int)
    parser.add_argument("--loader-batch-size", type=int, default=128)
    parser.add_argument("--expected-count", action="append", default=[])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    manifest = subparsers.add_parser("run-manifest", help="emit a RunManifest")
    manifest.add_argument("--run-id", required=True)
    manifest.add_argument("--backend", default="mariadb")
    manifest.add_argument("--backend-source-id", required=True)
    manifest.add_argument(
        "--declared-isolation", choices=("READ_COMMITTED", "REPEATABLE_READ"), required=True
    )
    manifest.add_argument("--benchmark", default="ycsb")
    manifest.add_argument("--session-id", action="append", default=[])
    manifest.add_argument("--application-table", action="append", default=[])

    attestation = subparsers.add_parser(
        "schema-attestation", help="validate and emit a SchemaAttestation"
    )
    attestation.add_argument("--run-id", required=True)
    attestation.add_argument("--phase", choices=("STARTUP", "FINAL"), required=True)
    attestation.add_argument("--backend", required=True)
    attestation.add_argument("--backend-source-id", required=True)
    attestation.add_argument("--benchmark", required=True)
    attestation.add_argument("--tables-file", required=True)
    attestation.add_argument("--facts-file", required=True)

    bootstrap = subparsers.add_parser("bootstrap-manifest", help="emit a BootstrapManifest")
    bootstrap.add_argument("--run-id", required=True)
    bootstrap.add_argument("--benchmark", default="ycsb")
    # Retained as ignored compatibility arguments for already-running older
    # coordinators; current adapters derive physical identity from registration.
    bootstrap.add_argument("--schema")
    bootstrap.add_argument("--table")
    bootstrap.add_argument("--scan-file", required=True)
    bootstrap.add_argument("--max-rows", type=int, default=1000000)
    bootstrap.add_argument("--max-input-bytes", type=int, default=4294967296)
    bootstrap.add_argument("--max-canonical-bytes", type=int, default=2147483648)

    bootstrap_queries = subparsers.add_parser(
        "bootstrap-queries", help="emit adapter-owned canonical scan queries"
    )
    bootstrap_queries.add_argument("--benchmark", required=True)
    bootstrap_queries.add_argument("--backend", default="mariadb")

    bootstrap_reconcile = subparsers.add_parser(
        "bootstrap-reconcile",
        help="compare monitor-normalized bootstrap state with BootstrapManifest",
    )
    bootstrap_reconcile.add_argument("--benchmark", required=True)
    bootstrap_reconcile.add_argument("--manifest-file", required=True)
    bootstrap_reconcile.add_argument("--normalized-state-file", required=True)
    bootstrap_reconcile.add_argument("--max-rows", type=int, default=1000000)
    bootstrap_reconcile.add_argument("--max-input-bytes", type=int, default=4294967296)
    bootstrap_reconcile.add_argument("--max-canonical-bytes", type=int, default=2147483648)

    adapter = subparsers.add_parser(
        "adapter-contract", help="validate and emit one shared adapter contract"
    )
    adapter.add_argument("--benchmark", required=True)
    adapter.add_argument("--transaction-type")
    adapter.add_argument("--application-table", action="append", default=[])

    readiness = subparsers.add_parser(
        "adapter-readiness", help="report monitor and end-to-end adapter readiness"
    )
    readiness.add_argument("--benchmark", required=True)

    backend = subparsers.add_parser(
        "backend-contract", help="validate and emit one shared backend contract"
    )
    backend.add_argument("--backend", default="mariadb")

    attestation_tables = subparsers.add_parser(
        "attestation-tables", help="emit normalized benchmark/control table descriptions"
    )
    attestation_tables.add_argument("--benchmark", required=True)

    schema_check = subparsers.add_parser(
        "schema-check", help="validate server/session/database/table metadata"
    )
    schema_check.add_argument("--backend", default="mariadb")
    schema_check.add_argument("--benchmark", required=True)

    loader_plan = subparsers.add_parser(
        "loader-plan", help="validate and normalize loader declarations"
    )
    add_loader_declaration_argument(loader_plan)

    loader_queries = subparsers.add_parser(
        "loader-queries", help="emit adapter-owned independent SQL validation queries"
    )
    loader_queries.add_argument("--benchmark", required=True)
    loader_queries.add_argument("--backend", default="mariadb")

    loader_check = subparsers.add_parser(
        "loader-check", help="validate independent SQL loader observations"
    )
    add_loader_declaration_argument(loader_check)

    independent_loader_check = subparsers.add_parser(
        "loader-independent-check",
        help="validate SQL state when a historical loader cannot declare counts",
    )
    add_independent_loader_arguments(independent_loader_check)

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
        elif args.command == "schema-attestation":
            dump_record(build_schema_attestation(args))
        elif args.command == "bootstrap-manifest":
            dump_record(build_bootstrap_manifest(args))
        elif args.command == "bootstrap-queries":
            dump_record(build_bootstrap_queries(args))
        elif args.command == "bootstrap-reconcile":
            dump_record(reconcile_bootstrap_state(args))
        elif args.command == "adapter-contract":
            dump_record(inspect_adapter_contract(args))
        elif args.command == "adapter-readiness":
            dump_record(inspect_adapter_readiness(args))
        elif args.command == "backend-contract":
            dump_record(inspect_backend_contract(args))
        elif args.command == "attestation-tables":
            dump_record(inspect_attestation_tables(args))
        elif args.command == "schema-check":
            check_schema_contract(args)
        elif args.command == "loader-plan":
            dump_record(build_loader_plan(args))
        elif args.command == "loader-queries":
            dump_record(build_loader_queries(args))
        elif args.command == "loader-check":
            dump_record(check_loader_state(args))
        elif args.command == "loader-independent-check":
            dump_record(check_independent_loader_state(args))
        elif args.command == "run-failed":
            dump_record(build_run_failed(args))
        elif args.command == "status-action":
            action, lifecycle = classify_status(args)
            print(f"{action}\t{lifecycle}")
        else:  # pragma: no cover - argparse owns this invariant
            raise RecordError(f"unsupported command: {args.command}")
    except (
        OSError,
        RecordError,
        BootstrapAdapterError,
        BackendSqlRendererError,
        UnicodeError,
        json.JSONDecodeError,
    ) as error:
        print(f"black-box record error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
