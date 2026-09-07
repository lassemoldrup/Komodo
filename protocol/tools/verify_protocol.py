#!/usr/bin/env python3
"""Verify black-box protocol schemas, examples, and golden byte vectors."""

from __future__ import annotations

import base64
import hashlib
import json
import re
import struct
import sys
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

try:
    from jsonschema import Draft202012Validator, RefResolver
except ImportError as error:  # pragma: no cover - exercised by environment setup
    raise SystemExit(
        "python3-jsonschema is required; rebuild the canonical development container"
    ) from error

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tpcc_codec import CodecError, TpccCodec, materialize_invalid_vector  # noqa: E402
from tpcc_client_fixture import ClientFixtureError, verify_client_fixture  # noqa: E402
from tpcc_transaction_fixture import (  # noqa: E402
    TransactionFixtureError,
    verify_transaction_fixture,
)


PROTOCOL_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_ROOT = PROTOCOL_ROOT / "schema"
EXAMPLE_ROOT = PROTOCOL_ROOT / "examples"
GOLDEN_ROOT = PROTOCOL_ROOT / "golden"
ADAPTER_ROOT = PROTOCOL_ROOT / "adapters"
BACKEND_ROOT = PROTOCOL_ROOT / "backends"
BACKEND_EXAMPLE_ROOT = EXAMPLE_ROOT / "backends"

READ_DOMAIN = b"db-monitoring/read-digest\x00"
BOOTSTRAP_DOMAIN = b"db-monitoring/bootstrap-state-digest\x00"
CORRELATION_SEED_DOMAIN = b"db-monitoring/session-marker-seed\x00"
CORRELATION_STEP_DOMAIN = b"db-monitoring/session-marker-step\x00"
SCHEMA_ATTESTATION_DOMAIN = b"db-monitoring/schema-attestation\x00"

PRESENT = 0x01
ABSENT = 0x02
SNAPSHOT = 0x01
LOCAL = 0x02
SAFE_JSON_INTEGER = 9_007_199_254_740_991
CANONICAL_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
ADAPTER_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")

TPCC_TRANSACTION_TYPES = (
    "NewOrder",
    "Payment",
    "OrderStatus",
    "Delivery",
    "StockLevel",
)
TPCC_APPLICATION_TABLES = (
    "benchbase.warehouse",
    "benchbase.item",
    "benchbase.stock",
    "benchbase.district",
    "benchbase.customer",
    "benchbase.history",
    "benchbase.oorder",
    "benchbase.new_order",
    "benchbase.order_line",
)
TPCC_TABLE_CONTRACT_SHA256 = (
    "811786b59371deb6fafd3c7ff535f93df08d5f0667a584e492ab4083771c71de"
)
TPCC_LIVE_FIXTURE_SHA256 = (
    "fa4f72247f42eb5062082a964b118acdbd54a99c991c7ae4aeda99bcc267bb82"
)
TPCC_TRANSACTION_FIXTURE_SHA256 = (
    "517d3f63e8b3ca9e006bd518a68ca051b7b52c78272655dd29b490f4de249657"
)
TPCC_CLIENT_FIXTURE_SHA256 = (
    "e515b82f6896a31fe1b7265c26b5bdcc0467962ded92f055e174883f1f1e6cad"
)
MARIADB_SQL_MODE = (
    "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,"
    "NO_ENGINE_SUBSTITUTION,PAD_CHAR_TO_FULL_LENGTH"
)
TPCC_RESULT_SET_CONTRACT = {
    ("NewOrder", "stmtGetCustSQL"): ("benchbase.customer", "snapshot", "exact_key_row", None),
    ("NewOrder", "stmtGetWhseSQL"): ("benchbase.warehouse", "snapshot", "exact_key_row", None),
    ("NewOrder", "stmtGetDistSQL"): (
        "benchbase.district",
        "omit",
        "exact_key_row",
        "locking_current_read",
    ),
    ("NewOrder", "stmtGetItemSQL"): ("benchbase.item", "snapshot", "exact_key_row", None),
    ("NewOrder", "stmtGetStockSQL"): (
        "benchbase.stock",
        "omit",
        "exact_key_row",
        "locking_current_read",
    ),
    ("Payment", "payGetWhseSQL"): ("benchbase.warehouse", "local", "exact_key_row", None),
    ("Payment", "payGetDistSQL"): ("benchbase.district", "local", "exact_key_row", None),
    ("Payment", "payGetCustSQL"): ("benchbase.customer", "snapshot", "exact_key_row", None),
    ("Payment", "payGetCustCdataSQL"): (
        "benchbase.customer",
        "snapshot",
        "exact_key_row",
        None,
    ),
    ("Payment", "customerByNameSQL"): (
        "benchbase.customer",
        "snapshot",
        "returned_rows",
        None,
    ),
    ("OrderStatus", "ordStatGetNewestOrdSQL"): (
        "benchbase.oorder",
        "snapshot",
        "returned_rows",
        None,
    ),
    ("OrderStatus", "ordStatGetOrderLinesSQL"): (
        "benchbase.order_line",
        "snapshot",
        "returned_rows",
        None,
    ),
    ("OrderStatus", "payGetCustSQL"): (
        "benchbase.customer",
        "snapshot",
        "exact_key_row",
        None,
    ),
    ("OrderStatus", "customerByNameSQL"): (
        "benchbase.customer",
        "snapshot",
        "returned_rows",
        None,
    ),
    ("Delivery", "delivGetOrderIdSQL"): (
        "benchbase.new_order",
        "snapshot",
        "returned_rows",
        None,
    ),
    ("Delivery", "delivGetCustIdSQL"): (
        "benchbase.oorder",
        "snapshot",
        "exact_key_row",
        None,
    ),
    ("Delivery", "delivSumOrderAmountSQL"): (
        "benchbase.order_line",
        "omit",
        "aggregate_scalar",
        "aggregate_scalar",
    ),
    ("StockLevel", "stockGetDistOrderIdSQL"): (
        "benchbase.district",
        "snapshot",
        "exact_key_row",
        None,
    ),
    ("StockLevel", "stockGetCountStockSQL"): (
        "benchbase.stock",
        "omit",
        "aggregate_scalar",
        "aggregate_scalar",
    ),
}
TPCC_TRACE_STATEMENTS = {
    ("NewOrder", "stmtGetCustSQL"): "stmtGetCustFullSQL",
    ("NewOrder", "stmtGetWhseSQL"): "stmtGetWhseFullSQL",
    ("NewOrder", "stmtGetDistSQL"): None,
    ("NewOrder", "stmtGetItemSQL"): "stmtGetItemFullSQL",
    ("NewOrder", "stmtGetStockSQL"): None,
    ("Payment", "payGetWhseSQL"): "payGetWhseFullSQL",
    ("Payment", "payGetDistSQL"): "payGetDistFullSQL",
    ("Payment", "payGetCustSQL"): "payGetCustFullSQL",
    ("Payment", "payGetCustCdataSQL"): "payGetCustCdataFullSQL",
    ("Payment", "customerByNameSQL"): "customerByNameFullSQL",
    ("OrderStatus", "ordStatGetNewestOrdSQL"): "ordStatGetNewestOrdFullSQL",
    ("OrderStatus", "ordStatGetOrderLinesSQL"): "ordStatGetOrderLinesFullSQL",
    ("OrderStatus", "payGetCustSQL"): "payGetCustFullSQL",
    ("OrderStatus", "customerByNameSQL"): "customerByNameFullSQL",
    ("Delivery", "delivGetOrderIdSQL"): "delivGetOrderIdFullSQL",
    ("Delivery", "delivGetCustIdSQL"): "delivGetCustIdFullSQL",
    ("Delivery", "delivSumOrderAmountSQL"): None,
    ("StockLevel", "stockGetDistOrderIdSQL"): "stockGetDistOrderIdFullSQL",
    ("StockLevel", "stockGetCountStockSQL"): None,
}


class VerificationError(RuntimeError):
    pass


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise VerificationError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicate_keys)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise VerificationError(f"cannot load {path}: {error}") from error


def require(condition: bool, message: str) -> None:
    if not condition:
        raise VerificationError(message)


def u8(value: int) -> bytes:
    require(0 <= value <= 0xFF, f"U8 out of range: {value}")
    return bytes((value,))


def u32(value: int) -> bytes:
    require(0 <= value <= 0xFFFFFFFF, f"U32 out of range: {value}")
    return struct.pack(">I", value)


def u64(value: int) -> bytes:
    require(0 <= value <= 0xFFFFFFFFFFFFFFFF, f"U64 out of range: {value}")
    return struct.pack(">Q", value)


def i32(value: int) -> bytes:
    require(-0x80000000 <= value <= 0x7FFFFFFF, f"I32 out of range: {value}")
    return struct.pack(">i", value)


def lp(value: bytes) -> bytes:
    return u32(len(value)) + value


def sha256_hex(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def schema_attestation_digest(attestation: dict[str, Any]) -> str:
    body = {
        "backend": attestation["backend"],
        "backend_source_id": attestation["backend_source_id"],
        "tables": attestation["tables"],
        "backend_facts": attestation["backend_facts"],
    }
    canonical = json.dumps(
        body, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8", "strict")
    return sha256_hex(SCHEMA_ATTESTATION_DOMAIN + canonical)


def canonical_base64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def decode_base64(value: str, label: str) -> bytes:
    try:
        decoded = base64.b64decode(value, validate=True)
    except (ValueError, UnicodeEncodeError) as error:
        raise VerificationError(f"invalid Base64 for {label}: {error}") from error
    require(canonical_base64(decoded) == value, f"noncanonical Base64 for {label}")
    return decoded


def encode_ycsb_key(schema: str, table: str, key: int) -> bytes:
    require(
        CANONICAL_IDENTIFIER.fullmatch(schema) is not None,
        f"schema is not a canonical lowercase identifier: {schema}",
    )
    require(
        CANONICAL_IDENTIFIER.fullmatch(table) is not None,
        f"table is not a canonical lowercase identifier: {table}",
    )
    return lp(schema.encode("utf-8", "strict")) + lp(table.encode("utf-8", "strict")) + i32(key)


def encode_ycsb_value(fields: list[Any]) -> bytes:
    require(len(fields) == 10, f"YCSB value must contain ten fields, got {len(fields)}")
    result = bytearray()
    for index, field in enumerate(fields, start=1):
        if field is None:
            result.extend(u8(0))
            continue
        require(isinstance(field, str), f"FIELD{index} is not string/null")
        require(len(field) <= 100, f"FIELD{index} exceeds VARCHAR(100)")
        encoded = field.encode("utf-8", "strict")
        result.extend(u8(1))
        result.extend(lp(encoded))
    return bytes(result)


def encode_read(read: dict[str, Any]) -> bytes:
    key = decode_base64(read["key_bytes_base64"], "read key")
    result = bytearray(u32(read["event_seq"]) + lp(key))
    if read["value_state"] == "present":
        value = decode_base64(read["value_bytes_base64"], "read value")
        result.extend(u8(PRESENT))
        result.extend(lp(value))
    elif read["value_state"] == "absent":
        require("value_bytes_base64" not in read, "absent read carries value bytes")
        result.extend(u8(ABSENT))
    else:
        raise VerificationError(f"unknown read value_state: {read['value_state']}")

    source = {"snapshot": SNAPSHOT, "local": LOCAL}.get(read["source"])
    require(source is not None, f"unknown read source: {read['source']}")
    result.extend(u8(source))
    return bytes(result)


def read_digest_preimage(reads: list[dict[str, Any]]) -> bytes:
    sequences = [read["event_seq"] for read in reads]
    require(sequences == list(range(len(reads))), f"read event_seq is not dense: {sequences}")
    return READ_DOMAIN + u32(len(reads)) + b"".join(encode_read(read) for read in reads)


def bootstrap_digest_preimage(rows: list[dict[str, Any]]) -> bytes:
    decoded_rows = [
        (
            decode_base64(row["key_bytes_base64"], "bootstrap key"),
            decode_base64(row["value_bytes_base64"], "bootstrap value"),
        )
        for row in rows
    ]
    keys = [key for key, _ in decoded_rows]
    require(len(keys) == len(set(keys)), "duplicate bootstrap canonical key")
    decoded_rows.sort(key=lambda item: item[0])
    body = b"".join(lp(key) + lp(value) for key, value in decoded_rows)
    return BOOTSTRAP_DOMAIN + u64(len(decoded_rows)) + body


def correlation_seed(run_id: str, session_id: str) -> tuple[bytes, bytes]:
    preimage = (
        CORRELATION_SEED_DOMAIN
        + uuid.UUID(run_id).bytes
        + lp(session_id.encode("utf-8", "strict"))
    )
    return preimage, hashlib.sha256(preimage).digest()


def correlation_entry(session_seq: int, txn_id: int) -> bytes:
    require(1 <= txn_id <= SAFE_JSON_INTEGER, f"txn_id out of range: {txn_id}")
    return u64(session_seq) + u64(txn_id)


def validate_session_attempt_progress(entries: list[dict[str, int]], label: str) -> None:
    previous_surplus: int | None = None
    for entry in entries:
        session_seq = entry["session_seq"]
        txn_id = entry["txn_id"]
        require(txn_id >= session_seq, f"{label}: marker txn_id is behind session_seq")
        surplus = txn_id - session_seq
        require(
            previous_surplus is None or surplus >= previous_surplus,
            f"{label}: marker retry surplus decreased",
        )
        previous_surplus = surplus


def validate_final_attempt_feasibility(
    entries: list[dict[str, int]],
    final_logical_transactions: int,
    final_physical_attempts: int,
    label: str,
) -> None:
    last_session_seq = entries[-1]["session_seq"] if entries else 0
    last_txn_id = entries[-1]["txn_id"] if entries else 0
    require(
        last_session_seq <= final_logical_transactions,
        f"{label}: final logical progress precedes the last published transaction",
    )
    require(
        last_txn_id <= final_physical_attempts,
        f"{label}: final physical progress precedes the last published attempt",
    )

    remaining_logical_transactions = final_logical_transactions - last_session_seq
    remaining_physical_attempts = final_physical_attempts - last_txn_id
    if remaining_logical_transactions == 0:
        require(
            remaining_physical_attempts == 0,
            f"{label}: final attempt history allocates attempts after logical completion",
        )
    else:
        require(
            remaining_physical_attempts >= remaining_logical_transactions,
            f"{label}: too few remaining attempts for remaining logical transactions",
        )


def correlation_step(previous: bytes, entry: bytes) -> bytes:
    return hashlib.sha256(CORRELATION_STEP_DOMAIN + previous + entry).digest()


def check_encoded_vector(label: str, expected: dict[str, Any], actual: bytes) -> None:
    if "length" in expected:
        require(len(actual) == expected["length"], f"{label}: length mismatch")
    if "hex" in expected:
        require(actual.hex() == expected["hex"], f"{label}: hex mismatch")
    if "base64" in expected:
        require(canonical_base64(actual) == expected["base64"], f"{label}: Base64 mismatch")
    if "sha256" in expected:
        require(sha256_hex(actual) == expected["sha256"], f"{label}: SHA-256 mismatch")


def documented_failure_codes() -> set[str]:
    result_code_document = (PROTOCOL_ROOT / "result_error_codes.md").read_text(
        encoding="utf-8"
    )
    require("## Error codes" in result_code_document, "error-code section is missing")
    error_code_section = result_code_document.split("## Error codes", maxsplit=1)[1]
    return set(
        re.findall(
            r"^\| `([A-Z][A-Z0-9_]*)` \|",
            error_code_section,
            re.MULTILINE,
        )
    )


def load_schema_documents() -> list[dict[str, Any]]:
    schemas = [load_json(path) for path in sorted(SCHEMA_ROOT.glob("*.schema.json"))]
    for schema in schemas:
        Draft202012Validator.check_schema(schema)
    return schemas


def schema_validator(
    schemas: list[dict[str, Any]], schema_name: str
) -> Draft202012Validator:
    schema = next(
        candidate
        for candidate in schemas
        if candidate["$id"].endswith(f"/{schema_name}")
    )
    resolver = RefResolver.from_schema(
        schema, store={candidate["$id"]: candidate for candidate in schemas}
    )
    return Draft202012Validator(schema, resolver=resolver)


def verify_document_schema(
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
        raise VerificationError(f"schema validation failed for {label}: {rendered}")


def validate_table_contract(table: dict[str, Any], canonical_schema: str) -> None:
    label = f"table {table['name']}"
    require(
        table["name"].startswith(f"{canonical_schema}."),
        f"{label}: table is outside canonical schema",
    )
    columns = table["columns"]
    column_names = [column["name"] for column in columns]
    require(
        len(column_names) == len(set(column_names)),
        f"{label}: duplicate column name",
    )
    key_columns = table["key_columns"]
    value_columns = table["value_columns"]
    require(
        all(column in column_names for column in key_columns),
        f"{label}: key names an unknown column",
    )
    require(
        all(column in column_names for column in value_columns),
        f"{label}: value names an unknown column",
    )
    require(
        not set(key_columns).intersection(value_columns),
        f"{label}: key/value columns overlap",
    )
    require(
        value_columns
        == [column for column in column_names if column not in set(key_columns)],
        f"{label}: value columns are not all non-key columns in schema order",
    )
    columns_by_name = {column["name"]: column for column in columns}
    for key_column in key_columns:
        require(
            not columns_by_name[key_column]["nullable"],
            f"{label}: primary-key column is nullable",
        )

    expected_canonical_types = {
        "INT": "i32",
        "BIGINT": "i64",
        "FLOAT": "float32",
        "TIMESTAMP": "timestamp_utc",
    }
    for column in columns:
        sql_type = column["sql_type"]
        if sql_type.startswith("DECIMAL("):
            expected_type = "fixed_decimal"
        elif sql_type.startswith("VARCHAR("):
            expected_type = "utf8_varchar"
        elif sql_type.startswith("CHAR("):
            expected_type = "utf8_char"
        else:
            expected_type = expected_canonical_types.get(sql_type)
        require(
            expected_type == column["canonical_type"],
            f"{label}.{column['name']}: SQL/canonical type mismatch",
        )
        if "generated" in column:
            require(
                column["name"] in key_columns
                and column["generated"] == "auto_increment"
                and sql_type == "BIGINT"
                and not column["nullable"],
                f"{label}.{column['name']}: invalid generated-key contract",
            )


def validate_cardinality_contract(
    cardinality: dict[str, Any], tables_by_name: dict[str, dict[str, Any]], label: str
) -> None:
    table_name = cardinality["table"]
    require(table_name in tables_by_name, f"{label}: cardinality names unknown table")
    kind = cardinality["kind"]
    required_by_kind = {
        "constant": {"value"},
        "scale_multiplier": {"multiplier"},
        "sum_column": {"source_table", "source_column"},
        "configuration": {"configuration_name"},
    }
    common = {"table", "kind"}
    require(
        set(cardinality) == common | required_by_kind[kind],
        f"{label}: {kind} cardinality has wrong fields",
    )
    if kind == "sum_column":
        source_table = cardinality["source_table"]
        require(
            source_table in tables_by_name,
            f"{label}: cardinality source table is unknown",
        )
        source_columns = {
            column["name"] for column in tables_by_name[source_table]["columns"]
        }
        require(
            cardinality["source_column"] in source_columns,
            f"{label}: cardinality source column is unknown",
        )


def validate_adapter_contract(contract: dict[str, Any], label: str) -> None:
    benchmark = contract["benchmark"]
    require(
        ADAPTER_NAME.fullmatch(benchmark) is not None,
        f"{label}: benchmark name is not bounded/canonical",
    )
    transactions = contract["transaction_types"]
    require(
        all(ADAPTER_NAME.fullmatch(name) is not None for name in transactions),
        f"{label}: transaction type is not bounded/canonical",
    )
    tables = contract["tables"]
    table_names = [table["name"] for table in tables]
    require(
        table_names == contract["application_tables"],
        f"{label}: table definitions disagree with application roster",
    )
    require(len(table_names) == len(set(table_names)), f"{label}: duplicate table")
    for table in tables:
        validate_table_contract(table, contract["canonical_schema"])
    tables_by_name = {table["name"]: table for table in tables}

    statement_keys: set[tuple[str, str]] = set()
    for result_set in contract["result_sets"]:
        statement_key = (result_set["procedure"], result_set["statement_id"])
        require(
            statement_key not in statement_keys,
            f"{label}: duplicate result-producing statement {statement_key}",
        )
        statement_keys.add(statement_key)
        require(
            result_set["transaction_type"] in transactions,
            f"{label}: result set names unknown transaction type",
        )
        require(
            result_set["procedure"] == result_set["transaction_type"],
            f"{label}: procedure and wire transaction name differ",
        )
        require(
            result_set["table"] in tables_by_name,
            f"{label}: result set names unknown table",
        )
        classification = result_set["classification"]
        omission_reason = result_set.get("omission_reason")
        require(
            (classification == "omit") == (omission_reason is not None),
            f"{label}: omission classification/reason mismatch",
        )
        if classification == "omit":
            require(
                result_set["trace_statement_id"] is None
                and result_set["trace_projection"] == "omitted",
                f"{label}: omitted result unexpectedly has a trace SQL variant",
            )
        else:
            require(
                isinstance(result_set["trace_statement_id"], str)
                and result_set["trace_projection"] == "full_row",
                f"{label}: captured result lacks a full-row trace SQL variant",
            )
        require(
            classification == "omit"
            or result_set["result_shape"] != "aggregate_scalar",
            f"{label}: aggregate result is presented as a point read",
        )
        if omission_reason == "aggregate_scalar":
            require(
                result_set["result_shape"] == "aggregate_scalar"
                and "aggregate" in result_set["semantics_ignored"],
                f"{label}: aggregate omission is incomplete",
            )
        if omission_reason == "locking_current_read":
            require(
                "locking" in result_set["semantics_ignored"],
                f"{label}: locking omission is incomplete",
            )

    cardinalities = contract["loader"]["cardinalities"]
    cardinality_tables = [cardinality["table"] for cardinality in cardinalities]
    require(
        len(cardinality_tables) == len(set(cardinality_tables))
        and set(cardinality_tables) == set(table_names),
        f"{label}: loader cardinalities disagree with table roster",
    )
    for index, cardinality in enumerate(cardinalities):
        validate_cardinality_contract(
            cardinality, tables_by_name, f"{label}: cardinality {index}"
        )
    for relationship in contract["loader"]["relationships"]:
        parent = tables_by_name.get(relationship["parent_table"])
        child = tables_by_name.get(relationship["child_table"])
        require(parent is not None and child is not None, f"{label}: relationship table is unknown")
        parent_columns = {column["name"] for column in parent["columns"]}
        child_columns = {column["name"] for column in child["columns"]}
        require(
            relationship["parent_key_columns"] == parent["key_columns"],
            f"{label}: relationship parent key differs from table key",
        )
        require(
            len(relationship["parent_key_columns"])
            == len(relationship["child_parent_columns"]),
            f"{label}: relationship key arity mismatch",
        )
        require(
            all(name in child_columns for name in relationship["child_parent_columns"]),
            f"{label}: relationship child key column is unknown",
        )
        require(
            relationship["expected_count_column"] in parent_columns
            and relationship["child_sequence_column"] in child_columns,
            f"{label}: relationship count/sequence column is unknown",
        )
        require(
            relationship["minimum_count"] <= relationship["maximum_count"],
            f"{label}: relationship count bounds are reversed",
        )


def adapter_runtime_readiness(contract: dict[str, Any]) -> dict[str, bool]:
    capabilities = contract["runtime_capabilities"]
    monitor_ready = capabilities["cdc_normalization"] and capabilities["bootstrap_scan"]
    return {
        "bootstrap_ready": capabilities["bootstrap_scan"],
        "monitor_ready": monitor_ready,
        "client_ready": capabilities["client_trace"],
        "end_to_end_ready": monitor_ready and capabilities["client_trace"],
    }


def verify_tpcc_contract(contract: dict[str, Any]) -> None:
    require(
        tuple(contract["transaction_types"]) == TPCC_TRANSACTION_TYPES,
        "TPC-C transaction-type contract differs from the accepted wire names",
    )
    require(
        tuple(contract["application_tables"]) == TPCC_APPLICATION_TABLES,
        "TPC-C application-table roster differs from the accepted schema",
    )
    require(
        contract["declared_isolations"] == ["REPEATABLE_READ"],
        "TPC-C isolation contract is not repeatable read only",
    )
    require(
        contract["runtime_capabilities"]["client_trace"] is True
        and contract["runtime_capabilities"]["bootstrap_scan"] is True,
        "TPC-C runtime capabilities differ from implemented boundaries",
    )
    readiness = adapter_runtime_readiness(contract)
    require(readiness["bootstrap_ready"], "TPC-C bootstrap scanner is not ready")
    require(readiness["monitor_ready"], "TPC-C monitor normalization is not ready")
    require(readiness["client_ready"], "TPC-C client tracing is not ready")
    require(readiness["end_to_end_ready"], "TPC-C end-to-end path is not ready")
    table_contract_bytes = json.dumps(
        contract["tables"],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    require(
        sha256_hex(table_contract_bytes) == TPCC_TABLE_CONTRACT_SHA256,
        "TPC-C table columns/types/nullability/order differ from the accepted schema",
    )

    actual_result_sets = {
        (item["transaction_type"], item["statement_id"]): (
            item["table"],
            item["classification"],
            item["result_shape"],
            item.get("omission_reason"),
        )
        for item in contract["result_sets"]
    }
    require(
        actual_result_sets == TPCC_RESULT_SET_CONTRACT,
        "TPC-C statement capture/omit matrix differs from BenchBase procedures",
    )
    actual_trace_statements = {
        (item["transaction_type"], item["statement_id"]): item["trace_statement_id"]
        for item in contract["result_sets"]
    }
    require(
        actual_trace_statements == TPCC_TRACE_STATEMENTS,
        "TPC-C full-row trace SQL variant identities differ from BenchBase procedures",
    )

    tables = {table["name"]: table for table in contract["tables"]}
    history = tables["benchbase.history"]
    require(
        history["columns"][0]
        == {
            "name": "h_id",
            "sql_type": "BIGINT",
            "nullable": False,
            "canonical_type": "i64",
            "generated": "auto_increment",
        }
        and history["key_columns"] == ["h_id"],
        "TPC-C history evidence key contract is wrong",
    )
    require(
        tables["benchbase.new_order"]["value_columns"] == [],
        "TPC-C new_order canonical value must be empty",
    )

    expected_cardinalities = {
        "benchbase.item": ("constant", 100000),
        "benchbase.warehouse": ("scale_multiplier", 1),
        "benchbase.stock": ("scale_multiplier", 100000),
        "benchbase.district": ("scale_multiplier", 10),
        "benchbase.customer": ("scale_multiplier", 30000),
        "benchbase.history": ("scale_multiplier", 30000),
        "benchbase.oorder": ("scale_multiplier", 30000),
        "benchbase.new_order": ("scale_multiplier", 9000),
        "benchbase.order_line": ("sum_column", ("benchbase.oorder", "o_ol_cnt")),
    }
    actual_cardinalities: dict[str, tuple[str, Any]] = {}
    for item in contract["loader"]["cardinalities"]:
        if item["kind"] == "constant":
            value: Any = item["value"]
        elif item["kind"] == "scale_multiplier":
            value = item["multiplier"]
        elif item["kind"] == "sum_column":
            value = (item["source_table"], item["source_column"])
        else:
            value = item.get("configuration_name")
        actual_cardinalities[item["table"]] = (item["kind"], value)
    require(
        actual_cardinalities == expected_cardinalities,
        "TPC-C loader cardinality contract differs from the accepted counts",
    )
    relationship = contract["loader"]["relationships"]
    require(len(relationship) == 1, "TPC-C loader must define one order-line relationship")
    relationship = relationship[0]
    require(
        relationship["parent_table"] == "benchbase.oorder"
        and relationship["child_table"] == "benchbase.order_line"
        and relationship["parent_key_columns"] == ["o_w_id", "o_d_id", "o_id"]
        and relationship["child_parent_columns"] == ["ol_w_id", "ol_d_id", "ol_o_id"]
        and relationship["child_sequence_column"] == "ol_number"
        and relationship["expected_count_column"] == "o_ol_cnt"
        and relationship["first_sequence_value"] == 1
        and relationship["sequence_step"] == 1
        and relationship["minimum_count"] == 5
        and relationship["maximum_count"] == 15,
        "TPC-C order-line relationship contract is wrong",
    )


def load_adapter_contracts(
    schemas: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    schemas = schemas or load_schema_documents()
    validator = schema_validator(schemas, "adapter-contract.schema.json")
    contracts: dict[str, dict[str, Any]] = {}
    for path in sorted(ADAPTER_ROOT.glob("*.json")):
        contract = load_json(path)
        verify_document_schema(validator, contract, path.name)
        validate_adapter_contract(contract, path.name)
        benchmark = contract["benchmark"]
        require(benchmark not in contracts, f"duplicate adapter contract: {benchmark}")
        require(path.stem == benchmark, f"adapter filename does not match benchmark: {path.name}")
        contracts[benchmark] = contract
    require("ycsb" in contracts and "tpcc" in contracts, "required adapter contract is missing")
    verify_tpcc_contract(contracts["tpcc"])
    return contracts


def load_backend_contracts(
    schemas: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    schemas = schemas or load_schema_documents()
    validator = schema_validator(schemas, "backend-contract.schema.json")
    contracts: dict[str, dict[str, Any]] = {}
    for path in sorted(BACKEND_ROOT.glob("*.json")):
        contract = load_json(path)
        verify_document_schema(validator, contract, path.name)
        backend = contract["backend"]
        require(backend not in contracts, f"duplicate backend contract: {backend}")
        require(path.stem == backend, f"backend filename does not match name: {path.name}")
        observation_names = [
            item["name"] for item in contract["session_contract"]["required_observations"]
        ]
        require(
            len(observation_names) == len(set(observation_names)),
            f"{path.name}: duplicate session observation",
        )
        fact_names = [item["name"] for item in contract["attestation_fact_contract"]]
        require(
            len(fact_names) == len(set(fact_names)),
            f"{path.name}: duplicate attestation fact",
        )
        namespace = contract["physical_namespace"]
        require(
            namespace["canonical_schema"] == "benchbase"
            and namespace["identifier_case"] in {"lower", "upper"},
            f"{path.name}: physical namespace lacks an exact canonical mapping",
        )
        contracts[backend] = contract
    require(
        "mariadb" in contracts and "yugabytedb_ysql" in contracts,
        "required backend contract is missing",
    )

    mariadb = contracts["mariadb"]
    require(
        mariadb["accepted_database_types"] == ["MARIADB"]
        and mariadb["session_contract"]["configuration_statements"][0]
        == "SET NAMES utf8mb4 COLLATE utf8mb4_bin"
        and any(
            item == {"name": "sql_mode", "expected_value": MARIADB_SQL_MODE}
            for item in mariadb["session_contract"]["required_observations"]
        )
        and mariadb["normalized_capabilities"]["connector_startup_schema_evidence"]
        == "required",
        "MariaDB backend contract differs from the accepted normalized settings",
    )

    yugabyte = contracts["yugabytedb_ysql"]
    yugabyte_facts = {
        item["name"]: item for item in yugabyte["attestation_fact_contract"]
    }
    require(
        yugabyte["accepted_database_types"] == ["POSTGRES"]
        and yugabyte["physical_namespace"]
        == {
            "database": "benchbase",
            "schema": "benchbase",
            "search_path": "benchbase",
            "canonical_schema": "benchbase",
            "identifier_case": "lower",
        }
        and yugabyte["normalized_capabilities"]["connector_startup_schema_evidence"]
        == "not_available"
        and yugabyte["connector_contract"]
        == {
            "connector_class": "io.debezium.connector.postgresql.YugabyteDBConnector",
            "source_connector": "postgresql",
            "source_version": "dz.2.5.2.yb.2026.1",
            "envelope_profile": "yugabytedb_pgoutput_2_5_2",
            "transaction_metadata": True,
            "tasks": 1,
            "redis_stream": "cdc",
        }
        and yugabyte_facts["slot_plugin"]["expected_value"] == "pgoutput"
        and yugabyte_facts["slot_lsn_type"]["expected_value"] == "SEQUENCE"
        and yugabyte_facts["streaming_mode"]["expected_value"] == "default"
        and yugabyte_facts["connector_sha256"]["expected_value"]
        == "4e83132cbebdec53e553f69d4cfdbe48dac563105aabc5db4b880cdcf0118d98",
        "YugabyteDB backend contract differs from the pinned Milestone 9 settings",
    )
    return contracts


def normalized_attestation_tables(adapter: dict[str, Any]) -> list[dict[str, Any]]:
    result = [
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
        for table in adapter["tables"]
    ]
    result.extend(
        [
            {
                "canonical_table_name": "benchbase.checker_run_fence",
                "primary_key_columns": ["run_id", "phase"],
                "columns": [
                    {"name": "run_id", "logical_sql_type": "CHAR(36)", "nullable": False},
                    {"name": "phase", "logical_sql_type": "VARCHAR(32)", "nullable": False},
                    {"name": "nonce", "logical_sql_type": "CHAR(36)", "nullable": False},
                ],
            },
            {
                "canonical_table_name": "benchbase.session_txn",
                "primary_key_columns": ["session_id", "txn_id"],
                "columns": [
                    {"name": "session_id", "logical_sql_type": "VARCHAR(64)", "nullable": False},
                    {"name": "txn_id", "logical_sql_type": "BIGINT", "nullable": False},
                ],
            },
        ]
    )
    return sorted(result, key=lambda item: item["canonical_table_name"])


def validate_schema_attestation(
    attestation: dict[str, Any],
    backend: dict[str, Any],
    adapter: dict[str, Any],
    label: str,
) -> None:
    require(attestation["backend"] == backend["backend"], f"{label}: backend differs")
    tables = attestation["tables"]
    table_names = [item["canonical_table_name"] for item in tables]
    require(table_names == sorted(set(table_names)), f"{label}: tables are not unique/sorted")
    require(
        tables == normalized_attestation_tables(adapter),
        f"{label}: normalized table roster/schema differs from benchmark contract",
    )
    facts = attestation["backend_facts"]
    fact_names = [item["name"] for item in facts]
    rules = backend["attestation_fact_contract"]
    require(
        fact_names == [item["name"] for item in rules],
        f"{label}: backend facts are missing, extra, or reordered",
    )
    for fact, rule in zip(facts, rules, strict=True):
        value = fact["canonical_value"]
        if rule["value_rule"] == "exact":
            require(value == rule["expected_value"], f"{label}: fact {fact['name']} differs")
        elif rule["value_rule"] == "sha256":
            require(re.fullmatch(r"[0-9a-f]{64}", value) is not None, f"{label}: fact {fact['name']} is not SHA-256")
        elif rule["value_rule"] == "canonical_identifier":
            require(CANONICAL_IDENTIFIER.fullmatch(value) is not None, f"{label}: fact {fact['name']} is not a canonical identifier")
        elif rule["value_rule"] == "positive_integer":
            require(value.isdigit() and int(value) > 0 and str(int(value)) == value, f"{label}: fact {fact['name']} is not a canonical positive integer")
        elif rule["value_rule"] == "record_only":
            pass  # Preserve the observed setting without pinning its value.
        else:  # pragma: no cover - schema owns the enumeration
            raise VerificationError(f"{label}: unknown backend fact rule")
    require(
        attestation["canonical_attestation_digest"] == schema_attestation_digest(attestation),
        f"{label}: canonical attestation digest differs",
    )


def verify_backend_attestation_examples(
    backends: dict[str, dict[str, Any]],
    adapters: dict[str, dict[str, Any]],
    schemas: list[dict[str, Any]],
) -> int:
    validator = schema_validator(schemas, "schema-attestation.schema.json")
    count = 0
    covered_backends: set[str] = set()
    paths = sorted(BACKEND_EXAMPLE_ROOT.glob("*.json")) + sorted(
        EXAMPLE_ROOT.glob("schema-attestation-*.json")
    )
    for path in paths:
        document = load_json(path)
        label = f"backends/{path.name}" if path.parent == BACKEND_EXAMPLE_ROOT else path.name
        verify_document_schema(validator, document, label)
        backend = backends.get(document["backend"])
        require(backend is not None, f"{label}: unregistered backend")
        validate_schema_attestation(
            document, backend, adapters["ycsb"], label
        )
        covered_backends.add(document["backend"])
        count += 1
    require(
        covered_backends == set(backends),
        "backend attestation examples do not cover every registered backend",
    )
    return count


def verify_tpcc_executable_vectors(
    contracts: dict[str, dict[str, Any]], schemas: list[dict[str, Any]]
) -> None:
    del schemas  # executable goldens are validated by the shared codec, not a shape-only schema
    path = GOLDEN_ROOT / "tpcc_rows.json"
    golden = load_json(path)
    require(
        isinstance(golden, dict)
        and set(golden) == {"benchmark", "canonical_schema", "vectors", "invalid_vectors"},
        "TPC-C executable golden document has wrong fields",
    )
    require(golden["benchmark"] == "tpcc", "TPC-C golden benchmark mismatch")
    contract = contracts[golden["benchmark"]]
    require(
        golden["canonical_schema"] == contract["canonical_schema"],
        "TPC-C golden canonical schema mismatch",
    )
    codec = TpccCodec(contract)
    vectors = golden["vectors"]
    require(isinstance(vectors, list) and vectors, "TPC-C valid vectors are missing")
    valid_by_name: dict[str, dict[str, Any]] = {}
    tables_seen: set[str] = set()
    all_integer_values: set[int] = set()
    float_bits: set[str] = set()
    timestamp_values: set[str] = set()
    has_short_char = False
    has_utf8 = False
    for vector in vectors:
        require(
            isinstance(vector, dict)
            and set(vector)
            == {"name", "table", "row", "expected_key_hex", "expected_value_hex"},
            "TPC-C valid vector has wrong fields",
        )
        name = vector["name"]
        require(
            isinstance(name, str) and CANONICAL_IDENTIFIER.fullmatch(name) is not None,
            "TPC-C valid vector name is not canonical",
        )
        require(name not in valid_by_name, f"duplicate TPC-C valid vector: {name}")
        require(vector["table"] in contract["application_tables"], f"{name}: unknown table")
        for field in ("expected_key_hex", "expected_value_hex"):
            require(
                isinstance(vector[field], str)
                and re.fullmatch(r"(?:[0-9a-f]{2})*", vector[field]) is not None,
                f"{name}: {field} is not canonical hex",
            )
        try:
            key, value = codec.encode_row(vector["table"], vector["row"])
        except CodecError as error:
            raise VerificationError(f"TPC-C valid vector {name} failed: {error.code}: {error}") from error
        require(key.hex() == vector["expected_key_hex"], f"{name}: expected key differs")
        require(value.hex() == vector["expected_value_hex"], f"{name}: expected value differs")
        valid_by_name[name] = vector
        tables_seen.add(vector["table"])
        table = codec.table(vector["table"])
        columns = {column["name"]: column for column in table["columns"]}
        for column_name, typed_value in vector["row"].items():
            if isinstance(typed_value, int) and not isinstance(typed_value, bool):
                all_integer_values.add(typed_value)
            elif isinstance(typed_value, dict) and "raw_bits_hex" in typed_value:
                float_bits.add(typed_value["raw_bits_hex"])
            elif isinstance(typed_value, str):
                if columns[column_name]["sql_type"] == "TIMESTAMP":
                    timestamp_values.add(typed_value)
                if columns[column_name]["sql_type"].startswith("CHAR("):
                    width = int(columns[column_name]["sql_type"].split("(")[1][:-1])
                    has_short_char |= len(typed_value) < width
                has_utf8 |= any(ord(character) > 127 for character in typed_value)

    require(
        tables_seen == set(contract["application_tables"]),
        "TPC-C executable goldens do not cover every table",
    )
    require(
        {-2147483648, 2147483647, -9223372036854775808, 9223372036854775807}
        <= all_integer_values,
        "TPC-C executable goldens lack signed key boundaries",
    )
    require(
        {"3eaaaaab", "bf800000", "7f7fffff"} <= float_bits,
        "TPC-C executable goldens lack fractional/negative/maximum finite float32",
    )
    require(
        {"1969-12-31T23:59:59Z", "1970-01-01T00:00:00Z"} <= timestamp_values,
        "TPC-C executable goldens lack negative/zero UTC epoch cases",
    )
    require(has_short_char and has_utf8, "TPC-C executable goldens lack CHAR padding/UTF-8")
    require(
        valid_by_name["oorder_carrier_null"]["row"]["o_carrier_id"] is None
        and valid_by_name["order_line_delivery_null"]["row"]["ol_delivery_d"] is None,
        "TPC-C executable goldens lack nullable carrier/delivery cases",
    )
    require(
        valid_by_name["new_order_empty_value"]["expected_value_hex"] == "",
        "TPC-C new_order present value is not empty",
    )

    invalid_vectors = golden["invalid_vectors"]
    require(isinstance(invalid_vectors, list) and invalid_vectors, "TPC-C invalid vectors missing")
    invalid_names: set[str] = set()
    errors_seen: set[str] = set()
    for vector in invalid_vectors:
        require(
            isinstance(vector, dict)
            and set(vector)
            == {"name", "table", "base_vector", "mutations", "expected_error"},
            "TPC-C invalid vector has wrong fields",
        )
        name = vector["name"]
        require(
            isinstance(name, str) and CANONICAL_IDENTIFIER.fullmatch(name) is not None,
            "TPC-C invalid vector name is not canonical",
        )
        require(name not in invalid_names and name not in valid_by_name, f"duplicate vector: {name}")
        invalid_names.add(name)
        try:
            row = materialize_invalid_vector(vector, valid_by_name)
            codec.encode_row(vector["table"], row)
        except CodecError as error:
            require(
                error.code == vector["expected_error"],
                f"{name}: expected {vector['expected_error']}, got {error.code}",
            )
            errors_seen.add(error.code)
        else:
            raise VerificationError(f"TPC-C invalid vector unexpectedly encoded: {name}")
    require(
        {
            "row_shape",
            "nullability",
            "integer_range",
            "decimal_format",
            "decimal_scale",
            "decimal_range",
            "float_non_finite",
            "timestamp_format",
            "text_length",
            "utf8",
        }
        <= errors_seen,
        "TPC-C invalid vectors lack required malformed categories",
    )


def verify_tpcc_live_fixture(contracts: dict[str, dict[str, Any]]) -> None:
    path = PROTOCOL_ROOT / "fixtures" / "mariadb_tpcc_typed_rows.json"
    require(
        sha256_hex(path.read_bytes()) == TPCC_LIVE_FIXTURE_SHA256,
        "TPC-C live fixture checksum drift",
    )
    fixture = load_json(path)
    require(
        isinstance(fixture, dict)
        and set(fixture)
        == {"backend", "mariadb_image", "debezium_version", "session_contract", "rows"},
        "TPC-C live fixture has wrong root fields",
    )
    require(fixture["backend"] == "mariadb", "TPC-C live fixture backend mismatch")
    require(fixture["mariadb_image"] == "mariadb:12.3", "TPC-C MariaDB fixture version drift")
    require(
        fixture["debezium_version"] == "3.6.1.Final",
        "TPC-C Debezium fixture version drift",
    )
    require(
        fixture["session_contract"] == "protocol/backends/mariadb.json",
        "TPC-C live fixture session-contract reference drift",
    )
    require(
        (PROTOCOL_ROOT.parent / fixture["session_contract"]).is_file(),
        "TPC-C live fixture session contract does not exist",
    )
    rows = fixture["rows"]
    require(isinstance(rows, list) and len(rows) == 11, "TPC-C live fixture must have 11 rows")
    codec = TpccCodec(contracts["tpcc"])
    expected_table_counts = {
        "benchbase.warehouse": 1,
        "benchbase.item": 1,
        "benchbase.stock": 1,
        "benchbase.district": 1,
        "benchbase.customer": 1,
        "benchbase.history": 1,
        "benchbase.oorder": 2,
        "benchbase.new_order": 1,
        "benchbase.order_line": 2,
    }
    actual_table_counts: dict[str, int] = defaultdict(int)
    has_numeric_float = False
    has_trimmed_char = False
    has_nullable_null = False
    has_nullable_present = False
    for index, row_fixture in enumerate(rows):
        require(
            isinstance(row_fixture, dict)
            and set(row_fixture)
            == {"table", "key", "after", "expected_key_hex", "expected_value_hex", "stream_id"},
            f"TPC-C live fixture row {index} has wrong fields",
        )
        require(
            row_fixture["stream_id"] == f"1787259444600-{index}",
            f"TPC-C live fixture row {index} stream order drift",
        )
        table_name = row_fixture["table"]
        require(table_name in expected_table_counts, f"TPC-C live fixture row {index} table")
        table = codec.table(table_name)
        key_columns = table["key_columns"]
        require(
            isinstance(row_fixture["key"], dict)
            and set(row_fixture["key"]) == set(key_columns)
            and all(
                row_fixture["key"][column] == row_fixture["after"][column]
                for column in key_columns
            ),
            f"TPC-C live fixture row {index} key image mismatch",
        )
        try:
            key, value = codec.encode_row(table_name, row_fixture["after"])
        except CodecError as error:
            raise VerificationError(
                f"TPC-C live fixture row {index} failed: {error.code}: {error}"
            ) from error
        require(key.hex() == row_fixture["expected_key_hex"], f"live row {index} key bytes")
        require(value.hex() == row_fixture["expected_value_hex"], f"live row {index} value bytes")
        actual_table_counts[table_name] += 1
        columns = {column["name"]: column for column in table["columns"]}
        for column_name, typed_value in row_fixture["after"].items():
            column = columns[column_name]
            has_numeric_float |= column["sql_type"] == "FLOAT" and isinstance(
                typed_value, (int, float)
            )
            if column["sql_type"].startswith("CHAR(") and isinstance(typed_value, str):
                width = int(column["sql_type"].split("(")[1][:-1])
                has_trimmed_char |= len(typed_value) < width
            if column["nullable"]:
                has_nullable_null |= typed_value is None
                has_nullable_present |= typed_value is not None
    require(
        dict(actual_table_counts) == expected_table_counts,
        "TPC-C live fixture table counts differ",
    )
    require(has_numeric_float, "TPC-C live fixture lacks numeric FLOAT")
    require(has_trimmed_char, "TPC-C live fixture lacks trimmed CHAR")
    require(
        has_nullable_null and has_nullable_present,
        "TPC-C live fixture lacks nullable null/present rows",
    )


def verify_tpcc_transaction_fixture(contracts: dict[str, dict[str, Any]]) -> None:
    path = PROTOCOL_ROOT / "fixtures" / "mariadb_tpcc_transactions.json"
    require(
        sha256_hex(path.read_bytes()) == TPCC_TRANSACTION_FIXTURE_SHA256,
        "TPC-C transaction fixture checksum drift",
    )
    fixture = load_json(path)
    require(
        isinstance(fixture, dict)
        and set(fixture)
        == {
            "adapter_contract",
            "backend",
            "backend_contract",
            "bootstrap_manifest",
            "debezium_version",
            "mariadb_image",
            "run_id",
            "startup_schema",
            "transactions",
        },
        "TPC-C transaction fixture has wrong root fields",
    )
    require(fixture["backend"] == "mariadb", "TPC-C transaction fixture backend")
    require(
        fixture["mariadb_image"] == "mariadb:12.3"
        and fixture["debezium_version"] == "3.6.1.Final",
        "TPC-C transaction fixture source version drift",
    )
    require(
        fixture["adapter_contract"] == "protocol/adapters/tpcc.json"
        and fixture["backend_contract"] == "protocol/backends/mariadb.json",
        "TPC-C transaction fixture contract references drift",
    )
    require(
        (PROTOCOL_ROOT.parent / fixture["adapter_contract"]).is_file()
        and (PROTOCOL_ROOT.parent / fixture["backend_contract"]).is_file(),
        "TPC-C transaction fixture contract reference is missing",
    )
    require(
        fixture["run_id"] == "87654321-4321-4321-8321-cba987654322",
        "TPC-C transaction fixture run ID drift",
    )
    try:
        normalized = verify_transaction_fixture(fixture, TpccCodec(contracts["tpcc"]))
    except (TransactionFixtureError, CodecError) as error:
        raise VerificationError(f"TPC-C transaction fixture failed: {error}") from error
    require(len(normalized) == 12, "TPC-C transaction normalization count differs")
    require(
        [transaction["kind"] for transaction in normalized]
        == [
            "RUN_CONTROL",
            "BOOTSTRAP_APP",
            "RUN_CONTROL",
            "WORKLOAD_MARKED",
            "WORKLOAD_MARKED",
            "WORKLOAD_MARKED",
            "WORKLOAD_MARKED",
            "WORKLOAD_MARKED",
            "WORKLOAD_MARKED",
            "WORKLOAD_MARKED",
            "WORKLOAD_MARKED",
            "RUN_CONTROL",
        ],
        "TPC-C transaction classifications differ",
    )


def verify_tpcc_client_fixture(
    contracts: dict[str, dict[str, Any]], schemas: list[dict[str, Any]]
) -> None:
    path = PROTOCOL_ROOT / "fixtures" / "mariadb_tpcc_client_txns.json"
    require(
        sha256_hex(path.read_bytes()) == TPCC_CLIENT_FIXTURE_SHA256,
        "TPC-C client fixture checksum drift",
    )
    fixture = load_json(path)
    require(
        isinstance(fixture, dict)
        and set(fixture)
        == {
            "adapter_contract",
            "backend",
            "backend_contract",
            "discarded_attempt_reads",
            "empty_result_assertions",
            "jdbc_driver_version",
            "mariadb_image",
            "raw_transaction_fixture",
            "row_catalog",
            "run_manifest",
            "semantic_transactions",
            "session_done",
            "synthetic_join_clients",
        },
        "TPC-C client fixture has wrong root fields",
    )
    require(
        fixture["backend"] == "mariadb"
        and fixture["mariadb_image"] == "mariadb:12.3"
        and fixture["jdbc_driver_version"] == "3.5.1",
        "TPC-C client fixture source version drift",
    )
    require(
        fixture["adapter_contract"] == "protocol/adapters/tpcc.json"
        and fixture["backend_contract"] == "protocol/backends/mariadb.json",
        "TPC-C client fixture contract reference drift",
    )
    raw_reference = fixture["raw_transaction_fixture"]
    require(
        raw_reference
        == {
            "path": "protocol/fixtures/mariadb_tpcc_transactions.json",
            "sha256": TPCC_TRANSACTION_FIXTURE_SHA256,
        },
        "TPC-C client fixture raw transaction reference drift",
    )
    raw_path = PROTOCOL_ROOT.parent / raw_reference["path"]
    require(sha256_hex(raw_path.read_bytes()) == raw_reference["sha256"], "raw fixture checksum")
    raw_fixture = load_json(raw_path)
    validator = schema_validator(schemas, "records.schema.json")
    verify_record_schema(validator, fixture["run_manifest"], "TPC-C client RunManifest")
    for entry in [*fixture["semantic_transactions"], *fixture["synthetic_join_clients"]]:
        verify_record_schema(validator, entry["client_txn"], f"TPC-C {entry['name']} ClientTxn")
    verify_record_schema(validator, fixture["session_done"], "TPC-C fixture SessionDone")
    try:
        clients = verify_client_fixture(fixture, TpccCodec(contracts["tpcc"]), raw_fixture)
    except (ClientFixtureError, CodecError) as error:
        raise VerificationError(f"TPC-C client fixture failed: {error}") from error
    require(len(clients) == 8, "TPC-C client fixture accepted-client count differs")
    require(
        len(fixture["semantic_transactions"]) == 5
        and len(fixture["synthetic_join_clients"]) == 3,
        "TPC-C client fixture semantic/synthetic split differs",
    )


def load_schema_validator() -> tuple[Draft202012Validator, list[dict[str, Any]]]:
    schemas = load_schema_documents()

    common_schema = next(
        schema for schema in schemas if schema["$id"].endswith("/common.schema.json")
    )
    schema_failure_codes = set(common_schema["$defs"]["failureCode"]["enum"])
    require(
        schema_failure_codes == documented_failure_codes(),
        "RunFailed schema codes differ from documented error codes",
    )

    return schema_validator(schemas, "records.schema.json"), schemas


def verify_record_schema(
    validator: Draft202012Validator, record: dict[str, Any], label: str
) -> None:
    verify_document_schema(validator, record, label)


def verify_schemas_and_examples() -> dict[str, list[dict[str, Any]]]:
    validator, _ = load_schema_validator()

    examples_by_kind: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for path in sorted(EXAMPLE_ROOT.glob("*.json")):
        record = load_json(path)
        verify_record_schema(validator, record, path.name)
        examples_by_kind[record["kind"]].append(record)
    return examples_by_kind


def verify_adapter_record_examples(
    contracts: dict[str, dict[str, Any]], schemas: list[dict[str, Any]]
) -> int:
    validator = schema_validator(schemas, "records.schema.json")
    paths = sorted((EXAMPLE_ROOT / "adapters").glob("*.json"))
    records = [load_json(path) for path in paths]
    for path, record in zip(paths, records, strict=True):
        verify_record_schema(validator, record, f"adapters/{path.name}")
    manifests = [record for record in records if record["kind"] == "run_manifest"]
    clients = [record for record in records if record["kind"] == "client_txn"]
    require(len(manifests) == 1, "adapter record examples require one RunManifest")
    manifest = manifests[0]
    adapter = contracts.get(manifest["benchmark"])
    require(adapter is not None, "adapter RunManifest names an unregistered benchmark")
    require(
        manifest["declared_isolation"] in adapter["declared_isolations"],
        "adapter RunManifest isolation is unsupported",
    )
    require(
        len(manifest["application_table_allowlist"])
        == len(adapter["application_tables"])
        and set(manifest["application_table_allowlist"])
        == set(adapter["application_tables"]),
        "adapter RunManifest table roster mismatch",
    )
    expected_sessions = set(manifest["expected_session_ids"])
    for client in clients:
        require(client["run_id"] == manifest["run_id"], "adapter ClientTxn run mismatch")
        require(
            client["session_id"] in expected_sessions,
            "adapter ClientTxn session is not in manifest",
        )
        require(
            client["txn_type"] in adapter["transaction_types"],
            "adapter ClientTxn transaction type is not owned by adapter",
        )
        require(client["read_count"] == len(client["reads"]), "adapter ClientTxn read count")
        require(
            sha256_hex(read_digest_preimage(client["reads"])) == client["read_digest"],
            "adapter ClientTxn read digest",
        )
    return len(records)


def verify_golden_vectors() -> None:
    keys = load_json(GOLDEN_ROOT / "ycsb_keys.json")
    for vector in keys["vectors"]:
        actual = encode_ycsb_key(keys["canonical_schema"], keys["canonical_table"], vector["key"])
        check_encoded_vector(f"YCSB key {vector['key']}", vector, actual)

    values = load_json(GOLDEN_ROOT / "ycsb_values.json")
    for vector in values["vectors"]:
        actual = encode_ycsb_value(vector["fields"])
        check_encoded_vector(f"YCSB value {vector['name']}", vector, actual)

    reads = load_json(GOLDEN_ROOT / "read_digests.json")
    for vector in reads["vectors"]:
        preimage = read_digest_preimage(vector["reads"])
        if "preimage_length" in vector:
            require(len(preimage) == vector["preimage_length"], f"read {vector['name']}: length")
        require(preimage.hex() == vector["preimage_hex"], f"read {vector['name']}: preimage hex")
        if "preimage_base64" in vector:
            require(
                canonical_base64(preimage) == vector["preimage_base64"],
                f"read {vector['name']}: preimage Base64",
            )
        require(sha256_hex(preimage) == vector["digest"], f"read {vector['name']}: digest")

    bootstraps = load_json(GOLDEN_ROOT / "bootstrap_digests.json")
    for vector in bootstraps["vectors"]:
        preimage = bootstrap_digest_preimage(vector["rows"])
        if "preimage_length" in vector:
            require(len(preimage) == vector["preimage_length"], f"bootstrap {vector['name']}: length")
        require(preimage.hex() == vector["preimage_hex"], f"bootstrap {vector['name']}: preimage hex")
        if "preimage_base64" in vector:
            require(
                canonical_base64(preimage) == vector["preimage_base64"],
                f"bootstrap {vector['name']}: preimage Base64",
            )
        require(sha256_hex(preimage) == vector["digest"], f"bootstrap {vector['name']}: digest")

    correlations = load_json(GOLDEN_ROOT / "session_correlation_digests.json")
    for vector in correlations["vectors"]:
        validate_session_attempt_progress(vector["entries"], f"correlation {vector['name']}")
        seed_preimage, digest = correlation_seed(vector["run_id"], vector["session_id"])
        require(seed_preimage.hex() == vector["seed_preimage_hex"], f"correlation {vector['name']}: seed")
        require(digest.hex() == vector["seed_digest"], f"correlation {vector['name']}: D0")
        require(len(vector["entries"]) == len(vector["steps"]), "correlation step count mismatch")
        for entry_spec, step_spec in zip(vector["entries"], vector["steps"], strict=True):
            entry = correlation_entry(entry_spec["session_seq"], entry_spec["txn_id"])
            require(entry.hex() == step_spec["entry_hex"], "correlation entry hex mismatch")
            require(canonical_base64(entry) == step_spec["entry_base64"], "correlation entry Base64 mismatch")
            digest = correlation_step(digest, entry)
            require(digest.hex() == step_spec["digest"], "correlation step digest mismatch")
        require(digest.hex() == vector["final_digest"], "correlation final digest mismatch")


def verify_final_verdict(status: dict[str, Any], expected_sessions: int) -> None:
    require(status["lifecycle_state"] == "FINAL", "example status is not final")
    if status["outcome"] == "INVALID":
        return
    require(
        status["outcome"] in {"PASS", "VIOLATION"},
        "final status has a nonterminal outcome",
    )

    reconciliation = status["reconciliation"]
    require(reconciliation["evidence_reconciled"], "final verdict lacks reconciliation")
    require(
        reconciliation["startup_schema_attested"]
        and reconciliation["final_schema_attested"]
        and re.fullmatch(r"[0-9a-f]{64}", reconciliation["schema_attestation_digest"])
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
        "final verdict expected-session count mismatch",
    )
    require(
        reconciliation["completed_sessions"] == expected_sessions,
        "final verdict completed-session count mismatch",
    )
    require(
        reconciliation["accepted_client_transactions"]
        == reconciliation["joined_workload_transactions"],
        "final verdict client/joined counts differ",
    )
    require(
        reconciliation["bootstrap_cdc_transactions"]
        == reconciliation["bootstrap_checker_transactions"],
        "final verdict bootstrap counts differ",
    )
    require(
        reconciliation["phase_anchor_transactions"] == expected_sessions,
        "final verdict phase-anchor count mismatch",
    )
    expected_checker_transactions = (
        reconciliation["joined_workload_transactions"]
        + reconciliation["bootstrap_checker_transactions"]
        + reconciliation["phase_anchor_transactions"]
    )
    require(
        reconciliation["checker_consumed_transactions"]
        == expected_checker_transactions,
        "final verdict checker transaction algebra differs",
    )
    require(
        reconciliation["unmatched_client_records"] == 0
        and reconciliation["unmatched_workload_cdc_slots"] == 0
        and reconciliation["ambiguous_attempts"] == 0,
        "final verdict retains incomplete or ambiguous evidence",
    )
    require(status["pending_cdc_slots"] == 0, "final verdict retains pending CDC slots")
    require(
        status["assembled_cdc_transactions"]
        == reconciliation["bootstrap_cdc_transactions"]
        + reconciliation["joined_workload_transactions"]
        + 3,
        "final verdict assembled CDC counter differs from bootstrap/workload/fences",
    )
    require(
        status["joined_transactions"]
        == reconciliation["joined_workload_transactions"],
        "final verdict joined counters differ",
    )
    require(
        status["checked_transactions"]
        == reconciliation["checker_consumed_transactions"],
        "final verdict checked counters differ",
    )
    require(
        status["unmatched_client_records"]
        == reconciliation["unmatched_client_records"],
        "final verdict unmatched-client counters differ",
    )


def verify_cross_record_semantics(
    records: dict[str, list[dict[str, Any]]],
    contracts: dict[str, dict[str, Any]] | None = None,
    backends: dict[str, dict[str, Any]] | None = None,
) -> None:
    contracts = contracts or load_adapter_contracts()
    backends = backends or load_backend_contracts()
    manifests = records["run_manifest"]
    require(len(manifests) == 1, "examples require exactly one RunManifest")
    manifest = manifests[0]
    require(manifest["benchmark"] in contracts, "RunManifest names unknown adapter")
    require(manifest["backend"] in backends, "RunManifest names unknown backend")
    adapter = contracts[manifest["benchmark"]]
    backend = backends[manifest["backend"]]
    require(
        manifest["declared_isolation"] in adapter["declared_isolations"],
        "RunManifest isolation is unsupported by adapter",
    )
    require(
        len(manifest["application_table_allowlist"])
        == len(adapter["application_tables"])
        and set(manifest["application_table_allowlist"])
        == set(adapter["application_tables"]),
        "RunManifest table roster differs from adapter contract",
    )
    run_id = manifest["run_id"]

    attestations = records["schema_attestation"]
    require(len(attestations) == 2, "examples require STARTUP and FINAL SchemaAttestation")
    require(
        [item["phase"] for item in attestations] == ["FINAL", "STARTUP"]
        or [item["phase"] for item in attestations] == ["STARTUP", "FINAL"],
        "SchemaAttestation phases differ",
    )
    attestations_by_phase = {item["phase"]: item for item in attestations}
    for phase, attestation in attestations_by_phase.items():
        require(attestation["run_id"] == run_id, f"{phase} SchemaAttestation run mismatch")
        require(attestation["backend"] == manifest["backend"], f"{phase} SchemaAttestation backend mismatch")
        require(
            attestation["backend_source_id"] == manifest["backend_source_id"],
            f"{phase} SchemaAttestation source mismatch",
        )
        validate_schema_attestation(attestation, backend, adapter, f"{phase} SchemaAttestation")
    startup = attestations_by_phase["STARTUP"]
    final = attestations_by_phase["FINAL"]
    require(startup["tables"] == final["tables"], "startup/final table attestation drift")
    require(startup["backend_facts"] == final["backend_facts"], "startup/final backend fact drift")
    require(
        startup["canonical_attestation_digest"] == final["canonical_attestation_digest"],
        "startup/final attestation digest drift",
    )
    application_tables = set(manifest["application_table_allowlist"])
    control_tables = set(manifest["control_table_allowlist"])
    marker_table = manifest["transaction_marker_table"]
    require(marker_table not in application_tables, "marker table is in application allowlist")
    require(marker_table not in control_tables, "marker table is in control allowlist")
    require(not application_tables.intersection(control_tables), "application/control tables overlap")

    for kind, kind_records in records.items():
        for record in kind_records:
            require(record["run_id"] == run_id, f"{kind}: run_id mismatch")

    clients = records["client_txn"]
    logical_ids: set[str] = set()
    attempt_ids: set[str] = set()
    correlations: set[tuple[str, str, int]] = set()
    clients_by_session: dict[str, list[dict[str, Any]]] = defaultdict(list)
    expected_sessions = set(manifest["expected_session_ids"])

    for client in clients:
        require(
            client["txn_type"] in adapter["transaction_types"],
            "ClientTxn transaction type is not owned by selected adapter",
        )
        require(client["session_id"] in expected_sessions, "ClientTxn has unknown session")
        require(client["read_count"] == len(client["reads"]), "ClientTxn read_count mismatch")
        preimage = read_digest_preimage(client["reads"])
        require(sha256_hex(preimage) == client["read_digest"], "ClientTxn read digest mismatch")
        correlation = client["correlation_id"]
        require(correlation["run_id"] == run_id, "nested correlation run_id mismatch")
        require(correlation["session_id"] == client["session_id"], "nested session_id mismatch")
        correlation_entry(client["session_seq"], correlation["txn_id"])
        correlation_key = (
            correlation["run_id"],
            correlation["session_id"],
            correlation["txn_id"],
        )
        require(correlation_key not in correlations, "duplicate correlation ID")
        require(client["logical_txn_id"] not in logical_ids, "duplicate logical transaction ID")
        require(client["attempt_id"] not in attempt_ids, "duplicate attempt ID")
        correlations.add(correlation_key)
        logical_ids.add(client["logical_txn_id"])
        attempt_ids.add(client["attempt_id"])
        clients_by_session[client["session_id"]].append(client)

    done_records = records["session_done"]
    done_session_ids = [record["session_id"] for record in done_records]
    require(
        len(done_session_ids) == len(set(done_session_ids)),
        "duplicate SessionDone for one session",
    )
    require(set(done_session_ids) == expected_sessions, "SessionDone roster mismatch")
    for done in done_records:
        require(done["final_session_seq"] == done["logical_transactions_started"], "final session sequence mismatch")
        require(
            done["logical_transactions_started"]
            == done["successful_transactions"] + done["terminal_aborted_transactions"],
            "logical SessionDone arithmetic mismatch",
        )
        require(
            done["successful_transactions"] == done["published_transactions"],
            "successful SessionDone arithmetic mismatch",
        )
        require(
            done["physical_attempts_started"]
            == done["successful_transactions"]
            + done["definitively_aborted_attempts"]
            + done["ambiguous_attempts"],
            "attempt SessionDone arithmetic mismatch",
        )
        require(
            done["retry_attempts"]
            == done["physical_attempts_started"] - done["logical_transactions_started"],
            "retry SessionDone arithmetic mismatch",
        )
        require(done["ambiguous_attempts"] == 0, "clean SessionDone has ambiguous attempt")

        _, digest = correlation_seed(run_id, done["session_id"])
        session_clients = sorted(clients_by_session[done["session_id"]], key=lambda item: item["session_seq"])
        sequences = [client["session_seq"] for client in session_clients]
        require(sequences == sorted(set(sequences)), "published session sequence is not increasing")
        txn_ids = [client["correlation_id"]["txn_id"] for client in session_clients]
        require(txn_ids == sorted(set(txn_ids)), "marker txn_id is not increasing")
        validate_session_attempt_progress(
            [
                {
                    "session_seq": client["session_seq"],
                    "txn_id": client["correlation_id"]["txn_id"],
                }
                for client in session_clients
            ],
            f"session {done['session_id']}",
        )
        require(
            not txn_ids or txn_ids[-1] <= done["physical_attempts_started"],
            "marker txn_id exceeds physical attempt count",
        )
        validate_final_attempt_feasibility(
            [
                {
                    "session_seq": client["session_seq"],
                    "txn_id": client["correlation_id"]["txn_id"],
                }
                for client in session_clients
            ],
            done["logical_transactions_started"],
            done["physical_attempts_started"],
            f"session {done['session_id']}",
        )
        for client in session_clients:
            digest = correlation_step(
                digest,
                correlation_entry(client["session_seq"], client["correlation_id"]["txn_id"]),
            )
        require(len(session_clients) == done["published_transactions"], "published count mismatch")
        require(digest.hex() == done["rolling_correlation_digest"], "rolling correlation digest mismatch")

    bootstrap = records["bootstrap_manifest"]
    require(len(bootstrap) == 1, "examples require exactly one BootstrapManifest")
    bootstrap = bootstrap[0]
    tables = [item["table"] for item in bootstrap["table_row_counts"]]
    require(tables == sorted(set(tables)), "bootstrap tables are duplicate or unsorted")
    require(tables == sorted(manifest["application_table_allowlist"]), "bootstrap table roster mismatch")
    mixed_state = next(
        vector
        for vector in load_json(GOLDEN_ROOT / "bootstrap_digests.json")["vectors"]
        if vector["name"] == "mixed_two_rows"
    )
    require(
        sum(item["row_count"] for item in bootstrap["table_row_counts"]) == len(mixed_state["rows"]),
        "bootstrap row count mismatch",
    )
    require(
        bootstrap["canonical_state_digest"]
        == sha256_hex(bootstrap_digest_preimage(mixed_state["rows"])),
        "bootstrap manifest digest mismatch",
    )

    statuses = records["status"]
    require(len(statuses) == 1, "examples require exactly one MonitorStatus")
    status = statuses[0]
    reconciliation = status["reconciliation"]
    require(status["outcome"] == "PASS", "example status is not PASS")
    verify_final_verdict(status, len(expected_sessions))
    require(
        reconciliation["completed_sessions"] == len(done_records),
        "completed session count mismatch",
    )
    require(reconciliation["accepted_client_transactions"] == len(clients), "accepted client count mismatch")
    require(
        reconciliation["joined_workload_transactions"] == len(clients),
        "joined workload count mismatch",
    )

    error_codes = documented_failure_codes()
    for failed in records["run_failed"]:
        require(failed["reason_code"] in error_codes, "RunFailed reason_code is undocumented")


def main() -> int:
    try:
        _, schemas = load_schema_validator()
        contracts = load_adapter_contracts(schemas)
        backends = load_backend_contracts(schemas)
        verify_tpcc_executable_vectors(contracts, schemas)
        verify_tpcc_live_fixture(contracts)
        verify_tpcc_transaction_fixture(contracts)
        verify_tpcc_client_fixture(contracts, schemas)
        records = verify_schemas_and_examples()
        adapter_example_count = verify_adapter_record_examples(contracts, schemas)
        backend_example_count = verify_backend_attestation_examples(
            backends, contracts, schemas
        )
        verify_golden_vectors()
        verify_cross_record_semantics(records, contracts, backends)
    except VerificationError as error:
        print(f"protocol verification failed: {error}", file=sys.stderr)
        return 1
    print(
        "protocol verification passed: "
        f"{sum(len(records) for records in records.values()) + adapter_example_count + backend_example_count} examples, "
        f"{len(list(SCHEMA_ROOT.glob('*.schema.json')))} schemas, "
        f"{len(contracts)} adapter contracts, "
        f"{len(backends)} backend contracts"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
