#!/usr/bin/env python3
"""Strict reference implementation of the shared TPC-C canonical row codec."""

from __future__ import annotations

import copy
import json
import math
import re
import struct
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


I32_MIN = -(1 << 31)
I32_MAX = (1 << 31) - 1
I64_MIN = -(1 << 63)
I64_MAX = (1 << 63) - 1
U32_MAX = (1 << 32) - 1
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
DECIMAL_TYPE = re.compile(r"DECIMAL\(([1-9][0-9]*),([0-9]+)\)\Z")
STRING_TYPE = re.compile(r"(VARCHAR|CHAR)\(([1-9][0-9]*)\)\Z")
DECIMAL_TEXT = re.compile(r"(-?)(0|[1-9][0-9]*)\.([0-9]+)\Z")
FLOAT_BITS = re.compile(r"[0-9a-f]{8}\Z")
FLOAT_TEXT = re.compile(
    r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z"
)
UTC_SECOND = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z\Z"
)


class CodecError(ValueError):
    """A typed value or row is outside the canonical TPC-C contract."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def fail(code: str, message: str) -> None:
    raise CodecError(code, message)


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            fail("row_shape", f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def load_json(path: Path) -> Any:
    try:
        return json.loads(
            path.read_text(encoding="utf-8", errors="strict"),
            object_pairs_hook=reject_duplicate_keys,
        )
    except CodecError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        fail("row_shape", f"cannot load {path}: {error}")


def lp(value: bytes) -> bytes:
    if len(value) > U32_MAX:
        fail("text_length", "length-prefixed value exceeds U32")
    return struct.pack(">I", len(value)) + value


def encode_identifier(value: str, label: str) -> bytes:
    if IDENTIFIER.fullmatch(value) is None:
        fail("row_shape", f"{label} is not a canonical identifier: {value}")
    return lp(value.encode("ascii"))


def require_integer(value: Any, minimum: int, maximum: int, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        fail("integer_range", f"{label} is not an integer")
    if not minimum <= value <= maximum:
        fail("integer_range", f"{label} is outside [{minimum}, {maximum}]")
    return value


def encode_decimal(value: Any, precision: int, scale: int, label: str) -> bytes:
    if not isinstance(value, str):
        fail("decimal_format", f"{label} decimal is not a string")
    match = DECIMAL_TEXT.fullmatch(value)
    if match is None:
        fail("decimal_format", f"{label} decimal is not canonical plain text")
    sign, integer_digits, fractional_digits = match.groups()
    if len(fractional_digits) != scale:
        fail("decimal_scale", f"{label} decimal scale is not {scale}")
    if sign and int(integer_digits) == 0 and int(fractional_digits or "0") == 0:
        fail("decimal_format", f"{label} decimal uses negative zero")
    unscaled = int(integer_digits) * (10**scale) + int(fractional_digits or "0")
    if sign:
        unscaled = -unscaled
    if abs(unscaled) > (10**precision) - 1 or not I64_MIN <= unscaled <= I64_MAX:
        fail("decimal_range", f"{label} decimal exceeds DECIMAL({precision},{scale})/I64")
    return struct.pack(">q", unscaled)


def encode_float32(value: Any, label: str) -> bytes:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric_text = repr(value)
        parsed = float(value)
    elif isinstance(value, str):
        if FLOAT_TEXT.fullmatch(value) is None:
            fail("float_format", f"{label} float32 text is not canonical finite decimal")
        numeric_text = value
        try:
            parsed = float(value)
        except ValueError as error:  # pragma: no cover - regex already excludes this
            fail("float_format", f"{label} float32 text is invalid: {error}")
    else:
        numeric_text = ""
        parsed = 0.0
    if numeric_text:
        try:
            encoded = struct.pack(">f", parsed)
        except OverflowError as error:
            fail("float_non_finite", f"{label} float32 is out of finite range: {error}")
        if not math.isfinite(parsed):
            fail("float_non_finite", f"{label} float32 is non-finite")
        bits = struct.unpack(">I", encoded)[0]
        if bits & 0x7F800000 == 0x7F800000:
            fail("float_non_finite", f"{label} float32 rounded to non-finite")
        try:
            exact_decimal = Decimal(numeric_text)
        except InvalidOperation as error:  # pragma: no cover - regex already excludes this
            fail("float_format", f"{label} float32 is invalid: {error}")
        if exact_decimal != 0 and bits & 0x7FFFFFFF == 0:
            fail("float_range", f"{label} float32 underflows to zero")
        return encoded
    if not isinstance(value, dict) or set(value) != {"raw_bits_hex"}:
        fail("float_format", f"{label} float32 must contain raw_bits_hex")
    raw_bits = value["raw_bits_hex"]
    if not isinstance(raw_bits, str) or FLOAT_BITS.fullmatch(raw_bits) is None:
        fail("float_format", f"{label} raw_bits_hex is not eight lowercase hex digits")
    bits = int(raw_bits, 16)
    if bits & 0x7F800000 == 0x7F800000:
        fail("float_non_finite", f"{label} float32 is non-finite")
    return struct.pack(">I", bits)


def encode_timestamp(value: Any, label: str) -> bytes:
    if not isinstance(value, str):
        fail("timestamp_format", f"{label} timestamp is not a string")
    match = UTC_SECOND.fullmatch(value)
    if match is None:
        fail("timestamp_format", f"{label} timestamp is not UTC second precision")
    try:
        year, month, day, hour, minute, second = map(int, match.groups())
        instant = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    except ValueError as error:
        fail("timestamp_format", f"{label} timestamp is invalid: {error}")
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = instant - epoch
    epoch_seconds = delta.days * 86400 + delta.seconds
    if not I64_MIN <= epoch_seconds <= I64_MAX:  # pragma: no cover - datetime is narrower
        fail("timestamp_range", f"{label} timestamp epoch seconds exceed I64")
    return struct.pack(">q", epoch_seconds)


def encode_text(value: Any, maximum_characters: int, fixed: bool, label: str) -> bytes:
    if isinstance(value, dict) and set(value) == {"utf8_hex"}:
        try:
            encoded = bytes.fromhex(value["utf8_hex"])
            encoded.decode("utf-8", "strict")
        except (TypeError, ValueError, UnicodeDecodeError) as error:
            fail("utf8", f"{label} contains malformed UTF-8: {error}")
        fail("text_format", f"{label} must be a JSON string")
    if not isinstance(value, str):
        fail("text_format", f"{label} must be a JSON string")
    if len(value) > maximum_characters:
        fail("text_length", f"{label} exceeds {maximum_characters} Unicode characters")
    if fixed:
        value = value + (" " * (maximum_characters - len(value)))
    try:
        encoded = value.encode("utf-8", "strict")
    except UnicodeEncodeError as error:
        fail("utf8", f"{label} is not strict UTF-8: {error}")
    return lp(encoded)


class TpccCodec:
    def __init__(self, contract: dict[str, Any]):
        if contract.get("benchmark") != "tpcc":
            fail("row_shape", "TPC-C codec received another adapter contract")
        self.contract = contract
        self.tables = {table["name"]: table for table in contract["tables"]}

    @classmethod
    def from_protocol_root(cls, protocol_root: Path) -> "TpccCodec":
        contract = load_json(protocol_root / "adapters" / "tpcc.json")
        if not isinstance(contract, dict):
            fail("row_shape", "TPC-C adapter contract is not an object")
        return cls(contract)

    def table(self, table_name: str) -> dict[str, Any]:
        table = self.tables.get(table_name)
        if table is None:
            fail("row_shape", f"unknown TPC-C table: {table_name}")
        return table

    def encode_nonnull(self, column: dict[str, Any], value: Any, label: str) -> bytes:
        sql_type = column["sql_type"]
        if sql_type == "INT":
            return struct.pack(">i", require_integer(value, I32_MIN, I32_MAX, label))
        if sql_type == "BIGINT":
            return struct.pack(">q", require_integer(value, I64_MIN, I64_MAX, label))
        if sql_type == "FLOAT":
            return encode_float32(value, label)
        if sql_type == "TIMESTAMP":
            return encode_timestamp(value, label)
        decimal = DECIMAL_TYPE.fullmatch(sql_type)
        if decimal is not None:
            return encode_decimal(value, int(decimal.group(1)), int(decimal.group(2)), label)
        string = STRING_TYPE.fullmatch(sql_type)
        if string is not None:
            return encode_text(value, int(string.group(2)), string.group(1) == "CHAR", label)
        fail("row_shape", f"{label} has unsupported SQL type {sql_type}")

    def encode_column(self, column: dict[str, Any], value: Any, label: str) -> bytes:
        if value is None:
            if not column["nullable"]:
                fail("nullability", f"{label} is non-nullable")
            return b"\x00"
        encoded = self.encode_nonnull(column, value, label)
        return (b"\x01" + encoded) if column["nullable"] else encoded

    def encode_row(self, table_name: str, row: dict[str, Any]) -> tuple[bytes, bytes]:
        table = self.table(table_name)
        if not isinstance(row, dict):
            fail("row_shape", f"{table_name} row is not an object")
        columns = table["columns"]
        expected_columns = [column["name"] for column in columns]
        if len(row) != len(expected_columns) or set(row) != set(expected_columns):
            fail("row_shape", f"{table_name} row columns differ from table contract")
        columns_by_name = {column["name"]: column for column in columns}
        schema_name, table_separator, unqualified_table = table_name.partition(".")
        if table_separator != ".":  # pragma: no cover - adapter schema already checks this
            fail("row_shape", f"unqualified table name: {table_name}")
        key = bytearray(
            encode_identifier(schema_name, "schema")
            + encode_identifier(unqualified_table, "table")
        )
        for column_name in table["key_columns"]:
            column = columns_by_name[column_name]
            if row[column_name] is None:
                fail("nullability", f"{table_name}.{column_name} primary key is null")
            key.extend(self.encode_nonnull(column, row[column_name], f"{table_name}.{column_name}"))
        value = bytearray()
        for column_name in table["value_columns"]:
            column = columns_by_name[column_name]
            value.extend(self.encode_column(column, row[column_name], f"{table_name}.{column_name}"))
        return bytes(key), bytes(value)


def materialize_invalid_vector(
    vector: dict[str, Any], valid_vectors: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    base_name = vector["base_vector"]
    base = valid_vectors.get(base_name)
    if base is None:
        fail("row_shape", f"invalid vector names unknown base: {base_name}")
    if base["table"] != vector["table"]:
        fail("row_shape", f"invalid vector table differs from base: {vector['name']}")
    row = copy.deepcopy(base["row"])
    mutations = vector["mutations"]
    if not isinstance(mutations, dict) or set(mutations) != {"set", "remove", "add"}:
        fail("row_shape", f"invalid vector mutations are malformed: {vector['name']}")
    for column in mutations["remove"]:
        row.pop(column, None)
    row.update(copy.deepcopy(mutations["set"]))
    row.update(copy.deepcopy(mutations["add"]))
    return row


def scan_json_lines(
    codec: TpccCodec,
    path: Path,
    max_rows: int,
    max_input_bytes: int,
    max_canonical_bytes: int,
) -> tuple[list[tuple[bytes, bytes]], dict[str, int]]:
    if not 1 <= max_rows:
        fail("row_shape", "max_rows must be positive")
    if not 1 <= max_input_bytes or not 1 <= max_canonical_bytes:
        fail("row_shape", "bootstrap byte limits must be positive")
    try:
        input_size = path.stat().st_size
    except OSError as error:
        fail("row_shape", f"cannot stat bootstrap scan {path}: {error}")
    if input_size > max_input_bytes:
        fail("row_shape", "bootstrap scan exceeds input byte limit")
    rows: list[tuple[bytes, bytes]] = []
    counts = {table: 0 for table in codec.contract["application_tables"]}
    canonical_bytes = 0
    try:
        with path.open("r", encoding="utf-8", errors="strict") as stream:
            for line_number, line in enumerate(stream, start=1):
                if line.endswith("\n"):
                    line = line[:-1]
                if line.endswith("\r"):
                    line = line[:-1]
                if line == "":
                    fail("row_shape", f"bootstrap scan has empty line {line_number}")
                try:
                    record = json.loads(line, object_pairs_hook=reject_duplicate_keys)
                except CodecError:
                    raise
                except json.JSONDecodeError as error:
                    fail("row_shape", f"bootstrap line {line_number} is invalid JSON: {error}")
                if not isinstance(record, dict) or set(record) != {"table", "row"}:
                    fail("row_shape", f"bootstrap line {line_number} has wrong fields")
                table_name = record["table"]
                if not isinstance(table_name, str):
                    fail("row_shape", f"bootstrap line {line_number} table is not a string")
                if table_name not in counts:
                    fail("row_shape", f"bootstrap line {line_number} names unknown table")
                key, value = codec.encode_row(table_name, record["row"])
                rows.append((key, value))
                counts[table_name] += 1
                canonical_bytes += len(key) + len(value)
                if len(rows) > max_rows:
                    fail("row_shape", "bootstrap scan exceeds row limit")
                if canonical_bytes > max_canonical_bytes:
                    fail("row_shape", "bootstrap scan exceeds canonical byte limit")
    except CodecError:
        raise
    except (OSError, UnicodeError) as error:
        fail("utf8", f"cannot read bootstrap scan {path}: {error}")
    keys = [key for key, _ in rows]
    if len(keys) != len(set(keys)):
        fail("row_shape", "bootstrap scan contains duplicate canonical key")
    rows.sort(key=lambda item: item[0])
    return rows, counts


def quote_sql_identifier(identifier: str) -> str:
    if IDENTIFIER.fullmatch(identifier) is None:
        fail("row_shape", f"SQL identifier is not canonical: {identifier}")
    return f"`{identifier}`"


def bootstrap_select_queries(codec: TpccCodec) -> list[dict[str, str]]:
    queries: list[dict[str, str]] = []
    for table_name in codec.contract["application_tables"]:
        table = codec.table(table_name)
        schema_name, _, unqualified_table = table_name.partition(".")
        row_arguments: list[str] = []
        for column in table["columns"]:
            name = column["name"]
            rendered_column = quote_sql_identifier(name)
            sql_type = column["sql_type"]
            if DECIMAL_TYPE.fullmatch(sql_type) is not None or sql_type == "FLOAT":
                expression = f"CAST({rendered_column} AS CHAR)"
            elif sql_type == "TIMESTAMP":
                expression = f"DATE_FORMAT({rendered_column}, '%Y-%m-%dT%H:%i:%sZ')"
            else:
                expression = rendered_column
            row_arguments.extend((f"'{name}'", expression))
        row_object = "JSON_OBJECT(" + ", ".join(row_arguments) + ")"
        order = ", ".join(quote_sql_identifier(column) for column in table["key_columns"])
        sql = (
            "SELECT JSON_OBJECT('table', '"
            + table_name
            + "', 'row', "
            + row_object
            + ") FROM "
            + quote_sql_identifier(schema_name)
            + "."
            + quote_sql_identifier(unqualified_table)
            + " ORDER BY "
            + order
        )
        queries.append({"table": table_name, "sql": sql})
    return queries
