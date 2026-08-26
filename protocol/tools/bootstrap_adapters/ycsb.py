#!/usr/bin/env python3
"""YCSB bootstrap adapter: legacy tabular scan and canonical row encoding."""

from __future__ import annotations

import base64
import re
import struct
from pathlib import Path
from typing import Any

from bootstrap_adapter_registry import BootstrapAdapterError


IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")


class YcsbBootstrapAdapter:
    benchmark = "ycsb"

    def build_queries(self, contract: dict[str, Any]) -> dict[str, Any]:
        table = contract["application_tables"][0]
        return {
            "benchmark": self.benchmark,
            "format": "ycsb_tsv",
            "queries": [
                {
                    "table": table,
                    "columns": [
                        {"name": column["name"], "sql_type": column["sql_type"]}
                        for column in contract["tables"][0]["columns"]
                    ],
                    "order_by": contract["tables"][0]["key_columns"],
                }
            ],
        }

    @staticmethod
    def _lp(value: bytes) -> bytes:
        if len(value) > 0xFFFFFFFF:
            raise BootstrapAdapterError("length-prefixed value exceeds U32")
        return struct.pack(">I", len(value)) + value

    @classmethod
    def _key(cls, schema: str, table: str, key: int) -> bytes:
        if not -0x80000000 <= key <= 0x7FFFFFFF:
            raise BootstrapAdapterError(f"YCSB key is outside signed I32: {key}")
        return cls._lp(schema.encode("ascii")) + cls._lp(table.encode("ascii")) + struct.pack(">i", key)

    @classmethod
    def _value(cls, tokens: list[str], line_number: int) -> bytes:
        if len(tokens) != 10:
            raise BootstrapAdapterError(
                f"scan line {line_number} does not contain ten YCSB fields"
            )
        result = bytearray()
        for field_number, token in enumerate(tokens, start=1):
            if token == "N":
                result.append(0)
                continue
            if not token.startswith("P"):
                raise BootstrapAdapterError(
                    f"scan line {line_number} FIELD{field_number} lacks N/P tag"
                )
            encoded = token[1:]
            try:
                decoded = base64.b64decode(encoded, validate=True)
                text = decoded.decode("utf-8", "strict")
            except (ValueError, UnicodeError) as error:
                raise BootstrapAdapterError(
                    f"scan line {line_number} FIELD{field_number} is invalid: {error}"
                ) from error
            if base64.b64encode(decoded).decode("ascii") != encoded:
                raise BootstrapAdapterError(
                    f"scan line {line_number} FIELD{field_number} has noncanonical Base64"
                )
            if len(text) > 100:
                raise BootstrapAdapterError(
                    f"scan line {line_number} FIELD{field_number} exceeds VARCHAR(100)"
                )
            result.append(1)
            result.extend(cls._lp(decoded))
        return bytes(result)

    def encode_scan(
        self,
        contract: dict[str, Any],
        path: Path,
        *,
        max_rows: int,
        max_input_bytes: int,
        max_canonical_bytes: int,
        options: dict[str, Any],
    ) -> tuple[list[tuple[bytes, bytes]], dict[str, int]]:
        del options
        expected_table = contract["application_tables"][0]
        schema, separator, table = expected_table.partition(".")
        if separator != ".":
            raise BootstrapAdapterError("YCSB scan table is not qualified")
        if IDENTIFIER.fullmatch(schema) is None or IDENTIFIER.fullmatch(table) is None:
            raise BootstrapAdapterError("schema/table is not a canonical lowercase identifier")
        if contract["application_tables"] != [expected_table]:
            raise BootstrapAdapterError("YCSB scan table differs from adapter roster")
        try:
            if path.stat().st_size > max_input_bytes:
                raise BootstrapAdapterError("bootstrap scan exceeds input byte limit")
        except OSError as error:
            raise BootstrapAdapterError(f"cannot stat bootstrap scan {path}: {error}") from error
        rows: list[tuple[bytes, bytes]] = []
        canonical_bytes = 0
        try:
            with path.open("r", encoding="utf-8", errors="strict") as stream:
                for line_number, line in enumerate(stream, start=1):
                    line = line.rstrip("\r\n")
                    if line == "":
                        raise BootstrapAdapterError(
                            f"bootstrap scan contains empty line {line_number}"
                        )
                    columns = line.split("\t")
                    if len(columns) != 11:
                        raise BootstrapAdapterError(
                            f"scan line {line_number} has {len(columns)} columns, expected 11"
                        )
                    try:
                        key_number = int(columns[0], 10)
                    except ValueError as error:
                        raise BootstrapAdapterError(
                            f"scan line {line_number} has non-integer YCSB key"
                        ) from error
                    key = self._key(schema, table, key_number)
                    value = self._value(columns[1:], line_number)
                    rows.append((key, value))
                    canonical_bytes += len(key) + len(value)
                    if len(rows) > max_rows:
                        raise BootstrapAdapterError("bootstrap scan exceeds row limit")
                    if canonical_bytes > max_canonical_bytes:
                        raise BootstrapAdapterError("bootstrap scan exceeds canonical byte limit")
        except (OSError, UnicodeError) as error:
            raise BootstrapAdapterError(f"cannot read bootstrap scan {path}: {error}") from error
        keys = [key for key, _ in rows]
        if len(keys) != len(set(keys)):
            raise BootstrapAdapterError("bootstrap scan contains duplicate canonical key")
        rows.sort(key=lambda item: item[0])
        return rows, {expected_table: len(rows)}


ADAPTER = YcsbBootstrapAdapter()
