#!/usr/bin/env python3
"""Strict canonical SmallBank row codec shared by bootstrap tooling and tests."""

from __future__ import annotations

import json
import re
import struct
from pathlib import Path
from typing import Any


class SmallBankCodecError(ValueError):
    pass


DECIMAL = re.compile(r"-?(?:0|[1-9][0-9]*)\.[0-9]{2}\Z")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SmallBankCodecError(message)


def lp(value: bytes) -> bytes:
    require(len(value) <= 0xFFFFFFFF, "length-prefixed value exceeds U32")
    return struct.pack(">I", len(value)) + value


class SmallBankCodec:
    def __init__(self, contract: dict[str, Any]):
        require(contract.get("benchmark") == "smallbank", "contract is not SmallBank")
        self.tables = {table["name"]: table for table in contract["tables"]}
        require(
            tuple(self.tables) == (
                "benchbase.accounts",
                "benchbase.checking",
                "benchbase.savings",
            ),
            "SmallBank table roster differs",
        )

    @staticmethod
    def _i64(value: Any) -> bytes:
        require(type(value) is int, "custid is not an integer")
        require(-(1 << 63) <= value < (1 << 63), "custid exceeds signed I64")
        return struct.pack(">q", value)

    @staticmethod
    def _decimal(value: Any) -> bytes:
        require(isinstance(value, str) and DECIMAL.fullmatch(value) is not None, "balance is not canonical scale-2 decimal text")
        negative = value.startswith("-")
        digits = value[1:] if negative else value
        integer, fraction = digits.split(".", 1)
        require(len(integer.lstrip("0")) + 2 <= 18, "balance exceeds DECIMAL(18,2)")
        unscaled = int(integer) * 100 + int(fraction)
        if negative:
            unscaled = -unscaled
        require(-(1 << 63) <= unscaled < (1 << 63), "balance unscaled value exceeds I64")
        return struct.pack(">q", unscaled)

    @staticmethod
    def _name(value: Any) -> bytes:
        require(isinstance(value, str), "account name is not text")
        encoded = value.encode("utf-8", "strict")
        require(len(value) <= 64, "account name exceeds VARCHAR(64)")
        return lp(encoded)

    def encode_row(self, table_name: str, row: dict[str, Any]) -> tuple[bytes, bytes]:
        table = self.tables.get(table_name)
        require(table is not None, "row names an unknown SmallBank table")
        expected = [column["name"] for column in table["columns"]]
        require(isinstance(row, dict) and list(row) == expected, "row column roster/order differs")
        custid = self._i64(row["custid"])
        _, leaf = table_name.split(".", 1)
        key = lp(b"benchbase") + lp(leaf.encode("ascii")) + custid
        value = self._name(row["name"]) if leaf == "accounts" else self._decimal(row["bal"])
        return key, value

    def query_plan(self) -> list[dict[str, Any]]:
        return [
            {
                "table": table["name"],
                "columns": [
                    {"name": column["name"], "sql_type": column["sql_type"]}
                    for column in table["columns"]
                ],
                "order_by": table["key_columns"],
            }
            for table in self.tables.values()
        ]


def scan_json_lines(
    codec: SmallBankCodec,
    path: Path,
    max_rows: int,
    max_input_bytes: int,
    max_canonical_bytes: int,
) -> tuple[list[tuple[bytes, bytes]], dict[str, int]]:
    require(path.stat().st_size <= max_input_bytes, "bootstrap scan exceeds input byte limit")
    rows: list[tuple[bytes, bytes]] = []
    counts = {table: 0 for table in codec.tables}
    retained = 0
    with path.open("r", encoding="utf-8", errors="strict") as stream:
        for line_number, line in enumerate(stream, start=1):
            require(line.endswith("\n"), f"scan line {line_number} lacks newline")
            document = json.loads(line)
            require(
                isinstance(document, dict) and list(document) == ["table", "row"],
                f"scan line {line_number} wrapper differs",
            )
            table = document["table"]
            key, value = codec.encode_row(table, document["row"])
            rows.append((key, value))
            counts[table] += 1
            retained += len(key) + len(value)
            require(len(rows) <= max_rows, "bootstrap scan exceeds row limit")
            require(retained <= max_canonical_bytes, "bootstrap scan exceeds canonical byte limit")
    keys = [key for key, _ in rows]
    require(len(keys) == len(set(keys)), "bootstrap scan contains duplicate canonical key")
    rows.sort(key=lambda item: item[0])
    return rows, counts
