#!/usr/bin/env python3
"""Validate an Oracle physical catalog against one logical adapter contract."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def parse_metadata(text: str) -> dict[str, Any]:
    result: dict[str, Any] = {"database": None, "tables": {}}
    for number, line in enumerate(text.splitlines(), start=1):
        require(line != "", f"empty catalog line {number}")
        fields = line.split("\t")
        kind = fields[0]
        if kind == "DATABASE":
            require(len(fields) == 4 and result["database"] is None, "database record differs")
            result["database"] = fields[1:]
        elif kind == "TABLE":
            require(len(fields) == 4, f"table record {number} differs")
            name = fields[1]
            require(name not in result["tables"], f"duplicate table {name}")
            result["tables"][name] = {
                "supplemental": fields[2],
                "temporary": fields[3],
                "columns": [],
                "primary": [],
            }
        elif kind == "COLUMN":
            require(len(fields) == 11, f"column record {number} differs")
            table = result["tables"].get(fields[1])
            require(table is not None, f"column names unknown table {fields[1]}")
            table["columns"].append(fields[2:])
        elif kind == "PRIMARY":
            require(len(fields) == 4, f"primary-key record {number} differs")
            table = result["tables"].get(fields[1])
            require(table is not None, f"primary key names unknown table {fields[1]}")
            table["primary"].append((int(fields[3]), fields[2]))
        else:
            raise ValueError(f"unknown catalog record {kind}")
    require(result["database"] == ["FREEPDB1", "AL32UTF8", "BINARY"], "database identity/NLS differs")
    return result


def physical_type(column: dict[str, Any]) -> tuple[str, str, str, str]:
    sql_type = column["sql_type"]
    if sql_type == "INT":
        return "NUMBER", "10", "0", "-"
    if sql_type == "BIGINT":
        return "NUMBER", "19", "0", "-"
    if sql_type == "FLOAT":
        return "BINARY_FLOAT", "-", "-", "-"
    if sql_type == "TIMESTAMP":
        return "TIMESTAMP(0)", "-", "0", "-"
    if sql_type.startswith("DECIMAL("):
        precision, scale = sql_type.removeprefix("DECIMAL(").removesuffix(")").split(",")
        return "NUMBER", precision, scale, "-"
    if sql_type.startswith("VARCHAR("):
        length = sql_type.removeprefix("VARCHAR(").removesuffix(")")
        return "VARCHAR2", "-", "-", length
    if sql_type.startswith("CHAR("):
        length = sql_type.removeprefix("CHAR(").removesuffix(")")
        return "CHAR", "-", "-", length
    raise ValueError(f"Oracle mapping is not defined for {sql_type}")


def common_tables() -> list[dict[str, Any]]:
    return [
        {
            "name": "benchbase.checker_run_fence",
            "key_columns": ["run_id", "phase"],
            "columns": [
                {"name": "run_id", "sql_type": "CHAR(36)", "nullable": False},
                {"name": "phase", "sql_type": "VARCHAR(32)", "nullable": False},
                {"name": "nonce", "sql_type": "CHAR(36)", "nullable": False},
            ],
        },
        {
            "name": "benchbase.session_txn",
            "key_columns": ["session_id", "txn_id"],
            "columns": [
                {"name": "session_id", "sql_type": "VARCHAR(64)", "nullable": False},
                {"name": "txn_id", "sql_type": "BIGINT", "nullable": False},
            ],
        },
    ]


def validate(metadata: dict[str, Any], adapter: dict[str, Any]) -> None:
    contracts = [*adapter["tables"], *common_tables()]
    expected_names = {table["name"] for table in contracts}
    require(set(metadata["tables"]) == expected_names, "Oracle table roster differs")
    for contract in contracts:
        actual = metadata["tables"][contract["name"]]
        require(actual["supplemental"] == "ALL_COLUMNS", f"supplemental logging differs for {contract['name']}")
        require(actual["temporary"] == "N", f"temporary-table status differs for {contract['name']}")
        primary = [name for _, name in sorted(actual["primary"])]
        require(primary == contract["key_columns"], f"primary key differs for {contract['name']}")
        require(len(actual["columns"]) == len(contract["columns"]), f"column count differs for {contract['name']}")
        for ordinal, (observed, expected) in enumerate(zip(actual["columns"], contract["columns"], strict=True), start=1):
            name, position, data_type, nullable, precision, scale, char_length, char_used, identity = observed
            expected_type, expected_precision, expected_scale, expected_length = physical_type(expected)
            require(name == expected["name"] and int(position) == ordinal, f"column identity differs for {contract['name']}")
            require(data_type == expected_type, f"column type differs for {contract['name']}.{name}")
            require(nullable == ("Y" if expected["nullable"] else "N"), f"nullability differs for {contract['name']}.{name}")
            require((precision, scale, char_length) == (expected_precision, expected_scale, expected_length), f"type parameters differ for {contract['name']}.{name}")
            require(char_used == ("C" if expected_type in {"CHAR", "VARCHAR2"} else "-"), f"character semantics differ for {contract['name']}.{name}")
            expected_identity = "YES" if expected.get("generated") == "auto_increment" else "NO"
            require(identity == expected_identity, f"generated column differs for {contract['name']}.{name}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--adapter", type=Path, required=True)
    args = parser.parse_args()
    adapter = json.loads(args.adapter.read_text(encoding="utf-8"))
    validate(parse_metadata(sys.stdin.read()), adapter)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise SystemExit(f"Oracle catalog validation failed: {error}")
