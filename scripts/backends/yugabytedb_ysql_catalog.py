#!/usr/bin/env python3
"""Validate a YugabyteDB YSQL catalog scan and emit normalized attestation tables."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]


class CatalogError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise CatalogError(message)


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
            "key_columns": ["session_id"],
            "columns": [
                {"name": "session_id", "sql_type": "VARCHAR(64)", "nullable": False},
                {"name": "txn_id", "sql_type": "BIGINT", "nullable": False},
            ],
        },
    ]


def parse(text: str) -> dict[str, Any]:
    result: dict[str, Any] = {"database": None, "tables": {}}
    for number, line in enumerate(text.splitlines(), start=1):
        require(bool(line), f"empty catalog line {number}")
        fields = line.split("\t")
        kind = fields[0]
        if kind == "DATABASE":
            require(len(fields) == 4 and result["database"] is None, "invalid DATABASE record")
            result["database"] = fields[1:]
        elif kind == "TABLE":
            require(len(fields) == 4 and fields[1] not in result["tables"], "invalid TABLE record")
            result["tables"][fields[1]] = {
                "relkind": fields[2],
                "replica_identity": fields[3],
                "columns": [],
                "primary_key": [],
            }
        elif kind == "COLUMN":
            require(len(fields) == 13 and fields[1] in result["tables"], "invalid COLUMN record")
            result["tables"][fields[1]]["columns"].append(
                {
                    "name": fields[2],
                    "ordinal": int(fields[3]),
                    "data_type": fields[4],
                    "nullable": fields[5] == "YES",
                    "precision": fields[6],
                    "scale": fields[7],
                    "length": fields[8],
                    "datetime_precision": fields[9],
                    "identity": fields[12] == "identity",
                }
            )
        elif kind == "PRIMARY":
            require(len(fields) == 4 and fields[1] in result["tables"], "invalid PRIMARY record")
            result["tables"][fields[1]]["primary_key"].append((int(fields[3]), fields[2]))
        else:
            raise CatalogError(f"unknown catalog record {kind} on line {number}")
    require(result["database"] is not None, "database catalog record is missing")
    return result


def physical_type(logical: str) -> tuple[str, str | None]:
    if logical == "INT":
        return "integer", None
    if logical == "BIGINT":
        return "bigint", None
    if logical == "FLOAT":
        return "real", None
    if logical == "TIMESTAMP":
        # Logical TPC-C timestamps are frozen as UTC whole seconds; the YSQL
        # physical mapping is TIMESTAMPTZ(0) so pgoutput emits ISO UTC text.
        return "timestamp with time zone", "datetime:0"
    match = re.fullmatch(r"(VARCHAR|CHAR)\(([1-9][0-9]*)\)", logical)
    if match:
        return ("character varying" if match.group(1) == "VARCHAR" else "character"), match.group(2)
    match = re.fullmatch(r"DECIMAL\(([1-9][0-9]*),([0-9]+)\)", logical)
    if match:
        return "numeric", f"{match.group(1)}:{match.group(2)}"
    raise CatalogError(f"unsupported logical SQL type: {logical}")


def validate(benchmark: str, metadata: dict[str, Any]) -> list[dict[str, Any]]:
    adapter = json.loads((ROOT / "protocol" / "adapters" / f"{benchmark}.json").read_text())
    database_name, encoding, _collation = metadata["database"]
    require(database_name == "benchbase" and encoding == "UTF8", "database identity/encoding differs")
    contracts = adapter["tables"] + common_tables()
    expected_names = {item["name"] for item in contracts}
    require(set(metadata["tables"]) == expected_names, "table roster differs")
    normalized: list[dict[str, Any]] = []
    for contract in contracts:
        actual = metadata["tables"][contract["name"]]
        require(
            actual["relkind"] == "r" and actual["replica_identity"] == "f",
            f"{contract['name']}: relation/replica identity differs",
        )
        columns = sorted(actual["columns"], key=lambda item: item["ordinal"])
        require(len(columns) == len(contract["columns"]), f"{contract['name']}: column count differs")
        for ordinal, (observed, expected) in enumerate(
            zip(columns, contract["columns"], strict=True), start=1
        ):
            expected_type, modifier = physical_type(expected["sql_type"])
            require(
                observed["ordinal"] == ordinal
                and observed["name"] == expected["name"]
                and observed["data_type"] == expected_type
                and observed["nullable"] == expected["nullable"],
                f"{contract['name']}.{expected['name']}: column contract differs",
            )
            if modifier and ":" not in modifier:
                require(observed["length"] == modifier, f"{contract['name']}: character length differs")
            if modifier and modifier.startswith("datetime:"):
                require(
                    observed["datetime_precision"] == modifier.partition(":")[2],
                    f"{contract['name']}: timestamp precision differs",
                )
            elif modifier and ":" in modifier:
                precision, scale = modifier.split(":")
                require(
                    observed["precision"] == precision and observed["scale"] == scale,
                    f"{contract['name']}: numeric typmod differs",
                )
            require(
                observed["identity"] == ("generated" in expected),
                f"{contract['name']}: generated identity differs",
            )
        primary = [name for _, name in sorted(actual["primary_key"])]
        require(primary == contract["key_columns"], f"{contract['name']}: primary key differs")
        normalized.append(
            {
                "canonical_table_name": contract["name"],
                "primary_key_columns": contract["key_columns"],
                "columns": [
                    {
                        "name": column["name"],
                        "logical_sql_type": column["sql_type"],
                        "nullable": column["nullable"],
                        **({"generated": True} if "generated" in column else {}),
                    }
                    for column in contract["columns"]
                ],
            }
        )
    return sorted(normalized, key=lambda item: item["canonical_table_name"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", required=True)
    parser.add_argument("--emit-tables", action="store_true")
    args = parser.parse_args()
    try:
        tables = validate(args.benchmark, parse(sys.stdin.read()))
    except (CatalogError, OSError, ValueError, json.JSONDecodeError) as error:
        print(f"YugabyteDB catalog error: {error}", file=sys.stderr)
        return 2
    if args.emit_tables:
        print(json.dumps(tables, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
