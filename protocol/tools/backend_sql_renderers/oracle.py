#!/usr/bin/env python3
"""Oracle SQL rendering for logical loader and canonical bootstrap plans."""

from __future__ import annotations

import re
from typing import Any


IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
DECIMAL = re.compile(r"DECIMAL\(([1-9][0-9]*),([0-9]+)\)\Z")


def physical(identifier: str) -> str:
    if IDENTIFIER.fullmatch(identifier) is None:
        raise ValueError(f"invalid logical SQL identifier: {identifier}")
    return f'"{identifier.upper()}"'


def qualified(table: str) -> str:
    schema, separator, leaf = table.partition(".")
    if separator != "." or schema != "benchbase":
        raise ValueError(f"invalid canonical Oracle table: {table}")
    return f'"BLACKBOX_OWNER".{physical(leaf)}'


def decimal_text(column: str, sql_type: str) -> str:
    match = DECIMAL.fullmatch(sql_type)
    if match is None:
        raise ValueError(f"invalid decimal type: {sql_type}")
    precision, scale = (int(value) for value in match.groups())
    integer_digits = precision - scale
    pattern = "FM" + ("9" * max(0, integer_digits - 1)) + "0"
    if scale:
        pattern += "D" + ("0" * scale)
    return (
        f"TO_CHAR({column}, '{pattern}', "
        "'NLS_NUMERIC_CHARACTERS=''.,''')"
    )


def relationship_sql(spec: dict[str, Any]) -> str:
    parent_keys = spec["parent_key_columns"]
    child_keys = spec["child_parent_columns"]
    join = " AND ".join(
        f"p.{physical(parent)} = c.{physical(child)}"
        for parent, child in zip(parent_keys, child_keys, strict=True)
    )
    selected = ", ".join(f"p.{physical(column)}" for column in parent_keys)
    expected = f"p.{physical(spec['expected_count_column'])}"
    grouped = ", ".join(
        [*(f"p.{physical(column)}" for column in parent_keys), expected]
    )
    sequence = f"c.{physical(spec['child_sequence_column'])}"
    return (
        "SELECT COUNT(*) FROM ("
        f"SELECT {selected}, {expected} expected_count, "
        f"COUNT({sequence}) line_count, "
        f"COUNT(DISTINCT {sequence}) distinct_line_count, "
        f"MIN({sequence}) minimum_sequence, MAX({sequence}) maximum_sequence "
        f"FROM {qualified(spec['parent_table'])} p "
        f"LEFT JOIN {qualified(spec['child_table'])} c ON {join} GROUP BY {grouped}"
        ") grouped_rows WHERE "
        f"expected_count < {spec['minimum_count']} OR "
        f"expected_count > {spec['maximum_count']} OR "
        "line_count <> expected_count OR distinct_line_count <> expected_count OR "
        f"COALESCE(minimum_sequence, 0) <> {spec['first_sequence_value']} OR "
        f"COALESCE(maximum_sequence, 0) <> ({spec['first_sequence_value']} + "
        f"(expected_count - 1) * {spec['sequence_step']})"
    )


class OracleRenderer:
    backend = "oracle"

    def loader_queries(self, adapter: dict[str, Any]) -> list[dict[str, Any]]:
        queries = [
            {
                "observation": "count",
                "table": table,
                "sql": f"SELECT COUNT(*) FROM {qualified(table)}",
            }
            for table in adapter["application_tables"]
        ]
        for cardinality in adapter["loader"]["cardinalities"]:
            if cardinality["kind"] == "sum_column":
                queries.append(
                    {
                        "observation": "sum_column",
                        "table": cardinality["table"],
                        "source_table": cardinality["source_table"],
                        "source_column": cardinality["source_column"],
                        "sql": (
                            f"SELECT COALESCE(SUM({physical(cardinality['source_column'])}), 0) "
                            f"FROM {qualified(cardinality['source_table'])}"
                        ),
                    }
                )
        for index, relationship in enumerate(adapter["loader"]["relationships"]):
            queries.append(
                {
                    "observation": "relationship_violations",
                    "relationship_index": index,
                    "sql": relationship_sql(relationship),
                }
            )
        return queries

    def bootstrap_queries(
        self, adapter: dict[str, Any], logical_plan: dict[str, Any]
    ) -> list[dict[str, Any]]:
        del adapter
        queries: list[dict[str, Any]] = []
        for plan in logical_plan["queries"]:
            if logical_plan["format"] == "ycsb_tsv":
                fields = [
                    f"CASE WHEN {physical(column['name'])} IS NULL THEN 'N' ELSE "
                    "'P'||REPLACE(REPLACE(UTL_RAW.CAST_TO_VARCHAR2("
                    f"UTL_ENCODE.BASE64_ENCODE(UTL_RAW.CAST_TO_RAW({physical(column['name'])}))),"
                    "CHR(10),''),CHR(13),'') END"
                    for column in plan["columns"][1:]
                ]
                line = "||CHR(9)||".join(
                    [f"TO_CHAR({physical(plan['columns'][0]['name'])}, 'FM9999999990')", *fields]
                )
                sql = (
                    f"SELECT {line} "
                    f"FROM {qualified(plan['table'])} ORDER BY "
                    + ", ".join(physical(value) for value in plan["order_by"])
                )
                queries.append({"table": plan["table"], "sql": sql})
                continue
            if logical_plan["format"] not in {"smallbank_jsonl", "tpcc_jsonl"}:
                raise ValueError("Oracle renderer has no admitted bootstrap format")
            arguments: list[str] = []
            for column in plan["columns"]:
                name = column["name"]
                rendered = physical(name)
                if column["sql_type"].startswith("DECIMAL("):
                    rendered = decimal_text(rendered, column["sql_type"])
                elif column["sql_type"] == "TIMESTAMP":
                    rendered = f"TO_CHAR({rendered}, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')"
                arguments.append(f"'{name}' VALUE {rendered}")
            row = "JSON_OBJECT(" + ", ".join(arguments) + " RETURNING VARCHAR2)"
            sql = (
                f"SELECT JSON_OBJECT('table' VALUE '{plan['table']}', 'row' VALUE {row} "
                f"RETURNING VARCHAR2) FROM {qualified(plan['table'])} ORDER BY "
                + ", ".join(physical(value) for value in plan["order_by"])
            )
            queries.append({"table": plan["table"], "sql": sql})
        return queries


RENDERER = OracleRenderer()
