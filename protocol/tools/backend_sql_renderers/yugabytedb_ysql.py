#!/usr/bin/env python3
from __future__ import annotations

import re
from typing import Any


IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")


def quote(identifier: str) -> str:
    if IDENTIFIER.fullmatch(identifier) is None:
        raise ValueError(f"invalid SQL identifier: {identifier}")
    return f'"{identifier}"'


def qualified(table: str) -> str:
    schema, separator, leaf = table.partition(".")
    if separator != ".":
        raise ValueError(f"invalid qualified table: {table}")
    return f"{quote(schema)}.{quote(leaf)}"


def relationship_sql(spec: dict[str, Any]) -> str:
    parent_keys = spec["parent_key_columns"]
    child_keys = spec["child_parent_columns"]
    join = " AND ".join(
        f"p.{quote(parent)} = c.{quote(child)}"
        for parent, child in zip(parent_keys, child_keys, strict=True)
    )
    selected = ", ".join(f"p.{quote(column)}" for column in parent_keys)
    expected = f"p.{quote(spec['expected_count_column'])}"
    grouped = ", ".join([*(f"p.{quote(column)}" for column in parent_keys), expected])
    sequence = f"c.{quote(spec['child_sequence_column'])}"
    return (
        "SELECT COUNT(*) FROM ("
        f"SELECT {selected}, {expected} AS expected_count, COUNT({sequence}) AS line_count, "
        f"COUNT(DISTINCT {sequence}) AS distinct_line_count, MIN({sequence}) AS minimum_sequence, "
        f"MAX({sequence}) AS maximum_sequence FROM {qualified(spec['parent_table'])} AS p "
        f"LEFT JOIN {qualified(spec['child_table'])} AS c ON {join} GROUP BY {grouped}"
        ") AS grouped_rows WHERE "
        f"expected_count < {spec['minimum_count']} OR expected_count > {spec['maximum_count']} OR "
        "line_count <> expected_count OR distinct_line_count <> expected_count OR "
        f"COALESCE(minimum_sequence, 0) <> {spec['first_sequence_value']} OR "
        f"COALESCE(maximum_sequence, 0) <> ({spec['first_sequence_value']} + "
        f"(expected_count - 1) * {spec['sequence_step']})"
    )


class YugabyteYsqlRenderer:
    backend = "yugabytedb_ysql"

    def loader_queries(self, adapter: dict[str, Any]) -> list[dict[str, Any]]:
        queries = [
            {"observation": "count", "table": table, "sql": f"SELECT COUNT(*) FROM {qualified(table)}"}
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
                        "sql": f"SELECT COALESCE(SUM({quote(cardinality['source_column'])}), 0) FROM {qualified(cardinality['source_table'])}",
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
                fields = ", ".join(
                    f"CASE WHEN {quote(column['name'])} IS NULL THEN 'N' ELSE "
                    f"'P' || encode(convert_to({quote(column['name'])}, 'UTF8'), 'base64') END"
                    for column in plan["columns"][1:]
                )
                sql = (
                    f"SELECT {quote(plan['columns'][0]['name'])}, {fields} FROM {qualified(plan['table'])} "
                    f"ORDER BY {', '.join(quote(value) for value in plan['order_by'])}"
                )
            else:
                row_arguments: list[str] = []
                for column in plan["columns"]:
                    rendered = quote(column["name"])
                    sql_type = column["sql_type"]
                    if sql_type.startswith("DECIMAL(") or sql_type == "FLOAT":
                        rendered = f"CAST({rendered} AS TEXT)"
                    elif sql_type == "TIMESTAMP":
                        rendered = (
                            f"to_char({rendered} AT TIME ZONE 'UTC', "
                            "'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')"
                        )
                    row_arguments.extend((f"'{column['name']}'", rendered))
                row = "json_build_object(" + ", ".join(row_arguments) + ")"
                sql = (
                    f"SELECT json_build_object('table', '{plan['table']}', 'row', {row})::text "
                    f"FROM {qualified(plan['table'])} ORDER BY "
                    + ", ".join(quote(value) for value in plan["order_by"])
                )
            queries.append({"table": plan["table"], "sql": sql})
        return queries


RENDERER = YugabyteYsqlRenderer()
