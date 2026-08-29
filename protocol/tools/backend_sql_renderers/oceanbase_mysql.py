from __future__ import annotations

import json
from typing import Any

from backend_sql_renderers.mariadb import MariaDbRenderer, qualified, quote


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def ordered_json_value(column: dict[str, Any]) -> str:
    rendered = quote(column["name"])
    sql_type = column["sql_type"]
    if sql_type in {"INT", "BIGINT"}:
        return f"IF({rendered} IS NULL, 'null', CAST({rendered} AS CHAR))"
    if sql_type.startswith("DECIMAL(") or sql_type == "FLOAT":
        text = f"CAST({rendered} AS CHAR)"
    elif sql_type == "TIMESTAMP":
        text = f"DATE_FORMAT({rendered}, '%Y-%m-%dT%H:%i:%sZ')"
    elif sql_type.startswith("VARCHAR(") or sql_type.startswith("CHAR("):
        text = rendered
    else:
        raise ValueError(f"unsupported OceanBase bootstrap SQL type: {sql_type}")
    return f"IF({rendered} IS NULL, 'null', JSON_QUOTE({text}))"


def ordered_json_row(columns: list[dict[str, Any]]) -> str:
    pieces: list[str] = []
    for index, column in enumerate(columns):
        prefix = "{" if index == 0 else ","
        property_prefix = prefix + json.dumps(column["name"]) + ":"
        pieces.extend((sql_string(property_prefix), ordered_json_value(column)))
    pieces.append(sql_string("}"))
    return "CONCAT(" + ", ".join(pieces) + ")"


class OceanBaseMysqlRenderer(MariaDbRenderer):
    backend = "oceanbase_mysql"

    def bootstrap_queries(
        self, adapter: dict[str, Any], logical_plan: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if logical_plan["format"] == "ycsb_tsv":
            return super().bootstrap_queries(adapter, logical_plan)
        queries: list[dict[str, Any]] = []
        for plan in logical_plan["queries"]:
            row = ordered_json_row(plan["columns"])
            wrapper = (
                "CONCAT('{\"table\":', "
                f"JSON_QUOTE({sql_string(plan['table'])}), "
                f"',\"row\":', {row}, '}}')"
            )
            sql = (
                f"SELECT {wrapper} FROM {qualified(plan['table'])} ORDER BY "
                + ", ".join(quote(value) for value in plan["order_by"])
            )
            queries.append({"table": plan["table"], "sql": sql})
        return queries


RENDERER = OceanBaseMysqlRenderer()
