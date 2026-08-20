#!/usr/bin/env python3

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def split_sql_items(body: str) -> list[str]:
    items: list[str] = []
    start = 0
    depth = 0
    for index, character in enumerate(body):
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif character == "," and depth == 0:
            items.append(body[start:index].strip())
            start = index + 1
    items.append(body[start:].strip())
    return items


def parse_create_tables(sql: str) -> dict[str, dict[str, object]]:
    tables: dict[str, dict[str, object]] = {}
    pattern = re.compile(
        r"CREATE\s+TABLE\s+([a-z][a-z0-9_]*)\s*\((.*?)\)\s*"
        r"ENGINE\s*=\s*InnoDB\s+CHARACTER\s+SET\s+utf8mb4\s+"
        r"COLLATE\s+utf8mb4_bin\s*;",
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(sql):
        table_name, body = match.groups()
        columns: list[dict[str, object]] = []
        table_primary_key: list[str] | None = None
        inline_primary_key: list[str] = []
        for item in split_sql_items(body):
            normalized = " ".join(item.split())
            primary_match = re.fullmatch(
                r"PRIMARY KEY \(([^)]+)\)", normalized, re.IGNORECASE
            )
            if primary_match:
                table_primary_key = [
                    column.strip().lower()
                    for column in primary_match.group(1).split(",")
                ]
                continue
            if re.match(r"^(?:FOREIGN KEY|UNIQUE)", normalized, re.IGNORECASE):
                continue
            column_match = re.fullmatch(
                r"([a-z][a-z0-9_]*)\s+"
                r"(DECIMAL\([0-9]+,\s*[0-9]+\)|VARCHAR\([0-9]+\)|"
                r"CHAR\([0-9]+\)|BIGINT|INT|FLOAT|TIMESTAMP)\s*(.*)",
                normalized,
                re.IGNORECASE,
            )
            if column_match is None:
                raise AssertionError(f"cannot parse {table_name} DDL item: {normalized}")
            name, sql_type, attributes = column_match.groups()
            column: dict[str, object] = {
                "name": name.lower(),
                "sql_type": re.sub(r"\s+", "", sql_type.upper()),
                "nullable": "NOT NULL" not in attributes.upper(),
            }
            if "AUTO_INCREMENT" in attributes.upper():
                column["generated"] = "auto_increment"
            if "PRIMARY KEY" in attributes.upper():
                inline_primary_key.append(name.lower())
            columns.append(column)
        tables[table_name.lower()] = {
            "columns": columns,
            "key_columns": table_primary_key or inline_primary_key,
        }
    return tables


class MariaDbSchemaAssetTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tpcc_contract = json.loads(
            (ROOT / "protocol/adapters/tpcc.json").read_text(encoding="utf-8")
        )
        cls.backend_contract = json.loads(
            (ROOT / "protocol/backends/mariadb.json").read_text(encoding="utf-8")
        )

    def test_tpcc_sql_matches_shared_column_and_key_contract(self) -> None:
        actual = parse_create_tables(
            (ROOT / "mariadb/initdb/adapters/tpcc.sql").read_text(encoding="utf-8")
        )
        expected_names = [name.split(".", maxsplit=1)[1] for name in self.tpcc_contract["application_tables"]]
        self.assertEqual(list(actual), expected_names)
        for table in self.tpcc_contract["tables"]:
            table_name = table["name"].split(".", maxsplit=1)[1]
            expected_columns = []
            for column in table["columns"]:
                expected_column = {
                    "name": column["name"],
                    "sql_type": column["sql_type"],
                    "nullable": column["nullable"],
                }
                if "generated" in column:
                    expected_column["generated"] = column["generated"]
                expected_columns.append(expected_column)
            self.assertEqual(actual[table_name]["columns"], expected_columns, table_name)
            self.assertEqual(actual[table_name]["key_columns"], table["key_columns"], table_name)

    def test_selected_schema_assets_have_exact_rosters(self) -> None:
        common_sql = (ROOT / "mariadb/initdb/02-schema.sql").read_text(encoding="utf-8")
        self.assertEqual(
            re.findall(r"CREATE\s+TABLE\s+([a-z][a-z0-9_]*)", common_sql, re.IGNORECASE),
            ["checker_run_fence", "session_txn"],
        )
        ycsb_sql = (ROOT / "mariadb/initdb/adapters/ycsb.sql").read_text(encoding="utf-8")
        self.assertEqual(
            re.findall(r"CREATE\s+TABLE\s+([a-z][a-z0-9_]*)", ycsb_sql, re.IGNORECASE),
            ["usertable"],
        )

    def test_compose_mounts_only_one_selected_application_schema(self) -> None:
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        target = "/docker-entrypoint-initdb.d/03-application-schema.sql"
        self.assertEqual(compose.count(target), 1)
        self.assertIn("${BLACKBOX_ADAPTER_SCHEMA_FILE:-./mariadb/initdb/adapters/ycsb.sql}", compose)

    def test_server_defaults_match_backend_contract(self) -> None:
        settings: dict[str, str] = {}
        for line in (ROOT / "mariadb/conf.d/cdc.cnf").read_text(encoding="utf-8").splitlines():
            line = line.split("#", maxsplit=1)[0].strip()
            if not line or line.startswith("[") or "=" not in line:
                continue
            key, value = (part.strip() for part in line.split("=", maxsplit=1))
            settings[key] = value
        server = self.backend_contract["server_defaults"]
        self.assertEqual(settings["character_set_server"], server["character_set"])
        self.assertEqual(settings["collation_server"], server["collation"])
        self.assertEqual(settings["default_time_zone"], server["time_zone"])
        self.assertEqual(settings["sql_mode"], server["sql_mode"])
        self.assertEqual(settings["transaction_isolation"], "REPEATABLE-READ")
        self.assertEqual(settings["innodb_snapshot_isolation"], "ON")


if __name__ == "__main__":
    unittest.main()
