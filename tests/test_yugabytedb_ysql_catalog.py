from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "scripts" / "backends" / "yugabytedb_ysql_catalog.py"


def catalog() -> str:
    lines = ["DATABASE\tbenchbase\tUTF8\tC.UTF-8"]
    tables = [
        (
            "benchbase.checker_run_fence",
            [("run_id", "character", "NO", "36"), ("phase", "character varying", "NO", "32"), ("nonce", "character", "NO", "36")],
            ["run_id", "phase"],
        ),
        (
            "benchbase.session_txn",
            [("session_id", "character varying", "NO", "64"), ("txn_id", "bigint", "NO", "-")],
            ["session_id", "txn_id"],
        ),
        (
            "benchbase.usertable",
            [("ycsb_key", "integer", "NO", "-")]
            + [(f"field{index}", "character varying", "YES", "100") for index in range(1, 11)],
            ["ycsb_key"],
        ),
    ]
    for table, _, _ in tables:
        lines.append(f"TABLE\t{table}\tr\tf")
    for table, columns, _ in tables:
        for ordinal, (name, data_type, nullable, length) in enumerate(columns, start=1):
            lines.append(
                "\t".join(
                    ("COLUMN", table, name, str(ordinal), data_type, nullable, "-", "-", length, "-", "-", "-", "-")
                )
            )
    for table, _, keys in tables:
        for ordinal, key in enumerate(keys, start=1):
            lines.append(f"PRIMARY\t{table}\t{key}\t{ordinal}")
    return "\n".join(lines) + "\n"


def tpcc_physical_column(column: dict) -> tuple[str, str, str, str, str, str]:
    """Return (data_type, precision, scale, length, datetime_precision, identity)."""
    sql_type = column["sql_type"]
    identity = "identity" if "generated" in column else "-"
    if sql_type == "INT":
        return "integer", "-", "-", "-", "-", identity
    if sql_type == "BIGINT":
        return "bigint", "-", "-", "-", "-", identity
    if sql_type == "FLOAT":
        return "real", "-", "-", "-", "-", identity
    if sql_type == "TIMESTAMP":
        return "timestamp with time zone", "-", "-", "-", "0", identity
    if sql_type.startswith("DECIMAL("):
        precision, _, scale = sql_type[len("DECIMAL(") : -1].partition(",")
        return "numeric", precision, scale, "-", "-", identity
    kind, _, width = sql_type.partition("(")
    physical = "character varying" if kind == "VARCHAR" else "character"
    return physical, "-", "-", width[:-1], "-", identity


def tpcc_catalog() -> str:
    adapter = json.loads(
        (ROOT / "protocol" / "adapters" / "tpcc.json").read_text(encoding="utf-8")
    )
    common_tables = [
        (
            "benchbase.checker_run_fence",
            [
                {"name": "run_id", "sql_type": "CHAR(36)", "nullable": False},
                {"name": "phase", "sql_type": "VARCHAR(32)", "nullable": False},
                {"name": "nonce", "sql_type": "CHAR(36)", "nullable": False},
            ],
            ["run_id", "phase"],
        ),
        (
            "benchbase.session_txn",
            [
                {"name": "session_id", "sql_type": "VARCHAR(64)", "nullable": False},
                {"name": "txn_id", "sql_type": "BIGINT", "nullable": False},
            ],
            ["session_id", "txn_id"],
        ),
    ]
    tables = [
        (table["name"], table["columns"], table["key_columns"])
        for table in adapter["tables"]
    ] + common_tables
    lines = ["DATABASE\tbenchbase\tUTF8\tC.UTF-8"]
    for name, _, _ in tables:
        lines.append(f"TABLE\t{name}\tr\tf")
    for name, columns, _ in tables:
        for ordinal, column in enumerate(columns, start=1):
            data_type, precision, scale, length, datetime_precision, identity = (
                tpcc_physical_column(column)
            )
            nullable = "YES" if column["nullable"] else "NO"
            lines.append(
                "\t".join(
                    (
                        "COLUMN", name, column["name"], str(ordinal), data_type,
                        nullable, precision, scale, length, datetime_precision,
                        "-", "-", identity,
                    )
                )
            )
    for name, _, keys in tables:
        for ordinal, key in enumerate(keys, start=1):
            lines.append(f"PRIMARY\t{name}\t{key}\t{ordinal}")
    return "\n".join(lines) + "\n"


class YugabyteYsqlCatalogTest(unittest.TestCase):
    def run_tool(
        self,
        metadata: str,
        expected: int = 0,
        benchmark: str = "ycsb",
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["python3", str(TOOL), "--benchmark", benchmark, "--emit-tables"],
            input=metadata,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, expected, result.stderr)
        return result

    def test_exact_catalog_emits_backend_neutral_tables(self) -> None:
        result = self.run_tool(catalog())
        tables = json.loads(result.stdout)
        self.assertEqual(
            [table["canonical_table_name"] for table in tables],
            ["benchbase.checker_run_fence", "benchbase.session_txn", "benchbase.usertable"],
        )
        self.assertEqual(tables[-1]["columns"][0]["logical_sql_type"], "INT")

    def test_exact_tpcc_catalog_emits_typed_whole_row_contract(self) -> None:
        result = self.run_tool(tpcc_catalog(), benchmark="tpcc")
        tables = {table["canonical_table_name"]: table for table in json.loads(result.stdout)}
        self.assertEqual(len(tables), 11)
        history = tables["benchbase.history"]
        self.assertEqual(history["primary_key_columns"], ["h_id"])
        self.assertEqual(
            history["columns"][0],
            {"name": "h_id", "logical_sql_type": "BIGINT", "nullable": False, "generated": True},
        )
        customer = {column["name"]: column for column in tables["benchbase.customer"]["columns"]}
        self.assertEqual(customer["c_discount"]["logical_sql_type"], "DECIMAL(4,4)")
        self.assertEqual(customer["c_ytd_payment"]["logical_sql_type"], "FLOAT")
        self.assertEqual(customer["c_since"]["logical_sql_type"], "TIMESTAMP")
        order_line = {column["name"]: column for column in tables["benchbase.order_line"]["columns"]}
        self.assertTrue(order_line["ol_delivery_d"]["nullable"])

    def test_tpcc_catalog_drift_fails_closed(self) -> None:
        base = tpcc_catalog()
        customer_since = (
            "COLUMN\tbenchbase.customer\tc_since\t19\ttimestamp with time zone\tNO\t-\t-\t-\t0\t-\t-\t-"
        )
        history_id = "COLUMN\tbenchbase.history\th_id\t1\tbigint\tNO\t-\t-\t-\t-\t-\t-\tidentity"
        district_ytd = "COLUMN\tbenchbase.district\td_ytd\t3\tnumeric\tNO\t12\t2\t-\t-\t-\t-\t-"
        customer_float = "COLUMN\tbenchbase.customer\tc_ytd_payment\t10\treal\tNO\t-\t-\t-\t-\t-\t-\t-"
        self.assertIn(customer_since, base)
        self.assertIn(history_id, base)
        self.assertIn(district_ytd, base)
        self.assertIn(customer_float, base)
        mutations = (
            base.replace(customer_since, customer_since.replace("\t0\t", "\t6\t")),
            base.replace(
                customer_since,
                customer_since.replace("timestamp with time zone", "timestamp without time zone"),
            ),
            base.replace(history_id, history_id.replace("identity", "-")),
            base.replace(district_ytd, district_ytd.replace("\t12\t2\t", "\t12\t4\t")),
            base.replace(customer_float, customer_float.replace("real", "double precision")),
        )
        for metadata in mutations:
            with self.subTest(metadata=metadata[-120:]):
                self.assertNotEqual(metadata, base)
                result = self.run_tool(metadata, expected=2, benchmark="tpcc")
                self.assertIn("catalog error", result.stderr)

    def test_catalog_drift_fails_closed(self) -> None:
        mutations = (
            catalog().replace("TABLE\tbenchbase.usertable\tr\tf", "TABLE\tbenchbase.usertable\tr\td"),
            catalog().replace("COLUMN\tbenchbase.usertable\tycsb_key\t1\tinteger", "COLUMN\tbenchbase.usertable\tycsb_key\t1\tbigint"),
            catalog() + "TABLE\tbenchbase.extra\tr\tf\n",
        )
        for metadata in mutations:
            with self.subTest(metadata=metadata[-80:]):
                result = self.run_tool(metadata, expected=2)
                self.assertIn("catalog error", result.stderr)


if __name__ == "__main__":
    unittest.main()
