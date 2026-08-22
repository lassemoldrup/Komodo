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
            ["session_id"],
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


class YugabyteYsqlCatalogTest(unittest.TestCase):
    def run_tool(self, metadata: str, expected: int = 0) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["python3", str(TOOL), "--benchmark", "ycsb", "--emit-tables"],
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
