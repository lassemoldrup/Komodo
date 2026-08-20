#!/usr/bin/env python3

from __future__ import annotations

import base64
import json
import re
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "scripts" / "blackbox_record.py"
RUN_ID = "00112233-4455-6677-8899-aabbccddeeff"


class BlackboxRecordTest(unittest.TestCase):
    def run_tool(
        self, *arguments: str, input_text: str | None = None, expected_status: int = 0
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["python3", str(TOOL), *arguments],
            input=input_text,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, expected_status, result.stderr)
        return result

    @staticmethod
    def schema_metadata(benchmark: str) -> str:
        backend = json.loads((ROOT / "protocol/backends/mariadb.json").read_text())
        adapter = json.loads((ROOT / f"protocol/adapters/{benchmark}.json").read_text())
        server = backend["server_defaults"]
        session = backend["session_requirements"]
        settings = {
            "server.character_set": server["character_set"],
            "server.collation": server["collation"],
            "server.time_zone": server["time_zone"],
            "server.sql_mode": server["sql_mode"],
            "server.transaction_isolation": "REPEATABLE-READ",
            "server.snapshot_isolation": "1",
            "session.character_set_client": session["character_set_client"],
            "session.character_set_connection": session["character_set_connection"],
            "session.character_set_results": session["character_set_results"],
            "session.collation_connection": session["collation_connection"],
            "session.time_zone": session["time_zone"],
            "session.sql_mode": session["sql_mode"],
            "session.transaction_isolation": "REPEATABLE-READ",
            "session.snapshot_isolation": "1",
        }
        common_tables = [
            {
                "name": "benchbase.checker_run_fence",
                "key_columns": ["run_id", "phase"],
                "columns": [
                    {"name": "run_id", "sql_type": "CHAR(36)", "nullable": False, "character_set": "ascii", "collation": "ascii_bin"},
                    {"name": "phase", "sql_type": "VARCHAR(32)", "nullable": False, "character_set": "ascii", "collation": "ascii_bin"},
                    {"name": "nonce", "sql_type": "CHAR(36)", "nullable": False, "character_set": "ascii", "collation": "ascii_bin"},
                ],
            },
            {
                "name": "benchbase.session_txn",
                "key_columns": ["session_id"],
                "columns": [
                    {"name": "session_id", "sql_type": "VARCHAR(64)", "nullable": False, "character_set": "ascii", "collation": "ascii_bin"},
                    {"name": "txn_id", "sql_type": "BIGINT", "nullable": False},
                ],
            },
        ]
        tables = adapter["tables"] + common_tables
        lines = [f"SETTING\t{key}\t{value}" for key, value in settings.items()]
        lines.append("DATABASE\tbenchbase\tutf8mb4\tutf8mb4_bin")
        lines.extend(
            f"TABLE\t{table['name']}\tInnoDB\tutf8mb4_bin" for table in tables
        )
        for table in tables:
            for ordinal, column in enumerate(table["columns"], start=1):
                sql_type = column["sql_type"]
                data_type = re.match(r"^[A-Z]+", sql_type).group(0).lower()
                precision = scale = character_length = datetime_precision = "-"
                decimal_match = re.fullmatch(r"DECIMAL\(([0-9]+),([0-9]+)\)", sql_type)
                string_match = re.fullmatch(r"(?:VARCHAR|CHAR)\(([0-9]+)\)", sql_type)
                if decimal_match:
                    precision, scale = decimal_match.groups()
                elif string_match:
                    character_length = string_match.group(1)
                elif sql_type == "INT":
                    precision, scale = "10", "0"
                elif sql_type == "BIGINT":
                    precision, scale = "19", "0"
                elif sql_type == "TIMESTAMP":
                    datetime_precision = "0"
                character_set = column.get("character_set", "-")
                collation = column.get("collation", "-")
                if string_match and character_set == "-":
                    character_set = "utf8mb4"
                    collation = "utf8mb4_bin"
                extra = "auto_increment" if column.get("generated") else "-"
                lines.append(
                    "\t".join(
                        (
                            "COLUMN",
                            table["name"],
                            column["name"],
                            str(ordinal),
                            data_type,
                            "YES" if column["nullable"] else "NO",
                            precision,
                            scale,
                            character_length,
                            datetime_precision,
                            character_set,
                            collation,
                            extra,
                        )
                    )
                )
        for table in tables:
            lines.extend(
                f"PRIMARY\t{table['name']}\t{column}\t{ordinal}"
                for ordinal, column in enumerate(table["key_columns"], start=1)
            )
        return "\n".join(lines) + "\n"

    def test_run_manifest_matches_frozen_example(self) -> None:
        result = self.run_tool(
            "run-manifest",
            "--run-id",
            RUN_ID,
            "--backend-source-id",
            "mariadb-main",
            "--declared-isolation",
            "REPEATABLE_READ",
            "--session-id",
            "ycsb/worker/1",
            "--session-id",
            "ycsb/worker/2",
        )
        expected = json.loads((ROOT / "protocol/examples/run-manifest.json").read_text())
        self.assertEqual(json.loads(result.stdout), expected)
        self.assertNotIn("version", result.stdout)

    def test_adapter_contract_exposes_exact_tpcc_membership_and_roster(self) -> None:
        result = self.run_tool(
            "adapter-contract",
            "--benchmark",
            "tpcc",
            "--transaction-type",
            "NewOrder",
        )
        contract = json.loads(result.stdout)
        self.assertEqual(
            contract["transaction_types"],
            ["NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"],
        )
        self.assertEqual(len(contract["application_tables"]), 9)
        self.assertFalse(any(contract["runtime_capabilities"].values()))

    def test_backend_contract_exposes_pinned_mariadb_session(self) -> None:
        result = self.run_tool("backend-contract", "--backend", "mariadb")
        contract = json.loads(result.stdout)
        self.assertEqual(contract["server_defaults"]["character_set"], "utf8mb4")
        self.assertEqual(contract["server_defaults"]["collation"], "utf8mb4_bin")
        self.assertEqual(contract["session_requirements"]["time_zone"], "+00:00")

    def test_schema_check_accepts_ycsb_and_tpcc_contracts(self) -> None:
        for benchmark in ("ycsb", "tpcc"):
            with self.subTest(benchmark=benchmark):
                self.run_tool(
                    "schema-check",
                    "--benchmark",
                    benchmark,
                    input_text=self.schema_metadata(benchmark),
                )

    def test_schema_check_rejects_session_contract_drift(self) -> None:
        metadata = self.schema_metadata("tpcc").replace(
            "SETTING\tsession.collation_connection\tutf8mb4_bin",
            "SETTING\tsession.collation_connection\tutf8mb4_general_ci",
        )
        result = self.run_tool(
            "schema-check",
            "--benchmark",
            "tpcc",
            input_text=metadata,
            expected_status=2,
        )
        self.assertIn("server/session settings differ", result.stderr)

    def test_schema_check_rejects_tpcc_column_and_primary_key_drift(self) -> None:
        metadata = self.schema_metadata("tpcc").replace(
            "COLUMN\tbenchbase.history\th_id\t1\tbigint\tNO",
            "COLUMN\tbenchbase.history\th_id\t1\tbigint\tYES",
        )
        result = self.run_tool(
            "schema-check",
            "--benchmark",
            "tpcc",
            input_text=metadata,
            expected_status=2,
        )
        self.assertIn("history.h_id: column nullability mismatch", result.stderr)

        metadata = self.schema_metadata("tpcc").replace(
            "PRIMARY\tbenchbase.history\th_id\t1",
            "PRIMARY\tbenchbase.history\th_c_id\t1",
        )
        result = self.run_tool(
            "schema-check",
            "--benchmark",
            "tpcc",
            input_text=metadata,
            expected_status=2,
        )
        self.assertIn("history: primary-key order mismatch", result.stderr)

    def test_schema_check_rejects_extra_application_table(self) -> None:
        metadata = self.schema_metadata("ycsb").replace(
            "TABLE\tbenchbase.usertable\tInnoDB\tutf8mb4_bin",
            "TABLE\tbenchbase.usertable\tInnoDB\tutf8mb4_bin\n"
            "TABLE\tbenchbase.unexpected\tInnoDB\tutf8mb4_bin",
        )
        result = self.run_tool(
            "schema-check",
            "--benchmark",
            "ycsb",
            input_text=metadata,
            expected_status=2,
        )
        self.assertIn("table roster differs", result.stderr)

    def test_adapter_contract_rejects_unknown_transaction_membership(self) -> None:
        result = self.run_tool(
            "adapter-contract",
            "--benchmark",
            "tpcc",
            "--transaction-type",
            "new_order",
            expected_status=2,
        )
        self.assertIn("not owned by adapter tpcc", result.stderr)

    def test_adapter_contract_rejects_roster_mismatch(self) -> None:
        result = self.run_tool(
            "adapter-contract",
            "--benchmark",
            "tpcc",
            "--application-table",
            "benchbase.warehouse",
            expected_status=2,
        )
        self.assertIn("table roster differs", result.stderr)

    def test_adapter_name_syntax_is_bounded(self) -> None:
        result = self.run_tool(
            "adapter-contract",
            "--benchmark",
            "1invalid",
            expected_status=2,
        )
        self.assertIn("must match [A-Za-z]", result.stderr)
        result = self.run_tool(
            "adapter-contract",
            "--benchmark",
            "A" * 65,
            expected_status=2,
        )
        self.assertIn("must match [A-Za-z]", result.stderr)

    def test_run_manifest_fails_closed_for_incomplete_tpcc_runtime(self) -> None:
        result = self.run_tool(
            "run-manifest",
            "--run-id",
            RUN_ID,
            "--backend-source-id",
            "mariadb-main",
            "--declared-isolation",
            "REPEATABLE_READ",
            "--benchmark",
            "tpcc",
            "--session-id",
            "tpcc/worker/1",
            expected_status=2,
        )
        self.assertIn("adapter tpcc runtime is incomplete", result.stderr)

    def test_bootstrap_scan_fails_closed_for_incomplete_tpcc_codec(self) -> None:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as scan:
            result = self.run_tool(
                "bootstrap-manifest",
                "--run-id",
                RUN_ID,
                "--benchmark",
                "tpcc",
                "--schema",
                "benchbase",
                "--table",
                "warehouse",
                "--scan-file",
                scan.name,
                expected_status=2,
            )
        self.assertIn("bootstrap scanner is not implemented", result.stderr)

    def test_ycsb_run_manifest_roster_is_adapter_validated(self) -> None:
        result = self.run_tool(
            "run-manifest",
            "--run-id",
            RUN_ID,
            "--backend-source-id",
            "mariadb-main",
            "--declared-isolation",
            "REPEATABLE_READ",
            "--benchmark",
            "ycsb",
            "--session-id",
            "ycsb/worker/1",
            "--application-table",
            "benchbase.warehouse",
            expected_status=2,
        )
        self.assertIn("table roster differs", result.stderr)

    @staticmethod
    def field_token(value: str | None) -> str:
        if value is None:
            return "N"
        return "P" + base64.b64encode(value.encode("utf-8")).decode("ascii")

    def test_bootstrap_manifest_matches_frozen_mixed_digest(self) -> None:
        mixed = [None, "", "A", "é", "中", "0", "hello", "🙂", "a\x00b", "last"]
        # Input is deliberately signed-numeric order. Canonical byte order is
        # key 42 followed by key -1, as pinned by the golden vector.
        lines = [
            "\t".join(["-1", *["N"] * 10]),
            "\t".join(["42", *[self.field_token(value) for value in mixed]]),
        ]
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as scan:
            scan.write("\n".join(lines) + "\n")
            scan.flush()
            result = self.run_tool(
                "bootstrap-manifest", "--run-id", RUN_ID, "--scan-file", scan.name
            )
        expected = json.loads((ROOT / "protocol/examples/bootstrap-manifest.json").read_text())
        self.assertEqual(json.loads(result.stdout), expected)

    def test_empty_bootstrap_uses_empty_state_digest(self) -> None:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as scan:
            result = self.run_tool(
                "bootstrap-manifest", "--run-id", RUN_ID, "--scan-file", scan.name
            )
        record = json.loads(result.stdout)
        self.assertEqual(record["table_row_counts"][0]["row_count"], 0)
        self.assertEqual(
            record["canonical_state_digest"],
            "944dd6136dbad7047e31af3b6419eb6a632d4dea64bec8159e93307163810bc2",
        )

    def test_empty_bootstrap_still_validates_identifiers(self) -> None:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as scan:
            result = self.run_tool(
                "bootstrap-manifest",
                "--run-id",
                RUN_ID,
                "--schema",
                "INVALID",
                "--scan-file",
                scan.name,
                expected_status=2,
            )
        self.assertIn("canonical lowercase identifier", result.stderr)

    def test_duplicate_scan_key_is_rejected(self) -> None:
        line = "\t".join(["7", *["N"] * 10])
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as scan:
            scan.write(f"{line}\n{line}\n")
            scan.flush()
            result = self.run_tool(
                "bootstrap-manifest",
                "--run-id",
                RUN_ID,
                "--scan-file",
                scan.name,
                expected_status=2,
            )
        self.assertIn("duplicate canonical key", result.stderr)

    def test_status_validation_rejects_extra_field(self) -> None:
        status = json.loads((ROOT / "protocol/examples/monitor-status.json").read_text())
        status["unexpected_field"] = 1
        result = self.run_tool(
            "status-action",
            "--run-id",
            RUN_ID,
            "--expected-sessions",
            "2",
            "--lifecycle",
            "FINAL",
            input_text=json.dumps(status),
            expected_status=2,
        )
        self.assertIn("schema validation", result.stderr)

    def test_final_pass_requires_reconciled_evidence(self) -> None:
        status = json.loads((ROOT / "protocol/examples/monitor-status.json").read_text())
        status["reconciliation"]["evidence_reconciled"] = False
        status["reconciliation"]["end_fence_seen"] = False
        result = self.run_tool(
            "status-action",
            "--run-id",
            RUN_ID,
            "--expected-sessions",
            "2",
            "--lifecycle",
            "FINAL",
            input_text=json.dumps(status),
            expected_status=2,
        )
        self.assertIn("schema validation", result.stderr)
        self.assertIn("evidence_reconciled", result.stderr)

    def test_final_pass_requires_coordinator_session_count(self) -> None:
        status = (ROOT / "protocol/examples/monitor-status.json").read_text()
        result = self.run_tool(
            "status-action",
            "--run-id",
            RUN_ID,
            "--expected-sessions",
            "3",
            "--lifecycle",
            "FINAL",
            input_text=status,
            expected_status=2,
        )
        self.assertIn("coordinator roster", result.stderr)

    def test_final_pass_requires_assembled_cdc_algebra(self) -> None:
        status = json.loads((ROOT / "protocol/examples/monitor-status.json").read_text())
        status["assembled_cdc_transactions"] = 0
        result = self.run_tool(
            "status-action",
            "--run-id",
            RUN_ID,
            "--expected-sessions",
            "2",
            "--lifecycle",
            "FINAL",
            input_text=json.dumps(status),
            expected_status=2,
        )
        self.assertIn("schema validation", result.stderr)
        self.assertIn("assembled_cdc_transactions", result.stderr)

        status["assembled_cdc_transactions"] = 5
        result = self.run_tool(
            "status-action",
            "--run-id",
            RUN_ID,
            "--expected-sessions",
            "2",
            "--lifecycle",
            "FINAL",
            input_text=json.dumps(status),
            expected_status=2,
        )
        self.assertIn("assembled CDC counter", result.stderr)

    def test_status_lifecycle_regression_is_rejected(self) -> None:
        status = json.loads((ROOT / "protocol/examples/monitor-status.json").read_text())
        status["lifecycle_state"] = "STARTING"
        status["outcome"] = "RUNNING"
        status["checker_observation"] = "NOT_STARTED"
        del status["reconciliation"]
        result = self.run_tool(
            "status-action",
            "--run-id",
            RUN_ID,
            "--expected-sessions",
            "2",
            "--previous-lifecycle",
            "LOAD_FINISHED_SEEN",
            "--lifecycle",
            "FINAL",
            input_text=json.dumps(status),
            expected_status=2,
        )
        self.assertIn("regressed", result.stderr)

    def test_consistency_outcome_is_not_a_run_failure_code(self) -> None:
        result = self.run_tool(
            "run-failed",
            "--run-id",
            RUN_ID,
            "--phase",
            "COORDINATOR",
            "--reason-code",
            "PASS",
            "--diagnostic",
            "not an error code",
            expected_status=2,
        )
        self.assertIn("not in result_error_codes.md", result.stderr)

    def test_session_contract_mismatch_is_a_stable_run_failure_code(self) -> None:
        result = self.run_tool(
            "run-failed",
            "--run-id",
            RUN_ID,
            "--phase",
            "CONNECTION_SETUP",
            "--reason-code",
            "SESSION_CONTRACT_MISMATCH",
            "--diagnostic",
            "collation_connection differs from backend contract",
        )
        self.assertEqual(
            json.loads(result.stdout)["reason_code"], "SESSION_CONTRACT_MISMATCH"
        )


if __name__ == "__main__":
    unittest.main()
