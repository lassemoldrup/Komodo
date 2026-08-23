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
        facts = {
            item["name"]: item["expected_value"]
            for item in backend["attestation_fact_contract"]
            if item["value_rule"] == "exact"
        }
        session = {
            item["name"]: item["expected_value"]
            for item in backend["session_contract"]["required_observations"]
        }
        settings = {
            "server.character_set": facts["server_character_set"],
            "server.collation": facts["server_collation"],
            "server.time_zone": facts["server_time_zone"],
            "server.sql_mode": facts["server_sql_mode"],
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

    @staticmethod
    def tpcc_loader_declaration(
        order_line_count: int = 300000, loader_batch_size: int = 128
    ) -> dict[str, object]:
        counts = {
            "benchbase.warehouse": 1,
            "benchbase.item": 100000,
            "benchbase.stock": 100000,
            "benchbase.district": 10,
            "benchbase.customer": 30000,
            "benchbase.history": 30000,
            "benchbase.oorder": 30000,
            "benchbase.new_order": 9000,
            "benchbase.order_line": order_line_count,
        }
        return {
            "kind": "loader_declaration",
            "benchmark": "tpcc",
            "loader_batch_size": loader_batch_size,
            "scale_parameter": {"name": "warehouses", "value": 1},
            "declared_table_row_counts": [
                {"table": table, "row_count": count} for table, count in counts.items()
            ],
        }

    def run_tpcc_loader_tool(
        self,
        command: str,
        *,
        declaration: dict[str, object] | None = None,
        input_text: str | None = None,
        expected_status: int = 0,
        expected_batch_size: int = 128,
        expected_scale_value: int = 1,
    ) -> subprocess.CompletedProcess[str]:
        declaration = declaration or self.tpcc_loader_declaration()
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as declaration_file:
            json.dump(declaration, declaration_file, separators=(",", ":"))
            declaration_file.write("\n")
            declaration_file.flush()
            return self.run_tool(
                command,
                "--benchmark",
                "tpcc",
                "--loader-declaration-file",
                declaration_file.name,
                "--expected-loader-batch-size",
                str(expected_batch_size),
                "--expected-scale-value",
                str(expected_scale_value),
                input_text=input_text,
                expected_status=expected_status,
            )

    @staticmethod
    def tpcc_loader_observations(
        order_line_count: int = 300000,
        source_sum: int = 300000,
        relationship_violations: int = 0,
    ) -> str:
        counts = {
            "benchbase.warehouse": 1,
            "benchbase.item": 100000,
            "benchbase.stock": 100000,
            "benchbase.district": 10,
            "benchbase.customer": 30000,
            "benchbase.history": 30000,
            "benchbase.oorder": 30000,
            "benchbase.new_order": 9000,
            "benchbase.order_line": order_line_count,
        }
        lines = [f"COUNT\t{table}\t{count}" for table, count in counts.items()]
        lines.append(f"SUM\tbenchbase.order_line\t{source_sum}")
        lines.append(f"RELATIONSHIP\t0\t{relationship_violations}")
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
        self.assertTrue(contract["runtime_capabilities"]["client_trace"])
        self.assertTrue(contract["runtime_capabilities"]["bootstrap_scan"])
        self.assertIsInstance(contract["runtime_capabilities"]["cdc_normalization"], bool)

    def test_adapter_readiness_separates_monitor_from_end_to_end(self) -> None:
        ycsb = json.loads(
            self.run_tool("adapter-readiness", "--benchmark", "ycsb").stdout
        )
        self.assertTrue(ycsb["monitor_ready"])
        self.assertTrue(ycsb["end_to_end_ready"])

        tpcc = json.loads(
            self.run_tool("adapter-readiness", "--benchmark", "tpcc").stdout
        )
        contract = json.loads((ROOT / "protocol/adapters/tpcc.json").read_text())
        self.assertEqual(
            tpcc["monitor_ready"],
            contract["runtime_capabilities"]["cdc_normalization"]
            and contract["runtime_capabilities"]["bootstrap_scan"],
        )
        self.assertTrue(tpcc["bootstrap_ready"])
        self.assertTrue(tpcc["monitor_ready"])
        self.assertEqual(tpcc["missing_for_monitor"], [])
        self.assertTrue(tpcc["client_ready"])
        self.assertTrue(tpcc["end_to_end_ready"])
        self.assertEqual(tpcc["missing_for_end_to_end"], [])

    def test_backend_contract_exposes_pinned_mariadb_session(self) -> None:
        result = self.run_tool("backend-contract", "--backend", "mariadb")
        contract = json.loads(result.stdout)
        facts = {
            item["name"]: item["expected_value"]
            for item in contract["attestation_fact_contract"]
            if item["value_rule"] == "exact"
        }
        self.assertEqual(facts["server_character_set"], "utf8mb4")
        self.assertEqual(facts["server_collation"], "utf8mb4_bin")
        self.assertIn(
            {"name": "time_zone", "expected_value": "+00:00"},
            contract["session_contract"]["required_observations"],
        )

    def test_schema_attestation_builder_validates_registry_tables_facts_and_digest(self) -> None:
        example = json.loads(
            (ROOT / "protocol/examples/schema-attestation-startup.json").read_text()
        )
        with tempfile.TemporaryDirectory(prefix="schema-attestation-test-") as temporary:
            root = Path(temporary)
            tables = root / "tables.json"
            facts = root / "facts.json"
            tables.write_text(json.dumps(example["tables"]), encoding="utf-8")
            facts.write_text(json.dumps(example["backend_facts"]), encoding="utf-8")
            result = self.run_tool(
                "schema-attestation",
                "--run-id",
                RUN_ID,
                "--phase",
                "STARTUP",
                "--backend",
                "mariadb",
                "--backend-source-id",
                "mariadb-main",
                "--benchmark",
                "ycsb",
                "--tables-file",
                str(tables),
                "--facts-file",
                str(facts),
            )
            self.assertEqual(json.loads(result.stdout), example)

            mutated = json.loads(facts.read_text())
            mutated[0]["canonical_value"] = "other"
            facts.write_text(json.dumps(mutated), encoding="utf-8")
            failed = self.run_tool(
                "schema-attestation",
                "--run-id",
                RUN_ID,
                "--phase",
                "STARTUP",
                "--backend",
                "mariadb",
                "--backend-source-id",
                "mariadb-main",
                "--benchmark",
                "ycsb",
                "--tables-file",
                str(tables),
                "--facts-file",
                str(facts),
                expected_status=2,
            )
            self.assertIn("database_name differs", failed.stderr)

    def test_run_manifest_selects_yugabyte_without_a_generic_backend_enum(self) -> None:
        result = self.run_tool(
            "run-manifest",
            "--run-id",
            RUN_ID,
            "--backend",
            "yugabytedb_ysql",
            "--backend-source-id",
            "yugabytedb-ysql-main",
            "--declared-isolation",
            "REPEATABLE_READ",
            "--benchmark",
            "ycsb",
            "--session-id",
            "ycsb/worker/1",
        )
        self.assertEqual(json.loads(result.stdout)["backend"], "yugabytedb_ysql")

    def test_read_committed_is_admitted_only_by_fractured_probe_adapter(self) -> None:
        rejected = self.run_tool(
            "run-manifest",
            "--run-id",
            RUN_ID,
            "--backend",
            "yugabytedb_ysql",
            "--backend-source-id",
            "yugabytedb-ysql-main",
            "--declared-isolation",
            "READ_COMMITTED",
            "--benchmark",
            "ycsb",
            "--session-id",
            "ycsb/worker/1",
            expected_status=2,
        )
        self.assertIn("unsupported by adapter ycsb", rejected.stderr)

        admitted = self.run_tool(
            "run-manifest",
            "--run-id",
            RUN_ID,
            "--backend",
            "yugabytedb_ysql",
            "--backend-source-id",
            "yugabytedb-ysql-main",
            "--declared-isolation",
            "READ_COMMITTED",
            "--benchmark",
            "ycsb_fractured_read_probe",
            "--session-id",
            "ycsb/worker/1",
            "--session-id",
            "ycsb/worker/2",
        )
        self.assertEqual(
            json.loads(admitted.stdout)["benchmark"],
            "ycsb_fractured_read_probe",
        )

    def test_schema_check_accepts_ycsb_and_tpcc_contracts(self) -> None:
        for benchmark in ("ycsb", "ycsb_fractured_read_probe", "tpcc"):
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

    def test_loader_plan_enforces_tpcc_scale_counts_and_records_batch_size(self) -> None:
        result = self.run_tpcc_loader_tool("loader-plan")
        plan = json.loads(result.stdout)
        self.assertEqual(plan["loader_batch_size"], 128)
        self.assertEqual(plan["scale_parameter"], {"name": "warehouses", "value": 1})

        result = self.run_tpcc_loader_tool(
            "loader-plan",
            declaration=self.tpcc_loader_declaration(loader_batch_size=64),
            expected_batch_size=64,
        )
        self.assertEqual(json.loads(result.stdout)["loader_batch_size"], 64)

        invalid = self.tpcc_loader_declaration()
        next(
            item
            for item in invalid["declared_table_row_counts"]
            if item["table"] == "benchbase.stock"
        )["row_count"] = 99999
        result = self.run_tpcc_loader_tool(
            "loader-plan", declaration=invalid, expected_status=2
        )
        self.assertIn("differs from contract 100000", result.stderr)

    def test_loader_queries_cover_all_tpcc_counts_and_relationships(self) -> None:
        result = self.run_tool("loader-queries", "--benchmark", "tpcc")
        document = json.loads(result.stdout)
        self.assertEqual(len(document["queries"]), 11)
        relationship = document["queries"][-1]
        self.assertEqual(relationship["observation"], "relationship_violations")
        self.assertIn("COUNT(DISTINCT c.`ol_number`)", relationship["sql"])
        self.assertIn("expected_count < 5", relationship["sql"])
        self.assertIn("expected_count > 15", relationship["sql"])
        self.assertIn("COALESCE(minimum_sequence, 0) <> 1", relationship["sql"])

    def test_loader_check_accepts_exact_tpcc_declaration_and_sql_state(self) -> None:
        result = self.run_tpcc_loader_tool(
            "loader-check",
            input_text=self.tpcc_loader_observations(),
        )
        report = json.loads(result.stdout)
        self.assertTrue(report["validated"])
        self.assertEqual(report["loader_batch_size"], 128)
        self.assertEqual(report["derived_cardinality_checks"][0]["observed_source_sum"], 300000)
        self.assertEqual(report["relationship_checks"][0]["violation_count"], 0)

    def test_loader_check_rejects_count_sum_and_sequence_mismatches(self) -> None:
        cases = (
            (self.tpcc_loader_observations(order_line_count=299999), "independent SQL count"),
            (self.tpcc_loader_observations(source_sum=299999), "row count differs from SUM"),
            (self.tpcc_loader_observations(relationship_violations=1), "violating parent rows"),
        )
        for observations, diagnostic in cases:
            with self.subTest(diagnostic=diagnostic):
                result = self.run_tpcc_loader_tool(
                    "loader-check",
                    input_text=observations,
                    expected_status=2,
                )
                self.assertIn(diagnostic, result.stderr)

    def test_loader_plan_requires_exact_declared_roster(self) -> None:
        declaration = self.tpcc_loader_declaration()
        declaration["declared_table_row_counts"].pop()
        result = self.run_tpcc_loader_tool(
            "loader-plan", declaration=declaration, expected_status=2
        )
        self.assertIn("count roster differs", result.stderr)

    def test_loader_plan_rejects_batch_and_scale_different_from_coordinator(self) -> None:
        result = self.run_tpcc_loader_tool(
            "loader-plan", expected_batch_size=64, expected_status=2
        )
        self.assertIn("batch size differs", result.stderr)
        result = self.run_tpcc_loader_tool(
            "loader-plan", expected_scale_value=2, expected_status=2
        )
        self.assertIn("scale differs", result.stderr)

    def test_historical_loader_check_records_declaration_unavailable(self) -> None:
        result = self.run_tool(
            "loader-independent-check",
            "--benchmark",
            "ycsb",
            "--loader-batch-size",
            "128",
            "--expected-count",
            "benchbase.usertable=1000",
            input_text="COUNT\tbenchbase.usertable\t1000\n",
        )
        report = json.loads(result.stdout)
        self.assertFalse(report["declaration_available"])
        self.assertIsNone(report["declared_table_row_counts"])
        self.assertEqual(
            report["expected_table_row_counts"],
            [{"table": "benchbase.usertable", "row_count": 1000}],
        )

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

    def test_run_manifest_accepts_ready_tpcc_runtime(self) -> None:
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
        )
        manifest = json.loads(result.stdout)
        self.assertEqual(manifest["benchmark"], "tpcc")
        self.assertEqual(manifest["expected_session_ids"], ["tpcc/worker/1"])
        contract = json.loads((ROOT / "protocol/adapters/tpcc.json").read_text())
        self.assertEqual(
            manifest["application_table_allowlist"], contract["application_tables"]
        )

    def test_empty_tpcc_bootstrap_uses_full_roster_and_empty_digest(self) -> None:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as scan:
            result = self.run_tool(
                "bootstrap-manifest",
                "--run-id",
                RUN_ID,
                "--benchmark",
                "tpcc",
                "--scan-file",
                scan.name,
            )
        manifest = json.loads(result.stdout)
        self.assertEqual(len(manifest["table_row_counts"]), 9)
        self.assertTrue(all(entry["row_count"] == 0 for entry in manifest["table_row_counts"]))
        self.assertEqual(
            manifest["canonical_state_digest"],
            "944dd6136dbad7047e31af3b6419eb6a632d4dea64bec8159e93307163810bc2",
        )

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
