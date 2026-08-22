#!/usr/bin/env python3

from __future__ import annotations

import base64
import copy
import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "tests" / "verify_tpcc_order_status_acceptance.py"
SPEC = importlib.util.spec_from_file_location("tpcc_order_status_acceptance", TOOL)
if SPEC is None or SPEC.loader is None:  # pragma: no cover
    raise RuntimeError(f"cannot load {TOOL}")
ACCEPTANCE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ACCEPTANCE)

CODEC_SPEC = importlib.util.spec_from_file_location(
    "tpcc_codec", ROOT / "protocol" / "tools" / "tpcc_codec.py"
)
if CODEC_SPEC is None or CODEC_SPEC.loader is None:  # pragma: no cover
    raise RuntimeError("cannot load TPC-C codec")
CODEC = importlib.util.module_from_spec(CODEC_SPEC)
CODEC_SPEC.loader.exec_module(CODEC)


RUN_ID = "87654321-4321-4321-8321-cba987654322"
SESSION_ID = "tpcc/worker/1"


class TpccOrderStatusAcceptanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.codec = CODEC.TpccCodec.from_protocol_root(ROOT / "protocol")
        client_fixture = json.loads(
            (ROOT / "protocol/fixtures/mariadb_tpcc_client_txns.json").read_text()
        )
        cls.catalog_rows = {
            entry["name"]: entry
            for entry in client_fixture["row_catalog"]
        }
        cls.run_manifest = client_fixture["run_manifest"]
        raw_fixture = json.loads(
            (ROOT / "protocol/fixtures/mariadb_tpcc_transactions.json").read_text()
        )
        cls.bootstrap_manifest = raw_fixture["bootstrap_manifest"]

    @staticmethod
    def encoded(value: bytes) -> str:
        return base64.b64encode(value).decode("ascii")

    def read(self, table: str, row: dict, event_seq: int) -> dict:
        key, value = self.codec.encode_row(table, row)
        return {
            "event_seq": event_seq,
            "key_bytes_base64": self.encoded(key),
            "source": "snapshot",
            "value_bytes_base64": self.encoded(value),
            "value_state": "present",
        }

    def customer(self, customer_id: int, first: str) -> dict:
        row = copy.deepcopy(self.catalog_rows["customer_after_delivery"]["row"])
        row.update({"c_id": customer_id, "c_first": first, "c_last": "BARBARBAR"})
        return row

    @staticmethod
    def order(order_id: int, customer_id: int, carrier: int | None) -> dict:
        return {
            "o_w_id": 1,
            "o_d_id": 1,
            "o_id": order_id,
            "o_c_id": customer_id,
            "o_carrier_id": carrier,
            "o_ol_cnt": 2,
            "o_all_local": 1,
            "o_entry_d": "2026-08-20T12:37:00Z",
        }

    @staticmethod
    def line(order_id: int, number: int, delivered: bool) -> dict:
        return {
            "ol_w_id": 1,
            "ol_d_id": 1,
            "ol_o_id": order_id,
            "ol_number": number,
            "ol_i_id": number,
            "ol_delivery_d": "2026-08-20T12:38:00Z" if delivered else None,
            "ol_amount": f"{number}.00",
            "ol_supply_w_id": 1,
            "ol_quantity": number,
            "ol_dist_info": f"line{number}",
        }

    def client(self, sequence: int, customers: list[dict], delivered: bool) -> dict:
        order_id = sequence + 10
        selected_customer = customers[len(customers) // 2]
        rows = [
            *[("benchbase.customer", row) for row in customers],
            ("benchbase.oorder", self.order(order_id, selected_customer["c_id"], 7 if delivered else None)),
            ("benchbase.order_line", self.line(order_id, 1, delivered)),
            ("benchbase.order_line", self.line(order_id, 2, delivered)),
        ]
        reads = [self.read(table, row, index) for index, (table, row) in enumerate(rows)]
        return {
            "attempt_id": f"20000000-0000-4000-8000-{sequence:012d}",
            "correlation_id": {
                "run_id": RUN_ID,
                "session_id": SESSION_ID,
                "txn_id": sequence,
            },
            "correlation_kind": "session_txn",
            "kind": "client_txn",
            "logical_txn_id": f"10000000-0000-4000-8000-{sequence:012d}",
            "read_count": len(reads),
            "read_digest": ACCEPTANCE.read_digest(reads),
            "reads": reads,
            "run_id": RUN_ID,
            "session_id": SESSION_ID,
            "session_seq": sequence,
            "txn_type": "OrderStatus",
        }

    @staticmethod
    def payload_xrange(records: list[dict], start: int = 1) -> list:
        return [
            [f"{start + index}-0", ["payload", json.dumps(record, separators=(",", ":"))]]
            for index, record in enumerate(records)
        ]

    @staticmethod
    def raw_entry(entry_id: str, key: dict, envelope: dict) -> list:
        return [
            entry_id,
            [json.dumps(key, separators=(",", ":")), json.dumps(envelope, separators=(",", ":"))],
        ]

    def cdc_transaction(
        self,
        prefix: int,
        transaction_id: str,
        table: str,
        key: dict,
        before: dict | None,
        after: dict,
    ) -> list:
        collection = f"benchbase.{table}"
        return [
            self.raw_entry(
                f"{prefix}-0",
                {"id": transaction_id},
                {"status": "BEGIN", "id": transaction_id, "event_count": None, "data_collections": None},
            ),
            self.raw_entry(
                f"{prefix}-1",
                key,
                {
                    "before": before,
                    "after": after,
                    "op": "c" if before is None else "u",
                    "source": {
                        "version": "3.6.1.Final",
                        "connector": "mariadb",
                        "name": "mariadb-main",
                        "db": "benchbase",
                        "table": table,
                    },
                    "transaction": {
                        "id": transaction_id,
                        "total_order": 1,
                        "data_collection_order": 1,
                    },
                },
            ),
            self.raw_entry(
                f"{prefix}-2",
                {"id": transaction_id},
                {
                    "status": "END",
                    "id": transaction_id,
                    "event_count": 1,
                    "data_collections": [{"data_collection": collection, "event_count": 1}],
                },
            ),
        ]

    def documents(self) -> dict[str, object]:
        customers = [self.customer(1, "A"), self.customer(2, "B"), self.customer(3, "C")]
        by_name = self.client(1, customers, delivered=True)
        by_id = self.client(2, [customers[0]], delivered=False)
        clients = [by_name, by_id]
        done = {
            "ambiguous_attempts": 0,
            "definitively_aborted_attempts": 0,
            "final_session_seq": 2,
            "kind": "session_done",
            "logical_transactions_started": 2,
            "physical_attempts_started": 2,
            "published_transactions": 2,
            "retry_attempts": 0,
            "rolling_correlation_digest": ACCEPTANCE.rolling_digest(RUN_ID, SESSION_ID, clients),
            "run_id": RUN_ID,
            "session_id": SESSION_ID,
            "successful_transactions": 2,
            "terminal_aborted_transactions": 0,
        }
        client_records = [
            copy.deepcopy(self.run_manifest),
            copy.deepcopy(self.bootstrap_manifest),
            *clients,
            done,
        ]
        load_status = {
            "kind": "status",
            "run_id": RUN_ID,
            "lifecycle_state": "LOAD_FINISHED_SEEN",
            "outcome": "RUNNING",
            "checker_observation": "PASS_SO_FAR",
            "last_cdc_stream_id": "100-0",
            "last_client_stream_id": "2-0",
            "assembled_cdc_transactions": 3,
            "joined_transactions": 0,
            "checked_transactions": 2,
            "pending_cdc_slots": 0,
            "unmatched_client_records": 0,
            "diagnostics": [],
        }
        reconciliation = {
            "evidence_reconciled": True,
            "startup_schema_attested": True,
            "final_schema_attested": True,
            "schema_attestation_digest": "0" * 64,
            "bootstrap_manifest_validated": True,
            "end_fence_seen": True,
            "expected_sessions": 1,
            "completed_sessions": 1,
            "accepted_client_transactions": 2,
            "joined_workload_transactions": 2,
            "bootstrap_cdc_transactions": 1,
            "bootstrap_checker_transactions": 1,
            "phase_anchor_transactions": 1,
            "checker_consumed_transactions": 4,
            "unmatched_client_records": 0,
            "unmatched_workload_cdc_slots": 0,
            "ambiguous_attempts": 0,
        }
        final = {
            "kind": "status",
            "run_id": RUN_ID,
            "lifecycle_state": "FINAL",
            "outcome": "PASS",
            "checker_observation": "PASS_SO_FAR",
            "last_cdc_stream_id": "103-2",
            "last_client_stream_id": "6-0",
            "assembled_cdc_transactions": 6,
            "joined_transactions": 2,
            "checked_transactions": 4,
            "pending_cdc_slots": 0,
            "unmatched_client_records": 0,
            "diagnostics": [],
            "reconciliation": reconciliation,
        }
        marker_one = {"session_id": SESSION_ID, "txn_id": 1}
        marker_two = {"session_id": SESSION_ID, "txn_id": 2}
        cdc = [
            *self.cdc_transaction(101, "0-1-1", "session_txn", {"session_id": SESSION_ID}, None, marker_one),
            *self.cdc_transaction(102, "0-1-2", "session_txn", {"session_id": SESSION_ID}, marker_one, marker_two),
            *self.cdc_transaction(
                103,
                "0-1-3",
                "checker_run_fence",
                {"run_id": RUN_ID, "phase": "END_FENCE"},
                None,
                {
                    "run_id": RUN_ID,
                    "phase": "END_FENCE",
                    "nonce": "00112233-4455-6677-8899-aabbccddeeff",
                },
            ),
        ]
        metrics = {
            "kind": "performance_measurement",
            "run_id": RUN_ID,
            "processing_mode": "audited",
            "completed": True,
            "completion_status": "completed",
            "evidence_reconciled": True,
            "outcome": "PASS",
            "diagnostics": [],
            "limits": {
                "joiner": {
                    "max_bootstrap_state_rows": 1_000_000,
                    "max_bootstrap_state_bytes": 1_073_741_824,
                }
            },
            "assembler": {
                "completed_run_control_transactions": 3,
                "completed_workload_transactions": 2,
            },
            "joiner": {
                "accepted_client_transactions": 2,
                "accepted_workload_cdc_transactions": 2,
                "joined_workload_transactions": 2,
                "marker_only_workload_transactions": 2,
                "joined_read_events": sum(client["read_count"] for client in clients),
                "joined_write_events": 0,
                "pending_client_records": 0,
                "pending_cdc_slots": 0,
                "unmatched_client_records": 0,
                "unmatched_workload_cdc_slots": 0,
            },
            "checker": {
                "workload_transactions": 2,
                "read_events": sum(client["read_count"] for client in clients),
            },
            "reconciliation": copy.deepcopy(reconciliation),
        }
        config = {
            "kind": "tpcc_order_status_acceptance_config",
            "benchmark": "tpcc",
            "backend": "mariadb",
            "declared_isolation": "REPEATABLE_READ",
            "warehouses": 1,
            "terminals": 1,
            "weights": [0, 0, 100, 0, 0],
            "random_seed": 3,
            "warmup_seconds": 0,
            "duration_seconds": 2,
            "rate": "unlimited",
            "loader_threads": 1,
            "loader_batch_size": 128,
            "minimum_transactions": 2,
            "minimum_id_transactions": 1,
            "minimum_last_name_transactions": 1,
            "minimum_multirow_transactions": 1,
            "monitor_max_bootstrap_state_rows": 1_000_000,
            "monitor_max_bootstrap_state_bytes": 1_073_741_824,
            "stack_timeout_seconds": 180,
            "loader_timeout_seconds": 900,
            "workload_timeout_seconds": 300,
            "status_timeout_seconds": 900,
            "case_timeout_seconds": 1800,
            "kill_after_seconds": 15,
            "keep_stack": False,
            "redis_host": "host.docker.internal",
            "redis_port": 6379,
            "db_host": "host.docker.internal",
            "db_port": 3306,
            "expected_session_ids": [SESSION_ID],
        }
        catalog = [
            {
                "c_w_id": 1,
                "c_d_id": 1,
                "c_id": customer_id,
                "c_last": "BARBARBAR",
                "last_name_group_count": 3,
            }
            for customer_id in (1, 2, 3)
        ]
        return {
            "config": config,
            "client": self.payload_xrange(client_records),
            "status": self.payload_xrange([load_status, final], start=20),
            "cdc": cdc,
            "metrics": metrics,
            "catalog": catalog,
            "run_id": RUN_ID,
        }

    def write_documents(self, root: Path, documents: dict[str, object]) -> dict[str, Path]:
        paths = {name: root / f"{name}.json" for name in ("config", "client", "status", "cdc", "metrics")}
        for name, path in paths.items():
            path.write_text(json.dumps(documents[name], separators=(",", ":")), encoding="utf-8")
        catalog = root / "catalog.jsonl"
        catalog.write_text(
            "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in documents["catalog"]),
            encoding="utf-8",
        )
        run_id = root / "run-id.txt"
        run_id.write_text(str(documents["run_id"]) + "\n", encoding="ascii")
        paths.update({"catalog": catalog, "run_id": run_id})
        return paths

    def validate(self, documents: dict[str, object]) -> dict:
        with tempfile.TemporaryDirectory() as directory:
            paths = self.write_documents(Path(directory), documents)
            return ACCEPTANCE.validate_acceptance(
                paths["config"],
                paths["run_id"],
                paths["client"],
                paths["cdc"],
                paths["status"],
                paths["catalog"],
                paths["metrics"],
            )

    def client_records(self, documents: dict[str, object]) -> list[dict]:
        return [json.loads(entry[1][1]) for entry in documents["client"]]

    def replace_client_records(self, documents: dict[str, object], records: list[dict]) -> None:
        documents["client"] = self.payload_xrange(records)

    def test_clean_acceptance_proves_both_branches_and_marker_only_reconciliation(self) -> None:
        report = self.validate(self.documents())
        self.assertTrue(report["passed"])
        self.assertEqual(report["coverage"]["confirmed_id"], 1)
        self.assertEqual(report["coverage"]["confirmed_last_name"], 1)
        self.assertEqual(report["cdc"]["markers"], 2)
        self.assertEqual(report["reconciliation"]["joined_workload_transactions"], 2)

    def test_incomplete_last_name_group_fails_branch_proof(self) -> None:
        documents = self.documents()
        records = self.client_records(documents)
        client = next(record for record in records if record.get("kind") == "client_txn")
        client["reads"].pop(1)
        for index, read in enumerate(client["reads"]):
            read["event_seq"] = index
        client["read_count"] = len(client["reads"])
        client["read_digest"] = ACCEPTANCE.read_digest(client["reads"])
        self.replace_client_records(documents, records)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "complete last-name group"):
            self.validate(documents)

    def test_application_write_in_workload_cdc_fails_marker_only_gate(self) -> None:
        documents = self.documents()
        row_entry = documents["cdc"][1]
        envelope = json.loads(row_entry[1][1])
        envelope["source"]["table"] = "customer"
        row_entry[1][1] = json.dumps(envelope, separators=(",", ":"))
        end = json.loads(documents["cdc"][2][1][1])
        end["data_collections"][0]["data_collection"] = "benchbase.customer"
        documents["cdc"][2][1][1] = json.dumps(end, separators=(",", ":"))
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "application table customer"):
            self.validate(documents)

    def test_nullable_and_multirow_coverage_are_required(self) -> None:
        documents = self.documents()
        records = self.client_records(documents)
        clients = [record for record in records if record.get("kind") == "client_txn"]
        delivered = clients[0]
        undelivered = clients[1]
        delivered["reads"][-3:] = copy.deepcopy(undelivered["reads"][-3:])
        for index, read in enumerate(delivered["reads"]):
            read["event_seq"] = index
        delivered["read_digest"] = ACCEPTANCE.read_digest(delivered["reads"])
        self.replace_client_records(documents, records)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "carrier lacks NULL/present"):
            self.validate(documents)

        documents = self.documents()
        documents["config"]["minimum_multirow_transactions"] = 3
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "multi-row order lines"):
            self.validate(documents)

    def test_session_digest_and_final_reconciliation_mutations_fail(self) -> None:
        documents = self.documents()
        records = self.client_records(documents)
        done = next(record for record in records if record.get("kind") == "session_done")
        done["rolling_correlation_digest"] = "0" * 64
        self.replace_client_records(documents, records)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "rolling correlation digest"):
            self.validate(documents)

        documents = self.documents()
        final = json.loads(documents["status"][-1][1][1])
        final["reconciliation"]["joined_workload_transactions"] = 1
        documents["status"][-1][1][1] = json.dumps(final, separators=(",", ":"))
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "final reconciliation counters"):
            self.validate(documents)

    def test_monitor_limit_provenance_must_match_recorded_control(self) -> None:
        documents = self.documents()
        documents["metrics"]["limits"]["joiner"]["max_bootstrap_state_bytes"] = 268_435_456
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "limit provenance"):
            self.validate(documents)


class TpccOrderStatusAcceptanceDriverTest(unittest.TestCase):
    DRIVER = ROOT / "tests" / "run_tpcc_order_status_acceptance.sh"

    def run_driver(
        self, *arguments: str, environment: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        merged = os.environ.copy()
        if environment:
            merged.update(environment)
        return subprocess.run(
            [str(self.DRIVER), *arguments],
            text=True,
            capture_output=True,
            check=False,
            env=merged,
        )

    def test_print_config_records_environment_and_cli_controls(self) -> None:
        result = self.run_driver(
            "--print-config",
            "--terminals",
            "2",
            "--loader-threads",
            "5",
            "--monitor-max-bootstrap-state-rows",
            "900000",
            environment={
                "BLACKBOX_TPCC_ORDER_STATUS_LOADER_THREADS": "7",
                "BLACKBOX_TPCC_ORDER_STATUS_DURATION_SECONDS": "9",
                "BLACKBOX_TPCC_ORDER_STATUS_MONITOR_MAX_BOOTSTRAP_STATE_BYTES": "805306368",
            },
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        config = json.loads(result.stdout)
        self.assertEqual(config["weights"], [0, 0, 100, 0, 0])
        self.assertEqual(config["terminals"], 2)
        self.assertEqual(config["expected_session_ids"], ["tpcc/worker/1", "tpcc/worker/2"])
        self.assertEqual(config["loader_threads"], 5)
        self.assertEqual(config["duration_seconds"], 9)
        self.assertEqual(config["monitor_max_bootstrap_state_rows"], 900000)
        self.assertEqual(config["monitor_max_bootstrap_state_bytes"], 805306368)
        self.assertEqual(config["status_timeout_seconds"], 900)

    def test_print_config_rejects_non_order_status_or_invalid_limits(self) -> None:
        result = self.run_driver("--print-config", "--weights", "0,0,99,1,0")
        self.assertEqual(result.returncode, 2)
        self.assertIn("weights must be exactly", result.stderr)

        result = self.run_driver(
            "--print-config", "--monitor-max-bootstrap-state-bytes", "0"
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("must be a positive integer", result.stderr)

    def test_print_config_enforces_benchbase_random_seed_range(self) -> None:
        for seed in ("-2147483648", "2147483647"):
            with self.subTest(seed=seed):
                result = self.run_driver("--print-config", "--random-seed", seed)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(result.stdout)["random_seed"], int(seed))
        for seed in ("-2147483649", "2147483648"):
            with self.subTest(seed=seed):
                result = self.run_driver("--print-config", "--random-seed", seed)
                self.assertEqual(result.returncode, 2)
                self.assertIn("signed 32-bit", result.stderr)

    def test_live_preflight_uses_default_docker_without_unbound_variable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            distribution = root / "dist"
            config = distribution / "config" / "mariadb" / "blackbox_tpcc_config.xml"
            config.parent.mkdir(parents=True)
            config.write_text("<parameters/>\n", encoding="utf-8")
            (distribution / "benchbase.jar").write_bytes(b"fixture")
            monitor = root / "monitor"
            monitor.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            monitor.chmod(0o755)
            e2e = root / "e2e"
            e2e.write_text("#!/usr/bin/env bash\nexit 7\n", encoding="utf-8")
            e2e.chmod(0o755)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            docker = fake_bin / "docker"
            docker.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            docker.chmod(0o755)
            output = root / "output"
            merged = os.environ.copy()
            merged.pop("BLACKBOX_DOCKER", None)
            merged.update(
                {
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "BLACKBOX_TPCC_ORDER_STATUS_E2E": str(e2e),
                    "BLACKBOX_LIVE_LOCK_TARGET": str(root),
                }
            )
            result = subprocess.run(
                [
                    str(self.DRIVER),
                    "--benchbase-dist",
                    str(distribution),
                    "--monitor-bin",
                    str(monitor),
                    "--output-dir",
                    str(output),
                ],
                text=True,
                capture_output=True,
                check=False,
                env=merged,
            )
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertNotIn("unbound variable", result.stderr)


if __name__ == "__main__":
    unittest.main()
