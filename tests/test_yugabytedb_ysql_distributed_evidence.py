from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "connector-contract" / "yugabytedb_ysql"
sys.path.insert(0, str(CONTRACT))

from verify_distributed_experiment import verify_conflicts  # noqa: E402
from verify_resilience_experiment import (  # noqa: E402
    cdc_state,
    exact_expected_state,
    verify_closing_boundary,
    verify_provenance,
    verify_transition_overlap,
)


class YugabyteYsqlDistributedEvidenceTest(unittest.TestCase):
    def test_complete_evidence_index_shape(self) -> None:
        result = subprocess.run(
            [sys.executable, str(CONTRACT / "verify_distributed_evidence_index.py")],
            check=True,
            text=True,
            capture_output=True,
        )
        self.assertIn("distributed correctness evidence index verified", result.stdout)
        index = json.loads(
            (CONTRACT / "evidence" / "distributed_correctness_bundles.json").read_text()
        )
        self.assertEqual(len(index["bundles"]), 11)
        self.assertEqual(
            {bundle["purpose"] for bundle in index["bundles"]},
            {
                "cross_tablet_marker_last",
                "cross_table_test_and_tpcc",
                "commit_order_reversal",
                "commit_order_realtime",
                "concurrent_four_tablets",
                "large_transaction_boundaries",
                "write_shape_predecessors",
                "serialization_deadlock_retries",
                "redis_backpressure_order",
                "connector_queue_saturation_order",
                "tablet_split_leader_movement_tserver_loss",
            },
        )

    def test_conflict_attempt_ledger_records_and_verifies_exact_sqlstates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            serialization = root / "serialization.log"
            left = root / "left.log"
            right = root / "right.log"
            output = root / "ledger.json"
            serialization.write_text("ERROR:  40001: stale snapshot\n")
            left.write_text("ACK deadlock_left 1\n")
            right.write_text("ERROR:  40P01: deadlock detected\n")
            subprocess.run(
                [
                    sys.executable,
                    str(CONTRACT / "write_attempt_ledger.py"),
                    "--serialization-log",
                    str(serialization),
                    "--deadlock-left-status",
                    "0",
                    "--deadlock-left-log",
                    str(left),
                    "--deadlock-right-status",
                    "3",
                    "--deadlock-right-log",
                    str(right),
                    "--output",
                    str(output),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            ledger = json.loads(output.read_text())
            self.assertEqual(ledger["serialization_failure_sqlstate"], "40001")
            self.assertEqual(ledger["deadlock_failure_sqlstate"], "40P01")
            self.assertEqual(ledger["deadlock_loser"], "deadlock_right")
            self.assertEqual(ledger["deadlock_loser_successful_attempt_marker"], 2)

            serialization.write_text("ERROR:  XX000: unexpected\n")
            subprocess.run(
                [
                    sys.executable,
                    str(CONTRACT / "write_attempt_ledger.py"),
                    "--serialization-log",
                    str(serialization),
                    "--deadlock-left-status",
                    "0",
                    "--deadlock-left-log",
                    str(left),
                    "--deadlock-right-status",
                    "3",
                    "--deadlock-right-log",
                    str(right),
                    "--output",
                    str(output),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(json.loads(output.read_text())["serialization_failure_sqlstate"], "XX000")
            with self.assertRaisesRegex(SystemExit, "serialization SQLSTATE differs"):
                verify_conflicts([], output)

    def test_contract_only_tpcc_tables_follow_frozen_column_contract(self) -> None:
        schema = (CONTRACT / "schema.sql").read_text(encoding="utf-8")
        adapter = json.loads((ROOT / "protocol" / "adapters" / "tpcc.json").read_text())
        for table_name in ("warehouse", "district"):
            expected = next(
                table for table in adapter["tables"]
                if table["name"] == f"benchbase.{table_name}"
            )
            match = re.search(
                rf"CREATE TABLE {table_name} \((.*?)\n\) SPLIT INTO 1 TABLETS;",
                schema,
                re.DOTALL,
            )
            self.assertIsNotNone(match)
            assert match is not None
            column_names = []
            for line in match.group(1).splitlines():
                stripped = line.strip()
                if not stripped:
                    continue
                name = stripped.split(maxsplit=1)[0].rstrip(",")
                if name != "PRIMARY":
                    column_names.append(name)
            self.assertEqual(column_names, [column["name"] for column in expected["columns"]])

        production_schema = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (ROOT / "yugabytedb_ysql" / "initdb" / "adapters").glob("*.sql")
        )
        self.assertNotIn("contract_account_", production_schema)
        self.assertNotRegex(production_schema, r"(?i)CREATE TABLE\s+warehouse")

    def test_topology_operation_must_overlap_the_inflight_transaction(self) -> None:
        valid = {
            "transaction_started_ns": 10,
            "operation_started_ns": 20,
            "command_returned_ns": 30,
            "transaction_commit_ack_ns": 40,
            "topology_completed_ns": 50,
        }
        self.assertEqual(
            verify_transition_overlap({"split": valid}, "split"), valid
        )
        invalid = dict(valid, operation_started_ns=45)
        with self.assertRaisesRegex(SystemExit, "did not overlap"):
            verify_transition_overlap({"split": invalid}, "split")

    def test_exact_resilience_rows_detect_same_count_substitution(self) -> None:
        expected = exact_expected_state()
        frames: list[list[dict[str, object]]] = [[], [], [], []]
        table_frame = {
            "contract_account_a": 0,
            "contract_account_b": 2,
            "contract_account_c": 3,
        }
        for (table, key), payload in expected.items():
            frames[table_frame[table]].append(
                {
                    "source": {"table": table},
                    "op": "c",
                    "before": None,
                    "after": {"account_id": key, "payload": payload},
                }
            )
        frames[1] = [
            {
                "source": {"table": "contract_account_a"},
                "op": "c",
                "before": None,
                "after": {"account_id": 9000, "payload": "resilience-2"},
            },
            {"source": {"table": "session_txn"}},
        ]
        # The key 9000 belongs only to the second frame.
        frames[0] = [
            row for row in frames[0]
            if row.get("after", {}).get("account_id") != 9000  # type: ignore[union-attr]
        ]
        frames[0].append({"source": {"table": "session_txn"}})
        frames[2].append({"source": {"table": "session_txn"}})
        frames[3].append({"source": {"table": "session_txn"}})
        self.assertEqual(cdc_state(frames), expected)

        frames[2][0]["after"]["payload"] = "same-count-corruption"  # type: ignore[index]
        self.assertNotEqual(cdc_state(frames), expected)

    def test_closing_boundary_rejects_late_stream_growth(self) -> None:
        closing = {
            "kind": "yugabytedb_ysql_resilience_closing_boundary",
            "stream_length_before": 100,
            "stream_length_after": 100,
            "end_count_before": 5,
            "end_count_after": 5,
            "stabilization_started_ns": 1,
            "stabilization_completed_ns": 5_000_000_001,
            "connector_stopped_ns": 5_000_000_002,
        }
        self.assertEqual(verify_closing_boundary(closing, 100), closing)
        with self.assertRaisesRegex(SystemExit, "stream changed"):
            verify_closing_boundary(dict(closing, stream_length_after=101), 100)

    def test_resilience_provenance_rejects_forged_runtime_image(self) -> None:
        lock = json.loads((CONTRACT / "provenance.lock.json").read_text())
        artifacts = lock["artifacts"]
        expected_id = artifacts["connector_contract_image"]["local_image_id"]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "provenance.lock.json").write_text(json.dumps(lock))
            (root / "connector-contract-image.json").write_text(
                json.dumps([{"Id": expected_id}])
            )
            (root / "container-resilience-contract-debezium-server.json").write_text(
                json.dumps([{"Image": expected_id}])
            )
            (root / "connector-classpath.txt").write_text(
                artifacts["connector"]["sha256"]
                + "  yugabytedb-source-connector.jar\n"
                + "io/debezium/connector/postgresql/YugabyteDBConnector.class\n"
            )
            self.assertEqual(verify_provenance(root), expected_id)
            (root / "connector-contract-image.json").write_text(
                json.dumps([{"Id": "sha256:" + "0" * 64}])
            )
            with self.assertRaisesRegex(SystemExit, "image differs"):
                verify_provenance(root)

    def test_queue_saturation_requires_zero_remaining_capacity(self) -> None:
        header = (
            "epoch_ms\tmonotonic_ns\tobject_name\tqueue_total_capacity\t"
            "queue_remaining_capacity\tcurrent_queue_size_bytes\tmax_queue_size_bytes\n"
        )
        with tempfile.TemporaryDirectory() as temporary:
            metrics = Path(temporary) / "queue.tsv"
            metrics.write_text(
                header
                + "1\t1\tdebezium.postgres:type=connector-metrics\t32\t1\t10\t100\n"
                + "2\t2\tdebezium.postgres:type=connector-metrics\t32\t0\t20\t100\n"
            )
            result = subprocess.run(
                [
                    sys.executable,
                    str(CONTRACT / "verify_queue_saturation.py"),
                    "--metrics",
                    str(metrics),
                    "--expected-capacity",
                    "32",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(json.loads(result.stdout)["minimum_remaining_capacity"], 0)
            metrics.write_text(
                header
                + "1\t1\tdebezium.postgres:type=connector-metrics\t32\t1\t10\t100\n"
            )
            result = subprocess.run(
                [
                    sys.executable,
                    str(CONTRACT / "verify_queue_saturation.py"),
                    "--metrics",
                    str(metrics),
                    "--expected-capacity",
                    "32",
                ],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("did not saturate", result.stderr)


if __name__ == "__main__":
    unittest.main()
