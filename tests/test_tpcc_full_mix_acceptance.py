#!/usr/bin/env python3

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "tests/verify_tpcc_full_mix_acceptance.py"
SPEC = importlib.util.spec_from_file_location("tpcc_full_mix_acceptance", TOOL)
assert SPEC is not None and SPEC.loader is not None
ACCEPTANCE = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(ACCEPTANCE)
RUN_ID = "87654321-4321-4321-8321-cba987654322"
SESSIONS = [f"tpcc/worker/{index}" for index in range(1, 5)]
TRANSACTION_TYPES = ACCEPTANCE.TRANSACTION_TYPES


class FullMixAcceptanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = json.loads(
            (ROOT / "protocol/fixtures/mariadb_tpcc_client_txns.json").read_text()
        )
        cls.raw_fixture = json.loads(
            (ROOT / "protocol/fixtures/mariadb_tpcc_transactions.json").read_text()
        )

    @staticmethod
    def payload(rows, start=1):
        return [[f"{start+i}-0", ["payload", json.dumps(row, separators=(",", ":"))]]
                for i, row in enumerate(rows)]

    @staticmethod
    def raw(stream_id, key, value):
        return [stream_id, [json.dumps(key, separators=(",", ":")),
                            json.dumps(value, separators=(",", ":"))]]

    def marker_transaction(self, prefix, session, txn_id, previous):
        tx = f"{session}-{txn_id}"; before = None
        after = {"session_id": session, "txn_id": txn_id}
        return [self.raw(f"{prefix}-0", {"id": tx}, {"status": "BEGIN", "id": tx,
                "event_count": None, "data_collections": None}),
            self.raw(f"{prefix}-1", {"session_id": session, "txn_id": txn_id},
                {"before": before, "after": after, "op": "c",
                 "source": {"name": "mariadb-main", "db": "benchbase", "table": "session_txn"},
                 "transaction": {"id": tx, "total_order": 1, "data_collection_order": 1}}),
            self.raw(f"{prefix}-2", {"id": tx}, {"status": "END", "id": tx,
                "event_count": 1, "data_collections": [{"data_collection": "benchbase.session_txn", "event_count": 1}]})]

    def fence(self, prefix):
        tx = "end"; key = {"run_id": RUN_ID, "phase": "END_FENCE"}
        return [self.raw(f"{prefix}-0", {"id": tx}, {"status": "BEGIN", "id": tx,
                "event_count": None, "data_collections": None}),
            self.raw(f"{prefix}-1", key, {"before": None, "after": {**key, "nonce": RUN_ID}, "op": "c",
                "source": {"name": "mariadb-main", "db": "benchbase", "table": "checker_run_fence"},
                "transaction": {"id": tx, "total_order": 1, "data_collection_order": 1}}),
            self.raw(f"{prefix}-2", {"id": tx}, {"status": "END", "id": tx,
                "event_count": 1, "data_collections": [{"data_collection": "benchbase.checker_run_fence", "event_count": 1}]})]

    def documents(self):
        source_clients = [entry["client_txn"] for entry in self.fixture["semantic_transactions"]]
        source_clients += [entry["client_txn"] for entry in self.fixture["synthetic_join_clients"][:3]]
        clients, by_session = [], {session: [] for session in SESSIONS}
        for index, source in enumerate(source_clients):
            client = copy.deepcopy(source); session = SESSIONS[index % 4]
            seq = len(by_session[session]) + 1
            client.update({"run_id": RUN_ID, "session_id": session, "session_seq": seq,
                           "logical_txn_id": f"10000000-0000-4000-8000-{index+1:012d}",
                           "attempt_id": f"20000000-0000-4000-8000-{index+1:012d}"})
            client["correlation_id"] = {"run_id": RUN_ID, "session_id": session, "txn_id": seq}
            clients.append(client); by_session[session].append(client)
        manifest = copy.deepcopy(self.fixture["run_manifest"]); manifest["expected_session_ids"] = SESSIONS
        bootstrap = copy.deepcopy(self.raw_fixture["bootstrap_manifest"])
        done = []
        for session, rows in by_session.items():
            count = len(rows)
            done.append({"ambiguous_attempts": 0, "definitively_aborted_attempts": 0,
                "final_session_seq": count, "kind": "session_done",
                "logical_transactions_started": count, "physical_attempts_started": count,
                "published_transactions": count, "retry_attempts": 0,
                "rolling_correlation_digest": ACCEPTANCE.common.rolling_digest(RUN_ID, session, rows),
                "run_id": RUN_ID, "session_id": session, "successful_transactions": count,
                "terminal_aborted_transactions": 0})
        client_records = [manifest, bootstrap, *clients, *done]
        cdc, prefix = [], 101
        for session in SESSIONS:
            previous = None
            for client in by_session[session]:
                txn_id = client["correlation_id"]["txn_id"]
                cdc += self.marker_transaction(prefix, session, txn_id, previous)
                prefix += 1; previous = txn_id
        cdc += self.fence(prefix)
        per_type = {}
        for client in clients: per_type[client["txn_type"]] = per_type.get(client["txn_type"], 0) + 1
        reconciliation = {"evidence_reconciled": True,
            "startup_schema_attested": True, "final_schema_attested": True,
            "schema_attestation_digest": "0" * 64, "bootstrap_manifest_validated": True,
            "end_fence_seen": True, "expected_sessions": 4, "completed_sessions": 4,
            "accepted_client_transactions": len(clients), "joined_workload_transactions": len(clients),
            "bootstrap_cdc_transactions": 1, "bootstrap_checker_transactions": 1,
            "phase_anchor_transactions": 4, "checker_consumed_transactions": 5 + len(clients),
            "unmatched_client_records": 0, "unmatched_workload_cdc_slots": 0, "ambiguous_attempts": 0}
        def status(lifecycle, outcome="RUNNING", reconciliation_value=None, cdc_id="100-0"):
            row = {"kind": "status", "run_id": RUN_ID, "lifecycle_state": lifecycle,
                "outcome": outcome, "checker_observation": "PASS_SO_FAR",
                "last_cdc_stream_id": cdc_id, "last_client_stream_id": "1-0",
                "assembled_cdc_transactions": 3 if lifecycle != "FINAL" else 4 + len(clients),
                "joined_transactions": 0 if lifecycle != "FINAL" else len(clients),
                "checked_transactions": 0 if lifecycle != "FINAL" else 5 + len(clients),
                "pending_cdc_slots": 0, "unmatched_client_records": 0, "diagnostics": []}
            if reconciliation_value is not None: row["reconciliation"] = reconciliation_value
            return row
        statuses = [status("START_FENCE_SEEN", cdc_id="10-0"),
                    status("LOAD_FINISHED_SEEN"),
                    status("FINAL", "PASS", reconciliation, f"{prefix}-2")]
        reads = sum(client["read_count"] for client in clients)
        metrics = {"kind": "performance_measurement", "run_id": RUN_ID,
            "processing_mode": "audited", "completed": True, "completion_status": "completed",
            "evidence_reconciled": True, "outcome": "PASS", "diagnostics": [],
            "joiner": {"accepted_client_transactions": len(clients),
                "accepted_workload_cdc_transactions": len(clients),
                "joined_workload_transactions": len(clients), "joined_read_events": reads,
                "pending_client_records": 0, "pending_cdc_slots": 0,
                "unmatched_client_records": 0, "unmatched_workload_cdc_slots": 0},
            "reconciliation": reconciliation}
        config = {"kind": "tpcc_full_mix_acceptance_config", "benchmark": "tpcc",
            "backend": "mariadb", "declared_isolation": "REPEATABLE_READ",
            "warehouses": 1, "terminals": 4, "weights": [45,43,4,4,4],
            "random_seed": 3, "warmup_seconds": 0, "duration_seconds": 20,
            "rate": "unlimited", "loader_threads": 1, "loader_batch_size": 128,
            "monitor_max_bootstrap_state_rows": 1_000_000,
            "monitor_max_bootstrap_state_bytes": 1_073_741_824,
            "stack_timeout_seconds": 180, "loader_timeout_seconds": 900,
            "workload_timeout_seconds": 300, "status_timeout_seconds": 900,
            "case_timeout_seconds": 1800, "kill_after_seconds": 15,
            "keep_stack": False, "redis_host": "host.docker.internal", "redis_port": 6379,
            "db_host": "host.docker.internal", "db_port": 3306,
            "expected_session_ids": SESSIONS, "minimum_per_type": 1,
            "accepted_outcomes": ["PASS", "VIOLATION"], "replay_capture_max_bytes": 10_000_000,
            "resource_exhaustion_metrics": None}
        log = "Completed Transactions:\n" + "\n".join(
            f"com.oltpbenchmark.benchmarks.tpcc.procedures.{name}/01 [{per_type.get(name,0)}]"
            for name in TRANSACTION_TYPES
        ) + """
Aborted Transactions:
<EMPTY>
Rejected Transactions (Server Retry):
<EMPTY>
Rejected Transactions (Retry Different):
<EMPTY>
Unexpected SQL Errors:
<EMPTY>
Unknown Status Transactions:
<EMPTY>
"""
        loader = {"observed_table_row_counts": bootstrap["table_row_counts"]}
        return {"config": config, "client": self.payload(client_records), "cdc": cdc,
                "status": self.payload(statuses, 20), "metrics": metrics, "log": log,
                "loader": loader, "run_id": RUN_ID, "per_type": per_type}

    def write(self, root, docs):
        paths = {}
        for name in ("client", "cdc", "status", "metrics", "loader"):
            paths[name] = root/f"{name}.json"; paths[name].write_text(json.dumps(docs[name]))
        paths["run"] = root/"run.txt"; paths["run"].write_text(RUN_ID)
        paths["log"] = root/"e2e.log"; paths["log"].write_text(docs["log"])
        resource = {"kind": "performance_measurement", "completed": False,
            "completion_status": "failed", "evidence_reconciled": False, "outcome": "INVALID",
            "diagnostics": [{"code": "RESOURCE_EXHAUSTED"}],
            "joiner": {"bootstrap_state_bytes": 268435152, "bootstrap_state_validated": False}}
        paths["resource"] = root/"resource.json"; paths["resource"].write_text(json.dumps(resource))
        docs["config"]["resource_exhaustion_metrics"] = {
            "path": str(paths["resource"]),
            "sha256": hashlib.sha256(paths["resource"].read_bytes()).hexdigest()}
        paths["config"] = root/"config.json"; paths["config"].write_text(json.dumps(docs["config"]))
        replay = {"kind": "tpcc_full_mix_replay", "run_id": RUN_ID,
            "capture_max_bytes": docs["config"]["replay_capture_max_bytes"], "streams": {}}
        for label in ("cdc", "client"):
            path = paths[label]; replay["streams"][label] = {"path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size}
        paths["replay"] = root/"replay.json"; paths["replay"].write_text(json.dumps(replay))
        return paths

    def validate(self, docs):
        with tempfile.TemporaryDirectory() as directory:
            p = self.write(Path(directory), docs)
            return ACCEPTANCE.validate_acceptance(p["config"], p["run"], p["client"], p["cdc"],
                p["status"], p["metrics"], p["log"], p["loader"], p["replay"])

    def test_clean_full_mix_has_exact_positive_per_type_counts(self):
        docs = self.documents(); report = self.validate(docs)
        self.assertEqual({name: row["clients"] for name, row in report["per_type"].items()}, docs["per_type"])
        self.assertEqual(report["sessions"], {"expected": 4, "completed": 4})
        self.assertEqual(report["resource_exhaustion"]["outcome"], "INVALID")

    def test_missing_type_marker_roster_and_resource_mutations_fail(self):
        docs = self.documents(); rows = [json.loads(e[1][1]) for e in docs["client"]]
        target = next(row for row in rows if row.get("kind") == "client_txn" and row["txn_type"] == "StockLevel")
        rows.remove(target); docs["client"] = self.payload(rows)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "lacks a transaction type"):
            self.validate(docs)
        docs = self.documents(); docs["cdc"] = docs["cdc"][:-6] + docs["cdc"][-3:]
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "marker reconciliation"):
            self.validate(docs)
        docs = self.documents(); rows = [json.loads(e[1][1]) for e in docs["client"]]
        manifest = next(row for row in rows if row.get("kind") == "run_manifest")
        manifest["application_table_allowlist"].pop(); docs["client"] = self.payload(rows)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "RunManifest|schema"):
            self.validate(docs)
        docs = self.documents()
        with tempfile.TemporaryDirectory() as directory:
            p = self.write(Path(directory), docs)
            p["resource"].write_text("{}")
            with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "checksum"):
                ACCEPTANCE.validate_acceptance(p["config"], p["run"], p["client"], p["cdc"],
                    p["status"], p["metrics"], p["log"], p["loader"], p["replay"])

    def test_benchbase_abort_retry_and_error_sections_are_fail_closed(self):
        docs = self.documents()
        docs["log"] = docs["log"].replace(
            "Aborted Transactions:\n<EMPTY>",
            "Aborted Transactions:\ncom.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder/01 [1]",
        )
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "aborted total"):
            self.validate(docs)
        docs = self.documents()
        docs["log"] += "Unexpected SQL Errors:\n<EMPTY>\n"
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "must contain one"):
            self.validate(docs)


class FullMixDriverTest(unittest.TestCase):
    DRIVER = ROOT / "tests/run_tpcc_full_mix_acceptance.sh"

    def test_print_config_requires_resource_reference_and_records_mix(self):
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as resource:
            resource.write("{}\n"); resource.flush()
            result = subprocess.run([str(self.DRIVER), "--print-config",
                "--resource-exhaustion-metrics", resource.name], text=True,
                capture_output=True, check=False, env=os.environ.copy())
        self.assertEqual(result.returncode, 0, result.stderr)
        config = json.loads(result.stdout)
        self.assertEqual(config["weights"], [45,43,4,4,4])
        self.assertEqual(config["terminals"], 4)


if __name__ == "__main__":
    unittest.main()
