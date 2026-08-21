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
TOOL = ROOT / "tests/verify_tpcc_stock_level_acceptance.py"
SPEC = importlib.util.spec_from_file_location("tpcc_stock_level_acceptance", TOOL)
assert SPEC is not None and SPEC.loader is not None
ACCEPTANCE = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(ACCEPTANCE)
CODEC_SPEC = importlib.util.spec_from_file_location(
    "tpcc_codec", ROOT / "protocol/tools/tpcc_codec.py"
)
assert CODEC_SPEC is not None and CODEC_SPEC.loader is not None
CODEC = importlib.util.module_from_spec(CODEC_SPEC); CODEC_SPEC.loader.exec_module(CODEC)
RUN_ID = "87654321-4321-4321-8321-cba987654322"
SESSION = "tpcc/worker/1"


class StockLevelAcceptanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.codec = CODEC.TpccCodec.from_protocol_root(ROOT / "protocol")
        fixture = json.loads((ROOT / "protocol/fixtures/mariadb_tpcc_client_txns.json").read_text())
        cls.rows = {entry["name"]: entry["row"] for entry in fixture["row_catalog"]}
        cls.manifest = fixture["run_manifest"]
        raw = json.loads((ROOT / "protocol/fixtures/mariadb_tpcc_transactions.json").read_text())
        cls.bootstrap = raw["bootstrap_manifest"]
        typed = json.loads((ROOT / "protocol/fixtures/mariadb_tpcc_typed_rows.json").read_text())
        cls.stock = next(row["after"] for row in typed["rows"] if row["table"] == "benchbase.stock")

    def read(self, table, row, source="snapshot", state="present"):
        key, value = self.codec.encode_row(table, row)
        result = {"event_seq": 0, "key_bytes_base64": base64.b64encode(key).decode(),
                  "source": source, "value_state": state}
        if state == "present": result["value_bytes_base64"] = base64.b64encode(value).decode()
        return result

    def client(self, seq, district):
        row = copy.deepcopy(self.rows["district_after_new_order"]); row["d_id"] = district
        reads = [self.read("benchbase.district", row)]
        return {"attempt_id": f"20000000-0000-4000-8000-{seq:012d}",
            "correlation_id": {"run_id": RUN_ID, "session_id": SESSION, "txn_id": seq},
            "correlation_kind": "session_txn", "kind": "client_txn",
            "logical_txn_id": f"10000000-0000-4000-8000-{seq:012d}",
            "read_count": 1, "read_digest": ACCEPTANCE.common.read_digest(reads), "reads": reads,
            "run_id": RUN_ID, "session_id": SESSION, "session_seq": seq, "txn_type": "StockLevel"}

    @staticmethod
    def payload(rows, start=1):
        return [[f"{start+i}-0", ["payload", json.dumps(row, separators=(",", ":"))]]
                for i, row in enumerate(rows)]

    @staticmethod
    def raw(stream_id, key, value):
        return [stream_id, [json.dumps(key, separators=(",", ":")),
                            json.dumps(value, separators=(",", ":"))]]

    def marker_transaction(self, prefix, seq, previous):
        txid = f"tx{seq}"; before = None if previous is None else {"session_id": SESSION, "txn_id": previous}
        after = {"session_id": SESSION, "txn_id": seq}
        return [self.raw(f"{prefix}-0", {"id": txid}, {"status": "BEGIN", "id": txid,
                "event_count": None, "data_collections": None}),
            self.raw(f"{prefix}-1", {"session_id": SESSION},
                {"before": before, "after": after, "op": "c" if previous is None else "u",
                 "source": {"version": "3.6.1.Final", "connector": "mariadb",
                            "name": "mariadb-main", "db": "benchbase", "table": "session_txn"},
                 "transaction": {"id": txid, "total_order": 1, "data_collection_order": 1}}),
            self.raw(f"{prefix}-2", {"id": txid}, {"status": "END", "id": txid,
                "event_count": 1, "data_collections": [{"data_collection": "benchbase.session_txn",
                                                          "event_count": 1}]})]

    def fence(self):
        txid = "fence"; key = {"run_id": RUN_ID, "phase": "END_FENCE"}
        return [self.raw("103-0", {"id": txid}, {"status": "BEGIN", "id": txid,
                "event_count": None, "data_collections": None}),
            self.raw("103-1", key, {"before": None, "after": {**key, "nonce": RUN_ID}, "op": "c",
                "source": {"version": "3.6.1.Final", "connector": "mariadb", "name": "mariadb-main",
                           "db": "benchbase", "table": "checker_run_fence"},
                "transaction": {"id": txid, "total_order": 1, "data_collection_order": 1}}),
            self.raw("103-2", {"id": txid}, {"status": "END", "id": txid, "event_count": 1,
                "data_collections": [{"data_collection": "benchbase.checker_run_fence", "event_count": 1}]})]

    def documents(self):
        clients = [self.client(1, 1), self.client(2, 2)]
        done = {"ambiguous_attempts": 0, "definitively_aborted_attempts": 0,
            "final_session_seq": 2, "kind": "session_done", "logical_transactions_started": 2,
            "physical_attempts_started": 2, "published_transactions": 2, "retry_attempts": 0,
            "rolling_correlation_digest": ACCEPTANCE.common.rolling_digest(RUN_ID, SESSION, clients),
            "run_id": RUN_ID, "session_id": SESSION, "successful_transactions": 2,
            "terminal_aborted_transactions": 0}
        records = [copy.deepcopy(self.manifest), copy.deepcopy(self.bootstrap), *clients, done]
        cdc = [*self.marker_transaction(101, 1, None), *self.marker_transaction(102, 2, 1), *self.fence()]
        reconciliation = {"evidence_reconciled": True, "bootstrap_manifest_validated": True,
            "end_fence_seen": True, "expected_sessions": 1, "completed_sessions": 1,
            "accepted_client_transactions": 2, "joined_workload_transactions": 2,
            "bootstrap_cdc_transactions": 1, "bootstrap_checker_transactions": 1,
            "phase_anchor_transactions": 1, "checker_consumed_transactions": 4,
            "unmatched_client_records": 0, "unmatched_workload_cdc_slots": 0,
            "ambiguous_attempts": 0}
        load = {"kind": "status", "run_id": RUN_ID, "lifecycle_state": "LOAD_FINISHED_SEEN",
            "outcome": "RUNNING", "checker_observation": "PASS_SO_FAR",
            "last_cdc_stream_id": "100-0", "last_client_stream_id": "2-0",
            "assembled_cdc_transactions": 3, "joined_transactions": 0,
            "checked_transactions": 2, "pending_cdc_slots": 0,
            "unmatched_client_records": 0, "diagnostics": []}
        final = {"kind": "status", "run_id": RUN_ID, "lifecycle_state": "FINAL",
            "outcome": "PASS", "checker_observation": "PASS_SO_FAR",
            "last_cdc_stream_id": "103-2", "last_client_stream_id": "6-0",
            "assembled_cdc_transactions": 6, "joined_transactions": 2,
            "checked_transactions": 4, "pending_cdc_slots": 0,
            "unmatched_client_records": 0, "diagnostics": [], "reconciliation": reconciliation}
        metrics = {"kind": "performance_measurement", "run_id": RUN_ID,
            "processing_mode": "audited", "completed": True, "completion_status": "completed",
            "evidence_reconciled": True, "outcome": "PASS", "diagnostics": [],
            "limits": {"joiner": {"max_bootstrap_state_rows": 1_000_000,
                                   "max_bootstrap_state_bytes": 1_073_741_824}},
            "assembler": {"completed_run_control_transactions": 3, "completed_workload_transactions": 2},
            "joiner": {"accepted_client_transactions": 2, "accepted_workload_cdc_transactions": 2,
                "joined_workload_transactions": 2, "marker_only_workload_transactions": 2,
                "joined_read_events": 2, "joined_write_events": 0,
                "pending_client_records": 0, "pending_cdc_slots": 0,
                "unmatched_client_records": 0, "unmatched_workload_cdc_slots": 0},
            "checker": {"workload_transactions": 2, "read_events": 2},
            "reconciliation": copy.deepcopy(reconciliation)}
        config = {"kind": "tpcc_stock_level_acceptance_config", "benchmark": "tpcc",
            "backend": "mariadb", "declared_isolation": "REPEATABLE_READ", "warehouses": 1,
            "terminals": 1, "weights": [0, 0, 0, 0, 100], "random_seed": 3,
            "warmup_seconds": 0, "duration_seconds": 5, "rate": "unlimited",
            "loader_threads": 1, "loader_batch_size": 128, "minimum_transactions": 2,
            "monitor_max_bootstrap_state_rows": 1_000_000,
            "monitor_max_bootstrap_state_bytes": 1_073_741_824,
            "stack_timeout_seconds": 180, "loader_timeout_seconds": 900,
            "workload_timeout_seconds": 300, "status_timeout_seconds": 900,
            "case_timeout_seconds": 1800, "kill_after_seconds": 15, "keep_stack": False,
            "redis_host": "host.docker.internal", "redis_port": 6379,
            "db_host": "host.docker.internal", "db_port": 3306,
            "expected_session_ids": [SESSION]}
        log = """
Completed Transactions:
com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder/01 [0]
com.oltpbenchmark.benchmarks.tpcc.procedures.Payment/02 [0]
com.oltpbenchmark.benchmarks.tpcc.procedures.OrderStatus/03 [0]
com.oltpbenchmark.benchmarks.tpcc.procedures.Delivery/04 [0]
com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel/05 [2]
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
        return {"config": config, "client": self.payload(records), "cdc": cdc,
                "status": self.payload([load, final], 20), "metrics": metrics,
                "log": log, "run_id": RUN_ID}

    def validate(self, docs):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); paths = {}
            for name in ("config", "client", "cdc", "status", "metrics"):
                paths[name] = root / f"{name}.json"; paths[name].write_text(json.dumps(docs[name]))
            paths["run"] = root / "run.txt"; paths["run"].write_text(docs["run_id"])
            paths["log"] = root / "e2e.log"; paths["log"].write_text(docs["log"])
            return ACCEPTANCE.validate_acceptance(paths["config"], paths["run"], paths["client"],
                paths["cdc"], paths["status"], paths["metrics"], paths["log"])

    def test_clean_one_district_snapshot_and_marker_only(self):
        report = self.validate(self.documents())
        self.assertEqual(report["coverage"], {"transactions": 2, "reads": 2})
        self.assertEqual(report["cdc"]["markers"], 2)
        self.assertEqual(report["reconciliation"]["unmatched_client_records"], 0)

    def test_extra_stock_local_or_absent_read_is_rejected(self):
        for mutation in ("stock", "local", "absent"):
            with self.subTest(mutation=mutation):
                docs = self.documents(); rows = [json.loads(e[1][1]) for e in docs["client"]]
                client = next(row for row in rows if row.get("kind") == "client_txn")
                if mutation == "stock":
                    extra = self.read("benchbase.stock", self.stock); extra["event_seq"] = 1
                    client["reads"].append(extra); client["read_count"] = 2
                elif mutation == "local": client["reads"][0]["source"] = "local"
                else:
                    client["reads"][0].pop("value_bytes_base64"); client["reads"][0]["value_state"] = "absent"
                client["read_digest"] = ACCEPTANCE.common.read_digest(client["reads"])
                docs["client"] = self.payload(rows)
                with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "exactly one read|snapshot read"):
                    self.validate(docs)

    def test_application_cdc_session_final_and_log_mutations_fail(self):
        docs = self.documents(); row = docs["cdc"][1]
        envelope = json.loads(row[1][1]); envelope["source"]["table"] = "stock"; row[1][1] = json.dumps(envelope)
        end = json.loads(docs["cdc"][2][1][1]); end["data_collections"][0]["data_collection"] = "benchbase.stock"
        docs["cdc"][2][1][1] = json.dumps(end)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "application/aggregate"):
            self.validate(docs)
        docs = self.documents(); rows = [json.loads(e[1][1]) for e in docs["client"]]
        done = next(row for row in rows if row.get("kind") == "session_done")
        done["rolling_correlation_digest"] = "0" * 64; docs["client"] = self.payload(rows)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "rolling digest"):
            self.validate(docs)
        docs = self.documents(); final = json.loads(docs["status"][-1][1][1])
        final["reconciliation"]["joined_workload_transactions"] = 1; docs["status"][-1][1][1] = json.dumps(final)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "final reconciliation"):
            self.validate(docs)
        docs = self.documents(); docs["log"] = docs["log"].replace("StockLevel/05 [2]", "StockLevel/05 [1]")
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "completed counts"):
            self.validate(docs)


class StockLevelDriverTest(unittest.TestCase):
    DRIVER = ROOT / "tests/run_tpcc_stock_level_acceptance.sh"

    def test_print_config_and_wrong_weights(self):
        result = subprocess.run([str(self.DRIVER), "--print-config"], text=True,
                                capture_output=True, check=False, env=os.environ.copy())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["weights"], [0, 0, 0, 0, 100])
        result = subprocess.run([str(self.DRIVER), "--print-config", "--weights", "0,0,0,1,99"],
                                text=True, capture_output=True, check=False)
        self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
