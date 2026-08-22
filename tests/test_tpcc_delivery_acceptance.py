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
from collections import Counter
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "tests" / "verify_tpcc_delivery_acceptance.py"
SPEC = importlib.util.spec_from_file_location("tpcc_delivery_acceptance", TOOL)
if SPEC is None or SPEC.loader is None:  # pragma: no cover
    raise RuntimeError(f"cannot load {TOOL}")
ACCEPTANCE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ACCEPTANCE)

CODEC_SPEC = importlib.util.spec_from_file_location(
    "tpcc_codec", ROOT / "protocol" / "tools" / "tpcc_codec.py"
)
CODEC = importlib.util.module_from_spec(CODEC_SPEC)
assert CODEC_SPEC.loader is not None
CODEC_SPEC.loader.exec_module(CODEC)

RUN_ID = "87654321-4321-4321-8321-cba987654322"


class TpccDeliveryAcceptanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.codec = CODEC.TpccCodec.from_protocol_root(ROOT / "protocol")
        fixture = json.loads(
            (ROOT / "protocol/fixtures/mariadb_tpcc_client_txns.json").read_text()
        )
        cls.rows = {entry["name"]: entry["row"] for entry in fixture["row_catalog"]}
        cls.manifest = fixture["run_manifest"]
        raw = json.loads((ROOT / "protocol/fixtures/mariadb_tpcc_transactions.json").read_text())
        cls.bootstrap = raw["bootstrap_manifest"]

    def read(self, table, row, seq):
        key, value = self.codec.encode_row(table, row)
        return {"event_seq": seq, "key_bytes_base64": base64.b64encode(key).decode(),
                "source": "snapshot", "value_bytes_base64": base64.b64encode(value).decode(),
                "value_state": "present"}

    def pair(self, district, order_id, customer_id, line_count):
        new_order = {"no_w_id": 1, "no_d_id": district, "no_o_id": order_id}
        order = copy.deepcopy(self.rows["oorder_before_delivery"])
        order.update({"o_w_id": 1, "o_d_id": district, "o_id": order_id,
                      "o_c_id": customer_id, "o_carrier_id": None,
                      "o_ol_cnt": line_count})
        return new_order, order

    def client(self, session, seq, txn_id, pairs):
        reads = []
        for new_order, order in pairs:
            reads.append(self.read("benchbase.new_order", new_order, len(reads)))
            reads.append(self.read("benchbase.oorder", order, len(reads)))
        return {
            "attempt_id": f"20000000-0000-4000-8000-{txn_id:012d}",
            "correlation_id": {"run_id": RUN_ID, "session_id": session, "txn_id": txn_id},
            "correlation_kind": "session_txn", "kind": "client_txn",
            "logical_txn_id": f"10000000-0000-4000-8000-{seq:012d}",
            "read_count": len(reads), "read_digest": ACCEPTANCE.common.read_digest(reads),
            "reads": reads, "run_id": RUN_ID, "session_id": session,
            "session_seq": seq, "txn_type": "Delivery",
        }

    @staticmethod
    def payload(records, start=1):
        return [[f"{start+i}-0", ["payload", json.dumps(row, separators=(",", ":"))]]
                for i, row in enumerate(records)]

    @staticmethod
    def raw(entry_id, key, envelope):
        return [entry_id, [json.dumps(key, separators=(",", ":")),
                           json.dumps(envelope, separators=(",", ":"))]]

    def customer(self, district, customer_id):
        row = copy.deepcopy(self.rows["customer_after_delivery"])
        row.update({"c_w_id": 1, "c_d_id": district, "c_id": customer_id,
                    "c_balance": "1000.00", "c_delivery_cnt": 0})
        return row

    def app_rows(self, pair, carrier, line_count):
        new_order, order = pair
        district, order_id, customer_id = order["o_d_id"], order["o_id"], order["o_c_id"]
        order_after = copy.deepcopy(order); order_after["o_carrier_id"] = carrier
        rows = [("new_order", new_order, new_order, None, "d"),
                ("oorder", {"o_w_id": 1, "o_d_id": district, "o_id": order_id},
                 order, order_after, "u")]
        total = Decimal("0.00")
        for number in range(1, line_count + 1):
            before = {"ol_w_id": 1, "ol_d_id": district, "ol_o_id": order_id,
                      "ol_number": number, "ol_i_id": number, "ol_delivery_d": None,
                      "ol_amount": f"{number}.00", "ol_supply_w_id": 1,
                      "ol_quantity": number, "ol_dist_info": f"line{number}"}
            after = copy.deepcopy(before); after["ol_delivery_d"] = "2026-08-20T12:38:00Z"
            key = {name: before[name] for name in ("ol_w_id", "ol_d_id", "ol_o_id", "ol_number")}
            rows.append(("order_line", key, before, after, "u")); total += Decimal(before["ol_amount"])
        customer_before = self.customer(district, customer_id)
        customer_after = copy.deepcopy(customer_before)
        customer_after["c_balance"] = f"{Decimal(customer_before['c_balance']) + total:.2f}"
        customer_after["c_delivery_cnt"] += 1
        customer_key = {"c_w_id": 1, "c_d_id": district, "c_id": customer_id}
        rows.append(("customer", customer_key, customer_before, customer_after, "u"))
        return rows

    def transaction(self, prefix, txid, session, marker_id, previous, app_rows):
        all_rows = [*app_rows, ("session_txn", {"session_id": session},
                    None if previous is None else {"session_id": session, "txn_id": previous},
                    {"session_id": session, "txn_id": marker_id}, "c" if previous is None else "u")]
        result = [self.raw(f"{prefix}-0", {"id": txid},
                           {"status": "BEGIN", "id": txid, "event_count": None,
                            "data_collections": None})]
        counts, orders = Counter(), Counter()
        for total, (table, key, before, after, op) in enumerate(all_rows, 1):
            collection = f"benchbase.{table}"; counts[collection] += 1; orders[collection] += 1
            result.append(self.raw(f"{prefix}-{total}", key,
                {"before": before, "after": after, "op": op,
                 "source": {"version": "3.6.1.Final", "connector": "mariadb",
                            "name": "mariadb-main", "db": "benchbase", "table": table},
                 "transaction": {"id": txid, "total_order": total,
                                 "data_collection_order": orders[collection]}}))
        result.append(self.raw(f"{prefix}-{len(all_rows)+1}", {"id": txid},
            {"status": "END", "id": txid, "event_count": len(all_rows),
             "data_collections": [{"data_collection": name, "event_count": count}
                                  for name, count in sorted(counts.items())]}))
        return result

    def fence(self, prefix):
        txid = "fence"; key = {"run_id": RUN_ID, "phase": "END_FENCE"}
        return [self.raw(f"{prefix}-0", {"id": txid}, {"status": "BEGIN", "id": txid,
                "event_count": None, "data_collections": None}),
                self.raw(f"{prefix}-1", key,
                {"before": None, "after": {**key, "nonce": "00112233-4455-6677-8899-aabbccddeeff"},
                 "op": "c", "source": {"version": "3.6.1.Final", "connector": "mariadb",
                 "name": "mariadb-main", "db": "benchbase", "table": "checker_run_fence"},
                 "transaction": {"id": txid, "total_order": 1, "data_collection_order": 1}}),
                self.raw(f"{prefix}-2", {"id": txid}, {"status": "END", "id": txid,
                 "event_count": 1, "data_collections": [{"data_collection": "benchbase.checker_run_fence",
                                                           "event_count": 1}]})]

    def done(self, session, count):
        clients = self._clients_by_session[session]
        return {"ambiguous_attempts": 0, "definitively_aborted_attempts": 0,
                "final_session_seq": count, "kind": "session_done",
                "logical_transactions_started": count, "physical_attempts_started": count,
                "published_transactions": count, "retry_attempts": 0,
                "rolling_correlation_digest": ACCEPTANCE.common.rolling_digest(RUN_ID, session, clients),
                "run_id": RUN_ID, "session_id": session, "successful_transactions": count,
                "terminal_aborted_transactions": 0}

    def documents(self):
        s1, s2 = "tpcc/worker/1", "tpcc/worker/2"
        p1 = [self.pair(1, 2101, 101, 2), self.pair(2, 2102, 102, 3)]
        p2 = [self.pair(6, 2106, 106, 2)]
        c1, empty, c2 = self.client(s1, 1, 1, p1), self.client(s1, 2, 3, []), self.client(s2, 1, 1, p2)
        self._clients_by_session = {s1: [c1, empty], s2: [c2]}
        manifest = copy.deepcopy(self.manifest)
        manifest["expected_session_ids"] = [s1, s2]
        done_one = self.done(s1, 2)
        done_one.update(
            {
                "definitively_aborted_attempts": 1,
                "physical_attempts_started": 3,
                "retry_attempts": 1,
            }
        )
        records = [manifest, copy.deepcopy(self.bootstrap), c1, empty, c2,
                   done_one, self.done(s2, 1)]
        cdc = [*self.transaction(101, "a", s1, 1, None,
                 self.app_rows(p1[0], 7, 2) + self.app_rows(p1[1], 7, 3)),
               *self.transaction(102, "b", s1, 3, 1, []),
               *self.transaction(103, "c", s2, 1, None, self.app_rows(p2[0], 4, 2)),
               *self.fence(104)]
        reconciliation = {"evidence_reconciled": True,
            "startup_schema_attested": True, "final_schema_attested": True,
            "schema_attestation_digest": "0" * 64, "bootstrap_manifest_validated": True,
            "end_fence_seen": True, "expected_sessions": 2, "completed_sessions": 2,
            "accepted_client_transactions": 3, "joined_workload_transactions": 3,
            "bootstrap_cdc_transactions": 1, "bootstrap_checker_transactions": 1,
            "phase_anchor_transactions": 2, "checker_consumed_transactions": 6,
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
            "last_cdc_stream_id": "104-2", "last_client_stream_id": "7-0",
            "assembled_cdc_transactions": 7, "joined_transactions": 3,
            "checked_transactions": 6, "pending_cdc_slots": 0,
            "unmatched_client_records": 0, "diagnostics": [], "reconciliation": reconciliation}
        reads = sum(c["read_count"] for c in (c1, empty, c2)); writes = 16
        metrics = {"kind": "performance_measurement", "run_id": RUN_ID,
            "processing_mode": "audited", "completed": True, "completion_status": "completed",
            "evidence_reconciled": True, "outcome": "PASS", "diagnostics": [],
            "limits": {"joiner": {"max_bootstrap_state_rows": 1_000_000,
                                   "max_bootstrap_state_bytes": 1_073_741_824}},
            "assembler": {"completed_run_control_transactions": 3, "completed_workload_transactions": 3},
            "joiner": {"accepted_client_transactions": 3, "accepted_workload_cdc_transactions": 3,
                "joined_workload_transactions": 3, "marker_only_workload_transactions": 1,
                "joined_read_events": reads, "joined_write_events": writes,
                "pending_client_records": 0, "pending_cdc_slots": 0,
                "unmatched_client_records": 0, "unmatched_workload_cdc_slots": 0},
            "checker": {"workload_transactions": 3, "read_events": reads},
            "reconciliation": copy.deepcopy(reconciliation)}
        config = {"kind": "tpcc_delivery_acceptance_config", "benchmark": "tpcc",
            "backend": "mariadb", "declared_isolation": "REPEATABLE_READ", "warehouses": 1,
            "terminals": 2, "weights": [0, 0, 0, 100, 0], "random_seed": 0,
            "warmup_seconds": 0, "duration_seconds": 10, "rate": "unlimited",
            "loader_threads": 1, "loader_batch_size": 128, "minimum_transactions": 3,
            "minimum_empty_transactions": 1, "minimum_active_transactions": 1,
            "minimum_multi_district_transactions": 1, "minimum_multi_line_districts": 1,
            "minimum_retries": 1, "monitor_max_bootstrap_state_rows": 1_000_000,
            "monitor_max_bootstrap_state_bytes": 1_073_741_824, "stack_timeout_seconds": 180,
            "loader_timeout_seconds": 900, "workload_timeout_seconds": 300,
            "status_timeout_seconds": 900, "case_timeout_seconds": 1800,
            "kill_after_seconds": 15, "keep_stack": False, "redis_host": "host.docker.internal",
            "redis_port": 6379, "db_host": "host.docker.internal", "db_port": 3306,
            "expected_session_ids": [s1, s2]}
        log = """
Completed Transactions:
com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder/01 [0]
com.oltpbenchmark.benchmarks.tpcc.procedures.Payment/02 [0]
com.oltpbenchmark.benchmarks.tpcc.procedures.OrderStatus/03 [0]
com.oltpbenchmark.benchmarks.tpcc.procedures.Delivery/04 [3]
com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel/05 [0]
Aborted Transactions:
<EMPTY>
Rejected Transactions (Server Retry):
com.oltpbenchmark.benchmarks.tpcc.procedures.Delivery/04 [1]
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
                path = root / f"{name}.json"; path.write_text(json.dumps(docs[name])); paths[name] = path
            paths["run"] = root / "run.txt"; paths["run"].write_text(docs["run_id"])
            paths["log"] = root / "e2e.log"; paths["log"].write_text(docs["log"])
            return ACCEPTANCE.validate_acceptance(paths["config"], paths["run"], paths["client"],
                paths["cdc"], paths["status"], paths["metrics"], paths["log"])

    def test_clean_covers_empty_active_multi_district_and_multi_line(self):
        report = self.validate(self.documents())
        self.assertEqual(report["coverage"]["empty_transactions"], 1)
        self.assertEqual(report["coverage"]["multi_district_transactions"], 1)
        self.assertEqual(report["cdc"]["multi_line_districts"], 3)
        self.assertEqual(report["cdc"]["marker_only_transactions"], 1)
        self.assertEqual(report["reconciliation"]["unmatched_client_records"], 0)

    def test_aggregate_or_wrong_read_order_is_rejected(self):
        docs = self.documents(); records = [json.loads(e[1][1]) for e in docs["client"]]
        client = next(r for r in records if r.get("kind") == "client_txn" and r["read_count"] > 0)
        client["reads"].reverse()
        for i, read in enumerate(client["reads"]): read["event_seq"] = i
        client["read_digest"] = ACCEPTANCE.common.read_digest(client["reads"])
        docs["client"] = self.payload(records)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "aggregate/other read|pair order"):
            self.validate(docs)

    def test_active_district_outside_worker_partition_is_rejected(self):
        docs = self.documents()
        records = [json.loads(entry[1][1]) for entry in docs["client"]]
        client = next(
            row
            for row in records
            if row.get("kind") == "client_txn"
            and row.get("session_id") == "tpcc/worker/1"
            and row.get("session_seq") == 1
        )
        new_order, order = self.pair(6, 2106, 106, 2)
        client["reads"] = [
            self.read("benchbase.new_order", new_order, 0),
            self.read("benchbase.oorder", order, 1),
        ]
        client["read_count"] = 2
        client["read_digest"] = ACCEPTANCE.common.read_digest(client["reads"])
        docs["client"] = self.payload(records)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "worker range"):
            self.validate(docs)

    def test_delete_delivery_and_customer_mutations_fail(self):
        docs = self.documents()
        delete = next(e for e in docs["cdc"] if json.loads(e[1][1]).get("source", {}).get("table") == "new_order")
        envelope = json.loads(delete[1][1]); envelope["op"] = "u"; envelope["after"] = envelope["before"]
        delete[1][1] = json.dumps(envelope)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "new_order delete"):
            self.validate(docs)
        docs = self.documents()
        line = next(e for e in docs["cdc"] if json.loads(e[1][1]).get("source", {}).get("table") == "order_line")
        envelope = json.loads(line[1][1]); envelope["after"]["ol_delivery_d"] = None; line[1][1] = json.dumps(envelope)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "delivery update"):
            self.validate(docs)
        docs = self.documents()
        customer = next(e for e in docs["cdc"] if json.loads(e[1][1]).get("source", {}).get("table") == "customer")
        envelope = json.loads(customer[1][1]); envelope["after"]["c_delivery_cnt"] += 1; customer[1][1] = json.dumps(envelope)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "customer delivery"):
            self.validate(docs)

        docs = self.documents()
        lines = [
            entry
            for entry in docs["cdc"]
            if json.loads(entry[1][1]).get("source", {}).get("table") == "order_line"
            and json.loads(entry[1][1]).get("after", {}).get("ol_d_id") == 1
        ]
        envelope = json.loads(lines[1][1][1])
        envelope["after"]["ol_delivery_d"] = "2026-08-20T12:38:01Z"
        lines[1][1][1] = json.dumps(envelope)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "timestamps differ"):
            self.validate(docs)

        docs = self.documents()
        customer = next(
            entry
            for entry in docs["cdc"]
            if json.loads(entry[1][1]).get("source", {}).get("table") == "customer"
        )
        envelope = json.loads(customer[1][1])
        envelope["after"]["c_balance"] = str(
            Decimal(envelope["after"]["c_balance"]) + Decimal("0.01")
        )
        customer[1][1] = json.dumps(envelope)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "customer delivery"):
            self.validate(docs)

    def test_order_line_count_must_equal_captured_o_ol_cnt(self):
        docs = self.documents()
        records = [json.loads(entry[1][1]) for entry in docs["client"]]
        client = next(
            row
            for row in records
            if row.get("kind") == "client_txn"
            and row.get("session_id") == "tpcc/worker/1"
            and row["session_seq"] == 1
        )
        _, changed_order = self.pair(1, 2101, 101, 3)
        client["reads"][1] = self.read("benchbase.oorder", changed_order, 1)
        client["read_digest"] = ACCEPTANCE.common.read_digest(client["reads"])
        docs["client"] = self.payload(records)
        order_entry = next(
            entry
            for entry in docs["cdc"]
            if json.loads(entry[1][1]).get("source", {}).get("table") == "oorder"
            and json.loads(entry[1][1]).get("after", {}).get("o_d_id") == 1
        )
        envelope = json.loads(order_entry[1][1])
        envelope["before"]["o_ol_cnt"] = 3
        envelope["after"]["o_ol_cnt"] = 3
        order_entry[1][1] = json.dumps(envelope)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "update count differs"):
            self.validate(docs)

    def test_coverage_session_final_and_log_mutations_fail(self):
        docs = self.documents(); docs["config"]["minimum_empty_transactions"] = 2
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "empty Delivery"):
            self.validate(docs)
        docs = self.documents(); records = [json.loads(e[1][1]) for e in docs["client"]]
        done = next(r for r in records if r.get("kind") == "session_done")
        done["rolling_correlation_digest"] = "0" * 64; docs["client"] = self.payload(records)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "rolling digest"):
            self.validate(docs)
        docs = self.documents(); final = json.loads(docs["status"][-1][1][1])
        final["reconciliation"]["joined_workload_transactions"] = 2; docs["status"][-1][1][1] = json.dumps(final)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "final reconciliation"):
            self.validate(docs)
        docs = self.documents(); docs["log"] = docs["log"].replace("Delivery/04 [3]", "Delivery/04 [2]")
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "completed counts"):
            self.validate(docs)


class TpccDeliveryDriverTest(unittest.TestCase):
    DRIVER = ROOT / "tests/run_tpcc_delivery_acceptance.sh"

    def test_print_config_and_reject_topology(self):
        result = subprocess.run([str(self.DRIVER), "--print-config", "--minimum-retries", "2"],
                                text=True, capture_output=True, check=False, env=os.environ.copy())
        self.assertEqual(result.returncode, 0, result.stderr)
        config = json.loads(result.stdout)
        self.assertEqual(config["weights"], [0, 0, 0, 100, 0])
        self.assertEqual(config["minimum_retries"], 2)
        result = subprocess.run([str(self.DRIVER), "--print-config", "--terminals", "1"],
                                text=True, capture_output=True, check=False)
        self.assertEqual(result.returncode, 2)
        self.assertIn("between 2 and 10", result.stderr)
        result = subprocess.run([str(self.DRIVER), "--print-config", "--terminals", "11"],
                                text=True, capture_output=True, check=False)
        self.assertEqual(result.returncode, 2)
        self.assertIn("between 2 and 10", result.stderr)


if __name__ == "__main__":
    unittest.main()
