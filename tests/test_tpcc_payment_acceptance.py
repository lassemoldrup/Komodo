#!/usr/bin/env python3

from __future__ import annotations

import base64
import copy
import importlib.util
import json
import os
import struct
import subprocess
import tempfile
import unittest
from collections import Counter
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "tests" / "verify_tpcc_payment_acceptance.py"
SPEC = importlib.util.spec_from_file_location("tpcc_payment_acceptance", TOOL)
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


class TpccPaymentAcceptanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.codec = CODEC.TpccCodec.from_protocol_root(ROOT / "protocol")
        fixture = json.loads(
            (ROOT / "protocol/fixtures/mariadb_tpcc_client_txns.json").read_text()
        )
        cls.rows = {entry["name"]: entry["row"] for entry in fixture["row_catalog"]}
        cls.run_manifest = fixture["run_manifest"]
        raw = json.loads(
            (ROOT / "protocol/fixtures/mariadb_tpcc_transactions.json").read_text()
        )
        cls.bootstrap_manifest = raw["bootstrap_manifest"]

    @staticmethod
    def b64(value: bytes) -> str:
        return base64.b64encode(value).decode("ascii")

    def read(self, table: str, row: dict, sequence: int, source: str) -> dict:
        key, value = self.codec.encode_row(table, row)
        return {
            "event_seq": sequence,
            "key_bytes_base64": self.b64(key),
            "source": source,
            "value_bytes_base64": self.b64(value),
            "value_state": "present",
        }

    def warehouse(self, ytd: str) -> dict:
        row = copy.deepcopy(self.rows["warehouse_after_payment"])
        row["w_ytd"] = ytd
        return row

    def district(self, district_id: int, ytd: str) -> dict:
        row = copy.deepcopy(self.rows["district_after_payment"])
        row.update({"d_id": district_id, "d_ytd": ytd})
        return row

    def customer(
        self, customer_id: int, last: str, first: str, credit: str
    ) -> dict:
        row = copy.deepcopy(self.rows["customer_before_payment"])
        row.update(
            {
                "c_w_id": 1,
                "c_d_id": 1,
                "c_id": customer_id,
                "c_last": last,
                "c_first": first,
                "c_credit": credit,
                "c_balance": "1000.00",
                "c_ytd_payment": 10.0,
                "c_payment_cnt": 1,
                "c_data": f"data-{customer_id}",
            }
        )
        return row

    @staticmethod
    def customer_after(before: dict, amount: Decimal) -> dict:
        after = copy.deepcopy(before)
        payment = struct.unpack(">f", struct.pack(">f", float(amount)))[0]
        balance = struct.unpack(
            ">f",
            struct.pack(
                ">f",
                struct.unpack(">f", struct.pack(">f", float(before["c_balance"])))[0]
                - payment,
            ),
        )[0]
        after["c_balance"] = str(
            Decimal(str(balance)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        )
        before_ytd = struct.unpack(
            ">f", struct.pack(">f", float(before["c_ytd_payment"]))
        )[0]
        after["c_ytd_payment"] = struct.unpack(
            ">f", struct.pack(">f", before_ytd + payment)
        )[0]
        after["c_payment_cnt"] += 1
        if before["c_credit"] == "BC":
            after["c_data"] = f"{before['c_id']} payment | {before['c_data']}"
        return after

    def client(
        self,
        sequence: int,
        warehouse_after: dict,
        district_after: dict,
        selection: list[dict],
        selected: dict,
    ) -> dict:
        rows = [
            ("benchbase.warehouse", warehouse_after, "local"),
            ("benchbase.district", district_after, "local"),
            *[("benchbase.customer", row, "snapshot") for row in selection],
        ]
        if selected["c_credit"] == "BC":
            rows.append(("benchbase.customer", selected, "snapshot"))
        reads = [
            self.read(table, row, index, source)
            for index, (table, row, source) in enumerate(rows)
        ]
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
            "read_digest": ACCEPTANCE.common.read_digest(reads),
            "reads": reads,
            "run_id": RUN_ID,
            "session_id": SESSION_ID,
            "session_seq": sequence,
            "txn_type": "Payment",
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
        sequence: int,
        rows: list[tuple[str, dict, dict | None, dict, str]],
        previous_marker: int | None,
    ) -> list:
        transaction_id = f"0-1-{sequence}"
        all_rows = [
            *rows,
            (
                "session_txn",
                {"session_id": SESSION_ID, "txn_id": sequence},
                None,
                {"session_id": SESSION_ID, "txn_id": sequence},
                "c",
            ),
        ]
        entries = [
            self.raw_entry(
                f"{prefix}-0",
                {"id": transaction_id},
                {"status": "BEGIN", "id": transaction_id, "event_count": None, "data_collections": None},
            )
        ]
        orders: Counter[str] = Counter()
        counts: Counter[str] = Counter()
        for total, (table, key, before, after, operation) in enumerate(all_rows, start=1):
            collection = f"benchbase.{table}"
            orders[collection] += 1
            counts[collection] += 1
            entries.append(
                self.raw_entry(
                    f"{prefix}-{total}",
                    key,
                    {
                        "before": before,
                        "after": after,
                        "op": operation,
                        "source": {
                            "version": "3.6.1.Final",
                            "connector": "mariadb",
                            "name": "mariadb-main",
                            "db": "benchbase",
                            "table": table,
                        },
                        "transaction": {
                            "id": transaction_id,
                            "total_order": total,
                            "data_collection_order": orders[collection],
                        },
                    },
                )
            )
        entries.append(
            self.raw_entry(
                f"{prefix}-{len(all_rows) + 1}",
                {"id": transaction_id},
                {
                    "status": "END",
                    "id": transaction_id,
                    "event_count": len(all_rows),
                    "data_collections": [
                        {"data_collection": name, "event_count": count}
                        for name, count in sorted(counts.items())
                    ],
                },
            )
        )
        return entries

    def payment_rows(
        self,
        warehouse_before: dict,
        district_before: dict,
        customer_before: dict,
        amount: Decimal,
        h_id: int,
    ) -> tuple[list, dict, dict, dict]:
        warehouse_after = copy.deepcopy(warehouse_before)
        warehouse_after["w_ytd"] = f"{Decimal(warehouse_before['w_ytd']) + amount:.2f}"
        district_after = copy.deepcopy(district_before)
        district_after["d_ytd"] = f"{Decimal(district_before['d_ytd']) + amount:.2f}"
        customer_after = self.customer_after(customer_before, amount)
        history = {
            "h_id": h_id,
            "h_c_d_id": customer_before["c_d_id"],
            "h_c_w_id": customer_before["c_w_id"],
            "h_c_id": customer_before["c_id"],
            "h_d_id": district_before["d_id"],
            "h_w_id": 1,
            "h_date": "2026-08-20T12:38:00Z",
            "h_amount": f"{amount:.2f}",
            "h_data": "warehouse    district",
        }
        rows = [
            ("warehouse", {"w_id": 1}, warehouse_before, warehouse_after, "u"),
            (
                "district",
                {"d_w_id": 1, "d_id": district_before["d_id"]},
                district_before,
                district_after,
                "u",
            ),
            (
                "customer",
                {
                    "c_w_id": customer_before["c_w_id"],
                    "c_d_id": customer_before["c_d_id"],
                    "c_id": customer_before["c_id"],
                },
                customer_before,
                customer_after,
                "u",
            ),
            ("history", {"h_id": h_id}, None, history, "c"),
        ]
        return rows, warehouse_after, district_after, customer_after

    def end_fence(self, prefix: int) -> list:
        transaction_id = "0-1-99"
        key = {"run_id": RUN_ID, "phase": "END_FENCE"}
        after = {
            **key,
            "nonce": "00112233-4455-6677-8899-aabbccddeeff",
        }
        return [
            self.raw_entry(
                f"{prefix}-0", {"id": transaction_id},
                {"status": "BEGIN", "id": transaction_id, "event_count": None, "data_collections": None},
            ),
            self.raw_entry(
                f"{prefix}-1", key,
                {
                    "before": None,
                    "after": after,
                    "op": "c",
                    "source": {"version": "3.6.1.Final", "connector": "mariadb", "name": "mariadb-main", "db": "benchbase", "table": "checker_run_fence"},
                    "transaction": {"id": transaction_id, "total_order": 1, "data_collection_order": 1},
                },
            ),
            self.raw_entry(
                f"{prefix}-2", {"id": transaction_id},
                {"status": "END", "id": transaction_id, "event_count": 1, "data_collections": [{"data_collection": "benchbase.checker_run_fence", "event_count": 1}]},
            ),
        ]

    def documents(self) -> dict[str, object]:
        groups = {
            "IDGOOD": [self.customer(i, "IDGOOD", chr(64 + i), "GC") for i in (1, 2, 3)],
            "IDBAD": [self.customer(i, "IDBAD", chr(64 + i), "BC" if i == 4 else "GC") for i in (4, 5, 6)],
            "NAMEGOOD": [self.customer(i, "NAMEGOOD", chr(55 + i), "GC") for i in (10, 11, 12)],
            "NAMEBAD": [self.customer(i, "NAMEBAD", chr(45 + i), "BC" if i == 21 else "GC") for i in (20, 21, 22)],
        }
        cases = [
            (groups["IDGOOD"][:1], groups["IDGOOD"][0], Decimal("10.00")),
            (groups["IDBAD"][:1], groups["IDBAD"][0], Decimal("20.00")),
            (groups["NAMEGOOD"], groups["NAMEGOOD"][1], Decimal("30.00")),
            (groups["NAMEBAD"], groups["NAMEBAD"][1], Decimal("40.00")),
        ]
        warehouse_state = self.warehouse("1000.00")
        clients = []
        cdc = []
        previous_marker = None
        for sequence, (selection, selected, amount) in enumerate(cases, start=1):
            district_before = self.district(sequence, "100.00")
            rows, warehouse_after, district_after, _ = self.payment_rows(
                warehouse_state, district_before, selected, amount, 3 + sequence
            )
            clients.append(
                self.client(sequence, warehouse_after, district_after, selection, selected)
            )
            cdc.extend(self.cdc_transaction(100 + sequence, sequence, rows, previous_marker))
            previous_marker = sequence
            warehouse_state = warehouse_after
        cdc.extend(self.end_fence(105))
        done = {
            "ambiguous_attempts": 0,
            "definitively_aborted_attempts": 0,
            "final_session_seq": 4,
            "kind": "session_done",
            "logical_transactions_started": 4,
            "physical_attempts_started": 4,
            "published_transactions": 4,
            "retry_attempts": 0,
            "rolling_correlation_digest": ACCEPTANCE.common.rolling_digest(RUN_ID, SESSION_ID, clients),
            "run_id": RUN_ID,
            "session_id": SESSION_ID,
            "successful_transactions": 4,
            "terminal_aborted_transactions": 0,
        }
        client_records = [copy.deepcopy(self.run_manifest), copy.deepcopy(self.bootstrap_manifest), *clients, done]
        load_status = {
            "kind": "status", "run_id": RUN_ID, "lifecycle_state": "LOAD_FINISHED_SEEN",
            "outcome": "RUNNING", "checker_observation": "PASS_SO_FAR",
            "last_cdc_stream_id": "100-0", "last_client_stream_id": "2-0",
            "assembled_cdc_transactions": 3, "joined_transactions": 0,
            "checked_transactions": 2, "pending_cdc_slots": 0,
            "unmatched_client_records": 0, "diagnostics": [],
        }
        reconciliation = {
            "evidence_reconciled": True,
            "startup_schema_attested": True, "final_schema_attested": True,
            "schema_attestation_digest": "0" * 64, "bootstrap_manifest_validated": True,
            "end_fence_seen": True, "expected_sessions": 1, "completed_sessions": 1,
            "accepted_client_transactions": 4, "joined_workload_transactions": 4,
            "bootstrap_cdc_transactions": 1, "bootstrap_checker_transactions": 1,
            "phase_anchor_transactions": 1, "checker_consumed_transactions": 6,
            "unmatched_client_records": 0, "unmatched_workload_cdc_slots": 0,
            "ambiguous_attempts": 0,
        }
        final = {
            "kind": "status", "run_id": RUN_ID, "lifecycle_state": "FINAL",
            "outcome": "PASS", "checker_observation": "PASS_SO_FAR",
            "last_cdc_stream_id": "105-2", "last_client_stream_id": "8-0",
            "assembled_cdc_transactions": 8, "joined_transactions": 4,
            "checked_transactions": 6, "pending_cdc_slots": 0,
            "unmatched_client_records": 0, "diagnostics": [],
            "reconciliation": reconciliation,
        }
        total_reads = sum(client["read_count"] for client in clients)
        metrics = {
            "kind": "performance_measurement", "run_id": RUN_ID,
            "processing_mode": "audited", "completed": True,
            "completion_status": "completed", "evidence_reconciled": True,
            "outcome": "PASS", "diagnostics": [],
            "limits": {"joiner": {"max_bootstrap_state_rows": 1_000_000, "max_bootstrap_state_bytes": 1_073_741_824}},
            "assembler": {"completed_run_control_transactions": 3, "completed_workload_transactions": 4},
            "joiner": {
                "accepted_client_transactions": 4, "accepted_workload_cdc_transactions": 4,
                "joined_workload_transactions": 4, "marker_only_workload_transactions": 0,
                "joined_read_events": total_reads, "joined_write_events": 16,
                "pending_client_records": 0, "pending_cdc_slots": 0,
                "unmatched_client_records": 0, "unmatched_workload_cdc_slots": 0,
            },
            "checker": {"workload_transactions": 4, "read_events": total_reads},
            "reconciliation": copy.deepcopy(reconciliation),
        }
        config = {
            "kind": "tpcc_payment_acceptance_config", "benchmark": "tpcc",
            "backend": "mariadb", "declared_isolation": "REPEATABLE_READ",
            "warehouses": 1, "terminals": 1, "weights": [0, 100, 0, 0, 0],
            "random_seed": 0, "warmup_seconds": 0, "duration_seconds": 10,
            "rate": "unlimited", "loader_threads": 1, "loader_batch_size": 128,
            "minimum_transactions": 4, "minimum_id_transactions": 1,
            "minimum_last_name_transactions": 1, "minimum_good_credit_transactions": 1,
            "minimum_bad_credit_transactions": 1,
            "minimum_remote_customer_transactions": 0, "minimum_retries": 0,
            "remote_customer_live_applicable": False,
            "monitor_max_bootstrap_state_rows": 1_000_000,
            "monitor_max_bootstrap_state_bytes": 1_073_741_824,
            "stack_timeout_seconds": 180, "loader_timeout_seconds": 900,
            "workload_timeout_seconds": 300, "status_timeout_seconds": 900,
            "case_timeout_seconds": 1800, "kill_after_seconds": 15,
            "keep_stack": False, "redis_host": "host.docker.internal", "redis_port": 6379,
            "db_host": "host.docker.internal", "db_port": 3306,
            "expected_session_ids": [SESSION_ID],
        }
        catalog = [
            {"c_w_id": row["c_w_id"], "c_d_id": row["c_d_id"], "c_id": row["c_id"],
             "c_last": row["c_last"], "c_first": row["c_first"], "c_credit": row["c_credit"],
             "last_name_group_count": 3}
            for group in groups.values() for row in group
        ]
        log = """
Completed Transactions:
com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder/01 [0]
com.oltpbenchmark.benchmarks.tpcc.procedures.Payment/02 [4]
com.oltpbenchmark.benchmarks.tpcc.procedures.OrderStatus/03 [0]
com.oltpbenchmark.benchmarks.tpcc.procedures.Delivery/04 [0]
com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel/05 [0]
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
        return {
            "config": config, "client": self.payload_xrange(client_records),
            "status": self.payload_xrange([load_status, final], 20), "cdc": cdc,
            "metrics": metrics, "catalog": catalog, "history": list(range(1, 8)),
            "loader": {"kind": "loader_validation", "benchmark": "tpcc",
                       "observed_table_row_counts": [{"table": "benchbase.history", "row_count": 3}]},
            "log": log, "run_id": RUN_ID,
        }

    def write_documents(self, root: Path, docs: dict[str, object]) -> dict[str, Path]:
        paths = {}
        for name in ("config", "client", "status", "cdc", "metrics", "loader"):
            path = root / f"{name}.json"
            path.write_text(json.dumps(docs[name], separators=(",", ":")), encoding="utf-8")
            paths[name] = path
        catalog = root / "customer.jsonl"
        catalog.write_text("".join(json.dumps(row) + "\n" for row in docs["catalog"]), encoding="utf-8")
        history = root / "history.jsonl"
        history.write_text("".join(json.dumps({"h_id": value}) + "\n" for value in docs["history"]), encoding="utf-8")
        log = root / "e2e.log"
        log.write_text(str(docs["log"]), encoding="utf-8")
        run_id = root / "run-id.txt"
        run_id.write_text(str(docs["run_id"]) + "\n", encoding="ascii")
        paths.update({"catalog": catalog, "history": history, "log": log, "run_id": run_id})
        return paths

    def validate(self, docs: dict[str, object]) -> dict:
        with tempfile.TemporaryDirectory() as directory:
            p = self.write_documents(Path(directory), docs)
            return ACCEPTANCE.validate_acceptance(
                p["config"], p["run_id"], p["client"], p["cdc"], p["status"],
                p["catalog"], p["history"], p["loader"], p["metrics"], p["log"]
            )

    @staticmethod
    def client_records(docs):
        return [json.loads(entry[1][1]) for entry in docs["client"]]

    def test_clean_acceptance_covers_all_payment_branches_and_history_identity(self):
        report = self.validate(self.documents())
        self.assertTrue(report["passed"])
        self.assertEqual(report["coverage"]["confirmed_id"], 2)
        self.assertEqual(report["coverage"]["confirmed_last_name"], 2)
        self.assertEqual(report["cdc"]["good_credit_transactions"], 2)
        self.assertEqual(report["cdc"]["bad_credit_transactions"], 2)
        self.assertEqual(report["history_identity"]["bootstrap"], 3)
        self.assertEqual(report["reconciliation"]["unmatched_client_records"], 0)

    def test_local_snapshot_order_and_cdata_reread_are_required(self):
        docs = self.documents()
        records = self.client_records(docs)
        client = next(row for row in records if row.get("kind") == "client_txn")
        client["reads"][0]["source"] = "snapshot"
        client["read_digest"] = ACCEPTANCE.common.read_digest(client["reads"])
        docs["client"] = self.payload_xrange(records)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "local/snapshot"):
            self.validate(docs)
        docs = self.documents()
        records = self.client_records(docs)
        bad = next(row for row in records if row.get("kind") == "client_txn" and row["read_count"] == 4)
        bad["reads"].pop()
        bad["read_count"] -= 1
        bad["read_digest"] = ACCEPTANCE.common.read_digest(bad["reads"])
        docs["client"] = self.payload_xrange(records)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "c_data reread presence"):
            self.validate(docs)

    def test_incomplete_name_group_and_wrong_write_amount_are_rejected(self):
        docs = self.documents()
        records = self.client_records(docs)
        name = next(row for row in records if row.get("kind") == "client_txn" and row["read_count"] == 5)
        name["reads"].pop(3)
        for index, read in enumerate(name["reads"]):
            read["event_seq"] = index
        name["read_count"] -= 1
        name["read_digest"] = ACCEPTANCE.common.read_digest(name["reads"])
        docs["client"] = self.payload_xrange(records)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "complete name group"):
            self.validate(docs)
        docs = self.documents()
        history = next(
            entry for entry in docs["cdc"]
            if json.loads(entry[1][1]).get("source", {}).get("table") == "history"
        )
        envelope = json.loads(history[1][1])
        envelope["after"]["h_amount"] = "999.00"
        history[1][1] = json.dumps(envelope, separators=(",", ":"))
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "payment amounts differ"):
            self.validate(docs)

    def test_history_collision_and_remote_customer_are_rejected(self):
        docs = self.documents()
        docs["history"][-1] = 3
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "history catalog repeats"):
            self.validate(docs)
        docs = self.documents()
        customer = next(
            entry for entry in docs["cdc"]
            if json.loads(entry[1][1]).get("source", {}).get("table") == "customer"
        )
        key, envelope = json.loads(customer[1][0]), json.loads(customer[1][1])
        key["c_w_id"] = 2
        envelope["before"]["c_w_id"] = 2
        envelope["after"]["c_w_id"] = 2
        customer[1][0] = json.dumps(key)
        customer[1][1] = json.dumps(envelope)
        with self.assertRaisesRegex(
            ACCEPTANCE.AcceptanceError, "customer warehouse differs"
        ):
            self.validate(docs)

    def test_customer_balance_cent_and_ytd_ulp_mutations_are_rejected(self):
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
        customer[1][1] = json.dumps(envelope, separators=(",", ":"))
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "customer payment arithmetic"):
            self.validate(docs)

        docs = self.documents()
        customer = next(
            entry
            for entry in docs["cdc"]
            if json.loads(entry[1][1]).get("source", {}).get("table") == "customer"
        )
        envelope = json.loads(customer[1][1])
        bits = struct.unpack(">I", struct.pack(">f", envelope["after"]["c_ytd_payment"]))[0]
        envelope["after"]["c_ytd_payment"] = struct.unpack(">f", struct.pack(">I", bits + 1))[0]
        customer[1][1] = json.dumps(envelope, separators=(",", ":"))
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "customer payment arithmetic"):
            self.validate(docs)

    def test_session_final_limit_and_benchbase_outcome_mutations_fail(self):
        docs = self.documents()
        records = self.client_records(docs)
        done = next(row for row in records if row.get("kind") == "session_done")
        done["rolling_correlation_digest"] = "0" * 64
        docs["client"] = self.payload_xrange(records)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "rolling correlation"):
            self.validate(docs)
        docs = self.documents()
        final = json.loads(docs["status"][-1][1][1])
        final["reconciliation"]["joined_workload_transactions"] = 3
        docs["status"][-1][1][1] = json.dumps(final)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "final reconciliation"):
            self.validate(docs)
        docs = self.documents()
        docs["metrics"]["limits"]["joiner"]["max_bootstrap_state_bytes"] = 1
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "limit provenance"):
            self.validate(docs)
        docs = self.documents()
        docs["log"] = docs["log"].replace("Payment/02 [4]", "Payment/02 [3]")
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "completed counts"):
            self.validate(docs)


class TpccPaymentAcceptanceDriverTest(unittest.TestCase):
    DRIVER = ROOT / "tests" / "run_tpcc_payment_acceptance.sh"

    def run_driver(self, *args, environment=None):
        env = os.environ.copy()
        if environment:
            env.update(environment)
        return subprocess.run([str(self.DRIVER), *args], text=True, capture_output=True, check=False, env=env)

    def test_print_config_records_profile_controls(self):
        result = self.run_driver(
            "--print-config", "--minimum-transactions", "20",
            "--minimum-bad-credit-transactions", "2",
            environment={"BLACKBOX_TPCC_PAYMENT_DURATION_SECONDS": "7"},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        config = json.loads(result.stdout)
        self.assertEqual(config["weights"], [0, 100, 0, 0, 0])
        self.assertEqual(config["minimum_transactions"], 20)
        self.assertEqual(config["minimum_bad_credit_transactions"], 2)
        self.assertFalse(config["remote_customer_live_applicable"])
        self.assertEqual(config["duration_seconds"], 7)

    def test_print_config_rejects_wrong_weights(self):
        result = self.run_driver("--print-config", "--weights", "1,99,0,0,0")
        self.assertEqual(result.returncode, 2)
        self.assertIn("weights must be exactly", result.stderr)


if __name__ == "__main__":
    unittest.main()
