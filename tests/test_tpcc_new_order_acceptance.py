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
TOOL = ROOT / "tests" / "verify_tpcc_new_order_acceptance.py"
SPEC = importlib.util.spec_from_file_location("tpcc_new_order_acceptance", TOOL)
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


class TpccNewOrderAcceptanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.codec = CODEC.TpccCodec.from_protocol_root(ROOT / "protocol")
        client_fixture = json.loads(
            (ROOT / "protocol/fixtures/mariadb_tpcc_client_txns.json").read_text()
        )
        cls.catalog = {entry["name"]: entry for entry in client_fixture["row_catalog"]}
        cls.run_manifest = client_fixture["run_manifest"]
        raw_fixture = json.loads(
            (ROOT / "protocol/fixtures/mariadb_tpcc_transactions.json").read_text()
        )
        cls.bootstrap_manifest = raw_fixture["bootstrap_manifest"]
        typed_fixture = json.loads(
            (ROOT / "protocol/fixtures/mariadb_tpcc_typed_rows.json").read_text()
        )
        cls.stock_template = next(
            copy.deepcopy(row["after"])
            for row in typed_fixture["rows"]
            if row["table"] == "benchbase.stock"
        )

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

    def customer(self, district_id: int, customer_id: int) -> dict:
        row = copy.deepcopy(self.catalog["customer_after_delivery"]["row"])
        row.update({"c_d_id": district_id, "c_id": customer_id})
        return row

    def warehouse(self) -> dict:
        return copy.deepcopy(self.catalog["warehouse_after_payment"]["row"])

    def item(self, item_id: int) -> dict:
        row = copy.deepcopy(self.catalog["item_snapshot"]["row"])
        row.update({"i_id": item_id, "i_name": f"item{item_id}"})
        return row

    def client(
        self,
        session_sequence: int,
        txn_id: int,
        district_id: int,
        customer_id: int,
        item_ids: list[int],
    ) -> dict:
        rows = [
            ("benchbase.customer", self.customer(district_id, customer_id)),
            ("benchbase.warehouse", self.warehouse()),
            *[("benchbase.item", self.item(item_id)) for item_id in item_ids],
        ]
        reads = [self.read(table, row, index) for index, (table, row) in enumerate(rows)]
        return {
            "attempt_id": f"20000000-0000-4000-8000-{txn_id:012d}",
            "correlation_id": {
                "run_id": RUN_ID,
                "session_id": SESSION_ID,
                "txn_id": txn_id,
            },
            "correlation_kind": "session_txn",
            "kind": "client_txn",
            "logical_txn_id": f"10000000-0000-4000-8000-{session_sequence:012d}",
            "read_count": len(reads),
            "read_digest": ACCEPTANCE.common.read_digest(reads),
            "reads": reads,
            "run_id": RUN_ID,
            "session_id": SESSION_ID,
            "session_seq": session_sequence,
            "txn_type": "NewOrder",
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

    @staticmethod
    def district(district_id: int, next_order_id: int) -> dict:
        return {
            "d_w_id": 1,
            "d_id": district_id,
            "d_ytd": "0.00",
            "d_tax": "0.0000",
            "d_next_o_id": next_order_id,
            "d_name": "D",
            "d_street_1": "street",
            "d_street_2": "",
            "d_city": "city",
            "d_state": "Z",
            "d_zip": "9",
        }

    def stock(self, item_id: int, quantity: int = 100, ytd: str = "0.00", count: int = 0) -> dict:
        row = copy.deepcopy(self.stock_template)
        row.update(
            {
                "s_w_id": 1,
                "s_i_id": item_id,
                "s_quantity": quantity,
                "s_ytd": ytd,
                "s_order_cnt": count,
                "s_remote_cnt": 0,
            }
        )
        return row

    @staticmethod
    def stock_after(before: dict, quantity: int, initial_quantity: int | None = None) -> dict:
        after = copy.deepcopy(before)
        base_quantity = before["s_quantity"] if initial_quantity is None else initial_quantity
        after["s_quantity"] = (
            base_quantity - quantity
            if base_quantity - quantity >= 10
            else base_quantity - quantity + 91
        )
        after["s_ytd"] = f"{float(before['s_ytd']) + quantity:.2f}"
        after["s_order_cnt"] += 1
        return after

    @staticmethod
    def order_line_amount(quantity: int) -> str:
        price = struct.unpack(">f", struct.pack(">f", 999.99))[0]
        multiplied = struct.unpack(">f", struct.pack(">f", price * quantity))[0]
        return str(
            Decimal(str(multiplied)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        )

    def workload_rows(
        self,
        district_id: int,
        customer_id: int,
        item_ids: list[int],
        next_order_id: int,
    ) -> list[tuple[str, dict, dict | None, dict, str]]:
        before_district = self.district(district_id, next_order_id)
        after_district = copy.deepcopy(before_district)
        after_district["d_next_o_id"] += 1
        rows: list[tuple[str, dict, dict | None, dict, str]] = [
            (
                "district",
                {"d_w_id": 1, "d_id": district_id},
                before_district,
                after_district,
                "u",
            ),
            (
                "oorder",
                {"o_w_id": 1, "o_d_id": district_id, "o_id": next_order_id},
                None,
                {
                    "o_w_id": 1,
                    "o_d_id": district_id,
                    "o_id": next_order_id,
                    "o_c_id": customer_id,
                    "o_carrier_id": None,
                    "o_ol_cnt": len(item_ids),
                    "o_all_local": 1,
                    "o_entry_d": "2026-08-20T12:37:00Z",
                },
                "c",
            ),
            (
                "new_order",
                {"no_w_id": 1, "no_d_id": district_id, "no_o_id": next_order_id},
                None,
                {"no_w_id": 1, "no_d_id": district_id, "no_o_id": next_order_id},
                "c",
            ),
        ]
        line_quantities: list[int] = []
        for number, item_id in enumerate(item_ids, start=1):
            quantity = number
            line_quantities.append(quantity)
            rows.append(
                (
                    "order_line",
                    {
                        "ol_w_id": 1,
                        "ol_d_id": district_id,
                        "ol_o_id": next_order_id,
                        "ol_number": number,
                    },
                    None,
                    {
                        "ol_w_id": 1,
                        "ol_d_id": district_id,
                        "ol_o_id": next_order_id,
                        "ol_number": number,
                        "ol_i_id": item_id,
                        "ol_delivery_d": None,
                        "ol_amount": self.order_line_amount(quantity),
                        "ol_supply_w_id": 1,
                        "ol_quantity": quantity,
                        "ol_dist_info": f"line{number}",
                    },
                    "c",
                )
            )
        stock_state: dict[int, dict] = {}
        initial_stock: dict[int, dict] = {}
        for item_id, quantity in zip(item_ids, line_quantities, strict=True):
            initial_stock.setdefault(item_id, self.stock(item_id))
            before = copy.deepcopy(stock_state.get(item_id, initial_stock[item_id]))
            after = self.stock_after(
                before, quantity, initial_stock[item_id]["s_quantity"]
            )
            stock_state[item_id] = after
            rows.append(
                (
                    "stock",
                    {"s_w_id": 1, "s_i_id": item_id},
                    before,
                    after,
                    "u",
                )
            )
        return rows

    def cdc_transaction(
        self,
        prefix: int,
        transaction_id: str,
        application_rows: list[tuple[str, dict, dict | None, dict, str]],
        txn_id: int,
        previous_txn_id: int | None,
    ) -> list:
        rows = [
            *application_rows,
            (
                "session_txn",
                {"session_id": SESSION_ID},
                None
                if previous_txn_id is None
                else {"session_id": SESSION_ID, "txn_id": previous_txn_id},
                {"session_id": SESSION_ID, "txn_id": txn_id},
                "c" if previous_txn_id is None else "u",
            ),
        ]
        entries = [
            self.raw_entry(
                f"{prefix}-0",
                {"id": transaction_id},
                {"status": "BEGIN", "id": transaction_id, "event_count": None, "data_collections": None},
            )
        ]
        collection_orders: Counter[str] = Counter()
        collection_counts: Counter[str] = Counter()
        for total_order, (table, key, before, after, operation) in enumerate(rows, start=1):
            collection = f"benchbase.{table}"
            collection_orders[collection] += 1
            collection_counts[collection] += 1
            entries.append(
                self.raw_entry(
                    f"{prefix}-{total_order}",
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
                            "total_order": total_order,
                            "data_collection_order": collection_orders[collection],
                        },
                    },
                )
            )
        entries.append(
            self.raw_entry(
                f"{prefix}-{len(rows) + 1}",
                {"id": transaction_id},
                {
                    "status": "END",
                    "id": transaction_id,
                    "event_count": len(rows),
                    "data_collections": [
                        {"data_collection": table, "event_count": count}
                        for table, count in sorted(collection_counts.items())
                    ],
                },
            )
        )
        return entries

    def end_fence(self, prefix: int) -> list:
        transaction_id = "0-1-9"
        key = {"run_id": RUN_ID, "phase": "END_FENCE"}
        after = {
            "run_id": RUN_ID,
            "phase": "END_FENCE",
            "nonce": "00112233-4455-6677-8899-aabbccddeeff",
        }
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
                    "before": None,
                    "after": after,
                    "op": "c",
                    "source": {
                        "version": "3.6.1.Final",
                        "connector": "mariadb",
                        "name": "mariadb-main",
                        "db": "benchbase",
                        "table": "checker_run_fence",
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
                    "data_collections": [
                        {
                            "data_collection": "benchbase.checker_run_fence",
                            "event_count": 1,
                        }
                    ],
                },
            ),
        ]

    def documents(self) -> dict[str, object]:
        items_one = [1, 2, 1, 3, 4]
        items_two = [5, 6, 7, 8, 9]
        first = self.client(1, 1, 1, 10, items_one)
        second = self.client(3, 4, 2, 20, items_two)
        clients = [first, second]
        done = {
            "ambiguous_attempts": 0,
            "definitively_aborted_attempts": 2,
            "final_session_seq": 3,
            "kind": "session_done",
            "logical_transactions_started": 3,
            "physical_attempts_started": 4,
            "published_transactions": 2,
            "retry_attempts": 1,
            "rolling_correlation_digest": ACCEPTANCE.common.rolling_digest(
                RUN_ID, SESSION_ID, clients
            ),
            "run_id": RUN_ID,
            "session_id": SESSION_ID,
            "successful_transactions": 2,
            "terminal_aborted_transactions": 1,
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
        first_rows = self.workload_rows(1, 10, items_one, 3001)
        second_rows = self.workload_rows(2, 20, items_two, 3001)
        cdc = [
            *self.cdc_transaction(101, "0-1-1", first_rows, 1, None),
            *self.cdc_transaction(102, "0-1-4", second_rows, 4, 1),
            *self.end_fence(103),
        ]
        normalized_writes = (3 + len(items_one) + len(set(items_one))) + (
            3 + len(items_two) + len(set(items_two))
        )
        total_reads = sum(client["read_count"] for client in clients)
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
                "marker_only_workload_transactions": 0,
                "joined_read_events": total_reads,
                "joined_write_events": normalized_writes,
                "pending_client_records": 0,
                "pending_cdc_slots": 0,
                "unmatched_client_records": 0,
                "unmatched_workload_cdc_slots": 0,
            },
            "checker": {"workload_transactions": 2, "read_events": total_reads},
            "reconciliation": copy.deepcopy(reconciliation),
        }
        config = {
            "kind": "tpcc_new_order_acceptance_config",
            "benchmark": "tpcc",
            "backend": "mariadb",
            "declared_isolation": "REPEATABLE_READ",
            "warehouses": 1,
            "terminals": 1,
            "weights": [100, 0, 0, 0, 0],
            "random_seed": 3,
            "warmup_seconds": 0,
            "duration_seconds": 5,
            "rate": "unlimited",
            "loader_threads": 1,
            "loader_batch_size": 128,
            "minimum_transactions": 2,
            "minimum_invalid_item_aborts": 1,
            "minimum_repeated_key_transactions": 1,
            "minimum_retries": 1,
            "remote_item_live_applicable": False,
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
        return {
            "config": config,
            "client": self.payload_xrange(client_records),
            "status": self.payload_xrange([load_status, final], start=20),
            "cdc": cdc,
            "metrics": metrics,
            "benchbase_log": """
Completed Transactions:
com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder/01 [2]
com.oltpbenchmark.benchmarks.tpcc.procedures.Payment/02 [0]
com.oltpbenchmark.benchmarks.tpcc.procedures.OrderStatus/03 [0]
com.oltpbenchmark.benchmarks.tpcc.procedures.Delivery/04 [0]
com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel/05 [0]
Aborted Transactions:
com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder/01 [1]
Rejected Transactions (Server Retry):
com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder/01 [1]
Rejected Transactions (Retry Different):
<EMPTY>
Unexpected SQL Errors:
<EMPTY>
Unknown Status Transactions:
<EMPTY>
""",
            "run_id": RUN_ID,
        }

    def write_documents(self, root: Path, documents: dict[str, object]) -> dict[str, Path]:
        paths = {name: root / f"{name}.json" for name in ("config", "client", "status", "cdc", "metrics")}
        for name, path in paths.items():
            path.write_text(json.dumps(documents[name], separators=(",", ":")), encoding="utf-8")
        run_id = root / "run-id.txt"
        run_id.write_text(str(documents["run_id"]) + "\n", encoding="ascii")
        benchbase_log = root / "e2e.log"
        benchbase_log.write_text(str(documents["benchbase_log"]), encoding="utf-8")
        paths.update({"run_id": run_id, "benchbase_log": benchbase_log})
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
                paths["metrics"],
                paths["benchbase_log"],
            )

    @staticmethod
    def client_records(documents: dict[str, object]) -> list[dict]:
        return [json.loads(entry[1][1]) for entry in documents["client"]]

    def replace_client_records(self, documents: dict[str, object], records: list[dict]) -> None:
        documents["client"] = self.payload_xrange(records)

    def test_clean_acceptance_proves_reads_writes_abort_retry_and_repeated_keys(self) -> None:
        report = self.validate(self.documents())
        self.assertTrue(report["passed"])
        self.assertEqual(report["coverage"]["terminal_invalid_item_aborts"], 1)
        self.assertEqual(report["coverage"]["retries"], 1)
        self.assertEqual(report["coverage"]["retry_surplus_increases"], 1)
        self.assertEqual(report["benchbase_outcomes"]["expected_user_aborts"], 1)
        self.assertEqual(report["coverage"]["repeated_key_transactions"], 1)
        self.assertEqual(report["cdc"]["markers"], 2)
        self.assertEqual(report["reconciliation"]["unmatched_client_records"], 0)

    def test_district_or_stock_client_read_is_rejected(self) -> None:
        documents = self.documents()
        records = self.client_records(documents)
        client = next(record for record in records if record.get("kind") == "client_txn")
        stock = self.stock(99)
        client["reads"].append(self.read("benchbase.stock", stock, len(client["reads"])))
        client["read_count"] += 1
        client["read_digest"] = ACCEPTANCE.common.read_digest(client["reads"])
        self.replace_client_records(documents, records)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "customer, warehouse, item-only"):
            self.validate(documents)

    def test_missing_or_arithmetically_wrong_stock_write_is_rejected(self) -> None:
        documents = self.documents()
        first_end_index = next(
            index
            for index, entry in enumerate(documents["cdc"])
            if json.loads(entry[1][1]).get("status") == "END"
        )
        stock_index = next(
            index
            for index, entry in enumerate(documents["cdc"][:first_end_index])
            if json.loads(entry[1][1]).get("source", {}).get("table") == "stock"
        )
        documents["cdc"].pop(stock_index)
        end = json.loads(documents["cdc"][first_end_index - 1][1][1])
        end["event_count"] -= 1
        stock_collection = next(
            item for item in end["data_collections"] if item["data_collection"] == "benchbase.stock"
        )
        stock_collection["event_count"] -= 1
        documents["cdc"][first_end_index - 1][1][1] = json.dumps(end, separators=(",", ":"))
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "row metadata differs"):
            self.validate(documents)

        documents = self.documents()
        stock_entry = next(
            entry
            for entry in documents["cdc"]
            if json.loads(entry[1][1]).get("source", {}).get("table") == "stock"
        )
        envelope = json.loads(stock_entry[1][1])
        envelope["after"]["s_quantity"] += 1
        stock_entry[1][1] = json.dumps(envelope, separators=(",", ":"))
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "stock arithmetic"):
            self.validate(documents)

        documents = self.documents()
        line_entry = next(
            entry
            for entry in documents["cdc"]
            if json.loads(entry[1][1]).get("source", {}).get("table") == "order_line"
        )
        envelope = json.loads(line_entry[1][1])
        envelope["after"]["ol_amount"] = str(
            Decimal(envelope["after"]["ol_amount"]) + Decimal("0.01")
        )
        line_entry[1][1] = json.dumps(envelope, separators=(",", ":"))
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "order-line amount"):
            self.validate(documents)

    def test_invalid_abort_and_retry_attempt_identity_accounting_are_required(self) -> None:
        documents = self.documents()
        records = self.client_records(documents)
        done = next(record for record in records if record.get("kind") == "session_done")
        done["terminal_aborted_transactions"] = 0
        self.replace_client_records(documents, records)
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "attempt/logical algebra"):
            self.validate(documents)

        documents = self.documents()
        records = self.client_records(documents)
        clients = [record for record in records if record.get("kind") == "client_txn"]
        clients[1]["correlation_id"]["txn_id"] = 3
        done = next(record for record in records if record.get("kind") == "session_done")
        done["rolling_correlation_digest"] = ACCEPTANCE.common.rolling_digest(
            RUN_ID, SESSION_ID, clients
        )
        self.replace_client_records(documents, records)
        marker_entry = next(
            entry
            for entry in documents["cdc"]
            if json.loads(entry[1][1]).get("after") == {"session_id": SESSION_ID, "txn_id": 4}
        )
        marker = json.loads(marker_entry[1][1])
        marker["after"]["txn_id"] = 3
        marker_entry[1][1] = json.dumps(marker, separators=(",", ":"))
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "new-attempt marker identity"):
            self.validate(documents)

    def test_benchbase_outcomes_must_attribute_abort_and_exclude_error_categories(self) -> None:
        documents = self.documents()
        documents["benchbase_log"] = documents["benchbase_log"].replace(
            "NewOrder/01 [1]\nRejected Transactions (Server Retry)",
            "NewOrder/01 [0]\nRejected Transactions (Server Retry)",
        )
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "aborted count"):
            self.validate(documents)

        documents = self.documents()
        documents["benchbase_log"] = documents["benchbase_log"].replace(
            "Unexpected SQL Errors:\n<EMPTY>",
            "Unexpected SQL Errors:\ncom.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder/01 [1]",
        )
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "Unexpected SQL Errors.*not empty"):
            self.validate(documents)

    def test_remote_stock_key_and_final_or_limit_mismatch_are_rejected(self) -> None:
        documents = self.documents()
        stock_entry = next(
            entry
            for entry in documents["cdc"]
            if json.loads(entry[1][1]).get("source", {}).get("table") == "stock"
        )
        key = json.loads(stock_entry[1][0])
        envelope = json.loads(stock_entry[1][1])
        key["s_w_id"] = 2
        envelope["before"]["s_w_id"] = 2
        envelope["after"]["s_w_id"] = 2
        stock_entry[1][0] = json.dumps(key, separators=(",", ":"))
        stock_entry[1][1] = json.dumps(envelope, separators=(",", ":"))
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "stock update key/shape"):
            self.validate(documents)

        documents = self.documents()
        final = json.loads(documents["status"][-1][1][1])
        final["reconciliation"]["joined_workload_transactions"] = 1
        documents["status"][-1][1][1] = json.dumps(final, separators=(",", ":"))
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "final reconciliation counters"):
            self.validate(documents)

        documents = self.documents()
        documents["metrics"]["limits"]["joiner"]["max_bootstrap_state_bytes"] = 268_435_456
        with self.assertRaisesRegex(ACCEPTANCE.AcceptanceError, "limit provenance"):
            self.validate(documents)


class TpccNewOrderAcceptanceDriverTest(unittest.TestCase):
    DRIVER = ROOT / "tests" / "run_tpcc_new_order_acceptance.sh"

    def run_driver(self, *arguments: str, environment: dict[str, str] | None = None):
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

    def test_print_config_records_new_order_profile_and_common_controls(self) -> None:
        result = self.run_driver(
            "--print-config",
            "--terminals",
            "3",
            "--minimum-transactions",
            "20",
            "--minimum-retries",
            "2",
            environment={"BLACKBOX_TPCC_NEW_ORDER_DURATION_SECONDS": "7"},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        config = json.loads(result.stdout)
        self.assertEqual(config["weights"], [100, 0, 0, 0, 0])
        self.assertEqual(config["terminals"], 3)
        self.assertEqual(config["minimum_transactions"], 20)
        self.assertEqual(config["minimum_retries"], 2)
        self.assertFalse(config["remote_item_live_applicable"])
        self.assertEqual(config["duration_seconds"], 7)

    def test_print_config_rejects_wrong_weights_and_seed_overflow(self) -> None:
        result = self.run_driver("--print-config", "--weights", "99,1,0,0,0")
        self.assertEqual(result.returncode, 2)
        self.assertIn("weights must be exactly", result.stderr)
        result = self.run_driver("--print-config", "--random-seed", "2147483648")
        self.assertEqual(result.returncode, 2)
        self.assertIn("signed 32-bit", result.stderr)


if __name__ == "__main__":
    unittest.main()
