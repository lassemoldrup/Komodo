#!/usr/bin/env python3

from __future__ import annotations

import copy
import base64
import importlib.util
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "protocol" / "tools"
sys.path.insert(0, str(TOOLS))


def load_module(name: str):
    path = TOOLS / name
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:  # pragma: no cover
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CODEC = load_module("tpcc_codec.py")
CLIENTS = load_module("tpcc_client_fixture.py")


class TpccClientFixtureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        fixture_root = ROOT / "protocol" / "fixtures"
        cls.document = json.loads(
            (fixture_root / "mariadb_tpcc_client_txns.json").read_text(encoding="utf-8")
        )
        cls.raw_transactions = json.loads(
            (fixture_root / "mariadb_tpcc_transactions.json").read_text(encoding="utf-8")
        )
        cls.codec = CODEC.TpccCodec.from_protocol_root(ROOT / "protocol")

    def verify(self, document: dict) -> list[dict]:
        return CLIENTS.verify_client_fixture(document, self.codec, self.raw_transactions)

    def test_independent_validator_matches_all_correlated_clients(self) -> None:
        clients = self.verify(self.document)
        self.assertEqual(len(clients), 8)
        self.assertEqual(
            [client["txn_type"] for client in clients],
            [
                "Payment",
                "NewOrder",
                "Delivery",
                "OrderStatus",
                "Payment",
                "Payment",
                "NewOrder",
                "StockLevel",
            ],
        )
        semantic = self.document["semantic_transactions"]
        self.assertEqual({entry["client_txn"]["txn_type"] for entry in semantic}, {
            "NewOrder",
            "Payment",
            "OrderStatus",
            "Delivery",
            "StockLevel",
        })

    def test_client_bytes_and_digest_mutations_fail_independently(self) -> None:
        document = copy.deepcopy(self.document)
        client = document["semantic_transactions"][0]["client_txn"]
        raw_value = base64.b64decode(client["reads"][0]["value_bytes_base64"])
        client["reads"][0]["value_bytes_base64"] = base64.b64encode(
            raw_value + b"\x00"
        ).decode("ascii")
        client["read_digest"] = CLIENTS.read_digest(client["reads"])
        with self.assertRaisesRegex(CLIENTS.ClientFixtureError, "client value bytes"):
            self.verify(document)

        document = copy.deepcopy(self.document)
        document["semantic_transactions"][0]["client_txn"]["read_digest"] = "0" * 64
        with self.assertRaisesRegex(CLIENTS.ClientFixtureError, "client read digest mismatch"):
            self.verify(document)

    def test_statement_identity_and_correlation_mutations_fail(self) -> None:
        document = copy.deepcopy(self.document)
        document["semantic_transactions"][0]["read_provenance"][0][
            "trace_statement_id"
        ] = "payGetWhseSQL"
        with self.assertRaisesRegex(
            CLIENTS.ClientFixtureError, "provenance differs from statement contract"
        ):
            self.verify(document)

        document = copy.deepcopy(self.document)
        document["semantic_transactions"][0]["client_txn"]["correlation_id"][
            "txn_id"
        ] = 2
        with self.assertRaisesRegex(CLIENTS.ClientFixtureError, "correlation mismatch"):
            self.verify(document)

    def test_self_consistent_read_that_differs_from_raw_state_fails(self) -> None:
        document = copy.deepcopy(self.document)
        catalog_row = next(
            entry
            for entry in document["row_catalog"]
            if entry["name"] == "order_status_order"
        )
        catalog_row["row"]["o_c_id"] = 2
        key, value = self.codec.encode_row(catalog_row["table"], catalog_row["row"])
        order_status = next(
            entry
            for entry in document["semantic_transactions"]
            if entry["client_txn"]["txn_type"] == "OrderStatus"
        )
        read = order_status["client_txn"]["reads"][1]
        read["key_bytes_base64"] = base64.b64encode(key).decode("ascii")
        read["value_bytes_base64"] = base64.b64encode(value).decode("ascii")
        order_status["client_txn"]["read_digest"] = CLIENTS.read_digest(
            order_status["client_txn"]["reads"]
        )
        with self.assertRaisesRegex(
            CLIENTS.ClientFixtureError, "differs from raw CDC state"
        ):
            self.verify(document)

    def test_discarded_absent_read_is_never_published_and_has_real_key_bytes(self) -> None:
        document = copy.deepcopy(self.document)
        document["discarded_attempt_reads"][0]["published"] = True
        with self.assertRaisesRegex(CLIENTS.ClientFixtureError, "discarded attempt fixture semantics"):
            self.verify(document)

        document = copy.deepcopy(self.document)
        document["discarded_attempt_reads"][0]["read_provenance"][-1]["key"][
            "i_id"
        ] -= 1
        with self.assertRaisesRegex(CLIENTS.ClientFixtureError, "absent key bytes"):
            self.verify(document)

    def test_empty_delivery_result_emits_no_read(self) -> None:
        document = copy.deepcopy(self.document)
        document["empty_result_assertions"][0]["emitted_reads"] = [{}]
        with self.assertRaisesRegex(CLIENTS.ClientFixtureError, "empty-result assertion"):
            self.verify(document)

    def test_multirow_empty_value_and_session_digest_are_required(self) -> None:
        document = copy.deepcopy(self.document)
        order_status = next(
            entry
            for entry in document["semantic_transactions"]
            if entry["client_txn"]["txn_type"] == "OrderStatus"
        )
        order_status["client_txn"]["reads"].pop()
        order_status["read_provenance"].pop()
        order_status["client_txn"]["read_count"] -= 1
        order_status["client_txn"]["read_digest"] = CLIENTS.read_digest(
            order_status["client_txn"]["reads"]
        )
        with self.assertRaisesRegex(CLIENTS.ClientFixtureError, "multirow coverage"):
            self.verify(document)

        document = copy.deepcopy(self.document)
        delivery = next(
            entry
            for entry in document["semantic_transactions"]
            if entry["client_txn"]["txn_type"] == "Delivery"
        )
        delivery["client_txn"]["reads"].pop(0)
        delivery["read_provenance"].pop(0)
        delivery["client_txn"]["reads"][0]["event_seq"] = 0
        delivery["read_provenance"][0]["event_seq"] = 0
        delivery["client_txn"]["read_count"] = 1
        delivery["client_txn"]["read_digest"] = CLIENTS.read_digest(
            delivery["client_txn"]["reads"]
        )
        with self.assertRaisesRegex(CLIENTS.ClientFixtureError, "empty canonical value"):
            self.verify(document)

        document = copy.deepcopy(self.document)
        document["session_done"]["rolling_correlation_digest"] = "0" * 64
        with self.assertRaisesRegex(CLIENTS.ClientFixtureError, "rolling digest mismatch"):
            self.verify(document)


if __name__ == "__main__":
    unittest.main()
