#!/usr/bin/env python3

from __future__ import annotations

import copy
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
TRANSACTIONS = load_module("tpcc_transaction_fixture.py")


class TpccTransactionFixtureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.path = ROOT / "protocol" / "fixtures" / "mariadb_tpcc_transactions.json"
        cls.document = json.loads(cls.path.read_text(encoding="utf-8"))
        cls.codec = CODEC.TpccCodec.from_protocol_root(ROOT / "protocol")

    def test_independent_normalizer_matches_all_captured_transactions(self) -> None:
        normalized = TRANSACTIONS.verify_transaction_fixture(self.document, self.codec)
        self.assertEqual(len(normalized), 11)
        self.assertEqual(normalized[1]["kind"], "BOOTSTRAP_APP")
        self.assertEqual(normalized[3]["correlation_id"]["txn_id"], 1)
        self.assertEqual(normalized[6]["writes"], [])
        self.assertEqual(
            [write["state"] for write in normalized[7]["writes"]],
            ["absent", "present"],
        )
        self.assertEqual(normalized[8]["writes"][0]["state"], "absent")
        self.assertEqual(normalized[9]["writes"][0]["state"], "present")

    def test_raw_end_count_mutation_is_not_self_validating(self) -> None:
        document = copy.deepcopy(self.document)
        raw_end = document["transactions"][3]["raw_entries"][-1][1][1]
        envelope = json.loads(raw_end)
        envelope["event_count"] += 1
        document["transactions"][3]["raw_entries"][-1][1][1] = json.dumps(envelope)
        with self.assertRaisesRegex(
            TRANSACTIONS.TransactionFixtureError, "END event_count mismatch"
        ):
            TRANSACTIONS.verify_transaction_fixture(document, self.codec)

    def test_expected_write_mutation_is_not_self_validating(self) -> None:
        document = copy.deepcopy(self.document)
        document["transactions"][4]["expected"]["writes"][0]["value_hex"] += "00"
        with self.assertRaisesRegex(
            TRANSACTIONS.TransactionFixtureError, "expected normalization differs"
        ):
            TRANSACTIONS.verify_transaction_fixture(document, self.codec)

    def test_marker_and_bootstrap_manifest_mutations_fail(self) -> None:
        document = copy.deepcopy(self.document)
        marker_transaction = document["transactions"][6]
        marker_transaction["raw_entries"].pop(-2)
        end = json.loads(marker_transaction["raw_entries"][-1][1][1])
        end["event_count"] -= 1
        end["data_collections"] = []
        marker_transaction["raw_entries"][-1][1][1] = json.dumps(end)
        with self.assertRaisesRegex(
            TRANSACTIONS.TransactionFixtureError, "raw entries missing"
        ):
            TRANSACTIONS.verify_transaction_fixture(document, self.codec)

        document = copy.deepcopy(self.document)
        document["bootstrap_manifest"]["canonical_state_digest"] = "0" * 64
        with self.assertRaisesRegex(
            TRANSACTIONS.TransactionFixtureError, "bootstrap digest mismatch"
        ):
            TRANSACTIONS.verify_transaction_fixture(document, self.codec)

    def test_marker_before_image_chain_is_independently_checked(self) -> None:
        document = copy.deepcopy(self.document)
        marker_entry = document["transactions"][4]["raw_entries"][-2]
        envelope = json.loads(marker_entry[1][1])
        envelope["before"]["txn_id"] = 99
        marker_entry[1][1] = json.dumps(envelope)
        with self.assertRaisesRegex(
            TRANSACTIONS.TransactionFixtureError, "marker before-image chain mismatch"
        ):
            TRANSACTIONS.verify_transaction_fixture(document, self.codec)


if __name__ == "__main__":
    unittest.main()
