from __future__ import annotations

import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Callable


ROOT = Path(__file__).resolve().parents[1] / "connector-contract" / "yugabytedb_ysql"
FIXTURE = ROOT / "fixtures" / "pgoutput_opaque_transaction_id_mismatch" / "redis-stream.json"
LIVE_CAPTURES = (
    ("pgoutput_cross_tablet_batching_gate", 2, 1),
    ("pgoutput_concurrent_commit_order_gate", 3, 2),
    ("pgoutput_redis_backpressure_gate", 2, 12),
    ("pgoutput_production_assets_gate_v3", 2, 1),
)


class YugabyteYsqlConnectorContractTest(unittest.TestCase):
    def run_verifier(self, capture: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(ROOT / "verify_raw_contract.py"),
                "--plugin",
                "pgoutput",
                *arguments,
                str(capture),
            ],
            check=True,
            text=True,
            capture_output=True,
        )

    def mutated_capture(
        self, mutate: Callable[[list[list[object]]], None], expected_violation: str
    ) -> None:
        capture = json.loads(FIXTURE.read_text(encoding="utf-8"))
        mutate(capture)
        with tempfile.TemporaryDirectory(prefix="yugabytedb-ysql-contract-test-") as temporary:
            path = Path(temporary) / "redis-stream.json"
            path.write_text(json.dumps(capture, separators=(",", ":")), encoding="utf-8")
            self.run_verifier(path, "--expect-violation", expected_violation)

    @staticmethod
    def value(capture: list[list[object]], index: int) -> dict[str, object]:
        fields = capture[index][1]
        assert isinstance(fields, list)
        value = json.loads(fields[1])
        assert isinstance(value, dict)
        return value

    @staticmethod
    def replace_value(capture: list[list[object]], index: int, value: dict[str, object]) -> None:
        fields = capture[index][1]
        assert isinstance(fields, list)
        fields[1] = json.dumps(value, separators=(",", ":"))

    def test_provenance_is_immutable_and_single_slot(self) -> None:
        subprocess.run([sys.executable, str(ROOT / "verify_provenance.py")], check=True)

    def test_captured_pgoutput_fixture_accepts_cross_record_id_variation(self) -> None:
        result = self.run_verifier(FIXTURE, "--redis-batch-size", "1000")
        summary = json.loads(result.stdout)
        self.assertEqual(summary["completed_transactions"], 1)
        self.assertEqual(summary["frames"][0]["frame_identity"], "1787413277950-0")
        self.assertEqual(summary["frames"][0]["marker"], {"session_id": "cross_tablet", "txn_id": 1})
        self.assertNotIn("opaque_transaction_id", summary)

    def test_live_gate_captures_and_artifact_manifests_pass(self) -> None:
        for directory, redis_batch_size, completed_transactions in LIVE_CAPTURES:
            with self.subTest(directory=directory):
                capture_dir = ROOT / "fixtures" / directory
                result = self.run_verifier(
                    capture_dir / "redis-stream.json",
                    "--redis-batch-size",
                    str(redis_batch_size),
                )
                summary = json.loads(result.stdout)
                self.assertEqual(summary["completed_transactions"], completed_transactions)
                subprocess.run(
                    ["sha256sum", "--check", "SHA256SUMS"],
                    check=True,
                    cwd=capture_dir,
                    text=True,
                    capture_output=True,
                )

    def test_arbitrary_bounded_cross_record_ids_remain_diagnostic_only(self) -> None:
        capture = json.loads(FIXTURE.read_text(encoding="utf-8"))
        for index in (0, 5):
            fields = capture[index][1]
            assert isinstance(fields, list)
            diagnostic_id = f"metadata-diagnostic-{index}"
            fields[0] = json.dumps({"id": diagnostic_id})
            value = json.loads(fields[1])
            value["id"] = diagnostic_id
            fields[1] = json.dumps(value, separators=(",", ":"))
        for index in range(1, 5):
            value = self.value(capture, index)
            transaction = value["transaction"]
            assert isinstance(transaction, dict)
            transaction["id"] = f"row-diagnostic-{index}"
            self.replace_value(capture, index, value)
        with tempfile.TemporaryDirectory(prefix="yugabytedb-ysql-contract-test-") as temporary:
            path = Path(temporary) / "redis-stream.json"
            path.write_text(json.dumps(capture, separators=(",", ":")), encoding="utf-8")
            self.run_verifier(path)

    def test_same_begin_record_key_envelope_id_mismatch_fails(self) -> None:
        def mutate(capture: list[list[object]]) -> None:
            fields = capture[0][1]
            assert isinstance(fields, list)
            fields[0] = json.dumps({"id": "different-in-the-same-record"})

        self.mutated_capture(mutate, "SAME_RECORD_TRANSACTION_ID_MISMATCH")

    def test_same_end_record_key_envelope_id_mismatch_fails(self) -> None:
        def mutate(capture: list[list[object]]) -> None:
            fields = capture[-1][1]
            assert isinstance(fields, list)
            fields[0] = json.dumps({"id": "different-in-the-same-record"})

        self.mutated_capture(mutate, "SAME_RECORD_TRANSACTION_ID_MISMATCH")

    def test_nested_begin_fails_as_interleaving(self) -> None:
        def mutate(capture: list[list[object]]) -> None:
            original_begin_fields = capture[0][1]
            assert isinstance(original_begin_fields, list)
            capture[1][1] = copy.deepcopy(original_begin_fields)

        self.mutated_capture(mutate, "TRANSACTION_INTERLEAVING")

    def test_partial_delivery_without_end_fails(self) -> None:
        self.mutated_capture(lambda capture: capture.pop(), "PARTIAL_DELIVERY")

    def test_noncontiguous_total_order_fails(self) -> None:
        def mutate(capture: list[list[object]]) -> None:
            value = self.value(capture, 2)
            transaction = value["transaction"]
            assert isinstance(transaction, dict)
            transaction["total_order"] = 7
            self.replace_value(capture, 2, value)

        self.mutated_capture(mutate, "ROW_ORDINAL_INVALID")

    def test_noncontiguous_collection_order_fails(self) -> None:
        def mutate(capture: list[list[object]]) -> None:
            value = self.value(capture, 2)
            transaction = value["transaction"]
            assert isinstance(transaction, dict)
            transaction["data_collection_order"] = 3
            self.replace_value(capture, 2, value)

        self.mutated_capture(mutate, "ROW_ORDINAL_INVALID")

    def test_wrong_end_total_count_fails(self) -> None:
        def mutate(capture: list[list[object]]) -> None:
            value = self.value(capture, 5)
            value["event_count"] = 3
            self.replace_value(capture, 5, value)

        self.mutated_capture(mutate, "END_COUNT_MISMATCH")

    def test_wrong_end_collection_count_fails(self) -> None:
        def mutate(capture: list[list[object]]) -> None:
            value = self.value(capture, 5)
            collections = value["data_collections"]
            assert isinstance(collections, list)
            collection = collections[1]
            assert isinstance(collection, dict)
            collection["event_count"] = 1
            self.replace_value(capture, 5, value)

        self.mutated_capture(mutate, "END_COUNT_MISMATCH")

    def test_marker_reordering_fails(self) -> None:
        def mutate(capture: list[list[object]]) -> None:
            capture[3][1], capture[4][1] = capture[4][1], capture[3][1]
            for index, total_order in ((3, 3), (4, 4)):
                value = self.value(capture, index)
                transaction = value["transaction"]
                assert isinstance(transaction, dict)
                transaction["total_order"] = total_order
                self.replace_value(capture, index, value)

        self.mutated_capture(mutate, "MARKER_NOT_LAST")

    def test_incomplete_full_image_fails(self) -> None:
        def mutate(capture: list[list[object]]) -> None:
            value = self.value(capture, 1)
            after = value["after"]
            assert isinstance(after, dict)
            del after["field10"]
            self.replace_value(capture, 1, value)

        self.mutated_capture(mutate, "FULL_IMAGE_INVALID")

    def test_source_version_drift_fails(self) -> None:
        def mutate(capture: list[list[object]]) -> None:
            value = self.value(capture, 1)
            source = value["source"]
            assert isinstance(source, dict)
            source["version"] = "unexpected"
            self.replace_value(capture, 1, value)

        self.mutated_capture(mutate, "SOURCE_VERSION_MISMATCH")


if __name__ == "__main__":
    unittest.main()
