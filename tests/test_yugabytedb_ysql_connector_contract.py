from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Callable


ROOT = Path(__file__).resolve().parents[1] / "connector-contract" / "yugabytedb_ysql"


def canonical_capture() -> list[list[object]]:
    def encoded_entry(
        stream_id: str, key: dict[str, object], value: dict[str, object]
    ) -> list[object]:
        return [
            stream_id,
            [
                json.dumps(key, separators=(",", ":")),
                json.dumps(value, separators=(",", ":")),
            ],
        ]

    def source(table: str, sequence: int) -> dict[str, object]:
        return {
            "version": "dz.2.5.2.yb.2026.1",
            "connector": "postgresql",
            "name": "connector_contract",
            "ts_ms": 1,
            "snapshot": "false",
            "db": "benchbase",
            "sequence": f'["1","{sequence}"]',
            "schema": "benchbase",
            "table": table,
            "txId": 1,
            "lsn": sequence,
            "xmin": None,
            "origin": None,
            "origin_lsn": None,
        }

    def row(
        stream_id: str,
        table: str,
        key: dict[str, object],
        after: dict[str, object],
        total_order: int,
        collection_order: int,
    ) -> list[object]:
        return encoded_entry(
            stream_id,
            key,
            {
                "before": None,
                "after": after,
                "source": source(table, total_order + 1),
                "op": "c",
                "ts_ms": 1,
                "transaction": {
                    "id": f"row-diagnostic-{total_order}",
                    "total_order": total_order,
                    "data_collection_order": collection_order,
                },
            },
        )

    user_one: dict[str, object] = {"ycsb_key": 1}
    user_two: dict[str, object] = {"ycsb_key": 2}
    for index in range(1, 11):
        user_one[f"field{index}"] = "alpha" if index == 1 else None
        user_two[f"field{index}"] = "beta" if index == 1 else None

    return [
        encoded_entry(
            "1-0",
            {"id": "begin-diagnostic"},
            {
                "status": "BEGIN",
                "id": "begin-diagnostic",
                "event_count": None,
                "data_collections": None,
                "ts_ms": 1,
            },
        ),
        row("2-0", "usertable", {"ycsb_key": 1}, user_one, 1, 1),
        row("3-0", "usertable", {"ycsb_key": 2}, user_two, 2, 2),
        row(
            "4-0",
            "checker_run_fence",
            {"run_id": "00000000-0000-0000-0000-000000000001", "phase": "TEST"},
            {
                "run_id": "00000000-0000-0000-0000-000000000001",
                "phase": "TEST",
                "nonce": "00000000-0000-0000-0000-000000000002",
            },
            3,
            1,
        ),
        row(
            "5-0",
            "session_txn",
            {"session_id": "cross_tablet"},
            {"session_id": "cross_tablet", "txn_id": 1},
            4,
            1,
        ),
        encoded_entry(
            "6-0",
            {"id": "end-diagnostic"},
            {
                "status": "END",
                "id": "end-diagnostic",
                "event_count": 4,
                "data_collections": [
                    {"data_collection": "benchbase.usertable", "event_count": 2},
                    {
                        "data_collection": "benchbase.checker_run_fence",
                        "event_count": 1,
                    },
                    {"data_collection": "benchbase.session_txn", "event_count": 1},
                ],
                "ts_ms": 1,
            },
        ),
    ]


class YugabyteYsqlConnectorContractTest(unittest.TestCase):
    def test_monitor_decoder_preserves_literal_hex_escape_text(self) -> None:
        sys.path.insert(0, str(ROOT))
        module_path = ROOT / "extract_connector_output.py"
        spec = importlib.util.spec_from_file_location(
            "yugabytedb_ysql_extract_connector_output", module_path
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        literal = module.decode_redis_monitor_command(
            '"XADD" "cdc" "*" "field" "literal\\\\x41"', 1
        )
        encoded = module.decode_redis_monitor_command(
            '"XADD" "cdc" "*" "field" "\\x41"', 2
        )
        self.assertEqual(literal[-1], r"literal\x41")
        self.assertEqual(encoded[-1], "A")

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
        capture = canonical_capture()
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
        with tempfile.TemporaryDirectory(prefix="opaque-id-raw-fixture-") as temporary:
            capture = Path(temporary) / "redis-stream.json"
            capture.write_text(json.dumps(canonical_capture()), encoding="utf-8")
            result = self.run_verifier(capture, "--redis-batch-size", "1000")
        summary = json.loads(result.stdout)
        self.assertEqual(summary["completed_transactions"], 1)
        self.assertEqual(summary["frames"][0]["frame_identity"], "1-0")
        self.assertEqual(summary["frames"][0]["marker"], {"session_id": "cross_tablet", "txn_id": 1})
        self.assertNotIn("opaque_transaction_id", summary)

    def test_capture_report_requires_independent_ordering_artifacts(self) -> None:
        with tempfile.TemporaryDirectory(prefix="missing-order-evidence-") as temporary:
            capture = Path(temporary) / "redis-stream.json"
            capture.write_text(json.dumps(canonical_capture()), encoding="utf-8")
            metadata = {
                "capture_id": "synthetic",
                "captured_at_utc": "2026-01-01T00:00:00Z",
                "scenario": "cross_tablet_marker_last",
                "platform": "linux/amd64",
                "plugin": "pgoutput",
                "slot_lsn_type": "SEQUENCE",
                "streaming_mode": "default",
                "logical_slots": 1,
                "publication_count": 1,
                "publication_name": "connector_contract_publication",
                "connector_tasks": 1,
                "redis_streams": ["cdc"],
                "source_max_batch_size": 2,
                "redis_sink_batch_size": 2,
                "database_image": "pinned",
                "database_binary_version": "pinned",
                "database_build_hash": "0" * 40,
                "connector_version": "dz.2.5.2.yb.2026.1",
                "connector_sha256": "0" * 64,
                "debezium_server_version": "2.5.2.Final",
                "java_runtime_version": "21",
                "redis_version": "7",
                "locally_built_image_id": "sha256:" + "0" * 64,
                "expected_completed_transactions": [
                    {"session_id": "cross_tablet", "txn_id": 1}
                ],
                "gate": {
                    "name": "raw_connector_redis_contract",
                    "status": "PASSED",
                    "decision_boundary": (
                        "Connector IDs, source.txId, LSN, and HybridTime are diagnostic metadata only; "
                        "the verifier frames only the unique contiguous BEGIN/rows/END interval and "
                        "correlates completed frames only by the SQL-last client marker."
                    ),
                },
                "file_sha256": {
                    "redis-stream.json": hashlib.sha256(capture.read_bytes()).hexdigest()
                },
            }
            capture.with_name("capture-metadata.json").write_text(
                json.dumps(metadata), encoding="utf-8"
            )
            self.run_verifier(
                capture,
                "--redis-batch-size",
                "2",
                "--expect-violation",
                "INDEPENDENT_ORDER_EVIDENCE_MISSING",
            )

    def test_connector_output_extractor_uses_monitor_commands_not_expected_order(self) -> None:
        capture = canonical_capture()
        monitor_lines = ["OK"]
        for sequence, entry in enumerate(capture, start=1):
            fields = entry[1]
            assert isinstance(fields, list)
            command = ["XADD", "cdc", "*", fields[0], fields[1]]
            monitor_lines.append(
                f"{sequence}.0 [0 connector:6379] "
                + " ".join(json.dumps(token) for token in command)
            )
        monitor_lines[2] = monitor_lines[2].replace("alpha", r"\xc3\xa9")
        with tempfile.TemporaryDirectory(prefix="connector-output-extractor-") as temporary:
            monitor = Path(temporary) / "redis-monitor.log"
            monitor.write_text("\n".join(monitor_lines) + "\n", encoding="utf-8")
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "extract_connector_output.py"),
                    "--redis-batch-size",
                    "2",
                    str(monitor),
                ],
                check=True,
                text=True,
                capture_output=True,
            )
        evidence = json.loads(result.stdout)
        self.assertEqual(evidence["source"], "redis_monitor_connector_xadd_order")
        self.assertEqual(
            evidence["completed_transactions"],
            [{"session_id": "cross_tablet", "txn_id": 1}],
        )

    def test_arbitrary_bounded_cross_record_ids_remain_diagnostic_only(self) -> None:
        capture = canonical_capture()
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
            collection = collections[0]
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
