#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import importlib.util
import json
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROTOCOL_ROOT = ROOT / "protocol"
TOOL_PATH = PROTOCOL_ROOT / "tools" / "tpcc_codec.py"
SPEC = importlib.util.spec_from_file_location("tpcc_codec", TOOL_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover - import setup invariant
    raise RuntimeError(f"cannot load {TOOL_PATH}")
CODEC_MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CODEC_MODULE)

RECORD_TOOL = ROOT / "scripts" / "blackbox_record.py"
RUN_ID = "00112233-4455-6677-8899-aabbccddeeff"
BOOTSTRAP_DOMAIN = b"db-monitoring/bootstrap-state-digest\x00"


class TpccCodecTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.codec = CODEC_MODULE.TpccCodec.from_protocol_root(PROTOCOL_ROOT)
        cls.golden = CODEC_MODULE.load_json(PROTOCOL_ROOT / "golden" / "tpcc_rows.json")
        cls.valid_by_name = {vector["name"]: vector for vector in cls.golden["vectors"]}

    def run_record_tool(
        self, *arguments: str, expected_status: int = 0
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["python3", str(RECORD_TOOL), *arguments],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, expected_status, result.stderr)
        return result

    def test_all_executable_valid_vectors_match_expected_bytes(self) -> None:
        for vector in self.golden["vectors"]:
            with self.subTest(vector=vector["name"]):
                key, value = self.codec.encode_row(vector["table"], vector["row"])
                self.assertEqual(key.hex(), vector["expected_key_hex"])
                self.assertEqual(value.hex(), vector["expected_value_hex"])

    def test_all_executable_invalid_vectors_fail_with_expected_category(self) -> None:
        for vector in self.golden["invalid_vectors"]:
            with self.subTest(vector=vector["name"]):
                row = CODEC_MODULE.materialize_invalid_vector(vector, self.valid_by_name)
                with self.assertRaises(CODEC_MODULE.CodecError) as caught:
                    self.codec.encode_row(vector["table"], row)
                self.assertEqual(caught.exception.code, vector["expected_error"])

    def test_live_mariadb_debezium_rows_match_expected_bytes(self) -> None:
        fixture = CODEC_MODULE.load_json(
            PROTOCOL_ROOT / "fixtures" / "mariadb_tpcc_typed_rows.json"
        )
        self.assertEqual(len(fixture["rows"]), 11)
        self.assertEqual({row["table"] for row in fixture["rows"]}, set(self.codec.tables))
        for row in fixture["rows"]:
            with self.subTest(stream_id=row["stream_id"]):
                key, value = self.codec.encode_row(row["table"], row["after"])
                self.assertEqual(key.hex(), row["expected_key_hex"])
                self.assertEqual(value.hex(), row["expected_value_hex"])

    def test_live_fixture_rows_build_one_global_bootstrap_manifest(self) -> None:
        fixture = CODEC_MODULE.load_json(
            PROTOCOL_ROOT / "fixtures" / "mariadb_tpcc_typed_rows.json"
        )
        records = [
            {"table": row["table"], "row": row["after"]} for row in fixture["rows"]
        ]
        encoded = [self.codec.encode_row(record["table"], record["row"]) for record in records]
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as scan:
            for record in records:
                scan.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
            scan.flush()
            result = self.run_record_tool(
                "bootstrap-manifest",
                "--run-id",
                RUN_ID,
                "--benchmark",
                "tpcc",
                "--scan-file",
                scan.name,
            )
        manifest = json.loads(result.stdout)
        self.assertEqual(sum(item["row_count"] for item in manifest["table_row_counts"]), 11)
        self.assertEqual(manifest["canonical_state_digest"], self.digest(encoded))

    def test_live_float_text_maps_to_same_raw_binary32(self) -> None:
        column = next(
            column
            for column in self.codec.table("benchbase.customer")["columns"]
            if column["name"] == "c_ytd_payment"
        )
        cases = {
            "0.33333334": "3eaaaaab",
            "0.1": "3dcccccd",
            "-1.5": "bfc00000",
            "-0": "80000000",
            "1.40129846e-45": "00000001",
            "-1.40129846e-45": "80000001",
        }
        for numeric_text, raw_bits in cases.items():
            with self.subTest(numeric_text=numeric_text):
                expected = self.codec.encode_nonnull(
                    column, {"raw_bits_hex": raw_bits}, "customer.c_ytd_payment"
                )
                actual = self.codec.encode_nonnull(
                    column, numeric_text, "customer.c_ytd_payment"
                )
                self.assertEqual(actual, expected)
        with self.assertRaises(CODEC_MODULE.CodecError) as caught:
            self.codec.encode_nonnull(column, "3.5e38", "customer.c_ytd_payment")
        self.assertEqual(caught.exception.code, "float_non_finite")
        with self.assertRaises(CODEC_MODULE.CodecError) as caught:
            self.codec.encode_nonnull(column, "1e-1000", "customer.c_ytd_payment")
        self.assertEqual(caught.exception.code, "float_range")

    @staticmethod
    def digest(rows: list[tuple[bytes, bytes]]) -> str:
        rows = sorted(rows, key=lambda item: item[0])
        preimage = bytearray(BOOTSTRAP_DOMAIN + struct.pack(">Q", len(rows)))
        for key, value in rows:
            preimage.extend(struct.pack(">I", len(key)) + key)
            preimage.extend(struct.pack(">I", len(value)) + value)
        return hashlib.sha256(preimage).hexdigest()

    def test_multitable_bootstrap_is_globally_key_sorted_and_counted(self) -> None:
        chosen: dict[str, dict[str, object]] = {}
        for vector in self.golden["vectors"]:
            chosen.setdefault(vector["table"], vector)
        records = [
            {"table": table, "row": vector["row"]}
            for table, vector in reversed(list(chosen.items()))
        ]
        encoded = [self.codec.encode_row(record["table"], record["row"]) for record in records]
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as scan:
            for record in records:
                scan.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
            scan.flush()
            result = self.run_record_tool(
                "bootstrap-manifest",
                "--run-id",
                RUN_ID,
                "--benchmark",
                "tpcc",
                "--scan-file",
                scan.name,
            )
        manifest = json.loads(result.stdout)
        self.assertEqual(
            manifest["table_row_counts"],
            [{"table": table, "row_count": 1} for table in sorted(chosen)],
        )
        self.assertEqual(manifest["canonical_state_digest"], self.digest(encoded))

    def test_tpcc_bootstrap_queries_cover_roster_and_typed_text_forms(self) -> None:
        result = self.run_record_tool("bootstrap-queries", "--benchmark", "tpcc")
        document = json.loads(result.stdout)
        self.assertEqual(document["format"], "tpcc_jsonl")
        self.assertEqual(
            [query["table"] for query in document["queries"]],
            self.codec.contract["application_tables"],
        )
        customer_sql = next(
            query["sql"]
            for query in document["queries"]
            if query["table"] == "benchbase.customer"
        )
        self.assertIn("CAST(`c_discount` AS CHAR)", customer_sql)
        self.assertIn("CAST(`c_ytd_payment` AS CHAR)", customer_sql)
        self.assertIn("DATE_FORMAT(`c_since`, '%Y-%m-%dT%H:%i:%sZ')", customer_sql)
        self.assertIn("ORDER BY `c_w_id`, `c_d_id`, `c_id`", customer_sql)

    def test_multitable_bootstrap_rejects_duplicate_key_and_limits(self) -> None:
        vector = self.valid_by_name["new_order_empty_value"]
        record = {"table": vector["table"], "row": vector["row"]}
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as scan:
            rendered = json.dumps(record, separators=(",", ":"))
            scan.write(rendered + "\n" + rendered + "\n")
            scan.flush()
            result = self.run_record_tool(
                "bootstrap-manifest",
                "--run-id",
                RUN_ID,
                "--benchmark",
                "tpcc",
                "--scan-file",
                scan.name,
                expected_status=2,
            )
            self.assertIn("duplicate canonical key", result.stderr)

            result = self.run_record_tool(
                "bootstrap-manifest",
                "--run-id",
                RUN_ID,
                "--benchmark",
                "tpcc",
                "--scan-file",
                scan.name,
                "--max-rows",
                "1",
                expected_status=2,
            )
            self.assertIn("row limit", result.stderr)

            result = self.run_record_tool(
                "bootstrap-manifest",
                "--run-id",
                RUN_ID,
                "--benchmark",
                "tpcc",
                "--scan-file",
                scan.name,
                "--max-input-bytes",
                "1",
                expected_status=2,
            )
            self.assertIn("input byte limit", result.stderr)

            result = self.run_record_tool(
                "bootstrap-manifest",
                "--run-id",
                RUN_ID,
                "--benchmark",
                "tpcc",
                "--scan-file",
                scan.name,
                "--max-canonical-bytes",
                "1",
                expected_status=2,
            )
            self.assertIn("canonical byte limit", result.stderr)

    def test_synthetic_monitor_state_reconciles_before_load_finished(self) -> None:
        chosen: dict[str, dict[str, object]] = {}
        for vector in self.golden["vectors"]:
            chosen.setdefault(vector["table"], vector)
        source_records = [
            {"table": table, "row": vector["row"]} for table, vector in chosen.items()
        ]
        normalized_records = [
            {
                "table": table,
                "key_hex": vector["expected_key_hex"],
                "value_hex": vector["expected_value_hex"],
            }
            for table, vector in reversed(list(chosen.items()))
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_path = root / "source.jsonl"
            normalized_path = root / "normalized.jsonl"
            manifest_path = root / "manifest.json"
            source_path.write_text(
                "".join(
                    json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
                    for record in source_records
                ),
                encoding="utf-8",
            )
            normalized_path.write_text(
                "".join(
                    json.dumps(record, separators=(",", ":")) + "\n"
                    for record in normalized_records
                ),
                encoding="utf-8",
            )
            manifest = self.run_record_tool(
                "bootstrap-manifest",
                "--run-id",
                RUN_ID,
                "--benchmark",
                "tpcc",
                "--scan-file",
                str(source_path),
            ).stdout
            manifest_path.write_text(manifest, encoding="utf-8")
            result = self.run_record_tool(
                "bootstrap-reconcile",
                "--benchmark",
                "tpcc",
                "--manifest-file",
                str(manifest_path),
                "--normalized-state-file",
                str(normalized_path),
            )
            report = json.loads(result.stdout)
            self.assertTrue(report["reconciled"])
            self.assertEqual(report["run_id"], RUN_ID)

            normalized_path.write_text(
                "".join(
                    json.dumps(record, separators=(",", ":")) + "\n"
                    for record in normalized_records[:-1]
                ),
                encoding="utf-8",
            )
            result = self.run_record_tool(
                "bootstrap-reconcile",
                "--benchmark",
                "tpcc",
                "--manifest-file",
                str(manifest_path),
                "--normalized-state-file",
                str(normalized_path),
                expected_status=2,
            )
            self.assertIn("table counts differ", result.stderr)

            wrong_value = [dict(record) for record in normalized_records]
            wrong_value[0]["value_hex"] += "00"
            normalized_path.write_text(
                "".join(
                    json.dumps(record, separators=(",", ":")) + "\n"
                    for record in wrong_value
                ),
                encoding="utf-8",
            )
            result = self.run_record_tool(
                "bootstrap-reconcile",
                "--benchmark",
                "tpcc",
                "--manifest-file",
                str(manifest_path),
                "--normalized-state-file",
                str(normalized_path),
                expected_status=2,
            )
            self.assertIn("digest differs", result.stderr)


if __name__ == "__main__":
    unittest.main()
