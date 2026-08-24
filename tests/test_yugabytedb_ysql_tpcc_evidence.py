from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "connector-contract" / "yugabytedb_ysql"
sys.path.insert(0, str(CONTRACT))

from evidence_archive import archived_bundle  # noqa: E402


def load_index_verifier():
    spec = importlib.util.spec_from_file_location(
        "verify_tpcc_evidence_index", CONTRACT / "verify_tpcc_evidence_index.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def normalization_bundle() -> dict[str, object]:
    index = json.loads(
        (CONTRACT / "evidence" / "tpcc_acceptance_bundles.json").read_text(
            encoding="utf-8"
        )
    )
    return next(
        bundle
        for bundle in index["bundles"]
        if bundle["purpose"] == "tpcc_value_normalization"
    )


class YugabyteYsqlTpccEvidenceTest(unittest.TestCase):
    def test_tpcc_index_verifies_archives_from_a_clean_checkout(self) -> None:
        result = subprocess.run(
            [sys.executable, str(CONTRACT / "verify_tpcc_evidence_index.py")],
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertIn("YugabyteDB YSQL TPC-C evidence index verified", result.stdout)

    def test_mutated_jdbc_bytes_fail_the_raw_replay(self) -> None:
        verifier = load_index_verifier()
        with archived_bundle(CONTRACT, normalization_bundle()) as archived:
            with tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary) / "bundle"
                shutil.copytree(archived, directory)
                capture_path = directory / "jdbc-capture.json"
                capture = json.loads(capture_path.read_text(encoding="utf-8"))
                first = capture["tables"][0]["rows"][0]
                first["value_hex"] = "00" + first["value_hex"][2:]
                if first["value_hex"] == capture["tables"][0]["rows"][0]["value_hex"]:
                    first["value_hex"] = "ff" + first["value_hex"][2:]
                capture_path.write_text(json.dumps(capture), encoding="utf-8")
                with self.assertRaisesRegex(
                    SystemExit, "TPC-C normalization raw replay failed"
                ):
                    verifier.replay_bundle(
                        directory,
                        "tpcc_value_normalization",
                        directory / "normalization-report.json",
                    )

    def test_mutated_cdc_timestamp_text_fails_closed(self) -> None:
        verifier = load_index_verifier()
        with archived_bundle(CONTRACT, normalization_bundle()) as archived:
            with tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary) / "bundle"
                shutil.copytree(archived, directory)
                stream_path = directory / "redis-stream.json"
                text = stream_path.read_text(encoding="utf-8")
                mutated = text.replace("2000-01-01T00:00:00Z", "2000-01-01T00:00:00.000000Z")
                self.assertNotEqual(mutated, text)
                stream_path.write_text(mutated, encoding="utf-8")
                with self.assertRaisesRegex(
                    SystemExit, "TPC-C normalization raw replay failed"
                ):
                    verifier.replay_bundle(
                        directory,
                        "tpcc_value_normalization",
                        directory / "normalization-report.json",
                    )


if __name__ == "__main__":
    unittest.main()
