from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "connector-contract" / "yugabytedb_ysql"
VERIFIER = ROOT / "tests" / "verify_yugabytedb_ysql_ycsb_semantic.py"
sys.path.insert(0, str(CONTRACT))

from evidence_archive import archived_bundle  # noqa: E402


def semantic_bundle() -> dict[str, object]:
    index = json.loads(
        (CONTRACT / "evidence" / "ycsb_acceptance_bundles.json").read_text(
            encoding="utf-8"
        )
    )
    return next(
        bundle
        for bundle in index["bundles"]
        if bundle["purpose"] == "retry_and_isolation_semantics"
    )


def run_verifier(directory: Path, output: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(VERIFIER),
            "--directory",
            str(directory),
            "--provenance",
            str(directory / "provenance.lock.json"),
            "--output",
            str(output),
        ],
        text=True,
        capture_output=True,
    )


class YugabyteYsqlSemanticEvidenceTest(unittest.TestCase):
    def test_acceptance_index_verifies_archives_from_a_clean_checkout(self) -> None:
        result = subprocess.run(
            [sys.executable, str(CONTRACT / "verify_evidence_index.py")],
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertIn("YugabyteDB YSQL evidence index verified", result.stdout)

    def test_violation_rejects_mutated_final_attestation_digest(self) -> None:
        with archived_bundle(CONTRACT, semantic_bundle()) as archived:
            with tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary) / "evidence"
                shutil.copytree(archived, directory)
                client_path = directory / "fractured_read_committed-client.json"
                entries = json.loads(client_path.read_text(encoding="utf-8"))
                mutated = False
                for entry in entries:
                    payload = json.loads(entry[1][1])
                    if (
                        payload.get("kind") == "schema_attestation"
                        and payload.get("phase") == "FINAL"
                    ):
                        payload["canonical_attestation_digest"] = "0" * 64
                        entry[1][1] = json.dumps(payload, separators=(",", ":"))
                        mutated = True
                        break
                self.assertTrue(mutated)
                client_path.write_text(json.dumps(entries), encoding="utf-8")
                result = run_verifier(directory, Path(temporary) / "report.json")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("attestation identity or digest differs", result.stderr)

    def test_dropped_client_rejects_unregistered_marker_session(self) -> None:
        with archived_bundle(CONTRACT, semantic_bundle()) as archived:
            with tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary) / "evidence"
                shutil.copytree(archived, directory)
                cdc_path = directory / "dropped_client_record-cdc.json"
                entries = json.loads(cdc_path.read_text(encoding="utf-8"))
                mutated = False
                for entry in entries:
                    key = json.loads(entry[1][0])
                    envelope = json.loads(entry[1][1])
                    source = envelope.get("source", {})
                    after = envelope.get("after")
                    if (
                        source.get("table") == "session_txn"
                        and isinstance(after, dict)
                        and after.get("session_id") == "ycsb/worker/1"
                    ):
                        after["session_id"] = "ycsb/worker/unregistered"
                        if isinstance(key, dict) and "session_id" in key:
                            key["session_id"] = "ycsb/worker/unregistered"
                        entry[1][0] = json.dumps(key, separators=(",", ":"))
                        entry[1][1] = json.dumps(envelope, separators=(",", ":"))
                        mutated = True
                        break
                self.assertTrue(mutated)
                cdc_path.write_text(json.dumps(entries), encoding="utf-8")
                result = run_verifier(directory, Path(temporary) / "report.json")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("marker correlation roster differs", result.stderr)


if __name__ == "__main__":
    unittest.main()
