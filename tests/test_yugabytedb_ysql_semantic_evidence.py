from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import struct
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "connector-contract" / "yugabytedb_ysql"
VERIFIER = ROOT / "tests" / "verify_yugabytedb_ysql_ycsb_semantic.py"
REPLAY_BIN = Path(os.environ.get("BLACKBOX_EVIDENCE_REPLAY_BIN", ""))
sys.path.insert(0, str(CONTRACT))
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "protocol" / "tools"))

from evidence_archive import archived_bundle  # noqa: E402
from tpcc_client_fixture import read_digest  # noqa: E402
from verify_evidence_index import replay_bundle as replay_acceptance_bundle  # noqa: E402


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


def acceptance_bundle(purpose: str) -> dict[str, object]:
    index = json.loads(
        (CONTRACT / "evidence" / "ycsb_acceptance_bundles.json").read_text(
            encoding="utf-8"
        )
    )
    return next(bundle for bundle in index["bundles"] if bundle["purpose"] == purpose)


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
            "--evidence-replay-bin",
            str(REPLAY_BIN),
        ],
        text=True,
        capture_output=True,
    )


class YugabyteYsqlSemanticEvidenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not REPLAY_BIN.is_file():
            raise RuntimeError("BLACKBOX_EVIDENCE_REPLAY_BIN is required")

    def test_acceptance_index_verifies_archives_from_a_clean_checkout(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                str(CONTRACT / "verify_evidence_index.py"),
                "--evidence-replay-bin",
                str(REPLAY_BIN),
            ],
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertIn("YugabyteDB YSQL evidence index verified", result.stdout)

    def test_acceptance_replay_binds_purpose_to_raw_scenario(self) -> None:
        bundle = acceptance_bundle("ordinary_two_session_workload")
        with archived_bundle(CONTRACT, bundle) as directory:
            with self.assertRaisesRegex(SystemExit, "normalization raw replay failed"):
                replay_acceptance_bundle(
                    directory,
                    "complete_value_normalization",
                    directory / "acceptance-report.json",
                    REPLAY_BIN,
                )

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
                self.assertTrue(
                    "attestation identity or digest differs" in result.stderr
                    or "captured FINAL differs from production decoder/checker replay"
                    in result.stderr
                )

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

    def test_production_replay_rejects_decoder_and_checker_mutations(self) -> None:
        def source_version(directory: Path) -> None:
            path = directory / "fractured_read_committed-cdc.json"
            entries = json.loads(path.read_text())
            for entry in entries:
                payload = json.loads(entry[1][1])
                if isinstance(payload.get("source"), dict):
                    payload["source"]["version"] = "unsupported-version"
                    entry[1][1] = json.dumps(payload, separators=(",", ":"))
                    break
            path.write_text(json.dumps(entries))

        def missing_full_image(directory: Path) -> None:
            path = directory / "fractured_read_committed-cdc.json"
            entries = json.loads(path.read_text())
            for entry in entries:
                payload = json.loads(entry[1][1])
                after = payload.get("after")
                if payload.get("source", {}).get("table") == "usertable" and isinstance(after, dict):
                    del after["field10"]
                    entry[1][1] = json.dumps(payload, separators=(",", ":"))
                    break
            path.write_text(json.dumps(entries))

        def client_read(directory: Path, mutation: str) -> None:
            path = directory / "fractured_read_committed-client.json"
            entries = json.loads(path.read_text())
            for entry in entries:
                payload = json.loads(entry[1][1])
                if payload.get("kind") != "client_txn" or payload.get("session_id") != "ycsb/worker/1":
                    continue
                if mutation == "local":
                    payload["reads"][2]["source"] = "local"
                elif mutation == "wrong_key":
                    payload["reads"][2]["key_bytes_base64"] = base64.b64encode(
                        struct.pack(">I", 9) + b"benchbase"
                        + struct.pack(">I", 9) + b"usertable"
                        + struct.pack(">i", 3)
                    ).decode()
                else:
                    payload["reads"][0]["key_bytes_base64"] = ""
                payload["read_digest"] = read_digest(payload["reads"])
                entry[1][1] = json.dumps(payload, separators=(",", ":"))
                break
            path.write_text(json.dumps(entries))

        mutations = {
            "source_version": source_version,
            "missing_full_after": missing_full_image,
            "snapshot_to_local": lambda directory: client_read(directory, "local"),
            "repeated_key_changed": lambda directory: client_read(directory, "wrong_key"),
            "empty_canonical_key": lambda directory: client_read(directory, "empty_key"),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), archived_bundle(CONTRACT, semantic_bundle()) as archived:
                with tempfile.TemporaryDirectory() as temporary:
                    directory = Path(temporary) / "evidence"
                    shutil.copytree(archived, directory)
                    mutate(directory)
                    result = run_verifier(directory, Path(temporary) / "report.json")
                    self.assertNotEqual(result.returncode, 0, name)

    def test_independent_reconciliation_rejects_lifecycle_mutations(self) -> None:
        def mutate_client(directory: Path, mutation: str) -> None:
            path = directory / "fractured_read_committed-client.json"
            entries = json.loads(path.read_text())
            payloads = [json.loads(entry[1][1]) for entry in entries]
            if mutation.startswith("bootstrap"):
                bootstrap = next(value for value in payloads if value.get("kind") == "bootstrap_manifest")
                if mutation == "bootstrap_count":
                    bootstrap["table_row_counts"][0]["row_count"] = 999
                elif mutation == "bootstrap_digest":
                    bootstrap["canonical_state_digest"] = "0" * 64
                else:
                    bootstrap["table_row_counts"].append(dict(bootstrap["table_row_counts"][0]))
            elif mutation == "client_after_done":
                client_index = next(
                    index for index, value in enumerate(payloads)
                    if value.get("kind") == "client_txn" and value.get("session_id") == "ycsb/worker/2"
                )
                done_index = next(
                    index for index, value in enumerate(payloads)
                    if value.get("kind") == "session_done" and value.get("session_id") == "ycsb/worker/2"
                )
                payloads[client_index], payloads[done_index] = payloads[done_index], payloads[client_index]
            else:
                done = next(value for value in payloads if value.get("kind") == "session_done")
                done["final_session_seq"] = 999
                done["retry_attempts"] = 998
            for entry, payload in zip(entries, payloads, strict=True):
                entry[1][1] = json.dumps(payload, separators=(",", ":"))
            path.write_text(json.dumps(entries))

        def mutate_final(directory: Path, mutation: str) -> None:
            path = directory / "fractured_read_committed-status.json"
            entries = json.loads(path.read_text())
            for entry in entries:
                payload = json.loads(entry[1][1])
                if payload.get("lifecycle_state") != "FINAL":
                    continue
                if mutation == "operational_diagnostic":
                    payload["diagnostics"].append(
                        {"category": "OPERATIONAL", "code": "RESOURCE_EXHAUSTED", "message": "mutated"}
                    )
                else:
                    payload["reconciliation"]["ambiguous_attempts"] = 1
                entry[1][1] = json.dumps(payload, separators=(",", ":"))
                break
            path.write_text(json.dumps(entries))

        mutations = {
            "bootstrap_count": lambda directory: mutate_client(directory, "bootstrap_count"),
            "bootstrap_digest": lambda directory: mutate_client(directory, "bootstrap_digest"),
            "bootstrap_duplicate": lambda directory: mutate_client(directory, "bootstrap_duplicate"),
            "client_after_done": lambda directory: mutate_client(directory, "client_after_done"),
            "session_done_ledger": lambda directory: mutate_client(directory, "session_done_ledger"),
            "operational_priority": lambda directory: mutate_final(directory, "operational_diagnostic"),
            "ambiguous_reconciliation": lambda directory: mutate_final(directory, "ambiguous"),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), archived_bundle(CONTRACT, semantic_bundle()) as archived:
                with tempfile.TemporaryDirectory() as temporary:
                    directory = Path(temporary) / "evidence"
                    shutil.copytree(archived, directory)
                    mutate(directory)
                    result = run_verifier(directory, Path(temporary) / "report.json")
                    self.assertNotEqual(result.returncode, 0, name)


if __name__ == "__main__":
    unittest.main()
