#!/usr/bin/env python3

from __future__ import annotations

import base64
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "scripts" / "blackbox_record.py"
RUN_ID = "00112233-4455-6677-8899-aabbccddeeff"


class BlackboxRecordTest(unittest.TestCase):
    def run_tool(
        self, *arguments: str, input_text: str | None = None, expected_status: int = 0
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["python3", str(TOOL), *arguments],
            input=input_text,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, expected_status, result.stderr)
        return result

    def test_run_manifest_matches_frozen_example(self) -> None:
        result = self.run_tool(
            "run-manifest",
            "--run-id",
            RUN_ID,
            "--backend-source-id",
            "mariadb-main",
            "--declared-isolation",
            "REPEATABLE_READ",
            "--session-id",
            "ycsb/worker/1",
            "--session-id",
            "ycsb/worker/2",
        )
        expected = json.loads((ROOT / "protocol/examples/run-manifest.json").read_text())
        self.assertEqual(json.loads(result.stdout), expected)
        self.assertNotIn("version", result.stdout)

    @staticmethod
    def field_token(value: str | None) -> str:
        if value is None:
            return "N"
        return "P" + base64.b64encode(value.encode("utf-8")).decode("ascii")

    def test_bootstrap_manifest_matches_frozen_mixed_digest(self) -> None:
        mixed = [None, "", "A", "é", "中", "0", "hello", "🙂", "a\x00b", "last"]
        # Input is deliberately signed-numeric order. Canonical byte order is
        # key 42 followed by key -1, as pinned by the golden vector.
        lines = [
            "\t".join(["-1", *["N"] * 10]),
            "\t".join(["42", *[self.field_token(value) for value in mixed]]),
        ]
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as scan:
            scan.write("\n".join(lines) + "\n")
            scan.flush()
            result = self.run_tool(
                "bootstrap-manifest", "--run-id", RUN_ID, "--scan-file", scan.name
            )
        expected = json.loads((ROOT / "protocol/examples/bootstrap-manifest.json").read_text())
        self.assertEqual(json.loads(result.stdout), expected)

    def test_empty_bootstrap_uses_empty_state_digest(self) -> None:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as scan:
            result = self.run_tool(
                "bootstrap-manifest", "--run-id", RUN_ID, "--scan-file", scan.name
            )
        record = json.loads(result.stdout)
        self.assertEqual(record["table_row_counts"][0]["row_count"], 0)
        self.assertEqual(
            record["canonical_state_digest"],
            "944dd6136dbad7047e31af3b6419eb6a632d4dea64bec8159e93307163810bc2",
        )

    def test_empty_bootstrap_still_validates_identifiers(self) -> None:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as scan:
            result = self.run_tool(
                "bootstrap-manifest",
                "--run-id",
                RUN_ID,
                "--schema",
                "INVALID",
                "--scan-file",
                scan.name,
                expected_status=2,
            )
        self.assertIn("canonical lowercase identifier", result.stderr)

    def test_duplicate_scan_key_is_rejected(self) -> None:
        line = "\t".join(["7", *["N"] * 10])
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as scan:
            scan.write(f"{line}\n{line}\n")
            scan.flush()
            result = self.run_tool(
                "bootstrap-manifest",
                "--run-id",
                RUN_ID,
                "--scan-file",
                scan.name,
                expected_status=2,
            )
        self.assertIn("duplicate canonical key", result.stderr)

    def test_status_validation_rejects_extra_field(self) -> None:
        status = json.loads((ROOT / "protocol/examples/monitor-status.json").read_text())
        status["unexpected_field"] = 1
        result = self.run_tool(
            "status-action",
            "--run-id",
            RUN_ID,
            "--expected-sessions",
            "2",
            "--lifecycle",
            "FINAL",
            input_text=json.dumps(status),
            expected_status=2,
        )
        self.assertIn("schema validation", result.stderr)

    def test_final_pass_requires_reconciled_evidence(self) -> None:
        status = json.loads((ROOT / "protocol/examples/monitor-status.json").read_text())
        status["reconciliation"]["evidence_reconciled"] = False
        status["reconciliation"]["end_fence_seen"] = False
        result = self.run_tool(
            "status-action",
            "--run-id",
            RUN_ID,
            "--expected-sessions",
            "2",
            "--lifecycle",
            "FINAL",
            input_text=json.dumps(status),
            expected_status=2,
        )
        self.assertIn("unreconciled evidence", result.stderr)

    def test_final_pass_requires_coordinator_session_count(self) -> None:
        status = (ROOT / "protocol/examples/monitor-status.json").read_text()
        result = self.run_tool(
            "status-action",
            "--run-id",
            RUN_ID,
            "--expected-sessions",
            "3",
            "--lifecycle",
            "FINAL",
            input_text=status,
            expected_status=2,
        )
        self.assertIn("coordinator roster", result.stderr)

    def test_final_pass_requires_assembled_cdc_algebra(self) -> None:
        status = json.loads((ROOT / "protocol/examples/monitor-status.json").read_text())
        status["assembled_cdc_transactions"] = 0
        result = self.run_tool(
            "status-action",
            "--run-id",
            RUN_ID,
            "--expected-sessions",
            "2",
            "--lifecycle",
            "FINAL",
            input_text=json.dumps(status),
            expected_status=2,
        )
        self.assertIn("assembled CDC counter", result.stderr)

    def test_status_lifecycle_regression_is_rejected(self) -> None:
        status = json.loads((ROOT / "protocol/examples/monitor-status.json").read_text())
        status["lifecycle_state"] = "STARTING"
        status["outcome"] = "RUNNING"
        status["checker_observation"] = "NOT_STARTED"
        del status["reconciliation"]
        result = self.run_tool(
            "status-action",
            "--run-id",
            RUN_ID,
            "--expected-sessions",
            "2",
            "--previous-lifecycle",
            "LOAD_FINISHED_SEEN",
            "--lifecycle",
            "FINAL",
            input_text=json.dumps(status),
            expected_status=2,
        )
        self.assertIn("regressed", result.stderr)

    def test_consistency_outcome_is_not_a_run_failure_code(self) -> None:
        result = self.run_tool(
            "run-failed",
            "--run-id",
            RUN_ID,
            "--phase",
            "COORDINATOR",
            "--reason-code",
            "PASS",
            "--diagnostic",
            "not an error code",
            expected_status=2,
        )
        self.assertIn("not in result_error_codes.md", result.stderr)


if __name__ == "__main__":
    unittest.main()
