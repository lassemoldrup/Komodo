#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import copy
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


MUTATOR = module(ROOT / "tests/tpcc_full_mix_mutations.py", "full_mix_mutator")
RESULT = module(
    ROOT / "tests/verify_tpcc_full_mix_mutation_result.py", "full_mix_mutation_result"
)
FIXTURE = module(ROOT / "tests/test_tpcc_full_mix_acceptance.py", "full_mix_fixture")


class FullMixMutationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.builder = FIXTURE.FullMixAcceptanceTest()
        cls.builder.setUpClass()

    def test_all_required_mutators_change_the_intended_evidence_boundary(self):
        docs = self.builder.documents()
        original_clients = docs["client"]
        original_cdc = docs["cdc"]
        dropped = MUTATOR.mutate_client(original_clients, "drop_client")
        self.assertEqual(len(dropped), len(original_clients) - 1)
        typed = MUTATOR.mutate_client(original_clients, "wrong_typed_client_value")
        typed_record = next(
            MUTATOR.payload(entry)
            for entry in typed
            if (MUTATOR.payload(entry) or {}).get("kind") == "client_txn"
            and (MUTATOR.payload(entry) or {}).get("reads")
        )
        self.assertIsInstance(typed_record["reads"][0]["value_bytes_base64"], int)
        roster = MUTATOR.mutate_client(original_clients, "table_roster_mismatch")
        manifest = next(
            MUTATOR.payload(entry)
            for entry in roster
            if (MUTATOR.payload(entry) or {}).get("kind") == "run_manifest"
        )
        self.assertEqual(len(manifest["application_table_allowlist"]), 8)
        dropped_transaction = MUTATOR.mutate_cdc(original_cdc, "drop_workload_transaction")
        self.assertLess(len(dropped_transaction), len(original_cdc))
        cdc_with_application = copy.deepcopy(original_cdc)
        start, _ = MUTATOR.workload_range(cdc_with_application, False)
        cdc_with_application.insert(
            start + 1,
            [
                "100-9",
                [
                    json.dumps({"d_w_id": 1, "d_id": 1}),
                    json.dumps(
                        {
                            "source": {"table": "district"},
                            "transaction": {"id": "fixture", "total_order": 1},
                        }
                    ),
                ],
            ],
        )
        dropped_row = MUTATOR.mutate_cdc(cdc_with_application, "drop_workload_row")
        self.assertEqual(len(dropped_row), len(cdc_with_application) - 1)

    def test_invalid_result_validator_requires_protocol_not_checker_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metrics = root / "metrics.json"
            status = root / "status.json"
            metrics.write_text(
                json.dumps(
                    {
                        "kind": "performance_measurement",
                        "run_id": FIXTURE.RUN_ID,
                        "completed": False,
                        "completion_status": "failed",
                        "evidence_reconciled": False,
                        "outcome": "INVALID",
                        "diagnostics": [
                            {"category": "PROTOCOL", "code": "RECONCILIATION_MISMATCH"}
                        ],
                    }
                ),
                encoding="utf-8",
            )
            final = {
                "kind": "status",
                "run_id": FIXTURE.RUN_ID,
                "lifecycle_state": "FINAL",
                "outcome": "INVALID",
            }
            status.write_text(
                json.dumps([["1-0", ["payload", json.dumps(final)]]]),
                encoding="utf-8",
            )
            report = RESULT.validate("drop_client", FIXTURE.RUN_ID, metrics, status)
            self.assertEqual(report["outcome"], "INVALID")
            document = json.loads(metrics.read_text())
            document["diagnostics"] = [
                {"category": "CHECKER", "code": "VIOLATION_OBSERVED"}
            ]
            metrics.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(RESULT.ResultError, "misclassified"):
                RESULT.validate("drop_client", FIXTURE.RUN_ID, metrics, status)

            document["diagnostics"] = [
                {"category": "PROTOCOL", "code": "UNEXPECTED_TABLE"}
            ]
            document.pop("outcome", None)
            metrics.write_text(json.dumps(document), encoding="utf-8")
            report = RESULT.validate(
                "table_roster_mismatch", FIXTURE.RUN_ID, metrics, status
            )
            self.assertEqual(report["outcome"], "INVALID")

    def test_runner_exposes_exact_required_case_roster_without_live_stack(self):
        runner = ROOT / "tests/run_tpcc_full_mix_mutations.sh"
        result = subprocess.run(
            [str(runner), "--print-cases"],
            text=True,
            capture_output=True,
            check=False,
            env=os.environ.copy(),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), list(MUTATOR.MUTATIONS))


if __name__ == "__main__":
    unittest.main()
