from __future__ import annotations

import csv
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_registered_evaluation_matrix.py"


def load_module():
    spec = importlib.util.spec_from_file_location("registered_matrix", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RegisteredEvaluationMatrixTest(unittest.TestCase):
    def test_cli_accepts_multiple_external_matrices(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            correctness = root / "correctness"
            correctness.mkdir()
            status = [
                [
                    "1-0",
                    [
                        "payload",
                        json.dumps(
                            {
                                "lifecycle_state": "FINAL",
                                "outcome": "PASS",
                                "reconciliation": {
                                    "evidence_reconciled": True,
                                    "unmatched_client_records": 0,
                                    "unmatched_workload_cdc_slots": 0,
                                },
                            }
                        ),
                    ],
                ]
            ]
            (correctness / "status-stream.json").write_text(
                json.dumps(status), encoding="utf-8"
            )
            with (correctness / "SHA256SUMS").open(
                "w", encoding="utf-8"
            ) as checksums:
                subprocess.run(
                    ["sha256sum", "status-stream.json"],
                    cwd=correctness,
                    check=True,
                    text=True,
                    stdout=checksums,
                )

            matrix_paths = []
            for index in (1, 2):
                matrix_path = root / f"matrix-{index}.json"
                matrix_path.write_text(
                    json.dumps(
                        {
                            "kind": "registered_evaluation_matrix",
                            "output_root": str(root / f"output-{index}"),
                            "cells": [
                                {
                                    "id": f"cell_{index}",
                                    "backend": "mariadb",
                                    "benchmark": "ycsb",
                                    "mode": "synthetic_mode",
                                    "workload": "synthetic_workload",
                                    "capture_profile": "synthetic_profile",
                                    "repetitions": 3,
                                    "archive": "/opaque/archive.tgz",
                                    "archive_sha256": "0" * 64,
                                    "config": "opaque/config.xml",
                                    "expected_sessions": "session/1",
                                    "monitor_bin": "/opaque/monitor",
                                    "database_container": "opaque-database",
                                    "connector_container": "opaque-connector",
                                    "correctness_evidence": str(correctness),
                                }
                            ],
                        }
                    ),
                    encoding="utf-8",
                )
                matrix_paths.append(matrix_path)

            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    *(str(path) for path in matrix_paths),
                    "--verify-only",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            jobs = json.loads(completed.stdout)["jobs"]
            self.assertEqual(len(jobs), 6)
            self.assertTrue(any(str(root / "output-1") in part for part in jobs[0]))
            self.assertTrue(any(str(root / "output-2") in part for part in jobs[3]))

    def test_synthetic_registrations_schedule_without_generic_edit(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "protocol/backends").mkdir(parents=True)
            (root / "protocol/adapters").mkdir(parents=True)
            (root / "scripts").mkdir()
            (root / "protocol/backends/synthetic_backend.json").write_text(
                json.dumps({"backend": "synthetic_backend"}), encoding="utf-8"
            )
            (root / "protocol/adapters/synthetic_benchmark.json").write_text(
                json.dumps({"benchmark": "synthetic_benchmark"}), encoding="utf-8"
            )
            correctness = root / "correctness"
            correctness.mkdir()
            status = [
                [
                    "1-0",
                    [
                        "payload",
                        json.dumps(
                            {
                                "lifecycle_state": "FINAL",
                                "outcome": "PASS",
                                "reconciliation": {
                                    "evidence_reconciled": True,
                                    "unmatched_client_records": 0,
                                    "unmatched_workload_cdc_slots": 0,
                                },
                            }
                        ),
                    ],
                ]
            ]
            (correctness / "status-stream.json").write_text(
                json.dumps(status), encoding="utf-8"
            )
            with (correctness / "SHA256SUMS").open("w", encoding="utf-8") as checksums:
                subprocess.run(
                    ["sha256sum", "status-stream.json"],
                    cwd=correctness,
                    check=True,
                    text=True,
                    stdout=checksums,
                )
            archive = root / "artifact.tgz"
            archive.write_bytes(b"synthetic")
            digest = __import__("hashlib").sha256(archive.read_bytes()).hexdigest()
            matrix_path = root / "matrix.json"
            matrix_path.write_text(
                json.dumps(
                    {
                        "kind": "registered_evaluation_matrix",
                        "output_root": str(root / "output"),
                        "cells": [
                            {
                                "id": "synthetic_cell",
                                "backend": "synthetic_backend",
                                "benchmark": "synthetic_benchmark",
                                "mode": "synthetic_mode",
                                "workload": "synthetic_workload",
                                "capture_profile": "synthetic_profile",
                                "repetitions": 3,
                                "archive": str(archive),
                                "archive_sha256": digest,
                                "config": "opaque/config.xml",
                                "expected_sessions": "session/1",
                                "monitor_bin": "/opaque/monitor",
                                "database_container": "opaque-database",
                                "connector_container": "opaque-connector",
                                "correctness_evidence": str(correctness),
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            module.ROOT = root
            matrix = module.load_matrix(matrix_path)
            command = module.command(matrix["cells"][0], root / "trial")
            self.assertIn("synthetic_backend", command)
            self.assertIn("synthetic_benchmark", command)

    def test_generic_scheduler_and_runner_have_no_product_name_decisions(self) -> None:
        text = SCRIPT.read_text(encoding="utf-8") + (
            ROOT / "scripts/run_registered_performance_cell.sh"
        ).read_text(encoding="utf-8") + (
            ROOT / "scripts/registered_performance_result.py"
        ).read_text(encoding="utf-8")
        for forbidden in (
            "mariadb",
            "yugabytedb_ysql",
            "oracle",
            "smallbank",
            "Amalgamate",
            "SendPayment",
        ):
            self.assertNotIn(forbidden, text)

    def test_summary_reports_only_requested_headline_metrics(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cells = []
            for cell_id, mode, execution_profile in (
                ("baseline", "benchbase_uninstrumented", "direct_benchbase"),
                ("measured", "full_e2e", "coordinated_e2e"),
            ):
                cells.append(
                    {
                        "id": cell_id,
                        "backend": "synthetic_backend",
                        "benchmark": "synthetic_benchmark",
                        "mode": mode,
                        "repetitions": 3,
                        "summary_metrics": ["client_p95_latency_microseconds"],
                    }
                )
                for repetition in range(1, 4):
                    trial = root / cell_id / f"repetition-{repetition:02d}"
                    trial.mkdir(parents=True)
                    result = {
                        "backend": "synthetic_backend",
                        "benchmark": "synthetic_benchmark",
                        "mode": mode,
                        "execution_profile": execution_profile,
                        "workload": "synthetic_workload",
                        "capture_profile": "synthetic_profile",
                        "outcome": "PASS",
                        "accepted": True,
                        "client_p95_latency_microseconds": 100 + repetition,
                        "client_logical_transactions_per_second": 200 + repetition,
                    }
                    if execution_profile == "coordinated_e2e":
                        result.update(
                            {
                                "e2e_end_to_verdict_seconds": 10 + repetition,
                                "monitor_checker_transactions_per_component_second": 300
                                + repetition,
                                "e2e_transactions_per_second": 150 + repetition,
                            }
                        )
                    (trial / "result.json").write_text(
                        json.dumps(result), encoding="utf-8"
                    )
                    with (trial / "SHA256SUMS").open(
                        "w", encoding="utf-8"
                    ) as checksums:
                        subprocess.run(
                            ["sha256sum", "result.json"],
                            cwd=trial,
                            check=True,
                            text=True,
                            stdout=checksums,
                        )

            matrix = {
                "kind": "registered_evaluation_matrix",
                "output_root": str(root),
                "cells": cells,
            }
            matrix_path = root / "source-matrix.json"
            matrix_path.write_text(json.dumps(matrix), encoding="utf-8")
            module.summarize(matrix, matrix_path)

            with (root / "summary.csv").open(newline="", encoding="utf-8") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(
                list(rows[0]),
                [
                    "backend",
                    "benchmark",
                    "mode",
                    "client_p95_latency_microseconds",
                    "e2e_time_seconds",
                    "client_throughput_transactions_per_second",
                    "monitor_throughput_transactions_per_second",
                    "e2e_throughput_transactions_per_second",
                ],
            )
            self.assertEqual(rows[0]["e2e_time_seconds"], "")
            self.assertEqual(rows[0]["client_p95_latency_microseconds"], "102.0")
            self.assertEqual(rows[1]["e2e_time_seconds"], "12.0")
            self.assertEqual(
                rows[1]["monitor_throughput_transactions_per_second"], "302.0"
            )
            self.assertIn("| 102.00 | — | 202.00 | — | — |", (root / "summary.md").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
