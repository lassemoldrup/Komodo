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
    def test_command_forwards_explicit_history_gc_setting(self) -> None:
        module = load_module()
        cell = {field: "value" for field in (
            "backend", "benchmark", "mode", "workload", "capture_profile",
            "archive", "archive_sha256", "config", "expected_sessions",
            "monitor_bin", "database_container", "connector_container")}
        for enabled in (False, True):
            cell["history_gc"] = enabled
            command = module.command(cell, Path("/tmp/output"))
            self.assertEqual(command[-2:], ["--history-gc", "on" if enabled else "off"])
            cell["workload_rss_sampling"] = enabled
            command = module.command(cell, Path("/tmp/output"))
            self.assertEqual(command[-2:], ["--workload-rss-sampling", "on" if enabled else "off"])
            del cell["workload_rss_sampling"]

    def test_optional_alternating_order_preserves_default(self) -> None:
        module = load_module()
        matrix = {"cells": [{"id": "off"}, {"id": "on"}]}
        self.assertEqual(module.cells_for_repetition(matrix, 2), matrix["cells"])
        matrix["alternate_cell_order"] = True
        self.assertEqual(module.cells_for_repetition(matrix, 1), matrix["cells"])
        self.assertEqual(module.cells_for_repetition(matrix, 2), list(reversed(matrix["cells"])))
        self.assertEqual(module.cells_for_repetition(matrix, 3), matrix["cells"])

    def test_performance_runner_defaults_to_compact_success_evidence(self) -> None:
        source = (
            ROOT / "scripts" / "run_registered_performance_cell.sh"
        ).read_text(encoding="utf-8")
        self.assertIn("EVIDENCE_RETENTION=compact", source)
        self.assertIn("--evidence-retention", source)
        self.assertIn("capture_compact_streams", source)
        self.assertIn("capture_full_streams", source)
        self.assertIn("result_status != 0", source)
        self.assertIn('mode.text = "disabled"', source)
        self.assertIn(
            'retained = {"mode", "backend", "declaredIsolation", "benchmark"}',
            source,
        )
        self.assertNotIn("root.remove(blackbox)", source)

    def test_single_repetition_requires_explicit_pilot_declaration(self) -> None:
        module = load_module()
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertIn('document.get("single_repetition_pilot", False)', source)
        self.assertIn("formal cell needs at least three repetitions", source)
        self.assertIn("pilot cell needs at least one repetition", source)

    def test_ycsb_cells_declare_all_tunable_workload_parameters(self) -> None:
        for name in (
            "mariadb_three_benchmark_performance_matrix.json",
            "oceanbase_mysql_three_benchmark_performance_matrix.json",
            "oracle_three_benchmark_performance_matrix.json",
            "yugabytedb_three_benchmark_performance_matrix.json",
        ):
            matrix = json.loads((ROOT / "experiments" / name).read_text(encoding="utf-8"))
            ycsb_cells = [cell for cell in matrix["cells"] if cell["benchmark"] == "ycsb"]
            self.assertEqual(len(ycsb_cells), 2)
            for cell in ycsb_cells:
                parameters = cell["parameters"]
                self.assertEqual(parameters["BENCHBASE_MEASUREMENT_ZIPFIAN_THETA"], 0.99)
                self.assertEqual(
                    parameters["BENCHBASE_MEASUREMENT_OPERATIONS_PER_TRANSACTION"], 1
                )
                self.assertEqual(parameters["BENCHBASE_MEASUREMENT_TERMINALS"], 4)
                self.assertIn(
                    "client_logical_operations_per_second", cell["summary_metrics"]
                )

    def test_resume_rejects_workload_parameter_drift(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            cell = {
                "backend": "synthetic_backend",
                "benchmark": "synthetic_benchmark",
                "mode": "synthetic_mode",
                "workload": "synthetic_workload",
                "capture_profile": "synthetic_profile",
                "parameters": {"BENCHBASE_MEASUREMENT_TERMINALS": 4},
            }
            result = {
                **{field: cell[field] for field in (
                    "backend", "benchmark", "mode", "workload", "capture_profile"
                )},
                "repetition": 1,
                "accepted": True,
            }
            (output / "result.json").write_text(json.dumps(result), encoding="utf-8")
            (output / "workload-parameters.txt").write_text(
                "BENCHBASE_MEASUREMENT_TERMINALS=2\n", encoding="utf-8"
            )
            with (output / "SHA256SUMS").open("w", encoding="utf-8") as checksums:
                for name in ("result.json", "workload-parameters.txt"):
                    subprocess.run(
                        ["sha256sum", name],
                        cwd=output,
                        check=True,
                        text=True,
                        stdout=checksums,
                    )

            with self.assertRaisesRegex(ValueError, "workload parameters differ"):
                module.verify_existing_repetition(cell, output, 1)

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
                                    "load_finished_cdc_stream_id": "1-0",
                                    "end_fence_cdc_stream_id": "2-0",
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
                                    "load_finished_cdc_stream_id": "1-0",
                                    "end_fence_cdc_stream_id": "2-0",
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
        runner = (
            ROOT / "scripts/run_registered_performance_cell.sh"
        ).read_text(encoding="utf-8")
        before_loader, loader_marker, remainder = runner.partition('LOADER_COMMAND="cd ')
        loader, workload_marker, after_loader = remainder.partition('WORKLOAD_COMMAND="cd ')
        self.assertTrue(loader_marker)
        self.assertTrue(workload_marker)
        # This loader-only JDBC optimization is covered by test_oceanbase_loader.
        # Exempt its one guard, not the entire block or other backend decisions.
        loader_guard = "if [[ $BACKEND == oceanbase_mysql ]]; then\n"
        self.assertEqual(loader.count(loader_guard), 1)
        runner = (before_loader + loader_marker + loader.replace(loader_guard, "", 1)
                  + workload_marker + after_loader)
        text = SCRIPT.read_text(encoding="utf-8") + runner + (
            ROOT / "scripts/registered_performance_result.py"
        ).read_text(encoding="utf-8")
        for forbidden in (
            "mariadb",
            "yugabytedb_ysql",
            "oracle",
            "oceanbase_mysql",
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
                    if cell_id == "baseline":
                        p95 = (100, 100, 100)[repetition - 1]
                        client_throughput = (200, 200, 200)[repetition - 1]
                    else:
                        p95 = (90, 100, 130)[repetition - 1]
                        client_throughput = (180, 200, 260)[repetition - 1]
                    result = {
                        "backend": "synthetic_backend",
                        "benchmark": "synthetic_benchmark",
                        "mode": mode,
                        "execution_profile": execution_profile,
                        "workload": "synthetic_workload",
                        "capture_profile": "synthetic_profile",
                        "outcome": "PASS",
                        "accepted": True,
                        "client_p95_latency_microseconds": p95,
                        "client_logical_transactions_per_second": client_throughput,
                    }
                    if execution_profile == "coordinated_e2e":
                        result.update(
                            {
                                "e2e_end_to_verdict_seconds": (10, 10, 20)[
                                    repetition - 1
                                ],
                                "monitor_checker_transactions_per_component_second": (
                                    300,
                                    300,
                                    330,
                                )[repetition - 1],
                                "e2e_transactions_per_second": (150, 150, 180)[
                                    repetition - 1
                                ],
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
                "comparisons": [
                    {
                        "id": "measured_over_baseline",
                        "baseline_cell": "baseline",
                        "measured_cell": "measured",
                        "metric": "client_p95_latency_microseconds",
                    }
                ],
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
            self.assertEqual(rows[0]["client_p95_latency_microseconds"], "100.0")
            self.assertAlmostEqual(float(rows[1]["e2e_time_seconds"]), 40 / 3)
            self.assertEqual(
                rows[1]["monitor_throughput_transactions_per_second"], "310.0"
            )
            markdown = (root / "summary.md").read_text(encoding="utf-8")
            self.assertIn("arithmetic means", markdown)
            self.assertIn("| 100.00 | — | 200.00 | — | — |", markdown)
            summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
            baseline_p95 = summary["cells"]["baseline"]["metrics"][
                "client_p95_latency_microseconds"
            ]
            self.assertEqual(baseline_p95["mean"], 100.0)
            self.assertNotIn("median", baseline_p95)
            comparison = summary["comparisons"]["measured_over_baseline"]
            self.assertAlmostEqual(comparison["mean_overhead_percent"], -20 / 3)
            self.assertNotIn("median_overhead_percent", comparison)


if __name__ == "__main__":
    unittest.main()
