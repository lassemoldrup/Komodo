#!/usr/bin/env python3
"""Registration-validated scheduler for externally declared evaluation cells."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import statistics
import subprocess
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
NAME = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,63}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
STREAM_ID = re.compile(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)\Z")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def registry(directory: Path, field: str) -> set[str]:
    result: set[str] = set()
    for path in sorted(directory.glob("*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        name = document.get(field)
        require(isinstance(name, str) and NAME.fullmatch(name) is not None, f"invalid registration {path}")
        require(path.stem == name and name not in result, f"duplicate/misnamed registration {path}")
        result.add(name)
    return result


def stream_key(value: Any) -> tuple[int, int] | None:
    if not isinstance(value, str):
        return None
    match = STREAM_ID.fullmatch(value)
    if match is None or value == "0-0":
        return None
    result = int(match.group(1)), int(match.group(2))
    if result[0] > 0xFFFFFFFFFFFFFFFF or result[1] > 0xFFFFFFFFFFFFFFFF:
        return None
    return result


def verified_correctness(path: Path) -> None:
    require(path.is_dir(), f"correctness artifact directory is missing: {path}")
    subprocess.run(["sha256sum", "--check", "SHA256SUMS"], cwd=path, check=True, capture_output=True)
    status_paths = [
        path / name
        for name in ("status-stream.json", "status_stream.json", "redis-status.json")
        if (path / name).is_file()
    ]
    require(len(status_paths) == 1, f"correctness status artifact count differs: {path}")
    statuses = json.loads(status_paths[0].read_text(encoding="utf-8"))
    terminal = [json.loads(entry[1][1]) for entry in statuses if json.loads(entry[1][1]).get("lifecycle_state") == "FINAL"]
    require(len(terminal) == 1, f"correctness terminal status count differs: {path}")
    final = terminal[0]
    reconciliation = final.get("reconciliation", {})
    load_finished_id = stream_key(reconciliation.get("load_finished_cdc_stream_id"))
    end_fence_id = stream_key(reconciliation.get("end_fence_cdc_stream_id"))
    require(
        final.get("outcome") == "PASS"
        and reconciliation.get("evidence_reconciled") is True
        and reconciliation.get("unmatched_client_records") == 0
        and reconciliation.get("unmatched_workload_cdc_slots") == 0,
        f"correctness prerequisite is not admitted: {path}",
    )
    require(
        load_finished_id is not None
        and end_fence_id is not None
        and load_finished_id < end_fence_id,
        f"correctness prerequisite lacks ordered completed fence IDs: {path}",
    )


def load_matrix(path: Path, *, allow_existing_output: bool = False) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    require(document.get("kind") == "registered_evaluation_matrix", "matrix kind differs")
    backends = registry(ROOT / "protocol" / "backends", "backend")
    benchmarks = registry(ROOT / "protocol" / "adapters", "benchmark")
    single_repetition_pilot = document.get("single_repetition_pilot", False)
    require(
        isinstance(single_repetition_pilot, bool),
        "single_repetition_pilot is not boolean",
    )
    cells = document.get("cells")
    require(isinstance(cells, list) and cells, "matrix has no cells")
    ids: set[str] = set()
    for cell in cells:
        require(isinstance(cell, dict), "matrix cell is not an object")
        for field in ("id", "backend", "benchmark", "mode", "workload", "capture_profile"):
            value = cell.get(field)
            require(isinstance(value, str) and NAME.fullmatch(value) is not None, f"cell {field} is invalid")
        require(cell["id"] not in ids, "matrix cell ID is duplicated")
        ids.add(cell["id"])
        require(cell["backend"] in backends, f"unregistered backend: {cell['backend']}")
        require(cell["benchmark"] in benchmarks, f"unregistered benchmark: {cell['benchmark']}")
        minimum_repetitions = 1 if single_repetition_pilot else 3
        require(
            type(cell.get("repetitions")) is int
            and cell["repetitions"] >= minimum_repetitions,
            "pilot cell needs at least one repetition"
            if single_repetition_pilot
            else "formal cell needs at least three repetitions",
        )
        for field in ("archive", "config", "expected_sessions", "database_container", "connector_container"):
            require(isinstance(cell.get(field), str) and cell[field], f"cell {field} is missing")
        require(isinstance(cell.get("archive_sha256"), str) and SHA256.fullmatch(cell["archive_sha256"]), "archive checksum is invalid")
        runner = cell.get("runner", "run_registered_performance_cell.sh")
        require(
            isinstance(runner, str)
            and re.fullmatch(r"[a-z][a-z0-9_]*[.]sh", runner) is not None,
            "cell runner is invalid",
        )
        arguments = cell.get("runner_arguments", [])
        require(
            isinstance(arguments, list)
            and all(isinstance(value, str) and value for value in arguments),
            "cell runner arguments are invalid",
        )
        parameters = cell.get("parameters", {})
        require(
            isinstance(parameters, dict)
            and all(
                isinstance(name, str)
                and re.fullmatch(r"[A-Z][A-Z0-9_]{0,95}", name) is not None
                and isinstance(value, (str, int, float))
                and not isinstance(value, bool)
                for name, value in parameters.items()
            ),
            "cell parameters are invalid",
        )
        metrics = cell.get(
            "summary_metrics",
            [
                "client_logical_transactions_per_second",
                "client_successful_transactions_per_second",
                "client_average_latency_microseconds",
                "client_p95_latency_microseconds",
            ],
        )
        require(
            isinstance(metrics, list)
            and metrics
            and all(isinstance(value, str) and NAME.fullmatch(value) is not None for value in metrics),
            "cell summary metrics are invalid",
        )
        verified_correctness(Path(cell["correctness_evidence"]))
    comparisons = document.get("comparisons", [])
    require(isinstance(comparisons, list), "matrix comparisons are not an array")
    for comparison in comparisons:
        require(isinstance(comparison, dict), "matrix comparison is not an object")
        require(
            set(comparison) == {"id", "baseline_cell", "measured_cell", "metric"}
            and all(
                isinstance(comparison[field], str)
                and NAME.fullmatch(comparison[field]) is not None
                for field in comparison
            ),
            "matrix comparison is invalid",
        )
        require(
            comparison["baseline_cell"] in ids
            and comparison["measured_cell"] in ids,
            "matrix comparison references an unknown cell",
        )
    output = Path(document["output_root"])
    require(
        output.is_absolute() and (allow_existing_output or not output.exists()),
        "matrix output root must be a new absolute path",
    )
    return document


def command(cell: dict[str, Any], output: Path, repetition: int = 1) -> list[str]:
    runner = cell.get("runner", "run_registered_performance_cell.sh")
    result = [
        str(ROOT / "scripts" / runner),
        "--backend", cell["backend"],
        "--benchmark", cell["benchmark"],
        "--mode", cell["mode"],
        "--workload", cell["workload"],
        "--capture-profile", cell["capture_profile"],
        "--archive", cell["archive"],
        "--archive-sha256", cell["archive_sha256"],
        "--config", cell["config"],
        "--expected-sessions", cell["expected_sessions"],
        "--output", str(output),
        "--monitor-bin", cell["monitor_bin"],
        "--database-container", cell["database_container"],
        "--connector-container", cell["connector_container"],
        "--loader-timeout-seconds", str(cell.get("loader_timeout_seconds", 600)),
        "--stack-timeout-seconds", str(cell.get("stack_timeout_seconds", 300)),
        "--status-timeout-seconds", str(cell.get("status_timeout_seconds", 300)),
        "--repetition", str(repetition),
    ]
    for name, value in sorted(cell.get("parameters", {}).items()):
        result.extend(("--parameter", f"{name}={value}"))
    result.extend(cell.get("runner_arguments", []))
    return result


def verify_existing_repetition(
    cell: dict[str, Any], output: Path, repetition: int
) -> None:
    subprocess.run(
        ["sha256sum", "--check", "SHA256SUMS"],
        cwd=output,
        check=True,
        capture_output=True,
    )
    existing = json.loads((output / "result.json").read_text(encoding="utf-8"))
    require(existing.get("accepted") is True, f"existing repetition is not accepted: {output}")
    for field in ("backend", "benchmark", "mode", "workload", "capture_profile"):
        require(
            existing.get(field) == cell[field],
            f"existing repetition {field} differs: {output}",
        )
    require(
        existing.get("repetition") == repetition,
        f"existing repetition number differs: {output}",
    )

    parameter_path = output / "workload-parameters.txt"
    require(parameter_path.is_file(), f"existing workload parameters are missing: {output}")
    actual_parameters = parameter_path.read_text(encoding="utf-8").splitlines()
    expected_parameters = [
        f"{name}={value}" for name, value in sorted(cell.get("parameters", {}).items())
    ]
    require(
        actual_parameters == expected_parameters,
        f"existing workload parameters differ: {output}",
    )


def summarize(matrix: dict[str, Any], matrix_path: Path) -> None:
    root = Path(matrix["output_root"])
    rows: list[dict[str, Any]] = []
    summary: dict[str, Any] = {
        "kind": "registered_evaluation_matrix_result",
        "matrix_sha256": hashlib.sha256(matrix_path.read_bytes()).hexdigest(),
        "cells": {},
        "comparisons": {},
    }
    results_by_cell: dict[str, list[dict[str, Any]]] = {}
    for cell in matrix["cells"]:
        results = []
        for repetition in range(1, cell["repetitions"] + 1):
            path = root / cell["id"] / f"repetition-{repetition:02d}"
            subprocess.run(["sha256sum", "--check", "SHA256SUMS"], cwd=path, check=True, capture_output=True)
            result = json.loads((path / "result.json").read_text(encoding="utf-8"))
            require(result.get("accepted") is True, f"repetition is not accepted: {path}")
            result["repetition"] = repetition
            result["artifact_dir"] = str(path)
            results.append(result)
            rows.append(result)
        metrics = {}
        for field in cell.get(
            "summary_metrics",
            [
                "client_logical_transactions_per_second",
                "client_successful_transactions_per_second",
                "client_average_latency_microseconds",
                "client_p95_latency_microseconds",
            ],
        ):
            values = [float(result[field]) for result in results]
            metrics[field] = {"per_repetition": values, "mean": statistics.fmean(values)}
        summary["cells"][cell["id"]] = {
            "accepted_repetitions": len(results),
            "rejected_repetitions": 0,
            "metrics": metrics,
            "artifact_dirs": [result["artifact_dir"] for result in results],
        }
        results_by_cell[cell["id"]] = results
    for comparison in matrix.get("comparisons", []):
        baseline = results_by_cell[comparison["baseline_cell"]]
        measured = results_by_cell[comparison["measured_cell"]]
        require(len(baseline) == len(measured), "comparison repetition counts differ")
        overheads = []
        for baseline_result, measured_result in zip(baseline, measured, strict=True):
            require(
                baseline_result["backend"] == measured_result["backend"]
                and baseline_result["benchmark"] == measured_result["benchmark"]
                and baseline_result["workload"] == measured_result["workload"],
                "comparison cell identities differ",
            )
            require(
                baseline_result.get("bootstrap_canonical_state_digest")
                == measured_result.get("bootstrap_canonical_state_digest"),
                "comparison loader states differ",
            )
            baseline_value = float(baseline_result[comparison["metric"]])
            measured_value = float(measured_result[comparison["metric"]])
            require(baseline_value > 0, "comparison baseline metric is not positive")
            overheads.append((baseline_value - measured_value) * 100.0 / baseline_value)
        summary["comparisons"][comparison["id"]] = {
            "baseline_cell": comparison["baseline_cell"],
            "measured_cell": comparison["measured_cell"],
            "metric": comparison["metric"],
            "overhead_percent_per_repetition": overheads,
            "mean_overhead_percent": statistics.fmean(overheads),
        }
    (root / "matrix.json").write_bytes(matrix_path.read_bytes())
    (root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    identity_fields = [
        "backend", "benchmark", "mode", "execution_profile", "workload",
        "capture_profile", "repetition", "outcome", "accepted",
    ]
    fields = identity_fields + sorted(
        {key for row in rows for key in row if key not in identity_fields and key != "artifact_dir"}
    ) + ["artifact_dir"]
    csv_rows = [
        {
            key: (
                json.dumps(value, sort_keys=True, separators=(",", ":"))
                if isinstance(value, (dict, list))
                else value
            )
            for key, value in row.items()
        }
        for row in rows
    ]
    with (root / "raw-results.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(csv_rows)

    concise_fields = [
        "backend",
        "benchmark",
        "mode",
        "client_p95_latency_microseconds",
        "e2e_time_seconds",
        "client_throughput_transactions_per_second",
        "monitor_throughput_transactions_per_second",
        "e2e_throughput_transactions_per_second",
    ]
    concise_rows: list[dict[str, Any]] = []
    for cell in matrix["cells"]:
        results = results_by_cell[cell["id"]]

        def optional_mean(field: str) -> float | str:
            values = [float(result[field]) for result in results if result.get(field) is not None]
            return statistics.fmean(values) if values else ""

        concise_rows.append(
            {
                "backend": cell["backend"],
                "benchmark": cell["benchmark"],
                "mode": cell["mode"],
                "client_p95_latency_microseconds": optional_mean(
                    "client_p95_latency_microseconds"
                ),
                "e2e_time_seconds": optional_mean("e2e_end_to_verdict_seconds"),
                "client_throughput_transactions_per_second": optional_mean(
                    "client_logical_transactions_per_second"
                ),
                "monitor_throughput_transactions_per_second": optional_mean(
                    "monitor_checker_transactions_per_component_second"
                ),
                "e2e_throughput_transactions_per_second": optional_mean(
                    "e2e_transactions_per_second"
                ),
            }
        )
    with (root / "summary.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=concise_fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(concise_rows)

    def display(value: Any) -> str:
        return "—" if value == "" else f"{float(value):.2f}"

    markdown = [
        "# Performance summary",
        "",
        "All values are arithmetic means across accepted repetitions.",
        "",
        "| Backend | Benchmark | Mode | Client p95 (µs) | E2E time (s) | Client throughput (txn/s) | Monitor throughput (txn/s) | E2E throughput (txn/s) |",
        "|---|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in concise_rows:
        markdown.append(
            "| {backend} | {benchmark} | {mode} | {p95} | {e2e_time} | {client} | {monitor} | {e2e} |".format(
                backend=row["backend"],
                benchmark=row["benchmark"],
                mode=row["mode"],
                p95=display(row["client_p95_latency_microseconds"]),
                e2e_time=display(row["e2e_time_seconds"]),
                client=display(row["client_throughput_transactions_per_second"]),
                monitor=display(row["monitor_throughput_transactions_per_second"]),
                e2e=display(row["e2e_throughput_transactions_per_second"]),
            )
        )
    (root / "summary.md").write_text("\n".join(markdown) + "\n", encoding="utf-8")
    subprocess.run(
        ["bash", "-lc", "find . -type f ! -name SHA256SUMS -print0 | LC_ALL=C sort -z | xargs -0 sha256sum >SHA256SUMS && sha256sum --check SHA256SUMS >/dev/null"],
        cwd=root,
        check=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("matrices", nargs="+", type=Path)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    matrices = [
        (path, load_matrix(path, allow_existing_output=args.resume))
        for path in args.matrices
    ]
    output_roots = [Path(matrix["output_root"]) for _, matrix in matrices]
    require(len(set(output_roots)) == len(output_roots), "matrix output roots are duplicated")

    scheduled: list[tuple[Path, dict[str, Any], list[list[str]]]] = []
    all_jobs: list[list[str]] = []
    for matrix_path, matrix in matrices:
        root = Path(matrix["output_root"])
        jobs = []
        maximum_repetitions = max(cell["repetitions"] for cell in matrix["cells"])
        for repetition in range(1, maximum_repetitions + 1):
            for cell in matrix["cells"]:
                if repetition > cell["repetitions"]:
                    continue
                output = root / cell["id"] / f"repetition-{repetition:02d}"
                if (
                    args.resume
                    and (output / "result.json").is_file()
                    and (output / "SHA256SUMS").is_file()
                ):
                    verify_existing_repetition(cell, output, repetition)
                    continue
                jobs.append(command(cell, output, repetition))
        scheduled.append((matrix_path, matrix, jobs))
        all_jobs.extend(jobs)
    if args.verify_only:
        print(json.dumps({"jobs": all_jobs}, indent=2))
        return 0

    for matrix_path, matrix, jobs in scheduled:
        root = Path(matrix["output_root"])
        root.mkdir(parents=True, exist_ok=args.resume)
        for job in jobs:
            subprocess.run(job, cwd=ROOT, check=True)
        summarize(matrix, matrix_path)

    for matrix_path, matrix, _ in scheduled:
        root = Path(matrix["output_root"])
        print(f"[registered-evaluation] matrix={matrix_path.resolve()}")
        print(f"[registered-evaluation] raw_results_csv={root / 'raw-results.csv'}")
        print(f"[registered-evaluation] summary_csv={root / 'summary.csv'}")
        print(f"[registered-evaluation] summary_markdown={root / 'summary.md'}")
        print(f"[registered-evaluation] summary_json={root / 'summary.json'}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"registered evaluation matrix failed: {error}")
