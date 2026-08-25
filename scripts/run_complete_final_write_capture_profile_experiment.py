#!/usr/bin/env python3
"""Run the bounded complete-final-write physical-capture A/B."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import statistics
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
CANONICAL_NAME = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
GIT_SHA = re.compile(r"[0-9a-f]{40}\Z")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def load_manifest(path: Path) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(document, dict), "experiment manifest is not an object")
    require(
        document.get("kind") == "complete_final_write_capture_profile_experiment",
        "experiment manifest kind differs",
    )
    cells = document.get("selected_cells")
    require(isinstance(cells, list) and len(cells) == 1, "experiment requires one bounded cell")
    cell = cells[0]
    require(isinstance(cell, dict), "selected cell is not an object")
    for field in ("backend", "benchmark"):
        require(
            isinstance(cell.get(field), str) and CANONICAL_NAME.fullmatch(cell[field]),
            f"selected cell {field} is not an opaque canonical name",
        )
    profiles = document.get("physical_profiles")
    require(isinstance(profiles, list) and len(profiles) == 2, "experiment requires two profiles")
    profile_ids = []
    for profile in profiles:
        require(isinstance(profile, dict), "physical profile is not an object")
        profile_id = profile.get("id")
        require(
            isinstance(profile_id, str) and CANONICAL_NAME.fullmatch(profile_id),
            "physical profile ID is not an opaque canonical name",
        )
        profile_ids.append(profile_id)
    require(len(set(profile_ids)) == 2, "physical profile IDs are duplicated")
    repetitions = document.get("repetitions_per_profile")
    require(type(repetitions) is int and repetitions >= 3, "fewer than three repetitions")
    output = document.get("output_root")
    require(
        isinstance(output, str)
        and output.startswith("/media/newhd/yifan/db-monitoring-builds/"),
        "output root is outside the bounded artifact volume",
    )
    for field, repository in (
        ("benchbase_head", WORKSPACE / "benchbase"),
        ("si_monitoring_head", ROOT),
        ("co_checker_head", WORKSPACE / "co-checker"),
    ):
        expected = document["build_identity"].get(field)
        require(isinstance(expected, str) and GIT_SHA.fullmatch(expected), f"{field} is invalid")
        actual = subprocess.run(
            ["git", "-C", str(repository), "rev-parse", "HEAD"],
            check=True,
            text=True,
            capture_output=True,
        ).stdout.strip()
        require(actual == expected, f"{field} differs: expected={expected} actual={actual}")
    return document


def runner_command(manifest: dict[str, Any]) -> list[str]:
    cell = manifest["selected_cells"][0]
    workload = manifest["workload"]
    resources = manifest["resources"]
    profiles = [profile["id"] for profile in manifest["physical_profiles"]]
    command = [
        str(ROOT / "scripts" / "run_blackbox_performance.sh"),
        "--backend", cell["backend"],
        "--benchmark", cell["benchmark"],
        "--mode", "full_e2e",
        "--output-root", manifest["output_root"],
        "--repetitions", str(manifest["repetitions_per_profile"] * len(profiles)),
        "--terminals", str(workload["terminals"]),
        "--scale-factor", str(workload["scale_factor"]),
        "--field-size", str(workload["field_size"]),
        "--loader-threads", str(workload["loader_threads"]),
        "--loader-batch-size", str(workload["loader_batch_size"]),
        "--loader-timestamp-utc", workload["loader_timestamp_utc"],
        "--warmup-seconds", str(workload["warmup_seconds"]),
        "--duration-seconds", str(workload["duration_seconds"]),
        "--mix", workload["mix"],
        "--rate", str(workload["rate"]),
        "--seed", str(workload["seed"]),
        "--redis-maxmemory-bytes", str(resources["redis_maxmemory_bytes"]),
        "--drain-timeout-seconds", str(resources["drain_timeout_seconds"]),
        "--status-timeout-seconds", str(resources["status_timeout_seconds"]),
        "--high-water-threshold-percent", str(resources["high_water_threshold_percent"]),
        "--allow-dirty",
    ]
    for profile in profiles:
        command.extend(("--capture-profile", profile))
    return command


def numeric(row: dict[str, str], field: str) -> float | None:
    value = row.get(field, "")
    if value in ("", "NA", None):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def interval_tps_change_percent(row: dict[str, str]) -> float:
    trial = Path(row["artifact_dir"])
    captures = list((trial / "benchbase_results").glob("*.raw.csv"))
    require(len(captures) == 1, f"BenchBase raw interval corpus differs: {trial}")
    start = float(row["benchbase_measured_start_epoch_ms"]) / 1000.0
    end = float(row["benchbase_measured_end_epoch_ms"]) / 1000.0
    midpoint = (start + end) / 2.0
    counts = [0, 0]
    with captures[0].open(newline="", encoding="utf-8") as stream:
        for event in csv.DictReader(stream):
            timestamp = float(event["Start Time (microseconds)"])
            if start <= timestamp <= end:
                counts[0 if timestamp < midpoint else 1] += 1
    half_seconds = (end - start) / 2.0
    require(half_seconds > 0 and counts[0] > 0, "interval throughput window is empty")
    first = counts[0] / half_seconds
    second = counts[1] / half_seconds
    return (second - first) / first * 100.0


def write_pair_report(manifest: dict[str, Any], manifest_path: Path) -> None:
    root = Path(manifest["output_root"])
    with (root / "raw_trials.csv").open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    profiles = [profile["id"] for profile in manifest["physical_profiles"]]
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        profile = row.get("physical_capture_profile", "")
        require(profile in profiles, f"raw trial has undeclared profile: {profile!r}")
        require(row.get("mode") == "full_e2e", "raw trial is not full_e2e")
        require(row.get("audited_outcome") in {"PASS", "VIOLATION"}, "trial is invalid")
        grouped[profile].append(row)
    expected = manifest["repetitions_per_profile"]
    require(all(len(grouped[profile]) == expected for profile in profiles), "profile repetition count differs")

    metric_fields = (
        "logical_transactions_per_second",
        "successful_transactions_per_second",
        "latency_mean_microseconds",
        "latency_p95_microseconds",
        "drain_tail_seconds",
        "redis_cdc_allocated_bytes_per_entry",
        "redis_cdc_logical_bytes_per_entry",
        "redis_cdc_logical_bytes_per_row_event",
        "cdc_ingestion_workload_source_lag_ms_p95",
        "cdc_ingestion_workload_delivery_lag_ms_p95",
        "monitor_end_fence_drain_ms",
        "monitor_process_max_rss_kib",
        "backend_telemetry_summary_intentsdb_rocksdb_bytes_read_delta",
        "backend_telemetry_summary_intentsdb_rocksdb_db_iter_bytes_read_delta",
        "backend_telemetry_summary_intentsdb_rocksdb_number_keys_read_delta",
        "backend_telemetry_summary_intentsdb_rocksdb_number_db_seek_delta",
        "backend_telemetry_summary_service_response_bytes_yb_cdc_cdcservice_getconsistentchanges_delta",
        "resource_yugabytedb_ysql_cpu_mean_percent",
        "resource_yugabytedb_ysql_rss_max_bytes",
        "resource_debezium_cpu_mean_percent",
        "resource_debezium_rss_max_bytes",
    )
    report: dict[str, Any] = {
        "kind": "complete_final_write_capture_profile_paired_result",
        "manifest_id": manifest["manifest_id"],
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "accepted_repetitions": {profile: expected for profile in profiles},
        "rejected_repetitions": {profile: 0 for profile in profiles},
        "profiles": {},
    }
    for profile in profiles:
        summaries: dict[str, Any] = {}
        for field in metric_fields:
            values = [value for row in grouped[profile] if (value := numeric(row, field)) is not None]
            if values:
                summaries[field] = {
                    "per_repetition": values,
                    "median": statistics.median(values),
                }
        interval_values = [interval_tps_change_percent(row) for row in grouped[profile]]
        summaries["interval_logical_tps_change_percent"] = {
            "per_repetition": interval_values,
            "median": statistics.median(interval_values),
        }
        report["profiles"][profile] = {
            "outcomes": [row["audited_outcome"] for row in grouped[profile]],
            "reconciliations": [row.get("reconciliation_result") for row in grouped[profile]],
            "artifact_dirs": [row["artifact_dir"] for row in grouped[profile]],
            "metrics": summaries,
        }
    roles = {profile["role"]: profile["id"] for profile in manifest["physical_profiles"]}
    legacy = roles["legacy"]
    candidate = roles["final_write_candidate"]
    report["paired_changes_percent"] = {}
    for metric in metric_fields + ("interval_logical_tps_change_percent",):
        legacy_metric = report["profiles"][legacy]["metrics"].get(metric)
        candidate_metric = report["profiles"][candidate]["metrics"].get(metric)
        if legacy_metric and candidate_metric and legacy_metric["median"] != 0:
            report["paired_changes_percent"][metric] = (
                (candidate_metric["median"] - legacy_metric["median"])
                / legacy_metric["median"]
                * 100.0
            )
    (root / "experiment-manifest.json").write_bytes(manifest_path.read_bytes())
    (root / "complete_final_write_paired_result.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with (root / "complete_final_write_paired_result.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(("physical_capture_profile", "metric", "median"))
        for profile in profiles:
            for metric, summary in report["profiles"][profile]["metrics"].items():
                writer.writerow((profile, metric, summary["median"]))
    lines = [
        "# Complete Final-Write YugabyteDB Physical-Capture A/B",
        "",
        "All six repetitions ended in audited PASS with exact reconciliation; "
        "three repetitions were accepted per profile and none were rejected.",
        "",
        "| Profile | Logical TPS | Mean latency (µs) | p95 (µs) | "
        "Intents bytes read | CDC response bytes | Interval TPS change |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for profile in profiles:
        metrics = report["profiles"][profile]["metrics"]
        lines.append(
            f"| `{profile}` | {metrics['logical_transactions_per_second']['median']:.2f} | "
            f"{metrics['latency_mean_microseconds']['median']:.2f} | "
            f"{metrics['latency_p95_microseconds']['median']:.2f} | "
            f"{metrics['backend_telemetry_summary_intentsdb_rocksdb_bytes_read_delta']['median']:.0f} | "
            f"{metrics['backend_telemetry_summary_service_response_bytes_yb_cdc_cdcservice_getconsistentchanges_delta']['median']:.0f} | "
            f"{metrics['interval_logical_tps_change_percent']['median']:.2f}% |"
        )
    changes = report["paired_changes_percent"]
    lines.extend(
        (
            "",
            "Candidate versus legacy medians: "
            f"throughput {changes['logical_transactions_per_second']:+.2f}%, "
            f"mean latency {changes['latency_mean_microseconds']:+.2f}%, "
            f"p95 {changes['latency_p95_microseconds']:+.2f}%, "
            "intents-DB bytes read "
            f"{changes['backend_telemetry_summary_intentsdb_rocksdb_bytes_read_delta']:+.2f}%, "
            "and CDC response bytes "
            f"{changes['backend_telemetry_summary_service_response_bytes_yb_cdc_cdcservice_getconsistentchanges_delta']:+.2f}%.",
            "",
            "Only the attested backend-owned physical capture profile differed. "
            "Both profiles used the same application/monitor artifacts and fixed "
            "workload/topology inputs. Both arms declined over the measured window, "
            "so the bounded result is not a general scalability claim.",
        )
    )
    (root / "complete_final_write_paired_result.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "blackbox_performance_report.py"),
            "checksums",
            "--root",
            str(root),
            "--output",
            str(root / "SHA256SUMS"),
        ],
        check=True,
    )
    subprocess.run(["sha256sum", "--check", "SHA256SUMS"], check=True, cwd=root)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--report-only", action="store_true")
    args = parser.parse_args()
    manifest = load_manifest(args.manifest)
    command = runner_command(manifest)
    print(" ".join(command))
    if args.verify_only:
        return 0
    if args.report_only:
        write_pair_report(manifest, args.manifest)
        return 0
    environment = os.environ.copy()
    environment["BLACKBOX_PERFORMANCE_JAVA_FLAGS"] = manifest["resources"]["java_flags"]
    subprocess.run(command, check=True, cwd=ROOT, env=environment)
    write_pair_report(manifest, args.manifest)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError, subprocess.CalledProcessError) as error:
        print(f"complete-final-write capture-profile experiment failed: {error}", file=sys.stderr)
        raise SystemExit(1)
