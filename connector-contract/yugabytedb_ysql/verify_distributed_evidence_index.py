#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from evidence_archive import archived_bundle, digest


ROOT = Path(__file__).resolve().parent
EXPECTED_PURPOSES = {
    "cross_tablet_marker_last",
    "cross_table_test_and_tpcc",
    "commit_order_reversal",
    "commit_order_realtime",
    "concurrent_four_tablets",
    "large_transaction_boundaries",
    "write_shape_predecessors",
    "serialization_deadlock_retries",
    "redis_backpressure_order",
    "connector_queue_saturation_order",
    "tablet_split_leader_movement_tserver_loss",
}
PURPOSE_SCENARIOS = {
    "cross_tablet_marker_last": "cross_tablet_marker_last",
    "cross_table_test_and_tpcc": "cross_table_transaction",
    "commit_order_reversal": "commit_order_reversal",
    "commit_order_realtime": "commit_order_realtime",
    "concurrent_four_tablets": "concurrent_tablet_commits",
    "large_transaction_boundaries": "large_transaction",
    "write_shape_predecessors": "write_shape_matrix",
    "serialization_deadlock_retries": "conflict_retry_matrix",
    "redis_backpressure_order": "redis_backpressure_reversal",
    "connector_queue_saturation_order": "queue_saturation_order",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def verify_manifest(directory: Path, manifest: Path) -> None:
    expected_files = {
        item.name for item in directory.iterdir()
        if item.is_file() and item.name != manifest.name
    }
    listed_files: set[str] = set()
    for line in manifest.read_text(encoding="utf-8").splitlines():
        checksum, separator, name = line.partition("  ")
        require(bool(separator) and len(checksum) == 64 and name == Path(name).name,
                f"malformed checksum manifest row: {line!r}")
        require(name not in listed_files, f"duplicate checksum manifest row: {name}")
        listed_files.add(name)
        target = directory / name
        require(target.is_file(), f"manifest file is missing: {target}")
        require(digest(target) == checksum, f"manifest file checksum differs: {target}")
    require(listed_files == expected_files,
            f"manifest file roster differs: {directory}")


def replay_bundle(directory: Path, purpose: str, saved_report: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="yugabytedb-distributed-replay-") as temporary:
        output = Path(temporary) / "report.json"
        if purpose == "tablet_split_leader_movement_tserver_loss":
            command = [
                sys.executable,
                str(ROOT / "verify_resilience_experiment.py"),
                "--directory",
                str(directory),
                "--report",
                str(output),
            ]
        else:
            scenario = PURPOSE_SCENARIOS.get(purpose)
            require(scenario is not None, f"distributed replay purpose is unsupported: {purpose}")
            metadata = json.loads(
                (directory / "capture-metadata.json").read_text(encoding="utf-8")
            )
            require(
                metadata.get("scenario") == scenario,
                f"distributed evidence purpose/scenario differs: {purpose}",
            )
            command = [
                sys.executable,
                str(ROOT / "verify_distributed_experiment.py"),
                "--scenario",
                scenario,
                "--capture",
                str(directory / "redis-stream.json"),
                "--placement",
                str(directory / "tablet-placement.json"),
                "--redis-batch-size",
                str(metadata["redis_sink_batch_size"]),
                "--source-batch-size",
                str(metadata["source_max_batch_size"]),
                "--report",
                str(output),
            ]
            if scenario == "conflict_retry_matrix":
                command.extend(["--attempt-ledger", str(directory / "attempt-ledger.json")])
            if scenario == "queue_saturation_order":
                command.extend(
                    ["--queue-evidence", str(directory / "queue-saturation-evidence.json")]
                )
        result = subprocess.run(command, text=True, capture_output=True)
        require(result.returncode == 0,
                f"distributed raw replay failed for {purpose}: {result.stderr.strip()}")
        require(
            json.loads(output.read_text(encoding="utf-8"))
            == json.loads(saved_report.read_text(encoding="utf-8")),
            f"distributed saved report differs from raw replay: {purpose}",
        )


def verify_bundle(directory: Path, bundle: dict[str, object]) -> None:
    purpose = bundle["purpose"]
    manifest = directory / "SHA256SUMS"
    report = directory / str(bundle["report"])
    require(digest(manifest) == bundle["manifest_sha256"],
            f"manifest digest differs: {purpose}")
    verify_manifest(directory, manifest)
    require(digest(report) == bundle["report_sha256"],
            f"report digest differs: {purpose}")
    replay_bundle(directory, str(purpose), report)
    regular_files = [item for item in directory.rglob("*") if item.is_file()]
    require(len(regular_files) == bundle["regular_file_count"] + 1,
            f"bundle file count differs: {purpose}")
    document = json.loads(report.read_text(encoding="utf-8"))
    require(document.get("status") == "PASSED", f"bundle report did not pass: {purpose}")
    require(document.get("consistency_verdict_claimed") is not True,
            f"diagnostic bundle claims a consistency verdict: {purpose}")
    if purpose == "connector_queue_saturation_order":
        measurement = document.get("details", {}).get("queue_saturation_measurement", {})
        require(
            measurement.get("metric_source")
            == "remote_jmx_streaming_connector_mbean"
            and measurement.get("minimum_remaining_capacity") == 0
            and measurement.get("maximum_observed_occupancy")
            == measurement.get("configured_and_observed_total_capacity")
            and measurement.get("saturated_samples", 0) > 0,
            "indexed connector queue evidence is not a measured saturation",
        )
    if purpose == "tablet_split_leader_movement_tserver_loss":
        overlap = document.get("topology_operations_overlapped_transactions", {})
        closing = document.get("closing_boundary", {})
        require(
            set(overlap) == {"split", "leader_movement", "tserver_loss"}
            and document.get("exact_cdc_sql_state_equal") is True
            and document.get("exact_application_rows") == 8195
            and closing.get("stream_length_before")
            == closing.get("stream_length_after")
            and closing.get("end_count_before") == closing.get("end_count_after") == 5
            and isinstance(document.get("locked_connector_image_id"), str),
            "indexed resilience evidence lacks overlap, exact state, closure, or provenance",
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--require-local", action="store_true")
    args = parser.parse_args()
    index = json.loads(
        (ROOT / "evidence" / "distributed_correctness_bundles.json").read_text()
    )
    require(
        index.get("kind") == "yugabytedb_ysql_distributed_correctness_bundle_index",
        "distributed evidence index kind differs",
    )
    require(index.get("platform") == "linux/amd64", "distributed evidence platform differs")
    purposes: set[str] = set()
    for bundle in index.get("bundles", []):
        purpose = bundle.get("purpose")
        require(isinstance(purpose, str) and purpose not in purposes, "bundle purpose differs")
        purposes.add(purpose)
        path = ROOT / str(bundle["bundle_path"])
        require(not args.require_local or path.is_dir(),
                f"local evidence bundle is missing: {path}")
        with archived_bundle(ROOT, bundle) as archived:
            verify_bundle(archived, bundle)
        if path.is_dir():
            verify_bundle(path, bundle)
    require(purposes == EXPECTED_PURPOSES, "distributed evidence purpose roster differs")
    print("YugabyteDB YSQL distributed correctness evidence index verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
