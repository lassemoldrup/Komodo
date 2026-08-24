#!/usr/bin/env python3
"""Verify tracked pointers to sealed YugabyteDB YSQL TPC-C evidence bundles."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from evidence_archive import archived_bundle, digest


ROOT = Path(__file__).resolve().parent


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def verify_manifest(directory: Path, manifest: Path) -> None:
    expected_files = {
        item.relative_to(directory).as_posix()
        for item in directory.rglob("*")
        if item.is_file() and item != manifest
    }
    listed_files: set[str] = set()
    for line in manifest.read_text(encoding="utf-8").splitlines():
        checksum, separator, raw_name = line.partition("  ")
        name = raw_name.removeprefix("./")
        require(bool(separator) and len(checksum) == 64 and name,
                f"malformed evidence manifest row: {line!r}")
        require(name not in listed_files, f"duplicate evidence manifest row: {name}")
        listed_files.add(name)
        target = directory / name
        require(target.is_file(), f"evidence manifest file is missing: {target}")
        require(digest(target) == checksum, f"evidence file checksum differs: {target}")
    require(listed_files == expected_files,
            f"evidence manifest file roster differs: {directory}")


def run_command(command: list[str], label: str) -> None:
    result = subprocess.run(command, text=True, capture_output=True)
    require(result.returncode == 0, f"{label} failed: {result.stderr.strip()}")


def replay_bundle(directory: Path, purpose: str, saved_report: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="yugabytedb-tpcc-replay-") as temporary:
        output = Path(temporary) / "report.json"
        if purpose == "tpcc_value_normalization":
            command = [
                sys.executable,
                str(ROOT / "verify_tpcc_value_normalization.py"),
                "--redis-stream", str(directory / "redis-stream.json"),
                "--bootstrap-scan", str(directory / "bootstrap-scan.jsonl"),
                "--jdbc-capture", str(directory / "jdbc-capture.json"),
                "--cpp-codec-replay", str(directory / "cpp-codec-replay.json"),
                "--report", str(output),
            ]
            run_command(command, "TPC-C normalization raw replay")
        elif purpose in {
            "tpcc_new_order_workload",
            "tpcc_payment_workload",
            "tpcc_order_status_workload",
            "tpcc_delivery_workload",
            "tpcc_stock_level_workload",
            "tpcc_full_mix_workload",
        }:
            slug = purpose.removeprefix("tpcc_").removesuffix("_workload")
            validator = ROOT.parents[1] / "tests" / f"verify_tpcc_{slug}_acceptance.py"
            extra_flags = {
                "new_order": ("benchbase-log",),
                "payment": (
                    "customer-catalog", "history-catalog",
                    "loader-validation", "benchbase-log",
                ),
                "order_status": ("customer-catalog",),
                "delivery": ("benchbase-log",),
                "stock_level": ("benchbase-log",),
                "full_mix": ("benchbase-log", "loader-validation", "replay-manifest"),
            }[slug]
            extra_files = {
                "benchbase-log": "e2e.log",
                "customer-catalog": "customer-catalog.json",
                "history-catalog": "history-catalog.json",
                "loader-validation": "loader-validation.json",
                "replay-manifest": "replay-manifest.json",
            }
            command = [
                sys.executable, str(validator),
                "--config", str(directory / "run-config.json"),
                "--run-id-file", str(directory / "run-id.txt"),
                "--client-stream", str(directory / "client-stream.json"),
                "--cdc-tail", str(directory / "cdc-tail.json"),
                "--status-stream", str(directory / "status-stream.json"),
                "--monitor-metrics", str(directory / "monitor-metrics.json"),
                "--output", str(output),
            ]
            for flag in extra_flags:
                command += [f"--{flag}", str(directory / extra_files[flag])]
            run_command(command, f"{purpose} raw replay")
        else:
            raise SystemExit(f"unsupported TPC-C evidence purpose: {purpose}")
        require(
            json.loads(output.read_text(encoding="utf-8"))
            == json.loads(saved_report.read_text(encoding="utf-8")),
            f"saved TPC-C report differs from raw replay: {purpose}",
        )


def verify_bundle(directory: Path, bundle: dict[str, object]) -> None:
    purpose = str(bundle["purpose"])
    manifest = directory / str(bundle["manifest"])
    report = directory / str(bundle["report"])
    require(digest(manifest) == bundle["manifest_sha256"],
            f"manifest digest differs: {purpose}")
    verify_manifest(directory, manifest)
    require(digest(report) == bundle["report_sha256"],
            f"report digest differs: {purpose}")
    replay_bundle(directory, purpose, report)
    regular_files = [item for item in directory.rglob("*") if item.is_file()]
    require(
        len(regular_files) == bundle["regular_file_count"] + 1,
        f"bundle file count differs: {purpose}",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--require-local", action="store_true")
    args = parser.parse_args()
    index = json.loads(
        (ROOT / "evidence" / "tpcc_acceptance_bundles.json").read_text(encoding="utf-8")
    )
    require(
        index.get("kind") == "yugabytedb_ysql_tpcc_acceptance_bundle_index",
        "TPC-C evidence index kind differs",
    )
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
    require("tpcc_value_normalization" in purposes,
            "TPC-C evidence lacks the value-normalization gate")
    print("YugabyteDB YSQL TPC-C evidence index verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
