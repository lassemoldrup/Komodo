#!/usr/bin/env python3
"""Verify tracked pointers to sealed local YugabyteDB YSQL evidence bundles."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--require-local", action="store_true")
    args = parser.parse_args()
    index = json.loads(
        (ROOT / "evidence" / "ycsb_acceptance_bundles.json").read_text(
            encoding="utf-8"
        )
    )
    require(
        index.get("kind") == "yugabytedb_ysql_ycsb_acceptance_bundle_index",
        "evidence index kind differs",
    )
    purposes: set[str] = set()
    for bundle in index.get("bundles", []):
        purpose = bundle.get("purpose")
        require(isinstance(purpose, str) and purpose not in purposes, "bundle purpose differs")
        purposes.add(purpose)
        path = ROOT / bundle["bundle_path"]
        if not path.is_dir():
            require(not args.require_local, f"local evidence bundle is missing: {path}")
            continue
        manifest = path / bundle["manifest"]
        report = path / bundle["report"]
        require(digest(manifest) == bundle["manifest_sha256"], f"manifest digest differs: {purpose}")
        verify_manifest(path, manifest)
        require(digest(report) == bundle["report_sha256"], f"report digest differs: {purpose}")
        regular_files = [item for item in path.rglob("*") if item.is_file()]
        require(
            len(regular_files) == bundle["regular_file_count"] + 1,
            f"bundle file count differs: {purpose}",
        )
        if purpose == "retry_and_isolation_semantics":
            document = json.loads(report.read_text(encoding="utf-8"))
            dropped = document.get("dropped_client_record", {})
            require(
                dropped.get("outcome") == "INVALID"
                and dropped.get("diagnostic") == "CLIENT_HALF_MISSING"
                and dropped.get("evidence_reconciled") is False,
                "semantic evidence lacks the dropped-client INVALID gate",
            )
    require(
        purposes
        == {
            "complete_value_normalization",
            "ordinary_two_session_workload",
            "retry_and_isolation_semantics",
        },
        "evidence purpose roster differs",
    )
    print("YugabyteDB YSQL evidence index verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
