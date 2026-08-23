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
        require(digest(report) == bundle["report_sha256"], f"report digest differs: {purpose}")
        regular_files = [item for item in path.rglob("*") if item.is_file()]
        require(
            len(regular_files) == bundle["regular_file_count"] + 1,
            f"bundle file count differs: {purpose}",
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
