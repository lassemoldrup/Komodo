#!/usr/bin/env python3
"""Verify tracked pointers to sealed local YugabyteDB YSQL evidence bundles."""

from __future__ import annotations

import argparse
import json
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


def verify_bundle(directory: Path, bundle: dict[str, object]) -> None:
    purpose = bundle["purpose"]
    manifest = directory / str(bundle["manifest"])
    report = directory / str(bundle["report"])
    require(digest(manifest) == bundle["manifest_sha256"],
            f"manifest digest differs: {purpose}")
    verify_manifest(directory, manifest)
    require(digest(report) == bundle["report_sha256"],
            f"report digest differs: {purpose}")
    regular_files = [item for item in directory.rglob("*") if item.is_file()]
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
            and dropped.get("evidence_reconciled") is False
            and document.get("connector_stall_invalid", {}).get("outcome") == "INVALID",
            "semantic evidence lacks the dropped-client or connector-stall INVALID gate",
        )
    if purpose == "ordinary_two_session_workload":
        provenance = json.loads(
            (directory / "portable-evidence-provenance.json").read_text(
                encoding="utf-8"
            )
        )
        omitted = provenance.get("omitted_runtime_artifacts", {})
        revisions = provenance.get("repository_revisions", {})
        require(
            all(
                isinstance(omitted.get(name, {}).get("sha256"), str)
                and len(omitted[name]["sha256"]) == 64
                and omitted[name].get("size_bytes", 0) > 0
                for name in ("benchbase_archive", "monitor_binary")
            )
            and all(
                isinstance(revisions.get(name), str) and len(revisions[name]) == 40
                for name in (
                    "evidence_packaging_head",
                    "benchbase_head",
                    "co_checker_head",
                )
            ),
            "ordinary evidence lacks runtime artifact or repository provenance",
        )
    if purpose == "redis_server_operational_failures":
        document = json.loads(report.read_text(encoding="utf-8"))
        require(
            document.get("status") == "PASSED"
            and document.get("wrong_noeviction_policy_rejected") is True
            and document.get("actual_server_xadd_oom_observed") is True
            and document.get("stream_length_after_rejected_xadd") == 0
            and str(document.get("oom_response", "")).startswith(
                "OOM command not allowed"
            ),
            "Redis evidence lacks actual policy rejection or server XADD OOM",
        )


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
        path = ROOT / str(bundle["bundle_path"])
        require(not args.require_local or path.is_dir(),
                f"local evidence bundle is missing: {path}")
        with archived_bundle(ROOT, bundle) as archived:
            verify_bundle(archived, bundle)
        if path.is_dir():
            verify_bundle(path, bundle)
    require(
        purposes
        == {
            "complete_value_normalization",
            "ordinary_two_session_workload",
            "retry_and_isolation_semantics",
            "redis_server_operational_failures",
        },
        "evidence purpose roster differs",
    )
    print("YugabyteDB YSQL evidence index verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
