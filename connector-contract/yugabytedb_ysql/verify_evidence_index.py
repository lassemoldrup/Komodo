#!/usr/bin/env python3
"""Verify tracked pointers to sealed local YugabyteDB YSQL evidence bundles."""

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


def production_replay(
    replay_bin: Path, client: Path, cdc: Path, captured_final: dict[str, object]
) -> dict[str, object]:
    result = subprocess.run(
        [
            str(replay_bin),
            "--client-input", str(client),
            "--cdc-input", str(cdc),
        ],
        text=True,
        capture_output=True,
    )
    require(result.returncode == 0,
            f"production decoder/checker replay failed: {result.stderr.strip()}")
    replay = json.loads(result.stdout)
    require(
        replay.get("kind") == "production_evidence_replay"
        and replay.get("final_status") == captured_final,
        "captured FINAL differs from production decoder/checker replay",
    )
    return replay


def replay_bundle(
    directory: Path,
    purpose: str,
    saved_report: Path,
    replay_bin: Path,
) -> None:
    with tempfile.TemporaryDirectory(prefix="yugabytedb-acceptance-replay-") as temporary:
        output = Path(temporary) / "report.json"
        if purpose == "complete_value_normalization":
            command = [
                sys.executable,
                str(ROOT / "verify_ycsb_value_normalization.py"),
                "--redis-stream", str(directory / "redis-stream.json"),
                "--bootstrap-scan", str(directory / "bootstrap-scan.tsv"),
                "--jdbc-capture", str(directory / "jdbc-capture.json"),
                "--cpp-codec-replay", str(directory / "cpp-codec-replay.json"),
                "--report", str(output),
            ]
            run_command(command, "normalization raw replay")
        elif purpose == "ordinary_two_session_workload":
            outcomes = list((directory / "workload-results").glob("*.outcomes.json"))
            require(len(outcomes) == 1, "ordinary outcome artifact roster differs")
            command = [
                sys.executable,
                str(ROOT.parents[1] / "tests" / "verify_yugabytedb_ysql_ycsb_ordinary.py"),
                "--monitor", str(directory / "monitor-metrics.json"),
                "--client", str(directory / "redis-client.json"),
                "--cdc", str(directory / "redis-cdc.json"),
                "--status", str(directory / "redis-status.json"),
                "--outcomes", str(outcomes[0]),
                "--bootstrap", str(directory / "bootstrap-manifest.json"),
                "--loader-validation", str(directory / "loader-validation.json"),
                "--tablet-count", str(directory / "usertable-tablet-count.txt"),
                "--provenance", str(directory / "provenance.lock.json"),
                "--connector-container",
                str(directory / "cdc-debezium-yugabytedb-ysql-inspect.json"),
                "--connector-image", str(directory / "connector-image.json"),
                "--connector-identity", str(directory / "connector-identity.txt"),
                "--output", str(output),
            ]
            run_command(command, "ordinary raw replay")
            statuses = json.loads((directory / "redis-status.json").read_text())
            finals = [
                json.loads(entry[1][1])
                for entry in statuses
                if json.loads(entry[1][1]).get("lifecycle_state") == "FINAL"
            ]
            require(len(finals) == 1, "ordinary FINAL roster differs")
            production_replay(
                replay_bin,
                directory / "redis-client.json",
                directory / "redis-cdc.json",
                finals[0],
            )
        elif purpose == "retry_and_isolation_semantics":
            command = [
                sys.executable,
                str(ROOT.parents[1] / "tests" / "verify_yugabytedb_ysql_ycsb_semantic.py"),
                "--directory", str(directory),
                "--provenance", str(directory / "provenance.lock.json"),
                "--evidence-replay-bin", str(replay_bin),
                "--output", str(output),
            ]
            run_command(command, "semantic raw replay")
        elif purpose == "redis_server_operational_failures":
            saved = json.loads(saved_report.read_text(encoding="utf-8"))
            require(
                saved.get("kind") == "redis_audited_failure_acceptance"
                and saved.get("status") == "PASSED"
                and saved.get("wrong_noeviction_policy_rejected") is True
                and saved.get("actual_server_xadd_oom_observed") is True
                and saved.get("consistency_verdict_claimed") is False,
                "Redis audited report identity differs",
            )
            final = saved.get("authoritative_final")
            require(isinstance(final, dict) and final.get("outcome") == "INVALID",
                    "Redis report lacks authoritative FINAL INVALID")
            production_replay(
                replay_bin,
                directory / "redis_oom_invalid-client.json",
                directory / "redis_oom_invalid-cdc.json",
                final,
            )
            require(
                "OOM command not allowed"
                in (directory / "redis_oom_invalid-operation.txt").read_text(
                    encoding="utf-8"
                ),
                "Redis raw evidence lacks the server OOM",
            )
            return
        else:
            raise SystemExit(f"unsupported acceptance evidence purpose: {purpose}")
        require(
            json.loads(output.read_text(encoding="utf-8"))
            == json.loads(saved_report.read_text(encoding="utf-8")),
            f"saved acceptance report differs from raw replay: {purpose}",
        )


def verify_bundle(
    directory: Path, bundle: dict[str, object], replay_bin: Path
) -> None:
    purpose = bundle["purpose"]
    manifest = directory / str(bundle["manifest"])
    report = directory / str(bundle["report"])
    require(digest(manifest) == bundle["manifest_sha256"],
            f"manifest digest differs: {purpose}")
    verify_manifest(directory, manifest)
    require(digest(report) == bundle["report_sha256"],
            f"report digest differs: {purpose}")
    if purpose == "complete_value_normalization":
        legacy_replay = json.loads(
            (directory / "cpp-codec-replay.json").read_text(encoding="utf-8")
        )
        events = legacy_replay.get("frames", [{}])[0].get("application_events", [])
        require(
            bool(events)
            and "before" in events[0]
            and "final_write" not in events[0],
            "legacy normalization archive shape differs",
        )
        print(
            "complete_value_normalization archive checksum verified; "
            "normalized replay is explicitly superseded by the complete-final-write contract",
            file=sys.stderr,
        )
    else:
        replay_bundle(directory, str(purpose), report, replay_bin)
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
            and document.get("connector_stall_invalid", {}).get("outcome") == "INVALID"
            and document.get("redis_oom_invalid", {}).get("outcome") == "INVALID"
            and document.get("redis_oom_invalid", {}).get("authoritative_final") is True,
            "semantic evidence lacks a required authoritative INVALID gate",
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
            and document.get("authoritative_final", {}).get("outcome") == "INVALID"
            and document.get("authoritative_final", {})
            .get("reconciliation", {})
            .get("evidence_reconciled") is False,
            "Redis evidence lacks policy rejection, server OOM, or FINAL INVALID",
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--require-local", action="store_true")
    parser.add_argument("--evidence-replay-bin", type=Path, required=True)
    args = parser.parse_args()
    require(args.evidence_replay_bin.is_file(), "production evidence replay binary is missing")
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
            verify_bundle(archived, bundle, args.evidence_replay_bin)
        if path.is_dir():
            verify_bundle(path, bundle, args.evidence_replay_bin)
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
