#!/usr/bin/env python3
import argparse
import hashlib
import json
from pathlib import Path


def verify_checksums(directory: Path, failures: list[str]) -> None:
    manifest = directory / "SHA256SUMS"
    if not manifest.is_file():
        failures.append(f"missing checksum manifest: {directory}")
        return
    for line in manifest.read_text(encoding="utf-8").splitlines():
        digest, relative = line.split(maxsplit=1)
        relative = relative.removeprefix("*").removeprefix("./")
        path = directory / relative
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            failures.append(f"checksum mismatch: {path}")


def final_status(path: Path) -> dict:
    entries = json.loads(path.read_text(encoding="utf-8"))
    statuses = [
        json.loads(entry[1][1])
        for entry in entries
        if json.loads(entry[1][1]).get("lifecycle_state") == "FINAL"
    ]
    if len(statuses) != 1:
        raise ValueError(f"expected one final status in {path}, got {len(statuses)}")
    return statuses[0]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rr", type=Path, required=True)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--ycsb", type=Path, required=True)
    parser.add_argument("--semantic", type=Path, required=True)
    parser.add_argument("--dropped-client", type=Path, required=True)
    args = parser.parse_args()
    failures: list[str] = []

    for directory in (args.rr, args.raw, args.ycsb, args.semantic, args.dropped_client):
        verify_checksums(directory.resolve(), failures)

    rr = json.loads((args.rr / "verifier-output.json").read_text(encoding="utf-8"))
    raw = json.loads((args.raw / "verifier-output.json").read_text(encoding="utf-8"))
    ycsb = json.loads((args.ycsb / "acceptance-report.json").read_text(encoding="utf-8"))
    if not rr.get("passed"):
        failures.append("RR semantic prerequisite failed")
    if not raw.get("passed"):
        failures.append("raw capability gate failed")
    if not ycsb.get("passed") or ycsb.get("outcome") != "PASS":
        failures.append("ordinary YCSB vertical slice failed")

    try:
        rr_status = final_status(args.semantic / "fractured_repeatable_read-status.json")
        rc_status = final_status(args.semantic / "fractured_read_committed-status.json")
        dropped_status = final_status(args.dropped_client / "dropped_client_record-status.json")
    except (OSError, ValueError, json.JSONDecodeError) as error:
        failures.append(str(error))
        rr_status = rc_status = dropped_status = {}

    if rr_status.get("outcome") != "PASS" or not (
        rr_status.get("reconciliation") or {}
    ).get("evidence_reconciled"):
        failures.append("audited RR fractured-read run did not reconcile as PASS")
    if rc_status.get("outcome") != "VIOLATION" or not (
        rc_status.get("reconciliation") or {}
    ).get("evidence_reconciled"):
        failures.append("audited RC fractured-read run did not reconcile as VIOLATION")
    if dropped_status.get("outcome") != "INVALID" or (
        dropped_status.get("reconciliation") or {}
    ).get("evidence_reconciled"):
        failures.append("dropped-client run did not fail closed as INVALID")

    result = {
        "kind": "oceanbase_mysql_admission_verification",
        "passed": not failures,
        "capability_gate_passed": not failures,
        "verdict_capable": not failures,
        "ordinary_ycsb_outcome": ycsb.get("outcome"),
        "rr_fractured_read_outcome": rr_status.get("outcome"),
        "rc_fractured_read_outcome": rc_status.get("outcome"),
        "dropped_client_outcome": dropped_status.get("outcome"),
        "failures": failures,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
