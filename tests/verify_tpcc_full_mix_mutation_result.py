#!/usr/bin/env python3
"""Validate that one offline full-mix evidence mutation failed as INVALID."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


class ResultError(ValueError):
    pass


def load(path: Path, label: str):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ResultError(f"cannot read {label}: {error}") from error


def validate(case: str, run_id: str, metrics_path: Path, status_path: Path) -> dict:
    metrics = load(metrics_path, "monitor metrics")
    early_roster_failure = case == "table_roster_mismatch"
    if not (
        isinstance(metrics, dict)
        and metrics.get("kind") == "performance_measurement"
        and metrics.get("run_id") == run_id
        and metrics.get("completed") is False
        and metrics.get("completion_status") == "failed"
        and metrics.get("evidence_reconciled") is False
        and (
            metrics.get("outcome") == "INVALID"
            or (early_roster_failure and "outcome" not in metrics)
        )
        and isinstance(metrics.get("diagnostics"), list)
        and metrics["diagnostics"]
    ):
        raise ResultError("mutated monitor metrics are not failed INVALID")
    if any(item.get("category") == "CHECKER" and item.get("code") == "VIOLATION_OBSERVED"
           for item in metrics["diagnostics"]):
        raise ResultError("evidence mutation was misclassified as checker violation")
    if early_roster_failure and not any(
        item.get("code") == "UNEXPECTED_TABLE" for item in metrics["diagnostics"]
    ):
        raise ResultError("table-roster mutation lacks UNEXPECTED_TABLE diagnostic")
    status_document = load(status_path, "status stream")
    statuses = []
    for entry in status_document:
        try:
            record = json.loads(entry[1][1])
        except (IndexError, TypeError, json.JSONDecodeError) as error:
            raise ResultError(f"malformed status XRANGE: {error}") from error
        if record.get("lifecycle_state") == "FINAL":
            statuses.append(record)
    if len(statuses) != 1 or statuses[0].get("run_id") != run_id or statuses[0].get("outcome") != "INVALID":
        raise ResultError("mutation lacks exactly one final INVALID status")
    return {
        "kind": "tpcc_full_mix_mutation_result",
        "mutation": case,
        "run_id": run_id,
        "outcome": "INVALID",
        "evidence_reconciled": False,
        "diagnostic_codes": sorted({item["code"] for item in metrics["diagnostics"]}),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mutation", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--metrics", required=True, type=Path)
    parser.add_argument("--status", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        report = validate(args.mutation, args.run_id, args.metrics, args.status)
        with args.output.open("x", encoding="utf-8") as output:
            json.dump(report, output, sort_keys=True, separators=(",", ":"))
            output.write("\n")
    except ResultError as error:
        print(f"TPC-C full-mix mutation result failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
