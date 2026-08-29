#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def lines(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def status(path: Path) -> int:
    return int(path.read_text(encoding="utf-8").strip())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    evidence = parser.parse_args().evidence.resolve()
    failures: list[str] = []

    if lines(evidence / "session-contract.txt") != ["REPEATABLE-READ\tSTRONG"]:
        failures.append("session did not report REPEATABLE-READ and STRONG")
    if status(evidence / "snapshot-reader.status") != 0:
        failures.append("snapshot reader failed")
    if lines(evidence / "snapshot-reader.txt") != ["old", "old"]:
        failures.append("later read did not retain the first snapshot")
    if status(evidence / "write-conflict-first.status") != 0:
        failures.append("first same-row writer failed")
    if status(evidence / "write-conflict-second.status") == 0:
        failures.append("overlapping later same-row writer committed")
    if not lines(evidence / "write-conflict-second.txt") or lines(
        evidence / "write-conflict-second.txt"
    )[0] != "initial":
        failures.append("later writer did not establish the old-row snapshot")
    if lines(evidence / "write-conflict-final.txt") != ["winner"]:
        failures.append("write-conflict final value is not the winner")
    if status(evidence / "write-skew-first.status") != 0:
        failures.append("first write-skew transaction failed")
    if status(evidence / "write-skew-second.status") != 0:
        failures.append("second write-skew transaction failed")
    if lines(evidence / "write-skew-first.txt") != ["1"]:
        failures.append("first write-skew transaction did not read the old peer")
    if lines(evidence / "write-skew-second.txt") != ["1"]:
        failures.append("second write-skew transaction did not read the old peer")
    if lines(evidence / "write-skew-final.txt") != ["9201\t0", "9202\t0"]:
        failures.append("write-skew final state does not preserve both disjoint commits")

    result = {
        "kind": "oceanbase_mysql_rr_semantic_litmus",
        "passed": not failures,
        "snapshot_retained": "later read did not retain the first snapshot" not in failures,
        "same_row_write_conflict_aborted": "overlapping later same-row writer committed" not in failures,
        "write_skew_allowed": "write-skew final state does not preserve both disjoint commits" not in failures,
        "failures": failures,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
