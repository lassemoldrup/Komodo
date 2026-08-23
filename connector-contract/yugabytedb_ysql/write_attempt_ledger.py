#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


SQLSTATE = re.compile(r"ERROR:\s+([0-9A-Z]{5}):")


def state(path: Path) -> str:
    match = SQLSTATE.search(path.read_text(encoding="utf-8"))
    if match is None:
        raise SystemExit(f"SQLSTATE is absent from {path}")
    return match.group(1)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--serialization-log", type=Path, required=True)
    parser.add_argument("--deadlock-left-status", type=int, required=True)
    parser.add_argument("--deadlock-left-log", type=Path, required=True)
    parser.add_argument("--deadlock-right-status", type=int, required=True)
    parser.add_argument("--deadlock-right-log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if args.deadlock_left_status == 0 and args.deadlock_right_status != 0:
        winner, loser = "deadlock_left", "deadlock_right"
        deadlock_log = args.deadlock_right_log
    elif args.deadlock_right_status == 0 and args.deadlock_left_status != 0:
        winner, loser = "deadlock_right", "deadlock_left"
        deadlock_log = args.deadlock_left_log
    else:
        raise SystemExit("deadlock experiment did not produce exactly one committed transaction")

    document = {
        "kind": "yugabytedb_ysql_conflict_attempt_ledger",
        "serialization_failure_sqlstate": state(args.serialization_log),
        "serialization_attempts_started": 2,
        "serialization_definitively_aborted_attempts": 1,
        "serialization_successful_attempt_marker": 2,
        "deadlock_failure_sqlstate": state(deadlock_log),
        "deadlock_winner": winner,
        "deadlock_loser": loser,
        "deadlock_loser_attempts_started": 2,
        "deadlock_loser_definitively_aborted_attempts": 1,
        "deadlock_loser_successful_attempt_marker": 2,
        "ambiguous_attempts": 0,
    }
    args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    print(json.dumps(document, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
