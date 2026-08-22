#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shlex
import tempfile
from pathlib import Path

from verify_raw_contract import ContractViolation, verify_contract


def monitor_xadds(path: Path) -> tuple[int, list[list[object]]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    entries: list[list[object]] = []
    for line_number, line in enumerate(lines, start=1):
        if line == "OK":
            continue
        if line == "Error: Server closed the connection":
            continue
        separator = line.find("] ")
        if separator < 0:
            raise ContractViolation(
                "CONNECTOR_OUTPUT_CAPTURE_INVALID",
                f"Redis MONITOR line {line_number} has no command boundary",
            )
        try:
            command = shlex.split(line[separator + 2 :], posix=True)
        except ValueError as error:
            raise ContractViolation(
                "CONNECTOR_OUTPUT_CAPTURE_INVALID",
                f"Redis MONITOR line {line_number} cannot be decoded: {error}",
            ) from error
        if not command or command[0].upper() != "XADD":
            continue
        if len(command) != 5 or command[1] != "cdc" or command[2] != "*":
            raise ContractViolation(
                "CONNECTOR_OUTPUT_CAPTURE_INVALID",
                f"unexpected connector XADD shape at MONITOR line {line_number}",
            )
        entries.append([f"{len(entries) + 1}-0", command[3:5]])
    if not entries:
        raise ContractViolation(
            "CONNECTOR_OUTPUT_CAPTURE_INVALID", "Redis MONITOR captured no connector XADD"
        )
    return len(lines), entries


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("monitor_log", type=Path)
    parser.add_argument("--redis-batch-size", type=int, required=True)
    args = parser.parse_args()

    line_count, entries = monitor_xadds(args.monitor_log)
    with tempfile.TemporaryDirectory(prefix="connector-output-order-") as temporary:
        capture = Path(temporary) / "redis-stream.json"
        capture.write_text(json.dumps(entries, separators=(",", ":")), encoding="utf-8")
        summary = verify_contract(capture, "pgoutput", args.redis_batch_size)

    result = {
        "source": "redis_monitor_connector_xadd_order",
        "monitor_lines": line_count,
        "xadd_commands": len(entries),
        "completed_transactions": [frame["marker"] for frame in summary["frames"]],
        "frames": [
            {
                "connector_output_sequence": frame["cdc_seq"],
                "rows": frame["rows"],
                "marker": frame["marker"],
            }
            for frame in summary["frames"]
        ],
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
