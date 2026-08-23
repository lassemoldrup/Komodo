#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from verify_raw_contract import ContractViolation, verify_contract


def decode_redis_monitor_command(value: str, line_number: int) -> list[str]:
    arguments: list[str] = []
    index = 0
    while index < len(value):
        while index < len(value) and value[index].isspace():
            index += 1
        if index == len(value):
            break
        if value[index] != '"':
            raise ContractViolation(
                "CONNECTOR_OUTPUT_CAPTURE_INVALID",
                f"Redis MONITOR line {line_number} contains an unquoted argument",
            )
        index += 1
        encoded = bytearray()
        while index < len(value) and value[index] != '"':
            character = value[index]
            if character != "\\":
                encoded.extend(character.encode("utf-8"))
                index += 1
                continue
            index += 1
            if index == len(value):
                raise ContractViolation(
                    "CONNECTOR_OUTPUT_CAPTURE_INVALID",
                    f"Redis MONITOR line {line_number} ends inside an escape",
                )
            escape = value[index]
            if escape == "x":
                digits = value[index + 1 : index + 3]
                if len(digits) != 2 or any(
                    digit not in "0123456789abcdefABCDEF" for digit in digits
                ):
                    raise ContractViolation(
                        "CONNECTOR_OUTPUT_CAPTURE_INVALID",
                        f"Redis MONITOR line {line_number} has a malformed hexadecimal escape",
                    )
                encoded.append(int(digits, 16))
                index += 3
                continue
            escaped_bytes = {
                "\\": b"\\",
                '"': b'"',
                "n": b"\n",
                "r": b"\r",
                "t": b"\t",
                "a": b"\a",
                "b": b"\b",
            }.get(escape)
            if escaped_bytes is None:
                raise ContractViolation(
                    "CONNECTOR_OUTPUT_CAPTURE_INVALID",
                    f"Redis MONITOR line {line_number} has an unknown escape",
                )
            encoded.extend(escaped_bytes)
            index += 1
        if index == len(value):
            raise ContractViolation(
                "CONNECTOR_OUTPUT_CAPTURE_INVALID",
                f"Redis MONITOR line {line_number} has an unterminated argument",
            )
        index += 1
        if index < len(value) and not value[index].isspace():
            raise ContractViolation(
                "CONNECTOR_OUTPUT_CAPTURE_INVALID",
                f"Redis MONITOR line {line_number} has no argument boundary",
            )
        try:
            arguments.append(encoded.decode("utf-8", "strict"))
        except UnicodeDecodeError as error:
            raise ContractViolation(
                "CONNECTOR_OUTPUT_CAPTURE_INVALID",
                f"Redis MONITOR line {line_number} contains malformed UTF-8",
            ) from error
    return arguments


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
        command = decode_redis_monitor_command(line[separator + 2 :], line_number)
        if not command or command[0].upper() != "XADD":
            continue
        if len(command) != 5 or command[1] != "cdc" or command[2] != "*":
            raise ContractViolation(
                "CONNECTOR_OUTPUT_CAPTURE_INVALID",
                f"unexpected connector XADD shape at MONITOR line {line_number}",
            )
        entries.append(
            [
                f"{len(entries) + 1}-0",
                [command[3], command[4]],
            ]
        )
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
