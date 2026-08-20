#!/usr/bin/env python3
"""Convert a Redis XRANGE JSON capture to RESP XADD commands.

The complete JSON document is loaded and validated before the first byte is
written.  This makes malformed captures fail closed when stdout is piped to
``redis-cli --pipe``.  RESP output is then written one argument at a time, so
output buffering is bounded by the largest UTF-8 field rather than the total
capture size.  The parsed JSON document itself remains resident in memory;
this tool deliberately uses the standard-library ``json.load`` parser.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, BinaryIO, TextIO


STREAM_ID_PATTERN = re.compile(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)\Z")
MAX_U64 = (1 << 64) - 1


class XrangeReplayError(ValueError):
    """The capture cannot be replayed without changing or losing evidence."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise XrangeReplayError(message)


def reject_nonstandard_json_constant(value: str) -> None:
    raise XrangeReplayError(f"XRANGE JSON contains nonstandard constant {value}")


def load_xrange_json(input_stream: TextIO) -> Any:
    """Load strict JSON without accepting Python's NaN/Infinity extensions."""

    try:
        return json.load(input_stream, parse_constant=reject_nonstandard_json_constant)
    except XrangeReplayError:
        raise
    except (json.JSONDecodeError, UnicodeError) as error:
        raise XrangeReplayError(f"cannot decode XRANGE JSON: {error}") from error


def utf8_bytes(value: Any, label: str) -> bytes:
    require(isinstance(value, str), f"{label} is not text")
    try:
        return value.encode("utf-8", "strict")
    except UnicodeEncodeError as error:
        raise XrangeReplayError(f"{label} is not valid UTF-8 text") from error


def parse_stream_id(value: Any, label: str) -> tuple[int, int]:
    require(isinstance(value, str), f"{label} is not text")
    match = STREAM_ID_PATTERN.fullmatch(value)
    require(match is not None, f"{label} is not a canonical Redis stream ID")
    milliseconds = int(match.group(1))
    sequence = int(match.group(2))
    require(
        milliseconds <= MAX_U64 and sequence <= MAX_U64,
        f"{label} exceeds the unsigned 64-bit Redis stream ID range",
    )
    require((milliseconds, sequence) != (0, 0), f"{label} is the 0-0 cursor sentinel")
    return milliseconds, sequence


def validate_capture(document: Any) -> int:
    """Validate the exact flat-array shape emitted by ``redis-cli --json``."""

    require(isinstance(document, list), "XRANGE JSON root is not an array")
    previous_id: tuple[int, int] | None = None
    for entry_index, entry in enumerate(document):
        label = f"XRANGE entry {entry_index}"
        require(
            isinstance(entry, list) and len(entry) == 2,
            f"{label} is not [stream_id, fields]",
        )
        stream_id = parse_stream_id(entry[0], f"{label} stream ID")
        if previous_id is not None:
            require(stream_id > previous_id, "XRANGE stream IDs are not strictly increasing")
        previous_id = stream_id

        fields = entry[1]
        require(isinstance(fields, list), f"{label} fields are not a flat array")
        require(len(fields) >= 2, f"{label} has no field/value pair")
        require(len(fields) % 2 == 0, f"{label} has an unmatched field or value")
        for field_index in range(0, len(fields), 2):
            pair_number = field_index // 2
            utf8_bytes(fields[field_index], f"{label} field {pair_number}")
            utf8_bytes(fields[field_index + 1], f"{label} value {pair_number}")
    return len(document)


def write_resp_bulk(output: BinaryIO, value: bytes) -> None:
    output.write(b"$")
    output.write(str(len(value)).encode("ascii"))
    output.write(b"\r\n")
    output.write(value)
    output.write(b"\r\n")


def write_replay(document: Any, stream_name: str, output: BinaryIO) -> int:
    """Validate *document*, then emit explicit-ID XADD commands to *output*."""

    stream_bytes = utf8_bytes(stream_name, "Redis stream name")
    require(stream_bytes != b"", "Redis stream name is empty")
    entry_count = validate_capture(document)

    # Do not combine commands or the complete output in memory.  Validation
    # above intentionally completes before this loop to avoid a partial replay
    # caused by a malformed later entry.
    for stream_id, fields in document:
        output.write(b"*")
        output.write(str(3 + len(fields)).encode("ascii"))
        output.write(b"\r\n")
        write_resp_bulk(output, b"XADD")
        write_resp_bulk(output, stream_bytes)
        write_resp_bulk(output, stream_id.encode("ascii"))
        for field_or_value in fields:
            write_resp_bulk(output, field_or_value.encode("utf-8", "strict"))
    return entry_count


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "validate an XRANGE --json artifact and write explicit-ID RESP XADD "
            "commands for redis-cli --pipe"
        )
    )
    parser.add_argument(
        "--input-json",
        required=True,
        help="XRANGE JSON path, or - to read JSON from stdin",
    )
    parser.add_argument("--stream", required=True, help="destination Redis stream name")
    return parser.parse_args(argv)


def run(arguments: argparse.Namespace) -> int:
    if arguments.input_json == "-":
        document = load_xrange_json(sys.stdin)
    else:
        path = Path(arguments.input_json)
        try:
            with path.open("r", encoding="utf-8", errors="strict") as input_stream:
                document = load_xrange_json(input_stream)
        except OSError as error:
            raise XrangeReplayError(f"cannot read XRANGE JSON {path}: {error}") from error
    write_replay(document, arguments.stream, sys.stdout.buffer)
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        return run(parse_args(argv))
    except XrangeReplayError as error:
        print(f"XRANGE replay failed: {error}", file=sys.stderr)
        return 2
    except BrokenPipeError:
        print("XRANGE replay failed: output pipe closed before replay completed", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
