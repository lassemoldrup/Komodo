#!/usr/bin/env python3
"""Create one fail-closed mutation of a captured full-mix TPC-C corpus."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


MUTATIONS = (
    "drop_client",
    "drop_workload_transaction",
    "drop_workload_row",
    "wrong_typed_client_value",
    "table_roster_mismatch",
)


class MutationError(ValueError):
    pass


def load(path: Path) -> list[Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise MutationError(f"cannot load {path}: {error}") from error
    if not isinstance(value, list):
        raise MutationError(f"{path} is not an XRANGE array")
    return value


def payload(entry: Any) -> dict[str, Any] | None:
    try:
        fields = entry[1]
        if fields[0] != "payload":
            return None
        value = json.loads(fields[1])
        return value if isinstance(value, dict) else None
    except (IndexError, TypeError, json.JSONDecodeError):
        return None


def envelope(entry: Any) -> dict[str, Any] | None:
    try:
        value = json.loads(entry[1][1])
        return value if isinstance(value, dict) else None
    except (IndexError, TypeError, json.JSONDecodeError):
        return None


def transaction_ranges(cdc: list[Any]) -> list[tuple[int, int]]:
    ranges, start = [], None
    for index, entry in enumerate(cdc):
        value = envelope(entry)
        status = value.get("status") if value else None
        if status == "BEGIN":
            if start is not None:
                raise MutationError("captured CDC has nested BEGIN")
            start = index
        elif status == "END":
            if start is None:
                raise MutationError("captured CDC has END without BEGIN")
            ranges.append((start, index + 1))
            start = None
    if start is not None:
        raise MutationError("captured CDC ends inside a transaction")
    return ranges


def workload_range(cdc: list[Any], require_application_row: bool) -> tuple[int, int]:
    for start, end in transaction_ranges(cdc):
        tables = []
        for entry in cdc[start + 1 : end - 1]:
            value = envelope(entry)
            table = value.get("source", {}).get("table") if value else None
            tables.append(table)
        if "session_txn" in tables and (
            not require_application_row or any(table != "session_txn" for table in tables)
        ):
            return start, end
    raise MutationError("captured CDC lacks a suitable workload transaction")


def mutate_client(document: list[Any], mutation: str) -> list[Any]:
    result = json.loads(json.dumps(document))
    if mutation == "drop_client":
        index = next(
            (index for index, entry in enumerate(result) if (payload(entry) or {}).get("kind") == "client_txn"),
            None,
        )
        if index is None:
            raise MutationError("client stream lacks ClientTxn")
        result.pop(index)
    elif mutation == "wrong_typed_client_value":
        record = next(
            (payload(entry) for entry in result
             if (payload(entry) or {}).get("kind") == "client_txn"
             and (payload(entry) or {}).get("reads")),
            None,
        )
        if record is None:
            raise MutationError("client stream lacks a read-bearing ClientTxn")
        record["reads"][0]["value_bytes_base64"] = 7
        for entry in result:
            candidate = payload(entry)
            if candidate is not None and candidate.get("logical_txn_id") == record.get("logical_txn_id"):
                entry[1][1] = json.dumps(record, separators=(",", ":"))
                break
    elif mutation == "table_roster_mismatch":
        for entry in result:
            record = payload(entry)
            if record is not None and record.get("kind") == "run_manifest":
                if not record.get("application_table_allowlist"):
                    raise MutationError("RunManifest table roster is empty")
                record["application_table_allowlist"].pop()
                entry[1][1] = json.dumps(record, separators=(",", ":"))
                break
        else:
            raise MutationError("client stream lacks RunManifest")
    else:
        raise MutationError(f"unsupported client mutation {mutation}")
    return result


def mutate_cdc(document: list[Any], mutation: str) -> list[Any]:
    result = json.loads(json.dumps(document))
    if mutation == "drop_workload_transaction":
        start, end = workload_range(result, False)
        del result[start:end]
    elif mutation == "drop_workload_row":
        start, end = workload_range(result, True)
        for index in range(start + 1, end - 1):
            value = envelope(result[index])
            if value and value.get("source", {}).get("table") != "session_txn":
                result.pop(index)
                break
        else:  # pragma: no cover - guarded by workload_range
            raise MutationError("workload transaction lacks application row")
        # Retain END.event_count and transaction total_order. The missing raw
        # row must be detected as a framing/order failure, not normalized away.
    else:
        raise MutationError(f"unsupported CDC mutation {mutation}")
    return result


def publish(path: Path, document: list[Any]) -> None:
    encoded = json.dumps(document, ensure_ascii=False, separators=(",", ":")) + "\n"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write(encoded)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mutation", required=True, choices=MUTATIONS)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        document = load(args.input)
        if args.mutation in {"drop_workload_transaction", "drop_workload_row"}:
            mutated = mutate_cdc(document, args.mutation)
        else:
            mutated = mutate_client(document, args.mutation)
        publish(args.output, mutated)
    except MutationError as error:
        print(f"TPC-C full-mix mutation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
