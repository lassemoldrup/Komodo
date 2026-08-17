#!/usr/bin/env python3
"""Verify a captured pinned-stack CDC lifecycle stream."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any


STREAM_ID = re.compile(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)\Z")


class StreamError(ValueError):
    pass


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise StreamError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def require(condition: bool, message: str) -> None:
    if not condition:
        raise StreamError(message)


def load_json_text(value: str, label: str) -> Any:
    try:
        return json.loads(value, object_pairs_hook=reject_duplicate_keys)
    except json.JSONDecodeError as error:
        raise StreamError(f"{label} is not JSON: {error}") from error


def integer(value: Any, label: str) -> int:
    require(not isinstance(value, bool), f"{label} is boolean")
    try:
        converted = int(value)
    except (TypeError, ValueError) as error:
        raise StreamError(f"{label} is not an integer: {value!r}") from error
    require(str(converted) == str(value), f"{label} is not canonically spelled: {value!r}")
    return converted


def transaction_summary(
    transaction: dict[str, Any], run_id: str, marker_state: dict[str, int]
) -> dict[str, Any]:
    rows = transaction["rows"]
    tables = {row["table"] for row in rows}
    if tables == {"checker_run_fence"}:
        require(len(rows) == 1, "a fence transaction must contain one row event")
        after = rows[0]["after"]
        require(isinstance(after, dict), "fence row has no after image")
        require(after.get("run_id") == run_id, "fence run_id mismatch")
        phase = after.get("phase")
        require(phase in {"RUN_START", "LOAD_FINISHED", "END_FENCE"}, "unknown fence phase")
        return {
            "classification": "fence",
            "phase": phase,
            "event_count": len(rows),
        }
    if tables == {"usertable"}:
        return {
            "classification": "application",
            "operations": [row["op"] for row in rows],
            "event_count": len(rows),
        }
    require(
        tables in ({"session_txn"}, {"session_txn", "usertable"}),
        f"unexpected or mixed tables: {sorted(tables)}",
    )
    marker_indexes = [
        index for index, row in enumerate(rows) if row["table"] == "session_txn"
    ]
    require(marker_indexes == [len(rows) - 1], "workload marker is not exactly one final row event")
    marker = rows[-1]
    after = marker["after"]
    require(isinstance(after, dict), "workload marker lacks an after image")
    require(after.get("session_id") == "ycsb/worker/1", "workload marker session mismatch")
    session_id = after["session_id"]
    txn_id = integer(after.get("txn_id"), "marker txn_id")
    previous = marker_state.get(session_id)
    if previous is None:
        require(marker["op"] == "c", "first workload marker is not an insert")
        require(marker["before"] is None, "first workload marker has a before image")
    else:
        require(marker["op"] == "u", "later workload marker is not an update")
        before = marker["before"]
        require(isinstance(before, dict), "marker update lacks a before image")
        require(before.get("session_id") == session_id, "marker before session mismatch")
        require(
            integer(before.get("txn_id"), "marker before txn_id") == previous,
            "marker before-image chain mismatch",
        )
        require(txn_id > previous, "marker txn_id did not advance")
    marker_state[session_id] = txn_id
    return {
        "classification": "workload_marked",
        "correlation_id": {"session_id": session_id, "txn_id": txn_id},
        "operations": [row["op"] for row in rows if row["table"] == "usertable"],
        "event_count": len(rows),
    }


def verify_stream(entries: Any, run_id: str, backend_source_id: str) -> list[dict[str, Any]]:
    require(isinstance(entries, list), "XRANGE JSON root is not an array")
    active: dict[str, Any] | None = None
    completed: list[dict[str, Any]] = []
    marker_state: dict[str, int] = {}
    previous_stream_id: tuple[int, int] | None = None

    for entry_index, entry in enumerate(entries):
        require(isinstance(entry, list) and len(entry) == 2, f"entry {entry_index} shape is invalid")
        stream_id, fields = entry
        match = STREAM_ID.fullmatch(stream_id) if isinstance(stream_id, str) else None
        require(match is not None, f"entry {entry_index} has invalid stream ID")
        numeric_stream_id = (int(match.group(1)), int(match.group(2)))
        require(
            previous_stream_id is None or numeric_stream_id > previous_stream_id,
            "Redis stream IDs are not increasing",
        )
        previous_stream_id = numeric_stream_id
        require(
            isinstance(fields, list) and len(fields) == 2,
            f"entry {stream_id} must contain exactly one Debezium key/envelope pair",
        )
        load_json_text(fields[0], f"entry {stream_id} Debezium key")
        envelope = load_json_text(fields[1], f"entry {stream_id} envelope")
        require(isinstance(envelope, dict), f"entry {stream_id} envelope is not an object")

        if "status" in envelope:
            status = envelope["status"]
            transaction_id = envelope.get("id")
            require(isinstance(transaction_id, str) and transaction_id, "metadata lacks transaction id")
            if status == "BEGIN":
                require(active is None, "nested Debezium BEGIN")
                active = {"id": transaction_id, "rows": []}
                continue
            require(status == "END", f"unknown transaction status: {status}")
            require(active is not None, "Debezium END without BEGIN")
            require(transaction_id == active["id"], "BEGIN/END transaction id mismatch")
            event_count = integer(envelope.get("event_count"), "END.event_count")
            require(event_count == len(active["rows"]), "END.event_count mismatch")
            ordinals = [row["total_order"] for row in active["rows"]]
            require(ordinals == list(range(1, event_count + 1)), "row ordinals are not dense")
            collection_counts = Counter(row["collection"] for row in active["rows"])
            declared_counts = Counter()
            for collection in envelope.get("data_collections") or []:
                require(isinstance(collection, dict), "END data_collection is not an object")
                declared_counts[collection.get("data_collection")] += integer(
                    collection.get("event_count"), "END data_collection event_count"
                )
            require(collection_counts == declared_counts, "END per-table counts mismatch")
            completed.append(transaction_summary(active, run_id, marker_state))
            active = None
            continue

        require(active is not None, f"row event {stream_id} appears outside a transaction")
        require(envelope.get("op") in {"c", "u", "d"}, f"unsupported row op at {stream_id}")
        source = envelope.get("source")
        transaction = envelope.get("transaction")
        require(isinstance(source, dict), f"row {stream_id} lacks source metadata")
        require(isinstance(transaction, dict), f"row {stream_id} lacks transaction metadata")
        require(transaction.get("id") == active["id"], "row transaction id mismatch")
        require(source.get("gtid") == active["id"], "row GTID mismatch")
        require(source.get("name") == backend_source_id, "row backend source identity mismatch")
        database = source.get("db")
        table = source.get("table")
        require(database == "benchbase", f"row has unexpected database: {database}")
        require(isinstance(table, str), "row lacks table")
        active["rows"].append(
            {
                "table": table,
                "collection": f"{database}.{table}",
                "op": envelope["op"],
                "before": envelope.get("before"),
                "after": envelope.get("after"),
                "total_order": integer(transaction.get("total_order"), "row total_order"),
            }
        )

    require(active is None, "stream ends with an incomplete transaction")
    shape = [
        (item["classification"], item.get("phase"))
        for item in completed
    ]
    require(
        shape
        == [
            ("fence", "RUN_START"),
            ("application", None),
            ("fence", "LOAD_FINISHED"),
            ("workload_marked", None),
            ("workload_marked", None),
            ("workload_marked", None),
            ("workload_marked", None),
            ("workload_marked", None),
            ("workload_marked", None),
            ("workload_marked", None),
            ("fence", "END_FENCE"),
        ],
        f"unexpected completed transaction order: {shape}",
    )
    require(completed[1]["operations"] == ["c", "c"], "loader transaction is not two inserts")
    expected_operations = [
        ["u"],
        [],
        ["c", "u", "u", "d"],
        ["c"],
        ["u", "u"],
        ["d"],
        ["d", "c"],
    ]
    actual_operations = [item["operations"] for item in completed[3:10]]
    require(actual_operations == expected_operations, "unexpected workload row operations")
    require(
        [item["correlation_id"]["txn_id"] for item in completed[3:10]]
        == list(range(1, 8)),
        "workload marker chain is not 1..7",
    )
    return completed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--backend-source-id", default="mariadb-main")
    parser.add_argument("--summary-output")
    args = parser.parse_args()
    try:
        outer = load_json_text(Path(args.input).read_text(encoding="utf-8"), "XRANGE capture")
        summary = verify_stream(outer, args.run_id, args.backend_source_id)
        rendered = json.dumps(summary, indent=2, sort_keys=True) + "\n"
        if args.summary_output:
            Path(args.summary_output).write_text(rendered, encoding="utf-8")
        sys.stdout.write(rendered)
    except (OSError, StreamError, UnicodeError) as error:
        print(f"lifecycle stream verification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
