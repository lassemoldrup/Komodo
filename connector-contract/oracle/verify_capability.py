#!/usr/bin/env python3
"""Fail-closed verifier for the pinned Oracle LogMiner capability capture."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


EXPECTED_SOURCE = "oracle_blackbox_capability"
EXPECTED_VERSION = "3.6.1.Final"
EXPECTED_COLLECTION_PREFIX = "FREEPDB1.BLACKBOX_OWNER."


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def parse_capture(path: Path) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(document, list), "Redis capture is not an array")
    parsed: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    previous = (-1, -1)
    for raw in document:
        require(isinstance(raw, list) and len(raw) == 2, "Redis entry shape differs")
        stream_id, fields = raw
        require(isinstance(stream_id, str), "Redis stream ID is not text")
        milliseconds, sequence = (int(part) for part in stream_id.split("-", 1))
        require((milliseconds, sequence) > previous, "Redis stream IDs are not increasing")
        previous = (milliseconds, sequence)
        require(isinstance(fields, list) and len(fields) == 2, "Redis fields differ")
        key = json.loads(fields[0])
        envelope = json.loads(fields[1])
        require(isinstance(key, dict) and isinstance(envelope, dict), "JSON shape differs")
        parsed.append((stream_id, key, envelope))
    return parsed


def frames(entries: list[tuple[str, dict[str, Any], dict[str, Any]]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    active: dict[str, Any] | None = None
    for stream_id, key, envelope in entries:
        status = envelope.get("status")
        if status == "BEGIN":
            require(active is None, "nested BEGIN")
            require(set(key) == {"id"} and key["id"] == envelope.get("id"), "BEGIN ID differs")
            active = {"begin": stream_id, "id": envelope["id"], "rows": []}
            continue
        if status == "END":
            require(active is not None, "END outside a frame")
            require(set(key) == {"id"} and key["id"] == envelope.get("id"), "END ID differs")
            require(envelope["event_count"] == len(active["rows"]), "END count differs")
            counts: dict[str, int] = {}
            for row in active["rows"]:
                collection = EXPECTED_COLLECTION_PREFIX + row["source"]["table"]
                counts[collection] = counts.get(collection, 0) + 1
            declared = {
                item["data_collection"]: item["event_count"]
                for item in envelope["data_collections"]
            }
            require(declared == counts, "END per-collection counts differ")
            active["end"] = stream_id
            active["end_envelope"] = envelope
            result.append(active)
            active = None
            continue
        require(active is not None, "row outside a frame")
        require(envelope.get("op") in {"c", "u", "d"}, "unsupported row operation")
        source = envelope.get("source")
        transaction = envelope.get("transaction")
        require(isinstance(source, dict) and isinstance(transaction, dict), "row metadata differs")
        require(source.get("connector") == "oracle", "source connector differs")
        require(source.get("version") == EXPECTED_VERSION, "source version differs")
        require(source.get("name") == EXPECTED_SOURCE, "source identity differs")
        require(source.get("snapshot") == "false", "snapshot row observed")
        require(source.get("db") == "FREEPDB1" and source.get("schema") == "BLACKBOX_OWNER", "namespace differs")
        expected_order = len(active["rows"]) + 1
        require(transaction.get("total_order") == expected_order, "total order is not contiguous")
        require(transaction.get("id") == active["id"], "row transaction ID differs")
        op = envelope["op"]
        after = envelope.get("after")
        require((op == "d" and after is None) or (op != "d" and isinstance(after, dict)), "final row shape differs")
        require(key, "event key is empty")
        if after is not None:
            key_name = "SESSION_ID" if source["table"] == "SESSION_TXN" else "ID"
            require(key.get(key_name) == after.get(key_name), "event key/final row identity differs")
        active["rows"].append(envelope)
    require(active is None, "capture ends inside a frame")
    return result


def marker(frame: dict[str, Any]) -> tuple[str, int]:
    rows = frame["rows"]
    require(rows[-1]["source"]["table"] == "SESSION_TXN", "marker is not final")
    require(rows[-1]["op"] == "c", "marker is not CREATE")
    after = rows[-1]["after"]
    return after["SESSION_ID"], int(after["TXN_ID"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("capture", type=Path)
    args = parser.parse_args()
    captured = parse_capture(args.capture)
    require(len(captured) == 31, "capture does not contain the exact 31-record corpus")
    transactions = frames(captured)
    require(len(transactions) == 7, "capture does not contain seven transactions")
    by_marker = {marker(frame): frame for frame in transactions}
    expected_markers = {
        *(('oracle/capability/main', value) for value in range(1, 6)),
        ('oracle/capability/concurrent-a', 1),
        ('oracle/capability/concurrent-b', 1),
    }
    require(set(by_marker) == expected_markers, "marker roster differs")

    first = by_marker[("oracle/capability/main", 1)]["rows"]
    require([row["op"] for row in first] == ["c", "c"], "insert transaction shape differs")
    inserted = first[0]["after"]
    require(set(inserted) == {"ID", "AMOUNT", "FIXED_TEXT", "VARIABLE_TEXT", "CALENDAR_DATE", "EVENT_TIME"}, "insert final row is incomplete")
    require(inserted["AMOUNT"] == "12.30" and inserted["FIXED_TEXT"] == "A   ", "NUMBER/CHAR normalization differs")
    require(inserted["VARIABLE_TEXT"] is None, "Oracle empty string did not normalize to NULL")
    require(isinstance(inserted["CALENDAR_DATE"], int) and isinstance(inserted["EVENT_TIME"], int), "DATE/TIMESTAMP raw types differ")

    repeated = by_marker[("oracle/capability/main", 2)]["rows"]
    require([row["op"] for row in repeated] == ["u", "u", "c"], "repeated update shape differs")
    require(all(set(row["after"]) == set(inserted) for row in repeated[:2]), "update final row is sparse")
    require(repeated[0]["after"]["VARIABLE_TEXT"] == "temporary", "first update differs")
    require(repeated[1]["after"] == inserted, "update-to-original final row differs")

    insert_delete = by_marker[("oracle/capability/main", 3)]["rows"]
    require([row["op"] for row in insert_delete] == ["c", "d", "c"], "insert-delete shape differs")
    require(insert_delete[0]["after"]["AMOUNT"] is None, "SQL NULL differs")
    require(insert_delete[0]["source"]["table"] == "ORACLE_CAPABILITY_ROWS", "insert-delete table differs")

    key_change = by_marker[("oracle/capability/main", 4)]["rows"]
    require([row["op"] for row in key_change] == ["d", "c", "c"], "primary-key update shape differs")
    require(key_change[0]["before"]["ID"] == "1" and key_change[1]["after"]["ID"] == "101", "old/new primary keys differ")
    final_delete = by_marker[("oracle/capability/main", 5)]["rows"][0]
    require(final_delete["op"] == "d" and final_delete["before"]["ID"] == "101", "delete exact key differs")

    order = [marker(frame)[0] for frame in transactions]
    require(order[-2:] == ["oracle/capability/concurrent-b", "oracle/capability/concurrent-a"], "concurrent completed order differs")
    commit_scns = [int(frame["rows"][0]["source"]["commit_scn"]) for frame in transactions]
    require(commit_scns == sorted(commit_scns) and len(set(commit_scns)) == len(commit_scns), "commit SCN order differs")
    delivery_lags = []
    for frame in transactions:
        stream_ms = int(frame["end"].split("-", 1)[0])
        commit_ms = int(frame["rows"][0]["source"]["commit_ts_ms"])
        lag = stream_ms - commit_ms
        require(0 <= lag <= 30_000, "connector progress exceeded the 30-second bound")
        delivery_lags.append(lag)

    report = {
        "kind": "oracle_logminer_capability_result",
        "accepted": True,
        "redis_records": len(captured),
        "transactions": len(transactions),
        "row_events": sum(len(frame["rows"]) for frame in transactions),
        "max_commit_to_redis_end_ms": max(delivery_lags),
        "proved": [
            "contiguous_begin_rows_end",
            "exact_event_and_collection_counts",
            "concurrent_commit_order",
            "complete_insert_update_final_rows",
            "exact_delete_keys",
            "key_after_identity",
            "marker_last",
            "repeated_update",
            "update_to_original",
            "insert_delete",
            "sql_null",
            "empty_string_to_null",
            "primary_key_update_delete_create",
            "bounded_connector_progress_and_drain",
        ],
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise SystemExit(f"Oracle capability verification failed: {error}")
