#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / "protocol" / "tools"
sys.path.insert(0, str(TOOLS))

from bootstrap_adapters.ycsb import YcsbBootstrapAdapter  # noqa: E402


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-stream", type=Path, required=True)
    parser.add_argument("--bootstrap-scan", type=Path, required=True)
    parser.add_argument("--jdbc-capture", type=Path, required=True)
    parser.add_argument("--cpp-codec-replay", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()

    contract = json.loads(
        (ROOT / "protocol" / "adapters" / "ycsb.json").read_text(encoding="utf-8")
    )
    key_golden = json.loads(
        (ROOT / "protocol" / "golden" / "ycsb_keys.json").read_text(encoding="utf-8")
    )
    value_golden = json.loads(
        (ROOT / "protocol" / "golden" / "ycsb_values.json").read_text(encoding="utf-8")
    )
    keys = {vector["key"]: vector for vector in key_golden["vectors"]}
    values = {vector["name"]: vector for vector in value_golden["vectors"]}
    expected = {
        -2147483648: values["all_null"],
        42: values["live_sql_corpus"],
        2147483647: values["maximum_declared_varchar"],
    }

    capture = json.loads(args.redis_stream.read_text(encoding="utf-8"))
    decoded = [
        {
            "stream_id": entry[0],
            "key": json.loads(entry[1][0]),
            "value": json.loads(entry[1][1]),
        }
        for entry in capture
    ]
    frames: list[list[dict[str, object]]] = []
    active: list[dict[str, object]] | None = None
    for entry in decoded:
        value = entry["value"]
        status = value.get("status")
        if status == "BEGIN":
            require(active is None, "nested BEGIN in normalization capture")
            active = []
        elif status == "END":
            require(active is not None, "END without BEGIN in normalization capture")
            require(value["event_count"] == len(active), "END row count differs")
            frames.append(active)
            active = None
        else:
            require(active is not None, "row outside frame in normalization capture")
            active.append(entry)
    require(active is None, "normalization capture ends inside a frame")
    require(len(frames) == 2, "normalization capture must contain corpus and shape frames")
    for frame in frames:
        require(
            frame[-1]["value"]["source"]["table"] == "session_txn",
            "marker is not last",
        )
    rows = frames[0]
    application_rows = rows[:-1]
    require(len(application_rows) == 3, "application row count differs")
    for row in application_rows:
        key = row["key"]["ycsb_key"]
        require(key in expected, f"unexpected live YCSB key {key}")
        after = row["value"]["after"]
        require(after["ycsb_key"] == key, f"key/after mismatch for {key}")
        fields = [after[f"field{index}"] for index in range(1, 11)]
        require(fields == expected[key]["fields"], f"raw CDC fields differ for {key}")

    shape_rows = frames[1][:-1]
    updates_to_original = [
        row
        for row in shape_rows
        if row["value"]["source"]["table"] == "usertable"
        and row["value"]["op"] == "u"
        and row["value"]["before"]["ycsb_key"] == 42
        and row["value"]["after"]["ycsb_key"] == 42
    ]
    require(len(updates_to_original) == 2, "update-to-original raw CDC shape differs")
    require(
        updates_to_original[0]["value"]["after"]["field3"] == "changed"
        and updates_to_original[1]["value"]["after"]["field3"] == "A",
        "update-to-original raw CDC chain differs",
    )
    inserted_seven = any(
        row["value"]["op"] == "c" and row["value"]["after"]["ycsb_key"] == 7
        for row in shape_rows
    )
    deleted_seven = any(
        row["value"]["op"] == "d" and row["value"]["before"]["ycsb_key"] == 7
        for row in shape_rows
    )
    require(inserted_seven and deleted_seven, "insert-delete raw CDC shape differs")
    primary_key_update = any(
        row["value"]["op"] == "u"
        and row["value"]["before"]["ycsb_key"] == 100
        and row["value"]["after"]["ycsb_key"] == 101
        for row in shape_rows
    )
    primary_key_delete_insert = any(
        row["value"]["op"] == "d" and row["value"]["before"]["ycsb_key"] == 100
        for row in shape_rows
    ) and any(
        row["value"]["op"] == "c" and row["value"]["after"]["ycsb_key"] == 101
        for row in shape_rows
    )
    require(
        primary_key_update or primary_key_delete_insert,
        "primary-key-change raw CDC shape differs",
    )

    bootstrap_rows, counts = YcsbBootstrapAdapter().encode_scan(
        contract,
        args.bootstrap_scan,
        max_rows=100,
        max_input_bytes=1_000_000,
        max_canonical_bytes=1_000_000,
        options={"schema": "benchbase", "table": "usertable"},
    )
    actual = {key: value for key, value in bootstrap_rows}
    require(counts == {"benchbase.usertable": 3}, "bootstrap count differs")
    for key_number, value in expected.items():
        key_bytes = base64.b64decode(keys[key_number]["base64"], validate=True)
        value_bytes = base64.b64decode(value["base64"], validate=True)
        require(actual.get(key_bytes) == value_bytes, f"bootstrap bytes differ for {key_number}")

    jdbc_document = json.loads(args.jdbc_capture.read_text(encoding="utf-8"))
    require(jdbc_document.get("backend") == "yugabytedb_ysql", "JDBC backend differs")
    require(jdbc_document.get("table") == "benchbase.usertable", "JDBC table differs")
    jdbc_rows = {row["key"]: row for row in jdbc_document.get("rows", [])}
    require(set(jdbc_rows) == set(expected), "JDBC key roster differs")
    for key_number, value in expected.items():
        require(
            jdbc_rows[key_number]["key_base64"] == keys[key_number]["base64"],
            f"JDBC key bytes differ for {key_number}",
        )
        require(
            jdbc_rows[key_number]["value_base64"] == value["base64"],
            f"JDBC value bytes differ for {key_number}",
        )

    cpp = json.loads(args.cpp_codec_replay.read_text(encoding="utf-8"))
    require(cpp.get("completed_transactions") == 2, "C++ replay frame count differs")
    cpp_frames = cpp.get("frames")
    require(isinstance(cpp_frames, list) and len(cpp_frames) == 2, "C++ replay frames differ")
    corpus_events = cpp_frames[0]["application_events"]
    require(len(corpus_events) == 3, "C++ corpus event count differs")
    corpus_by_key = {event["event_key_hex"]: event for event in corpus_events}
    for key_number, value in expected.items():
        key_bytes = base64.b64decode(keys[key_number]["base64"], validate=True)
        value_bytes = base64.b64decode(value["base64"], validate=True)
        event = corpus_by_key.get(key_bytes.hex())
        require(event is not None, f"C++ corpus key is missing for {key_number}")
        require(event["before"] == {"state": "absent"}, f"C++ insert before differs for {key_number}")
        require(event["after"]["key_hex"] == key_bytes.hex(), f"C++ image key differs for {key_number}")
        require(event["after"]["value_hex"] == value_bytes.hex(), f"C++ value differs for {key_number}")

    shape_events = cpp_frames[1]["application_events"]
    require(
        cpp.get("application_events") == len(corpus_events) + len(shape_events),
        "C++ application event total differs",
    )
    key_42 = YcsbBootstrapAdapter._key("benchbase", "usertable", 42).hex()
    key_7 = YcsbBootstrapAdapter._key("benchbase", "usertable", 7).hex()
    key_100 = YcsbBootstrapAdapter._key("benchbase", "usertable", 100).hex()
    key_101 = YcsbBootstrapAdapter._key("benchbase", "usertable", 101).hex()
    cpp_updates = [
        event
        for event in shape_events
        if event["operation"] == "u" and event["event_key_hex"] == key_42
    ]
    require(len(cpp_updates) == 2, "C++ update-to-original event count differs")
    require(
        cpp_updates[-1]["after"]["value_hex"]
        == base64.b64decode(values["live_sql_corpus"]["base64"], validate=True).hex(),
        "C++ update-to-original final value differs",
    )
    require(
        any(event["operation"] == "c" and event["after"].get("key_hex") == key_7 for event in shape_events)
        and any(event["operation"] == "d" and event["before"].get("key_hex") == key_7 for event in shape_events),
        "C++ insert-delete shape differs",
    )
    require(
        any(
            event["before"].get("key_hex") == key_100
            and event["after"].get("key_hex") == key_101
            for event in shape_events
        )
        or (
            any(event["operation"] == "d" and event["before"].get("key_hex") == key_100 for event in shape_events)
            and any(event["operation"] == "c" and event["after"].get("key_hex") == key_101 for event in shape_events)
        ),
        "C++ primary-key-change shape differs",
    )

    report = {
        "gate": "ycsb_value_normalization",
        "status": "PASSED",
        "database_rows": len(application_rows),
        "raw_cdc_rows": len(application_rows),
        "bootstrap_rows": len(bootstrap_rows),
        "jdbc_rows": len(jdbc_rows),
        "cpp_corpus_rows": len(corpus_events),
        "cpp_shape_events": len(shape_events),
        "update_to_original_validated": True,
        "insert_delete_validated": True,
        "primary_key_change_validated": True,
        "keys": sorted(expected),
        "redis_stream_sha256": sha256(args.redis_stream),
        "bootstrap_scan_sha256": sha256(args.bootstrap_scan),
        "jdbc_capture_sha256": sha256(args.jdbc_capture),
        "cpp_codec_replay_sha256": sha256(args.cpp_codec_replay),
        "golden_keys_sha256": sha256(ROOT / "protocol" / "golden" / "ycsb_keys.json"),
        "golden_values_sha256": sha256(ROOT / "protocol" / "golden" / "ycsb_values.json"),
    }
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
