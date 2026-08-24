#!/usr/bin/env python3
"""Verify Java/C++/bootstrap TPC-C canonical byte identity on one live capture."""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / "protocol" / "tools"
sys.path.insert(0, str(TOOLS))

from bootstrap_adapters.tpcc import TpccBootstrapAdapter  # noqa: E402
from tpcc_codec import TpccCodec  # noqa: E402


EXPECTED_FINAL_COUNTS = {
    "benchbase.warehouse": 1,
    "benchbase.item": 2,
    "benchbase.stock": 1,
    "benchbase.district": 1,
    "benchbase.customer": 1,
    "benchbase.history": 1,
    "benchbase.oorder": 2,
    "benchbase.new_order": 1,
    "benchbase.order_line": 2,
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_frames(path: Path) -> list[list[dict[str, object]]]:
    capture = json.loads(path.read_text(encoding="utf-8"))
    frames: list[list[dict[str, object]]] = []
    active: list[dict[str, object]] | None = None
    for entry in capture:
        value = json.loads(entry[1][1])
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
            active.append({"key": json.loads(entry[1][0]), "value": value})
    require(active is None, "normalization capture ends inside a frame")
    return frames


def float_bits(value: object) -> int:
    require(isinstance(value, (int, float)) and not isinstance(value, bool),
            "raw float32 is not a JSON number")
    return struct.unpack(">I", struct.pack(">f", float(value)))[0]


def validate_corpus_boundaries(rows: list[dict[str, object]]) -> None:
    by_table: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        by_table.setdefault(row["value"]["source"]["table"], []).append(row)

    warehouse = by_table["warehouse"][0]["value"]["after"]
    require(warehouse["w_ytd"] == "300000.00", "warehouse trailing-zero decimal differs")
    require(warehouse["w_tax"] == "0.1234", "warehouse scale-4 decimal differs")
    # The pinned connector emits bpchar values with trailing padding
    # stripped; the frozen char_read_semantics=pad_to_declared_length
    # contract restores the canonical width in every codec.
    require(warehouse["w_state"] == "C", "warehouse CHAR(2) raw emission differs")
    require(warehouse["w_name"] == "仓库  ", "warehouse UTF-8 with trailing spaces differs")
    require(warehouse["w_street_1"] == "", "warehouse empty VARCHAR differs")

    district = by_table["district"][0]["value"]["after"]
    require(district["d_tax"] == "0.0000", "district zero scale-4 decimal differs")

    customer = by_table["customer"][0]["value"]["after"]
    require(customer["c_balance"] == "-10.00", "customer negative decimal differs")
    require(customer["c_since"] == "2000-01-01T00:00:00Z", "customer timestamptz text differs")
    require(float_bits(customer["c_ytd_payment"]) == 0x3DCCCCCD,
            "customer float32 bits differ")
    require(customer["c_discount"] == "0.9999", "customer boundary discount differs")

    history = by_table["history"][0]["value"]["after"]
    require(isinstance(history["h_id"], int) and history["h_id"] >= 1,
            "generated history identity differs")

    carriers = sorted(
        (row["value"]["after"]["o_id"], row["value"]["after"]["o_carrier_id"])
        for row in by_table["oorder"]
    )
    require(carriers == [(2999, 5), (3000, None)], "nullable carrier coverage differs")

    new_order = by_table["new_order"][0]["value"]["after"]
    require(set(new_order) == {"no_w_id", "no_d_id", "no_o_id"},
            "new_order empty value shape differs")

    deliveries = sorted(
        (row["value"]["after"]["ol_number"], row["value"]["after"]["ol_delivery_d"])
        for row in by_table["order_line"]
    )
    require(deliveries == [(1, None), (2, "2026-01-02T03:04:05Z")],
            "nullable delivery coverage differs")
    amounts = sorted(
        row["value"]["after"]["ol_amount"] for row in by_table["order_line"]
    )
    require(amounts == ["0.00", "9999.99"], "order_line boundary amounts differ")


def cross_check_python_codec(
    codec: TpccCodec,
    frames: list[list[dict[str, object]]],
    cpp_frames: list[dict[str, object]],
) -> int:
    checked = 0
    for frame, cpp_frame in zip(frames, cpp_frames, strict=True):
        application_rows = [
            row for row in frame
            if row["value"]["source"]["table"] not in ("session_txn", "checker_run_fence")
        ]
        events = cpp_frame["application_events"]
        require(len(application_rows) == len(events),
                "C++ replay event count differs from raw application rows")
        for row, event in zip(application_rows, events, strict=True):
            table = "benchbase." + row["value"]["source"]["table"]
            require(event["table"] == table, "C++ replay table differs")
            require(event["operation"] == row["value"]["op"], "C++ replay operation differs")
            for side in ("before", "after"):
                image = row["value"][side]
                rendered = event[side]
                if image is None:
                    require(rendered == {"state": "absent"},
                            f"C++ {side} image presence differs for {table}")
                    continue
                key, value = codec.encode_row(table, image)
                require(rendered.get("state") == "present"
                        and rendered.get("key_hex") == key.hex()
                        and rendered.get("value_hex") == value.hex(),
                        f"C++ {side} canonical bytes differ for {table}")
            checked += 1
    return checked


def final_state_from_events(cpp_frames: list[dict[str, object]]) -> dict[str, str]:
    state: dict[str, str] = {}
    for frame in cpp_frames:
        for event in frame["application_events"]:
            before = event["before"]
            after = event["after"]
            if before.get("state") == "present" and (
                after.get("state") != "present" or after["key_hex"] != before["key_hex"]
            ):
                require(before["key_hex"] in state, "delete/key-change predecessor missing")
                del state[before["key_hex"]]
            if after.get("state") == "present":
                state[after["key_hex"]] = after["value_hex"]
    return state


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-stream", type=Path, required=True)
    parser.add_argument("--bootstrap-scan", type=Path, required=True)
    parser.add_argument("--jdbc-capture", type=Path, required=True)
    parser.add_argument("--cpp-codec-replay", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()

    contract = json.loads(
        (ROOT / "protocol" / "adapters" / "tpcc.json").read_text(encoding="utf-8")
    )
    codec = TpccCodec(contract)

    frames = parse_frames(args.redis_stream)
    require(len(frames) == 2, "normalization capture must contain corpus and shape frames")
    for frame in frames:
        require(frame[-1]["value"]["source"]["table"] == "session_txn", "marker is not last")
    validate_corpus_boundaries(frames[0][:-1])

    cpp = json.loads(args.cpp_codec_replay.read_text(encoding="utf-8"))
    require(cpp.get("completed_transactions") == 2, "C++ replay frame count differs")
    cpp_frames = cpp["frames"]
    checked_events = cross_check_python_codec(codec, frames, cpp_frames)

    shape_events = cpp_frames[1]["application_events"]
    stock_updates = [event for event in shape_events
                     if event["table"] == "benchbase.stock" and event["operation"] == "u"]
    require(len(stock_updates) == 2, "update-update shape differs")
    require(stock_updates[1]["after"]["value_hex"]
            == stock_updates[0]["before"]["value_hex"],
            "update-to-original shape differs")
    key_change_pairs = [
        (event["before"]["key_hex"], event["after"]["key_hex"])
        for event in shape_events
        if event["operation"] == "u"
        and event["before"].get("state") == "present"
        and event["after"].get("state") == "present"
        and event["before"]["key_hex"] != event["after"]["key_hex"]
    ]
    item_inserts = {event["after"]["key_hex"] for event in shape_events
                    if event["table"] == "benchbase.item" and event["operation"] == "c"}
    item_deletes = {event["before"]["key_hex"] for event in shape_events
                    if event["table"] == "benchbase.item" and event["operation"] == "d"}
    key_600 = codec.encode_key("benchbase.item", {"i_id": 600}).hex()
    key_500 = codec.encode_key("benchbase.item", {"i_id": 500}).hex()
    key_501 = codec.encode_key("benchbase.item", {"i_id": 501}).hex()
    require(key_600 in item_inserts and key_600 in item_deletes,
            "insert-delete shape differs")
    # The pinned connector emits a primary-key change either as one update with
    # differing key images or as an old-key delete plus a new-key insert.
    require(
        (key_500, key_501) in key_change_pairs
        or (key_500 in item_deletes and key_501 in item_inserts),
        "primary-key change shape differs",
    )

    final_state = final_state_from_events(cpp_frames)
    require(len(final_state) == sum(EXPECTED_FINAL_COUNTS.values()),
            "final state row count differs")

    bootstrap_rows, counts = TpccBootstrapAdapter().encode_scan(
        contract,
        args.bootstrap_scan,
        max_rows=1000,
        max_input_bytes=10_000_000,
        max_canonical_bytes=10_000_000,
        options={},
    )
    require(counts == EXPECTED_FINAL_COUNTS, "bootstrap per-table counts differ")
    bootstrap_state = {key.hex(): value.hex() for key, value in bootstrap_rows}
    require(bootstrap_state == final_state,
            "bootstrap canonical bytes differ from normalized CDC final state")

    jdbc_document = json.loads(args.jdbc_capture.read_text(encoding="utf-8"))
    require(jdbc_document.get("backend") == "yugabytedb_ysql", "JDBC backend differs")
    jdbc_state: dict[str, str] = {}
    jdbc_counts: dict[str, int] = {}
    for table in jdbc_document["tables"]:
        jdbc_counts[table["table"]] = len(table["rows"])
        for row in table["rows"]:
            require(row["key_hex"] not in jdbc_state, "JDBC capture repeats a canonical key")
            jdbc_state[row["key_hex"]] = row["value_hex"]
    require(jdbc_counts == EXPECTED_FINAL_COUNTS, "JDBC per-table counts differ")
    require(jdbc_state == final_state,
            "JDBC canonical bytes differ from normalized CDC final state")

    report = {
        "gate": "tpcc_value_normalization",
        "status": "PASSED",
        "cdc_frames": len(frames),
        "cross_checked_events": checked_events,
        "final_state_rows": len(final_state),
        "bootstrap_rows": len(bootstrap_rows),
        "jdbc_rows": len(jdbc_state),
        "per_table_counts": EXPECTED_FINAL_COUNTS,
        "update_to_original_validated": True,
        "insert_delete_validated": True,
        "primary_key_change_validated": True,
        "redis_stream_sha256": sha256(args.redis_stream),
        "bootstrap_scan_sha256": sha256(args.bootstrap_scan),
        "jdbc_capture_sha256": sha256(args.jdbc_capture),
        "cpp_codec_replay_sha256": sha256(args.cpp_codec_replay),
        "adapter_contract_sha256": sha256(ROOT / "protocol" / "adapters" / "tpcc.json"),
    }
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("TPC-C value normalization gate passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
