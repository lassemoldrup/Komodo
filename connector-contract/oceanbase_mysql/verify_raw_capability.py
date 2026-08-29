#!/usr/bin/env python3
import argparse
import json
from collections import Counter
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    evidence = parser.parse_args().evidence.resolve()
    failures: list[str] = []
    if (evidence / "startup-catalog-attestation.txt").read_bytes() != (
        evidence / "final-catalog-attestation.txt"
    ).read_bytes():
        failures.append("startup and final catalog attestations differ")
    entries = json.loads((evidence / "raw-redis-cdc.json").read_text(encoding="utf-8"))
    frames: list[dict[str, object]] = []
    active: dict[str, object] | None = None

    for stream_id, fields in entries:
        key = json.loads(fields[0])
        value = json.loads(fields[1])
        status = value.get("status")
        if status == "BEGIN":
            if active is not None:
                failures.append(f"nested BEGIN at {stream_id}")
            active = {"id": value.get("id"), "rows": [], "begin": stream_id}
        elif status == "END":
            if active is None:
                failures.append(f"END outside frame at {stream_id}")
                continue
            rows = active["rows"]
            if value.get("id") != active["id"]:
                failures.append(f"END identity mismatch at {stream_id}")
            if value.get("event_count") != len(rows):
                failures.append(f"END event_count mismatch at {stream_id}")
            observed_counts = Counter(row["source"]["db"] + "." + row["source"]["table"] for _, row in rows)
            declared_counts = Counter(
                {item["data_collection"]: item["event_count"] for item in value.get("data_collections") or []}
            )
            if observed_counts != declared_counts:
                failures.append(f"END table counts mismatch at {stream_id}")
            active["end"] = stream_id
            active["end_value"] = value
            frames.append(active)
            active = None
        else:
            if active is None:
                failures.append(f"row outside frame at {stream_id}")
                continue
            transaction = value.get("transaction") or {}
            rows = active["rows"]
            if transaction.get("id") != active["id"]:
                failures.append(f"row transaction identity mismatch at {stream_id}")
            if transaction.get("total_order") != len(rows) + 1:
                failures.append(f"non-dense total_order at {stream_id}")
            source = value.get("source") or {}
            if source.get("version") != "3.6.1.Final" or source.get("connector") != "mysql":
                failures.append(f"source identity mismatch at {stream_id}")
            if source.get("snapshot") != "false":
                failures.append(f"snapshot row in workload at {stream_id}")
            operation = value.get("op")
            if operation in {"c", "u"}:
                after = value.get("after")
                if not isinstance(after, dict) or not after:
                    failures.append(f"incomplete final row at {stream_id}")
                elif any(after.get(name) != expected for name, expected in key.items()):
                    failures.append(f"key/after mismatch at {stream_id}")
            elif operation == "d":
                if value.get("after") is not None or not key:
                    failures.append(f"incomplete delete at {stream_id}")
            else:
                failures.append(f"unsupported operation at {stream_id}")
            rows.append((key, value))

    if active is not None:
        failures.append("unterminated final frame")

    frames_by_marker: dict[str, dict[str, object]] = {}
    fence_frames: dict[str, dict[str, object]] = {}
    for frame in frames:
        rows = frame["rows"]
        marker_rows = [row for row in rows if row[1]["source"]["table"] == "session_txn"]
        fence_rows = [row for row in rows if row[1]["source"]["table"] == "checker_run_fence"]
        if marker_rows:
            if len(marker_rows) != 1 or rows[-1] is not marker_rows[0]:
                failures.append(f"marker is not one final row in frame {frame['id']}")
            else:
                frames_by_marker[marker_rows[0][0]["session_id"]] = frame
        elif fence_rows:
            if len(rows) != 1 or len(fence_rows) != 1:
                failures.append(f"mixed fence frame {frame['id']}")
            else:
                fence_frames[fence_rows[0][0]["phase"]] = frame
        else:
            failures.append(f"unclassified frame {frame['id']}")

    expected_markers = {
        "shape/marker-only", "shape/insert", "shape/repeated-update",
        "shape/update-to-original", "shape/insert-delete", "shape/primary-key-update",
        "shape/delete", "shape/large-transaction", "order/commit-a-after-b",
        "order/commit-b-before-a", "order/realtime-a", "order/realtime-b",
        *(f"order/concurrent-{index}" for index in range(1, 9)),
        *(f"order/backpressure-{index}" for index in range(1, 9)),
    }
    if set(frames_by_marker) != expected_markers:
        failures.append("marker roster mismatch")
    if set(fence_frames) != {"RUN_START", "END_FENCE"}:
        failures.append("fence roster mismatch")

    def application_rows(marker: str) -> list[tuple[dict, dict]]:
        frame = frames_by_marker.get(marker)
        if not frame:
            return []
        return [row for row in frame["rows"] if row[1]["source"]["table"] == "usertable"]

    inserted = application_rows("shape/insert")
    if len(inserted) != 1:
        failures.append("insert shape row count mismatch")
    else:
        after = inserted[0][1].get("after") or {}
        if after.get("field1") is not None or after.get("field2") != "" or after.get("field4") != "汉字" or after.get("field5") != "🙂":
            failures.append("NULL/empty/UTF-8 insert values changed")
    if [row[1].get("op") for row in application_rows("shape/repeated-update")] != ["u", "u"]:
        failures.append("repeated update shape mismatch")
    if [row[1].get("op") for row in application_rows("shape/update-to-original")] != ["u", "u"]:
        failures.append("update-to-original shape mismatch")
    if [row[1].get("op") for row in application_rows("shape/insert-delete")] != ["c", "d"]:
        failures.append("insert-delete shape mismatch")
    pk_rows = application_rows("shape/primary-key-update")
    if [row[1].get("op") for row in pk_rows] != ["d", "c"] or [row[0].get("ycsb_key") for row in pk_rows] != [1, 101]:
        failures.append("primary-key update was not old-key delete plus new-key create")
    deleted = application_rows("shape/delete")
    if len(deleted) != 1 or deleted[0][1].get("op") != "d" or deleted[0][0].get("ycsb_key") != 101:
        failures.append("delete shape mismatch")
    if len(application_rows("shape/large-transaction")) != 20:
        failures.append("large transaction row count mismatch")

    frame_order = {marker: index for index, frame in enumerate(frames) for marker, candidate in frames_by_marker.items() if candidate is frame}
    if frame_order.get("order/commit-b-before-a", 10**9) >= frame_order.get("order/commit-a-after-b", -1):
        failures.append("B-before-A commit schedule reversed in CDC")
    if frame_order.get("order/realtime-a", 10**9) >= frame_order.get("order/realtime-b", -1):
        failures.append("real-time A-before-B order reversed in CDC")
    if frames and frames[-1] is not fence_frames.get("END_FENCE"):
        failures.append("END_FENCE is not the final frame")

    result = {
        "kind": "oceanbase_mysql_raw_capability_verification",
        "passed": not failures,
        "redis_entries": len(entries),
        "completed_frames": len(frames),
        "workload_frames": len(frames_by_marker),
        "fence_frames": len(fence_frames),
        "failures": failures,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
