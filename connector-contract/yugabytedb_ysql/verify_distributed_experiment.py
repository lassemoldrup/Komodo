#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from verify_raw_contract import load_entries, verify_contract


CONTRACT_TABLES = tuple(f"contract_account_{suffix}" for suffix in "abcd")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def frames(path: Path) -> list[list[dict[str, Any]]]:
    result: list[list[dict[str, Any]]] = []
    active: list[dict[str, Any]] | None = None
    for entry in load_entries(path):
        value = entry["value"]
        if value.get("status") == "BEGIN":
            require(active is None, "nested BEGIN in experiment capture")
            active = []
        elif value.get("status") == "END":
            require(active is not None, "END outside frame in experiment capture")
            result.append(active)
            active = None
        else:
            require(active is not None, "row outside frame in experiment capture")
            active.append(value)
    require(active is None, "partial experiment frame")
    return result


def table_names(frame: list[dict[str, Any]]) -> list[str]:
    return [row["source"]["table"] for row in frame]


def marker(frame: list[dict[str, Any]]) -> tuple[str, int]:
    after = frame[-1]["after"]
    return after["session_id"], after["txn_id"]


def verify_slot_sequence(raw_frames: list[list[dict[str, Any]]]) -> list[int]:
    positions = []
    for frame in raw_frames:
        row_positions = [row["source"]["lsn"] for row in frame]
        require(row_positions == sorted(row_positions), "diagnostic LSN order regressed inside a frame")
        positions.append(max(row_positions))
    require(positions == sorted(positions) and len(set(positions)) == len(positions),
            "diagnostic SEQUENCE positions differ from Redis END order")
    return positions


def load_placement(path: Path) -> dict[str, Any]:
    placement = json.loads(path.read_text(encoding="utf-8"))
    require(placement.get("kind") == "yugabytedb_tablet_placement", "tablet placement kind differs")
    return placement


def verify_cross_tablet(raw_frames: list[list[dict[str, Any]]], placement: dict[str, Any]) -> dict[str, Any]:
    require(len(raw_frames) == 1, "cross-tablet frame count differs")
    require(table_names(raw_frames[0]) == [
        "usertable", "usertable", "checker_run_fence", "session_txn"
    ], "cross-tablet row order differs")
    hashes = placement["key_hashes"]
    require(hashes.get("usertable:1", 65536) < 0x8000, "key 1 is not on the low hash tablet")
    require(hashes.get("usertable:2", 0) >= 0x8000, "key 2 is not on the high hash tablet")
    require(len(placement["tables"]["usertable"]) == 2, "usertable does not have two tablets")
    return {"keys_on_distinct_usertable_tablets": True}


def verify_cross_table(raw_frames: list[list[dict[str, Any]]], placement: dict[str, Any]) -> dict[str, Any]:
    require(len(raw_frames) == 2, "cross-table experiment frame count differs")
    require(table_names(raw_frames[0]) == [
        "contract_account_a", "contract_account_b", "session_txn"
    ], "test-adapter cross-table frame differs")
    require(table_names(raw_frames[1]) == ["warehouse", "district", "session_txn"],
            "TPC-C cross-table frame differs")
    table_ids = {
        table: placement["tables"][table][0]["tablet_id"]
        for table in ("contract_account_a", "contract_account_b", "warehouse", "district", "session_txn")
    }
    require(len(set(table_ids.values())) == len(table_ids), "cross-table rows are not on distinct tablets")
    return {"distinct_tablet_ids": table_ids, "tpcc_cross_table_frame": True}


def verify_order(raw_frames: list[list[dict[str, Any]]], expected: list[tuple[str, int]]) -> dict[str, Any]:
    actual = [marker(frame) for frame in raw_frames]
    require(actual == expected, f"completed transaction order differs: {actual}")
    return {"completed_transaction_order": actual}


def verify_concurrent(raw_frames: list[list[dict[str, Any]]], placement: dict[str, Any]) -> dict[str, Any]:
    expected = [(f"concurrent_{value:02d}", 1) for value in range(1, 25)]
    verify_order(raw_frames, expected)
    counts = Counter(frame[0]["source"]["table"] for frame in raw_frames)
    require(counts == Counter({table: 6 for table in CONTRACT_TABLES}),
            f"four-tablet transaction counts differ: {counts}")
    tablet_ids = [placement["tables"][table][0]["tablet_id"] for table in CONTRACT_TABLES]
    require(len(set(tablet_ids)) == 4, "concurrent workload used fewer than four tablets")
    return {"transactions": 24, "tablets": 4, "per_table": dict(counts)}


def verify_large(raw_frames: list[list[dict[str, Any]]], source_batch: int, redis_batch: int) -> dict[str, Any]:
    require(len(raw_frames) == 1, "large transaction was split into multiple frames")
    counts = Counter(table_names(raw_frames[0]))
    expected = Counter({table: 64 for table in CONTRACT_TABLES})
    expected["session_txn"] = 1
    require(counts == expected, f"large transaction row counts differ: {counts}")
    require(len(raw_frames[0]) > source_batch and len(raw_frames[0]) > redis_batch,
            "large transaction did not exceed configured batch boundaries")
    return {"rows": len(raw_frames[0]), "source_batch_size": source_batch,
            "redis_batch_size": redis_batch, "single_frame": True}


def row_key(row: dict[str, Any], image: dict[str, Any]) -> int:
    return image["ycsb_key"]


def verify_write_shapes(raw_frames: list[list[dict[str, Any]]]) -> dict[str, Any]:
    require(len(raw_frames) == 2, "write-shape frame count differs")
    state: dict[int, dict[str, Any]] = {}
    operations: list[tuple[str, int, int | None]] = []
    for frame in raw_frames:
        for row in frame[:-1]:
            before = row["before"]
            after = row["after"]
            if row["op"] == "c":
                assert after is not None
                key = row_key(row, after)
                require(key not in state, f"insert predecessor exists for key {key}")
                state[key] = after
                operations.append(("c", key, None))
            elif row["op"] == "u":
                assert before is not None and after is not None
                old_key = row_key(row, before)
                new_key = row_key(row, after)
                require(state.get(old_key) == before, f"update predecessor differs for key {old_key}")
                del state[old_key]
                require(new_key not in state, f"updated key already exists: {new_key}")
                state[new_key] = after
                operations.append(("u", old_key, new_key))
            else:
                assert before is not None
                key = row_key(row, before)
                require(state.get(key) == before, f"delete predecessor differs for key {key}")
                del state[key]
                operations.append(("d", key, None))
    require(state[100]["field1"] == "insert-updated", "insert-update final value differs")
    require(state[10]["field1"] == "update-two", "update-update final value differs")
    require(state[20]["field1"] == "return-original", "update-to-original final value differs")
    require(200 not in state, "insert-delete key remains present")
    require(300 not in state and state[301]["field1"] == "primary-key-change",
            "primary-key change final state differs")
    return {"operation_chain": operations, "immediate_predecessors_validated": True}


def verify_conflicts(raw_frames: list[list[dict[str, Any]]], ledger_path: Path) -> dict[str, Any]:
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    require(ledger.get("serialization_failure_sqlstate") == "40001", "serialization SQLSTATE differs")
    require(ledger.get("deadlock_failure_sqlstate") == "40P01", "deadlock SQLSTATE differs")
    loser = ledger["deadlock_loser"]
    winner = ledger["deadlock_winner"]
    actual = [marker(frame) for frame in raw_frames]
    expected = [
        ("conflict_seed", 1),
        ("serialization_winner", 1),
        ("serialization_retry", 2),
        (winner, 1),
        (loser, 2),
    ]
    require(actual == expected, f"conflict committed marker ledger differs: {actual}")
    require(("serialization_retry", 1) not in actual and (loser, 1) not in actual,
            "aborted attempt emitted a committed marker")
    return {"committed_markers": actual, "aborted_attempts_emitted_no_transaction": True,
            "logical_session_order_retained": True}


def verify_queue(
    raw_frames: list[list[dict[str, Any]]],
    placement: dict[str, Any],
    queue_evidence_path: Path,
) -> dict[str, Any]:
    expected = [("queue_pressure", value) for value in range(1, 97)]
    verify_order(raw_frames, expected)
    counts = Counter(frame[0]["source"]["table"] for frame in raw_frames)
    require(counts == Counter({table: 24 for table in CONTRACT_TABLES}),
            f"queue-pressure table counts differ: {counts}")
    tablet_ids = {placement["tables"][table][0]["tablet_id"] for table in CONTRACT_TABLES}
    require(len(tablet_ids) == 4, "queue-pressure workload used fewer than four tablets")
    queue_evidence = json.loads(queue_evidence_path.read_text(encoding="utf-8"))
    require(
        queue_evidence.get("kind") == "debezium_connector_queue_saturation_evidence"
        and queue_evidence.get("status") == "PASSED"
        and queue_evidence.get("minimum_remaining_capacity") == 0
        and queue_evidence.get("saturated_samples", 0) > 0,
        "connector queue saturation was not measured",
    )
    return {
        "transactions": 96,
        "redis_write_pause": True,
        "queue_saturation_measurement": queue_evidence,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--placement", type=Path, required=True)
    parser.add_argument("--redis-batch-size", type=int, required=True)
    parser.add_argument("--source-batch-size", type=int, required=True)
    parser.add_argument("--attempt-ledger", type=Path)
    parser.add_argument("--queue-evidence", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()

    summary = verify_contract(args.capture, "pgoutput", args.redis_batch_size)
    raw_frames = frames(args.capture)
    placement = load_placement(args.placement)
    positions = verify_slot_sequence(raw_frames)
    scenario = args.scenario
    if scenario == "cross_tablet_marker_last":
        details = verify_cross_tablet(raw_frames, placement)
    elif scenario == "cross_table_transaction":
        details = verify_cross_table(raw_frames, placement)
    elif scenario == "commit_order_reversal":
        details = verify_order(raw_frames, [("commit_b", 1), ("commit_a", 1)])
    elif scenario == "commit_order_realtime":
        details = verify_order(raw_frames, [("realtime_a", 1), ("realtime_b", 1)])
    elif scenario == "concurrent_tablet_commits":
        details = verify_concurrent(raw_frames, placement)
    elif scenario == "large_transaction":
        details = verify_large(raw_frames, args.source_batch_size, args.redis_batch_size)
    elif scenario == "write_shape_matrix":
        details = verify_write_shapes(raw_frames)
    elif scenario == "conflict_retry_matrix":
        require(args.attempt_ledger is not None, "conflict attempt ledger is required")
        details = verify_conflicts(raw_frames, args.attempt_ledger)
    elif scenario == "redis_backpressure_reversal":
        details = verify_order(raw_frames, [
            ("backpressure_commit_b", 1),
            ("backpressure_commit_a", 1),
            ("backpressure_realtime_a", 1),
            ("backpressure_realtime_b", 1),
        ])
        details["redis_write_pause"] = True
    elif scenario == "queue_saturation_order":
        require(args.queue_evidence is not None, "queue saturation evidence is required")
        details = verify_queue(raw_frames, placement, args.queue_evidence)
    else:
        raise SystemExit(f"unsupported distributed experiment: {scenario}")

    report = {
        "kind": "yugabytedb_ysql_distributed_experiment",
        "scenario": scenario,
        "status": "PASSED",
        "raw_contract": summary,
        "diagnostic_sequence_positions_in_redis_end_order": positions,
        "diagnostic_positions_not_used_for_correlation_or_checking": True,
        "details": details,
    }
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
