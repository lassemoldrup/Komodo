#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from verify_distributed_experiment import frames, marker, table_names, verify_slot_sequence
from verify_raw_contract import load_entries, verify_contract


TABLET = re.compile(r"^([0-9a-f]{32})\s+.+?\s+(\S+:\d+)\s+([0-9a-f]{32})$")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def tablets(path: Path) -> list[dict[str, str]]:
    result = []
    for line in path.read_text(encoding="utf-8").splitlines()[1:]:
        match = TABLET.fullmatch(line.strip())
        if match is not None:
            result.append({"tablet_id": match.group(1), "leader": match.group(2),
                           "leader_id": match.group(3)})
    require(bool(result), f"tablet evidence is empty: {path}")
    return result


def verify_transition_overlap(timeline: dict[str, Any], name: str) -> dict[str, int]:
    transition = timeline.get(name)
    require(isinstance(transition, dict), f"{name} transition timeline is absent")
    required = {
        "transaction_started_ns",
        "operation_started_ns",
        "command_returned_ns",
        "transaction_commit_ack_ns",
        "topology_completed_ns",
    }
    require(set(transition) == required, f"{name} transition timeline fields differ")
    require(all(type(transition[field]) is int and transition[field] > 0 for field in required),
            f"{name} transition timeline contains a nonpositive timestamp")
    require(
        transition["transaction_started_ns"]
        < transition["operation_started_ns"]
        < transition["transaction_commit_ack_ns"],
        f"{name} topology operation did not overlap the transaction",
    )
    require(
        transition["command_returned_ns"] >= transition["operation_started_ns"]
        and transition["topology_completed_ns"] >= transition["operation_started_ns"],
        f"{name} topology completion precedes operation start",
    )
    return transition


def verify_provenance(root: Path) -> str:
    lock = json.loads((root / "provenance.lock.json").read_text(encoding="utf-8"))
    artifacts = lock["artifacts"]
    expected_image_id = artifacts["connector_contract_image"]["local_image_id"]
    image = json.loads((root / "connector-contract-image.json").read_text(encoding="utf-8"))
    require(isinstance(image, list) and len(image) == 1
            and image[0].get("Id") == expected_image_id,
            "resilience connector image differs from locked provenance")
    container = json.loads(
        (root / "container-resilience-contract-debezium-server.json").read_text(
            encoding="utf-8"
        )
    )
    require(isinstance(container, list) and len(container) == 1
            and container[0].get("Image") == expected_image_id,
            "resilience connector container image differs from locked provenance")
    classpath = (root / "connector-classpath.txt").read_text(encoding="utf-8")
    require(artifacts["connector"]["sha256"] in classpath,
            "resilience connector JAR checksum differs from locked provenance")
    require("YugabyteDBConnector.class" in classpath
            and "PostgresConnector.class" not in classpath,
            "resilience connector classpath identity differs")
    return expected_image_id


def exact_expected_state() -> dict[tuple[str, int], str]:
    expected = {
        ("contract_account_a", value): "x" * 96 + str(value).zfill(4)
        for value in range(1, 8193)
    }
    expected[("contract_account_a", 9000)] = "resilience-2"
    expected[("contract_account_b", 3)] = "resilience-3"
    expected[("contract_account_c", 4)] = "resilience-4"
    return expected


def sql_state(path: Path) -> dict[tuple[str, int], str]:
    result: dict[tuple[str, int], str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        fields = line.rstrip("\r").split("\t")
        require(len(fields) == 3, f"malformed final SQL state row: {line!r}")
        key = (fields[0], int(fields[1]))
        require(key not in result, f"duplicate final SQL state key: {key}")
        result[key] = fields[2]
    return result


def cdc_state(raw_frames: list[list[dict[str, Any]]]) -> dict[tuple[str, int], str]:
    result: dict[tuple[str, int], str] = {}
    for frame in raw_frames[:4]:
        for row in frame[:-1]:
            table = row["source"]["table"]
            after = row["after"]
            require(table.startswith("contract_account_") and row["op"] == "c"
                    and isinstance(after, dict),
                    "resilience application CDC operation differs")
            key = (table, after["account_id"])
            require(key not in result, f"duplicate resilience CDC key: {key}")
            result[key] = after["payload"]
    return result


def verify_closing_boundary(closing: dict[str, Any], expected_entries: int) -> dict[str, Any]:
    require(closing.get("kind") == "yugabytedb_ysql_resilience_closing_boundary",
            "resilience closing-boundary kind differs")
    require(
        closing.get("stream_length_before") == closing.get("stream_length_after")
        == expected_entries
        and closing.get("end_count_before") == closing.get("end_count_after") == 5,
        "resilience stream changed after the closing fence",
    )
    require(
        closing["stabilization_completed_ns"] - closing["stabilization_started_ns"]
        >= 5_000_000_000
        and closing["connector_stopped_ns"] >= closing["stabilization_completed_ns"],
        "resilience closing stabilization interval differs",
    )
    return closing


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    root = args.directory
    capture = root / "redis-stream.json"

    summary = verify_contract(capture, "pgoutput", 4)
    raw_frames = frames(capture)
    require([marker(frame) for frame in raw_frames]
            == [("resilience_transition", value) for value in range(1, 6)],
            "resilience marker order differs")
    observed_tables = [table_names(frame) for frame in raw_frames]
    require(
        len(observed_tables[0]) == 8193
        and observed_tables[0][:-1] == ["contract_account_a"] * 8192
        and observed_tables[0][-1] == "session_txn"
        and observed_tables[1:] == [
            ["contract_account_a", "session_txn"],
            ["contract_account_b", "session_txn"],
            ["contract_account_c", "session_txn"],
            ["checker_run_fence", "session_txn"],
        ],
        "resilience transaction table sequence differs",
    )
    closing_fence = raw_frames[-1][0]["after"]
    require(
        closing_fence
        == {
            "run_id": "00000000-0000-0000-0000-000000000010",
            "phase": "END_FENCE",
            "nonce": "00000000-0000-0000-0000-000000000011",
        },
        "resilience closing fence differs",
    )
    entries = load_entries(capture)
    require(entries[-1]["value"].get("status") == "END",
            "resilience closing END is not the final Redis entry")

    expected_state = exact_expected_state()
    observed_cdc_state = cdc_state(raw_frames)
    observed_sql_state = sql_state(root / "database-state-after-loss.tsv")
    require(observed_cdc_state == expected_state,
            "resilience CDC keys or payloads differ from the exact corpus")
    require(observed_sql_state == expected_state,
            "resilience final SQL keys or payloads differ from the exact corpus")
    require(observed_cdc_state == observed_sql_state,
            "resilience CDC and final SQL state differ")
    positions = verify_slot_sequence(raw_frames)

    timeline = json.loads((root / "transition-timeline.json").read_text(encoding="utf-8"))
    require(timeline.get("kind") == "yugabytedb_ysql_resilience_transition_timeline",
            "resilience transition timeline kind differs")
    overlap = {
        name: verify_transition_overlap(timeline, name)
        for name in ("split", "leader_movement", "tserver_loss")
    }

    split_before = tablets(root / "split-before.txt")
    split_after = tablets(root / "split-after.txt")
    require(len(split_before) == 1 and len(split_after) == 2,
            "tablet split did not change one tablet into two")
    leader_before = tablets(root / "leader-before.txt")
    leader_after = tablets(root / "leader-after.txt")
    require(len(leader_before) == len(leader_after) == 1
            and leader_before[0]["leader_id"] != leader_after[0]["leader_id"],
            "tablet leader did not move")
    loss_before = tablets(root / "loss-before.txt")
    loss_after = tablets(root / "loss-after.txt")
    node_addresses = json.loads((root / "node-addresses.json").read_text())
    stopped_address = node_addresses["yb_node_3"] + ":9100"
    require(loss_before[0]["leader"] == stopped_address,
            "loss target was not the tablet leader")
    require(loss_after[0]["leader"] != stopped_address,
            "tablet leader did not leave the stopped TServer")

    connector_output = json.loads((root / "connector-output-order.json").read_text())
    commit_order = json.loads((root / "commit-order.json").read_text())
    require(connector_output["completed_transactions"] == commit_order["completed_transactions"]
            == [{"session_id": "resilience_transition", "txn_id": value}
                for value in range(1, 6)],
            "resilience connector/Redis/client completion order differs")

    closing = verify_closing_boundary(
        json.loads((root / "closing-boundary.json").read_text(encoding="utf-8")),
        summary["entries"],
    )

    database = (root / "database-state-after-loss.txt").read_text(encoding="utf-8")
    require(re.search(r"resilience_transition\s*\|\s*5", database) is not None,
            "post-loss database marker state differs")
    require(re.search(r"contract_account_c\s*\|\s*1", database) is not None,
            "post-loss application state differs")
    require("END_FENCE" in database, "resilience closing fence is absent from SQL state")
    expected_image_id = verify_provenance(root)

    report: dict[str, Any] = {
        "kind": "yugabytedb_ysql_resilience_experiment",
        "status": "PASSED",
        "scope": "diagnostic_no_restart_evidence",
        "topology_operations_overlapped_transactions": overlap,
        "tablet_split": {"before": len(split_before), "after": len(split_after)},
        "leader_movement": {"before": leader_before[0], "after": leader_after[0]},
        "tserver_loss": {"stopped": "resilience-contract-yb-3",
                         "leader_after_loss": loss_after[0]},
        "raw_contract": summary,
        "exact_application_rows": len(expected_state),
        "exact_cdc_sql_state_equal": True,
        "closing_boundary": closing,
        "locked_connector_image_id": expected_image_id,
        "diagnostic_sequence_positions_in_redis_end_order": positions,
        "silent_omission_reordering_or_deduplication": False,
        "consistency_verdict_claimed": False,
    }
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
