"""Independent raw-evidence checks shared by YugabyteDB YSQL acceptance gates."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def redis_records(path: Path, *, named_payload: bool) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for entry in load_json(path):
        require(isinstance(entry, list) and len(entry) == 2, f"malformed Redis entry: {path}")
        require(isinstance(entry[0], str), f"malformed Redis stream ID: {path}")
        fields = entry[1]
        require(isinstance(fields, list) and len(fields) == 2, f"malformed Redis fields: {path}")
        require(all(isinstance(field, str) for field in fields), f"non-string Redis field: {path}")
        if named_payload:
            require(fields[0] == "payload", f"wrong Redis payload field: {path}")
        value = json.loads(fields[1])
        require(isinstance(value, dict), f"Redis value is not an object: {path}")
        result.append(value)
    return result


def cdc_frames(path: Path) -> list[list[dict[str, Any]]]:
    frames: list[list[dict[str, Any]]] = []
    active: list[dict[str, Any]] | None = None
    for record in redis_records(path, named_payload=False):
        status = record.get("status")
        if status == "BEGIN":
            require(active is None, "nested CDC BEGIN")
            active = []
            continue
        if status == "END":
            require(active is not None, "CDC END without BEGIN")
            require(record.get("event_count") == len(active), "CDC END event_count differs")
            expected_counts = Counter(
                f"{row['source']['schema']}.{row['source']['table']}" for row in active
            )
            collections = record.get("data_collections")
            require(isinstance(collections, list), "CDC END collections are absent")
            observed_counts: dict[str, int] = {}
            for item in collections:
                require(
                    isinstance(item, dict)
                    and set(item) == {"data_collection", "event_count"}
                    and isinstance(item["data_collection"], str)
                    and type(item["event_count"]) is int
                    and item["event_count"] > 0,
                    "CDC END collection is malformed",
                )
                require(item["data_collection"] not in observed_counts, "duplicate CDC END collection")
                observed_counts[item["data_collection"]] = item["event_count"]
            require(observed_counts == dict(expected_counts), "CDC END collection counts differ")
            frames.append(active)
            active = None
            continue
        require(active is not None, "CDC row outside a frame")
        require(
            isinstance(record.get("source"), dict)
            and isinstance(record.get("transaction"), dict),
            "CDC row metadata is malformed",
        )
        transaction = record["transaction"]
        require(transaction.get("total_order") == len(active) + 1, "CDC total_order is not dense")
        table = record["source"].get("table")
        require(isinstance(table, str) and table, "CDC table identity is malformed")
        collection_order = sum(
            1 for row in active if row["source"].get("table") == table
        ) + 1
        require(
            transaction.get("data_collection_order") == collection_order,
            "CDC data_collection_order is not dense",
        )
        active.append(record)
    require(active is None, "CDC capture ends inside a frame")
    require(bool(frames), "CDC capture has no completed transaction")
    return frames


def marker_identity(row: dict[str, Any]) -> tuple[str, int]:
    source = row.get("source")
    after = row.get("after")
    require(
        isinstance(source, dict)
        and source.get("schema") == "benchbase"
        and source.get("table") == "session_txn"
        and row.get("op") == "c"
        and row.get("before") is None
        and isinstance(after, dict)
        and set(after) == {"session_id", "txn_id"},
        "CDC marker row is malformed",
    )
    session_id = after.get("session_id")
    txn_id = after.get("txn_id")
    require(isinstance(session_id, str) and session_id, "CDC marker session is malformed")
    require(type(txn_id) is int and txn_id > 0, "CDC marker txn_id is malformed")
    return session_id, txn_id


def validate_client_cdc_correlations(
    run_id: str, client_path: Path, cdc_path: Path
) -> tuple[list[dict[str, Any]], int]:
    client_stream = redis_records(client_path, named_payload=True)
    clients = [record for record in client_stream if record.get("kind") == "client_txn"]
    require(bool(clients), "client stream has no ClientTxn")
    client_identities: set[tuple[str, int]] = set()
    for client in clients:
        require(client.get("run_id") == run_id, "ClientTxn run_id differs")
        session_id = client.get("session_id")
        correlation = client.get("correlation_id")
        require(isinstance(session_id, str) and isinstance(correlation, dict), "ClientTxn identity malformed")
        require(
            correlation.get("run_id") == run_id
            and correlation.get("session_id") == session_id
            and type(correlation.get("txn_id")) is int
            and correlation["txn_id"] > 0,
            "ClientTxn correlation differs",
        )
        identity = (session_id, correlation["txn_id"])
        require(identity not in client_identities, "ClientTxn correlation is duplicated")
        client_identities.add(identity)

    marker_identities: set[tuple[str, int]] = set()
    marker_frames = 0
    for frame in cdc_frames(cdc_path):
        markers = [
            row
            for row in frame
            if row.get("source", {}).get("schema") == "benchbase"
            and row.get("source", {}).get("table") == "session_txn"
        ]
        if not markers:
            continue
        require(len(markers) == 1 and frame[-1] is markers[0], "CDC marker is not unique and final")
        identity = marker_identity(markers[0])
        require(identity not in marker_identities, "CDC marker identity is duplicated")
        marker_identities.add(identity)
        marker_frames += 1
    require(marker_identities == client_identities, "ClientTxn and CDC marker identities differ")
    require(marker_frames == len(clients), "marker-bearing CDC frame count differs")
    return client_stream, marker_frames


def final_status(path: Path, run_id: str) -> dict[str, Any]:
    finals = [
        record
        for record in redis_records(path, named_payload=True)
        if record.get("lifecycle_state") == "FINAL"
    ]
    require(len(finals) == 1, "final status count differs")
    require(finals[0].get("run_id") == run_id, "final status run_id differs")
    return finals[0]


def _one_inspection(path: Path) -> dict[str, Any]:
    value = load_json(path)
    require(isinstance(value, list) and len(value) == 1 and isinstance(value[0], dict), f"inspection shape differs: {path}")
    return value[0]


def validate_runtime_provenance(
    provenance_path: Path,
    connector_container_paths: Iterable[Path],
    *,
    connector_image_path: Path | None = None,
    connector_identity_paths: Iterable[Path] = (),
) -> str:
    provenance = load_json(provenance_path)
    artifacts = provenance["artifacts"]
    runtime = artifacts["runtime_connector_image"]
    expected_id = runtime["local_image_id"]
    expected_tag = runtime["image"]
    for path in connector_container_paths:
        container = _one_inspection(path)
        require(container.get("Image") == expected_id, f"connector container image differs: {path}")
        require(
            container.get("Config", {}).get("Image") == expected_tag,
            f"connector container tag differs: {path}",
        )
    if connector_image_path is not None:
        image = _one_inspection(connector_image_path)
        require(image.get("Id") == expected_id, "connector image ID differs from provenance")
        require(expected_tag in (image.get("RepoTags") or []), "connector image tag differs")
    connector_sha = artifacts["connector"]["sha256"]
    for path in connector_identity_paths:
        require(connector_sha in path.read_text(encoding="utf-8"), f"connector JAR checksum differs: {path}")
    return expected_id
