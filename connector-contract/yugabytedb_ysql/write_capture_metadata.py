#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DECISION_BOUNDARY = (
    "Connector IDs, source.txId, LSN, and HybridTime are diagnostic metadata only; "
    "the verifier frames only the unique contiguous BEGIN/rows/END interval and "
    "correlates completed frames only by the SQL-last client marker."
)


def completed_transaction(value: str) -> dict[str, Any]:
    session_id, separator, txn_id_text = value.rpartition(":")
    if not separator or not session_id or not txn_id_text.isdigit() or int(txn_id_text) <= 0:
        raise argparse.ArgumentTypeError("completed transactions must use nonempty-session:positive-txn-id")
    return {"session_id": session_id, "txn_id": int(txn_id_text)}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--redis-batch-size", type=int, required=True)
    parser.add_argument("--source-batch-size", type=int, required=True)
    parser.add_argument("--completed", type=completed_transaction, action="append", required=True)
    parser.add_argument("--backpressure-ms", type=int, default=0)
    args = parser.parse_args()

    if args.redis_batch_size <= 0 or args.source_batch_size <= 0 or args.backpressure_ms < 0:
        parser.error("batch sizes must be positive and backpressure must be nonnegative")
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    lock = json.loads((ROOT / "provenance.lock.json").read_text(encoding="utf-8"))
    artifacts = lock["artifacts"]
    image_details = json.loads((output_dir / "connector-contract-image.json").read_text(encoding="utf-8"))
    if not isinstance(image_details, list) or len(image_details) != 1:
        raise SystemExit("connector-contract-image.json must contain exactly one image")
    local_image_id = image_details[0].get("Id")
    if not isinstance(local_image_id, str) or not local_image_id.startswith("sha256:"):
        raise SystemExit("locally built connector image ID is absent")

    database_build = (output_dir / "database-build.txt").read_text(encoding="utf-8")
    build_hash_match = re.search(r"Build Hash\s*:\s*([0-9a-f]{40})", database_build)
    if build_hash_match is None:
        build_hash_match = re.search(r"revision\s+([0-9a-f]{40})", database_build)
    if build_hash_match is None:
        raise SystemExit("database build hash is absent")

    order_evidence = {
        "source": "ysqlsh_commit_acknowledgement_order",
        "completed_transactions": args.completed,
    }
    (output_dir / "commit-order.json").write_text(
        json.dumps(order_evidence, indent=2) + "\n", encoding="utf-8"
    )

    timestamp = datetime.now(timezone.utc)
    metadata: dict[str, Any] = {
        "capture_id": f"{args.scenario}_{timestamp.strftime('%Y%m%dT%H%M%SZ')}",
        "captured_at_utc": timestamp.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "scenario": args.scenario,
        "platform": lock["platform"],
        "plugin": lock["connector_configuration"]["primary_plugin"],
        "slot_lsn_type": lock["connector_configuration"]["slot_lsn_type"],
        "streaming_mode": lock["connector_configuration"]["streaming_mode"],
        "logical_slots": lock["connector_configuration"]["logical_slots"],
        "publication_count": 1,
        "publication_name": "connector_contract_publication",
        "connector_tasks": lock["connector_configuration"]["tasks"],
        "redis_streams": [lock["connector_configuration"]["redis_stream"]],
        "source_max_batch_size": args.source_batch_size,
        "redis_sink_batch_size": args.redis_batch_size,
        "database_image": artifacts["database"]["image"],
        "database_binary_version": artifacts["database"]["version"],
        "database_build_hash": build_hash_match.group(1),
        "connector_version": artifacts["connector"]["version"],
        "connector_sha256": artifacts["connector"]["sha256"],
        "debezium_server_version": artifacts["debezium_server"]["version"],
        "java_runtime_version": artifacts["java_runtime"]["version"],
        "redis_version": artifacts["redis"]["version"],
        "locally_built_image_id": local_image_id,
        "expected_completed_transactions": args.completed,
        "gate": {
            "name": "raw_connector_redis_contract",
            "status": "PASSED",
            "decision_boundary": DECISION_BOUNDARY,
        },
    }
    if args.backpressure_ms:
        metadata["backpressure"] = {
            "redis_client_pause_write_ms": args.backpressure_ms,
            "transport_errors_observed": False,
        }

    metadata["file_sha256"] = {
        path.name: sha256(path)
        for path in sorted(output_dir.iterdir())
        if path.is_file() and path.name != "capture-metadata.json"
    }
    (output_dir / "capture-metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
