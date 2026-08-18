#!/usr/bin/env python3
"""Verify black-box protocol schemas, examples, and golden byte vectors."""

from __future__ import annotations

import base64
import hashlib
import json
import re
import struct
import sys
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

try:
    from jsonschema import Draft202012Validator, RefResolver
except ImportError as error:  # pragma: no cover - exercised by environment setup
    raise SystemExit(
        "python3-jsonschema is required; rebuild the canonical development container"
    ) from error


PROTOCOL_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_ROOT = PROTOCOL_ROOT / "schema"
EXAMPLE_ROOT = PROTOCOL_ROOT / "examples"
GOLDEN_ROOT = PROTOCOL_ROOT / "golden"

READ_DOMAIN = b"db-monitoring/read-digest\x00"
BOOTSTRAP_DOMAIN = b"db-monitoring/bootstrap-state-digest\x00"
CORRELATION_SEED_DOMAIN = b"db-monitoring/session-marker-seed\x00"
CORRELATION_STEP_DOMAIN = b"db-monitoring/session-marker-step\x00"

PRESENT = 0x01
ABSENT = 0x02
SNAPSHOT = 0x01
LOCAL = 0x02
SAFE_JSON_INTEGER = 9_007_199_254_740_991


class VerificationError(RuntimeError):
    pass


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise VerificationError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicate_keys)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise VerificationError(f"cannot load {path}: {error}") from error


def require(condition: bool, message: str) -> None:
    if not condition:
        raise VerificationError(message)


def u8(value: int) -> bytes:
    require(0 <= value <= 0xFF, f"U8 out of range: {value}")
    return bytes((value,))


def u32(value: int) -> bytes:
    require(0 <= value <= 0xFFFFFFFF, f"U32 out of range: {value}")
    return struct.pack(">I", value)


def u64(value: int) -> bytes:
    require(0 <= value <= 0xFFFFFFFFFFFFFFFF, f"U64 out of range: {value}")
    return struct.pack(">Q", value)


def i32(value: int) -> bytes:
    require(-0x80000000 <= value <= 0x7FFFFFFF, f"I32 out of range: {value}")
    return struct.pack(">i", value)


def lp(value: bytes) -> bytes:
    return u32(len(value)) + value


def sha256_hex(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical_base64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def decode_base64(value: str, label: str) -> bytes:
    try:
        decoded = base64.b64decode(value, validate=True)
    except (ValueError, UnicodeEncodeError) as error:
        raise VerificationError(f"invalid Base64 for {label}: {error}") from error
    require(canonical_base64(decoded) == value, f"noncanonical Base64 for {label}")
    return decoded


def encode_ycsb_key(schema: str, table: str, key: int) -> bytes:
    return lp(schema.encode("utf-8", "strict")) + lp(table.encode("utf-8", "strict")) + i32(key)


def encode_ycsb_value(fields: list[Any]) -> bytes:
    require(len(fields) == 10, f"YCSB value must contain ten fields, got {len(fields)}")
    result = bytearray()
    for index, field in enumerate(fields, start=1):
        if field is None:
            result.extend(u8(0))
            continue
        require(isinstance(field, str), f"FIELD{index} is not string/null")
        encoded = field.encode("utf-8", "strict")
        result.extend(u8(1))
        result.extend(lp(encoded))
    return bytes(result)


def encode_read(read: dict[str, Any]) -> bytes:
    key = decode_base64(read["key_bytes_base64"], "read key")
    result = bytearray(u32(read["event_seq"]) + lp(key))
    if read["value_state"] == "present":
        value = decode_base64(read["value_bytes_base64"], "read value")
        result.extend(u8(PRESENT))
        result.extend(lp(value))
    elif read["value_state"] == "absent":
        require("value_bytes_base64" not in read, "absent read carries value bytes")
        result.extend(u8(ABSENT))
    else:
        raise VerificationError(f"unknown read value_state: {read['value_state']}")

    source = {"snapshot": SNAPSHOT, "local": LOCAL}.get(read["source"])
    require(source is not None, f"unknown read source: {read['source']}")
    result.extend(u8(source))
    return bytes(result)


def read_digest_preimage(reads: list[dict[str, Any]]) -> bytes:
    sequences = [read["event_seq"] for read in reads]
    require(sequences == list(range(len(reads))), f"read event_seq is not dense: {sequences}")
    return READ_DOMAIN + u32(len(reads)) + b"".join(encode_read(read) for read in reads)


def bootstrap_digest_preimage(rows: list[dict[str, Any]]) -> bytes:
    decoded_rows = [
        (
            decode_base64(row["key_bytes_base64"], "bootstrap key"),
            decode_base64(row["value_bytes_base64"], "bootstrap value"),
        )
        for row in rows
    ]
    keys = [key for key, _ in decoded_rows]
    require(len(keys) == len(set(keys)), "duplicate bootstrap canonical key")
    decoded_rows.sort(key=lambda item: item[0])
    body = b"".join(lp(key) + lp(value) for key, value in decoded_rows)
    return BOOTSTRAP_DOMAIN + u64(len(decoded_rows)) + body


def correlation_seed(run_id: str, session_id: str) -> tuple[bytes, bytes]:
    preimage = (
        CORRELATION_SEED_DOMAIN
        + uuid.UUID(run_id).bytes
        + lp(session_id.encode("utf-8", "strict"))
    )
    return preimage, hashlib.sha256(preimage).digest()


def correlation_entry(session_seq: int, txn_id: int) -> bytes:
    require(1 <= txn_id <= SAFE_JSON_INTEGER, f"txn_id out of range: {txn_id}")
    return u64(session_seq) + u64(txn_id)


def validate_session_attempt_progress(entries: list[dict[str, int]], label: str) -> None:
    previous_surplus: int | None = None
    for entry in entries:
        session_seq = entry["session_seq"]
        txn_id = entry["txn_id"]
        require(txn_id >= session_seq, f"{label}: marker txn_id is behind session_seq")
        surplus = txn_id - session_seq
        require(
            previous_surplus is None or surplus >= previous_surplus,
            f"{label}: marker retry surplus decreased",
        )
        previous_surplus = surplus


def correlation_step(previous: bytes, entry: bytes) -> bytes:
    return hashlib.sha256(CORRELATION_STEP_DOMAIN + previous + entry).digest()


def check_encoded_vector(label: str, expected: dict[str, Any], actual: bytes) -> None:
    if "length" in expected:
        require(len(actual) == expected["length"], f"{label}: length mismatch")
    if "hex" in expected:
        require(actual.hex() == expected["hex"], f"{label}: hex mismatch")
    if "base64" in expected:
        require(canonical_base64(actual) == expected["base64"], f"{label}: Base64 mismatch")
    if "sha256" in expected:
        require(sha256_hex(actual) == expected["sha256"], f"{label}: SHA-256 mismatch")


def verify_schemas_and_examples() -> dict[str, list[dict[str, Any]]]:
    schemas = [load_json(path) for path in sorted(SCHEMA_ROOT.glob("*.schema.json"))]
    for schema in schemas:
        Draft202012Validator.check_schema(schema)

    record_schema = next(schema for schema in schemas if schema["$id"].endswith("/records.schema.json"))
    resolver = RefResolver.from_schema(
        record_schema, store={schema["$id"]: schema for schema in schemas}
    )
    validator = Draft202012Validator(record_schema, resolver=resolver)

    examples_by_kind: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for path in sorted(EXAMPLE_ROOT.glob("*.json")):
        record = load_json(path)
        errors = sorted(validator.iter_errors(record), key=lambda error: list(error.absolute_path))
        if errors:
            rendered = "; ".join(
                f"{'/'.join(map(str, error.absolute_path)) or '<root>'}: {error.message}"
                for error in errors
            )
            raise VerificationError(f"schema validation failed for {path.name}: {rendered}")
        examples_by_kind[record["kind"]].append(record)
    return examples_by_kind


def verify_golden_vectors() -> None:
    keys = load_json(GOLDEN_ROOT / "ycsb_keys.json")
    for vector in keys["vectors"]:
        actual = encode_ycsb_key(keys["canonical_schema"], keys["canonical_table"], vector["key"])
        check_encoded_vector(f"YCSB key {vector['key']}", vector, actual)

    values = load_json(GOLDEN_ROOT / "ycsb_values.json")
    for vector in values["vectors"]:
        actual = encode_ycsb_value(vector["fields"])
        check_encoded_vector(f"YCSB value {vector['name']}", vector, actual)

    reads = load_json(GOLDEN_ROOT / "read_digests.json")
    for vector in reads["vectors"]:
        preimage = read_digest_preimage(vector["reads"])
        if "preimage_length" in vector:
            require(len(preimage) == vector["preimage_length"], f"read {vector['name']}: length")
        require(preimage.hex() == vector["preimage_hex"], f"read {vector['name']}: preimage hex")
        if "preimage_base64" in vector:
            require(
                canonical_base64(preimage) == vector["preimage_base64"],
                f"read {vector['name']}: preimage Base64",
            )
        require(sha256_hex(preimage) == vector["digest"], f"read {vector['name']}: digest")

    bootstraps = load_json(GOLDEN_ROOT / "bootstrap_digests.json")
    for vector in bootstraps["vectors"]:
        preimage = bootstrap_digest_preimage(vector["rows"])
        if "preimage_length" in vector:
            require(len(preimage) == vector["preimage_length"], f"bootstrap {vector['name']}: length")
        require(preimage.hex() == vector["preimage_hex"], f"bootstrap {vector['name']}: preimage hex")
        if "preimage_base64" in vector:
            require(
                canonical_base64(preimage) == vector["preimage_base64"],
                f"bootstrap {vector['name']}: preimage Base64",
            )
        require(sha256_hex(preimage) == vector["digest"], f"bootstrap {vector['name']}: digest")

    correlations = load_json(GOLDEN_ROOT / "session_correlation_digests.json")
    for vector in correlations["vectors"]:
        validate_session_attempt_progress(vector["entries"], f"correlation {vector['name']}")
        seed_preimage, digest = correlation_seed(vector["run_id"], vector["session_id"])
        require(seed_preimage.hex() == vector["seed_preimage_hex"], f"correlation {vector['name']}: seed")
        require(digest.hex() == vector["seed_digest"], f"correlation {vector['name']}: D0")
        require(len(vector["entries"]) == len(vector["steps"]), "correlation step count mismatch")
        for entry_spec, step_spec in zip(vector["entries"], vector["steps"], strict=True):
            entry = correlation_entry(entry_spec["session_seq"], entry_spec["txn_id"])
            require(entry.hex() == step_spec["entry_hex"], "correlation entry hex mismatch")
            require(canonical_base64(entry) == step_spec["entry_base64"], "correlation entry Base64 mismatch")
            digest = correlation_step(digest, entry)
            require(digest.hex() == step_spec["digest"], "correlation step digest mismatch")
        require(digest.hex() == vector["final_digest"], "correlation final digest mismatch")


def verify_cross_record_semantics(records: dict[str, list[dict[str, Any]]]) -> None:
    manifests = records["run_manifest"]
    require(len(manifests) == 1, "examples require exactly one RunManifest")
    manifest = manifests[0]
    run_id = manifest["run_id"]
    application_tables = set(manifest["application_table_allowlist"])
    control_tables = set(manifest["control_table_allowlist"])
    marker_table = manifest["transaction_marker_table"]
    require(marker_table not in application_tables, "marker table is in application allowlist")
    require(marker_table not in control_tables, "marker table is in control allowlist")
    require(not application_tables.intersection(control_tables), "application/control tables overlap")

    for kind, kind_records in records.items():
        for record in kind_records:
            require(record["run_id"] == run_id, f"{kind}: run_id mismatch")

    clients = records["client_txn"]
    logical_ids: set[str] = set()
    attempt_ids: set[str] = set()
    correlations: set[tuple[str, str, int]] = set()
    clients_by_session: dict[str, list[dict[str, Any]]] = defaultdict(list)
    expected_sessions = set(manifest["expected_session_ids"])

    for client in clients:
        require(client["session_id"] in expected_sessions, "ClientTxn has unknown session")
        require(client["read_count"] == len(client["reads"]), "ClientTxn read_count mismatch")
        preimage = read_digest_preimage(client["reads"])
        require(sha256_hex(preimage) == client["read_digest"], "ClientTxn read digest mismatch")
        correlation = client["correlation_id"]
        require(correlation["run_id"] == run_id, "nested correlation run_id mismatch")
        require(correlation["session_id"] == client["session_id"], "nested session_id mismatch")
        correlation_entry(client["session_seq"], correlation["txn_id"])
        correlation_key = (
            correlation["run_id"],
            correlation["session_id"],
            correlation["txn_id"],
        )
        require(correlation_key not in correlations, "duplicate correlation ID")
        require(client["logical_txn_id"] not in logical_ids, "duplicate logical transaction ID")
        require(client["attempt_id"] not in attempt_ids, "duplicate attempt ID")
        correlations.add(correlation_key)
        logical_ids.add(client["logical_txn_id"])
        attempt_ids.add(client["attempt_id"])
        clients_by_session[client["session_id"]].append(client)

    done_records = records["session_done"]
    require({record["session_id"] for record in done_records} == expected_sessions, "SessionDone roster mismatch")
    for done in done_records:
        require(done["final_session_seq"] == done["logical_transactions_started"], "final session sequence mismatch")
        require(
            done["logical_transactions_started"]
            == done["successful_transactions"] + done["terminal_aborted_transactions"],
            "logical SessionDone arithmetic mismatch",
        )
        require(
            done["successful_transactions"] == done["published_transactions"],
            "successful SessionDone arithmetic mismatch",
        )
        require(
            done["physical_attempts_started"]
            == done["successful_transactions"]
            + done["definitively_aborted_attempts"]
            + done["ambiguous_attempts"],
            "attempt SessionDone arithmetic mismatch",
        )
        require(
            done["retry_attempts"]
            == done["physical_attempts_started"] - done["logical_transactions_started"],
            "retry SessionDone arithmetic mismatch",
        )
        require(done["ambiguous_attempts"] == 0, "clean SessionDone has ambiguous attempt")

        _, digest = correlation_seed(run_id, done["session_id"])
        session_clients = sorted(clients_by_session[done["session_id"]], key=lambda item: item["session_seq"])
        sequences = [client["session_seq"] for client in session_clients]
        require(sequences == sorted(set(sequences)), "published session sequence is not increasing")
        txn_ids = [client["correlation_id"]["txn_id"] for client in session_clients]
        require(txn_ids == sorted(set(txn_ids)), "marker txn_id is not increasing")
        validate_session_attempt_progress(
            [
                {
                    "session_seq": client["session_seq"],
                    "txn_id": client["correlation_id"]["txn_id"],
                }
                for client in session_clients
            ],
            f"session {done['session_id']}",
        )
        require(
            not txn_ids or txn_ids[-1] <= done["physical_attempts_started"],
            "marker txn_id exceeds physical attempt count",
        )
        for client in session_clients:
            digest = correlation_step(
                digest,
                correlation_entry(client["session_seq"], client["correlation_id"]["txn_id"]),
            )
        require(len(session_clients) == done["published_transactions"], "published count mismatch")
        require(digest.hex() == done["rolling_correlation_digest"], "rolling correlation digest mismatch")

    bootstrap = records["bootstrap_manifest"]
    require(len(bootstrap) == 1, "examples require exactly one BootstrapManifest")
    bootstrap = bootstrap[0]
    tables = [item["table"] for item in bootstrap["table_row_counts"]]
    require(tables == sorted(set(tables)), "bootstrap tables are duplicate or unsorted")
    require(tables == sorted(manifest["application_table_allowlist"]), "bootstrap table roster mismatch")
    mixed_state = next(
        vector
        for vector in load_json(GOLDEN_ROOT / "bootstrap_digests.json")["vectors"]
        if vector["name"] == "mixed_two_rows"
    )
    require(
        sum(item["row_count"] for item in bootstrap["table_row_counts"]) == len(mixed_state["rows"]),
        "bootstrap row count mismatch",
    )
    require(
        bootstrap["canonical_state_digest"]
        == sha256_hex(bootstrap_digest_preimage(mixed_state["rows"])),
        "bootstrap manifest digest mismatch",
    )

    statuses = records["status"]
    require(len(statuses) == 1, "examples require exactly one MonitorStatus")
    status = statuses[0]
    reconciliation = status["reconciliation"]
    require(status["lifecycle_state"] == "FINAL", "example status is not final")
    require(status["outcome"] == "PASS", "example status is not PASS")
    require(reconciliation["evidence_reconciled"], "PASS without reconciliation")
    require(reconciliation["expected_sessions"] == len(expected_sessions), "expected session count mismatch")
    require(reconciliation["completed_sessions"] == len(done_records), "completed session count mismatch")
    require(reconciliation["accepted_client_transactions"] == len(clients), "accepted client count mismatch")
    require(
        reconciliation["joined_workload_transactions"] == len(clients),
        "joined workload count mismatch",
    )
    require(
        reconciliation["checker_consumed_transactions"]
        == reconciliation["joined_workload_transactions"]
        + reconciliation["bootstrap_checker_transactions"]
        + reconciliation["phase_anchor_transactions"],
        "checker reconciliation count mismatch",
    )
    require(status["checked_transactions"] == reconciliation["checker_consumed_transactions"], "checked count mismatch")
    require(reconciliation["unmatched_client_records"] == 0, "PASS has unmatched client")
    require(reconciliation["unmatched_workload_cdc_slots"] == 0, "PASS has unmatched CDC")
    require(reconciliation["ambiguous_attempts"] == 0, "PASS has ambiguous attempts")

    error_codes = set(
        re.findall(
            r"^\| `([A-Z][A-Z0-9_]*)` \|",
            (PROTOCOL_ROOT / "result_error_codes.md").read_text(encoding="utf-8"),
            re.MULTILINE,
        )
    )
    for failed in records["run_failed"]:
        require(failed["reason_code"] in error_codes, "RunFailed reason_code is undocumented")


def main() -> int:
    try:
        records = verify_schemas_and_examples()
        verify_golden_vectors()
        verify_cross_record_semantics(records)
    except VerificationError as error:
        print(f"protocol verification failed: {error}", file=sys.stderr)
        return 1
    print(
        "protocol verification passed: "
        f"{sum(len(records) for records in records.values())} examples, "
        f"{len(list(SCHEMA_ROOT.glob('*.schema.json')))} schemas"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
