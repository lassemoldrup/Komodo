#!/usr/bin/env python3
"""Build and validate coordinator-owned black-box protocol records."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import struct
import sys
from pathlib import Path
from typing import Any, Iterable

from jsonschema import Draft202012Validator, RefResolver


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_ROOT = ROOT / "protocol" / "schema"

UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z"
)
SESSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]*\Z")
CANONICAL_IDENTIFIER_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
ERROR_CODE_PATTERN = re.compile(r"[A-Z][A-Z0-9_]{0,95}\Z")

BOOTSTRAP_DOMAIN = b"db-monitoring/bootstrap-state-digest\x00"
MAX_SAFE_INTEGER = 9_007_199_254_740_991
LIFECYCLE_ORDER = (
    "STARTING",
    "START_FENCE_SEEN",
    "BOOTSTRAP",
    "LOAD_FINISHED_SEEN",
    "RUNNING",
    "DRAINING",
    "RECONCILED",
    "FINAL",
)


class RecordError(ValueError):
    """The requested record or scan is outside the frozen contract."""


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RecordError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RecordError(message)


def validate_uuid(value: str, label: str = "run_id") -> str:
    require(UUID_PATTERN.fullmatch(value) is not None, f"{label} is not a lowercase UUID")
    return value


def validate_session_id(value: str) -> str:
    require(1 <= len(value) <= 256, "session_id length is outside 1..256")
    require(SESSION_PATTERN.fullmatch(value) is not None, f"invalid session_id: {value}")
    return value


def validate_identifier(value: str, label: str) -> str:
    require(
        CANONICAL_IDENTIFIER_PATTERN.fullmatch(value) is not None,
        f"{label} is not a canonical lowercase identifier: {value}",
    )
    return value


def length_prefix(value: bytes) -> bytes:
    require(len(value) <= 0xFFFFFFFF, "length-prefixed value exceeds U32")
    return struct.pack(">I", len(value)) + value


def canonical_ycsb_key(schema: str, table: str, key: int) -> bytes:
    validate_identifier(schema, "schema")
    validate_identifier(table, "table")
    require(-0x80000000 <= key <= 0x7FFFFFFF, f"YCSB key is outside signed I32: {key}")
    return length_prefix(schema.encode("ascii")) + length_prefix(table.encode("ascii")) + struct.pack(">i", key)


def decode_present_field(value: str, line_number: int, field_number: int) -> bytes:
    encoded = value[1:]
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (ValueError, UnicodeEncodeError) as error:
        raise RecordError(
            f"scan line {line_number} FIELD{field_number} has invalid Base64: {error}"
        ) from error
    require(
        base64.b64encode(decoded).decode("ascii") == encoded,
        f"scan line {line_number} FIELD{field_number} has noncanonical Base64",
    )
    try:
        decoded.decode("utf-8", "strict")
    except UnicodeDecodeError as error:
        raise RecordError(
            f"scan line {line_number} FIELD{field_number} is not strict UTF-8"
        ) from error
    return decoded


def canonical_ycsb_value(tokens: list[str], line_number: int) -> bytes:
    require(len(tokens) == 10, f"scan line {line_number} does not contain ten fields")
    result = bytearray()
    for field_number, token in enumerate(tokens, start=1):
        if token == "N":
            result.append(0)
            continue
        require(
            token.startswith("P"),
            f"scan line {line_number} FIELD{field_number} lacks N/P state tag",
        )
        decoded = decode_present_field(token, line_number, field_number)
        result.append(1)
        result.extend(length_prefix(decoded))
    return bytes(result)


def scan_rows(path: Path, schema: str, table: str) -> list[tuple[bytes, bytes]]:
    try:
        text = path.read_text(encoding="utf-8", errors="strict")
    except (OSError, UnicodeError) as error:
        raise RecordError(f"cannot read bootstrap scan {path}: {error}") from error

    rows: list[tuple[bytes, bytes]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        require(line != "", f"bootstrap scan contains an empty line at {line_number}")
        columns = line.split("\t")
        require(len(columns) == 11, f"scan line {line_number} has {len(columns)} columns, expected 11")
        try:
            key_number = int(columns[0], 10)
        except ValueError as error:
            raise RecordError(f"scan line {line_number} has a non-integer YCSB key") from error
        rows.append(
            (
                canonical_ycsb_key(schema, table, key_number),
                canonical_ycsb_value(columns[1:], line_number),
            )
        )

    keys = [key for key, _ in rows]
    require(len(keys) == len(set(keys)), "bootstrap scan contains a duplicate canonical key")
    rows.sort(key=lambda row: row[0])
    return rows


def bootstrap_digest(rows: Iterable[tuple[bytes, bytes]]) -> tuple[int, str]:
    materialized = list(rows)
    require(len(materialized) <= MAX_SAFE_INTEGER, "bootstrap row count exceeds safe JSON integer")
    preimage = bytearray(BOOTSTRAP_DOMAIN)
    preimage.extend(struct.pack(">Q", len(materialized)))
    for key, value in materialized:
        preimage.extend(length_prefix(key))
        preimage.extend(length_prefix(value))
    return len(materialized), hashlib.sha256(preimage).hexdigest()


def dump_record(record: dict[str, Any]) -> None:
    print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


def build_run_manifest(args: argparse.Namespace) -> dict[str, Any]:
    run_id = validate_uuid(args.run_id)
    require(1 <= len(args.backend_source_id) <= 256, "backend_source_id length is outside 1..256")
    sessions = [validate_session_id(value) for value in args.session_id]
    require(sessions, "at least one expected session is required")
    require(len(sessions) == len(set(sessions)), "expected session IDs must be unique")
    return {
        "kind": "run_manifest",
        "run_id": run_id,
        "backend": "mariadb",
        "backend_source_id": args.backend_source_id,
        "declared_isolation": args.declared_isolation,
        "benchmark": "ycsb",
        "expected_session_ids": sessions,
        "application_table_allowlist": ["benchbase.usertable"],
        "control_table_allowlist": ["benchbase.checker_run_fence"],
        "result_scope": "write_projected_soww",
    }


def build_bootstrap_manifest(args: argparse.Namespace) -> dict[str, Any]:
    run_id = validate_uuid(args.run_id)
    validate_identifier(args.schema, "schema")
    validate_identifier(args.table, "table")
    rows = scan_rows(Path(args.scan_file), args.schema, args.table)
    row_count, digest = bootstrap_digest(rows)
    return {
        "kind": "bootstrap_manifest",
        "run_id": run_id,
        "table_row_counts": [{"table": f"{args.schema}.{args.table}", "row_count": row_count}],
        "canonical_state_digest": digest,
    }


def build_run_failed(args: argparse.Namespace) -> dict[str, Any]:
    record: dict[str, Any] = {
        "kind": "run_failed",
        "run_id": validate_uuid(args.run_id),
        "phase": args.phase,
        "reason_code": args.reason_code,
        "diagnostic": args.diagnostic,
    }
    require(ERROR_CODE_PATTERN.fullmatch(args.reason_code) is not None, "invalid reason_code")
    result_code_document = (ROOT / "protocol" / "result_error_codes.md").read_text(
        encoding="utf-8"
    )
    require("## Error codes" in result_code_document, "result error-code section is missing")
    error_code_section = result_code_document.split("## Error codes", maxsplit=1)[1]
    documented_codes = set(
        re.findall(
            r"^\| `([A-Z][A-Z0-9_]*)` \|",
            error_code_section,
            re.MULTILINE,
        )
    )
    require(args.reason_code in documented_codes, "reason_code is not in result_error_codes.md")
    require(1 <= len(args.diagnostic) <= 4096, "diagnostic length is outside 1..4096")
    return record


def status_validator() -> Draft202012Validator:
    schema_paths = sorted(SCHEMA_ROOT.glob("*.schema.json"))
    schemas = [
        json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicate_keys)
        for path in schema_paths
    ]
    status_schema = next(schema for schema in schemas if schema["$id"].endswith("/monitor-status.schema.json"))
    resolver = RefResolver.from_schema(
        status_schema, store={schema["$id"]: schema for schema in schemas}
    )
    return Draft202012Validator(status_schema, resolver=resolver)


def validate_final_verdict(record: dict[str, Any], expected_sessions: int) -> None:
    if record["lifecycle_state"] != "FINAL" or record["outcome"] == "INVALID":
        return
    reconciliation = record["reconciliation"]
    require(reconciliation["evidence_reconciled"], "final verdict has unreconciled evidence")
    require(
        reconciliation["bootstrap_manifest_validated"],
        "final verdict lacks validated bootstrap state",
    )
    require(reconciliation["end_fence_seen"], "final verdict lacks END_FENCE")
    require(
        reconciliation["expected_sessions"] == expected_sessions,
        "final verdict expected-session count differs from coordinator roster",
    )
    require(
        reconciliation["completed_sessions"] == expected_sessions,
        "final verdict has incomplete sessions",
    )
    require(
        reconciliation["accepted_client_transactions"]
        == reconciliation["joined_workload_transactions"],
        "final verdict client/joined transaction counts differ",
    )
    require(
        reconciliation["bootstrap_cdc_transactions"]
        == reconciliation["bootstrap_checker_transactions"],
        "final verdict bootstrap transaction counts differ",
    )
    require(
        reconciliation["phase_anchor_transactions"] == expected_sessions,
        "final verdict phase-anchor count differs from expected sessions",
    )
    expected_checker_transactions = (
        reconciliation["joined_workload_transactions"]
        + reconciliation["bootstrap_checker_transactions"]
        + reconciliation["phase_anchor_transactions"]
    )
    require(
        reconciliation["checker_consumed_transactions"] == expected_checker_transactions,
        "final verdict checker transaction algebra differs",
    )
    require(
        reconciliation["unmatched_client_records"] == 0
        and reconciliation["unmatched_workload_cdc_slots"] == 0
        and reconciliation["ambiguous_attempts"] == 0,
        "final verdict retains incomplete or ambiguous evidence",
    )
    require(record["pending_cdc_slots"] == 0, "final verdict retains pending CDC slots")
    require(
        record["assembled_cdc_transactions"]
        == reconciliation["bootstrap_cdc_transactions"]
        + reconciliation["joined_workload_transactions"]
        + 3,
        "final verdict assembled CDC counter differs from bootstrap/workload/fences",
    )
    require(
        record["joined_transactions"] == reconciliation["joined_workload_transactions"],
        "final verdict joined counters differ",
    )
    require(
        record["checked_transactions"] == reconciliation["checker_consumed_transactions"],
        "final verdict checked counters differ",
    )
    require(
        record["unmatched_client_records"] == reconciliation["unmatched_client_records"],
        "final verdict unmatched-client counters differ",
    )


def classify_status(args: argparse.Namespace) -> tuple[str, str]:
    require(args.expected_sessions > 0, "expected session count must be positive")
    try:
        record = json.loads(sys.stdin.read(), object_pairs_hook=reject_duplicate_keys)
    except json.JSONDecodeError as error:
        raise RecordError(f"monitor status is not valid JSON: {error}") from error

    errors = sorted(status_validator().iter_errors(record), key=lambda error: list(error.absolute_path))
    if errors:
        rendered = "; ".join(
            f"{'/'.join(map(str, error.absolute_path)) or '<root>'}: {error.message}"
            for error in errors
        )
        raise RecordError(f"monitor status fails schema validation: {rendered}")
    require(record["run_id"] == validate_uuid(args.run_id), "monitor status run_id mismatch")
    validate_final_verdict(record, args.expected_sessions)
    current_lifecycle = record["lifecycle_state"]
    current_rank = LIFECYCLE_ORDER.index(current_lifecycle)
    if args.previous_lifecycle:
        previous_rank = LIFECYCLE_ORDER.index(args.previous_lifecycle)
        require(current_rank >= previous_rank, "monitor lifecycle state regressed")
    if current_lifecycle == args.lifecycle:
        return "match", current_lifecycle
    if current_lifecycle == "FINAL":
        return "terminal", current_lifecycle
    required_rank = LIFECYCLE_ORDER.index(args.lifecycle)
    require(current_rank < required_rank, f"monitor skipped required lifecycle state {args.lifecycle}")
    return "continue", current_lifecycle


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    manifest = subparsers.add_parser("run-manifest", help="emit a RunManifest")
    manifest.add_argument("--run-id", required=True)
    manifest.add_argument("--backend-source-id", required=True)
    manifest.add_argument(
        "--declared-isolation", choices=("READ_COMMITTED", "REPEATABLE_READ"), required=True
    )
    manifest.add_argument("--session-id", action="append", default=[])

    bootstrap = subparsers.add_parser("bootstrap-manifest", help="emit a BootstrapManifest")
    bootstrap.add_argument("--run-id", required=True)
    bootstrap.add_argument("--schema", default="benchbase")
    bootstrap.add_argument("--table", default="usertable")
    bootstrap.add_argument("--scan-file", required=True)

    failed = subparsers.add_parser("run-failed", help="emit a coordinator RunFailed record")
    failed.add_argument("--run-id", required=True)
    failed.add_argument(
        "--phase",
        choices=(
            "CONFIGURATION",
            "CONNECTION_SETUP",
            "ATTEMPT_BODY",
            "ROLLBACK",
            "COMMIT",
            "GTID_LOOKUP",
            "REDIS_PUBLICATION",
            "SESSION_SHUTDOWN",
            "COORDINATOR",
        ),
        required=True,
    )
    failed.add_argument("--reason-code", required=True)
    failed.add_argument("--diagnostic", required=True)

    status = subparsers.add_parser("status-action", help="validate and classify one MonitorStatus")
    status.add_argument("--run-id", required=True)
    status.add_argument("--expected-sessions", type=int, required=True)
    status.add_argument("--previous-lifecycle", choices=LIFECYCLE_ORDER)
    status.add_argument(
        "--lifecycle",
        choices=(
            "STARTING",
            "START_FENCE_SEEN",
            "BOOTSTRAP",
            "LOAD_FINISHED_SEEN",
            "RUNNING",
            "DRAINING",
            "RECONCILED",
            "FINAL",
        ),
        required=True,
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "run-manifest":
            dump_record(build_run_manifest(args))
        elif args.command == "bootstrap-manifest":
            dump_record(build_bootstrap_manifest(args))
        elif args.command == "run-failed":
            dump_record(build_run_failed(args))
        elif args.command == "status-action":
            action, lifecycle = classify_status(args)
            print(f"{action}\t{lifecycle}")
        else:  # pragma: no cover - argparse owns this invariant
            raise RecordError(f"unsupported command: {args.command}")
    except (OSError, RecordError, UnicodeError) as error:
        print(f"black-box record error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
