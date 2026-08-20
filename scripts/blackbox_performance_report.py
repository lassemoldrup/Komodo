#!/usr/bin/env python3
"""Assemble selected performance-stage artifacts without interpreting verdicts."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import re
import statistics
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


STAGES = (
    "benchbase_uninstrumented",
    "read_capture_encoding",
    "read_capture_encoding_redis",
    "cdc_capture",
    "assembly_join",
    "assembly_join_check",
)

IDENTITY_FIELDS = (
    "stage",
    "repetition",
    "stage_order_position",
    "trial_id",
    "run_id",
    "artifact_dir",
    "measurement_completion",
    "reconciliation_result",
    "audited_outcome",
    "checker_observation",
)

OVERHEAD_METRICS = (
    ("throughput_requests_per_second", "throughput_loss_percent"),
    ("latency_mean_microseconds", "latency_increase_percent"),
    ("latency_p50_microseconds", "latency_increase_percent"),
    ("latency_p95_microseconds", "latency_increase_percent"),
    ("latency_p99_microseconds", "latency_increase_percent"),
)


class ReportError(ValueError):
    """An artifact set cannot be summarized safely."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ReportError(message)


def load_json(path: Path, *, required: bool = False) -> Any:
    if not path.exists():
        if required:
            raise ReportError(f"required artifact is missing: {path}")
        return None
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


def normalize_name(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return normalized or "value"


def flatten(prefix: str, value: Any, output: dict[str, Any]) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value):
            child = normalize_name(str(key))
            flatten(f"{prefix}_{child}" if prefix else child, value[key], output)
    elif isinstance(value, list):
        output[f"{prefix}_json"] = json.dumps(value, ensure_ascii=False, sort_keys=True)
    elif value is None:
        output[prefix] = ""
    elif isinstance(value, bool):
        output[prefix] = value
    else:
        output[prefix] = value


def reject_verdict_content(value: Any, path: str = "monitor") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = normalize_name(str(key))
            require(
                normalized not in {"outcome", "verdict"},
                f"join-only metrics contain forbidden verdict key at {path}.{key}",
            )
            reject_verdict_content(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            reject_verdict_content(child, f"{path}[{index}]")
    elif isinstance(value, str):
        require(
            value not in {"PASS", "VIOLATION", "INVALID"},
            f"join-only metrics contain forbidden verdict value at {path}",
        )


def finite_numbers(values: Iterable[Any]) -> list[float]:
    result: list[float] = []
    for value in values:
        if value in (None, "", "N/A", "NA") or isinstance(value, bool):
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            result.append(number)
    return result


def summarize_resources(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    values: dict[str, dict[str, list[float]]] = {}
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        require(reader.fieldnames is not None, f"resource CSV has no header: {path}")
        require(
            {"component", "cpu_percent", "rss_bytes"}.issubset(reader.fieldnames),
            f"resource CSV has an incomplete header: {path}",
        )
        for row in reader:
            component = normalize_name(row["component"])
            component_values = values.setdefault(component, {"cpu": [], "rss": []})
            component_values["cpu"].extend(finite_numbers((row["cpu_percent"],)))
            component_values["rss"].extend(finite_numbers((row["rss_bytes"],)))

    result: dict[str, Any] = {}
    for component, component_values in sorted(values.items()):
        cpu = component_values["cpu"]
        rss = component_values["rss"]
        result[f"resource_{component}_cpu_mean_percent"] = (
            statistics.fmean(cpu) if cpu else ""
        )
        result[f"resource_{component}_cpu_max_percent"] = max(cpu) if cpu else ""
        result[f"resource_{component}_rss_max_bytes"] = max(rss) if rss else ""
        result[f"resource_{component}_sample_count"] = max(len(cpu), len(rss))
    return result


def parse_status_xrange(document: Any) -> dict[str, Any]:
    if document is None:
        return {}
    require(isinstance(document, list), "status XRANGE artifact is not an array")
    statuses: list[dict[str, Any]] = []
    for entry in document:
        require(isinstance(entry, list) and len(entry) == 2, "malformed status XRANGE entry")
        raw_fields = entry[1]
        if isinstance(raw_fields, dict):
            payload = raw_fields.get("payload")
        else:
            require(
                isinstance(raw_fields, list) and len(raw_fields) % 2 == 0,
                "malformed status XRANGE fields",
            )
            fields = dict(zip(raw_fields[::2], raw_fields[1::2]))
            payload = fields.get("payload")
        require(isinstance(payload, str), "status XRANGE entry has no payload")
        record = json.loads(payload)
        require(isinstance(record, dict), "status payload is not an object")
        statuses.append(record)
    if not statuses:
        return {}
    terminal = next(
        (record for record in reversed(statuses) if record.get("lifecycle_state") == "FINAL"),
        None,
    )
    latest = terminal or statuses[-1]
    reconciliation = latest.get("reconciliation")
    if isinstance(reconciliation, dict):
        reconciliation_result = reconciliation.get(
            "evidence_reconciled",
            reconciliation.get("result", reconciliation.get("status", "")),
        )
    else:
        reconciliation_result = ""
    return {
        "status_record_count": len(statuses),
        "status_latest_lifecycle_state": latest.get("lifecycle_state", ""),
        "status_latest_outcome": latest.get("outcome", ""),
        "status_latest_checker_observation": latest.get("checker_observation", ""),
        "status_reconciliation_result": reconciliation_result,
        "status_terminal_seen": terminal is not None,
    }


def xrange_logical_bytes(document: Any, label: str) -> int:
    require(isinstance(document, list), f"{label} XRANGE artifact is not an array")
    total = 0
    for entry in document:
        _, fields = xrange_fields(entry, label)
        total += sum(
            len(field.encode("utf-8")) + len(value.encode("utf-8"))
            for field, value in fields.items()
        )
    return total


def capture_measurement_summaries(
    log_path: Path,
    *,
    expected_run_id: str | None = None,
    expected_sessions: Sequence[str] = (),
) -> dict[str, Any]:
    prefix = "BLACKBOX_MEASUREMENT "
    summaries: list[dict[str, Any]] = []
    with log_path.open("r", encoding="utf-8", errors="replace") as stream:
        for line in stream:
            marker = line.find(prefix)
            if marker < 0:
                continue
            text = line[marker + len(prefix) :].strip()
            try:
                document = json.loads(text)
            except json.JSONDecodeError as error:
                raise ReportError(f"malformed capture measurement summary in {log_path}") from error
            require(isinstance(document, dict), "capture measurement summary is not an object")
            summaries.append(document)

    totals: dict[str, Any] = {
        "worker_summary_count": len(summaries),
        "record_count": 0,
        "client_txn_count": 0,
        "read_event_count": 0,
        "encoded_payload_bytes": 0,
        "transaction_type_counts": {},
        "session_summaries": {},
    }
    session_counter_names = (
        "final_session_seq",
        "logical_transactions_started",
        "successful_transactions",
        "terminal_aborted_transactions",
        "physical_attempts_started",
        "definitively_aborted_attempts",
        "published_transactions",
        "retry_attempts",
        "ambiguous_attempts",
    )
    for name in session_counter_names:
        totals[name] = 0
    sessions: set[str] = set()
    run_ids: set[str] = set()
    for summary in summaries:
        require(
            summary.get("kind") == "client_capture_measurement",
            "capture summary has the wrong kind",
        )
        summary_run_id = summary.get("run_id")
        require(isinstance(summary_run_id, str), "capture summary has no run_id")
        run_ids.add(summary_run_id)
        if expected_run_id is not None:
            require(summary_run_id == expected_run_id, "capture summary belongs to another run")
        session_id = summary.get("session_id")
        require(isinstance(session_id, str) and session_id != "", "capture summary has no session_id")
        sessions.add(session_id)
        require(
            session_id not in totals["session_summaries"],
            f"duplicate capture summary for session {session_id}",
        )
        totals["session_summaries"][session_id] = summary
        for name in ("record_count", "client_txn_count", "read_event_count", "encoded_payload_bytes"):
            value = summary.get(name, 0)
            require(isinstance(value, int) and value >= 0, f"capture summary {name} is invalid")
            totals[name] += value
        transaction_types = summary.get("transaction_type_counts")
        require(isinstance(transaction_types, dict), "capture transaction type counts are not an object")
        for transaction_type, value in transaction_types.items():
            require(
                isinstance(transaction_type, str)
                and isinstance(value, int)
                and value >= 0,
                "capture transaction type count is invalid",
            )
            totals["transaction_type_counts"][transaction_type] = (
                totals["transaction_type_counts"].get(transaction_type, 0) + value
            )
        counters = summary.get("session_done_counters")
        require(isinstance(counters, dict), "capture SessionDone counters are not an object")
        for name in session_counter_names:
            require(name in counters, f"capture SessionDone counter is missing: {name}")
            value = counters[name]
            require(isinstance(value, int) and value >= 0, f"capture summary {name} is invalid")
            totals[name] += value
    totals["session_count"] = len(sessions)
    require(len(run_ids) == 1, "capture summaries mix run epochs")
    if expected_sessions:
        require(
            sessions == set(expected_sessions),
            "capture summaries do not match the exact expected session roster",
        )
        require(
            len(summaries) == len(expected_sessions),
            "capture summaries do not contain exactly one record per expected session",
        )
    return totals


def benchbase_aliases(document: Mapping[str, Any]) -> dict[str, Any]:
    latency = document.get("latency_microseconds", {})
    require(isinstance(latency, Mapping), "BenchBase latency metrics are not an object")
    return {
        "benchbase_measured_count": document.get("measured_count", ""),
        "benchbase_elapsed_time_nanoseconds": document.get(
            "elapsed_time_nanoseconds", ""
        ),
        "throughput_requests_per_second": document.get(
            "throughput_requests_per_second", ""
        ),
        "goodput_requests_per_second": document.get("goodput_requests_per_second", ""),
        "latency_mean_microseconds": latency.get("mean", ""),
        "latency_p50_microseconds": latency.get("p50", ""),
        "latency_p95_microseconds": latency.get("p95", ""),
        "latency_p99_microseconds": latency.get("p99", ""),
        "latency_max_microseconds": latency.get("max", ""),
        "measured_start_epoch_ms": document.get("measured_start_epoch_ms", ""),
        "measured_end_epoch_ms": document.get("measured_end_epoch_ms", ""),
    }


def _candidate(flattened: Mapping[str, Any], suffixes: Sequence[str]) -> Any:
    for suffix in suffixes:
        exact = flattened.get(suffix)
        if exact not in (None, ""):
            return exact
        for key, value in flattened.items():
            if key.endswith("_" + suffix) and value not in (None, ""):
                return value
    return ""


def monitor_aliases(flattened: Mapping[str, Any]) -> dict[str, Any]:
    wait_current_ns = flattened.get(
        "monitor_joiner_missing_client_head_wait_current_nanoseconds", ""
    )
    wait_max_ns = flattened.get(
        "monitor_joiner_missing_client_head_wait_max_nanoseconds", ""
    )
    checked = flattened.get("monitor_checker_consumed_transactions", "")
    checker_ns = flattened.get("monitor_checker_consume_total_nanoseconds", "")
    rate: Any = ""
    numeric = finite_numbers((checked, checker_ns))
    if len(numeric) == 2 and numeric[1] > 0:
        rate = numeric[0] * 1_000_000_000.0 / numeric[1]

    def nanoseconds_to_milliseconds(value: Any) -> Any:
        values = finite_numbers((value,))
        return values[0] / 1_000_000.0 if values else ""

    return {
        "head_of_line_wait_current_ms": nanoseconds_to_milliseconds(wait_current_ns),
        "head_of_line_wait_max_ms": nanoseconds_to_milliseconds(wait_max_ns),
        "join_buffer_high_water_bytes": flattened.get(
            "monitor_joiner_join_buffer_high_water_bytes", ""
        ),
        "checker_rate_transactions_per_second": rate,
        "checker_consume_total_nanoseconds": checker_ns,
        "checker_consume_max_nanoseconds": flattened.get(
            "monitor_checker_consume_max_nanoseconds", ""
        ),
        "checker_retained_state_bytes": flattened.get(
            "monitor_checker_engine_retained_state_bytes", ""
        ),
        "checker_peak_retained_state_bytes": flattened.get(
            "monitor_checker_engine_peak_retained_state_bytes", ""
        ),
        "monitor_joined_transactions": flattened.get(
            "monitor_joiner_joined_workload_transactions", ""
        ),
        "monitor_checked_transactions": checked,
        "monitor_bootstrap_transactions": flattened.get(
            "monitor_joiner_released_bootstrap_transactions", ""
        ),
        "monitor_joined_read_events": flattened.get(
            "monitor_joiner_joined_read_events", ""
        ),
        "monitor_joined_write_events": flattened.get(
            "monitor_joiner_joined_write_events", ""
        ),
        "monitor_released_bootstrap_write_events": flattened.get(
            "monitor_joiner_released_bootstrap_write_events", ""
        ),
        "monitor_released_ordered_events": flattened.get(
            "monitor_joiner_released_ordered_events", ""
        ),
    }


def stream_id_milliseconds(stream_id: str) -> int:
    match = re.fullmatch(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)", stream_id)
    require(match is not None, f"invalid Redis stream ID: {stream_id!r}")
    return int(match.group(1))


def xrange_fields(entry: Any, label: str) -> tuple[str, dict[str, str]]:
    require(isinstance(entry, list) and len(entry) == 2, f"malformed {label} XRANGE entry")
    stream_id = entry[0]
    require(isinstance(stream_id, str), f"{label} stream ID is not text")
    stream_id_milliseconds(stream_id)
    raw_fields = entry[1]
    if isinstance(raw_fields, dict):
        fields = raw_fields
    else:
        require(
            isinstance(raw_fields, list) and len(raw_fields) % 2 == 0,
            f"malformed {label} XRANGE fields",
        )
        fields = dict(zip(raw_fields[::2], raw_fields[1::2]))
    require(
        all(isinstance(key, str) and isinstance(value, str) for key, value in fields.items()),
        f"{label} XRANGE fields are not text",
    )
    return stream_id, fields


def find_phase(value: Any, run_id: str, phases: dict[str, int], stream_ms: int) -> None:
    if isinstance(value, dict):
        phase = value.get("phase")
        record_run_id = value.get("run_id")
        if phase in {"RUN_START", "LOAD_FINISHED", "END_FENCE"} and (
            record_run_id is None or record_run_id == run_id
        ):
            phases.setdefault(str(phase), stream_ms)
        for child in value.values():
            find_phase(child, run_id, phases, stream_ms)
    elif isinstance(value, list):
        for child in value:
            find_phase(child, run_id, phases, stream_ms)
    elif isinstance(value, str) and value.startswith(("{", "[")):
        try:
            child = json.loads(value)
        except json.JSONDecodeError:
            return
        find_phase(child, run_id, phases, stream_ms)


def fence_observation_latencies(
    cdc_document: Any,
    status_document: Any,
    monitor_document: Any,
    run_id: str,
) -> dict[str, Any]:
    require(isinstance(cdc_document, list), "CDC XRANGE artifact is not an array")
    require(isinstance(status_document, list), "status XRANGE artifact is not an array")
    phases: dict[str, int] = {}
    for entry in cdc_document:
        stream_id, fields = xrange_fields(entry, "CDC")
        stream_ms = stream_id_milliseconds(stream_id)
        for value in fields.values():
            find_phase(value, run_id, phases, stream_ms)

    acknowledgements: dict[str, int] = {}
    lifecycle_to_phase = {
        "START_FENCE_SEEN": "RUN_START",
        "LOAD_FINISHED_SEEN": "LOAD_FINISHED",
        "FINAL": "END_FENCE",
    }
    for entry in status_document:
        stream_id, fields = xrange_fields(entry, "status")
        payload = fields.get("payload")
        require(payload is not None, "status XRANGE entry has no payload")
        record = json.loads(payload)
        require(isinstance(record, dict), "status payload is not an object")
        phase = lifecycle_to_phase.get(record.get("lifecycle_state"))
        if phase is not None:
            acknowledgements.setdefault(phase, stream_id_milliseconds(stream_id))

    if "END_FENCE" not in acknowledgements:
        require(isinstance(monitor_document, dict), "join-only monitor metrics are missing")
        finished = monitor_document.get("finished_at_utc")
        require(isinstance(finished, str), "join-only monitor finish timestamp is missing")
        try:
            parsed = dt.datetime.fromisoformat(finished.replace("Z", "+00:00"))
        except ValueError as error:
            raise ReportError("join-only monitor finish timestamp is invalid") from error
        require(parsed.tzinfo is not None, "join-only monitor finish timestamp has no timezone")
        acknowledgements["END_FENCE"] = int(parsed.timestamp() * 1000)

    output: dict[str, Any] = {}
    labels = {
        "RUN_START": "run_start_fence_observation_latency_ms",
        "LOAD_FINISHED": "load_finished_fence_observation_latency_ms",
        "END_FENCE": "end_fence_observation_latency_ms",
    }
    for phase, label in labels.items():
        require(phase in phases, f"CDC artifact has no {phase} fence row")
        require(phase in acknowledgements, f"status/monitor artifacts have no {phase} observation")
        output[label] = acknowledgements[phase] - phases[phase]
    return output


def contains_end_status(value: Any) -> bool:
    if isinstance(value, dict):
        if value.get("status") == "END":
            return True
        return any(contains_end_status(child) for child in value.values())
    if isinstance(value, list):
        return any(contains_end_status(child) for child in value)
    if isinstance(value, str) and value.startswith(("{", "[")):
        try:
            return contains_end_status(json.loads(value))
        except json.JSONDecodeError:
            return False
    return False


def fence_stream_time(
    document: Any, run_id: str, phase: str, *, transaction_end: bool = False
) -> int:
    require(phase in {"RUN_START", "LOAD_FINISHED", "END_FENCE"}, "unknown fence phase")
    require(isinstance(document, list), "CDC XRANGE artifact is not an array")
    phases: dict[str, int] = {}
    requested_phase_seen = False
    fence_transaction_id: str | None = None
    for entry in document:
        stream_id, fields = xrange_fields(entry, "CDC")
        stream_ms = stream_id_milliseconds(stream_id)
        for value in fields.values():
            find_phase(value, run_id, phases, stream_ms)
            try:
                envelope = json.loads(value)
            except json.JSONDecodeError:
                continue
            if not isinstance(envelope, dict):
                continue
            entry_phases: dict[str, int] = {}
            find_phase(envelope, run_id, entry_phases, stream_ms)
            if phase in entry_phases:
                transaction = envelope.get("transaction")
                if isinstance(transaction, dict) and isinstance(transaction.get("id"), str):
                    fence_transaction_id = transaction["id"]
            if (
                transaction_end
                and requested_phase_seen
                and fence_transaction_id is not None
                and envelope.get("status") == "END"
                and envelope.get("id") == fence_transaction_id
            ):
                return stream_ms
        if phase in phases:
            requested_phase_seen = True
    require(phase in phases, f"CDC artifact has no {phase} fence row")
    if transaction_end:
        raise ReportError(f"CDC artifact has no transaction END after {phase}")
    return phases[phase]


def trial_row(trial_dir: Path) -> dict[str, Any]:
    metadata = load_json(trial_dir / "metadata.json", required=True)
    require(isinstance(metadata, dict), f"metadata is not an object: {trial_dir}")
    stage = metadata.get("stage")
    require(stage in STAGES, f"unknown performance stage in {trial_dir}: {stage!r}")

    row: dict[str, Any] = {field: "" for field in IDENTITY_FIELDS}
    for key, value in metadata.items():
        normalized_key = normalize_name(str(key))
        if normalized_key == "configured_stages":
            require(
                isinstance(value, list),
                f"configured performance stages are not an array: {trial_dir}",
            )
            row["configured_stages_json"] = json.dumps(
                value, ensure_ascii=False, separators=(",", ":")
            )
        else:
            row[normalized_key] = value
    row["artifact_dir"] = str(trial_dir.resolve())

    benchbase = load_json(trial_dir / "benchbase_metrics.json", required=True)
    require(isinstance(benchbase, dict), "BenchBase metrics are not an object")
    row.update(benchbase_aliases(benchbase))
    flatten("benchbase", benchbase, row)

    for filename, prefix in (
        ("redis_metrics.json", "redis"),
        ("client_metrics.json", "client"),
        ("capture_metrics.json", "capture"),
        ("network_metrics.json", "redis_network"),
        ("redis_usage.json", "redis_memory"),
        ("redis_configuration.json", "redis_configuration"),
        ("bootstrap_manifest.json", "bootstrap"),
        ("analysis_window.json", "analysis_window"),
    ):
        document = load_json(trial_dir / filename)
        if document is not None:
            flatten(prefix, document, row)

    bootstrap = load_json(trial_dir / "bootstrap_manifest.json", required=True)
    require(isinstance(bootstrap, dict), "bootstrap manifest is not an object")
    counts = bootstrap.get("table_row_counts")
    require(isinstance(counts, list) and len(counts) == 1, "bootstrap table counts are incomplete")
    require(isinstance(counts[0], dict), "bootstrap table count is not an object")
    row_count = counts[0].get("row_count")
    require(isinstance(row_count, int) and row_count >= 0, "bootstrap row count is invalid")
    expected_rows = int(metadata.get("scale_factor", 0)) * 1000
    require(row_count == expected_rows, f"bootstrap row count {row_count} != expected {expected_rows}")
    digest = bootstrap.get("canonical_state_digest")
    require(
        isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest) is not None,
        "bootstrap canonical digest is invalid",
    )
    row["bootstrap_row_count"] = row_count
    row["bootstrap_canonical_state_digest"] = digest

    redis_logical_total = 0
    for filename, label in (
        ("cdc_stream.json", "cdc"),
        ("client_stream.json", "client"),
        ("status_stream.json", "status"),
        ("measurement_stream.json", "measurement"),
    ):
        document = load_json(trial_dir / filename)
        if document is None:
            row[f"redis_logical_{label}_stream_bytes"] = ""
            continue
        logical_bytes = xrange_logical_bytes(document, label)
        row[f"redis_logical_{label}_stream_bytes"] = logical_bytes
        redis_logical_total += logical_bytes
    row["redis_logical_total_stream_bytes"] = redis_logical_total if redis_logical_total else ""

    monitor = load_json(trial_dir / "monitor_metrics.json")
    monitor_flat: dict[str, Any] = {}
    if monitor is not None:
        if stage == "assembly_join":
            reject_verdict_content(monitor)
        flatten("monitor", monitor, monitor_flat)
        row.update(monitor_flat)
        row.update(monitor_aliases(monitor_flat))

    redis_stages = {
        "read_capture_encoding_redis",
        "cdc_capture",
        "assembly_join",
        "assembly_join_check",
    }
    if stage in redis_stages:
        redis_configuration = load_json(
            trial_dir / "redis_configuration.json", required=True
        )
        require(
            isinstance(redis_configuration, dict)
            and redis_configuration.get("requested_maxmemory_bytes")
            == metadata.get("redis_maxmemory_bytes")
            and redis_configuration.get("effective_maxmemory_bytes")
            == metadata.get("redis_maxmemory_bytes")
            and redis_configuration.get("maxmemory_policy") == "noeviction",
            "Redis capacity provenance differs from the requested noeviction configuration",
        )
    if stage in {"assembly_join", "assembly_join_check"}:
        require(
            monitor_flat.get("monitor_limits_joiner_max_pending_client_bytes")
            == metadata.get("monitor_max_pending_client_bytes"),
            "monitor pending-client limit differs from trial metadata",
        )
        require(
            monitor_flat.get(
                "monitor_limits_joiner_max_retained_identity_bytes"
            )
            == metadata.get("monitor_max_retained_identity_bytes"),
            "monitor retained-identity limit differs from trial metadata",
        )

    status = parse_status_xrange(load_json(trial_dir / "status_stream.json"))
    row.update(status)
    row.update(summarize_resources(trial_dir / "resource_samples.csv"))

    if stage == "assembly_join_check":
        row["audited_outcome"] = status.get("status_latest_outcome", "")
        row["checker_observation"] = status.get("status_latest_checker_observation", "")
        row["reconciliation_result"] = monitor_flat.get(
            "monitor_evidence_reconciled", status.get("status_reconciliation_result", "")
        )
        row["measurement_completion"] = (
            "complete" if status.get("status_terminal_seen") else "incomplete"
        )
    elif stage == "assembly_join":
        # Join-only monitor metrics carry measurement completion and evidence
        # reconciliation, but the status protocol intentionally has no FINAL verdict.
        row["measurement_completion"] = monitor_flat.get(
            "monitor_completion_status", monitor_flat.get("monitor_completed", "")
        )
        row["reconciliation_result"] = monitor_flat.get(
            "monitor_evidence_reconciled",
            monitor_flat.get("monitor_reconciliation_evidence_reconciled", ""),
        )
        row["audited_outcome"] = ""
        row["checker_observation"] = ""
    else:
        row["measurement_completion"] = "complete"
        row["reconciliation_result"] = ""
        row["audited_outcome"] = ""
        row["checker_observation"] = ""
    if stage in {"assembly_join", "assembly_join_check"}:
        row.update(
            fence_observation_latencies(
                load_json(trial_dir / "cdc_stream.json", required=True),
                load_json(trial_dir / "status_stream.json", required=True),
                monitor,
                str(metadata.get("run_id", "")),
            )
        )
    return row


def find_trial_dirs(root: Path) -> list[Path]:
    trial_root = root / "trials"
    require(trial_root.is_dir(), f"trial directory is missing: {trial_root}")
    result = sorted(path.parent for path in trial_root.rglob("metadata.json"))
    require(result, f"no trials found under {trial_root}")
    return result


def configured_stage_roster(rows: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    rosters: list[tuple[str, ...]] = []
    for row in rows:
        raw_roster = row.get("configured_stages_json", "")
        if raw_roster in (None, ""):
            # Artifacts created before selectable stages always ran all six.
            roster = STAGES
        else:
            require(
                isinstance(raw_roster, str),
                "configured performance stage roster is not JSON text",
            )
            try:
                parsed = json.loads(raw_roster)
            except json.JSONDecodeError as error:
                raise ReportError("configured performance stage roster is malformed") from error
            require(
                isinstance(parsed, list) and parsed,
                "configured performance stage roster is empty or not an array",
            )
            require(
                all(isinstance(stage, str) and stage in STAGES for stage in parsed),
                "configured performance stage roster contains an unknown stage",
            )
            roster = tuple(parsed)
            require(
                len(roster) == len(set(roster)),
                "configured performance stage roster contains duplicates",
            )
            canonical = tuple(stage for stage in STAGES if stage in roster)
            require(
                roster == canonical,
                "configured performance stage roster is not in canonical order",
            )
        rosters.append(roster)
    require(
        len(set(rosters)) == 1,
        "configured performance stage roster differs across trials",
    )
    return rosters[0]


def write_raw_trials(root: Path, output: Path) -> list[dict[str, Any]]:
    rows = [trial_row(path) for path in find_trial_dirs(root)]
    stage_roster = configured_stage_roster(rows)
    stages = {str(row["stage"]) for row in rows}
    require(
        stages == set(stage_roster),
        "raw trial stages differ from the configured roster",
    )
    row_counts = {row["bootstrap_row_count"] for row in rows}
    digests = {row["bootstrap_canonical_state_digest"] for row in rows}
    require(len(row_counts) == 1, "loaded row counts differ across performance trials")
    require(len(digests) == 1, "loaded canonical state digests differ across performance trials")
    repetitions = {int(row["repetition"]) for row in rows}
    require(repetitions, "raw trials have no repetition numbers")
    configured_repetitions = {int(row["configured_repetitions"]) for row in rows}
    require(
        len(configured_repetitions) == 1 and next(iter(configured_repetitions)) > 0,
        "configured repetition count differs across trials or is invalid",
    )
    expected_repetitions = set(range(1, next(iter(configured_repetitions)) + 1))
    require(
        repetitions == expected_repetitions,
        "raw trial repetitions differ from the configured roster",
    )
    for stage in stage_roster:
        stage_rows = [row for row in rows if row["stage"] == stage]
        require(
            len(stage_rows) == len(expected_repetitions),
            f"stage {stage} does not have exactly one trial per repetition",
        )
        require(
            {int(row["repetition"]) for row in stage_rows} == expected_repetitions,
            f"stage {stage} repetition roster differs",
        )
    trial_ids = [str(row["trial_id"]) for row in rows]
    run_ids = [str(row["run_id"]) for row in rows]
    require(len(trial_ids) == len(set(trial_ids)), "trial IDs are not unique")
    require(len(run_ids) == len(set(run_ids)), "run IDs are not unique")

    invariant_fields = (
        "terminals",
        "scale_factor",
        "field_size",
        "loader_threads",
        "warmup_seconds",
        "requested_duration_seconds",
        "random_seed",
        "configured_repetitions",
        "redis_maxmemory_bytes",
        "monitor_max_pending_client_bytes",
        "monitor_max_retained_identity_bytes",
        "output_max_bytes",
        "isolation",
        "rate",
        "java_flags",
        "si_monitoring_head",
        "benchbase_current_head",
        "benchbase_baseline_head",
        "co_checker_head",
        "smoke",
    )
    for field in invariant_fields:
        require(
            len({str(row.get(field, "")) for row in rows}) == 1,
            f"trial invariant differs across runs: {field}",
        )
    random_seed = int(rows[0]["random_seed"])
    expected_positions = set(range(1, len(stage_roster) + 1))
    for repetition in sorted(expected_repetitions):
        repetition_rows = [
            row for row in rows if int(row["repetition"]) == repetition
        ]
        positions = {int(row["stage_order_position"]) for row in repetition_rows}
        require(
            len(repetition_rows) == len(stage_roster) and positions == expected_positions,
            f"repetition {repetition} stage-order positions are incomplete or duplicated",
        )
        offset = (random_seed + repetition - 1) % len(stage_roster)
        for row in repetition_rows:
            position = int(row["stage_order_position"])
            expected_stage = stage_roster[
                (offset + position - 1) % len(stage_roster)
            ]
            require(
                row["stage"] == expected_stage,
                f"repetition {repetition} stage-order rotation differs at position {position}",
            )
    smoke = bool(rows[0].get("smoke"))
    if not smoke:
        for field in (
            "si_monitoring_dirty",
            "benchbase_current_dirty",
            "benchbase_baseline_dirty",
            "co_checker_dirty",
        ):
            require(not any(bool(row.get(field)) for row in rows), f"measured run is dirty: {field}")

    for row in rows:
        require(
            finite_numbers((row.get("benchbase_measured_count"),))
            and float(row["benchbase_measured_count"]) > 0,
            f"trial has no measured BenchBase requests: {row['trial_id']}",
        )
        require(
            row.get("benchbase_final_state") == "EXIT",
            f"BenchBase did not reach EXIT: {row['trial_id']}",
        )
        stage = str(row["stage"])
        if stage in {"read_capture_encoding", "read_capture_encoding_redis"}:
            require(
                int(row.get("capture_worker_summary_count", 0)) == int(row.get("terminals", 0)),
                f"capture summaries are incomplete: {row['trial_id']}",
            )
        if stage == "assembly_join":
            require(row.get("monitor_completed") is True, "join-only monitor did not complete")
            require(
                row.get("monitor_evidence_reconciled") is True,
                "join-only evidence did not reconcile",
            )
            require(row.get("audited_outcome") == "", "join-only row contains a verdict")
        if stage == "assembly_join_check":
            require(row.get("status_terminal_seen") is True, "audited monitor has no final status")
            require(
                row.get("monitor_evidence_reconciled") is True,
                "audited evidence did not reconcile",
            )
            require(
                row.get("audited_outcome") in {"PASS", "VIOLATION"},
                "audited trial has no valid consistency outcome",
            )
    remaining_fields = sorted({key for row in rows for key in row} - set(IDENTITY_FIELDS))
    fields = list(IDENTITY_FIELDS) + remaining_fields
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return rows


def median_for(rows: Sequence[Mapping[str, Any]], stage: str, metric: str) -> float | None:
    values = finite_numbers(row.get(metric) for row in rows if row.get("stage") == stage)
    return statistics.median(values) if values else None


def comparison_rows(
    rows: Sequence[Mapping[str, Any]],
    comparison_stage: str,
    stage: str,
    attribution: str,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for metric, kind in OVERHEAD_METRICS:
        comparison = median_for(rows, comparison_stage, metric)
        measured = median_for(rows, stage, metric)
        if kind == "throughput_loss_percent":
            formula = "100 * (comparison_median - stage_median) / comparison_median"
            overhead = (
                ""
                if comparison is None or measured is None
                else "N/A"
                if comparison == 0
                else (comparison - measured) / comparison * 100.0
            )
        else:
            formula = "100 * (stage_median - comparison_median) / comparison_median"
            overhead = (
                ""
                if comparison is None or measured is None
                else "N/A"
                if comparison == 0
                else (measured - comparison) / comparison * 100.0
            )
        result.append(
            {
                "comparison_stage": comparison_stage,
                "stage": stage,
                "attribution": attribution,
                "metric": metric,
                "comparison_median": "" if comparison is None else comparison,
                "stage_median": "" if measured is None else measured,
                "overhead_percent": overhead,
                "formula": formula,
            }
        )
    return result


def write_overhead(rows: Sequence[Mapping[str, Any]], output: Path) -> None:
    output_rows: list[dict[str, Any]] = []
    baseline = "benchbase_uninstrumented"
    available_stages = {str(row.get("stage", "")) for row in rows}
    if baseline in available_stages:
        for stage in STAGES:
            if stage in available_stages:
                output_rows.extend(
                    comparison_rows(rows, baseline, stage, "cumulative_vs_baseline")
                )
    comparisons = (
        (
            "benchbase_uninstrumented",
            "read_capture_encoding",
            "read_capture_encoding_increment",
        ),
        (
            "benchbase_uninstrumented",
            "cdc_capture",
            "cdc_capture_increment",
        ),
        (
            "read_capture_encoding",
            "read_capture_encoding_redis",
            "redis_publication_increment",
        ),
        (
            "assembly_join",
            "assembly_join_check",
            "checking_boundary_increment",
        ),
    )
    for comparison_stage, stage, attribution in comparisons:
        if {comparison_stage, stage}.issubset(available_stages):
            output_rows.extend(
                comparison_rows(rows, comparison_stage, stage, attribution)
            )
    fields = (
        "comparison_stage",
        "stage",
        "attribution",
        "metric",
        "comparison_median",
        "stage_median",
        "overhead_percent",
        "formula",
    )
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(output_rows)


def write_artifact_index(root: Path, output: Path) -> None:
    rows: list[dict[str, Any]] = []
    for trial_dir in find_trial_dirs(root):
        manifest = trial_dir / "SHA256SUMS"
        require(manifest.is_file(), f"trial checksum manifest is missing: {manifest}")
        rows.append(
            {
                "trial_dir": str(trial_dir.resolve()),
                "checksum_manifest": str(manifest.resolve()),
                "checksum_manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
            }
        )
    fields = ("trial_dir", "checksum_manifest", "checksum_manifest_sha256")
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_checksums(root: Path, output: Path) -> None:
    resolved_root = root.resolve()
    resolved_output = output.resolve()
    require(
        resolved_output == resolved_root or resolved_root in resolved_output.parents,
        "checksum output must be inside the checksummed root",
    )
    files = sorted(
        path for path in root.rglob("*") if path.is_file() and path.resolve() != resolved_output
    )
    with output.open("w", encoding="utf-8", newline="") as stream:
        for path in files:
            hasher = hashlib.sha256()
            with path.open("rb") as source:
                while chunk := source.read(1024 * 1024):
                    hasher.update(chunk)
            digest = hasher.hexdigest()
            stream.write(f"{digest}  {path.relative_to(root).as_posix()}\n")


def command_capture(args: argparse.Namespace) -> None:
    document = capture_measurement_summaries(
        Path(args.log),
        expected_run_id=args.run_id,
        expected_sessions=args.expected_session,
    )
    Path(args.output).write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def command_aggregate(args: argparse.Namespace) -> None:
    root = Path(args.root)
    raw_path = Path(args.raw_output)
    rows = write_raw_trials(root, raw_path)
    write_overhead(rows, Path(args.overhead_output))
    write_artifact_index(root, Path(args.artifact_index_output))


def command_checksums(args: argparse.Namespace) -> None:
    write_checksums(Path(args.root), Path(args.output))


def command_fence_time(args: argparse.Namespace) -> None:
    document = load_json(Path(args.input), required=True)
    print(
        fence_stream_time(
            document, args.run_id, args.phase, transaction_end=args.transaction_end
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    capture = subparsers.add_parser(
        "capture-summaries", help="aggregate BenchBase measurement summary log records"
    )
    capture.add_argument("--log", required=True)
    capture.add_argument("--output", required=True)
    capture.add_argument("--run-id")
    capture.add_argument("--expected-session", action="append", default=[])
    capture.set_defaults(handler=command_capture)

    aggregate = subparsers.add_parser(
        "aggregate", help="write the wide raw trial, overhead, and artifact-index CSV files"
    )
    aggregate.add_argument("--root", required=True)
    aggregate.add_argument("--raw-output", required=True)
    aggregate.add_argument("--overhead-output", required=True)
    aggregate.add_argument("--artifact-index-output", required=True)
    aggregate.set_defaults(handler=command_aggregate)

    checksums = subparsers.add_parser("checksums", help="write a sorted SHA-256 manifest")
    checksums.add_argument("--root", required=True)
    checksums.add_argument("--output", required=True)
    checksums.set_defaults(handler=command_checksums)

    fence_time = subparsers.add_parser(
        "fence-stream-time", help="print a run-scoped CDC fence Redis timestamp in milliseconds"
    )
    fence_time.add_argument("--input", required=True)
    fence_time.add_argument("--run-id", required=True)
    fence_time.add_argument(
        "--phase", required=True, choices=("RUN_START", "LOAD_FINISHED", "END_FENCE")
    )
    fence_time.add_argument(
        "--transaction-end",
        action="store_true",
        help="return the following Debezium transaction END stream time",
    )
    fence_time.set_defaults(handler=command_fence_time)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.handler(args)
    except (OSError, json.JSONDecodeError, ReportError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
