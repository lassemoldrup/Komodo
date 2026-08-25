#!/usr/bin/env python3
"""Assemble the baseline and full-E2E black-box performance artifacts."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import re
import statistics
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


MODES = (
    "benchbase_uninstrumented",
    "full_e2e",
)

IDENTITY_FIELDS = (
    "mode",
    "repetition",
    "mode_order_position",
    "trial_id",
    "run_id",
    "artifact_dir",
    "measurement_completion",
    "reconciliation_result",
    "audited_outcome",
    "checker_observation",
)

CLIENT_IMPACT_METRICS = (
    (
        "logical_transactions_per_second",
        "logical_throughput_loss_percent",
    ),
)

HEADLINE_CSV_FIELDS = (
    "mode",
    "logical_transactions_per_second_median",
    "successful_transactions_per_second_median",
    "latency_mean_microseconds_median",
    "latency_p95_microseconds_median",
    "logical_throughput_loss_vs_baseline_percent",
)

BENCHBASE_HEADLINE_MODES = MODES

HEADLINE_MODE_LABELS = {
    "benchbase_uninstrumented": "Uninstrumented BenchBase",
    "full_e2e": "Full black-box E2E",
}

TRANSACTION_RESULT_FIELDS = (
    "benchmark",
    "mode",
    "repetition",
    "trial_id",
    "row_scope",
    "transaction_type",
    "elapsed_time_nanoseconds",
    "logical_transactions_started",
    "successful_transactions",
    "terminal_aborted_transactions",
    "physical_attempts_started",
    "definitively_aborted_attempts",
    "retry_attempts",
    "retryable_failure_events",
    "error_transactions",
    "unknown_status_transactions",
    "failed_transactions",
    "ambiguous_attempts",
    "retry_different_events",
    "logical_transactions_per_second",
    "successful_transactions_per_second",
    "attempts_per_second",
    "retry_attempts_per_second",
    "terminal_aborted_transactions_per_second",
    "error_transactions_per_second",
    "unknown_status_transactions_per_second",
    "failed_transactions_per_second",
    "latency_mean_microseconds",
    "latency_p50_microseconds",
    "latency_p95_microseconds",
    "latency_p99_microseconds",
    "latency_max_microseconds",
    "new_order_per_minute",
)

COMPONENT_RESULT_FIELDS = (
    "benchmark",
    "mode",
    "repetition",
    "trial_id",
    "component_boundary",
    "component_owner",
    "interval_definition",
    "rate_interpretation",
    "interval_seconds",
    "workload_transactions",
    "event_kind",
    "workload_events",
    "transactions_per_second",
    "events_per_second",
    "source_lag_mean_ms",
    "source_lag_p95_ms",
    "source_lag_p99_ms",
    "source_lag_max_ms",
    "delivery_lag_mean_ms",
    "delivery_lag_p95_ms",
    "delivery_lag_p99_ms",
    "delivery_lag_max_ms",
)

OUTCOME_COUNTER_FIELDS = (
    "logical_transactions_started",
    "successful_transactions",
    "terminal_aborted_transactions",
    "physical_attempts_started",
    "definitively_aborted_attempts",
    "retry_attempts",
    "retryable_failure_events",
    "error_transactions",
    "unknown_status_transactions",
    "ambiguous_attempts",
    "retry_different_events",
)

CANONICAL_UTC_SECOND_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"
)
STREAM_ID_PATTERN = re.compile(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)\Z")


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


def numeric(value: Any, label: str, *, minimum: float = 0.0) -> float:
    values = finite_numbers((value,))
    require(len(values) == 1 and values[0] >= minimum, f"{label} is not a number >= {minimum}")
    return values[0]


def integer(value: Any, label: str, *, minimum: int = 0) -> int:
    require(
        isinstance(value, int) and not isinstance(value, bool) and value >= minimum,
        f"{label} is not an integer >= {minimum}",
    )
    return value


def canonical_utc_second(value: Any, label: str) -> str:
    require(
        isinstance(value, str) and CANONICAL_UTC_SECOND_RE.fullmatch(value) is not None,
        f"{label} is not canonical UTC whole-second text",
    )
    try:
        dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise ReportError(f"{label} is not a valid UTC calendar timestamp") from error
    return value


def parse_redis_info(path: Path) -> dict[str, int]:
    require(path.is_file(), f"Redis INFO artifact is missing: {path}")
    values: dict[str, int] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="strict").splitlines():
        if not raw_line or raw_line.startswith("#") or ":" not in raw_line:
            continue
        name, value = raw_line.split(":", 1)
        if name in {"used_memory", "used_memory_peak", "maxmemory"}:
            require(re.fullmatch(r"0|[1-9][0-9]*", value) is not None, f"Redis INFO {name} is invalid")
            values[name] = int(value)
    require(
        set(values) == {"used_memory", "used_memory_peak", "maxmemory"},
        "Redis INFO memory counters are incomplete",
    )
    return values


def directory_file_bytes(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


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
    statuses: list[tuple[str, dict[str, Any]]] = []
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
        stream_id = entry[0]
        require(isinstance(stream_id, str), "status stream ID is not text")
        stream_id_milliseconds(stream_id)
        statuses.append((stream_id, record))
    if not statuses:
        return {}
    terminal = next(
        (item for item in reversed(statuses) if item[1].get("lifecycle_state") == "FINAL"),
        None,
    )
    latest_id, latest = terminal or statuses[-1]
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
        "status_terminal_stream_id": latest_id if terminal is not None else "",
        "status_terminal_epoch_ms": (
            stream_id_milliseconds(latest_id) if terminal is not None else ""
        ),
    }


def final_status_boundary(document: Any, run_id: str) -> dict[str, Any]:
    require(isinstance(document, list), "status XRANGE artifact is not an array")
    matches: list[str] = []
    for entry in document:
        stream_id, fields = xrange_fields(entry, "status")
        payload = fields.get("payload")
        require(isinstance(payload, str), "status XRANGE entry has no payload")
        record = json.loads(payload)
        require(isinstance(record, dict), "status payload is not an object")
        if record.get("run_id") == run_id and record.get("lifecycle_state") == "FINAL":
            matches.append(stream_id)
    require(len(matches) == 1, "status stream does not contain exactly one run-scoped FINAL")
    return {"stream_id": matches[0], "epoch_ms": stream_id_milliseconds(matches[0])}


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
    checked = flattened.get("monitor_checker_workload_transactions", "")
    checker_ns = flattened.get("monitor_checker_workload_consume_total_nanoseconds", "")

    def nanoseconds_to_milliseconds(value: Any) -> Any:
        values = finite_numbers((value,))
        return values[0] / 1_000_000.0 if values else ""

    return {
        "head_of_line_wait_current_ms": nanoseconds_to_milliseconds(wait_current_ns),
        "head_of_line_wait_max_ms": nanoseconds_to_milliseconds(wait_max_ns),
        "join_buffer_high_water_bytes": flattened.get(
            "monitor_joiner_join_buffer_high_water_bytes", ""
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
        "monitor_normalized_write_events": flattened.get(
            "monitor_assembler_normalized_write_events", ""
        ),
        "cpp_monitor_assembler_workload_transactions": flattened.get(
            "monitor_assembler_completed_workload_transactions", ""
        ),
        "cpp_monitor_assembler_workload_write_events": flattened.get(
            "monitor_assembler_workload_normalized_write_events", ""
        ),
        "cpp_monitor_assembler_workload_raw_row_events": flattened.get(
            "monitor_assembler_workload_raw_row_events", ""
        ),
        "cpp_monitor_assembler_active_nanoseconds": flattened.get(
            "monitor_assembler_workload_processing_total_nanoseconds", ""
        ),
        "cpp_monitor_assembler_active_max_nanoseconds": flattened.get(
            "monitor_assembler_workload_processing_max_nanoseconds", ""
        ),
        "cpp_monitor_joiner_workload_transactions": flattened.get(
            "monitor_joiner_joined_workload_transactions", ""
        ),
        "cpp_monitor_joiner_workload_read_events": flattened.get(
            "monitor_joiner_joined_read_events", ""
        ),
        "cpp_monitor_joiner_workload_write_events": flattened.get(
            "monitor_joiner_joined_write_events", ""
        ),
        "cpp_monitor_joiner_active_nanoseconds": flattened.get(
            "monitor_joiner_workload_processing_total_nanoseconds", ""
        ),
        "cpp_monitor_joiner_active_max_nanoseconds": flattened.get(
            "monitor_joiner_workload_processing_max_nanoseconds", ""
        ),
        "cpp_monitor_checker_workload_transactions": checked,
        "cpp_monitor_checker_workload_read_events": flattened.get(
            "monitor_checker_workload_read_events", ""
        ),
        "cpp_monitor_checker_workload_write_events": flattened.get(
            "monitor_checker_workload_write_events", ""
        ),
        "cpp_monitor_checker_active_nanoseconds": checker_ns,
        "cpp_monitor_checker_active_max_nanoseconds": flattened.get(
            "monitor_checker_workload_consume_max_nanoseconds", ""
        ),
        "head_of_line_wait_count": flattened.get(
            "monitor_joiner_missing_client_head_wait_count", ""
        ),
        "head_of_line_wait_total_ms": nanoseconds_to_milliseconds(
            flattened.get("monitor_joiner_missing_client_head_wait_total_nanoseconds", "")
        ),
        "monitor_end_fence_drain_ms": nanoseconds_to_milliseconds(
            flattened.get("monitor_joiner_end_fence_drain_nanoseconds", "")
        ),
    }


MONITOR_CAPACITY_METRICS = (
    (
        "assembler_active_transactions",
        "monitor_max_assembler_active_transactions",
        "monitor_limits_assembler_max_active_transactions",
        "monitor_assembler_active_transactions_high_water",
    ),
    (
        "assembler_events_per_transaction",
        "monitor_max_assembler_events_per_transaction",
        "monitor_limits_assembler_max_events_per_transaction",
        "monitor_assembler_observed_events_per_transaction_high_water",
    ),
    (
        "assembler_active_bytes",
        "monitor_max_assembler_active_bytes",
        "monitor_limits_assembler_max_active_bytes",
        "monitor_assembler_active_assembly_high_water_bytes",
    ),
    (
        "assembler_completed_transactions",
        "monitor_max_assembler_completed_transactions",
        "monitor_limits_assembler_max_completed_transactions",
        "monitor_assembler_completed_transactions",
    ),
    (
        "assembler_retained_identity_bytes",
        "monitor_max_assembler_retained_identity_bytes",
        "monitor_limits_assembler_max_retained_identity_bytes",
        "monitor_assembler_retained_identity_high_water_bytes",
    ),
    (
        "pending_client_records",
        "monitor_max_pending_client_records",
        "monitor_limits_joiner_max_pending_client_records",
        "monitor_joiner_pending_client_high_water_records",
    ),
    (
        "pending_client_bytes",
        "monitor_max_pending_client_bytes",
        "monitor_limits_joiner_max_pending_client_bytes",
        "monitor_joiner_pending_client_high_water_bytes",
    ),
    (
        "pending_cdc_slots",
        "monitor_max_pending_cdc_slots",
        "monitor_limits_joiner_max_pending_cdc_slots",
        "monitor_joiner_pending_cdc_high_water_slots",
    ),
    (
        "pending_cdc_bytes",
        "monitor_max_pending_cdc_bytes",
        "monitor_limits_joiner_max_pending_cdc_bytes",
        "monitor_joiner_pending_cdc_high_water_bytes",
    ),
    (
        "retained_identity_records",
        "monitor_max_retained_identity_records",
        "monitor_limits_joiner_max_retained_identity_records",
        "monitor_joiner_retained_identity_high_water_records",
    ),
    (
        "retained_identity_bytes",
        "monitor_max_retained_identity_bytes",
        "monitor_limits_joiner_max_retained_identity_bytes",
        "monitor_joiner_retained_identity_high_water_bytes",
    ),
    (
        "events_per_transaction",
        "monitor_max_events_per_transaction",
        "monitor_limits_joiner_max_events_per_transaction",
        "monitor_joiner_released_events_per_transaction_high_water",
    ),
    (
        "bootstrap_state_rows",
        "monitor_max_bootstrap_state_rows",
        "monitor_limits_joiner_max_bootstrap_state_rows",
        "monitor_joiner_bootstrap_state_high_water_rows",
    ),
    (
        "bootstrap_state_bytes",
        "monitor_max_bootstrap_state_bytes",
        "monitor_limits_joiner_max_bootstrap_state_bytes",
        "monitor_joiner_bootstrap_state_high_water_bytes",
    ),
    (
        "checker_state_bytes",
        "monitor_max_checker_state_bytes",
        "monitor_limits_checker_max_retained_state_bytes",
        "monitor_checker_engine_peak_retained_state_bytes",
    ),
    (
        "checker_mapped_identity_records",
        "monitor_max_retained_identity_records",
        "monitor_limits_checker_max_mapped_identity_records",
        "monitor_checker_mapped_identity_high_water_records",
    ),
    (
        "checker_mapped_identity_bytes",
        "monitor_max_retained_identity_bytes",
        "monitor_limits_checker_max_mapped_identity_bytes",
        "monitor_checker_mapped_identity_high_water_bytes",
    ),
    (
        "checker_events_per_transaction",
        "monitor_max_events_per_transaction",
        "monitor_limits_checker_max_events_per_transaction",
        "monitor_checker_events_per_transaction_high_water",
    ),
)


def monitor_capacity_acceptance(
    metadata: Mapping[str, Any], flattened: Mapping[str, Any]
) -> dict[str, Any]:
    checker_enabled = metadata.get("mode") == "full_e2e"
    threshold = numeric(
        metadata.get("high_water_threshold_percent"),
        "high-water threshold percent",
        minimum=0.0,
    )
    require(0 < threshold <= 100, "high-water threshold percent is outside (0,100]")
    result: dict[str, Any] = {}
    ratios: list[float] = []
    for label, metadata_field, monitor_limit_field, observed_field in MONITOR_CAPACITY_METRICS:
        if label.startswith("checker_") and not checker_enabled:
            continue
        configured = integer(metadata.get(metadata_field), metadata_field, minimum=1)
        reported = integer(flattened.get(monitor_limit_field), monitor_limit_field, minimum=1)
        require(reported == configured, f"monitor {label} limit differs from trial metadata")
        observed = numeric(flattened.get(observed_field), observed_field)
        utilization = observed * 100.0 / configured
        result[f"resource_{label}_high_water"] = observed
        result[f"resource_{label}_capacity"] = configured
        result[f"resource_{label}_high_water_percent"] = utilization
        ratios.append(utilization)

    checker_limit = integer(metadata.get("monitor_max_checker_state_bytes"), "checker state limit", minimum=1)
    require(
        integer(flattened.get("monitor_limits_joiner_max_checker_state_bytes"), "joiner checker state limit", minimum=1)
        == checker_limit,
        "joiner checker-state limit differs from trial metadata",
    )
    if checker_enabled:
        require(
            integer(flattened.get("monitor_checker_engine_retained_state_capacity_bytes"), "checker engine capacity", minimum=1)
            == checker_limit,
            "checker engine capacity differs from trial metadata",
        )
        require(
            integer(flattened.get("monitor_limits_checker_max_events_per_transaction"), "checker event limit", minimum=1)
            == int(metadata["monitor_max_events_per_transaction"]),
            "checker event limit differs from trial metadata",
        )
        require(
            integer(flattened.get("monitor_limits_checker_max_mapped_identity_records"), "checker identity record limit", minimum=1)
            == int(metadata["monitor_max_retained_identity_records"]),
            "checker identity record limit differs from trial metadata",
        )
        require(
            integer(flattened.get("monitor_limits_checker_max_mapped_identity_bytes"), "checker identity byte limit", minimum=1)
            == int(metadata["monitor_max_retained_identity_bytes"]),
            "checker identity byte limit differs from trial metadata",
        )
    result["resource_high_water_max_percent"] = max(ratios)
    result["resource_high_water_threshold_percent"] = threshold
    result["resource_high_water_accepted"] = max(ratios) < threshold
    require(result["resource_high_water_accepted"], "monitor high-water threshold was reached or exceeded")
    return result


def monitor_terminal_queue_acceptance(flattened: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "monitor_assembler_active_transactions",
        "monitor_assembler_active_assembly_bytes",
        "monitor_joiner_pending_client_records",
        "monitor_joiner_pending_client_bytes",
        "monitor_joiner_pending_cdc_slots",
        "monitor_joiner_pending_cdc_bytes",
        "monitor_joiner_unmatched_client_records",
        "monitor_joiner_unmatched_workload_cdc_slots",
        "monitor_joiner_end_fence_drain_current_nanoseconds",
    )
    result: dict[str, Any] = {}
    for field in fields:
        value = numeric(flattened.get(field), field)
        result[f"terminal_{field.removeprefix('monitor_')}"] = value
        require(value == 0, f"terminal monitor queue is nonzero: {field}")
    result["terminal_monitor_queues_drained"] = True
    return result


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
    fence_row_seen = False
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
                fence_row_seen = True
            # Frames are contiguous and never interleave, so the first END at
            # or after the fence row closes the fence's own transaction.
            # Connector transaction IDs stay per-record diagnostics: the
            # pinned YugabyteDB connector never repeats one ID across records.
            if (
                transaction_end
                and fence_row_seen
                and envelope.get("status") == "END"
                and isinstance(envelope.get("id"), str)
            ):
                return stream_ms
    require(phase in phases, f"CDC artifact has no {phase} fence row")
    if transaction_end:
        raise ReportError(f"CDC artifact has no transaction END after {phase}")
    return phases[phase]


def component_fence_window(document: Any, run_id: str) -> dict[str, Any]:
    """Find both component-boundary transaction END records in one scan."""

    require(isinstance(document, list), "CDC XRANGE artifact is not an array")
    requested = ("LOAD_FINISHED", "END_FENCE")
    fence_rows_seen: set[str] = set()
    boundaries: dict[str, str] = {}
    for entry in document:
        stream_id, fields = xrange_fields(entry, "CDC")
        for value in fields.values():
            try:
                envelope = json.loads(value)
            except json.JSONDecodeError:
                continue
            if not isinstance(envelope, dict):
                continue
            entry_phases: dict[str, int] = {}
            find_phase(envelope, run_id, entry_phases, stream_id_milliseconds(stream_id))
            for phase in requested:
                if phase not in entry_phases:
                    continue
                transaction = envelope.get("transaction")
                require(
                    isinstance(transaction, dict)
                    and isinstance(transaction.get("id"), str)
                    and transaction["id"] != "",
                    f"CDC {phase} row has no transaction ID",
                )
                fence_rows_seen.add(phase)
            if envelope.get("status") != "END" or not isinstance(envelope.get("id"), str):
                continue
            # The first END at or after a fence row closes that fence's own
            # transaction; frames never interleave and connector transaction
            # IDs are per-record diagnostics, not cross-record identities.
            for phase in fence_rows_seen:
                boundaries.setdefault(phase, stream_id)

    result: dict[str, Any] = {}
    for phase in requested:
        require(phase in fence_rows_seen, f"CDC artifact has no {phase} fence row")
        require(
            phase in boundaries,
            f"CDC artifact has no transaction END after {phase}",
        )
        result[phase] = {
            "transaction_end_stream_id": boundaries[phase],
            "epoch_ms": stream_id_milliseconds(boundaries[phase]),
        }
    require(
        tuple(map(int, result["LOAD_FINISHED"]["transaction_end_stream_id"].split("-")))
        < tuple(map(int, result["END_FENCE"]["transaction_end_stream_id"].split("-"))),
        "component fence transaction END order is invalid",
    )
    return result


def optional_rate(count: Any, seconds: float) -> float | str:
    values = finite_numbers((count,))
    return values[0] / seconds if values and seconds > 0 else ""


def validate_outcome_metrics(
    outcome: Mapping[str, Any], benchbase: Mapping[str, Any], benchmark: str
) -> None:
    require(outcome.get("kind") == "transaction_outcome_metrics", "outcome metric kind differs")
    require(outcome.get("benchmark") == benchmark, "outcome benchmark differs from metadata")
    require(outcome.get("scope") == "measurement_window", "outcome scope differs")
    require(
        outcome.get("window_semantics") == "fully_contained_logical_invocations",
        "outcome window semantics differ",
    )
    elapsed_ns = integer(outcome.get("elapsed_time_nanoseconds"), "outcome elapsed", minimum=1)
    require(
        elapsed_ns == integer(benchbase.get("elapsed_time_nanoseconds"), "BenchBase elapsed", minimum=1),
        "outcome and BenchBase elapsed windows differ",
    )
    measured = integer(outcome.get("measured_requests"), "outcome measured requests")
    require(
        measured == integer(benchbase.get("measured_count"), "BenchBase measured requests"),
        "outcome and BenchBase measured counts differ",
    )
    require(outcome.get("phase_ids") == benchbase.get("phase_ids"), "outcome phase IDs differ")
    raw_counts = benchbase.get("transaction_counts")
    types = outcome.get("transaction_types")
    totals = outcome.get("totals")
    require(isinstance(raw_counts, Mapping), "BenchBase transaction counts are missing")
    require(isinstance(types, Mapping) and types, "outcome transaction types are missing")
    require(isinstance(totals, Mapping), "outcome totals are missing")
    summed = {name: 0 for name in OUTCOME_COUNTER_FIELDS}
    for transaction_name, raw_counters in types.items():
        require(isinstance(transaction_name, str) and transaction_name, "outcome transaction name is invalid")
        require(isinstance(raw_counters, Mapping), "outcome transaction counters are not an object")
        counters = {
            name: integer(raw_counters.get(name), f"{transaction_name}.{name}")
            for name in OUTCOME_COUNTER_FIELDS
        }
        require(
            counters["logical_transactions_started"]
            == counters["successful_transactions"]
            + counters["terminal_aborted_transactions"]
            + counters["error_transactions"]
            + counters["unknown_status_transactions"],
            f"{transaction_name} logical outcomes do not reconcile",
        )
        require(
            counters["physical_attempts_started"]
            == counters["successful_transactions"]
            + counters["definitively_aborted_attempts"]
            + counters["ambiguous_attempts"],
            f"{transaction_name} physical attempts do not reconcile",
        )
        require(
            counters["retry_attempts"]
            == counters["physical_attempts_started"] - counters["logical_transactions_started"],
            f"{transaction_name} retries do not reconcile",
        )
        require(
            counters["ambiguous_attempts"] == counters["unknown_status_transactions"],
            f"{transaction_name} ambiguity counters differ",
        )
        retry_failure_events = (
            counters["retryable_failure_events"]
            + counters["retry_different_events"]
        )
        require(
            counters["retry_attempts"]
            <= retry_failure_events
            <= counters["retry_attempts"] + counters["terminal_aborted_transactions"],
            f"{transaction_name} retry failure event algebra differs",
        )
        require(
            counters["logical_transactions_started"]
            == integer(raw_counts.get(transaction_name, 0), f"raw {transaction_name} count"),
            f"{transaction_name} raw/outcome count differs",
        )
        for name, value in counters.items():
            summed[name] += value
    for name, value in summed.items():
        require(integer(totals.get(name), f"outcome total {name}") == value, f"outcome total {name} differs")
    require(summed["logical_transactions_started"] == measured, "outcome logical total differs")
    if benchmark == "tpcc":
        require(
            set(types) == {"NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"},
            "TPC-C outcome transaction roster differs",
        )
        expected_rate = (
            integer(types["NewOrder"].get("successful_transactions"), "NewOrder successes")
            * 60.0
            / (elapsed_ns / 1_000_000_000.0)
        )
        actual_rate = numeric(outcome.get("new_order_per_minute"), "NewOrder/min")
        require(
            math.isclose(actual_rate, expected_rate, rel_tol=1e-15, abs_tol=1e-9),
            "NewOrder/min differs",
        )
        require(outcome.get("new_order_rate_label") == "NewOrder/min", "NewOrder rate label differs")


def add_interval_and_event_metrics(
    row: dict[str, Any],
    metadata: Mapping[str, Any],
    benchbase: Mapping[str, Any],
    outcome: Mapping[str, Any] | None,
    analysis_window: Mapping[str, Any] | None,
    monitor: Mapping[str, Any] | None,
) -> None:
    elapsed_ns = numeric(
        benchbase.get("elapsed_time_nanoseconds"), "BenchBase elapsed nanoseconds", minimum=1.0
    )
    workload_seconds = elapsed_ns / 1_000_000_000.0
    measured_start_ms = int(numeric(benchbase.get("measured_start_epoch_ms"), "measured start"))
    measured_end_ms = int(numeric(benchbase.get("measured_end_epoch_ms"), "measured end"))
    require(measured_end_ms >= measured_start_ms, "BenchBase measured window is reversed")
    row["client_measurement_window_seconds"] = workload_seconds

    if outcome is not None:
        totals = outcome.get("totals")
        require(isinstance(totals, Mapping), "outcome totals are missing")
        logical = numeric(
            totals.get("logical_transactions_started"), "logical transactions"
        )
        successful = numeric(totals.get("successful_transactions"), "successful transactions")
        row["logical_transactions_started"] = logical
        row["logical_transactions_started_per_second_client_measurement_window"] = (
            logical / workload_seconds
        )
        row["logical_transactions_per_second"] = logical / workload_seconds
        row["successful_transactions"] = successful
        row["successful_transactions_per_second"] = successful / workload_seconds
        for counter in (
            "logical_transactions_started",
            "physical_attempts_started",
            "retry_attempts",
            "retryable_failure_events",
            "terminal_aborted_transactions",
            "error_transactions",
            "unknown_status_transactions",
            "ambiguous_attempts",
            "retry_different_events",
        ):
            row[counter] = totals.get(counter, "")
            row[f"{counter}_per_second_client_measurement_window"] = optional_rate(
                totals.get(counter), workload_seconds
            )
        row["attempts_per_second_client_measurement_window"] = row[
            "physical_attempts_started_per_second_client_measurement_window"
        ]
        failed = numeric(totals.get("error_transactions"), "error transactions") + numeric(
            totals.get("unknown_status_transactions"), "unknown statuses"
        )
        row["failed_transactions"] = failed
        row["failed_transactions_per_second_client_measurement_window"] = (
            failed / workload_seconds
        )
        row["new_order_per_minute"] = outcome.get("new_order_per_minute", "")
        row["new_order_rate_label"] = outcome.get("new_order_rate_label", "")
    else:
        logical = numeric(benchbase.get("measured_count"), "BenchBase measured count")
        successful = logical
        row["logical_transactions_started"] = logical
        row["logical_transactions_started_per_second_client_measurement_window"] = (
            logical / workload_seconds
        )
        row["logical_transactions_per_second"] = logical / workload_seconds
        row["successful_transactions"] = successful
        row["successful_transactions_per_second"] = successful / workload_seconds
        row["attempts_per_second_client_measurement_window"] = (
            successful / workload_seconds
        )
        row["new_order_per_minute"] = ""
        row["new_order_rate_label"] = ""

    require(isinstance(analysis_window, Mapping), "analysis window document is missing")
    client_window = analysis_window.get("client_measurement_window")
    require(isinstance(client_window, Mapping), "client measurement window is missing")
    require(
        client_window.get("definition") == "benchbase_measured_request_window",
        "client measurement window definition differs",
    )
    client_start_ms = int(
        numeric(client_window.get("start_epoch_ms"), "client measurement start")
    )
    client_end_ms = int(
        numeric(client_window.get("end_epoch_ms"), "client measurement end")
    )
    require(
        (client_start_ms, client_end_ms) == (measured_start_ms, measured_end_ms),
        "analysis and BenchBase client measurement windows differ",
    )
    row["client_measurement_window_definition"] = client_window["definition"]
    row["client_measurement_window_start_epoch_ms"] = client_start_ms
    row["client_measurement_window_end_epoch_ms"] = client_end_ms

    component_window = analysis_window.get("component_workload_window")
    if metadata.get("mode") != "full_e2e":
        require(
            component_window is None,
            "uninstrumented baseline has a component workload window",
        )
        require(
            analysis_window.get("final_verdict") is None,
            "uninstrumented baseline has a final verdict boundary",
        )
    else:
        require(
            isinstance(component_window, Mapping),
            "full-E2E component workload window is missing",
        )
        require(
            component_window.get("definition")
            == "after_load_finished_transaction_end_id_through_end_fence_transaction_end_id_inclusive",
            "component workload window definition differs",
        )
        require(
            component_window.get("start_boundary") == "LOAD_FINISHED"
            and component_window.get("end_boundary") == "END_FENCE",
            "component workload boundary labels differ",
        )
        require(
            component_window.get("start_exclusive") is True
            and component_window.get("end_inclusive") is True,
            "component workload boundary inclusivity differs",
        )
        after_stream_id = component_window.get("after_stream_id")
        through_stream_id = component_window.get("through_stream_id")
        require(
            isinstance(after_stream_id, str)
            and isinstance(through_stream_id, str)
            and STREAM_ID_PATTERN.fullmatch(after_stream_id) is not None
            and STREAM_ID_PATTERN.fullmatch(through_stream_id) is not None,
            "component workload Redis stream IDs are invalid",
        )
        component_start_ms = int(
            numeric(component_window.get("start_epoch_ms"), "component workload start")
        )
        component_end_ms = int(
            numeric(component_window.get("end_epoch_ms"), "component workload end")
        )
        require(component_end_ms > component_start_ms, "component workload window is not positive")
        require(
            stream_id_milliseconds(after_stream_id) == component_start_ms
            and stream_id_milliseconds(through_stream_id) == component_end_ms,
            "component workload Redis IDs and millisecond bounds differ",
        )
        component_seconds = (component_end_ms - component_start_ms) / 1000.0
        row["component_workload_window_definition"] = component_window["definition"]
        row["component_workload_window_start_epoch_ms"] = component_start_ms
        row["component_workload_window_end_epoch_ms"] = component_end_ms
        row["component_workload_window_after_stream_id"] = after_stream_id
        row["component_workload_window_through_stream_id"] = through_stream_id
        row["component_workload_window_start_exclusive"] = True
        row["component_workload_window_end_inclusive"] = True
        row["component_workload_window_seconds"] = component_seconds

        final_verdict = analysis_window.get("final_verdict")
        require(isinstance(final_verdict, Mapping), "final verdict boundary is missing")
        final_stream_id = final_verdict.get("stream_id")
        require(
            isinstance(final_stream_id, str)
            and STREAM_ID_PATTERN.fullmatch(final_stream_id) is not None,
            "final verdict Redis stream ID is invalid",
        )
        final_verdict_ms = int(
            numeric(final_verdict.get("epoch_ms"), "final verdict timestamp")
        )
        require(
            stream_id_milliseconds(final_stream_id) == final_verdict_ms,
            "final verdict Redis ID and timestamp differ",
        )
        require(
            row.get("status_terminal_stream_id") == final_stream_id
            and int(numeric(row.get("status_terminal_epoch_ms"), "terminal status timestamp"))
            == final_verdict_ms,
            "analysis boundary and FINAL status record differ",
        )
        require(
            final_verdict_ms >= component_end_ms,
            "FINAL status precedes the component workload end",
        )
        row["final_verdict_stream_id"] = final_stream_id
        row["final_verdict_epoch_ms"] = final_verdict_ms

        require(row.get("redis_selection_window_bounded") is True, "CDC selection is not bounded")
        require(
            row.get("redis_selection_window_after_stream_id") == after_stream_id
            and row.get("redis_selection_window_through_stream_id") == through_stream_id
            and row.get("redis_selection_window_start_exclusive") is True
            and row.get("redis_selection_window_end_inclusive") is True,
            "CDC selection and component workload windows differ",
        )
        require(isinstance(monitor, Mapping), "full-E2E monitor metrics are missing")
        component_tail_ms = final_verdict_ms - component_end_ms
        row["client_measurement_end_to_verdict_tail_ms"] = (
            final_verdict_ms - measured_end_ms
        )
        row["component_workload_end_to_verdict_tail_ms"] = component_tail_ms
        drain_limit_ms = int(
            numeric(metadata.get("drain_timeout_seconds"), "drain timeout", minimum=1.0)
        ) * 1000
        joiner = monitor.get("joiner")
        require(isinstance(joiner, Mapping), "monitor joiner metrics are missing")
        end_fence_drain_ms = numeric(
            joiner.get("end_fence_drain_nanoseconds"),
            "monitor end-fence drain nanoseconds",
        ) / 1_000_000.0
        row["monitor_end_fence_drain_ms"] = end_fence_drain_ms
        row["drain_timeout_ms"] = drain_limit_ms
        row["drain_tail_accepted"] = (
            component_tail_ms <= drain_limit_ms
            and end_fence_drain_ms <= drain_limit_ms
        )
        require(row["drain_tail_accepted"], "component workload drain timeout was exceeded")

        cdc_rows = numeric(
            row.get("redis_cdc_workload_row_count"), "CDC workload row events"
        )
        cdc_control_rows = numeric(
            row.get("redis_cdc_control_row_count"), "CDC control row events"
        )
        cdc_all_rows = numeric(row.get("redis_cdc_row_count"), "all CDC row events")
        require(
            cdc_rows + cdc_control_rows == cdc_all_rows,
            "CDC workload/control row accounting does not reconcile",
        )
        require(cdc_control_rows == 1, "component window has an unexpected control-row count")
        row["cdc_ingestion_interval_definition"] = component_window["definition"]
        row["cdc_ingestion_interval_seconds"] = component_seconds
        row["cdc_ingestion_workload_rows"] = cdc_rows
        row["cdc_ingestion_control_rows"] = cdc_control_rows
        row["cdc_ingestion_workload_rows_per_second"] = cdc_rows / component_seconds
        for lag_kind in ("source", "delivery"):
            sample_count = row.get(
                f"redis_cdc_workload_{lag_kind}_lag_sample_count", ""
            )
            row[f"cdc_ingestion_workload_{lag_kind}_lag_sample_count"] = sample_count
            require(
                numeric(sample_count, f"CDC workload {lag_kind} lag samples") == cdc_rows,
                f"CDC workload {lag_kind} lag sample count differs from row count",
            )
            for statistic in ("current", "max", "mean", "p50", "p95", "p99"):
                row[f"cdc_ingestion_workload_{lag_kind}_lag_ms_{statistic}"] = row.get(
                    f"redis_cdc_workload_{lag_kind}_lag_ms_{statistic}", ""
                )

        def active_rate(count: float, nanoseconds_field: str, label: str) -> float:
            nanoseconds = numeric(
                row.get(nanoseconds_field), f"{label} active nanoseconds", minimum=1.0
            )
            return count * 1_000_000_000.0 / nanoseconds

        client_transactions = numeric(
            row.get("client_client_txn_count"), "client workload-process transactions"
        )
        client_published_transactions = numeric(
            row.get("client_session_done_counters_published_transactions"),
            "client published transaction accounting",
        )
        require(
            client_published_transactions == client_transactions,
            "client stream and SessionDone transaction counters do not reconcile",
        )
        expected_cdc_transactions = client_transactions + 1
        require(
            numeric(row.get("redis_begin_count"), "CDC BEGIN records")
            == expected_cdc_transactions
            and numeric(row.get("redis_end_count"), "CDC END records")
            == expected_cdc_transactions
            and numeric(
                row.get("redis_completed_transaction_count"),
                "CDC completed transactions",
            )
            == expected_cdc_transactions,
            "component CDC transaction boundaries do not reconcile",
        )
        client_reads = numeric(row.get("client_read_event_count"), "client read events")
        assembler_transactions = numeric(
            row.get("cpp_monitor_assembler_workload_transactions"),
            "assembler workload transactions",
        )
        assembler_raw_rows = numeric(
            row.get("cpp_monitor_assembler_workload_raw_row_events"),
            "assembler workload raw row events",
        )
        assembler_events = numeric(
            row.get("cpp_monitor_assembler_workload_write_events"),
            "assembler workload normalized events",
        )
        joiner_transactions = numeric(
            row.get("cpp_monitor_joiner_workload_transactions"),
            "joiner workload transactions",
        )
        joiner_reads = numeric(
            row.get("cpp_monitor_joiner_workload_read_events"),
            "joiner workload read events",
        )
        joiner_writes = numeric(
            row.get("cpp_monitor_joiner_workload_write_events"),
            "joiner workload write events",
        )
        checker_transactions = numeric(
            row.get("cpp_monitor_checker_workload_transactions"),
            "checker workload transactions",
        )
        checker_reads = numeric(
            row.get("cpp_monitor_checker_workload_read_events"),
            "checker workload read events",
        )
        checker_writes = numeric(
            row.get("cpp_monitor_checker_workload_write_events"),
            "checker workload write events",
        )
        require(
            client_transactions
            == assembler_transactions
            == joiner_transactions
            == checker_transactions,
            "component workload transaction counters do not reconcile",
        )
        require(
            cdc_rows == assembler_raw_rows,
            "CDC and assembler workload raw-row counters do not reconcile",
        )
        require(
            assembler_events == joiner_writes == checker_writes,
            "normalized workload write-event counters do not reconcile",
        )
        require(
            client_reads == joiner_reads == checker_reads,
            "component workload read-event counters do not reconcile",
        )
        row["component_workload_client_transactions"] = client_transactions
        row["component_workload_client_read_events"] = client_reads
        row["component_workload_cdc_raw_row_events"] = cdc_rows
        row["component_workload_application_write_events"] = assembler_events
        component_counts = {
            "assembler": (assembler_transactions, assembler_raw_rows),
            "joiner": (joiner_transactions, joiner_reads + joiner_writes),
            "checker": (checker_transactions, checker_reads + checker_writes),
        }
        service_scopes = {
            "assembler": "accept_call_service",
            "joiner": "ready_prefix_release_service",
            "checker": "engine_consume_service",
        }
        for component, (transaction_count, event_count) in component_counts.items():
            active_ns_field = f"cpp_monitor_{component}_active_nanoseconds"
            scope = service_scopes[component]
            row[f"cpp_monitor_{component}_{scope}_transactions_per_second"] = (
                active_rate(transaction_count, active_ns_field, component)
            )
            row[f"cpp_monitor_{component}_{scope}_events_per_second"] = active_rate(
                event_count, active_ns_field, component
            )

        pipeline_seconds = (final_verdict_ms - component_start_ms) / 1000.0
        require(pipeline_seconds > 0, "evidence pipeline interval is not positive")
        row["evidence_pipeline_interval_definition"] = (
            "load_finished_transaction_end_through_final_checker_verdict"
        )
        row["evidence_pipeline_interval_seconds"] = pipeline_seconds
        row["evidence_pipeline_workload_transactions"] = checker_transactions
        row["evidence_pipeline_workload_events"] = checker_reads + checker_writes
        row["evidence_pipeline_transactions_per_second"] = (
            checker_transactions / pipeline_seconds
        )
        row["evidence_pipeline_events_per_second"] = (
            (checker_reads + checker_writes) / pipeline_seconds
        )

    redis_entry_count = row.get("redis_entry_count", "")
    client_entry_count = row.get("client_entry_count", "")
    row["redis_cdc_logical_bytes_per_entry"] = (
        numeric(row["redis_logical_cdc_stream_bytes"], "CDC logical bytes")
        / numeric(redis_entry_count, "CDC Redis entries", minimum=1.0)
        if row.get("redis_logical_cdc_stream_bytes") not in (None, "")
        and finite_numbers((redis_entry_count,))
        and float(redis_entry_count) > 0
        else ""
    )
    row["redis_cdc_logical_bytes_per_row_event"] = (
        numeric(row["redis_logical_cdc_stream_bytes"], "CDC logical bytes")
        / numeric(row.get("redis_cdc_row_count"), "CDC row events", minimum=1.0)
        if row.get("redis_logical_cdc_stream_bytes") not in (None, "")
        and finite_numbers((row.get("redis_cdc_row_count"),))
        and float(row["redis_cdc_row_count"]) > 0
        else ""
    )
    client_logical_bytes = row.get("redis_logical_client_stream_bytes")
    if client_logical_bytes in (None, ""):
        client_logical_bytes = row.get("redis_logical_measurement_stream_bytes")
    row["redis_client_logical_bytes_per_entry"] = (
        numeric(client_logical_bytes, "client logical bytes")
        / numeric(client_entry_count, "client Redis entries", minimum=1.0)
        if client_logical_bytes not in (None, "")
        and finite_numbers((client_entry_count,))
        and float(client_entry_count) > 0
        else ""
    )
    cdc_memory = row.get("redis_memory_keys_cdc", "")
    client_memory = row.get("redis_memory_keys_client", "")
    if client_memory in (None, ""):
        client_memory = row.get("redis_memory_keys_measurement_client", "")
    row["redis_cdc_allocated_bytes_per_entry"] = (
        numeric(cdc_memory, "CDC Redis allocation")
        / numeric(redis_entry_count, "CDC Redis entries", minimum=1.0)
        if finite_numbers((cdc_memory,))
        and finite_numbers((redis_entry_count,))
        and float(redis_entry_count) > 0
        else ""
    )
    row["redis_client_allocated_bytes_per_entry"] = (
        numeric(client_memory, "client Redis allocation")
        / numeric(client_entry_count, "client Redis entries", minimum=1.0)
        if finite_numbers((client_memory,))
        and finite_numbers((client_entry_count,))
        and float(client_entry_count) > 0
        else ""
    )
    evidence_entries = sum(
        values[0]
        for value in (redis_entry_count, client_entry_count)
        if (values := finite_numbers((value,)))
    )
    for direction in ("input", "output"):
        network_bytes = row.get(f"redis_network_{direction}_bytes_delta", "")
        row[f"redis_network_{direction}_bytes_per_evidence_entry"] = (
            numeric(network_bytes, f"Redis network {direction} bytes") / evidence_entries
            if finite_numbers((network_bytes,)) and evidence_entries > 0
            else ""
        )


def trial_row(trial_dir: Path) -> dict[str, Any]:
    metadata = load_json(trial_dir / "metadata.json", required=True)
    require(isinstance(metadata, dict), f"metadata is not an object: {trial_dir}")
    mode = metadata.get("mode")
    require(mode in MODES, f"unknown performance mode in {trial_dir}: {mode!r}")
    benchmark = metadata.get("benchmark", "ycsb")
    require(benchmark in {"ycsb", "tpcc"}, f"unknown benchmark in {trial_dir}: {benchmark!r}")
    canonical_utc_second(metadata.get("loader_timestamp_utc"), "loader timestamp")

    row: dict[str, Any] = {field: "" for field in IDENTITY_FIELDS}
    for key, value in metadata.items():
        normalized_key = normalize_name(str(key))
        if normalized_key == "configured_modes":
            require(
                isinstance(value, list),
                f"configured performance modes are not an array: {trial_dir}",
            )
            row["configured_modes_json"] = json.dumps(
                value, ensure_ascii=False, separators=(",", ":")
            )
        else:
            row[normalized_key] = value
    row["artifact_dir"] = str(trial_dir.resolve())

    benchbase = load_json(trial_dir / "benchbase_metrics.json", required=True)
    require(isinstance(benchbase, dict), "BenchBase metrics are not an object")
    row.update(benchbase_aliases(benchbase))
    flatten("benchbase", benchbase, row)
    outcome = load_json(trial_dir / "outcome_metrics.json")
    if benchmark == "tpcc":
        require(isinstance(outcome, dict), "TPC-C measurement-window outcomes are missing")
        validate_outcome_metrics(outcome, benchbase, benchmark)
        outcome_types = outcome.get("transaction_types")
        require(
            isinstance(outcome_types, Mapping)
            and set(outcome_types)
            == {"NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"},
            "TPC-C outcome transaction roster differs",
        )
        require(outcome.get("new_order_rate_label") == "NewOrder/min", "NewOrder rate label differs")
    elif outcome is not None:
        require(isinstance(outcome, dict), "outcome metrics are not an object")
        validate_outcome_metrics(outcome, benchbase, benchmark)
    if isinstance(outcome, Mapping):
        clean_totals = outcome.get("totals")
        require(isinstance(clean_totals, Mapping), "outcome totals are missing")
        for counter in ("unknown_status_transactions", "ambiguous_attempts"):
            require(
                integer(clean_totals.get(counter), f"outcome total {counter}") == 0,
                f"clean performance trial has nonzero {counter}",
            )

    if mode == "full_e2e":
        for required_artifact in (
            "redis_metrics.json",
            "client_metrics.json",
            "cdc_stream.json",
            "client_stream.json",
            "status_stream.json",
            "analysis_window.json",
        ):
            require(
                (trial_dir / required_artifact).is_file(),
                f"full-E2E artifact is missing: {required_artifact}",
            )

    for filename, prefix in (
        ("outcome_metrics.json", "outcomes"),
        ("redis_metrics.json", "redis"),
        ("client_metrics.json", "client"),
        ("capture_metrics.json", "capture"),
        ("network_metrics.json", "redis_network"),
        ("redis_usage.json", "redis_memory"),
        ("redis_configuration.json", "redis_configuration"),
        ("marker_storage.json", "marker_storage"),
        ("loader_validation.json", "loader_validation"),
        ("bootstrap_manifest.json", "bootstrap"),
        ("analysis_window.json", "analysis_window"),
    ):
        document = load_json(trial_dir / filename)
        if document is not None:
            flatten(prefix, document, row)

    backend_telemetry = load_json(trial_dir / "backend_telemetry.json")
    if metadata.get("backend_telemetry_expected"):
        require(
            backend_telemetry is not None,
            "required backend performance telemetry is missing",
        )
    if backend_telemetry is not None:
        require(
            isinstance(backend_telemetry, Mapping)
            and backend_telemetry.get("kind") == "backend_performance_telemetry",
            "backend performance telemetry has an invalid shape",
        )
        telemetry_backend = backend_telemetry.get("backend")
        require(
            isinstance(telemetry_backend, str) and telemetry_backend,
            "backend performance telemetry identity is missing",
        )
        metadata_backend = metadata.get("backend")
        if metadata_backend is not None:
            require(
                telemetry_backend == metadata_backend,
                "backend performance telemetry identity differs",
            )
        compact_telemetry = {
            key: value
            for key, value in backend_telemetry.items()
            if key not in {"samples", "summary"}
        }
        flatten("backend_telemetry", compact_telemetry, row)
        telemetry_summary = backend_telemetry.get("summary")
        require(isinstance(telemetry_summary, Mapping), "backend telemetry summary is missing")
        telemetry_metrics = telemetry_summary.get("metrics")
        telemetry_series = telemetry_summary.get("series")
        require(isinstance(telemetry_metrics, Mapping), "backend metric summary is missing")
        require(isinstance(telemetry_series, list), "backend series summary is missing")
        flatten("backend_telemetry_summary", telemetry_metrics, row)
        row["backend_telemetry_summary_series_count"] = len(telemetry_series)

    bootstrap = load_json(trial_dir / "bootstrap_manifest.json", required=True)
    require(isinstance(bootstrap, dict), "bootstrap manifest is not an object")
    counts = bootstrap.get("table_row_counts")
    require(isinstance(counts, list) and counts, "bootstrap table counts are incomplete")
    require(all(isinstance(item, dict) for item in counts), "bootstrap table count is not an object")
    row_count = sum(int(item.get("row_count", -1)) for item in counts)
    require(row_count >= 0, "bootstrap row count is invalid")
    if benchmark == "ycsb":
        expected_rows = int(metadata.get("scale_factor", 0)) * 1000
        require(row_count == expected_rows, f"bootstrap row count {row_count} != expected {expected_rows}")
    digest = bootstrap.get("canonical_state_digest")
    require(
        isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest) is not None,
        "bootstrap canonical digest is invalid",
    )
    row["bootstrap_row_count"] = row_count
    row["bootstrap_canonical_state_digest"] = digest

    marker_storage = load_json(trial_dir / "marker_storage.json", required=True)
    require(isinstance(marker_storage, dict), "marker storage evidence is not an object")
    require(
        marker_storage.get("contract") == "append_only_per_committed_attempt",
        "marker storage contract differs",
    )
    require(
        marker_storage.get("mode") == mode,
        "marker storage mode differs",
    )
    marker_start_rows = integer(
        marker_storage.get("workload_start_row_count"), "marker workload-start rows"
    )
    marker_end_rows = integer(
        marker_storage.get("complete_run_end_row_count"), "marker complete-run rows"
    )
    marker_workload_end_rows = integer(
        marker_storage.get("workload_end_row_count"), "marker workload-end rows"
    )
    marker_workload_allocated_bytes = integer(
        marker_storage.get("workload_end_allocated_bytes"),
        "marker workload-end allocated bytes",
    )
    marker_allocated_bytes = integer(
        marker_storage.get("complete_run_end_allocated_bytes"),
        "marker complete-run allocated bytes",
    )
    require(marker_start_rows == 0, "marker table was not empty at workload start")
    require(
        marker_workload_end_rows == marker_end_rows,
        "marker rows changed after workload quiescence",
    )
    require(
        marker_workload_allocated_bytes == marker_allocated_bytes,
        "marker allocation changed after workload quiescence",
    )
    require(marker_allocated_bytes >= 0, "marker allocated bytes are negative")
    if mode == "benchbase_uninstrumented":
        require(marker_end_rows == 0, "uninstrumented mode contains marker rows")

    loader_validation = load_json(trial_dir / "loader_validation.json", required=True)
    require(isinstance(loader_validation, dict), "loader validation is not an object")
    require(loader_validation.get("validated") is True, "loader validation did not pass")
    require(
        loader_validation.get("loader_batch_size") == metadata.get("loader_batch_size"),
        "loader batch size differs between metadata and validation",
    )
    expected_counts = loader_validation.get("expected_table_row_counts")
    require(isinstance(expected_counts, list) and expected_counts, "loader expected counts are absent")
    # Loader state proof is outside the measured workload path and is required
    # identically in both formal modes.
    declaration_required = True
    require(
        loader_validation.get("declaration_available") is declaration_required,
        "loader declaration availability differs from mode contract",
    )
    require(
        loader_validation.get("observed_table_row_counts") == expected_counts
        and sorted(counts, key=lambda item: item["table"])
        == sorted(expected_counts, key=lambda item: item["table"]),
        "loader/bootstrap expected/observed counts differ",
    )
    require(
        loader_validation.get("declared_table_row_counts")
        == (expected_counts if declaration_required else None),
        "loader declared counts differ from mode contract",
    )

    redis_logical_total = 0
    stream_documents: dict[str, Any] = {}
    for filename, label in (
        ("cdc_stream.json", "cdc"),
        ("client_stream.json", "client"),
        ("status_stream.json", "status"),
        ("measurement_stream.json", "measurement"),
    ):
        document = load_json(trial_dir / filename)
        stream_documents[label] = document
        if document is None:
            row[f"redis_logical_{label}_stream_bytes"] = ""
            continue
        logical_bytes = xrange_logical_bytes(document, label)
        row[f"redis_logical_{label}_stream_bytes"] = logical_bytes
        redis_logical_total += logical_bytes
    row["redis_logical_total_stream_bytes"] = redis_logical_total if redis_logical_total else ""

    monitor = load_json(trial_dir / "monitor_metrics.json")
    if mode == "full_e2e":
        require(isinstance(monitor, dict), "full-E2E C++ monitor metrics are missing")
        require(
            monitor.get("processing_mode") == "audited",
            "full-E2E C++ monitor is not in audited mode",
        )
    else:
        require(monitor is None, "uninstrumented baseline contains C++ monitor metrics")
    monitor_flat: dict[str, Any] = {}
    if monitor is not None:
        flatten("monitor", monitor, monitor_flat)
        row.update(monitor_flat)
        row.update(monitor_aliases(monitor_flat))

    status_document = stream_documents.get("status")
    status = parse_status_xrange(status_document)
    row.update(status)
    analysis_window = load_json(trial_dir / "analysis_window.json")
    add_interval_and_event_metrics(
        row,
        metadata,
        benchbase,
        outcome if isinstance(outcome, dict) else None,
        analysis_window if isinstance(analysis_window, dict) else None,
        monitor if isinstance(monitor, dict) else None,
    )

    redis_modes = {"full_e2e"}
    if mode in redis_modes:
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
    if mode == "full_e2e":
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
        require(
            monitor_flat.get("monitor_limits_joiner_max_bootstrap_state_rows")
            == metadata.get("monitor_max_bootstrap_state_rows"),
            "monitor bootstrap-state row limit differs from trial metadata",
        )
        require(
            monitor_flat.get("monitor_limits_joiner_max_bootstrap_state_bytes")
            == metadata.get("monitor_max_bootstrap_state_bytes"),
            "monitor bootstrap-state byte limit differs from trial metadata",
        )

    if "high_water_threshold_percent" in metadata:
        threshold = numeric(
            metadata.get("high_water_threshold_percent"),
            "high-water threshold percent",
        )
        require(0 < threshold <= 100, "high-water threshold percent is outside (0,100]")
        utilization_values: list[float] = []
        if mode in redis_modes:
            redis_info = parse_redis_info(trial_dir / "redis_info_after.txt")
            require(
                redis_info["maxmemory"] == int(metadata["redis_maxmemory_bytes"]),
                "Redis INFO maxmemory differs from trial metadata",
            )
            redis_peak_percent = (
                redis_info["used_memory_peak"] * 100.0 / redis_info["maxmemory"]
            )
            row.update(
                {
                    "redis_used_memory_bytes": redis_info["used_memory"],
                    "redis_used_memory_peak_bytes": redis_info["used_memory_peak"],
                    "redis_used_memory_peak_percent": redis_peak_percent,
                }
            )
            utilization_values.append(redis_peak_percent)
        if mode == "full_e2e":
            capacity = monitor_capacity_acceptance(metadata, monitor_flat)
            row.update(capacity)
            row.update(monitor_terminal_queue_acceptance(monitor_flat))
            utilization_values.append(float(capacity["resource_high_water_max_percent"]))
        row["declared_resource_high_water_max_percent"] = (
            max(utilization_values) if utilization_values else 0.0
        )
        row["declared_resource_high_water_accepted"] = (
            row["declared_resource_high_water_max_percent"] < threshold
        )
        require(
            row["declared_resource_high_water_accepted"],
            "declared resource high-water threshold was reached or exceeded",
        )

    row.update(summarize_resources(trial_dir / "resource_samples.csv"))

    if mode == "full_e2e":
        row["audited_outcome"] = status.get("status_latest_outcome", "")
        row["checker_observation"] = status.get("status_latest_checker_observation", "")
        row["reconciliation_result"] = monitor_flat.get(
            "monitor_evidence_reconciled", status.get("status_reconciliation_result", "")
        )
        row["measurement_completion"] = (
            "complete" if status.get("status_terminal_seen") else "incomplete"
        )
    else:
        row["measurement_completion"] = "complete"
        row["reconciliation_result"] = ""
        row["audited_outcome"] = ""
        row["checker_observation"] = ""
    if mode == "full_e2e":
        cdc_document = stream_documents.get("cdc")
        boundary_document = component_fence_window(
            cdc_document, str(metadata.get("run_id", ""))
        )
        require(
            row.get("component_workload_window_after_stream_id")
            == boundary_document["LOAD_FINISHED"]["transaction_end_stream_id"]
            and row.get("component_workload_window_through_stream_id")
            == boundary_document["END_FENCE"]["transaction_end_stream_id"],
            "analysis component IDs differ from CDC fence transaction END IDs",
        )
        row.update(
            fence_observation_latencies(
                cdc_document,
                status_document,
                str(metadata.get("run_id", "")),
            )
        )
        require(
            marker_end_rows
            == integer(row.get("client_client_txn_count"), "client transaction count"),
            "append-only marker row count does not equal ClientTxn count",
        )
    return row


def find_trial_dirs(root: Path) -> list[Path]:
    trial_root = root / "trials"
    require(trial_root.is_dir(), f"trial directory is missing: {trial_root}")
    result = sorted(path.parent for path in trial_root.rglob("metadata.json"))
    require(result, f"no trials found under {trial_root}")
    return result


def configured_mode_roster(rows: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    rosters: list[tuple[str, ...]] = []
    for row in rows:
        raw_roster = row.get("configured_modes_json", "")
        if raw_roster in (None, ""):
            # A missing roster denotes the current complete formal experiment.
            roster = MODES
        else:
            require(
                isinstance(raw_roster, str),
                "configured performance mode roster is not JSON text",
            )
            try:
                parsed = json.loads(raw_roster)
            except json.JSONDecodeError as error:
                raise ReportError("configured performance mode roster is malformed") from error
            require(
                isinstance(parsed, list) and parsed,
                "configured performance mode roster is empty or not an array",
            )
            require(
                all(isinstance(mode, str) and mode in MODES for mode in parsed),
                "configured performance mode roster contains an unknown mode",
            )
            roster = tuple(parsed)
            require(
                len(roster) == len(set(roster)),
                "configured performance mode roster contains duplicates",
            )
            canonical = tuple(mode for mode in MODES if mode in roster)
            require(
                roster == canonical,
                "configured performance mode roster is not in canonical order",
            )
        rosters.append(roster)
    require(
        len(set(rosters)) == 1,
        "configured performance mode roster differs across trials",
    )
    return rosters[0]


def write_raw_trials(root: Path, output: Path) -> list[dict[str, Any]]:
    rows = [trial_row(path) for path in find_trial_dirs(root)]
    mode_roster = configured_mode_roster(rows)
    modes = {str(row["mode"]) for row in rows}
    require(
        modes == set(mode_roster),
        "raw trial modes differ from the configured roster",
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
    for mode in mode_roster:
        mode_rows = [row for row in rows if row["mode"] == mode]
        require(
            len(mode_rows) == len(expected_repetitions),
            f"mode {mode} does not have exactly one trial per repetition",
        )
        require(
            {int(row["repetition"]) for row in mode_rows} == expected_repetitions,
            f"mode {mode} repetition roster differs",
        )
    trial_ids = [str(row["trial_id"]) for row in rows]
    run_ids = [str(row["run_id"]) for row in rows]
    require(len(trial_ids) == len(set(trial_ids)), "trial IDs are not unique")
    require(len(run_ids) == len(set(run_ids)), "run IDs are not unique")

    invariant_fields = (
        "benchmark",
        "terminals",
        "scale_factor",
        "warehouses",
        "field_size",
        "loader_threads",
        "loader_batch_size",
        "loader_timestamp_utc",
        "warmup_seconds",
        "requested_duration_seconds",
        "random_seed",
        "configured_repetitions",
        "redis_maxmemory_bytes",
        "bootstrap_scan_max_rows",
        "bootstrap_scan_max_input_bytes",
        "bootstrap_scan_max_canonical_bytes",
        "monitor_max_assembler_active_transactions",
        "monitor_max_assembler_events_per_transaction",
        "monitor_max_assembler_active_bytes",
        "monitor_max_assembler_completed_transactions",
        "monitor_max_assembler_retained_identity_bytes",
        "monitor_max_pending_client_records",
        "monitor_max_pending_client_bytes",
        "monitor_max_pending_cdc_slots",
        "monitor_max_pending_cdc_bytes",
        "monitor_max_retained_identity_records",
        "monitor_max_retained_identity_bytes",
        "monitor_max_events_per_transaction",
        "monitor_max_bootstrap_state_rows",
        "monitor_max_bootstrap_state_bytes",
        "monitor_max_checker_state_bytes",
        "output_max_bytes",
        "isolation",
        "rate",
        "transaction_mix",
        "drain_timeout_seconds",
        "high_water_threshold_percent",
        "resource_interval_seconds",
        "cooldown_seconds",
        "compose_timeout_seconds",
        "loader_timeout_seconds",
        "trial_timeout_seconds",
        "status_timeout_seconds",
        "baseline_profile",
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
    expected_positions = set(range(1, len(mode_roster) + 1))
    for repetition in sorted(expected_repetitions):
        repetition_rows = [
            row for row in rows if int(row["repetition"]) == repetition
        ]
        positions = {int(row["mode_order_position"]) for row in repetition_rows}
        require(
            len(repetition_rows) == len(mode_roster) and positions == expected_positions,
            f"repetition {repetition} mode-order positions are incomplete or duplicated",
        )
        offset = (random_seed + repetition - 1) % len(mode_roster)
        for row in repetition_rows:
            position = int(row["mode_order_position"])
            expected_mode = mode_roster[
                (offset + position - 1) % len(mode_roster)
            ]
            require(
                row["mode"] == expected_mode,
                f"repetition {repetition} mode-order rotation differs at position {position}",
            )
    smoke = bool(rows[0].get("smoke"))
    if not smoke:
        attested_dirty = all(bool(row.get("attested_dirty_build")) for row in rows)
        if attested_dirty:
            provenance = root / "provenance"
            for label in ("si_monitoring", "current_benchbase", "baseline", "co_checker"):
                for suffix in ("status.txt", "diff.patch", "untracked-sha256.txt"):
                    require(
                        (provenance / f"{label}-{suffix}").is_file(),
                        f"attested dirty build lacks provenance: {label}-{suffix}",
                    )
        for field in (
            "si_monitoring_dirty",
            "benchbase_current_dirty",
            "benchbase_baseline_dirty",
            "co_checker_dirty",
        ):
            require(
                attested_dirty or not any(bool(row.get(field)) for row in rows),
                f"measured run is dirty: {field}",
            )

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
        mode = str(row["mode"])
        if mode == "full_e2e":
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


def write_transaction_results(rows: Sequence[Mapping[str, Any]], output: Path) -> None:
    result: list[dict[str, Any]] = []
    for row in rows:
        trial_dir = Path(str(row["artifact_dir"]))
        benchbase = load_json(trial_dir / "benchbase_metrics.json", required=True)
        require(isinstance(benchbase, Mapping), "BenchBase metrics are not an object")
        outcome = load_json(trial_dir / "outcome_metrics.json")
        elapsed_ns = int(numeric(benchbase.get("elapsed_time_nanoseconds"), "BenchBase elapsed", minimum=1.0))
        elapsed_seconds = elapsed_ns / 1_000_000_000.0
        raw_types = benchbase.get("transaction_types")
        require(isinstance(raw_types, Mapping), "BenchBase transaction types are missing")
        if isinstance(outcome, Mapping):
            outcome_types = outcome.get("transaction_types")
            require(isinstance(outcome_types, Mapping), "outcome transaction types are missing")
            transaction_names = sorted(outcome_types)
            outcome_totals = outcome.get("totals")
            require(isinstance(outcome_totals, Mapping), "outcome totals are missing")
        else:
            outcome_types = {}
            transaction_names = sorted(raw_types)
            outcome_totals = {}
        for transaction_name in (None, *transaction_names):
            overall = transaction_name is None
            raw_type = (
                {"latency_microseconds": benchbase.get("latency_microseconds", {})}
                if overall
                else raw_types.get(transaction_name, {})
            )
            require(isinstance(raw_type, Mapping), "BenchBase transaction type is not an object")
            latency = raw_type.get("latency_microseconds", {})
            require(isinstance(latency, Mapping), "transaction latency is not an object")
            counters = outcome_totals if overall else outcome_types.get(transaction_name, {})
            require(isinstance(counters, Mapping), "transaction outcomes are not an object")
            logical = counters.get(
                "logical_transactions_started",
                benchbase.get("measured_count", 0)
                if overall
                else raw_type.get("logical_invocation_count", 0),
            )
            successful = counters.get("successful_transactions", logical)
            item: dict[str, Any] = {
                "benchmark": row.get("benchmark", "ycsb"),
                "mode": row["mode"],
                "repetition": row["repetition"],
                "trial_id": row["trial_id"],
                "row_scope": "overall" if overall else "transaction_type",
                "transaction_type": "" if overall else transaction_name,
                "elapsed_time_nanoseconds": elapsed_ns,
                "logical_transactions_started": logical,
                "successful_transactions": successful,
                "logical_transactions_per_second": numeric(logical, "logical transactions") / elapsed_seconds,
                "successful_transactions_per_second": numeric(successful, "successful transactions") / elapsed_seconds,
                "latency_mean_microseconds": latency.get("mean", ""),
                "latency_p50_microseconds": latency.get("p50", ""),
                "latency_p95_microseconds": latency.get("p95", ""),
                "latency_p99_microseconds": latency.get("p99", ""),
                "latency_max_microseconds": latency.get("max", ""),
                "new_order_per_minute": (
                    outcome.get("new_order_per_minute", "")
                    if overall and isinstance(outcome, Mapping)
                    else (
                        numeric(successful, "NewOrder successes") * 60.0 / elapsed_seconds
                        if transaction_name == "NewOrder"
                        else ""
                    )
                ),
            }
            for counter in (
                "terminal_aborted_transactions",
                "physical_attempts_started",
                "definitively_aborted_attempts",
                "retry_attempts",
                "retryable_failure_events",
                "error_transactions",
                "unknown_status_transactions",
                "ambiguous_attempts",
                "retry_different_events",
            ):
                item[counter] = counters.get(counter, "")
            if counters:
                item["failed_transactions"] = numeric(
                    counters.get("error_transactions"), "error transactions"
                ) + numeric(counters.get("unknown_status_transactions"), "unknown statuses")
            else:
                item["failed_transactions"] = ""
            for counter in (
                "attempts",
                "retry_attempts",
                "terminal_aborted_transactions",
                "error_transactions",
                "unknown_status_transactions",
            ):
                source_name = "physical_attempts_started" if counter == "attempts" else counter
                value = counters.get(source_name)
                item[f"{counter}_per_second"] = optional_rate(value, elapsed_seconds)
            item["failed_transactions_per_second"] = optional_rate(
                item["failed_transactions"], elapsed_seconds
            )
            result.append(item)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=TRANSACTION_RESULT_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(result)


def write_component_results(
    rows: Sequence[Mapping[str, Any]], output: Path
) -> None:
    """Write explicit CDC, C++ monitor, and whole-pipeline boundaries."""

    result: list[dict[str, Any]] = []
    for row in rows:
        if row.get("mode") != "full_e2e":
            continue
        identity = {
            "benchmark": row.get("benchmark", "ycsb"),
            "mode": row["mode"],
            "repetition": row["repetition"],
            "trial_id": row["trial_id"],
        }
        empty_lag = {
            field: ""
            for field in COMPONENT_RESULT_FIELDS
            if field.startswith(("source_lag_", "delivery_lag_"))
        }
        result.append(
            {
                **identity,
                "component_boundary": "cdc_ingestion",
                "component_owner": "debezium_to_redis",
                "interval_definition": row.get("cdc_ingestion_interval_definition", ""),
                "rate_interpretation": "observed_component_window_average",
                "interval_seconds": row.get("cdc_ingestion_interval_seconds", ""),
                "workload_transactions": "",
                "event_kind": "raw_cdc_rows_including_markers_excluding_control",
                "workload_events": row.get("cdc_ingestion_workload_rows", ""),
                "transactions_per_second": "",
                "events_per_second": row.get(
                    "cdc_ingestion_workload_rows_per_second", ""
                ),
                "source_lag_mean_ms": row.get(
                    "cdc_ingestion_workload_source_lag_ms_mean", ""
                ),
                "source_lag_p95_ms": row.get(
                    "cdc_ingestion_workload_source_lag_ms_p95", ""
                ),
                "source_lag_p99_ms": row.get(
                    "cdc_ingestion_workload_source_lag_ms_p99", ""
                ),
                "source_lag_max_ms": row.get(
                    "cdc_ingestion_workload_source_lag_ms_max", ""
                ),
                "delivery_lag_mean_ms": row.get(
                    "cdc_ingestion_workload_delivery_lag_ms_mean", ""
                ),
                "delivery_lag_p95_ms": row.get(
                    "cdc_ingestion_workload_delivery_lag_ms_p95", ""
                ),
                "delivery_lag_p99_ms": row.get(
                    "cdc_ingestion_workload_delivery_lag_ms_p99", ""
                ),
                "delivery_lag_max_ms": row.get(
                    "cdc_ingestion_workload_delivery_lag_ms_max", ""
                ),
            }
        )
        component_definitions = {
            "assembler": (
                "workload_assembler_accept_call_active_time",
                "assembler_accept_call_service_diagnostic_not_sustainable_throughput",
                "raw_cdc_rows_accepted",
                "cpp_monitor_assembler_accept_call_service",
            ),
            "joiner": (
                "workload_joiner_ready_prefix_release_active_time_excluding_admission_wait_and_checker",
                "joiner_ready_prefix_release_service_diagnostic_not_sustainable_throughput",
                "joined_reads_plus_normalized_application_writes",
                "cpp_monitor_joiner_ready_prefix_release_service",
            ),
            "checker": (
                "workload_checker_engine_consume_active_time_excluding_adapter_mapping",
                "checker_engine_consume_service_diagnostic_not_sustainable_throughput",
                "checker_input_reads_plus_normalized_application_writes",
                "cpp_monitor_checker_engine_consume_service",
            ),
        }
        for component, (
            interval_definition,
            rate_interpretation,
            event_kind,
            rate_prefix,
        ) in component_definitions.items():
            active_nanoseconds = numeric(
                row.get(f"cpp_monitor_{component}_active_nanoseconds"),
                f"{component} active nanoseconds",
                minimum=1.0,
            )
            if component == "assembler":
                events = row.get("cpp_monitor_assembler_workload_raw_row_events", "")
            else:
                events = numeric(
                    row.get(f"cpp_monitor_{component}_workload_read_events"),
                    f"{component} read events",
                ) + numeric(
                    row.get(f"cpp_monitor_{component}_workload_write_events"),
                    f"{component} write events",
                )
            result.append(
                {
                    **identity,
                    **empty_lag,
                    "component_boundary": f"cpp_monitor_{component}",
                    "component_owner": "cpp_monitor",
                    "interval_definition": interval_definition,
                    "rate_interpretation": rate_interpretation,
                    "interval_seconds": active_nanoseconds / 1_000_000_000.0,
                    "workload_transactions": row.get(
                        f"cpp_monitor_{component}_workload_transactions", ""
                    ),
                    "event_kind": event_kind,
                    "workload_events": events,
                    "transactions_per_second": row.get(
                        f"{rate_prefix}_transactions_per_second", ""
                    ),
                    "events_per_second": row.get(
                        f"{rate_prefix}_events_per_second", ""
                    ),
                }
            )
        result.append(
            {
                **identity,
                **empty_lag,
                "component_boundary": "whole_evidence_pipeline",
                "component_owner": "black_box_e2e",
                "interval_definition": row.get(
                    "evidence_pipeline_interval_definition", ""
                ),
                "rate_interpretation": "externally_observed_end_to_end_average",
                "interval_seconds": row.get("evidence_pipeline_interval_seconds", ""),
                "workload_transactions": row.get(
                    "evidence_pipeline_workload_transactions", ""
                ),
                "event_kind": "checked_reads_plus_normalized_application_writes",
                "workload_events": row.get("evidence_pipeline_workload_events", ""),
                "transactions_per_second": row.get(
                    "evidence_pipeline_transactions_per_second", ""
                ),
                "events_per_second": row.get(
                    "evidence_pipeline_events_per_second", ""
                ),
            }
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=COMPONENT_RESULT_FIELDS, lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(result)


def median_for(rows: Sequence[Mapping[str, Any]], mode: str, metric: str) -> float | None:
    values = finite_numbers(row.get(metric) for row in rows if row.get("mode") == mode)
    return statistics.median(values) if values else None


def _headline_decimal(value: Any, label: str) -> Decimal:
    require(not isinstance(value, bool), f"{label} is not numeric")
    try:
        result = Decimal(str(value).strip())
    except InvalidOperation as error:
        raise ReportError(f"{label} is not numeric") from error
    require(result.is_finite(), f"{label} is not finite")
    require(result >= 0, f"{label} is negative")
    return result


def _decimal_median(values: Sequence[Decimal]) -> Decimal:
    require(bool(values), "cannot calculate a median without values")
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def read_headline_input(path: Path) -> list[dict[str, Any]]:
    required_fields = {
        "mode",
        "logical_transactions_per_second",
        "successful_transactions_per_second",
        "latency_mean_microseconds",
        "latency_p95_microseconds",
    }
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        require(reader.fieldnames is not None, f"raw trial CSV has no header: {path}")
        require(
            len(reader.fieldnames) == len(set(reader.fieldnames)),
            f"raw trial CSV has duplicate columns: {path}",
        )
        missing = required_fields - set(reader.fieldnames)
        require(
            not missing,
            "raw trial CSV is missing headline columns: " + ", ".join(sorted(missing)),
        )
        rows = [dict(row) for row in reader]
    require(bool(rows), f"raw trial CSV has no rows: {path}")
    return rows


def headline_rows(raw_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    metric_fields = (
        "logical_transactions_per_second",
        "successful_transactions_per_second",
        "latency_mean_microseconds",
        "latency_p95_microseconds",
    )
    values_by_mode: dict[str, dict[str, list[Decimal]]] = {}
    trial_counts: dict[str, int] = {}
    for row_number, row in enumerate(raw_rows, start=2):
        mode = row.get("mode")
        require(
            isinstance(mode, str) and mode in MODES,
            f"raw trial CSV row {row_number} has an unknown mode: {mode!r}",
        )
        mode_values = values_by_mode.setdefault(
            mode, {metric: [] for metric in metric_fields}
        )
        trial_counts[mode] = trial_counts.get(mode, 0) + 1
        for metric in metric_fields:
            mode_values[metric].append(
                _headline_decimal(row.get(metric), f"raw trial CSV row {row_number} {metric}")
            )

    require(
        len(set(trial_counts.values())) == 1,
        "headline modes do not have equal repetition counts",
    )
    medians = {
        mode: {
            metric: _decimal_median(mode_values[metric])
            for metric in metric_fields
        }
        for mode, mode_values in values_by_mode.items()
    }
    baseline = medians.get("benchbase_uninstrumented")
    baseline_logical_throughput = (
        baseline["logical_transactions_per_second"]
        if baseline is not None
        else None
    )

    result: list[dict[str, Any]] = []
    for mode in BENCHBASE_HEADLINE_MODES:
        if mode not in medians:
            continue
        mode_medians = medians[mode]
        loss: Decimal | str = ""
        if (
            mode == "full_e2e"
            and baseline_logical_throughput not in (None, 0)
        ):
            throughput = mode_medians[
                "logical_transactions_per_second"
            ]
            loss = (
                (baseline_logical_throughput - throughput)
                / baseline_logical_throughput
                * Decimal(100)
            )
        result.append(
            {
                "mode": mode,
                "Mode": HEADLINE_MODE_LABELS[mode],
                "Logical TPS": mode_medians[
                    "logical_transactions_per_second"
                ],
                "Successful TPS": mode_medians[
                    "successful_transactions_per_second"
                ],
                "Mean latency": mode_medians["latency_mean_microseconds"],
                "p95": mode_medians["latency_p95_microseconds"],
                "Logical TPS loss vs baseline": loss,
            }
        )
    return result


def _decimal_text(value: Any) -> str:
    if value == "":
        return ""
    require(isinstance(value, Decimal), "headline numeric value is not a decimal")
    return "0" if value == 0 else format(value, "f")


def write_headline_csv(rows: Sequence[Mapping[str, Any]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=HEADLINE_CSV_FIELDS, lineterminator="\n"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "mode": row["mode"],
                    "logical_transactions_per_second_median": _decimal_text(
                        row["Logical TPS"]
                    ),
                    "successful_transactions_per_second_median": _decimal_text(
                        row["Successful TPS"]
                    ),
                    "latency_mean_microseconds_median": _decimal_text(
                        row["Mean latency"]
                    ),
                    "latency_p95_microseconds_median": _decimal_text(row["p95"]),
                    "logical_throughput_loss_vs_baseline_percent": _decimal_text(
                        row["Logical TPS loss vs baseline"]
                    ),
                }
            )


def _human_decimal(value: Decimal, places: int, *, trim_fraction: bool = False) -> str:
    formatted = f"{value:,.{places}f}"
    if trim_fraction and "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    return formatted


def write_headline_markdown(rows: Sequence[Mapping[str, Any]], output: Path) -> None:
    lines = [
        "| Mode | Logical TPS | Successful TPS | Mean latency | p95 | Logical TPS loss vs baseline |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        loss = row["Logical TPS loss vs baseline"]
        loss_text = "—" if loss == "" else f"{_human_decimal(loss, 2)}%"
        lines.append(
            "| "
            + " | ".join(
                (
                    str(row["Mode"]),
                    f"{_human_decimal(row['Logical TPS'], 1)} tx/s",
                    f"{_human_decimal(row['Successful TPS'], 1)} tx/s",
                    f"{_human_decimal(row['Mean latency'], 1)} µs",
                    f"{_human_decimal(row['p95'], 1, trim_fraction=True)} µs",
                    loss_text,
                )
            )
            + " |"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_headline(input_csv: Path, csv_output: Path, markdown_output: Path) -> None:
    rows = headline_rows(read_headline_input(input_csv))
    write_headline_csv(rows, csv_output)
    write_headline_markdown(rows, markdown_output)


def comparison_rows(
    rows: Sequence[Mapping[str, Any]],
    comparison_mode: str,
    mode: str,
    attribution: str,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for metric, kind in CLIENT_IMPACT_METRICS:
        comparison = median_for(rows, comparison_mode, metric)
        measured = median_for(rows, mode, metric)
        if kind == "logical_throughput_loss_percent":
            formula = "100 * (comparison_median - mode_median) / comparison_median"
            overhead = (
                ""
                if comparison is None or measured is None
                else "N/A"
                if comparison == 0
                else (comparison - measured) / comparison * 100.0
            )
        else:  # Defensive: the formal contract currently has one comparison.
            raise ReportError(f"unknown client-impact metric kind: {kind}")
        result.append(
            {
                "comparison_mode": comparison_mode,
                "mode": mode,
                "attribution": attribution,
                "metric": metric,
                "comparison_median": "" if comparison is None else comparison,
                "mode_median": "" if measured is None else measured,
                "overhead_percent": overhead,
                "formula": formula,
            }
        )
    return result


def write_overhead(rows: Sequence[Mapping[str, Any]], output: Path) -> None:
    output_rows: list[dict[str, Any]] = []
    baseline = "benchbase_uninstrumented"
    available_modes = {str(row.get("mode", "")) for row in rows}
    if {baseline, "full_e2e"}.issubset(available_modes):
        output_rows.extend(
            comparison_rows(
                rows,
                baseline,
                "full_e2e",
                "full_e2e_client_total_effect",
            )
        )
    fields = (
        "comparison_mode",
        "mode",
        "attribution",
        "metric",
        "comparison_median",
        "mode_median",
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
    if args.transaction_output:
        write_transaction_results(rows, Path(args.transaction_output))
    if args.component_output:
        write_component_results(rows, Path(args.component_output))


def command_headline(args: argparse.Namespace) -> None:
    write_headline(
        Path(args.input_csv),
        Path(args.csv_output),
        Path(args.markdown_output),
    )


def command_checksums(args: argparse.Namespace) -> None:
    write_checksums(Path(args.root), Path(args.output))


def command_fence_time(args: argparse.Namespace) -> None:
    document = load_json(Path(args.input), required=True)
    print(
        fence_stream_time(
            document, args.run_id, args.phase, transaction_end=args.transaction_end
        )
    )


def command_fence_window(args: argparse.Namespace) -> None:
    document = load_json(Path(args.input), required=True)
    print(json.dumps(component_fence_window(document, args.run_id), sort_keys=True))


def command_final_status(args: argparse.Namespace) -> None:
    document = load_json(Path(args.input), required=True)
    print(json.dumps(final_status_boundary(document, args.run_id), sort_keys=True))


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
        "aggregate",
        help="write raw trials, the baseline/full-E2E client impact, and artifact index",
    )
    aggregate.add_argument("--root", required=True)
    aggregate.add_argument("--raw-output", required=True)
    aggregate.add_argument("--overhead-output", required=True)
    aggregate.add_argument("--artifact-index-output", required=True)
    aggregate.add_argument(
        "--transaction-output",
        help="write one overall/per-type transaction row per mode repetition",
    )
    aggregate.add_argument(
        "--component-output",
        help="write CDC, C++ monitor, and whole-pipeline rows per full-E2E repetition",
    )
    aggregate.set_defaults(handler=command_aggregate)

    headline = subparsers.add_parser(
        "headline", help="write concise CSV and Markdown tables from raw trial CSV"
    )
    headline.add_argument("--input-csv", required=True)
    headline.add_argument("--csv-output", required=True)
    headline.add_argument("--markdown-output", required=True)
    headline.set_defaults(handler=command_headline)

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

    fence_window = subparsers.add_parser(
        "fence-window",
        help="print LOAD_FINISHED and END_FENCE transaction-END Redis IDs in one scan",
    )
    fence_window.add_argument("--input", required=True)
    fence_window.add_argument("--run-id", required=True)
    fence_window.set_defaults(handler=command_fence_window)

    final_status = subparsers.add_parser(
        "final-status-boundary",
        help="print the exact run-scoped FINAL status Redis ID and timestamp",
    )
    final_status.add_argument("--input", required=True)
    final_status.add_argument("--run-id", required=True)
    final_status.set_defaults(handler=command_final_status)
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
