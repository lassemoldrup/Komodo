#!/usr/bin/env python3
"""Collect and analyze reproducible black-box performance measurements.

The analysis functions in this module are intentionally independent of the
end-to-end coordinator.  Partial measurement modes can use them without
creating or interpreting a consistency verdict.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import statistics
import subprocess
import sys
import time
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence, TextIO


NA = "N/A"
RESOURCE_FIELDS = ("timestamp", "component", "cpu_percent", "rss_bytes", "source")
TRIAL_SUMMARY_FIELDS = (
    "mode",
    "metric",
    "available_trials",
    "total_trials",
    "median",
    "min",
    "max",
    "mad",
    "overhead_vs_baseline_percent",
    "overhead_direction",
    "overhead_formula",
)
BENCHBASE_RAW_FIELDS = (
    "Transaction Type Index",
    "Transaction Name",
    "Start Time (microseconds)",
    "Latency (microseconds)",
    "Worker Id (start number)",
    "Phase Id (index in config file)",
)
STREAM_ID_PATTERN = re.compile(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)\Z")
MEMORY_PATTERN = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*([KMGT]?i?B)\Z", re.IGNORECASE)


class PerformanceDataError(ValueError):
    """An input artifact is malformed or internally inconsistent."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PerformanceDataError(message)


def _finite_float(value: Any, label: str, *, minimum: float | None = None) -> float:
    _require(not isinstance(value, bool), f"{label} is not a number")
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise PerformanceDataError(f"{label} is not a number") from error
    _require(math.isfinite(result), f"{label} is not finite")
    if minimum is not None:
        _require(result >= minimum, f"{label} is less than {minimum}")
    return result


def _nonnegative_int(value: Any, label: str) -> int:
    _require(not isinstance(value, bool), f"{label} is not an integer")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and re.fullmatch(r"0|[1-9][0-9]*", value):
        result = int(value)
    else:
        raise PerformanceDataError(f"{label} is not a nonnegative integer")
    _require(result >= 0, f"{label} is negative")
    return result


def _decimal(value: Any, label: str) -> Decimal:
    _require(not isinstance(value, bool), f"{label} is not a decimal number")
    try:
        result = Decimal(str(value).strip())
    except (InvalidOperation, AttributeError) as error:
        raise PerformanceDataError(f"{label} is not a decimal number") from error
    _require(result.is_finite(), f"{label} is not finite")
    return result


def _nearest_rank(values: Sequence[float | int], percentile: float) -> float | int | str:
    """Use BenchBase's index=floor(percentile * count) percentile rule."""

    if not values:
        return NA
    ordered = sorted(values)
    index = min(int(percentile * len(ordered)), len(ordered) - 1)
    return ordered[index]


def _distribution(values: Sequence[float | int]) -> dict[str, float | int | str]:
    if not values:
        return {
            "current": NA,
            "max": NA,
            "mean": NA,
            "p50": NA,
            "p95": NA,
            "p99": NA,
        }
    return {
        "current": values[-1],
        "max": max(values),
        "mean": statistics.fmean(values),
        "p50": _nearest_rank(values, 0.50),
        "p95": _nearest_rank(values, 0.95),
        "p99": _nearest_rank(values, 0.99),
    }


def read_benchbase_raw(stream: TextIO) -> list[dict[str, str]]:
    reader = csv.DictReader(stream)
    _require(reader.fieldnames is not None, "BenchBase raw CSV has no header")
    _require(len(reader.fieldnames) == len(set(reader.fieldnames)), "BenchBase raw CSV has duplicate columns")
    missing = [field for field in BENCHBASE_RAW_FIELDS if field not in reader.fieldnames]
    _require(not missing, f"BenchBase raw CSV is missing columns: {', '.join(missing)}")
    return [dict(row) for row in reader]


def analyze_benchbase(
    raw_rows: Iterable[Mapping[str, Any]], summary: Mapping[str, Any]
) -> dict[str, Any]:
    """Analyze BenchBase raw samples and the matching summary document."""

    measured_count = _nonnegative_int(summary.get("Measured Requests"), "Measured Requests")
    throughput = _finite_float(
        summary.get("Throughput (requests/second)"),
        "Throughput (requests/second)",
        minimum=0.0,
    )
    goodput = _finite_float(
        summary.get("Goodput (requests/second)"),
        "Goodput (requests/second)",
        minimum=0.0,
    )
    final_state = summary.get("Final State")
    _require(isinstance(final_state, str) and final_state != "", "Final State is missing")
    elapsed_ns = _nonnegative_int(summary.get("Elapsed Time (nanoseconds)"), "Elapsed Time (nanoseconds)")
    summary_start_ms = _nonnegative_int(
        summary.get("Start timestamp (milliseconds)"), "Start timestamp (milliseconds)"
    )

    latencies: list[int] = []
    starts_us: list[Decimal] = []
    completions_us: list[Decimal] = []
    transaction_counts: Counter[str] = Counter()
    transaction_latencies: dict[str, list[int]] = defaultdict(list)
    phase_ids: list[int] = []
    seen_phase_ids: set[int] = set()

    for row_number, row in enumerate(raw_rows, start=2):
        name = row.get("Transaction Name")
        _require(isinstance(name, str) and name != "", f"raw CSV row {row_number} has no transaction name")
        latency = _nonnegative_int(
            row.get("Latency (microseconds)"), f"raw CSV row {row_number} latency"
        )
        start_seconds = _decimal(
            row.get("Start Time (microseconds)"), f"raw CSV row {row_number} start time"
        )
        _require(start_seconds >= 0, f"raw CSV row {row_number} start time is negative")
        start_us = start_seconds * Decimal(1_000_000)
        latencies.append(latency)
        starts_us.append(start_us)
        completions_us.append(start_us + latency)
        transaction_counts[name] += 1
        transaction_latencies[name].append(latency)
        phase_id = _nonnegative_int(
            row.get("Phase Id (index in config file)"),
            f"raw CSV row {row_number} phase ID",
        )
        if phase_id not in seen_phase_ids:
            seen_phase_ids.add(phase_id)
            phase_ids.append(phase_id)

    _require(
        len(latencies) == measured_count,
        "BenchBase raw sample count differs from summary Measured Requests",
    )

    if starts_us:
        measured_start_ms = int((min(starts_us) / 1000).to_integral_value(rounding=ROUND_FLOOR))
        measured_end_ms = int(
            (max(completions_us) / 1000).to_integral_value(rounding=ROUND_CEILING)
        )
    else:
        measured_start_ms = summary_start_ms
        measured_end_ms = int(
            (Decimal(summary_start_ms) + Decimal(elapsed_ns) / Decimal(1_000_000)).to_integral_value(
                rounding=ROUND_CEILING
            )
        )

    latency_stats: dict[str, float | int | str]
    if latencies:
        latency_stats = {
            "mean": statistics.fmean(latencies),
            "p50": _nearest_rank(latencies, 0.50),
            "p95": _nearest_rank(latencies, 0.95),
            "p99": _nearest_rank(latencies, 0.99),
            "max": max(latencies),
        }
    else:
        latency_stats = {name: NA for name in ("mean", "p50", "p95", "p99", "max")}

    elapsed_seconds = elapsed_ns / 1_000_000_000.0
    transaction_types = {}
    for name in sorted(transaction_counts):
        values = transaction_latencies[name]
        transaction_types[name] = {
            "logical_invocation_count": transaction_counts[name],
            "logical_invocations_per_second": (
                transaction_counts[name] / elapsed_seconds if elapsed_seconds > 0 else NA
            ),
            "latency_microseconds": {
                "mean": statistics.fmean(values),
                "p50": _nearest_rank(values, 0.50),
                "p95": _nearest_rank(values, 0.95),
                "p99": _nearest_rank(values, 0.99),
                "max": max(values),
            },
        }

    return {
        "measured_count": measured_count,
        "elapsed_time_nanoseconds": elapsed_ns,
        "throughput_requests_per_second": throughput,
        "goodput_requests_per_second": goodput,
        "final_state": final_state,
        "latency_microseconds": latency_stats,
        "measured_start_epoch_ms": measured_start_ms,
        "measured_end_epoch_ms": measured_end_ms,
        "transaction_counts": dict(sorted(transaction_counts.items())),
        "transaction_types": transaction_types,
        "phase_ids": phase_ids,
        "new_order_logical_invocations_per_minute": (
            transaction_counts["NewOrder"] * 60.0 / elapsed_seconds
            if elapsed_seconds > 0
            else NA
        ),
    }


def analyze_benchbase_files(raw_csv: Path, summary_json: Path) -> dict[str, Any]:
    with raw_csv.open("r", encoding="utf-8", newline="") as raw_stream:
        rows = read_benchbase_raw(raw_stream)
    with summary_json.open("r", encoding="utf-8") as summary_stream:
        summary = json.load(summary_stream)
    _require(isinstance(summary, dict), "BenchBase summary JSON is not an object")
    return analyze_benchbase(rows, summary)


OUTCOME_COUNTERS = (
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


def analyze_transaction_outcomes(
    document: Mapping[str, Any], benchbase: Mapping[str, Any]
) -> dict[str, Any]:
    _require(document.get("kind") == "transaction_outcomes", "outcome artifact kind differs")
    _require(document.get("scope") == "measurement_window", "outcome artifact scope differs")
    benchmark = document.get("benchmark")
    _require(isinstance(benchmark, str) and benchmark, "outcome benchmark is missing")
    _require(
        document.get("window_semantics") == "fully_contained_logical_invocations",
        "outcome window semantics differ",
    )
    elapsed_ns = _nonnegative_int(document.get("elapsed_nanoseconds"), "outcome elapsed_nanoseconds")
    _require(elapsed_ns > 0, "outcome elapsed_nanoseconds is zero")
    _require(
        elapsed_ns == _nonnegative_int(benchbase.get("elapsed_time_nanoseconds"), "BenchBase elapsed"),
        "outcome and BenchBase elapsed windows differ",
    )
    measured_requests = _nonnegative_int(document.get("measured_requests"), "outcome measured_requests")
    _require(
        measured_requests == _nonnegative_int(benchbase.get("measured_count"), "BenchBase measured count"),
        "outcome and BenchBase measured request counts differ",
    )
    phase_ids = document.get("phase_ids")
    _require(
        isinstance(phase_ids, list)
        and all(isinstance(value, int) and not isinstance(value, bool) and value >= 0 for value in phase_ids)
        and len(phase_ids) == len(set(phase_ids)),
        "outcome phase_ids are invalid",
    )
    _require(phase_ids == benchbase.get("phase_ids"), "outcome/raw measurement phase IDs differ")
    raw_types = benchbase.get("transaction_types")
    raw_counts = benchbase.get("transaction_counts")
    outcome_types = document.get("transaction_types")
    totals = document.get("totals")
    _require(isinstance(raw_types, Mapping), "BenchBase transaction types are missing")
    _require(isinstance(raw_counts, Mapping), "BenchBase transaction counts are missing")
    _require(isinstance(outcome_types, Mapping) and outcome_types, "outcome transaction types are missing")
    _require(isinstance(totals, Mapping), "outcome totals are missing")
    _require(set(raw_types).issubset(outcome_types), "raw transaction type is absent from outcomes")
    elapsed_seconds = elapsed_ns / 1_000_000_000.0
    normalized: dict[str, Any] = {}
    summed: Counter[str] = Counter()
    for transaction_type in sorted(outcome_types):
        source = outcome_types[transaction_type]
        _require(isinstance(source, Mapping), f"{transaction_type} outcomes are not an object")
        counters = {
            name: _nonnegative_int(source.get(name), f"{transaction_type}.{name}")
            for name in OUTCOME_COUNTERS
        }
        _require(
            counters["logical_transactions_started"]
            == counters["successful_transactions"]
            + counters["terminal_aborted_transactions"]
            + counters["error_transactions"]
            + counters["unknown_status_transactions"],
            f"{transaction_type} logical outcome algebra differs",
        )
        _require(
            counters["physical_attempts_started"]
            == counters["successful_transactions"]
            + counters["definitively_aborted_attempts"]
            + counters["ambiguous_attempts"],
            f"{transaction_type} physical attempt algebra differs",
        )
        _require(
            counters["retry_attempts"]
            == counters["physical_attempts_started"]
            - counters["logical_transactions_started"],
            f"{transaction_type} retry algebra differs",
        )
        _require(
            counters["ambiguous_attempts"] == counters["unknown_status_transactions"],
            f"{transaction_type} ambiguous/unknown algebra differs",
        )
        retry_failure_events = (
            counters["retryable_failure_events"]
            + counters["retry_different_events"]
        )
        _require(
            counters["retry_attempts"]
            <= retry_failure_events
            <= counters["retry_attempts"] + counters["terminal_aborted_transactions"],
            f"{transaction_type} retry failure event algebra differs",
        )
        _require(
            counters["logical_transactions_started"]
            == _nonnegative_int(raw_counts.get(transaction_type, 0), f"raw {transaction_type} count"),
            f"{transaction_type} raw/outcome logical counts differ",
        )
        for name, value in counters.items():
            summed[name] += value
        rates = {
            f"{name}_per_second": value / elapsed_seconds
            for name, value in counters.items()
        }
        rates["attempts_per_second"] = (
            counters["physical_attempts_started"] / elapsed_seconds
        )
        rates["physical_attempts_per_second"] = rates["attempts_per_second"]
        failed_transactions = (
            counters["error_transactions"] + counters["unknown_status_transactions"]
        )
        rates["failed_transactions_per_second"] = failed_transactions / elapsed_seconds
        raw_type = raw_types.get(transaction_type)
        latency = (
            raw_type["latency_microseconds"]
            if isinstance(raw_type, Mapping)
            else {name: NA for name in ("mean", "p50", "p95", "p99", "max")}
        )
        normalized[transaction_type] = {
            **counters,
            "failed_transactions": failed_transactions,
            **rates,
            "latency_microseconds": latency,
        }
    for name in OUTCOME_COUNTERS:
        _require(
            _nonnegative_int(totals.get(name), f"totals.{name}") == summed[name],
            f"outcome total {name} differs from per-type sum",
        )
    return {
        "kind": "transaction_outcome_metrics",
        "benchmark": benchmark,
        "scope": "measurement_window",
        "window_semantics": "fully_contained_logical_invocations",
        "elapsed_time_nanoseconds": elapsed_ns,
        "measured_requests": measured_requests,
        "phase_ids": phase_ids,
        "transaction_types": normalized,
        "totals": {
            **dict(summed),
            "failed_transactions": (
                summed["error_transactions"] + summed["unknown_status_transactions"]
            ),
            **{
                f"{name}_per_second": value / elapsed_seconds
                for name, value in summed.items()
            },
            "failed_transactions_per_second": (
                summed["error_transactions"] + summed["unknown_status_transactions"]
            ) / elapsed_seconds,
        },
        "successful_transactions_per_second": (
            summed["successful_transactions"] / elapsed_seconds
            if elapsed_seconds > 0
            else NA
        ),
        "physical_attempts_per_second": (
            summed["physical_attempts_started"] / elapsed_seconds
            if elapsed_seconds > 0
            else NA
        ),
        "new_order_per_minute": (
            normalized.get("NewOrder", {}).get("successful_transactions", 0)
            * 60.0
            / elapsed_seconds
            if elapsed_seconds > 0
            else NA
        ),
        "new_order_rate_label": "NewOrder/min",
    }


def parse_xrange_entries(document: Any) -> list[tuple[str, list[tuple[str, str]]]]:
    """Parse the JSON emitted by ``redis-cli --json XRANGE``."""

    _require(isinstance(document, list), "XRANGE JSON is not an array")
    parsed: list[tuple[str, list[tuple[str, str]]]] = []
    previous_id: tuple[int, int] | None = None
    for index, entry in enumerate(document):
        label = f"XRANGE entry {index}"
        _require(isinstance(entry, list) and len(entry) == 2, f"{label} is not [id, fields]")
        stream_id = entry[0]
        _require(isinstance(stream_id, str), f"{label} stream ID is not a string")
        match = STREAM_ID_PATTERN.fullmatch(stream_id)
        _require(match is not None, f"{label} has an invalid stream ID")
        numeric_id = (int(match.group(1)), int(match.group(2)))
        if previous_id is not None:
            _require(numeric_id > previous_id, "XRANGE stream IDs are not strictly increasing")
        previous_id = numeric_id

        raw_fields = entry[1]
        fields: list[tuple[str, str]] = []
        if isinstance(raw_fields, dict):
            field_items = list(raw_fields.items())
        else:
            _require(
                isinstance(raw_fields, list) and len(raw_fields) % 2 == 0,
                f"{label} fields are not a flat field/value array",
            )
            field_items = list(zip(raw_fields[::2], raw_fields[1::2]))
        seen_fields: set[str] = set()
        for field, value in field_items:
            _require(isinstance(field, str), f"{label} field name is not UTF-8 text")
            _require(isinstance(value, str), f"{label} field value is not UTF-8 text")
            _require(field not in seen_fields, f"{label} repeats field {field!r}")
            seen_fields.add(field)
            fields.append((field, value))
        parsed.append((stream_id, fields))
    return parsed


def _stream_id_ms(stream_id: str) -> int:
    match = STREAM_ID_PATTERN.fullmatch(stream_id)
    if match is None:  # parse_xrange_entries is the public validation boundary.
        raise PerformanceDataError(f"invalid stream ID: {stream_id}")
    return int(match.group(1))


def _selected_entries(
    entries: Sequence[tuple[str, list[tuple[str, str]]]],
    window_after_stream_id: str | None,
    window_through_stream_id: str | None,
) -> list[tuple[str, list[tuple[str, str]]]]:
    _require(
        (window_after_stream_id is None) == (window_through_stream_id is None),
        "selection window requires both Redis stream IDs",
    )
    if window_after_stream_id is None:
        return list(entries)
    start_match = STREAM_ID_PATTERN.fullmatch(window_after_stream_id)
    end_match = STREAM_ID_PATTERN.fullmatch(window_through_stream_id)
    _require(start_match is not None, "selection start Redis stream ID is invalid")
    _require(end_match is not None, "selection end Redis stream ID is invalid")
    start = (int(start_match.group(1)), int(start_match.group(2)))
    end = (int(end_match.group(1)), int(end_match.group(2)))
    _require(start < end, "selection start Redis stream ID is not before end")
    input_ids = {stream_id for stream_id, _ in entries}
    _require(window_after_stream_id in input_ids, "selection start Redis stream ID is absent")
    _require(window_through_stream_id in input_ids, "selection end Redis stream ID is absent")

    def numeric_id(stream_id: str) -> tuple[int, int]:
        match = STREAM_ID_PATTERN.fullmatch(stream_id)
        assert match is not None
        return int(match.group(1)), int(match.group(2))

    return [entry for entry in entries if start < numeric_id(entry[0]) <= end]


def _json_object(value: str, label: str) -> dict[str, Any]:
    try:
        document = json.loads(value)
    except json.JSONDecodeError as error:
        raise PerformanceDataError(f"{label} is not JSON: {error.msg}") from error
    _require(isinstance(document, dict), f"{label} JSON is not an object")
    return document


def _logical_utf8_bytes(entries: Iterable[tuple[str, list[tuple[str, str]]]]) -> int:
    return sum(
        len(field.encode("utf-8")) + len(value.encode("utf-8"))
        for _, fields in entries
        for field, value in fields
    )


def analyze_redis_xrange(
    document: Any,
    *,
    window_after_stream_id: str | None = None,
    window_through_stream_id: str | None = None,
) -> dict[str, Any]:
    """Measure Redis bytes, CDC row lag, and transaction boundaries."""

    all_entries = parse_xrange_entries(document)
    entries = _selected_entries(
        all_entries, window_after_stream_id, window_through_stream_id
    )
    delivery_lag_samples: list[int] = []
    source_lag_samples: list[int] = []
    workload_delivery_lag_samples: list[int] = []
    workload_source_lag_samples: list[int] = []
    control_row_count = 0
    operation_counts: Counter[str] = Counter()
    table_counts: Counter[str] = Counter()
    begin_ids: Counter[str] = Counter()
    end_ids: Counter[str] = Counter()
    active_frame = False
    saw_begin = False
    completed_count = 0

    for stream_id, fields in entries:
        for field_number, (_, value) in enumerate(fields):
            envelope = _json_object(value, f"Redis entry {stream_id} field {field_number}")
            source = envelope.get("source")
            if source is not None:
                _require(isinstance(source, dict), f"Redis entry {stream_id} source is not an object")
                source_ts_ms = _nonnegative_int(
                    source.get("ts_ms"), f"Redis entry {stream_id} source.ts_ms"
                )
                envelope_ts_ms = _nonnegative_int(
                    envelope.get("ts_ms"), f"Redis entry {stream_id} envelope ts_ms"
                )
                stream_ms = _stream_id_ms(stream_id)
                delivery_lag = stream_ms - envelope_ts_ms
                source_lag = stream_ms - source_ts_ms
                delivery_lag_samples.append(delivery_lag)
                source_lag_samples.append(source_lag)
                operation = envelope.get("op")
                if isinstance(operation, str):
                    operation_counts[operation] += 1
                database = source.get("db")
                table = source.get("table")
                is_control_row = table == "checker_run_fence"
                if is_control_row:
                    control_row_count += 1
                else:
                    workload_delivery_lag_samples.append(delivery_lag)
                    workload_source_lag_samples.append(source_lag)
                if isinstance(database, str) and isinstance(table, str):
                    table_counts[f"{database}.{table}"] += 1
                elif isinstance(table, str):
                    table_counts[table] += 1

            status = envelope.get("status")
            if status in {"BEGIN", "END"}:
                transaction_id = envelope.get("id")
                _require(
                    isinstance(transaction_id, str) and transaction_id != "",
                    f"Redis entry {stream_id} {status} has no transaction ID",
                )
                if status == "BEGIN":
                    begin_ids[transaction_id] += 1
                    _require(
                        not active_frame,
                        f"Redis entry {stream_id} BEGIN interleaves an open frame",
                    )
                    active_frame = True
                    saw_begin = True
                else:
                    end_ids[transaction_id] += 1
                    if active_frame:
                        # Frames are contiguous and never interleave, so
                        # sequential BEGIN/END alternation defines completion;
                        # connector transaction IDs stay per-record diagnostics
                        # because the pinned YugabyteDB connector never repeats
                        # one across records.
                        active_frame = False
                        completed_count += 1
                    else:
                        # A selection window may begin inside a frame, leaving
                        # one leading END fragment that completes nothing.
                        _require(
                            not saw_begin,
                            f"Redis entry {stream_id} END closes no open frame",
                        )

    return {
        "selection_window": {
            "bounded": window_after_stream_id is not None,
            "after_stream_id": window_after_stream_id,
            "through_stream_id": window_through_stream_id,
            "start_exclusive": True if window_after_stream_id is not None else None,
            "end_inclusive": True if window_through_stream_id is not None else None,
        },
        "input_entry_count": len(all_entries),
        "input_logical_utf8_bytes": _logical_utf8_bytes(all_entries),
        "entry_count": len(entries),
        "logical_utf8_bytes": _logical_utf8_bytes(entries),
        "cdc_row_count": len(delivery_lag_samples),
        # Lifecycle fence rows share the Debezium stream but are not workload.
        # Keep the all-row counters for protocol diagnostics and provide the
        # workload-only counters used by formal performance reporting.
        "cdc_control_row_count": control_row_count,
        "cdc_workload_row_count": len(delivery_lag_samples) - control_row_count,
        "cdc_row_counts_by_operation": dict(sorted(operation_counts.items())),
        "cdc_row_counts_by_table": dict(sorted(table_counts.items())),
        "begin_count": sum(begin_ids.values()),
        "end_count": sum(end_ids.values()),
        "completed_transaction_count": completed_count,
        "cdc_delivery_lag_sample_count": len(delivery_lag_samples),
        "cdc_delivery_lag_ms": _distribution(delivery_lag_samples),
        "cdc_source_lag_sample_count": len(source_lag_samples),
        "cdc_source_lag_ms": _distribution(source_lag_samples),
        "cdc_workload_delivery_lag_sample_count": len(workload_delivery_lag_samples),
        "cdc_workload_delivery_lag_ms": _distribution(workload_delivery_lag_samples),
        "cdc_workload_source_lag_sample_count": len(workload_source_lag_samples),
        "cdc_workload_source_lag_ms": _distribution(workload_source_lag_samples),
    }


def analyze_client_xrange(document: Any) -> dict[str, Any]:
    """Count client measurement records and SessionDone accounting fields."""

    entries = parse_xrange_entries(document)
    kind_counts: Counter[str] = Counter()
    session_counts: Counter[str] = Counter()
    client_transaction_counts_by_session: Counter[str] = Counter()
    transaction_type_counts: Counter[str] = Counter()
    transaction_type_counts_by_session: dict[str, Counter[str]] = {}
    read_events_by_session: Counter[str] = Counter()
    session_done_by_session: dict[str, dict[str, int]] = {}
    session_done_totals: Counter[str] = Counter()
    read_event_count = 0
    counter_names = (
        "final_session_seq",
        "logical_transactions_started",
        "successful_transactions",
        "physical_attempts_started",
        "published_transactions",
        "retry_attempts",
        "terminal_aborted_transactions",
        "definitively_aborted_attempts",
        "ambiguous_attempts",
    )

    for stream_id, fields in entries:
        payloads = [value for field, value in fields if field == "payload"]
        _require(len(payloads) == 1, f"client entry {stream_id} does not contain exactly one payload")
        record = _json_object(payloads[0], f"client entry {stream_id} payload")
        kind = record.get("kind")
        _require(isinstance(kind, str) and kind != "", f"client entry {stream_id} has no kind")
        kind_counts[kind] += 1

        session_id = record.get("session_id")
        if session_id is not None:
            _require(
                isinstance(session_id, str) and session_id != "",
                f"client entry {stream_id} has an invalid session_id",
            )
            session_counts[session_id] += 1

        if kind == "client_txn":
            transaction_type = record.get("txn_type")
            _require(
                isinstance(transaction_type, str) and transaction_type != "",
                f"client entry {stream_id} has no txn_type",
            )
            reads = record.get("reads")
            _require(isinstance(reads, list), f"client entry {stream_id} reads is not an array")
            declared_read_count = _nonnegative_int(
                record.get("read_count"), f"client entry {stream_id} read_count"
            )
            _require(
                declared_read_count == len(reads),
                f"client entry {stream_id} read_count differs from reads length",
            )
            transaction_type_counts[transaction_type] += 1
            read_event_count += len(reads)
            if isinstance(session_id, str):
                client_transaction_counts_by_session[session_id] += 1
                transaction_type_counts_by_session.setdefault(
                    session_id, Counter()
                )[transaction_type] += 1
                read_events_by_session[session_id] += len(reads)

        if kind == "session_done":
            _require(isinstance(session_id, str), f"client entry {stream_id} SessionDone has no session")
            counters = session_done_by_session.setdefault(
                session_id, {name: 0 for name in counter_names}
            )
            for counter_name in counter_names:
                value = _nonnegative_int(
                    record.get(counter_name),
                    f"client entry {stream_id} SessionDone {counter_name}",
                )
                counters[counter_name] += value
                session_done_totals[counter_name] += value
            session_done_by_session[session_id] = counters

    return {
        "entry_count": len(entries),
        "logical_utf8_bytes": _logical_utf8_bytes(entries),
        "kind_counts": dict(sorted(kind_counts.items())),
        "client_txn_count": kind_counts["client_txn"],
        "session_counts": dict(sorted(session_counts.items())),
        "client_transaction_counts_by_session": dict(
            sorted(client_transaction_counts_by_session.items())
        ),
        "transaction_type_counts": dict(sorted(transaction_type_counts.items())),
        "transaction_type_counts_by_session": {
            session_id: dict(sorted(counts.items()))
            for session_id, counts in sorted(transaction_type_counts_by_session.items())
        },
        "read_event_count": read_event_count,
        "read_event_counts_by_session": dict(sorted(read_events_by_session.items())),
        "session_done_count": kind_counts["session_done"],
        "session_done_session_count": len(session_done_by_session),
        "session_done_counters": {
            name: session_done_totals[name] for name in counter_names
        },
        "session_done_counters_by_session": dict(sorted(session_done_by_session.items())),
    }


def _parse_trial_value(value: Any, label: str) -> float | str:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return ""
    if isinstance(value, str) and value.strip().upper() in {"N/A", "NA"}:
        return NA
    return _finite_float(value, label)


def _missing_marker(values: Sequence[float | str]) -> str:
    return NA if any(value == NA for value in values) else ""


def _is_higher_better(metric: str, explicit: set[str]) -> bool:
    normalized = metric.lower()
    return (
        metric in explicit
        or "throughput" in normalized
        or normalized.endswith("_rate")
        or normalized.endswith(" rate")
    )


def _trial_cells(
    rows: Sequence[Mapping[str, Any]],
    mode_column: str,
    metrics: Sequence[str] | None,
) -> tuple[list[str], list[str], dict[tuple[str, str], list[float | str]]]:
    _require(rows, "trial CSV contains no rows")
    modes: list[str] = []
    for row_number, row in enumerate(rows, start=2):
        mode = row.get(mode_column)
        _require(isinstance(mode, str) and mode.strip() != "", f"trial row {row_number} has no mode")
        mode = mode.strip()
        if mode not in modes:
            modes.append(mode)

    first_columns = list(rows[0].keys())
    long_form = "metric" in first_columns and "value" in first_columns and metrics is None
    cells: dict[tuple[str, str], list[float | str]] = {}
    metric_order: list[str] = []

    if long_form:
        occurrences: Counter[tuple[str, str]] = Counter()
        for row_number, row in enumerate(rows, start=2):
            mode = str(row[mode_column]).strip()
            metric = row.get("metric")
            _require(isinstance(metric, str) and metric.strip() != "", f"trial row {row_number} has no metric")
            metric = metric.strip()
            if metric not in metric_order:
                metric_order.append(metric)
            cells.setdefault((mode, metric), []).append(
                _parse_trial_value(row.get("value"), f"trial row {row_number} value")
            )
            occurrences[(mode, metric)] += 1
        for mode in modes:
            for metric in metric_order:
                cells.setdefault((mode, metric), [])
        return modes, metric_order, cells

    reserved = {
        mode_column.lower(),
        "trial",
        "trial_id",
        "repetition",
        "run_id",
        "artifact_dir",
        "notes",
    }
    if metrics is None:
        candidates = [column for column in first_columns if column.lower() not in reserved]
        inferred: list[str] = []
        for column in candidates:
            try:
                for row_number, row in enumerate(rows, start=2):
                    _parse_trial_value(row.get(column), f"trial row {row_number} column {column}")
            except PerformanceDataError:
                continue
            inferred.append(column)
        metrics = inferred
    _require(bool(metrics), "trial CSV has no numeric metric columns")
    metric_order = list(metrics)
    _require(len(metric_order) == len(set(metric_order)), "trial metric list contains duplicates")
    for metric in metric_order:
        _require(metric in first_columns, f"trial CSV is missing metric column {metric}")
    for row_number, row in enumerate(rows, start=2):
        mode = str(row[mode_column]).strip()
        for metric in metric_order:
            cells.setdefault((mode, metric), []).append(
                _parse_trial_value(row.get(metric), f"trial row {row_number} column {metric}")
            )
    return modes, metric_order, cells


def summarize_trials(
    rows: Sequence[Mapping[str, Any]],
    *,
    baseline_mode: str | None = None,
    mode_column: str = "mode",
    metrics: Sequence[str] | None = None,
    higher_is_better_metrics: Iterable[str] = (),
) -> list[dict[str, Any]]:
    """Summarize repeated trials with median headline, range, MAD, and overhead."""

    modes, metric_order, cells = _trial_cells(rows, mode_column, metrics)
    if baseline_mode is not None:
        _require(baseline_mode in modes, f"baseline mode {baseline_mode!r} is absent")
    explicit_higher = set(higher_is_better_metrics)
    summaries: dict[tuple[str, str], dict[str, Any]] = {}

    for mode in modes:
        for metric in metric_order:
            source_values = cells[(mode, metric)]
            numbers = [value for value in source_values if isinstance(value, float)]
            marker = _missing_marker(source_values)
            if numbers:
                median = statistics.median(numbers)
                summary = {
                    "mode": mode,
                    "metric": metric,
                    "available_trials": len(numbers),
                    "total_trials": len(source_values),
                    "median": median,
                    "min": min(numbers),
                    "max": max(numbers),
                    "mad": statistics.median(abs(value - median) for value in numbers),
                }
            else:
                summary = {
                    "mode": mode,
                    "metric": metric,
                    "available_trials": 0,
                    "total_trials": len(source_values),
                    "median": marker,
                    "min": marker,
                    "max": marker,
                    "mad": marker,
                }
            summaries[(mode, metric)] = summary

    result: list[dict[str, Any]] = []
    for mode in modes:
        for metric in metric_order:
            summary = summaries[(mode, metric)]
            median = summary["median"]
            if baseline_mode is None:
                overhead: float | str = ""
                direction = ""
                formula = ""
            else:
                baseline_median = summaries[(baseline_mode, metric)]["median"]
                higher_better = _is_higher_better(metric, explicit_higher)
                if higher_better:
                    direction = "decrease_from_baseline"
                    formula = "100 * (baseline_median - mode_median) / baseline_median"
                    numerator_sign = -1.0
                else:
                    direction = "increase_from_baseline"
                    formula = "100 * (mode_median - baseline_median) / baseline_median"
                    numerator_sign = 1.0

                if isinstance(median, (int, float)) and isinstance(
                    baseline_median, (int, float)
                ):
                    if baseline_median == 0:
                        overhead = NA
                    else:
                        overhead = (
                            numerator_sign
                            * (median - baseline_median)
                            / baseline_median
                            * 100.0
                        )
                elif median == NA or baseline_median == NA:
                    overhead = NA
                else:
                    overhead = ""
            summary.update(
                {
                    "overhead_vs_baseline_percent": overhead,
                    "overhead_direction": direction,
                    "overhead_formula": formula,
                }
            )
            result.append(summary)
    return result


def read_trial_csv(stream: TextIO) -> list[dict[str, str]]:
    reader = csv.DictReader(stream)
    _require(reader.fieldnames is not None, "trial CSV has no header")
    _require(len(reader.fieldnames) == len(set(reader.fieldnames)), "trial CSV has duplicate columns")
    return [dict(row) for row in reader]


def _parse_proc_stat(text: str, label: str) -> tuple[int, int, int]:
    closing_parenthesis = text.rfind(")")
    _require(closing_parenthesis > 0, f"{label} stat has no command terminator")
    try:
        pid = int(text[: text.find(" ")])
        fields = text[closing_parenthesis + 2 :].split()
        ppid = int(fields[1])
        ticks = int(fields[11]) + int(fields[12])
        rss_pages = int(fields[21])
    except (ValueError, IndexError) as error:
        raise PerformanceDataError(f"{label} stat is malformed") from error
    _require(pid >= 0 and ppid >= 0 and ticks >= 0, f"{label} stat contains a negative counter")
    return ppid, ticks, max(rss_pages, 0)


def read_proc_snapshot(proc_root: Path = Path("/proc")) -> dict[int, tuple[int, int, int]]:
    """Return PID -> (PPID, CPU ticks, RSS pages) for readable processes."""

    snapshot: dict[int, tuple[int, int, int]] = {}
    try:
        directories = list(proc_root.iterdir())
    except OSError:
        return snapshot
    for directory in directories:
        if not directory.name.isdigit():
            continue
        try:
            text = (directory / "stat").read_text(encoding="utf-8", errors="strict")
            ppid, ticks, rss_pages = _parse_proc_stat(text, f"process {directory.name}")
        except (OSError, UnicodeError, PerformanceDataError):
            continue
        snapshot[int(directory.name)] = (ppid, ticks, rss_pages)
    return snapshot


def process_tree_totals(
    snapshot: Mapping[int, tuple[int, int, int]], root_pid: int
) -> tuple[int, int] | None:
    if root_pid not in snapshot:
        return None
    children: dict[int, list[int]] = {}
    for pid, (parent, _, _) in snapshot.items():
        children.setdefault(parent, []).append(pid)
    pending = [root_pid]
    visited: set[int] = set()
    ticks = 0
    rss_pages = 0
    while pending:
        pid = pending.pop()
        if pid in visited or pid not in snapshot:
            continue
        visited.add(pid)
        _, process_ticks, process_rss = snapshot[pid]
        ticks += process_ticks
        rss_pages += process_rss
        pending.extend(children.get(pid, ()))
    return ticks, rss_pages


def parse_memory_bytes(value: str) -> int:
    used = value.split("/", maxsplit=1)[0].strip()
    match = MEMORY_PATTERN.fullmatch(used)
    _require(match is not None, f"Docker memory usage has an unknown format: {value!r}")
    amount = Decimal(match.group(1))
    unit = match.group(2).upper()
    multipliers = {
        "B": 1,
        "KB": 1000,
        "KIB": 1024,
        "MB": 1000**2,
        "MIB": 1024**2,
        "GB": 1000**3,
        "GIB": 1024**3,
        "TB": 1000**4,
        "TIB": 1024**4,
    }
    return int((amount * multipliers[unit]).to_integral_value(rounding=ROUND_CEILING))


def _docker_stats_record(record: Any) -> tuple[float, int]:
    _require(isinstance(record, dict), "Docker stats JSON is not an object")
    cpu_text = record.get("CPUPerc", record.get("CPUPERCENT"))
    memory_text = record.get("MemUsage", record.get("MEMUSAGE"))
    _require(isinstance(cpu_text, str) and cpu_text.endswith("%"), "Docker CPU percent is missing")
    _require(isinstance(memory_text, str), "Docker memory usage is missing")
    cpu_percent = _finite_float(cpu_text[:-1].strip(), "Docker CPU percent", minimum=0.0)
    return cpu_percent, parse_memory_bytes(memory_text)


def parse_docker_stats(text: str) -> tuple[float, int]:
    documents = [line for line in text.splitlines() if line.strip()]
    _require(documents, "Docker stats returned no JSON object")
    try:
        record = json.loads(documents[0])
    except json.JSONDecodeError as error:
        raise PerformanceDataError("Docker stats returned malformed JSON") from error
    return _docker_stats_record(record)


def docker_container_stats(name: str) -> tuple[float, int] | None:
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "json", name],
            text=True,
            capture_output=True,
            check=False,
            timeout=10.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    try:
        return parse_docker_stats(result.stdout)
    except PerformanceDataError:
        return None


def docker_containers_stats(names: Sequence[str]) -> dict[str, tuple[float, int] | None]:
    """Fetch all normal-path container samples with one Docker invocation."""

    if not names:
        return {}
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "json", *names],
            text=True,
            capture_output=True,
            check=False,
            timeout=10.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {name: None for name in names}
    if result.returncode != 0:
        # Docker emits no usable rows if any named container is absent.  The
        # slow fallback is limited to that diagnostic/error path.
        return {name: docker_container_stats(name) for name in names}

    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if len(lines) != len(names):
        return {name: None for name in names}
    parsed: dict[str, tuple[float, int]] = {}
    ordered: list[tuple[float, int]] = []
    try:
        for line in lines:
            record = json.loads(line)
            stats = _docker_stats_record(record)
            ordered.append(stats)
            if isinstance(record, dict):
                for identity_field in ("Name", "Container", "NAME", "CONTAINER"):
                    identity = record.get(identity_field)
                    if isinstance(identity, str):
                        parsed[identity] = stats
    except (json.JSONDecodeError, PerformanceDataError):
        return {name: None for name in names}
    return {name: parsed.get(name, ordered[index]) for index, name in enumerate(names)}


def parse_process_spec(spec: str) -> tuple[str, int]:
    label, separator, pid_text = spec.rpartition(":")
    _require(separator == ":" and label != "", f"invalid process specification: {spec!r}")
    pid = _nonnegative_int(pid_text, f"process {label} PID")
    _require(pid > 0, f"process {label} PID must be positive")
    return label, pid


def parse_container_spec(spec: str) -> tuple[str, str]:
    if "=" in spec:
        label, name = spec.split("=", maxsplit=1)
    else:
        label = name = spec
    _require(label != "" and name != "", f"invalid container specification: {spec!r}")
    return label, name


def sample_resource_rows(
    timestamp_epoch_ms: int,
    process_specs: Sequence[tuple[str, int]],
    container_specs: Sequence[tuple[str, str]],
    *,
    previous_process_ticks: Mapping[str, tuple[int, float]],
    monotonic_seconds: float,
    proc_snapshot: Mapping[int, tuple[int, int, int]] | None = None,
    clock_ticks_per_second: int | None = None,
    page_size: int | None = None,
    docker_sampler: Callable[
        [Sequence[str]], Mapping[str, tuple[float, int] | None]
    ] = docker_containers_stats,
) -> tuple[list[dict[str, Any]], dict[str, tuple[int, float]]]:
    """Take one resource sample; dependency arguments make this unit-testable."""

    if proc_snapshot is None:
        proc_snapshot = read_proc_snapshot()
    if clock_ticks_per_second is None:
        clock_ticks_per_second = int(os.sysconf("SC_CLK_TCK"))
    if page_size is None:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
    _require(clock_ticks_per_second > 0 and page_size > 0, "invalid host clock/page size")

    rows: list[dict[str, Any]] = []
    next_ticks: dict[str, tuple[int, float]] = {}
    for label, root_pid in process_specs:
        totals = process_tree_totals(proc_snapshot, root_pid)
        if totals is None:
            rows.append(
                {
                    "timestamp": timestamp_epoch_ms,
                    "component": label,
                    "cpu_percent": "",
                    "rss_bytes": "",
                    "source": "unavailable",
                }
            )
            continue
        ticks, rss_pages = totals
        cpu_percent: float | str = ""
        previous = previous_process_ticks.get(label)
        if previous is not None:
            previous_ticks, previous_time = previous
            elapsed = monotonic_seconds - previous_time
            if elapsed > 0 and ticks >= previous_ticks:
                cpu_percent = (
                    (ticks - previous_ticks) / clock_ticks_per_second / elapsed * 100.0
                )
        next_ticks[label] = (ticks, monotonic_seconds)
        rows.append(
            {
                "timestamp": timestamp_epoch_ms,
                "component": label,
                "cpu_percent": cpu_percent,
                "rss_bytes": rss_pages * page_size,
                "source": "proc",
            }
        )

    docker_stats = docker_sampler([name for _, name in container_specs])
    for label, name in container_specs:
        stats = docker_stats.get(name)
        if stats is None:
            rows.append(
                {
                    "timestamp": timestamp_epoch_ms,
                    "component": label,
                    "cpu_percent": "",
                    "rss_bytes": "",
                    "source": "unavailable",
                }
            )
            continue
        cpu_percent, rss_bytes = stats
        rows.append(
            {
                "timestamp": timestamp_epoch_ms,
                "component": label,
                "cpu_percent": cpu_percent,
                "rss_bytes": rss_bytes,
                "source": "docker",
            }
        )
    return rows, next_ticks


def sample_resources(
    output: TextIO,
    process_specs: Sequence[tuple[str, int]],
    container_specs: Sequence[tuple[str, str]],
    *,
    interval_seconds: float,
    duration_seconds: float,
    stop_file: Path | None = None,
) -> int:
    """Sample resources at a bounded cadence and return the sample count."""

    interval = _finite_float(interval_seconds, "sample interval", minimum=0.0)
    duration = _finite_float(duration_seconds, "sample duration", minimum=0.0)
    _require(interval > 0, "sample interval must be positive")
    _require(process_specs or container_specs, "at least one sampled component is required")
    labels = [label for label, _ in process_specs] + [label for label, _ in container_specs]
    _require(len(labels) == len(set(labels)), "sampled component labels must be unique")

    writer = csv.DictWriter(output, fieldnames=RESOURCE_FIELDS, lineterminator="\n")
    writer.writeheader()
    output.flush()
    sample_limit = math.floor(duration / interval) + 1
    started = time.monotonic()
    deadline = started + duration
    previous_ticks: dict[str, tuple[int, float]] = {}
    samples = 0
    for index in range(sample_limit):
        target = started + index * interval
        while True:
            now = time.monotonic()
            if index > 0 and now > deadline:
                return samples
            delay = target - now
            if delay <= 0:
                break
            if stop_file is not None and stop_file.exists():
                return samples
            time.sleep(min(delay, 0.1))
        if stop_file is not None and stop_file.exists():
            break
        monotonic_seconds = time.monotonic()
        rows, previous_ticks = sample_resource_rows(
            time.time_ns() // 1_000_000,
            process_specs,
            container_specs,
            previous_process_ticks=previous_ticks,
            monotonic_seconds=monotonic_seconds,
        )
        writer.writerows(rows)
        output.flush()
        samples += 1
    return samples


def _flatten(prefix: str, value: Any, rows: list[dict[str, Any]]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            _flatten(child_prefix, child, rows)
    else:
        rows.append({"metric": prefix, "value": value})


def metrics_rows(document: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    _flatten("", document, rows)
    return rows


def _write_json(path: str, document: Any) -> None:
    text = json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if path == "-":
        sys.stdout.write(text)
    else:
        Path(path).write_text(text, encoding="utf-8")


def _write_csv(path: str, rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> None:
    if path == "-":
        stream = sys.stdout
        close = False
    else:
        stream = Path(path).open("w", encoding="utf-8", newline="")
        close = True
    try:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    finally:
        if close:
            stream.close()


def _emit_metrics(args: argparse.Namespace, document: Mapping[str, Any]) -> None:
    output_json = args.output_json
    output_csv = args.output_csv
    if output_json is None and output_csv is None:
        output_json = "-"
    _require(
        not (output_json == "-" and output_csv == "-"),
        "JSON and CSV outputs cannot both use stdout",
    )
    if output_json is not None:
        _write_json(output_json, document)
    if output_csv is not None:
        _write_csv(output_csv, metrics_rows(document), ("metric", "value"))


def _load_json(path: str) -> Any:
    if path == "-":
        return json.load(sys.stdin)
    with Path(path).open("r", encoding="utf-8") as stream:
        return json.load(stream)


def _sample_command(args: argparse.Namespace) -> None:
    processes = [parse_process_spec(spec) for spec in args.process]
    containers = [parse_container_spec(spec) for spec in args.container]
    if args.output == "-":
        sample_resources(
            sys.stdout,
            processes,
            containers,
            interval_seconds=args.interval_seconds,
            duration_seconds=args.duration_seconds,
            stop_file=Path(args.stop_file) if args.stop_file else None,
        )
    else:
        with Path(args.output).open("w", encoding="utf-8", newline="") as stream:
            sample_resources(
                stream,
                processes,
                containers,
                interval_seconds=args.interval_seconds,
                duration_seconds=args.duration_seconds,
                stop_file=Path(args.stop_file) if args.stop_file else None,
            )


def _benchbase_command(args: argparse.Namespace) -> None:
    _emit_metrics(
        args,
        analyze_benchbase_files(Path(args.raw_csv), Path(args.summary_json)),
    )


def _outcomes_command(args: argparse.Namespace) -> None:
    _emit_metrics(
        args,
        analyze_transaction_outcomes(
            _load_json(args.outcomes_json), _load_json(args.benchbase_metrics_json)
        ),
    )


def _redis_command(args: argparse.Namespace) -> None:
    _emit_metrics(
        args,
        analyze_redis_xrange(
            _load_json(args.input_json),
            window_after_stream_id=args.window_after_stream_id,
            window_through_stream_id=args.window_through_stream_id,
        ),
    )


def _client_command(args: argparse.Namespace) -> None:
    _emit_metrics(args, analyze_client_xrange(_load_json(args.input_json)))


def _summarize_command(args: argparse.Namespace) -> None:
    with Path(args.input_csv).open("r", encoding="utf-8", newline="") as stream:
        rows = read_trial_csv(stream)
    summary = summarize_trials(
        rows,
        baseline_mode=args.baseline_mode,
        mode_column=args.mode_column,
        metrics=args.metric or None,
        higher_is_better_metrics=args.higher_is_better,
    )
    output_csv = args.output_csv
    output_json = args.output_json
    if output_csv is None and output_json is None:
        output_csv = "-"
    _require(
        not (output_csv == "-" and output_json == "-"),
        "JSON and CSV outputs cannot both use stdout",
    )
    if output_csv is not None:
        _write_csv(output_csv, summary, TRIAL_SUMMARY_FIELDS)
    if output_json is not None:
        overhead_formulas = (
            {
                "higher_is_better": "100 * (baseline_median - mode_median) / baseline_median",
                "lower_is_better": "100 * (mode_median - baseline_median) / baseline_median",
            }
            if args.baseline_mode is not None
            else {}
        )
        _write_json(
            output_json,
            {
                "baseline_mode": args.baseline_mode,
                "overhead_formulas": overhead_formulas,
                "summary": summary,
            },
        )


def _add_metric_outputs(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output-json", help="write JSON metrics to this path (or - for stdout)")
    parser.add_argument(
        "--output-csv", help="write flattened metric,value CSV to this path (or - for stdout)"
    )


def _bounded_copy_command(args: argparse.Namespace) -> None:
    maximum = _nonnegative_int(args.max_bytes, "bounded copy max bytes")
    _require(maximum > 0, "bounded copy max bytes must be positive")
    output = Path(args.output)
    written = 0
    created = False
    try:
        destination = output.open("xb")
        created = True
        with destination:
            while chunk := sys.stdin.buffer.read(1024 * 1024):
                if len(chunk) > maximum - written:
                    raise PerformanceDataError(
                        f"captured artifact exceeds its remaining {maximum}-byte budget"
                    )
                destination.write(chunk)
                written += len(chunk)
    except BaseException:
        if created:
            output.unlink(missing_ok=True)
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    bounded_copy = subparsers.add_parser(
        "bounded-copy", help="copy stdin to one new artifact within a byte limit"
    )
    bounded_copy.add_argument("--output", required=True)
    bounded_copy.add_argument("--max-bytes", required=True)
    bounded_copy.set_defaults(handler=_bounded_copy_command)

    sample = subparsers.add_parser("sample-resources", help="sample process-tree and container resources")
    sample.add_argument("--process", action="append", default=[], metavar="LABEL:PID")
    sample.add_argument(
        "--container",
        action="append",
        default=[],
        metavar="[LABEL=]NAME",
        help="sample a named Docker container; label defaults to its name",
    )
    sample.add_argument("--interval-seconds", required=True, type=float)
    sample.add_argument("--duration-seconds", required=True, type=float)
    sample.add_argument("--stop-file")
    sample.add_argument("--output", default="-")
    sample.set_defaults(handler=_sample_command)

    benchbase = subparsers.add_parser("analyze-benchbase", help="analyze BenchBase raw and summary output")
    benchbase.add_argument("--raw-csv", required=True)
    benchbase.add_argument("--summary-json", required=True)
    _add_metric_outputs(benchbase)
    benchbase.set_defaults(handler=_benchbase_command)

    outcomes = subparsers.add_parser(
        "analyze-outcomes", help="validate measurement-window per-type outcomes"
    )
    outcomes.add_argument("--outcomes-json", required=True)
    outcomes.add_argument("--benchbase-metrics-json", required=True)
    _add_metric_outputs(outcomes)
    outcomes.set_defaults(handler=_outcomes_command)

    redis = subparsers.add_parser("analyze-redis", help="analyze a Redis CDC XRANGE JSON dump")
    redis.add_argument("--input-json", required=True)
    redis.add_argument("--window-after-stream-id")
    redis.add_argument("--window-through-stream-id")
    _add_metric_outputs(redis)
    redis.set_defaults(handler=_redis_command)

    client = subparsers.add_parser("analyze-client", help="analyze a client-stream XRANGE JSON dump")
    client.add_argument("--input-json", required=True)
    _add_metric_outputs(client)
    client.set_defaults(handler=_client_command)

    summarize = subparsers.add_parser("summarize-trials", help="summarize repeated raw trial CSV rows")
    summarize.add_argument("--input-csv", required=True)
    summarize.add_argument(
        "--baseline-mode",
        help="optional mode used for overhead columns; omit for baseline-free summaries",
    )
    summarize.add_argument("--mode-column", default="mode")
    summarize.add_argument("--metric", action="append", default=[])
    summarize.add_argument("--higher-is-better", action="append", default=[])
    summarize.add_argument("--output-csv")
    summarize.add_argument("--output-json")
    summarize.set_defaults(handler=_summarize_command)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.handler(args)
    except (PerformanceDataError, OSError, json.JSONDecodeError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
