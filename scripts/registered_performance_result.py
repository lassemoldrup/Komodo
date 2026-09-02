#!/usr/bin/env python3
"""Build one benchmark/backend-neutral registered performance result."""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any


STREAM_ID = re.compile(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)\Z")
PARAMETER_NAME = re.compile(r"[A-Z][A-Z0-9_]{0,95}\Z")
OPERATIONS_PER_TRANSACTION = "BENCHBASE_MEASUREMENT_OPERATIONS_PER_TRANSACTION"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def workload_parameters(path: Path) -> dict[str, str]:
    parameters: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        name, separator, value = line.partition("=")
        require(
            bool(separator)
            and PARAMETER_NAME.fullmatch(name) is not None
            and bool(value)
            and name not in parameters,
            f"malformed or duplicate workload parameter: {line!r}",
        )
        parameters[name] = value
    return parameters


def declared_operations_per_transaction(parameters: dict[str, str]) -> int | None:
    raw = parameters.get(OPERATIONS_PER_TRANSACTION)
    if raw is None:
        return None
    require(re.fullmatch(r"[1-9][0-9]*", raw) is not None, "operations per transaction is invalid")
    return int(raw)


def only(directory: Path, pattern: str) -> Path:
    paths = sorted(directory.glob(pattern))
    require(len(paths) == 1, f"expected one {pattern} artifact in {directory}")
    return paths[0]


def number(value: Any, label: str) -> float:
    require(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value)),
        f"{label} is not finite numeric data",
    )
    return float(value)


def integer(value: Any, label: str) -> int:
    require(isinstance(value, int) and not isinstance(value, bool), f"{label} is not an integer")
    return value


def stream_key(stream_id: str) -> tuple[int, int]:
    match = STREAM_ID.fullmatch(stream_id)
    require(match is not None, f"invalid Redis stream ID: {stream_id!r}")
    result = int(match.group(1)), int(match.group(2))
    require(
        result[0] <= 0xFFFFFFFFFFFFFFFF and result[1] <= 0xFFFFFFFFFFFFFFFF,
        f"Redis stream ID exceeds uint64: {stream_id!r}",
    )
    return result


def stream_ms(stream_id: str) -> int:
    return stream_key(stream_id)[0]


def payload(entry: Any, label: str) -> dict[str, Any]:
    require(
        isinstance(entry, list)
        and len(entry) == 2
        and isinstance(entry[0], str)
        and isinstance(entry[1], list)
        and len(entry[1]) % 2 == 0,
        f"malformed {label} XRANGE entry",
    )
    fields = entry[1]
    candidates = [fields[index + 1] for index in range(0, len(fields), 2) if fields[index] == "payload"]
    if not candidates and len(fields) == 2:
        candidates = [fields[1]]
    require(len(candidates) == 1 and isinstance(candidates[0], str), f"{label} payload is missing")
    document = json.loads(candidates[0])
    require(isinstance(document, dict), f"{label} payload is not an object")
    return document


def cdc_entry(entry: Any) -> tuple[str, str, str]:
    require(
        isinstance(entry, list)
        and len(entry) == 2
        and isinstance(entry[0], str)
        and isinstance(entry[1], list)
        and len(entry[1]) == 2
        and isinstance(entry[1][0], str)
        and isinstance(entry[1][1], str),
        "malformed CDC XRANGE entry",
    )
    stream_key(entry[0])
    return entry[0], entry[1][0], entry[1][1]


def fence_window(
    entries: Any, reconciliation: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    require(isinstance(entries, list), "CDC XRANGE artifact is not an array")
    parsed = [cdc_entry(entry) for entry in entries]
    ids = [entry[0] for entry in parsed]
    require(
        all(stream_key(left) < stream_key(right) for left, right in zip(ids, ids[1:])),
        "CDC XRANGE stream IDs are not strictly increasing",
    )
    completed = {
        "LOAD_FINISHED": reconciliation.get("load_finished_cdc_stream_id"),
        "END_FENCE": reconciliation.get("end_fence_cdc_stream_id"),
    }
    result: dict[str, dict[str, Any]] = {}
    for phase, stream_id in completed.items():
        require(isinstance(stream_id, str) and stream_id != "0-0", f"{phase} completed CDC stream ID is missing")
        stream_key(stream_id)
        require(ids.count(stream_id) == 1, f"{phase} completed CDC entry is absent or duplicated")
        result[phase] = {
            "completed_cdc_stream_id": stream_id,
            "completed_cdc_epoch_ms": stream_ms(stream_id),
        }
    require(
        stream_key(result["LOAD_FINISHED"]["completed_cdc_stream_id"])
        < stream_key(result["END_FENCE"]["completed_cdc_stream_id"]),
        "component fence completed CDC order is invalid",
    )
    return result


def final_status(entries: Any, run_id: str) -> tuple[str, dict[str, Any]]:
    require(isinstance(entries, list), "status XRANGE artifact is not an array")
    finals = []
    for entry in entries:
        document = payload(entry, "status")
        if document.get("run_id") == run_id and document.get("lifecycle_state") == "FINAL":
            finals.append((entry[0], document))
    require(len(finals) == 1, "run-scoped FINAL status count differs")
    return finals[0]


def cdc_window_counts(entries: Any, after_id: str, through_id: str) -> dict[str, int]:
    lower_bound = stream_key(after_id)
    through = stream_key(through_id)
    require(lower_bound < through, "CDC transport window is not positive")
    counts = {
        "entries": 0,
        "logical_utf8_bytes": 0,
        "rows": 0,
        "control_rows": 0,
        "transaction_begins": 0,
        "transaction_ends": 0,
    }
    saw_after = False
    saw_through = False
    previous: tuple[int, int] | None = None
    for entry in entries:
        stream_id, field, value = cdc_entry(entry)
        identity = stream_key(stream_id)
        require(previous is None or previous < identity, "CDC XRANGE stream IDs are not strictly increasing")
        previous = identity
        saw_after = saw_after or stream_id == after_id
        saw_through = saw_through or stream_id == through_id
        if identity <= lower_bound or identity > through:
            continue
        counts["entries"] += 1
        counts["logical_utf8_bytes"] += len(field.encode("utf-8")) + len(value.encode("utf-8"))
        document = payload(entry, "CDC")
        if document.get("status") == "BEGIN":
            counts["transaction_begins"] += 1
        elif document.get("status") == "END":
            counts["transaction_ends"] += 1
        elif isinstance(document.get("source"), dict) and isinstance(
            document.get("op"), str
        ):
            counts["rows"] += 1
            after_row = document.get("after")
            if isinstance(after_row, dict):
                folded = {str(key).casefold(): value for key, value in after_row.items()}
                if isinstance(folded.get("run_id"), str) and isinstance(
                    folded.get("phase"), str
                ):
                    counts["control_rows"] += 1
    require(saw_after and saw_through, "CDC transport window boundary is absent")
    return counts


def compact_transport_entries(assembled_transactions: int, raw_rows: int) -> int:
    require(assembled_transactions >= 0, "assembled transaction count is negative")
    require(raw_rows >= 0, "raw row count is negative")
    return assembled_transactions * 2 + raw_rows + 3


def rate(count: int, seconds: float) -> float:
    require(seconds > 0, "measurement interval is not positive")
    return count / seconds


def redis_info(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if ":" not in line or line.startswith("#"):
            continue
        name, value = line.split(":", 1)
        value = value.rstrip("\r")
        if value.isdigit():
            values[name] = int(value)
    return values


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-dir", required=True, type=Path)
    parser.add_argument("--backend", required=True)
    parser.add_argument("--benchmark", required=True)
    parser.add_argument("--mode", required=True)
    parser.add_argument(
        "--execution-profile",
        required=True,
        choices=("direct_benchbase", "coordinated_cdc_only", "coordinated_e2e"),
    )
    parser.add_argument("--workload", required=True)
    parser.add_argument("--capture-profile", required=True)
    parser.add_argument("--repetition", required=True, type=int)
    parser.add_argument("--drain-limit-seconds", required=True, type=float)
    parser.add_argument("--high-water-threshold-percent", default=70.0, type=float)
    parser.add_argument(
        "--evidence-retention", required=True, choices=("compact", "full")
    )
    parser.add_argument("--headline-transaction-type")
    parser.add_argument("--headline-rate-label")
    args = parser.parse_args()

    summary = load(only(args.artifact_dir / "workload-results", "*.summary.json"))
    outcomes = load(only(args.artifact_dir / "workload-results", "*.outcomes.json"))
    require(summary.get("Benchmark Type") == args.benchmark, "BenchBase benchmark identity differs")
    require(outcomes.get("benchmark") == args.benchmark, "outcome benchmark identity differs")
    elapsed_ns = integer(outcomes.get("elapsed_nanoseconds"), "outcome elapsed nanoseconds")
    require(elapsed_ns > 0 and elapsed_ns == summary.get("Elapsed Time (nanoseconds)"), "measurement durations differ")
    elapsed_seconds = elapsed_ns / 1_000_000_000.0
    totals = outcomes.get("totals")
    types = outcomes.get("transaction_types")
    require(isinstance(totals, dict) and isinstance(types, dict) and types, "outcome counters are incomplete")
    for name, counters in types.items():
        require(isinstance(name, str) and isinstance(counters, dict), "transaction outcome entry is malformed")
    counters = {
        name: integer(totals.get(name), f"outcome total {name}")
        for name in (
            "logical_transactions_started",
            "physical_attempts_started",
            "successful_transactions",
            "retry_attempts",
            "retryable_failure_events",
            "definitively_aborted_attempts",
            "terminal_aborted_transactions",
            "error_transactions",
            "unknown_status_transactions",
            "ambiguous_attempts",
        )
    }
    latency = summary.get("Latency Distribution")
    require(isinstance(latency, dict), "BenchBase latency distribution is missing")
    parameters = workload_parameters(args.artifact_dir / "workload-parameters.txt")
    operations_per_transaction = declared_operations_per_transaction(parameters)
    retention = load(args.artifact_dir / "evidence-retention.json")
    require(
        retention.get("kind") == "performance_evidence_retention"
        and retention.get("retained") == args.evidence_retention
        and retention.get("full_streams_retained")
        is (args.evidence_retention == "full"),
        "evidence retention record differs",
    )
    result: dict[str, Any] = {
        "kind": "registered_performance_cell_result",
        "backend": args.backend,
        "benchmark": args.benchmark,
        "mode": args.mode,
        "execution_profile": args.execution_profile,
        "workload": args.workload,
        "capture_profile": args.capture_profile,
        "evidence_retention": args.evidence_retention,
        "repetition": args.repetition,
        "accepted": False,
        "client_measurement_window_seconds": elapsed_seconds,
        "client_logical_transactions_per_second": rate(counters["logical_transactions_started"], elapsed_seconds),
        "client_successful_transactions_per_second": rate(counters["successful_transactions"], elapsed_seconds),
        "client_physical_attempts_per_second": rate(counters["physical_attempts_started"], elapsed_seconds),
        "client_average_latency_microseconds": number(latency.get("Average Latency (microseconds)"), "mean latency"),
        "client_p95_latency_microseconds": number(latency.get("95th Percentile Latency (microseconds)"), "p95 latency"),
        "outcome_totals": counters,
        "transaction_types": types,
        "workload_parameters": parameters,
    }
    if operations_per_transaction is not None:
        result["client_operations_per_transaction"] = operations_per_transaction
        result["client_logical_operations_per_second"] = (
            result["client_logical_transactions_per_second"] * operations_per_transaction
        )
    bootstrap = load(args.artifact_dir / "bootstrap-manifest.json")
    loader_validation = load(args.artifact_dir / "loader-validation.json")
    require(
        bootstrap.get("kind") == "bootstrap_manifest"
        and loader_validation.get("validated") is True
        and sorted(bootstrap.get("table_row_counts", []), key=lambda value: value["table"])
        == sorted(loader_validation.get("observed_table_row_counts", []), key=lambda value: value["table"]),
        "bootstrap and loader validation differ",
    )
    digest = bootstrap.get("canonical_state_digest")
    require(
        isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest) is not None,
        "bootstrap digest is invalid",
    )
    result["bootstrap_canonical_state_digest"] = digest
    result["bootstrap_row_count"] = sum(
        integer(value.get("row_count"), "bootstrap row count")
        for value in bootstrap["table_row_counts"]
    )
    if args.headline_transaction_type is not None:
        require(args.headline_rate_label is not None, "headline rate label is missing")
        require(args.headline_transaction_type in types, "headline transaction type is absent")
        successful = integer(types[args.headline_transaction_type].get("successful_transactions"), "headline success count")
        result["headline_rate"] = {
            "transaction_type": args.headline_transaction_type,
            "label": args.headline_rate_label,
            "value": rate(successful, elapsed_seconds) * 60.0,
        }

    require(counters["unknown_status_transactions"] == 0, "unknown-status transactions are nonzero")
    require(counters["ambiguous_attempts"] == 0, "ambiguous attempts are nonzero")
    require(counters["logical_transactions_started"] > 0, "no measured logical transactions")

    if args.execution_profile in {"direct_benchbase", "coordinated_cdc_only"}:
        marker = load(args.artifact_dir / "marker-storage.json")
        require(marker.get("row_count") == 0, "uninstrumented workload wrote marker rows")
        outcome = "UNINSTRUMENTED"
        if args.execution_profile == "coordinated_cdc_only":
            evidence = load(args.artifact_dir / "cdc-only-evidence.json")
            require(
                evidence.get("kind") == "cdc_only_evidence"
                and evidence.get("client_instrumentation") is False
                and evidence.get("monitor_started") is False
                and evidence.get("load_fence_seen") is True
                and evidence.get("end_fence_seen") is True
                and integer(evidence.get("cdc_entries"), "CDC-only stream length") > 0,
                "CDC-only evidence is incomplete",
            )
            outcome = "CDC_ONLY"
        result.update(
            {
                "outcome": outcome,
                "evidence_reconciled": None,
                "joined_transactions": None,
                "unmatched_client_records": None,
                "unmatched_workload_cdc_slots": None,
                "accepted": True,
            }
        )
    else:
        require(counters["error_transactions"] == 0, "full E2E unexpected SQL errors are nonzero")
        run_id = (args.artifact_dir / "run-id.txt").read_text(encoding="utf-8").strip()
        statuses = load(args.artifact_dir / "status-stream.json")
        final_id, final = final_status(statuses, run_id)
        reconciliation = final.get("reconciliation")
        require(isinstance(reconciliation, dict), "FINAL status lacks reconciliation")
        if args.evidence_retention == "full":
            cdc = load(args.artifact_dir / "cdc-stream.json")
            boundaries = fence_window(cdc, reconciliation)
            window = cdc_window_counts(
                cdc,
                reconciliation["load_finished_cdc_stream_id"],
                reconciliation["end_fence_cdc_stream_id"],
            )
        else:
            compact_boundaries = load(
                args.artifact_dir / "cdc-boundary-entries.json"
            )
            boundaries = fence_window(compact_boundaries, reconciliation)
            window = None
        start_id = boundaries["LOAD_FINISHED"]["completed_cdc_stream_id"]
        end_id = boundaries["END_FENCE"]["completed_cdc_stream_id"]
        start_ms = boundaries["LOAD_FINISHED"]["completed_cdc_epoch_ms"]
        end_ms = boundaries["END_FENCE"]["completed_cdc_epoch_ms"]
        final_ms = stream_ms(final_id)
        component_seconds = (end_ms - start_ms) / 1000.0
        e2e_seconds = (final_ms - start_ms) / 1000.0
        drain_seconds = (final_ms - end_ms) / 1000.0
        require(component_seconds > 0 and e2e_seconds > 0 and drain_seconds >= 0, "Redis-clock intervals are invalid")
        require(drain_seconds <= args.drain_limit_seconds, "drain exceeded its configured limit")
        monitor = load(args.artifact_dir / "monitor-metrics.json")
        assembler = monitor.get("assembler")
        joiner = monitor.get("joiner")
        checker = monitor.get("checker")
        require(all(isinstance(value, dict) for value in (assembler, joiner, checker, reconciliation)), "monitor result is incomplete")
        assembled = integer(assembler.get("completed_workload_transactions"), "assembled workload transactions")
        joined = integer(joiner.get("joined_workload_transactions"), "joined workload transactions")
        checked = integer(checker.get("workload_transactions"), "checked workload transactions")
        raw_rows = integer(assembler.get("workload_raw_row_events"), "assembler raw workload rows")
        writes = integer(assembler.get("workload_normalized_write_events"), "assembler normalized writes")
        if window is None:
            transport_entries = compact_transport_entries(assembled, raw_rows)
        else:
            transport_entries = window["entries"]
            require(
                raw_rows == window["rows"] - window["control_rows"],
                "CDC and assembler raw workload rows differ",
            )
        require(
            assembled
            == joined
            == checked
            == integer(
                reconciliation.get("joined_workload_transactions"),
                "reconciled transactions",
            ),
            "assembler/joiner/checker totals differ",
        )
        require(final.get("outcome") == "PASS" and reconciliation.get("evidence_reconciled") is True, "full E2E verdict is not an evidence-complete PASS")
        require(reconciliation.get("unmatched_client_records") == 0 and reconciliation.get("unmatched_workload_cdc_slots") == 0, "full E2E evidence is unmatched")
        require(joiner.get("pending_client_records") == 0 and joiner.get("pending_cdc_slots") == 0, "terminal monitor queues are nonzero")
        limits = monitor.get("limits")
        require(isinstance(limits, dict), "monitor limit report is missing")
        assembler_limits = limits.get("assembler")
        joiner_limits = limits.get("joiner")
        checker_limits = limits.get("checker")
        require(
            all(isinstance(value, dict) for value in (assembler_limits, joiner_limits, checker_limits)),
            "monitor component limits are missing",
        )
        capacity_pairs = (
            (assembler.get("active_transactions_high_water"), assembler_limits.get("max_active_transactions")),
            (assembler.get("observed_events_per_transaction_high_water"), assembler_limits.get("max_events_per_transaction")),
            (assembler.get("active_assembly_high_water_bytes"), assembler_limits.get("max_active_bytes")),
            (assembler.get("completed_transactions"), assembler_limits.get("max_completed_transactions")),
            (assembler.get("retained_identity_high_water_bytes"), assembler_limits.get("max_retained_identity_bytes")),
            (joiner.get("pending_client_high_water_records"), joiner_limits.get("max_pending_client_records")),
            (joiner.get("pending_client_high_water_bytes"), joiner_limits.get("max_pending_client_bytes")),
            (joiner.get("pending_cdc_high_water_slots"), joiner_limits.get("max_pending_cdc_slots")),
            (joiner.get("pending_cdc_high_water_bytes"), joiner_limits.get("max_pending_cdc_bytes")),
            (joiner.get("retained_identity_high_water_records"), joiner_limits.get("max_retained_identity_records")),
            (joiner.get("retained_identity_high_water_bytes"), joiner_limits.get("max_retained_identity_bytes")),
            (joiner.get("released_events_per_transaction_high_water"), joiner_limits.get("max_events_per_transaction")),
            (joiner.get("bootstrap_state_high_water_rows"), joiner_limits.get("max_bootstrap_state_rows")),
            (joiner.get("bootstrap_state_high_water_bytes"), joiner_limits.get("max_bootstrap_state_bytes")),
            (checker.get("mapped_identity_high_water_records"), checker_limits.get("max_mapped_identity_records")),
            (checker.get("mapped_identity_high_water_bytes"), checker_limits.get("max_mapped_identity_bytes")),
            (checker.get("events_per_transaction_high_water"), checker_limits.get("max_events_per_transaction")),
            (checker.get("engine", {}).get("peak_retained_state_bytes"), checker_limits.get("max_retained_state_bytes")),
        )
        utilizations = []
        for index, (observed, capacity) in enumerate(capacity_pairs):
            observed_value = number(observed, f"capacity observation {index}")
            capacity_value = number(capacity, f"capacity limit {index}")
            require(capacity_value > 0, f"capacity limit {index} is not positive")
            utilizations.append(observed_value * 100.0 / capacity_value)
        info = redis_info(args.artifact_dir / "redis-info-after.txt")
        redis_limit = integer(info.get("maxmemory"), "Redis maxmemory")
        redis_peak = integer(info.get("used_memory_peak"), "Redis peak memory")
        require(redis_limit > 0, "Redis maxmemory is not positive")
        redis_utilization = redis_peak * 100.0 / redis_limit
        maximum_utilization = max([redis_utilization, *utilizations])
        require(
            0 < args.high_water_threshold_percent <= 100
            and maximum_utilization < args.high_water_threshold_percent,
            "declared resource high-water threshold was reached",
        )

        def active_rate(count: int, nanoseconds: Any, label: str) -> float:
            duration = integer(nanoseconds, f"{label} active nanoseconds") / 1_000_000_000.0
            return rate(count, duration)

        result.update(
            {
                "outcome": final["outcome"],
                "evidence_reconciled": True,
                "joined_transactions": joined,
                "unmatched_client_records": 0,
                "unmatched_workload_cdc_slots": 0,
                "component_window_after_stream_id": start_id,
                "component_window_through_stream_id": end_id,
                "component_window_start_epoch_ms": start_ms,
                "component_window_end_epoch_ms": end_ms,
                "component_workload_window_seconds": component_seconds,
                "final_status_stream_id": final_id,
                "final_status_epoch_ms": final_ms,
                "e2e_end_to_verdict_seconds": e2e_seconds,
                "drain_tail_seconds": drain_seconds,
                "resource_high_water_max_percent": maximum_utilization,
                "resource_high_water_threshold_percent": args.high_water_threshold_percent,
                "redis_used_memory_peak_bytes": redis_peak,
                "redis_used_memory_peak_percent": redis_utilization,
                "cdc_transport_entries": transport_entries,
                "cdc_transport_entry_count_source": (
                    "raw_stream" if window is not None
                    else "derived_from_validated_framing"
                ),
                "cdc_workload_rows": raw_rows,
                "cdc_workload_rows_per_second": rate(raw_rows, component_seconds),
                "monitor_assembled_transactions": assembled,
                "monitor_assembler_transactions_per_component_second": rate(assembled, component_seconds),
                "monitor_assembler_transactions_per_active_second": active_rate(assembled, assembler.get("workload_processing_total_nanoseconds"), "assembler"),
                "monitor_normalized_writes": writes,
                "monitor_joined_transactions": joined,
                "monitor_joiner_transactions_per_component_second": rate(joined, component_seconds),
                "monitor_joiner_transactions_per_active_second": active_rate(joined, joiner.get("workload_processing_total_nanoseconds"), "joiner"),
                "monitor_checked_transactions": checked,
                "monitor_checker_transactions_per_component_second": rate(checked, component_seconds),
                "monitor_checker_transactions_per_active_second": active_rate(checked, checker.get("workload_consume_total_nanoseconds"), "checker"),
                "e2e_transactions_per_second": rate(checked, e2e_seconds),
                "accepted": True,
            }
        )
        if window is not None:
            result["cdc_transport_logical_utf8_bytes"] = window[
                "logical_utf8_bytes"
            ]

    (args.artifact_dir / "result.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise SystemExit(f"registered performance result failed: {error}")
