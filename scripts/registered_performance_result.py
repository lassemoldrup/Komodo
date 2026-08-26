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


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


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
    return int(match.group(1)), int(match.group(2))


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


def folded(document: dict[str, Any]) -> dict[str, Any]:
    return {str(key).casefold(): value for key, value in document.items()}


def transaction_id(document: dict[str, Any]) -> str | None:
    transaction = document.get("transaction")
    if isinstance(transaction, dict) and isinstance(transaction.get("id"), str):
        return transaction["id"]
    source = document.get("source")
    if isinstance(source, dict):
        source_fields = folded(source)
        value = source_fields.get("txid")
        if isinstance(value, str):
            return value
    return None


def transaction_family(value: str) -> str | None:
    prefix, separator, suffix = value.rpartition(":")
    if separator == ":" and prefix and suffix.isdigit():
        return prefix
    return None


def fence_window(entries: Any, run_id: str) -> dict[str, dict[str, Any]]:
    require(isinstance(entries, list), "CDC XRANGE artifact is not an array")
    row_transactions: dict[str, tuple[str, str]] = {}
    end_records: list[tuple[str, str]] = []
    for entry in entries:
        document = payload(entry, "CDC")
        status = document.get("status")
        if status == "END" and isinstance(document.get("id"), str):
            end_records.append((document["id"], entry[0]))
            continue
        after = document.get("after")
        if not isinstance(after, dict):
            continue
        values = folded(after)
        if values.get("run_id") != run_id or values.get("phase") not in {
            "LOAD_FINISHED",
            "END_FENCE",
        }:
            continue
        tx_id = transaction_id(document)
        require(tx_id is not None, "run fence row lacks a transaction identity")
        phase = str(values["phase"])
        require(phase not in row_transactions, f"duplicate {phase} fence row")
        row_transactions[phase] = (tx_id, entry[0])
    require(set(row_transactions) == {"LOAD_FINISHED", "END_FENCE"}, "component fence rows are incomplete")
    result: dict[str, dict[str, Any]] = {}
    for phase, (tx_id, row_id) in row_transactions.items():
        candidates = [stream_id for identity, stream_id in end_records if identity == tx_id]
        if not candidates:
            family = transaction_family(tx_id)
            candidates = [
                stream_id
                for identity, stream_id in end_records
                if family is not None
                and transaction_family(identity) == family
                and stream_key(stream_id) > stream_key(row_id)
            ]
        require(len(candidates) == 1, f"{phase} transaction END is missing or ambiguous")
        end_id = candidates[0]
        require(stream_key(end_id) > stream_key(row_id), f"{phase} transaction END precedes its row")
        result[phase] = {
            "row_stream_id": row_id,
            "transaction_id": tx_id,
            "transaction_end_stream_id": end_id,
            "transaction_end_epoch_ms": stream_ms(end_id),
        }
    require(
        stream_key(result["LOAD_FINISHED"]["transaction_end_stream_id"])
        < stream_key(result["END_FENCE"]["transaction_end_stream_id"]),
        "component fence transaction END order is invalid",
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
    counts = {
        "entries": 0,
        "rows": 0,
        "control_rows": 0,
        "transaction_begins": 0,
        "transaction_ends": 0,
    }
    for entry in entries:
        identity = stream_key(entry[0])
        if identity <= lower_bound or identity > through:
            continue
        counts["entries"] += 1
        document = payload(entry, "CDC")
        if document.get("status") == "BEGIN":
            counts["transaction_begins"] += 1
        elif document.get("status") == "END":
            counts["transaction_ends"] += 1
        elif isinstance(document.get("source"), dict) and isinstance(document.get("op"), str):
            counts["rows"] += 1
            after_row = document.get("after")
            if isinstance(after_row, dict):
                values = folded(after_row)
                if isinstance(values.get("run_id"), str) and isinstance(values.get("phase"), str):
                    counts["control_rows"] += 1
    return counts


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
    parser.add_argument("--execution-profile", required=True, choices=("direct_benchbase", "coordinated_e2e"))
    parser.add_argument("--workload", required=True)
    parser.add_argument("--capture-profile", required=True)
    parser.add_argument("--repetition", required=True, type=int)
    parser.add_argument("--drain-limit-seconds", required=True, type=float)
    parser.add_argument("--high-water-threshold-percent", default=70.0, type=float)
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
    result: dict[str, Any] = {
        "kind": "registered_performance_cell_result",
        "backend": args.backend,
        "benchmark": args.benchmark,
        "mode": args.mode,
        "execution_profile": args.execution_profile,
        "workload": args.workload,
        "capture_profile": args.capture_profile,
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
    }
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

    if args.execution_profile == "direct_benchbase":
        marker = load(args.artifact_dir / "marker-storage.json")
        require(marker.get("row_count") == 0, "uninstrumented workload wrote marker rows")
        result.update(
            {
                "outcome": "UNINSTRUMENTED",
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
        cdc = load(args.artifact_dir / "cdc-stream.json")
        statuses = load(args.artifact_dir / "status-stream.json")
        boundaries = fence_window(cdc, run_id)
        final_id, final = final_status(statuses, run_id)
        start_id = boundaries["LOAD_FINISHED"]["transaction_end_stream_id"]
        end_id = boundaries["END_FENCE"]["transaction_end_stream_id"]
        start_ms = boundaries["LOAD_FINISHED"]["transaction_end_epoch_ms"]
        end_ms = boundaries["END_FENCE"]["transaction_end_epoch_ms"]
        final_ms = stream_ms(final_id)
        component_seconds = (end_ms - start_ms) / 1000.0
        e2e_seconds = (final_ms - start_ms) / 1000.0
        drain_seconds = (final_ms - end_ms) / 1000.0
        require(component_seconds > 0 and e2e_seconds > 0 and drain_seconds >= 0, "Redis-clock intervals are invalid")
        require(drain_seconds <= args.drain_limit_seconds, "drain exceeded its configured limit")
        window = cdc_window_counts(cdc, start_id, end_id)
        monitor = load(args.artifact_dir / "monitor-metrics.json")
        assembler = monitor.get("assembler")
        joiner = monitor.get("joiner")
        checker = monitor.get("checker")
        reconciliation = final.get("reconciliation")
        require(all(isinstance(value, dict) for value in (assembler, joiner, checker, reconciliation)), "monitor result is incomplete")
        assembled = integer(assembler.get("completed_workload_transactions"), "assembled workload transactions")
        joined = integer(joiner.get("joined_workload_transactions"), "joined workload transactions")
        checked = integer(checker.get("workload_transactions"), "checked workload transactions")
        raw_rows = integer(assembler.get("workload_raw_row_events"), "assembler raw workload rows")
        writes = integer(assembler.get("workload_normalized_write_events"), "assembler normalized writes")
        require(
            raw_rows == window["rows"] - window["control_rows"],
            "CDC and assembler raw workload rows differ",
        )
        require(joined == checked == integer(reconciliation.get("joined_workload_transactions"), "reconciled transactions"), "joined/checker totals differ")
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

    (args.artifact_dir / "result.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise SystemExit(f"registered performance result failed: {error}")
