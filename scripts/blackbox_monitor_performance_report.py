#!/usr/bin/env python3
"""Validate saturated monitor trials and write concise performance reports."""

from __future__ import annotations

import argparse
import csv
import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence


STAGES = ("assembly_join", "assembly_join_check")
MODE_LABELS = {
    "assembly_join": "Assembly/join",
    "assembly_join_check": "Full checking",
}

METRICS = (
    "workload_transactions",
    "monitor_wall_seconds",
    "throughput_transactions_per_second",
    "process_cpu_seconds",
    "transactions_per_cpu_second",
    "max_rss_mib",
)

JOIN_ONLY_CHECKER_COUNTERS = (
    "bootstrap_transactions",
    "workload_transactions",
    "phase_anchor_transactions",
    "read_events",
    "write_events",
    "consumed_transactions",
    "mapped_identity_records",
    "mapped_identity_bytes",
    "next_commit_seq",
    "consume_total_nanoseconds",
    "consume_max_nanoseconds",
)

RAW_FIELDS = (
    "stage",
    "mode",
    "repetition",
    "stage_order_position",
    "trial_id",
    "run_id",
    "artifact_dir",
    "configured_repetitions",
    "configured_stages_json",
    "random_seed",
    "source_trial_dir",
    "source_requested_duration_seconds",
    "source_trial_manifest_sha256",
    "source_cdc_sha256",
    "source_client_sha256",
    "source_cdc_entries",
    "source_client_entries",
    "source_workload_transactions",
    "redis_maxmemory_bytes",
    "monitor_max_pending_client_bytes",
    "monitor_max_retained_identity_bytes",
    "si_monitoring_head",
    "co_checker_head",
    "si_monitoring_dirty",
    "co_checker_dirty",
    "workload_transactions",
    "monitor_wall_time_nanoseconds",
    "monitor_wall_seconds",
    "throughput_transactions_per_second",
    "process_user_cpu_nanoseconds",
    "process_system_cpu_nanoseconds",
    "process_cpu_seconds",
    "transactions_per_cpu_second",
    "process_max_rss_kib",
    "max_rss_mib",
    "completion_status",
    "evidence_reconciled",
    "audited_outcome",
)

SUMMARY_FIELDS = (
    "stage",
    "mode",
    "metric",
    "available_trials",
    "total_trials",
    "median",
    "min",
    "max",
    "mad",
)

HEADLINE_FIELDS = (
    "mode",
    "workload_transactions",
    "monitor_wall_seconds",
    "throughput_transactions_per_second",
    "process_cpu_seconds",
    "transactions_per_cpu_second",
    "max_rss_mib",
    "throughput_loss_vs_join_only_percent",
)

METADATA_FIELDS = (
    "kind",
    "stage",
    "repetition",
    "stage_order_position",
    "configured_repetitions",
    "configured_stages",
    "random_seed",
    "trial_id",
    "run_id",
    "source_trial_dir",
    "source_requested_duration_seconds",
    "source_trial_manifest_sha256",
    "source_cdc_sha256",
    "source_client_sha256",
    "source_cdc_entries",
    "source_client_entries",
    "source_workload_transactions",
    "redis_maxmemory_bytes",
    "monitor_max_pending_client_bytes",
    "monitor_max_retained_identity_bytes",
    "si_monitoring_head",
    "co_checker_head",
    "si_monitoring_dirty",
    "co_checker_dirty",
)

INVARIANT_FIELDS = (
    "configured_repetitions",
    "configured_stages",
    "random_seed",
    "run_id",
    "source_trial_dir",
    "source_requested_duration_seconds",
    "source_trial_manifest_sha256",
    "source_cdc_sha256",
    "source_client_sha256",
    "source_cdc_entries",
    "source_client_entries",
    "source_workload_transactions",
    "redis_maxmemory_bytes",
    "monitor_max_pending_client_bytes",
    "monitor_max_retained_identity_bytes",
    "si_monitoring_head",
    "co_checker_head",
    "si_monitoring_dirty",
    "co_checker_dirty",
)


class MonitorReportError(ValueError):
    """A monitor performance artifact set is invalid or incomplete."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise MonitorReportError(message)


def load_object(path: Path) -> dict[str, Any]:
    require(path.is_file(), f"required artifact is missing: {path}")
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise MonitorReportError(f"cannot read valid JSON from {path}: {error}") from error
    require(isinstance(document, dict), f"JSON artifact is not an object: {path}")
    return document


def object_field(document: Mapping[str, Any], field: str, label: str) -> Mapping[str, Any]:
    value = document.get(field)
    require(isinstance(value, dict), f"{label}.{field} is not an object")
    return value


def integer(value: Any, label: str, *, minimum: int = 0) -> int:
    require(not isinstance(value, bool) and isinstance(value, int), f"{label} is not an integer")
    require(value >= minimum, f"{label} is less than {minimum}")
    return value


def text_value(value: Any, label: str) -> str:
    require(isinstance(value, str) and value != "", f"{label} is not nonempty text")
    return value


def digest(value: Any, label: str, length: int) -> str:
    result = text_value(value, label)
    require(
        re.fullmatch(rf"[0-9a-f]{{{length}}}", result) is not None,
        f"{label} is not a canonical lowercase hexadecimal digest",
    )
    return result


def decimal_text(value: Decimal) -> str:
    result = format(value, "f")
    if "." in result:
        result = result.rstrip("0").rstrip(".")
    return result or "0"


def json_number(value: Decimal) -> int | float:
    return int(value) if value == value.to_integral_value() else float(value)


def median(values: Sequence[Decimal]) -> Decimal:
    require(bool(values), "cannot calculate a median without observations")
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def configured_stages(metadata: Mapping[str, Any], label: str) -> tuple[str, ...]:
    value = metadata.get("configured_stages")
    require(isinstance(value, list) and value, f"{label}.configured_stages is not a nonempty array")
    require(
        all(isinstance(stage, str) and stage in STAGES for stage in value),
        f"{label}.configured_stages contains an unknown monitor stage",
    )
    roster = tuple(value)
    require(len(roster) == len(set(roster)), f"{label}.configured_stages contains duplicates")
    require(
        roster == tuple(stage for stage in STAGES if stage in roster),
        f"{label}.configured_stages is not in canonical order",
    )
    return roster


def validate_metadata(metadata: Mapping[str, Any], trial_dir: Path) -> dict[str, Any]:
    label = str(trial_dir / "metadata.json")
    missing = [field for field in METADATA_FIELDS if field not in metadata]
    require(not missing, f"metadata is missing required fields: {', '.join(missing)}")
    require(metadata["kind"] == "monitor_performance_trial", f"{label}.kind is invalid")
    stage = metadata["stage"]
    require(stage in STAGES, f"{label}.stage is not a monitor performance stage")
    roster = configured_stages(metadata, label)
    require(stage in roster, f"{label}.stage is absent from configured_stages")

    result = dict(metadata)
    result["configured_stages"] = roster
    result["repetition"] = integer(metadata["repetition"], f"{label}.repetition", minimum=1)
    result["stage_order_position"] = integer(
        metadata["stage_order_position"], f"{label}.stage_order_position", minimum=1
    )
    result["configured_repetitions"] = integer(
        metadata["configured_repetitions"], f"{label}.configured_repetitions", minimum=1
    )
    result["random_seed"] = integer(metadata["random_seed"], f"{label}.random_seed")
    for field in ("trial_id", "run_id", "source_trial_dir"):
        result[field] = text_value(metadata[field], f"{label}.{field}")
    result["source_requested_duration_seconds"] = integer(
        metadata["source_requested_duration_seconds"],
        f"{label}.source_requested_duration_seconds",
        minimum=1,
    )
    for field in (
        "source_cdc_entries",
        "source_client_entries",
        "source_workload_transactions",
        "redis_maxmemory_bytes",
        "monitor_max_pending_client_bytes",
        "monitor_max_retained_identity_bytes",
    ):
        result[field] = integer(metadata[field], f"{label}.{field}", minimum=1)
    for field in (
        "source_trial_manifest_sha256",
        "source_cdc_sha256",
        "source_client_sha256",
    ):
        result[field] = digest(metadata[field], f"{label}.{field}", 64)
    for field in ("si_monitoring_head", "co_checker_head"):
        result[field] = digest(metadata[field], f"{label}.{field}", 40)
    for field in ("si_monitoring_dirty", "co_checker_dirty"):
        require(isinstance(metadata[field], bool), f"{label}.{field} is not boolean")
        require(metadata[field] is False, f"measured monitor trial has a dirty worktree: {field}")
    return result


def metric_integer(document: Mapping[str, Any], field: str, label: str, *, minimum: int = 0) -> int:
    require(field in document, f"{label}.{field} is missing")
    return integer(document[field], f"{label}.{field}", minimum=minimum)


def trial_row(trial_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    metadata = validate_metadata(load_object(trial_dir / "metadata.json"), trial_dir)
    monitor = load_object(trial_dir / "monitor_metrics.json")
    label = str(trial_dir / "monitor_metrics.json")
    stage = metadata["stage"]
    expected_mode = "join-only" if stage == "assembly_join" else "audited"

    require(monitor.get("kind") == "performance_measurement", f"{label}.kind is invalid")
    require(monitor.get("run_id") == metadata["run_id"], f"{label}.run_id differs from metadata")
    require(monitor.get("processing_mode") == expected_mode, f"{label}.processing_mode is invalid")
    require(monitor.get("completed") is True, f"{label} did not complete")
    require(monitor.get("completion_status") == "completed", f"{label} has a failed completion status")
    require(monitor.get("evidence_reconciled") is True, f"{label} did not reconcile evidence")
    require(monitor.get("diagnostics") == [], f"{label} contains diagnostics")

    reconciliation = object_field(monitor, "reconciliation", label)
    require(
        reconciliation.get("evidence_reconciled") is True,
        f"{label}.reconciliation did not reconcile evidence",
    )
    source_transactions = metadata["source_workload_transactions"]
    reconciled_transactions = metric_integer(
        reconciliation, "joined_workload_transactions", f"{label}.reconciliation"
    )
    require(
        reconciled_transactions == source_transactions,
        f"{label} reconciliation workload count differs from source_workload_transactions",
    )

    limits = object_field(monitor, "limits", label)
    joiner_limits = object_field(limits, "joiner", f"{label}.limits")
    for metric_field, metadata_field in (
        ("max_pending_client_bytes", "monitor_max_pending_client_bytes"),
        ("max_retained_identity_bytes", "monitor_max_retained_identity_bytes"),
    ):
        observed = metric_integer(joiner_limits, metric_field, f"{label}.limits.joiner", minimum=1)
        require(
            observed == metadata[metadata_field],
            f"{label} {metric_field} differs from trial metadata {metadata_field}",
        )

    joiner = object_field(monitor, "joiner", label)
    joined = metric_integer(joiner, "joined_workload_transactions", f"{label}.joiner")
    require(
        joined == source_transactions,
        f"{label} joined workload count differs from source_workload_transactions",
    )

    checker = object_field(monitor, "checker", label)
    checker_workload = metric_integer(checker, "workload_transactions", f"{label}.checker")
    checker_consumed = metric_integer(checker, "consumed_transactions", f"{label}.checker")
    if stage == "assembly_join":
        require("outcome" not in monitor and "verdict" not in monitor, f"{label} join-only metrics contain an outcome")
        checker_activity = {
            field: metric_integer(checker, field, f"{label}.checker")
            for field in JOIN_ONLY_CHECKER_COUNTERS
        }
        require(
            not any(checker_activity.values()),
            f"{label} join-only mode invoked the checker",
        )
        workload_transactions = joined
        audited_outcome = ""
    else:
        require(monitor.get("outcome") == "PASS", f"{label} audited outcome is not PASS")
        require(
            checker_workload == joined,
            f"{label} checker workload count differs from joined transactions",
        )
        require(
            checker_consumed >= checker_workload,
            f"{label} checker consumed fewer than its workload transactions",
        )
        workload_transactions = checker_workload
        audited_outcome = "PASS"

    wall_ns = metric_integer(monitor, "monitor_wall_time_nanoseconds", label, minimum=1)
    process = object_field(monitor, "process", label)
    user_ns = metric_integer(process, "user_cpu_nanoseconds", f"{label}.process")
    system_ns = metric_integer(process, "system_cpu_nanoseconds", f"{label}.process")
    cpu_ns = user_ns + system_ns
    require(cpu_ns > 0, f"{label} process CPU time is zero")
    max_rss_kib = metric_integer(process, "max_rss_kib", f"{label}.process", minimum=1)

    billion = Decimal(1_000_000_000)
    wall_seconds = Decimal(wall_ns) / billion
    cpu_seconds = Decimal(cpu_ns) / billion
    throughput = Decimal(workload_transactions) / wall_seconds
    transactions_per_cpu_second = Decimal(workload_transactions) / cpu_seconds
    max_rss_mib = Decimal(max_rss_kib) / Decimal(1024)

    row = {
        "stage": stage,
        "mode": MODE_LABELS[stage],
        "repetition": metadata["repetition"],
        "stage_order_position": metadata["stage_order_position"],
        "trial_id": metadata["trial_id"],
        "run_id": metadata["run_id"],
        "artifact_dir": str(trial_dir.resolve()),
        "configured_repetitions": metadata["configured_repetitions"],
        "configured_stages_json": json.dumps(
            metadata["configured_stages"], separators=(",", ":")
        ),
        "random_seed": metadata["random_seed"],
        **{field: metadata[field] for field in RAW_FIELDS if field.startswith("source_")},
        "redis_maxmemory_bytes": metadata["redis_maxmemory_bytes"],
        "monitor_max_pending_client_bytes": metadata[
            "monitor_max_pending_client_bytes"
        ],
        "monitor_max_retained_identity_bytes": metadata[
            "monitor_max_retained_identity_bytes"
        ],
        "si_monitoring_head": metadata["si_monitoring_head"],
        "co_checker_head": metadata["co_checker_head"],
        "si_monitoring_dirty": metadata["si_monitoring_dirty"],
        "co_checker_dirty": metadata["co_checker_dirty"],
        "workload_transactions": workload_transactions,
        "monitor_wall_time_nanoseconds": wall_ns,
        "monitor_wall_seconds": wall_seconds,
        "throughput_transactions_per_second": throughput,
        "process_user_cpu_nanoseconds": user_ns,
        "process_system_cpu_nanoseconds": system_ns,
        "process_cpu_seconds": cpu_seconds,
        "transactions_per_cpu_second": transactions_per_cpu_second,
        "process_max_rss_kib": max_rss_kib,
        "max_rss_mib": max_rss_mib,
        "completion_status": "completed",
        "evidence_reconciled": True,
        "audited_outcome": audited_outcome,
    }
    return row, metadata


def find_trial_dirs(root: Path) -> list[Path]:
    trial_root = root / "trials"
    require(trial_root.is_dir(), f"monitor trial directory is missing: {trial_root}")
    result = sorted(path.parent for path in trial_root.rglob("metadata.json"))
    require(bool(result), f"no monitor trials found under {trial_root}")
    return result


def collect_trials(root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    pairs = [trial_row(path) for path in find_trial_dirs(root)]
    rows = [pair[0] for pair in pairs]
    metadata_rows = [pair[1] for pair in pairs]

    for field in INVARIANT_FIELDS:
        values = {json.dumps(metadata[field], sort_keys=True) for metadata in metadata_rows}
        require(len(values) == 1, f"monitor trial invariant differs across runs: {field}")

    roster = metadata_rows[0]["configured_stages"]
    repetitions = metadata_rows[0]["configured_repetitions"]
    random_seed = metadata_rows[0]["random_seed"]
    require(
        {metadata["stage"] for metadata in metadata_rows} == set(roster),
        "monitor trial stages differ from the configured roster",
    )
    require(
        len(metadata_rows) == repetitions * len(roster),
        "monitor trial count differs from the configured roster",
    )
    require(
        {metadata["repetition"] for metadata in metadata_rows} == set(range(1, repetitions + 1)),
        "monitor trial repetitions differ from the configured roster",
    )
    trial_ids = [metadata["trial_id"] for metadata in metadata_rows]
    require(len(trial_ids) == len(set(trial_ids)), "monitor trial IDs are not unique")

    for repetition in range(1, repetitions + 1):
        repetition_rows = [metadata for metadata in metadata_rows if metadata["repetition"] == repetition]
        positions = {metadata["stage_order_position"] for metadata in repetition_rows}
        require(
            len(repetition_rows) == len(roster) and positions == set(range(1, len(roster) + 1)),
            f"repetition {repetition} stage-order positions are incomplete or duplicated",
        )
        offset = (random_seed + repetition - 1) % len(roster)
        for metadata in repetition_rows:
            position = metadata["stage_order_position"]
            expected_stage = roster[(offset + position - 1) % len(roster)]
            require(
                metadata["stage"] == expected_stage,
                f"repetition {repetition} monitor stage-order rotation differs at position {position}",
            )

    rows.sort(key=lambda row: (int(row["repetition"]), int(row["stage_order_position"])))
    return rows, metadata_rows


def summary_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    available_stages = {str(row["stage"]) for row in rows}
    for stage in STAGES:
        if stage not in available_stages:
            continue
        stage_rows = [row for row in rows if row["stage"] == stage]
        for metric in METRICS:
            values = [Decimal(str(row[metric])) for row in stage_rows]
            center = median(values)
            result.append(
                {
                    "stage": stage,
                    "mode": MODE_LABELS[stage],
                    "metric": metric,
                    "available_trials": len(values),
                    "total_trials": len(stage_rows),
                    "median": center,
                    "min": min(values),
                    "max": max(values),
                    "mad": median([abs(value - center) for value in values]),
                }
            )
    return result


def headline_rows(summaries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_stage_metric = {
        (str(row["stage"]), str(row["metric"])): Decimal(str(row["median"]))
        for row in summaries
    }
    stages = [stage for stage in STAGES if (stage, METRICS[0]) in by_stage_metric]
    loss: Decimal | None = None
    if set(STAGES).issubset(stages):
        join_throughput = by_stage_metric[("assembly_join", "throughput_transactions_per_second")]
        full_throughput = by_stage_metric[("assembly_join_check", "throughput_transactions_per_second")]
        require(join_throughput > 0, "join-only median throughput is zero")
        loss = (join_throughput - full_throughput) / join_throughput * Decimal(100)

    result: list[dict[str, Any]] = []
    for stage in stages:
        row: dict[str, Any] = {"mode": MODE_LABELS[stage]}
        for metric in METRICS:
            row[metric] = by_stage_metric[(stage, metric)]
        row["throughput_loss_vs_join_only_percent"] = (
            loss if stage == "assembly_join_check" and loss is not None else ""
        )
        result.append(row)
    return result


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    field: decimal_text(value) if isinstance(value, Decimal) else value
                    for field, value in row.items()
                }
            )


def write_summary_json(
    path: Path,
    summaries: Sequence[Mapping[str, Any]],
    metadata: Mapping[str, Any],
) -> None:
    source = {
        field: metadata[field]
        for field in (
            "source_trial_dir",
            "source_requested_duration_seconds",
            "source_trial_manifest_sha256",
            "source_cdc_sha256",
            "source_client_sha256",
            "source_cdc_entries",
            "source_client_entries",
            "source_workload_transactions",
        )
    }
    serializable = [
        {
            key: json_number(value) if isinstance(value, Decimal) else value
            for key, value in row.items()
        }
        for row in summaries
    ]
    document = {
        "kind": "monitor_performance_summary",
        "configured_stages": list(metadata["configured_stages"]),
        "configured_repetitions": metadata["configured_repetitions"],
        "random_seed": metadata["random_seed"],
        "source_corpus": source,
        "provenance": {
            "redis_maxmemory_bytes": metadata["redis_maxmemory_bytes"],
            "monitor_max_pending_client_bytes": metadata[
                "monitor_max_pending_client_bytes"
            ],
            "monitor_max_retained_identity_bytes": metadata[
                "monitor_max_retained_identity_bytes"
            ],
            "si_monitoring_head": metadata["si_monitoring_head"],
            "co_checker_head": metadata["co_checker_head"],
            "si_monitoring_dirty": metadata["si_monitoring_dirty"],
            "co_checker_dirty": metadata["co_checker_dirty"],
        },
        "summary": serializable,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _format_decimal(value: Decimal, places: int, *, grouped: bool = False) -> str:
    spec = f",.{places}f" if grouped else f".{places}f"
    return format(value, spec)


def write_headline_markdown(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    lines = [
        "| Mode | Workload txns | Monitor wall seconds | Throughput (tx/s) | Process CPU seconds | Tx/CPU-sec | Max RSS (MiB) | Full vs join throughput loss |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        loss = row["throughput_loss_vs_join_only_percent"]
        loss_text = "—" if loss == "" else f"{_format_decimal(loss, 2)}%"
        lines.append(
            "| "
            + " | ".join(
                (
                    str(row["mode"]),
                    format(int(Decimal(str(row["workload_transactions"]))), ",d"),
                    _format_decimal(Decimal(str(row["monitor_wall_seconds"])), 3),
                    _format_decimal(Decimal(str(row["throughput_transactions_per_second"])), 1, grouped=True),
                    _format_decimal(Decimal(str(row["process_cpu_seconds"])), 3),
                    _format_decimal(Decimal(str(row["transactions_per_cpu_second"])), 1, grouped=True),
                    _format_decimal(Decimal(str(row["max_rss_mib"])), 1, grouped=True),
                    loss_text,
                )
            )
            + " |"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_reports(root: Path) -> None:
    rows, metadata_rows = collect_trials(root)
    summaries = summary_rows(rows)
    headlines = headline_rows(summaries)
    write_csv(root / "raw_monitor_trials.csv", rows, RAW_FIELDS)
    write_csv(root / "monitor_summary_statistics.csv", summaries, SUMMARY_FIELDS)
    write_summary_json(root / "monitor_summary_statistics.json", summaries, metadata_rows[0])
    write_csv(root / "monitor_headline_results.csv", headlines, HEADLINE_FIELDS)
    write_headline_markdown(root / "monitor_headline_results.md", headlines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        required=True,
        help="monitor measurement root containing trials/; reports are written into this root",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        write_reports(Path(args.root))
    except MonitorReportError as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
