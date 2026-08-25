#!/usr/bin/env python3
"""Capture bounded YugabyteDB source telemetry during a measured workload."""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping


METRIC_NAMES = frozenset(
    {
        "aborted_transactions_pending_cleanup",
        "cpu_stime",
        "cpu_utime",
        "current_fast_mode_rr_rc_transactions",
        "current_fast_mode_si_transactions",
        "fast_mode_rr_rc_transactions",
        "fast_mode_si_transactions",
        "handler_latency_yb_cdc_CDCService_GetChanges_count",
        "handler_latency_yb_cdc_CDCService_GetChanges_sum",
        "handler_latency_yb_cdc_CDCService_GetConsistentChanges_count",
        "handler_latency_yb_cdc_CDCService_GetConsistentChanges_sum",
        "handler_latency_yb_cdc_CDCService_GetCheckpoint_count",
        "handler_latency_yb_cdc_CDCService_GetCheckpoint_sum",
        "handler_latency_yb_cdc_CDCService_InitVirtualWALForCDC_count",
        "handler_latency_yb_cdc_CDCService_InitVirtualWALForCDC_sum",
        "handler_latency_yb_cdc_CDCService_SetCDCCheckpoint_count",
        "handler_latency_yb_cdc_CDCService_SetCDCCheckpoint_sum",
        "handler_latency_yb_cdc_CDCService_UpdateAndPersistLSN_count",
        "handler_latency_yb_cdc_CDCService_UpdateAndPersistLSN_sum",
        "handler_latency_yb_cdc_CDCService_UpdateCdcReplicatedIndex_count",
        "handler_latency_yb_cdc_CDCService_UpdateCdcReplicatedIndex_sum",
        "handler_latency_yb_tserver_TabletServerService_GetTransactionStatus_count",
        "handler_latency_yb_tserver_TabletServerService_GetTransactionStatus_sum",
        "handler_latency_yb_tserver_TabletServerService_Read_count",
        "handler_latency_yb_tserver_TabletServerService_Read_sum",
        "handler_latency_yb_tserver_TabletServerService_UpdateTransaction_count",
        "handler_latency_yb_tserver_TabletServerService_UpdateTransaction_sum",
        "handler_latency_yb_tserver_TabletServerService_Write_count",
        "handler_latency_yb_tserver_TabletServerService_Write_sum",
        "handler_latency_yb_ysqlserver_SQLProcessor_Transactions_count",
        "handler_latency_yb_ysqlserver_SQLProcessor_Transactions_sum",
        "history_cutoff_operations_inflight",
        "intentsdb_rocksdb_bytes_read",
        "intentsdb_rocksdb_bytes_written",
        "intentsdb_rocksdb_compact_read_bytes",
        "intentsdb_rocksdb_compact_write_bytes",
        "intentsdb_rocksdb_compaction_times_micros_count",
        "intentsdb_rocksdb_compaction_times_micros_sum",
        "intentsdb_rocksdb_current_version_num_sst_files",
        "intentsdb_rocksdb_current_version_sst_files_size",
        "intentsdb_rocksdb_db_iter_bytes_read",
        "intentsdb_rocksdb_db_seek_micros_count",
        "intentsdb_rocksdb_db_seek_micros_sum",
        "intentsdb_rocksdb_flush_write_bytes",
        "intentsdb_rocksdb_number_db_seek",
        "intentsdb_rocksdb_number_keys_read",
        "intentsdb_rocksdb_sst_read_micros_count",
        "intentsdb_rocksdb_sst_read_micros_sum",
        "intentsdb_rocksdb_stall_micros",
        "intentsdb_rocksdb_total_sst_files_size",
        "log_append_latency_count",
        "log_append_latency_sum",
        "log_sync_latency_count",
        "log_sync_latency_sum",
        "log_wal_size",
        "rocksdb_bytes_read",
        "rocksdb_bytes_written",
        "rocksdb_compact_read_bytes",
        "rocksdb_compact_write_bytes",
        "rocksdb_compaction_times_micros_count",
        "rocksdb_compaction_times_micros_sum",
        "rocksdb_current_version_num_sst_files",
        "rocksdb_current_version_sst_files_size",
        "rocksdb_db_iter_bytes_read",
        "rocksdb_db_seek_micros_count",
        "rocksdb_db_seek_micros_sum",
        "rocksdb_flush_write_bytes",
        "rocksdb_number_db_seek",
        "rocksdb_number_keys_read",
        "rocksdb_sst_read_micros_count",
        "rocksdb_sst_read_micros_sum",
        "rocksdb_stall_micros",
        "rocksdb_total_sst_files_size",
        "rpcs_in_queue_yb_cdc_CDCService",
        "service_response_bytes_yb_cdc_CDCService_GetChanges",
        "service_response_bytes_yb_cdc_CDCService_GetConsistentChanges",
        "slow_mode_si_transactions",
        "transaction_conflicts",
        "transaction_load_attempts",
        "transaction_not_found",
        "transactions_running",
    }
)

FLAG_NAMES = frozenset(
    {
        "cdc_checkpoint_opid_interval_ms",
        "cdc_enable_intra_transactional_before_image",
        "cdc_enable_postgres_replica_identity",
        "cdc_enable_time_based_intent_retention",
        "cdc_immediate_transaction_cleanup",
        "cdc_intent_retention_ms",
        "cdc_min_sec_to_retain_intent",
        "cdc_send_null_before_image_if_not_exists",
        "cdc_state_checkpoint_update_interval_ms",
        "cdc_wal_retention_time_secs",
        "cdcsdk_max_consistent_records",
        "cdcsdk_retention_barrier_no_revision_interval_secs",
        "cdcsdk_vwal_getchanges_resp_max_size_bytes",
        "db_write_buffer_size",
        "export_intentdb_metrics",
        "history_cutoff_propagation_interval_ms",
        "update_min_cdc_indices_interval_secs",
    }
)

PROMETHEUS_LINE = re.compile(
    r"^([A-Za-z_:][A-Za-z0-9_:]*)(?:\{([^}]*)\})?\s+([-+0-9.eE]+)(?:\s+\d+)?$"
)
LABEL = re.compile(r'(\w+)="((?:\\.|[^"\\])*)"')


class TelemetryError(RuntimeError):
    pass


def _finite_number(value: Any, context: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise TelemetryError(f"{context} is not numeric") from error
    if not math.isfinite(result):
        raise TelemetryError(f"{context} is not finite")
    return result


def parse_prometheus(text: str, allowed: Iterable[str] = METRIC_NAMES) -> list[dict[str, Any]]:
    allowed_names = frozenset(allowed)
    series: list[dict[str, Any]] = []
    for raw_line in text.splitlines():
        if not raw_line or raw_line.startswith("#"):
            continue
        match = PROMETHEUS_LINE.match(raw_line)
        if not match or match.group(1) not in allowed_names:
            continue
        labels = {
            label.group(1): bytes(label.group(2), "utf-8").decode("unicode_escape")
            for label in LABEL.finditer(match.group(2) or "")
        }
        series.append(
            {
                "name": match.group(1),
                "labels": labels,
                "value": _finite_number(match.group(3), match.group(1)),
            }
        )
    return series


def parse_json_metrics(text: str, allowed: Iterable[str] = METRIC_NAMES) -> list[dict[str, Any]]:
    allowed_names = frozenset(allowed)
    try:
        entities = json.loads(text)
    except json.JSONDecodeError as error:
        raise TelemetryError("YugabyteDB JSON metrics response is invalid") from error
    if not isinstance(entities, list):
        raise TelemetryError("YugabyteDB JSON metrics response is not an array")
    series: list[dict[str, Any]] = []
    for entity in entities:
        if not isinstance(entity, Mapping):
            continue
        labels = {
            "metric_type": str(entity.get("type", "")),
            "metric_id": str(entity.get("id", "")),
        }
        attributes = entity.get("attributes", {})
        if isinstance(attributes, Mapping):
            labels.update({str(key): str(value) for key, value in attributes.items()})
        metrics = entity.get("metrics", [])
        if not isinstance(metrics, list):
            continue
        for metric in metrics:
            if not isinstance(metric, Mapping) or not isinstance(metric.get("name"), str):
                continue
            name = str(metric["name"])
            values: list[tuple[str, Any]] = []
            if name in allowed_names and "value" in metric:
                values.append((name, metric["value"]))
            for suffix, field in (("_count", "total_count"), ("_sum", "total_sum")):
                derived_name = name + suffix
                if derived_name in allowed_names and field in metric:
                    values.append((derived_name, metric[field]))
            for selected_name, value in values:
                series.append(
                    {
                        "name": selected_name,
                        "labels": labels,
                        "value": _finite_number(value, selected_name),
                    }
                )
    return series


def aggregate_series(series: Iterable[Mapping[str, Any]]) -> dict[str, float]:
    totals: defaultdict[str, float] = defaultdict(float)
    for item in series:
        totals[str(item["name"])] += _finite_number(item["value"], str(item["name"]))
    return dict(sorted(totals.items()))


def parse_varz(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        if not line.startswith("--") or "=" not in line:
            continue
        name, value = line[2:].split("=", 1)
        if name in FLAG_NAMES:
            values[name] = value
    return dict(sorted(values.items()))


def summarize_samples(samples: list[Mapping[str, Any]]) -> dict[str, Any]:
    names = sorted(
        {
            name
            for sample in samples
            for name in sample.get("aggregates", {})
        }
    )
    metrics: dict[str, Any] = {}
    for name in names:
        values = [
            _finite_number(sample["aggregates"][name], name)
            for sample in samples
            if name in sample.get("aggregates", {})
        ]
        metrics[name] = {
            "first": values[0],
            "last": values[-1],
            "delta": values[-1] - values[0],
            "minimum": min(values),
            "maximum": max(values),
            "sample_count": len(values),
        }

    by_series: defaultdict[tuple[str, str], list[float]] = defaultdict(list)
    labels_by_key: dict[tuple[str, str], dict[str, str]] = {}
    for sample in samples:
        for item in sample.get("series", []):
            labels = {str(key): str(value) for key, value in item.get("labels", {}).items()}
            identity = json.dumps(labels, sort_keys=True, separators=(",", ":"))
            key = (str(item["name"]), identity)
            labels_by_key[key] = labels
            by_series[key].append(_finite_number(item["value"], str(item["name"])))
    series_summary = [
        {
            "name": name,
            "labels": labels_by_key[(name, identity)],
            "first": values[0],
            "last": values[-1],
            "delta": values[-1] - values[0],
            "minimum": min(values),
            "maximum": max(values),
            "sample_count": len(values),
        }
        for (name, identity), values in sorted(by_series.items())
    ]
    return {"metrics": metrics, "series": series_summary}


def _run(command: list[str]) -> str:
    completed = subprocess.run(command, check=True, text=True, capture_output=True)
    return completed.stdout


def _fetch(container: str, url: str) -> str:
    return _run(
        [
            "docker",
            "exec",
            container,
            "curl",
            "--silent",
            "--show-error",
            "--fail",
            url,
        ]
    )


def _ysql_json(container: str, query: str) -> Any:
    text = _run(
        [
            "docker",
            "exec",
            container,
            "bin/ysqlsh",
            "--host",
            "yugabytedb_ysql",
            "--username",
            "yugabyte",
            "--dbname",
            "benchbase",
            "--no-psqlrc",
            "--set",
            "ON_ERROR_STOP=on",
            "--tuples-only",
            "--no-align",
            "--command",
            query,
        ]
    ).strip()
    return json.loads(text or "null")


def _slot_state(container: str) -> list[dict[str, Any]]:
    return _ysql_json(
        container,
        "SELECT COALESCE(json_agg(row_to_json(s)), '[]'::json) FROM ("
        "SELECT slot_name, plugin, database, yb_lsn_type, active, active_pid, "
        "restart_lsn::text, confirmed_flush_lsn::text FROM pg_replication_slots ORDER BY slot_name"
        ") AS s",
    )


def _topology(container: str) -> list[dict[str, Any]]:
    return _ysql_json(
        container,
        "SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json) FROM ("
        "SELECT tablet_id, table_id, table_type, namespace_name, ysql_schema_name, table_name, "
        "encode(partition_key_start, 'hex') AS partition_key_start_hex, "
        "encode(partition_key_end, 'hex') AS partition_key_end_hex, state "
        "FROM yb_local_tablets ORDER BY namespace_name, ysql_schema_name, table_name, tablet_id"
        ") AS t",
    )


def _publication(container: str) -> list[dict[str, Any]]:
    return _ysql_json(
        container,
        "SELECT COALESCE(json_agg(row_to_json(p)), '[]'::json) FROM ("
        "SELECT pubname, schemaname, tablename FROM pg_publication_tables ORDER BY 1,2,3"
        ") AS p",
    )


def capture(args: argparse.Namespace) -> dict[str, Any]:
    start_monotonic = time.monotonic()
    deadline = start_monotonic + args.duration_seconds
    stop_file = Path(args.stop_file)
    tserver_prometheus = "http://yugabytedb_ysql:9000/prometheus-metrics"
    tserver_json = "http://yugabytedb_ysql:9000/metrics"
    ysql_prometheus = "http://yugabytedb_ysql:13000/prometheus-metrics"
    tserver_varz = "http://yugabytedb_ysql:9000/varz?raw=1"
    samples: list[dict[str, Any]] = []
    sample_index = 0
    next_sample = start_monotonic
    initial_topology = _topology(args.database_container)
    initial_publication = _publication(args.database_container)
    initial_flags = parse_varz(_fetch(args.database_container, tserver_varz))

    while time.monotonic() < deadline and not stop_file.exists():
        started = time.monotonic()
        try:
            try:
                tserver_series = parse_json_metrics(
                    _fetch(args.database_container, tserver_json)
                )
            except TelemetryError:
                tserver_series = parse_prometheus(
                    _fetch(args.database_container, tserver_prometheus)
                )
            ysql_series = parse_prometheus(
                _fetch(args.database_container, ysql_prometheus)
            )
            series = tserver_series + ysql_series
            slot = _slot_state(args.database_container)
            capture_duration_ms = round((time.monotonic() - started) * 1000, 3)
            samples.append(
                {
                    "sample": sample_index,
                    "epoch_ms": time.time_ns() // 1_000_000,
                    "capture_duration_ms": capture_duration_ms,
                    "slot": slot,
                    "aggregates": aggregate_series(series),
                    "series": series,
                }
            )
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError, TelemetryError) as error:
            raise TelemetryError(f"source telemetry sample {sample_index} failed: {error}") from error
        sample_index += 1
        next_sample += args.interval_seconds
        remaining = next_sample - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)

    if not samples:
        raise TelemetryError("source telemetry captured no samples")
    final_topology = _topology(args.database_container)
    final_publication = _publication(args.database_container)
    final_flags = parse_varz(_fetch(args.database_container, tserver_varz))
    summary = summarize_samples(samples)
    return {
        "kind": "backend_performance_telemetry",
        "backend": "yugabytedb_ysql",
        "interval_definition": "measured_workload_process_window",
        "requested_interval_seconds": args.interval_seconds,
        "requested_duration_seconds": args.duration_seconds,
        "sample_count": len(samples),
        "start_epoch_ms": samples[0]["epoch_ms"],
        "end_epoch_ms": samples[-1]["epoch_ms"],
        "initial_flags": initial_flags,
        "final_flags": final_flags,
        "initial_publication": initial_publication,
        "final_publication": final_publication,
        "initial_tablet_topology": initial_topology,
        "final_tablet_topology": final_topology,
        "summary": summary,
        "samples": samples,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--stop-file", required=True)
    parser.add_argument("--interval-seconds", type=float, required=True)
    parser.add_argument("--duration-seconds", type=float, required=True)
    parser.add_argument("--database-container", default="cdc-yugabytedb-ysql")
    args = parser.parse_args()
    if args.interval_seconds <= 0 or args.duration_seconds <= 0:
        parser.error("interval and duration must be positive")
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        document = capture(args)
    except TelemetryError as error:
        parser.error(str(error))
    output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
