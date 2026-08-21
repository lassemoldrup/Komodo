#!/usr/bin/env python3

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "scripts" / "blackbox_performance_report.py"
SPEC = importlib.util.spec_from_file_location("blackbox_performance_report", TOOL)
assert SPEC is not None and SPEC.loader is not None
report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(report)


def xrange_entry(stream_id: str, payload: dict[str, object]) -> list[object]:
    return [stream_id, ["payload", json.dumps(payload, separators=(",", ":"))]]


def cdc_fences(run_id: str) -> list[object]:
    return [
        xrange_entry("1000-0", {"after": {"run_id": run_id, "phase": "RUN_START"}}),
        xrange_entry(
            "2000-0", {"after": {"run_id": run_id, "phase": "LOAD_FINISHED"}}
        ),
        xrange_entry("3000-0", {"after": {"run_id": run_id, "phase": "END_FENCE"}}),
    ]


def status_records(run_id: str, audited: bool) -> list[object]:
    records = [
        xrange_entry(
            "1010-0",
            {
                "kind": "status",
                "run_id": run_id,
                "lifecycle_state": "START_FENCE_SEEN",
                "outcome": "RUNNING",
                "checker_observation": "NOT_STARTED",
            },
        ),
        xrange_entry(
            "2020-0",
            {
                "kind": "status",
                "run_id": run_id,
                "lifecycle_state": "LOAD_FINISHED_SEEN",
                "outcome": "RUNNING",
                "checker_observation": "NOT_STARTED",
            },
        ),
    ]
    if audited:
        records.append(
            xrange_entry(
                "3030-0",
                {
                    "kind": "status",
                    "run_id": run_id,
                    "lifecycle_state": "FINAL",
                    "outcome": "PASS",
                    "checker_observation": "PASS_SO_FAR",
                    "reconciliation": {"evidence_reconciled": True},
                },
            )
        )
    return records


class ReportTest(unittest.TestCase):
    def write_trial(
        self,
        root: Path,
        stage: str,
        position: int,
        configured_stages: tuple[str, ...] | None = None,
    ) -> None:
        trial = root / "trials" / "repetition-01" / f"{position:02d}-{stage}"
        trial.mkdir(parents=True)
        run_id = f"00000000-0000-0000-0000-{position:012d}"
        metadata = {
            "stage": stage,
            "repetition": 1,
            "stage_order_position": position,
            "trial_id": f"trial-{position}",
            "run_id": run_id,
            "terminals": 1,
            "scale_factor": 1,
            "field_size": 32,
            "loader_threads": 1,
            "loader_batch_size": 128,
            "loader_timestamp_utc": "2000-01-01T00:00:00Z",
            "warmup_seconds": 1,
            "requested_duration_seconds": 2,
            "random_seed": 20260818,
            "configured_repetitions": 1,
            "redis_maxmemory_bytes": 8589934592,
            "monitor_max_pending_client_bytes": 1073741824,
            "monitor_max_retained_identity_bytes": 2147483648,
            "monitor_max_bootstrap_state_rows": 10000000,
            "monitor_max_bootstrap_state_bytes": 268435456,
            "output_max_bytes": 107374182400,
            "isolation": "REPEATABLE_READ",
            "rate": "unlimited",
            "java_flags": "-Xms1g -Xmx1g",
            "si_monitoring_head": "s" * 40,
            "benchbase_current_head": "b" * 40,
            "benchbase_baseline_head": "2" * 40,
            "co_checker_head": "c" * 40,
            "smoke": True,
        }
        if configured_stages is not None:
            metadata["configured_stages"] = list(configured_stages)
        benchbase = {
            "measured_count": 100,
            "elapsed_time_nanoseconds": 2_000_000_000,
            "throughput_requests_per_second": 1000 - position * 10,
            "goodput_requests_per_second": 1000 - position * 10,
            "final_state": "EXIT",
            "latency_microseconds": {
                "mean": 100 + position,
                "p50": 90 + position,
                "p95": 150 + position,
                "p99": 200 + position,
                "max": 300 + position,
            },
            "measured_start_epoch_ms": 10_000,
            "measured_end_epoch_ms": 12_000,
            "transaction_counts": {"ReadRecord": 50, "UpdateRecord": 50},
        }
        bootstrap = {
            "kind": "bootstrap_manifest",
            "run_id": run_id,
            "table_row_counts": [{"table": "benchbase.usertable", "row_count": 1000}],
            "canonical_state_digest": "a" * 64,
        }
        loader_validation = {
            "kind": "loader_validation",
            "benchmark": "ycsb",
            "declaration_available": stage not in {"benchbase_uninstrumented", "cdc_capture"},
            "loader_batch_size": 128,
            "scale_parameter": None,
            "declared_table_row_counts": (
                [{"table": "benchbase.usertable", "row_count": 1000}]
                if stage not in {"benchbase_uninstrumented", "cdc_capture"}
                else None
            ),
            "expected_table_row_counts": [
                {"table": "benchbase.usertable", "row_count": 1000}
            ],
            "observed_table_row_counts": [
                {"table": "benchbase.usertable", "row_count": 1000}
            ],
            "derived_cardinality_checks": [],
            "relationship_checks": [],
            "validated": True,
        }
        (trial / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
        (trial / "benchbase_metrics.json").write_text(
            json.dumps(benchbase), encoding="utf-8"
        )
        (trial / "bootstrap_manifest.json").write_text(
            json.dumps(bootstrap), encoding="utf-8"
        )
        (trial / "loader_validation.json").write_text(
            json.dumps(loader_validation), encoding="utf-8"
        )
        if stage in {"read_capture_encoding", "read_capture_encoding_redis"}:
            (trial / "capture_metrics.json").write_text(
                json.dumps({"worker_summary_count": 1}), encoding="utf-8"
            )
        if stage in {
            "read_capture_encoding_redis",
            "cdc_capture",
            "assembly_join",
            "assembly_join_check",
        }:
            (trial / "redis_configuration.json").write_text(
                json.dumps(
                    {
                        "requested_maxmemory_bytes": 8589934592,
                        "effective_maxmemory_bytes": 8589934592,
                        "maxmemory_policy": "noeviction",
                    }
                ),
                encoding="utf-8",
            )
        if stage in {"assembly_join", "assembly_join_check"}:
            audited = stage == "assembly_join_check"
            (trial / "cdc_stream.json").write_text(
                json.dumps(cdc_fences(run_id)), encoding="utf-8"
            )
            (trial / "status_stream.json").write_text(
                json.dumps(status_records(run_id, audited)), encoding="utf-8"
            )
            monitor = {
                "processing_mode": "audited" if audited else "join-only",
                "completed": True,
                "completion_status": "complete",
                "evidence_reconciled": True,
                "finished_at_utc": "1970-01-01T00:00:03.040Z",
                "limits": {
                    "joiner": {
                        "max_pending_client_bytes": 1073741824,
                        "max_retained_identity_bytes": 2147483648,
                        "max_bootstrap_state_rows": 10000000,
                        "max_bootstrap_state_bytes": 268435456,
                    }
                },
                "joiner": {
                    "missing_client_head_wait_current_nanoseconds": 2_000_000,
                    "missing_client_head_wait_max_nanoseconds": 5_000_000,
                    "join_buffer_high_water_bytes": 4096,
                    "joined_workload_transactions": 100,
                    "joined_read_events": 50,
                    "joined_write_events": 50,
                    "unmatched_client_records": 0,
                    "unmatched_workload_cdc_slots": 0,
                    "released_bootstrap_write_events": 1000,
                    "released_ordered_events": 1100,
                },
                "checker": {
                    "consumed_transactions": 100 if audited else 0,
                    "consume_total_nanoseconds": 1_000_000_000 if audited else 0,
                    "engine": {"retained_state_bytes": 8192, "peak_retained_state_bytes": 9000},
                },
            }
            (trial / "monitor_metrics.json").write_text(
                json.dumps(monitor), encoding="utf-8"
            )
        report.write_checksums(trial, trial / "SHA256SUMS")

    def write_tpcc_trial(self, root: Path) -> Path:
        stage = "assembly_join_check"
        self.write_trial(root, stage, 1, (stage,))
        trial = root / "trials" / "repetition-01" / f"01-{stage}"
        metadata_path = trial / "metadata.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata.update(
            {
                "benchmark": "tpcc",
                "scale_factor": None,
                "warehouses": 1,
                "transaction_mix": [45, 43, 4, 4, 4],
                "drain_timeout_seconds": 30,
                "high_water_threshold_percent": 70,
                "resource_interval_seconds": 1,
                "cooldown_seconds": 2,
                "compose_timeout_seconds": 300,
                "loader_timeout_seconds": 600,
                "trial_timeout_seconds": 1800,
                "status_timeout_seconds": 900,
                "baseline_profile": "current_evidence_disabled_compatibility",
                "bootstrap_scan_max_rows": 1000000,
                "bootstrap_scan_max_input_bytes": 4294967296,
                "bootstrap_scan_max_canonical_bytes": 2147483648,
                "monitor_max_assembler_active_transactions": 1024,
                "monitor_max_assembler_events_per_transaction": 100000,
                "monitor_max_assembler_active_bytes": 268435456,
                "monitor_max_assembler_completed_transactions": 10000000,
                "monitor_max_assembler_retained_identity_bytes": 268435456,
                "monitor_max_pending_client_records": 1000000,
                "monitor_max_pending_cdc_slots": 1000000,
                "monitor_max_pending_cdc_bytes": 268435456,
                "monitor_max_retained_identity_records": 10000000,
                "monitor_max_events_per_transaction": 101000,
                "monitor_max_checker_state_bytes": 536870912,
            }
        )
        metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

        counts = [
            {"table": "benchbase.warehouse", "row_count": 1},
            {"table": "benchbase.district", "row_count": 10},
            {"table": "benchbase.customer", "row_count": 30000},
            {"table": "benchbase.history", "row_count": 30000},
            {"table": "benchbase.oorder", "row_count": 30000},
            {"table": "benchbase.new_order", "row_count": 9000},
            {"table": "benchbase.order_line", "row_count": 300000},
            {"table": "benchbase.item", "row_count": 100000},
            {"table": "benchbase.stock", "row_count": 100000},
        ]
        bootstrap_path = trial / "bootstrap_manifest.json"
        bootstrap = json.loads(bootstrap_path.read_text(encoding="utf-8"))
        bootstrap["table_row_counts"] = counts
        bootstrap_path.write_text(json.dumps(bootstrap), encoding="utf-8")
        loader_path = trial / "loader_validation.json"
        loader = json.loads(loader_path.read_text(encoding="utf-8"))
        loader.update(
            {
                "benchmark": "tpcc",
                "declaration_available": True,
                "scale_parameter": {"name": "warehouses", "value": 1},
                "declared_table_row_counts": counts,
                "expected_table_row_counts": counts,
                "observed_table_row_counts": counts,
            }
        )
        loader_path.write_text(json.dumps(loader), encoding="utf-8")

        names = ("NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel")
        counters = {
            "logical_transactions_started": 20,
            "successful_transactions": 20,
            "terminal_aborted_transactions": 0,
            "physical_attempts_started": 20,
            "definitively_aborted_attempts": 0,
            "retry_attempts": 0,
            "retryable_failure_events": 0,
            "error_transactions": 0,
            "unknown_status_transactions": 0,
            "ambiguous_attempts": 0,
            "retry_different_events": 0,
        }
        transaction_types = {
            name: {
                **counters,
                "successful_transactions_per_second": 10,
                "latency_microseconds": {
                    "mean": 100,
                    "p50": 90,
                    "p95": 150,
                    "p99": 200,
                    "max": 300,
                },
            }
            for name in names
        }
        totals = {name: value * 5 for name, value in counters.items()}
        outcome = {
            "kind": "transaction_outcome_metrics",
            "benchmark": "tpcc",
            "scope": "measurement_window",
            "window_semantics": "fully_contained_logical_invocations",
            "elapsed_time_nanoseconds": 2_000_000_000,
            "measured_requests": 100,
            "phase_ids": [1],
            "transaction_types": transaction_types,
            "totals": totals,
            "new_order_per_minute": 600,
            "new_order_rate_label": "NewOrder/min",
        }
        (trial / "outcome_metrics.json").write_text(json.dumps(outcome), encoding="utf-8")
        benchbase_path = trial / "benchbase_metrics.json"
        benchbase = json.loads(benchbase_path.read_text(encoding="utf-8"))
        benchbase["phase_ids"] = [1]
        benchbase["transaction_counts"] = {name: 20 for name in names}
        benchbase["transaction_types"] = {
            name: {
                "logical_invocation_count": 20,
                "latency_microseconds": transaction_types[name]["latency_microseconds"],
            }
            for name in names
        }
        benchbase_path.write_text(json.dumps(benchbase), encoding="utf-8")

        (trial / "redis_metrics.json").write_text(
            json.dumps({
                "entry_count": 50,
                "cdc_row_count": 40,
                "cdc_delivery_lag_ms": {"current": 2, "max": 5},
                "cdc_source_lag_ms": {"current": 3, "max": 7},
            }),
            encoding="utf-8",
        )
        (trial / "client_metrics.json").write_text(
            json.dumps({"entry_count": 2, "read_event_count": 60}), encoding="utf-8"
        )
        (trial / "client_stream.json").write_text(
            json.dumps([
                xrange_entry("11000-0", {"kind": "client_txn"}),
                xrange_entry("11500-0", {"kind": "session_done"}),
            ]),
            encoding="utf-8",
        )
        (trial / "analysis_window.json").write_text(
            json.dumps({
                "start_epoch_ms": 10000,
                "end_epoch_ms": 12020,
                "definition": "benchbase_measured_start_through_end_fence_transaction_end_cdc_arrival",
            }),
            encoding="utf-8",
        )
        (trial / "redis_info_after.txt").write_text(
            "used_memory:1000\nused_memory_peak:2000\nmaxmemory:8589934592\n",
            encoding="utf-8",
        )

        monitor_path = trial / "monitor_metrics.json"
        monitor = json.loads(monitor_path.read_text(encoding="utf-8"))
        monitor.update(
            {
                "finished_at_utc": "1970-01-01T00:00:12.100Z",
                "limits": {
                    "assembler": {
                        "max_active_transactions": 1024,
                        "max_events_per_transaction": 100000,
                        "max_active_bytes": 268435456,
                        "max_completed_transactions": 10000000,
                        "max_retained_identity_bytes": 268435456,
                    },
                    "joiner": {
                        "max_pending_client_records": 1000000,
                        "max_pending_client_bytes": 1073741824,
                        "max_pending_cdc_slots": 1000000,
                        "max_pending_cdc_bytes": 268435456,
                        "max_retained_identity_records": 10000000,
                        "max_retained_identity_bytes": 2147483648,
                        "max_events_per_transaction": 101000,
                        "max_bootstrap_state_rows": 10000000,
                        "max_bootstrap_state_bytes": 268435456,
                        "max_checker_state_bytes": 536870912,
                    },
                    "checker": {
                        "max_retained_state_bytes": 536870912,
                        "max_events_per_transaction": 101000,
                        "max_mapped_identity_records": 10000000,
                        "max_mapped_identity_bytes": 2147483648,
                    },
                },
                "assembler": {
                    "normalized_write_events": 1080,
                    "active_transactions": 0,
                    "active_transactions_high_water": 2,
                    "observed_events_per_transaction_high_water": 20,
                    "active_assembly_bytes": 0,
                    "active_assembly_high_water_bytes": 4096,
                    "completed_transactions": 100,
                    "retained_identity_high_water_bytes": 8192,
                },
            }
        )
        monitor["joiner"].update(
            {
                "pending_client_high_water_records": 3,
                "pending_client_records": 0,
                "pending_client_bytes": 0,
                "pending_client_high_water_bytes": 4096,
                "pending_cdc_slots": 0,
                "pending_cdc_bytes": 0,
                "pending_cdc_high_water_slots": 4,
                "pending_cdc_high_water_bytes": 8192,
                "retained_identity_high_water_records": 100,
                "retained_identity_high_water_bytes": 16384,
                "released_events_per_transaction_high_water": 30,
                "bootstrap_state_high_water_rows": 599011,
                "bootstrap_state_high_water_bytes": 100000000,
                "end_fence_drain_nanoseconds": 50000000,
                "end_fence_drain_current_nanoseconds": 0,
            }
        )
        monitor["checker"].update(
            {
                "mapped_identity_high_water_records": 100,
                "mapped_identity_high_water_bytes": 8192,
                "events_per_transaction_high_water": 30,
                "engine": {
                    "retained_state_capacity_bytes": 536870912,
                    "retained_state_bytes": 8192,
                    "peak_retained_state_bytes": 16384,
                },
            }
        )
        monitor_path.write_text(json.dumps(monitor), encoding="utf-8")
        report.write_checksums(trial, trial / "SHA256SUMS")
        return trial

    def test_aggregate_preserves_partial_no_verdict_and_fence_latencies(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for position, stage in enumerate(report.STAGES, start=1):
                self.write_trial(root, stage, position)
            raw = root / "raw_trials.csv"
            overhead = root / "overhead.csv"
            index = root / "artifact_index.csv"
            rows = report.write_raw_trials(root, raw)
            report.write_overhead(rows, overhead)
            report.write_artifact_index(root, index)

            with raw.open(encoding="utf-8", newline="") as stream:
                by_stage = {row["stage"]: row for row in csv.DictReader(stream)}
            join_only = by_stage["assembly_join"]
            audited = by_stage["assembly_join_check"]
            self.assertEqual(join_only["audited_outcome"], "")
            self.assertEqual(join_only["checker_observation"], "")
            self.assertEqual(join_only["reconciliation_result"], "True")
            self.assertEqual(join_only["run_start_fence_observation_latency_ms"], "10")
            self.assertEqual(join_only["load_finished_fence_observation_latency_ms"], "20")
            self.assertEqual(join_only["end_fence_observation_latency_ms"], "40")
            self.assertEqual(audited["audited_outcome"], "PASS")
            self.assertEqual(audited["end_fence_observation_latency_ms"], "30")
            self.assertEqual(audited["checker_rate_transactions_per_second"], "100.0")
            self.assertEqual(audited["head_of_line_wait_max_ms"], "5.0")

            with overhead.open(encoding="utf-8", newline="") as stream:
                overhead_rows = list(csv.DictReader(stream))
            attributions = {row["attribution"] for row in overhead_rows}
            self.assertIn("redis_publication_increment", attributions)
            self.assertIn("checking_boundary_increment", attributions)

    def test_tpcc_report_emits_window_rates_events_resources_and_per_type_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            self.write_tpcc_trial(root)
            raw_path = root / "raw_trials.csv"
            transaction_path = root / "transaction_results.csv"
            rows = report.write_raw_trials(root, raw_path)
            report.write_transaction_results(rows, transaction_path)
            row = rows[0]
            self.assertEqual(row["benchmark"], "tpcc")
            self.assertEqual(row["bootstrap_row_count"], 599011)
            self.assertEqual(row["successful_transactions_per_second_workload"], 50.0)
            self.assertEqual(row["new_order_per_minute"], 600)
            self.assertEqual(row["new_order_rate_label"], "NewOrder/min")
            self.assertEqual(row["workload_end_to_verdict_tail_ms"], 100)
            self.assertEqual(row["monitor_end_fence_drain_ms"], 50.0)
            self.assertEqual(row["client_read_events_per_second_workload"], 30.0)
            self.assertEqual(row["normalized_write_events_per_second_workload"], 40.0)
            self.assertTrue(row["declared_resource_high_water_accepted"])
            self.assertAlmostEqual(
                row["resource_bootstrap_state_bytes_high_water_percent"],
                100000000 * 100 / 268435456,
            )
            with transaction_path.open(encoding="utf-8", newline="") as stream:
                transaction_rows = list(csv.DictReader(stream))
            self.assertEqual(len(transaction_rows), 6)
            overall = next(item for item in transaction_rows if item["row_scope"] == "overall")
            self.assertEqual(overall["transaction_type"], "")
            self.assertEqual(overall["successful_transactions_per_second"], "50.0")
            self.assertEqual(overall["new_order_per_minute"], "600")
            by_name = {item["transaction_type"]: item for item in transaction_rows}
            self.assertEqual(by_name["NewOrder"]["row_scope"], "transaction_type")
            self.assertEqual(by_name["NewOrder"]["successful_transactions_per_second"], "10.0")
            self.assertEqual(by_name["NewOrder"]["new_order_per_minute"], "600.0")
            self.assertEqual(by_name["Payment"]["new_order_per_minute"], "")

    def test_join_only_capacity_acceptance_does_not_invent_a_checker_engine(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            trial = self.write_tpcc_trial(Path(directory_name))
            metadata = json.loads((trial / "metadata.json").read_text(encoding="utf-8"))
            metadata["stage"] = "assembly_join"
            monitor = json.loads(
                (trial / "monitor_metrics.json").read_text(encoding="utf-8")
            )
            monitor["checker"].update(
                {
                    "mapped_identity_high_water_records": 0,
                    "mapped_identity_high_water_bytes": 0,
                    "events_per_transaction_high_water": 0,
                    "engine": {
                        "retained_state_capacity_bytes": 0,
                        "retained_state_bytes": 0,
                        "peak_retained_state_bytes": 0,
                    },
                }
            )
            flattened: dict[str, object] = {}
            report.flatten("monitor", monitor, flattened)
            capacity = report.monitor_capacity_acceptance(metadata, flattened)
            self.assertTrue(capacity["resource_high_water_accepted"])
            self.assertNotIn("resource_checker_state_bytes_high_water", capacity)

    def test_tpcc_report_rejects_drain_resource_and_outcome_mutations(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            trial = self.write_tpcc_trial(root)
            monitor_path = trial / "monitor_metrics.json"
            monitor = json.loads(monitor_path.read_text(encoding="utf-8"))
            monitor["assembler"]["active_transactions_high_water"] = 800
            monitor_path.write_text(json.dumps(monitor), encoding="utf-8")
            with self.assertRaisesRegex(report.ReportError, "high-water threshold"):
                report.trial_row(trial)

            trial = self.write_tpcc_trial(Path(directory_name) / "terminal-queue")
            monitor_path = trial / "monitor_metrics.json"
            monitor = json.loads(monitor_path.read_text(encoding="utf-8"))
            monitor["joiner"]["pending_cdc_slots"] = 1
            monitor_path.write_text(json.dumps(monitor), encoding="utf-8")
            with self.assertRaisesRegex(report.ReportError, "terminal monitor queue"):
                report.trial_row(trial)

            trial = self.write_tpcc_trial(Path(directory_name) / "drain")
            monitor_path = trial / "monitor_metrics.json"
            monitor = json.loads(monitor_path.read_text(encoding="utf-8"))
            monitor["finished_at_utc"] = "1970-01-01T00:00:43.000Z"
            monitor_path.write_text(json.dumps(monitor), encoding="utf-8")
            with self.assertRaisesRegex(report.ReportError, "drain timeout"):
                report.trial_row(trial)

            trial = self.write_tpcc_trial(Path(directory_name) / "outcome")
            (trial / "outcome_metrics.json").unlink()
            with self.assertRaisesRegex(report.ReportError, "outcomes are missing"):
                report.trial_row(trial)

            trial = self.write_tpcc_trial(Path(directory_name) / "fractional-rate")
            elapsed_ns = 2_999_872_712
            benchbase_path = trial / "benchbase_metrics.json"
            benchbase = json.loads(benchbase_path.read_text(encoding="utf-8"))
            benchbase["elapsed_time_nanoseconds"] = elapsed_ns
            benchbase_path.write_text(json.dumps(benchbase), encoding="utf-8")
            outcome_path = trial / "outcome_metrics.json"
            outcome = json.loads(outcome_path.read_text(encoding="utf-8"))
            outcome["elapsed_time_nanoseconds"] = elapsed_ns
            outcome["new_order_per_minute"] = 20 * 60.0 / (elapsed_ns / 1_000_000_000.0)
            outcome_path.write_text(json.dumps(outcome), encoding="utf-8")
            report.trial_row(trial)
            outcome["new_order_per_minute"] += 0.01
            outcome_path.write_text(json.dumps(outcome), encoding="utf-8")
            with self.assertRaisesRegex(report.ReportError, "NewOrder/min differs"):
                report.trial_row(trial)

            trial = self.write_tpcc_trial(Path(directory_name) / "error-outcome")
            outcome_path = trial / "outcome_metrics.json"
            outcome = json.loads(outcome_path.read_text(encoding="utf-8"))
            new_order = outcome["transaction_types"]["NewOrder"]
            new_order["successful_transactions"] = 19
            new_order["error_transactions"] = 1
            new_order["definitively_aborted_attempts"] = 1
            outcome["totals"]["successful_transactions"] = 99
            outcome["totals"]["error_transactions"] = 1
            outcome["totals"]["definitively_aborted_attempts"] = 1
            outcome["new_order_per_minute"] = 570
            outcome_path.write_text(json.dumps(outcome), encoding="utf-8")
            row = report.trial_row(trial)
            self.assertEqual(row["error_transactions"], 1)

            trial = self.write_tpcc_trial(Path(directory_name) / "ambiguous-outcome")
            outcome_path = trial / "outcome_metrics.json"
            outcome = json.loads(outcome_path.read_text(encoding="utf-8"))
            new_order = outcome["transaction_types"]["NewOrder"]
            new_order["successful_transactions"] = 19
            new_order["unknown_status_transactions"] = 1
            new_order["ambiguous_attempts"] = 1
            outcome["totals"]["successful_transactions"] = 99
            outcome["totals"]["unknown_status_transactions"] = 1
            outcome["totals"]["ambiguous_attempts"] = 1
            outcome["new_order_per_minute"] = 570
            outcome_path.write_text(json.dumps(outcome), encoding="utf-8")
            with self.assertRaisesRegex(report.ReportError, "nonzero unknown_status_transactions"):
                report.trial_row(trial)

    def test_aggregate_accepts_one_selected_stage_without_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            roster = ("cdc_capture",)
            self.write_trial(root, roster[0], 1, roster)

            raw = root / "raw_trials.csv"
            overhead = root / "overhead.csv"
            rows = report.write_raw_trials(root, raw)
            report.write_overhead(rows, overhead)

            self.assertEqual([row["stage"] for row in rows], list(roster))
            with raw.open(encoding="utf-8", newline="") as stream:
                raw_rows = list(csv.DictReader(stream))
            self.assertEqual(
                raw_rows[0]["configured_stages_json"], '["cdc_capture"]'
            )
            with overhead.open(encoding="utf-8", newline="") as stream:
                self.assertEqual(list(csv.DictReader(stream)), [])

    def test_aggregate_emits_only_available_two_stage_comparison(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            roster = ("assembly_join", "assembly_join_check")
            for position, stage in enumerate(roster, start=1):
                self.write_trial(root, stage, position, roster)

            rows = report.write_raw_trials(root, root / "raw_trials.csv")
            overhead = root / "overhead.csv"
            report.write_overhead(rows, overhead)

            with overhead.open(encoding="utf-8", newline="") as stream:
                overhead_rows = list(csv.DictReader(stream))
            self.assertEqual(len(overhead_rows), len(report.OVERHEAD_METRICS))
            self.assertEqual(
                {row["attribution"] for row in overhead_rows},
                {"checking_boundary_increment"},
            )

    def test_headline_backfills_legacy_all_stage_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for position, stage in enumerate(report.STAGES, start=1):
                # Omitting configured_stages models artifacts written before
                # selectable stage rosters were introduced.
                self.write_trial(root, stage, position)
            raw = root / "raw_trials.csv"
            report.write_raw_trials(root, raw)
            csv_output = root / "headline_results.csv"
            markdown_output = root / "headline_results.md"
            report.write_headline(raw, csv_output, markdown_output)

            with csv_output.open(encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(
                list(rows[0]),
                [
                    "stage",
                    "throughput_requests_per_second_median",
                    "latency_mean_microseconds_median",
                    "latency_p95_microseconds_median",
                    "throughput_loss_vs_baseline_percent",
                ],
            )
            self.assertEqual(
                [row["stage"] for row in rows],
                list(report.BENCHBASE_HEADLINE_STAGES),
            )
            self.assertEqual(rows[0]["throughput_requests_per_second_median"], "990")
            self.assertEqual(rows[0]["throughput_loss_vs_baseline_percent"], "")
            self.assertEqual(
                rows[1]["throughput_loss_vs_baseline_percent"],
                "1.010101010101010101010101010",
            )

            markdown = markdown_output.read_text(encoding="utf-8")
            self.assertIn(
                "| Uninstrumented baseline | 990.0 req/s | 101.0 µs | 151 µs | — |",
                markdown,
            )
            self.assertIn(
                "| Capture/encoding | 980.0 req/s | 102.0 µs | 152 µs | 1.01% |",
                markdown,
            )
            self.assertNotIn("Assembly/join", markdown)
            self.assertNotIn("Full checking", markdown)

    def test_headline_excludes_monitor_stages(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            roster = ("assembly_join", "assembly_join_check")
            for position, stage in enumerate(roster, start=1):
                self.write_trial(root, stage, position, roster)
            raw = root / "raw_trials.csv"
            report.write_raw_trials(root, raw)
            csv_output = root / "headline_results.csv"
            markdown_output = root / "headline_results.md"
            report.write_headline(raw, csv_output, markdown_output)

            with csv_output.open(encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(rows, [])
            markdown = markdown_output.read_text(encoding="utf-8")
            self.assertNotIn("Assembly/join", markdown)
            self.assertNotIn("Full checking", markdown)

    def test_headline_selected_benchbase_stage_without_baseline_has_no_loss(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            roster = ("cdc_capture",)
            self.write_trial(root, roster[0], 1, roster)
            raw = root / "raw_trials.csv"
            report.write_raw_trials(root, raw)
            csv_output = root / "headline_results.csv"
            markdown_output = root / "headline_results.md"
            report.write_headline(raw, csv_output, markdown_output)

            with csv_output.open(encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual([row["stage"] for row in rows], ["cdc_capture"])
            self.assertEqual(rows[0]["throughput_loss_vs_baseline_percent"], "")
            self.assertIn(
                "| CDC only | 990.0 req/s | 101.0 µs | 151 µs | — |",
                markdown_output.read_text(encoding="utf-8"),
            )

    def test_headline_uses_exact_medians_across_repetitions(self) -> None:
        rows = report.headline_rows(
            [
                {
                    "stage": "benchbase_uninstrumented",
                    "throughput_requests_per_second": value,
                    "latency_mean_microseconds": latency,
                    "latency_p95_microseconds": p95,
                }
                for value, latency, p95 in (
                    ("100.1", "10.1", "20"),
                    ("300.3", "30.3", "40"),
                )
            ]
            + [
                {
                    "stage": "cdc_capture",
                    "throughput_requests_per_second": value,
                    "latency_mean_microseconds": latency,
                    "latency_p95_microseconds": p95,
                }
                for value, latency, p95 in (
                    ("80.08", "12.2", "22"),
                    ("240.24", "32.4", "42"),
                )
            ]
        )
        self.assertEqual(str(rows[0]["Throughput"]), "200.2")
        self.assertEqual(str(rows[0]["Mean latency"]), "20.2")
        self.assertEqual(str(rows[1]["Throughput"]), "160.16")
        self.assertEqual(str(rows[1]["Loss vs baseline"]), "20.0")

        faster = report.headline_rows(
            [
                {
                    "stage": "benchbase_uninstrumented",
                    "throughput_requests_per_second": "100",
                    "latency_mean_microseconds": "10",
                    "latency_p95_microseconds": "20",
                },
                {
                    "stage": "cdc_capture",
                    "throughput_requests_per_second": "110",
                    "latency_mean_microseconds": "11",
                    "latency_p95_microseconds": "21",
                },
            ]
        )
        self.assertEqual(str(faster[1]["Loss vs baseline"]), "-10.0")

    def test_aggregate_rejects_invalid_selected_stage_rosters(self) -> None:
        for roster, message in (
            (("cdc_capture", "cdc_capture"), "duplicates"),
            (("unknown_stage",), "unknown stage"),
        ):
            with self.subTest(roster=roster), tempfile.TemporaryDirectory() as directory_name:
                root = Path(directory_name)
                self.write_trial(root, "cdc_capture", 1, roster)
                with self.assertRaisesRegex(report.ReportError, message):
                    report.write_raw_trials(root, root / "raw_trials.csv")

    def test_capture_summary_merges_types_and_keeps_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            log = Path(directory_name) / "workload.log"
            summaries = []
            for worker in (1, 2):
                summaries.append(
                    {
                        "kind": "client_capture_measurement",
                        "run_id": "00000000-0000-0000-0000-000000000001",
                        "session_id": f"ycsb/worker/{worker}",
                        "record_count": 11,
                        "client_txn_count": 10,
                        "read_event_count": 5,
                        "encoded_payload_bytes": 100,
                        "transaction_type_counts": {"ReadRecord": 5, "UpdateRecord": 5},
                        "session_done_counters": {
                            "final_session_seq": 10,
                            "logical_transactions_started": 10,
                            "successful_transactions": 10,
                            "terminal_aborted_transactions": 0,
                            "physical_attempts_started": 10,
                            "definitively_aborted_attempts": 0,
                            "published_transactions": 10,
                            "retry_attempts": 0,
                            "ambiguous_attempts": 0,
                        },
                    }
                )
            log.write_text(
                "".join(f"INFO BLACKBOX_MEASUREMENT {json.dumps(item)}\n" for item in summaries),
                encoding="utf-8",
            )
            result = report.capture_measurement_summaries(log)
            self.assertEqual(result["final_session_seq"], 20)
            self.assertEqual(result["transaction_type_counts"], {"ReadRecord": 10, "UpdateRecord": 10})
            self.assertEqual(set(result["session_summaries"]), {"ycsb/worker/1", "ycsb/worker/2"})

    def test_join_only_rejects_any_verdict_content(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            self.write_trial(root, "assembly_join", 1)
            trial = root / "trials" / "repetition-01" / "01-assembly_join"
            monitor_path = trial / "monitor_metrics.json"
            monitor = json.loads(monitor_path.read_text(encoding="utf-8"))
            monitor["outcome"] = "PASS"
            monitor_path.write_text(json.dumps(monitor), encoding="utf-8")
            with self.assertRaisesRegex(report.ReportError, "forbidden verdict"):
                report.trial_row(trial)

    def test_trial_rejects_capacity_provenance_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            self.write_trial(root, "assembly_join", 1)
            trial = root / "trials" / "repetition-01" / "01-assembly_join"
            configuration_path = trial / "redis_configuration.json"
            configuration = json.loads(
                configuration_path.read_text(encoding="utf-8")
            )
            configuration["effective_maxmemory_bytes"] = 2147483648
            configuration_path.write_text(
                json.dumps(configuration), encoding="utf-8"
            )
            with self.assertRaisesRegex(report.ReportError, "Redis capacity"):
                report.trial_row(trial)

    def test_trial_rejects_pending_client_capacity_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            self.write_trial(root, "assembly_join", 1)
            trial = root / "trials" / "repetition-01" / "01-assembly_join"
            monitor_path = trial / "monitor_metrics.json"
            monitor = json.loads(monitor_path.read_text(encoding="utf-8"))
            monitor["limits"]["joiner"]["max_pending_client_bytes"] = 268435456
            monitor_path.write_text(json.dumps(monitor), encoding="utf-8")
            with self.assertRaisesRegex(report.ReportError, "pending-client limit"):
                report.trial_row(trial)

    def test_aggregate_rejects_pending_client_capacity_invariant_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for position, stage in enumerate(report.STAGES, start=1):
                self.write_trial(root, stage, position)
            metadata_path = (
                root
                / "trials"
                / "repetition-01"
                / "01-benchbase_uninstrumented"
                / "metadata.json"
            )
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            metadata["monitor_max_pending_client_bytes"] = 536870912
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
            with self.assertRaisesRegex(
                report.ReportError, "monitor_max_pending_client_bytes"
            ):
                report.write_raw_trials(root, root / "raw_trials.csv")

    def test_trial_rejects_bootstrap_state_capacity_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            self.write_trial(root, "assembly_join", 1)
            trial = root / "trials" / "repetition-01" / "01-assembly_join"
            monitor_path = trial / "monitor_metrics.json"
            monitor = json.loads(monitor_path.read_text(encoding="utf-8"))
            monitor["limits"]["joiner"]["max_bootstrap_state_bytes"] = 1
            monitor_path.write_text(json.dumps(monitor), encoding="utf-8")
            with self.assertRaisesRegex(report.ReportError, "bootstrap-state byte"):
                report.trial_row(trial)

    def test_trial_requires_canonical_loader_timestamp(self) -> None:
        invalid_values: tuple[object, ...] = (
            None,
            "2024-02-29T12:34:56+00:00",
            "2024-02-29T12:34:56.000Z",
            "2024-2-29T12:34:56Z",
            "2023-02-29T12:34:56Z",
            "2024-02-29T12:34:56Z ",
        )
        for index, value in enumerate(invalid_values):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as directory_name:
                root = Path(directory_name)
                self.write_trial(root, "benchbase_uninstrumented", 1)
                metadata_path = (
                    root
                    / "trials"
                    / "repetition-01"
                    / "01-benchbase_uninstrumented"
                    / "metadata.json"
                )
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
                if index == 0:
                    metadata.pop("loader_timestamp_utc")
                else:
                    metadata["loader_timestamp_utc"] = value
                metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
                with self.assertRaisesRegex(report.ReportError, "loader timestamp"):
                    report.trial_row(metadata_path.parent)

    def test_aggregate_rejects_loader_timestamp_invariant_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for position, stage in enumerate(report.STAGES, start=1):
                self.write_trial(root, stage, position)
            metadata_path = (
                root
                / "trials"
                / "repetition-01"
                / "01-benchbase_uninstrumented"
                / "metadata.json"
            )
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            metadata["loader_timestamp_utc"] = "2001-02-03T04:05:06Z"
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
            with self.assertRaisesRegex(
                report.ReportError, "loader_timestamp_utc"
            ):
                report.write_raw_trials(root, root / "raw_trials.csv")

    def test_aggregate_rejects_truncated_configured_repetitions(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for position, stage in enumerate(report.STAGES, start=1):
                self.write_trial(root, stage, position)
                metadata_path = (
                    root
                    / "trials"
                    / "repetition-01"
                    / f"{position:02d}-{stage}"
                    / "metadata.json"
                )
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
                metadata["smoke"] = False
                metadata["configured_repetitions"] = 3
                metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
            with self.assertRaisesRegex(report.ReportError, "configured roster"):
                report.write_raw_trials(root, root / "raw_trials.csv")

    def test_aggregate_rejects_stage_order_rotation_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for position, stage in enumerate(report.STAGES, start=1):
                self.write_trial(root, stage, position)
            metadata_path = (
                root
                / "trials"
                / "repetition-01"
                / "01-benchbase_uninstrumented"
                / "metadata.json"
            )
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            metadata["stage_order_position"] = 2
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
            with self.assertRaisesRegex(report.ReportError, "positions"):
                report.write_raw_trials(root, root / "raw_trials.csv")

    def test_checksum_writer_streams_large_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            payload = root / "large.bin"
            payload.write_bytes((b"0123456789abcdef" * 131_073))
            manifest = root / "SHA256SUMS"
            report.write_checksums(root, manifest)
            expected = hashlib.sha256(payload.read_bytes()).hexdigest()
            self.assertEqual(manifest.read_text(encoding="utf-8"), f"{expected}  large.bin\n")

    def test_fence_tail_uses_matching_transaction_end(self) -> None:
        run_id = "00000000-0000-0000-0000-000000000001"
        document = [
            xrange_entry(
                "3000-0",
                {
                    "after": {"run_id": run_id, "phase": "END_FENCE"},
                    "transaction": {"id": "fence-transaction"},
                },
            ),
            xrange_entry("3100-0", {"status": "END", "id": "another-transaction"}),
            xrange_entry("3200-0", {"status": "END", "id": "fence-transaction"}),
        ]
        self.assertEqual(
            report.fence_stream_time(
                document, run_id, "END_FENCE", transaction_end=True
            ),
            3200,
        )


if __name__ == "__main__":
    unittest.main()
