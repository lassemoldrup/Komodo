#!/usr/bin/env python3

from __future__ import annotations

import csv
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


def cdc_fences(
    run_id: str, load_end_id: str, end_fence_end_id: str
) -> list[object]:
    return [
        xrange_entry(
            "1000-0",
            {
                "after": {"run_id": run_id, "phase": "RUN_START"},
                "transaction": {"id": "run-start-tx"},
            },
        ),
        xrange_entry("1001-0", {"status": "END", "id": "run-start-tx"}),
        xrange_entry(
            "8999-0",
            {
                "after": {"run_id": run_id, "phase": "LOAD_FINISHED"},
                "transaction": {"id": "load-finished-tx"},
            },
        ),
        xrange_entry(load_end_id, {"status": "END", "id": "load-finished-tx"}),
        xrange_entry(
            "12510-0",
            {
                "after": {"run_id": run_id, "phase": "END_FENCE"},
                "transaction": {"id": "end-fence-tx"},
            },
        ),
        xrange_entry(end_fence_end_id, {"status": "END", "id": "end-fence-tx"}),
    ]


def status_records(run_id: str, final_status_id: str) -> list[object]:
    return [
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
            "9010-0",
            {
                "kind": "status",
                "run_id": run_id,
                "lifecycle_state": "LOAD_FINISHED_SEEN",
                "outcome": "RUNNING",
                "checker_observation": "NOT_STARTED",
            },
        ),
        xrange_entry(
            final_status_id,
            {
                "kind": "status",
                "run_id": run_id,
                "lifecycle_state": "FINAL",
                "outcome": "PASS",
                "checker_observation": "PASS_SO_FAR",
                "reconciliation": {"evidence_reconciled": True},
            },
        ),
    ]


class ReportTest(unittest.TestCase):
    maxDiff = None

    def write_trial(
        self,
        root: Path,
        mode: str,
        position: int,
        *,
        repetition: int = 1,
        roster: tuple[str, ...] = report.MODES,
        benchmark: str = "tpcc",
    ) -> Path:
        trial = (
            root
            / "trials"
            / f"repetition-{repetition:02d}"
            / f"{position:02d}-{mode}"
        )
        trial.mkdir(parents=True)
        run_id = f"00000000-0000-0000-{repetition:04d}-{position:012d}"
        full = mode == "full_e2e"
        elapsed_ns = 2_500_000_000 if full else 2_000_000_000
        measured_end_ms = 10_000 + elapsed_ns // 1_000_000
        load_finished_end_id = "9000-5"
        end_fence_end_id = "12520-7"
        final_status_id = "12600-3"
        monitor_end_ms = 12_700
        metadata = {
            "mode": mode,
            "configured_modes": list(roster),
            "repetition": repetition,
            "mode_order_position": position,
            "trial_id": f"trial-{repetition}-{position}",
            "run_id": run_id,
            "benchmark": benchmark,
            "terminals": 1,
            "scale_factor": 1 if benchmark == "ycsb" else None,
            "warehouses": 1,
            "field_size": 32,
            "loader_threads": 1,
            "loader_batch_size": 128,
            "loader_timestamp_utc": "2000-01-01T00:00:00Z",
            "warmup_seconds": 1,
            "requested_duration_seconds": 2,
            "random_seed": 0,
            "configured_repetitions": 1,
            "redis_maxmemory_bytes": 8589934592,
            "bootstrap_scan_max_rows": 1000000,
            "bootstrap_scan_max_input_bytes": 4294967296,
            "bootstrap_scan_max_canonical_bytes": 2147483648,
            "monitor_max_assembler_active_transactions": 1024,
            "monitor_max_assembler_events_per_transaction": 100000,
            "monitor_max_assembler_active_bytes": 268435456,
            "monitor_max_assembler_completed_transactions": 10000000,
            "monitor_max_assembler_retained_identity_bytes": 268435456,
            "monitor_max_pending_client_records": 1000000,
            "monitor_max_pending_client_bytes": 1073741824,
            "monitor_max_pending_cdc_slots": 1000000,
            "monitor_max_pending_cdc_bytes": 268435456,
            "monitor_max_retained_identity_records": 10000000,
            "monitor_max_retained_identity_bytes": 2147483648,
            "monitor_max_events_per_transaction": 101000,
            "monitor_max_bootstrap_state_rows": 1000000,
            "monitor_max_bootstrap_state_bytes": 1073741824,
            "monitor_max_checker_state_bytes": 536870912,
            "output_max_bytes": 107374182400,
            "isolation": "REPEATABLE_READ",
            "rate": "unlimited",
            "transaction_mix": [45, 43, 4, 4, 4] if benchmark == "tpcc" else [50, 50],
            "drain_timeout_seconds": 30,
            "high_water_threshold_percent": 70,
            "resource_interval_seconds": 1,
            "cooldown_seconds": 2,
            "compose_timeout_seconds": 300,
            "loader_timeout_seconds": 600,
            "trial_timeout_seconds": 1800,
            "status_timeout_seconds": 900,
            "baseline_profile": "ordinary_benchbase",
            "java_flags": "-Xms1g -Xmx1g",
            "si_monitoring_head": "s" * 40,
            "benchbase_current_head": "b" * 40,
            "benchbase_baseline_head": "b" * 40,
            "co_checker_head": "c" * 40,
            "smoke": True,
        }
        (trial / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")

        if benchmark == "tpcc":
            names = ("NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel")
            count_per_type = 20
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
        else:
            names = ("ReadRecord", "UpdateRecord")
            count_per_type = 50
            counts = [{"table": "benchbase.usertable", "row_count": 1000}]

        latency = {
            "mean": 125 if full else 100,
            "p50": 100 if full else 80,
            "p95": 200 if full else 150,
            "p99": 250 if full else 200,
            "max": 300,
        }
        benchbase = {
            "measured_count": 100,
            "elapsed_time_nanoseconds": elapsed_ns,
            "throughput_requests_per_second": 40 if full else 50,
            "goodput_requests_per_second": 40 if full else 50,
            "final_state": "EXIT",
            "latency_microseconds": latency,
            "measured_start_epoch_ms": 10_000,
            "measured_end_epoch_ms": measured_end_ms,
            "phase_ids": [1],
            "transaction_counts": {name: count_per_type for name in names},
            "transaction_types": {
                name: {
                    "logical_invocation_count": count_per_type,
                    "logical_invocations_per_second": count_per_type
                    / (elapsed_ns / 1_000_000_000),
                    "latency_microseconds": latency,
                }
                for name in names
            },
        }
        (trial / "benchbase_metrics.json").write_text(
            json.dumps(benchbase), encoding="utf-8"
        )

        zeros = {
            "terminal_aborted_transactions": 0,
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
                "logical_transactions_started": count_per_type,
                "successful_transactions": count_per_type,
                "physical_attempts_started": count_per_type,
                **zeros,
                "latency_microseconds": latency,
            }
            for name in names
        }
        counter_names = report.OUTCOME_COUNTER_FIELDS
        totals = {
            counter: sum(values[counter] for values in transaction_types.values())
            for counter in counter_names
        }
        outcome = {
            "kind": "transaction_outcome_metrics",
            "benchmark": benchmark,
            "scope": "measurement_window",
            "window_semantics": "fully_contained_logical_invocations",
            "elapsed_time_nanoseconds": elapsed_ns,
            "measured_requests": 100,
            "phase_ids": [1],
            "transaction_types": transaction_types,
            "totals": totals,
        }
        if benchmark == "tpcc":
            outcome["new_order_per_minute"] = (
                count_per_type * 60 / (elapsed_ns / 1_000_000_000)
            )
            outcome["new_order_rate_label"] = "NewOrder/min"
        (trial / "outcome_metrics.json").write_text(
            json.dumps(outcome), encoding="utf-8"
        )

        bootstrap = {
            "kind": "bootstrap_manifest",
            "run_id": run_id,
            "table_row_counts": counts,
            "canonical_state_digest": "a" * 64,
        }
        loader_validation = {
            "kind": "loader_validation",
            "benchmark": benchmark,
            "declaration_available": True,
            "loader_batch_size": 128,
            "scale_parameter": {
                "name": "warehouses" if benchmark == "tpcc" else "scale_factor",
                "value": 1,
            },
            "declared_table_row_counts": counts,
            "expected_table_row_counts": counts,
            "observed_table_row_counts": counts,
            "derived_cardinality_checks": [],
            "relationship_checks": [],
            "validated": True,
        }
        (trial / "bootstrap_manifest.json").write_text(
            json.dumps(bootstrap), encoding="utf-8"
        )
        (trial / "loader_validation.json").write_text(
            json.dumps(loader_validation), encoding="utf-8"
        )
        (trial / "marker_storage.json").write_text(
            json.dumps(
                {
                    "contract": "append_only_per_committed_attempt",
                    "mode": mode,
                    "workload_start_row_count": 0,
                    "workload_end_row_count": 120 if full else 0,
                    "workload_end_allocated_bytes": 32768 if full else 16384,
                    "complete_run_end_row_count": 120 if full else 0,
                    "complete_run_end_allocated_bytes": 32768 if full else 16384,
                }
            ),
            encoding="utf-8",
        )

        analysis_window: dict[str, object] = {
            "client_measurement_window": {
                "start_epoch_ms": 10000,
                "end_epoch_ms": measured_end_ms,
                "definition": "benchbase_measured_request_window",
            },
            "component_workload_window": None,
            "final_verdict": None,
        }

        if full:
            analysis_window["component_workload_window"] = {
                "after_stream_id": load_finished_end_id,
                "through_stream_id": end_fence_end_id,
                "start_epoch_ms": 9000,
                "end_epoch_ms": 12520,
                "start_exclusive": True,
                "end_inclusive": True,
                "definition": (
                    "after_load_finished_transaction_end_id_through_"
                    "end_fence_transaction_end_id_inclusive"
                ),
                "start_boundary": "LOAD_FINISHED",
                "end_boundary": "END_FENCE",
            }
            analysis_window["final_verdict"] = {
                "stream_id": final_status_id,
                "epoch_ms": 12600,
            }
            for name, document in {
                "redis_configuration.json": {
                    "requested_maxmemory_bytes": 8589934592,
                    "effective_maxmemory_bytes": 8589934592,
                    "maxmemory_policy": "noeviction",
                },
                "redis_metrics.json": {
                    "entry_count": 50,
                    "selection_window": {
                        "bounded": True,
                        "after_stream_id": load_finished_end_id,
                        "through_stream_id": end_fence_end_id,
                        "start_exclusive": True,
                        "end_inclusive": True,
                    },
                    "begin_count": 121,
                    "end_count": 121,
                    "completed_transaction_count": 121,
                    "cdc_row_count": 49,
                    "cdc_control_row_count": 1,
                    "cdc_workload_row_count": 48,
                    "cdc_workload_delivery_lag_sample_count": 48,
                    "cdc_workload_delivery_lag_ms": {
                        "current": 2,
                        "max": 5,
                        "mean": 3,
                        "p50": 3,
                        "p95": 4,
                        "p99": 5,
                    },
                    "cdc_workload_source_lag_sample_count": 48,
                    "cdc_workload_source_lag_ms": {
                        "current": 3,
                        "max": 7,
                        "mean": 4,
                        "p50": 4,
                        "p95": 6,
                        "p99": 7,
                    },
                },
                "client_metrics.json": {
                    "entry_count": 121,
                    "client_txn_count": 120,
                    "read_event_count": 72,
                    "session_done_counters": {"published_transactions": 120},
                },
            }.items():
                (trial / name).write_text(json.dumps(document), encoding="utf-8")
            (trial / "cdc_stream.json").write_text(
                json.dumps(cdc_fences(run_id, load_finished_end_id, end_fence_end_id)),
                encoding="utf-8",
            )
            (trial / "client_stream.json").write_text(
                json.dumps(
                    [
                        xrange_entry("11000-0", {"kind": "client_txn"}),
                        xrange_entry("11500-0", {"kind": "session_done"}),
                    ]
                ),
                encoding="utf-8",
            )
            (trial / "status_stream.json").write_text(
                json.dumps(status_records(run_id, final_status_id)), encoding="utf-8"
            )
            (trial / "redis_info_after.txt").write_text(
                "used_memory:1000\nused_memory_peak:2000\nmaxmemory:8589934592\n",
                encoding="utf-8",
            )
            monitor = self.monitor_document(
                run_id,
                monitor_end_ms,
                bootstrap_rows=sum(item["row_count"] for item in counts),
            )
            (trial / "monitor_metrics.json").write_text(
                json.dumps(monitor), encoding="utf-8"
            )

        (trial / "analysis_window.json").write_text(
            json.dumps(analysis_window), encoding="utf-8"
        )

        report.write_checksums(trial, trial / "SHA256SUMS")
        return trial

    @staticmethod
    def monitor_document(
        run_id: str, monitor_end_ms: int, *, bootstrap_rows: int
    ) -> dict[str, object]:
        return {
            "kind": "performance_measurement",
            "run_id": run_id,
            "processing_mode": "audited",
            "completed": True,
            "completion_status": "completed",
            "evidence_reconciled": True,
            "outcome": "PASS",
            "finished_at_utc": (
                "1970-01-01T00:00:" f"{monitor_end_ms / 1000:06.3f}Z"
            ),
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
                    "max_bootstrap_state_rows": 1000000,
                    "max_bootstrap_state_bytes": 1073741824,
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
                "normalized_write_events": bootstrap_rows + 40,
                "workload_raw_row_events": 48,
                "workload_normalized_write_events": 40,
                "completed_workload_transactions": 120,
                "workload_processing_total_nanoseconds": 500000000,
                "workload_processing_max_nanoseconds": 10000000,
                "active_transactions": 0,
                "active_transactions_high_water": 2,
                "observed_events_per_transaction_high_water": 20,
                "active_assembly_bytes": 0,
                "active_assembly_high_water_bytes": 4096,
                "completed_transactions": 1100,
                "retained_identity_high_water_bytes": 8192,
            },
            "joiner": {
                "joined_workload_transactions": 120,
                "joined_read_events": 72,
                "joined_write_events": 40,
                "workload_processing_total_nanoseconds": 250000000,
                "workload_processing_max_nanoseconds": 8000000,
                "released_bootstrap_write_events": bootstrap_rows,
                "released_ordered_events": bootstrap_rows + 100,
                "missing_client_head_wait_current_nanoseconds": 0,
                "missing_client_head_wait_max_nanoseconds": 5000000,
                "missing_client_head_wait_count": 0,
                "missing_client_head_wait_total_nanoseconds": 0,
                "join_buffer_high_water_bytes": 4096,
                "pending_client_records": 0,
                "pending_client_bytes": 0,
                "pending_client_high_water_records": 3,
                "pending_client_high_water_bytes": 4096,
                "pending_cdc_slots": 0,
                "pending_cdc_bytes": 0,
                "pending_cdc_high_water_slots": 4,
                "pending_cdc_high_water_bytes": 8192,
                "unmatched_client_records": 0,
                "unmatched_workload_cdc_slots": 0,
                "retained_identity_high_water_records": 100,
                "retained_identity_high_water_bytes": 16384,
                "released_events_per_transaction_high_water": 30,
                "bootstrap_state_high_water_rows": bootstrap_rows,
                "bootstrap_state_high_water_bytes": 100000000,
                "end_fence_drain_nanoseconds": 50000000,
                "end_fence_drain_current_nanoseconds": 0,
            },
            "checker": {
                "consumed_transactions": 1100,
                "workload_transactions": 120,
                "workload_read_events": 72,
                "workload_write_events": 40,
                "workload_consume_total_nanoseconds": 125000000,
                "workload_consume_max_nanoseconds": 4000000,
                "mapped_identity_high_water_records": 100,
                "mapped_identity_high_water_bytes": 8192,
                "events_per_transaction_high_water": 30,
                "engine": {
                    "retained_state_capacity_bytes": 536870912,
                    "retained_state_bytes": 8192,
                    "peak_retained_state_bytes": 16384,
                },
            },
        }

    def write_formal_pair(self, root: Path, *, benchmark: str = "tpcc") -> None:
        for position, mode in enumerate(report.MODES, start=1):
            self.write_trial(root, mode, position, benchmark=benchmark)

    def test_formal_mode_roster_has_exactly_two_modes(self) -> None:
        self.assertEqual(
            report.MODES, ("benchbase_uninstrumented", "full_e2e")
        )

    def test_aggregate_separates_client_cdc_cpp_monitor_and_pipeline(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            self.write_formal_pair(root)
            raw_path = root / "raw_trials.csv"
            impact_path = root / "overhead.csv"
            tx_path = root / "transaction_results.csv"
            component_path = root / "component_results.csv"
            rows = report.write_raw_trials(root, raw_path)
            report.write_overhead(rows, impact_path)
            report.write_transaction_results(rows, tx_path)
            report.write_component_results(rows, component_path)
            by_mode = {row["mode"]: row for row in rows}
            baseline = by_mode["benchbase_uninstrumented"]
            full = by_mode["full_e2e"]

            self.assertEqual(
                baseline["logical_transactions_per_second"], 50
            )
            self.assertEqual(
                full["logical_transactions_per_second"], 40
            )
            self.assertEqual(full["audited_outcome"], "PASS")
            self.assertEqual(full["reconciliation_result"], True)
            self.assertEqual(full["logical_transactions_started"], 100)
            self.assertEqual(full["component_workload_client_transactions"], 120)
            self.assertEqual(full["component_workload_window_seconds"], 3.52)
            self.assertEqual(full["component_workload_end_to_verdict_tail_ms"], 80)
            self.assertEqual(full["client_measurement_end_to_verdict_tail_ms"], 100)

            # CDC ingestion is Debezium/Redis delivery only; its lifecycle row
            # is reported but excluded from the workload numerator.
            self.assertEqual(full["cdc_ingestion_workload_rows"], 48)
            self.assertEqual(full["cdc_ingestion_control_rows"], 1)
            self.assertAlmostEqual(
                full["cdc_ingestion_workload_rows_per_second"], 48 / 3.52
            )
            self.assertEqual(
                full["cdc_ingestion_workload_source_lag_ms_p95"], 6
            )
            self.assertEqual(
                full["cdc_ingestion_workload_delivery_lag_ms_p95"], 4
            )

            # These are C++ component service rates and use only each
            # component's workload-active time.
            self.assertEqual(
                full[
                    "cpp_monitor_assembler_accept_call_service_transactions_per_second"
                ],
                240,
            )
            self.assertEqual(
                full["cpp_monitor_assembler_accept_call_service_events_per_second"],
                96,
            )
            self.assertEqual(
                full[
                    "cpp_monitor_joiner_ready_prefix_release_service_transactions_per_second"
                ],
                480,
            )
            self.assertEqual(
                full[
                    "cpp_monitor_joiner_ready_prefix_release_service_events_per_second"
                ],
                448,
            )
            self.assertEqual(
                full[
                    "cpp_monitor_checker_engine_consume_service_transactions_per_second"
                ],
                960,
            )
            self.assertEqual(
                full[
                    "cpp_monitor_checker_engine_consume_service_events_per_second"
                ],
                896,
            )

            # The end-to-end rate spans workload start through final checker
            # verdict and therefore includes Debezium, Redis, C++ and drain.
            self.assertEqual(full["evidence_pipeline_interval_seconds"], 3.6)
            self.assertAlmostEqual(
                full["evidence_pipeline_transactions_per_second"], 120 / 3.6
            )
            self.assertAlmostEqual(
                full["evidence_pipeline_events_per_second"], 112 / 3.6
            )
            self.assertTrue(full["drain_tail_accepted"])
            self.assertTrue(full["terminal_monitor_queues_drained"])

            with impact_path.open(encoding="utf-8", newline="") as stream:
                impact = list(csv.DictReader(stream))
            self.assertEqual(len(impact), 1)
            self.assertEqual(
                impact[0]["comparison_mode"], "benchbase_uninstrumented"
            )
            self.assertEqual(impact[0]["mode"], "full_e2e")
            self.assertNotIn("stage", impact[0])
            self.assertEqual(
                impact[0]["attribution"], "full_e2e_client_total_effect"
            )
            self.assertEqual(
                impact[0]["metric"],
                "logical_transactions_per_second",
            )
            self.assertEqual(float(impact[0]["overhead_percent"]), 20)

            with tx_path.open(encoding="utf-8", newline="") as stream:
                transaction_rows = list(csv.DictReader(stream))
            self.assertEqual(len(transaction_rows), 12)
            self.assertEqual(
                {row["mode"] for row in transaction_rows}, set(report.MODES)
            )

            with component_path.open(encoding="utf-8", newline="") as stream:
                component_rows = list(csv.DictReader(stream))
            self.assertEqual(
                [row["component_boundary"] for row in component_rows],
                [
                    "cdc_ingestion",
                    "cpp_monitor_assembler",
                    "cpp_monitor_joiner",
                    "cpp_monitor_checker",
                    "whole_evidence_pipeline",
                ],
            )
            cdc = component_rows[0]
            self.assertEqual(cdc["component_owner"], "debezium_to_redis")
            self.assertEqual(cdc["workload_events"], "48.0")
            self.assertEqual(
                cdc["event_kind"],
                "raw_cdc_rows_including_markers_excluding_control",
            )
            self.assertEqual(cdc["transactions_per_second"], "")
            checker = component_rows[3]
            self.assertEqual(checker["component_owner"], "cpp_monitor")
            self.assertEqual(checker["transactions_per_second"], "960.0")
            self.assertIn("not_sustainable_throughput", checker["rate_interpretation"])
            pipeline = component_rows[4]
            self.assertEqual(pipeline["component_owner"], "black_box_e2e")
            self.assertAlmostEqual(float(pipeline["events_per_second"]), 112 / 3.6)

    def test_headline_uses_logical_tps_as_primary_metric(self) -> None:
        rows = [
            {
                "mode": "benchbase_uninstrumented",
                "logical_transactions_per_second": "100",
                "successful_transactions_per_second": "98",
                "latency_mean_microseconds": "100",
                "latency_p95_microseconds": "150",
            },
            {
                "mode": "full_e2e",
                "logical_transactions_per_second": "80",
                "successful_transactions_per_second": "79",
                "latency_mean_microseconds": "125",
                "latency_p95_microseconds": "200",
            },
        ]
        headline = report.headline_rows(rows)
        self.assertEqual([row["mode"] for row in headline], list(report.MODES))
        self.assertEqual(str(headline[1]["Logical TPS loss vs baseline"]), "20.0")
        self.assertEqual(str(headline[1]["Successful TPS"]), "79")

    def test_both_modes_require_loader_declarations_for_ycsb_and_tpcc(self) -> None:
        for benchmark in ("ycsb", "tpcc"):
            with self.subTest(benchmark=benchmark):
                with tempfile.TemporaryDirectory() as directory_name:
                    root = Path(directory_name)
                    self.write_formal_pair(root, benchmark=benchmark)
                    rows = report.write_raw_trials(root, root / "raw.csv")
                    self.assertEqual(len(rows), 2)
                    for row in rows:
                        self.assertTrue(row["loader_validation_declaration_available"])

    def test_full_e2e_requires_audited_monitor_and_baseline_rejects_one(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            baseline = self.write_trial(root, "benchbase_uninstrumented", 1)
            (baseline / "monitor_metrics.json").write_text(
                json.dumps({"processing_mode": "audited"}), encoding="utf-8"
            )
            with self.assertRaisesRegex(
                report.ReportError, r"baseline contains C\+\+ monitor"
            ):
                report.trial_row(baseline)

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            full = self.write_trial(
                root, "full_e2e", 1, roster=("full_e2e",)
            )
            (full / "monitor_metrics.json").unlink()
            with self.assertRaisesRegex(report.ReportError, "monitor metrics are missing"):
                report.trial_row(full)

    def test_full_e2e_fails_closed_on_component_counter_or_timing_mismatch(self) -> None:
        mutations = (
            (
                lambda document: document["checker"].__setitem__(
                    "workload_transactions", 99
                ),
                "transaction counters do not reconcile",
            ),
            (
                lambda document: document["joiner"].__setitem__(
                    "workload_processing_total_nanoseconds", 0
                ),
                "active nanoseconds",
            ),
            (
                lambda document: document["assembler"].__setitem__(
                    "workload_normalized_write_events", 39
                ),
                "write-event counters do not reconcile",
            ),
        )
        for mutate, message in mutations:
            with self.subTest(message=message):
                with tempfile.TemporaryDirectory() as directory_name:
                    trial = self.write_trial(
                        Path(directory_name),
                        "full_e2e",
                        1,
                        roster=("full_e2e",),
                    )
                    path = trial / "monitor_metrics.json"
                    document = json.loads(path.read_text(encoding="utf-8"))
                    mutate(document)
                    path.write_text(json.dumps(document), encoding="utf-8")
                    with self.assertRaisesRegex(report.ReportError, message):
                        report.trial_row(trial)

    def test_full_e2e_fails_closed_on_cdc_control_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            trial = self.write_trial(
                Path(directory_name), "full_e2e", 1, roster=("full_e2e",)
            )
            path = trial / "redis_metrics.json"
            document = json.loads(path.read_text(encoding="utf-8"))
            document["cdc_control_row_count"] = 0
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(report.ReportError, "does not reconcile"):
                report.trial_row(trial)

    def test_append_only_marker_storage_is_required_and_reconciled(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            trial = self.write_trial(
                Path(directory_name), "full_e2e", 1, roster=("full_e2e",)
            )
            self.mutate_json(
                trial / "marker_storage.json",
                lambda value: value.update(
                    {
                        "workload_end_row_count": 119,
                        "complete_run_end_row_count": 119,
                    }
                ),
            )
            with self.assertRaisesRegex(report.ReportError, "marker row count"):
                report.trial_row(trial)

        with tempfile.TemporaryDirectory() as directory_name:
            trial = self.write_trial(
                Path(directory_name),
                "benchbase_uninstrumented",
                1,
                roster=("benchbase_uninstrumented",),
            )
            self.mutate_json(
                trial / "marker_storage.json",
                lambda value: value.update(
                    {
                        "workload_end_row_count": 1,
                        "complete_run_end_row_count": 1,
                    }
                ),
            )
            with self.assertRaisesRegex(report.ReportError, "uninstrumented mode"):
                report.trial_row(trial)

    def test_exact_component_and_final_boundaries_are_extracted_and_checked(self) -> None:
        run_id = "00000000-0000-0000-0000-000000000001"
        cdc = cdc_fences(run_id, "9000-5", "12520-7")
        self.assertEqual(
            report.component_fence_window(cdc, run_id),
            {
                "LOAD_FINISHED": {
                    "transaction_end_stream_id": "9000-5",
                    "epoch_ms": 9000,
                },
                "END_FENCE": {
                    "transaction_end_stream_id": "12520-7",
                    "epoch_ms": 12520,
                },
            },
        )
        self.assertEqual(
            report.final_status_boundary(status_records(run_id, "12600-3"), run_id),
            {"stream_id": "12600-3", "epoch_ms": 12600},
        )

        with tempfile.TemporaryDirectory() as directory_name:
            trial = self.write_trial(
                Path(directory_name), "full_e2e", 1, roster=("full_e2e",)
            )
            self.mutate_json(
                trial / "analysis_window.json",
                lambda value: value["component_workload_window"].__setitem__(
                    "after_stream_id", "9000-4"
                ),
            )
            self.mutate_json(
                trial / "redis_metrics.json",
                lambda value: value["selection_window"].__setitem__(
                    "after_stream_id", "9000-4"
                ),
            )
            with self.assertRaisesRegex(report.ReportError, "CDC fence transaction END IDs"):
                report.trial_row(trial)

    def test_cross_boundary_component_counter_mutations_fail_closed(self) -> None:
        mutations = (
            (
                "redis_metrics.json",
                lambda value: value.__setitem__("cdc_workload_row_count", 47),
                "CDC workload/control",
            ),
            (
                "monitor_metrics.json",
                lambda value: value["assembler"].__setitem__(
                    "workload_raw_row_events", 47
                ),
                "CDC and assembler",
            ),
            (
                "client_metrics.json",
                lambda value: value.__setitem__("client_txn_count", 119),
                "client stream and SessionDone",
            ),
        )
        for filename, mutate, message in mutations:
            with self.subTest(filename=filename):
                with tempfile.TemporaryDirectory() as directory_name:
                    trial = self.write_trial(
                        Path(directory_name), "full_e2e", 1, roster=("full_e2e",)
                    )
                    self.mutate_json(trial / filename, mutate)
                    with self.assertRaisesRegex(report.ReportError, message):
                        report.trial_row(trial)

    def test_outcome_algebra_and_required_artifacts_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            trial = self.write_trial(
                Path(directory_name), "full_e2e", 1, roster=("full_e2e",)
            )
            path = trial / "outcome_metrics.json"
            document = json.loads(path.read_text(encoding="utf-8"))
            document["transaction_types"]["NewOrder"]["error_transactions"] = 1
            document["totals"]["error_transactions"] = 1
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(report.ReportError, "logical outcomes"):
                report.trial_row(trial)

        with tempfile.TemporaryDirectory() as directory_name:
            trial = self.write_trial(
                Path(directory_name), "full_e2e", 1, roster=("full_e2e",)
            )
            (trial / "status_stream.json").unlink()
            with self.assertRaisesRegex(report.ReportError, "artifact is missing"):
                report.trial_row(trial)

    def test_loader_and_capacity_provenance_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            trial = self.write_trial(
                Path(directory_name),
                "benchbase_uninstrumented",
                1,
                roster=("benchbase_uninstrumented",),
            )
            self.mutate_json(
                trial / "loader_validation.json",
                lambda value: value.__setitem__("declaration_available", False),
            )
            with self.assertRaisesRegex(report.ReportError, "declaration availability"):
                report.trial_row(trial)

        with tempfile.TemporaryDirectory() as directory_name:
            trial = self.write_trial(
                Path(directory_name), "full_e2e", 1, roster=("full_e2e",)
            )
            self.mutate_json(
                trial / "monitor_metrics.json",
                lambda value: value["limits"]["joiner"].__setitem__(
                    "max_pending_client_bytes", 1
                ),
            )
            with self.assertRaisesRegex(report.ReportError, "limit differs"):
                report.trial_row(trial)

    def test_loader_timestamp_and_mode_order_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            trial = self.write_trial(
                Path(directory_name),
                "benchbase_uninstrumented",
                1,
                roster=("benchbase_uninstrumented",),
            )
            self.mutate_json(
                trial / "metadata.json",
                lambda value: value.__setitem__(
                    "loader_timestamp_utc", "2000-01-01T00:00:00.000Z"
                ),
            )
            with self.assertRaisesRegex(report.ReportError, "canonical UTC"):
                report.trial_row(trial)

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            self.write_formal_pair(root)
            full = root / "trials" / "repetition-01" / "02-full_e2e"
            self.mutate_json(
                full / "metadata.json",
                lambda value: value.__setitem__("mode_order_position", 1),
            )
            with self.assertRaisesRegex(report.ReportError, "positions"):
                report.write_raw_trials(root, root / "raw.csv")

    def test_single_selected_formal_mode_is_valid_without_a_comparison(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            self.write_trial(
                root, "full_e2e", 1, roster=("full_e2e",)
            )
            rows = report.write_raw_trials(root, root / "raw.csv")
            report.write_overhead(rows, root / "overhead.csv")
            self.assertEqual([row["mode"] for row in rows], ["full_e2e"])
            with (root / "overhead.csv").open(encoding="utf-8", newline="") as stream:
                self.assertEqual(list(csv.DictReader(stream)), [])

    def test_resource_and_drain_gates_remain_fail_closed(self) -> None:
        mutations = (
            (
                lambda trial: self.mutate_json(
                    trial / "monitor_metrics.json",
                    lambda value: value["assembler"].__setitem__(
                        "active_transactions_high_water", 800
                    ),
                ),
                "high-water threshold",
            ),
            (
                lambda trial: self.mutate_json(
                    trial / "monitor_metrics.json",
                    lambda value: value["joiner"].__setitem__(
                        "pending_cdc_slots", 1
                    ),
                ),
                "terminal monitor queue",
            ),
            (
                lambda trial: self.move_final_verdict(trial, "50000-0"),
                "drain timeout",
            ),
        )
        for mutate, message in mutations:
            with self.subTest(message=message):
                with tempfile.TemporaryDirectory() as directory_name:
                    trial = self.write_trial(
                        Path(directory_name),
                        "full_e2e",
                        1,
                        roster=("full_e2e",),
                    )
                    mutate(trial)
                    with self.assertRaisesRegex(report.ReportError, message):
                        report.trial_row(trial)

    @staticmethod
    def mutate_json(path: Path, mutate: object) -> None:
        document = json.loads(path.read_text(encoding="utf-8"))
        mutate(document)  # type: ignore[operator]
        path.write_text(json.dumps(document), encoding="utf-8")

    def move_final_verdict(self, trial: Path, stream_id: str) -> None:
        epoch_ms = int(stream_id.split("-", 1)[0])
        self.mutate_json(
            trial / "status_stream.json",
            lambda value: value[-1].__setitem__(0, stream_id),
        )
        self.mutate_json(
            trial / "analysis_window.json",
            lambda value: value.__setitem__(
                "final_verdict", {"stream_id": stream_id, "epoch_ms": epoch_ms}
            ),
        )

    def test_selected_roster_is_canonical_and_unique(self) -> None:
        for roster, message in (
            (("full_e2e", "benchbase_uninstrumented"), "canonical order"),
            (("full_e2e", "full_e2e"), "duplicates"),
            (("unknown",), "unknown mode"),
        ):
            with self.subTest(roster=roster):
                with tempfile.TemporaryDirectory() as directory_name:
                    root = Path(directory_name)
                    self.write_trial(
                        root,
                        "full_e2e" if "full_e2e" in roster else "benchbase_uninstrumented",
                        1,
                        roster=roster,
                    )
                    with self.assertRaisesRegex(report.ReportError, message):
                        report.write_raw_trials(root, root / "raw.csv")

    def test_capture_summary_keeps_session_and_type_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            log = Path(directory_name) / "workload.log"
            summaries = [
                {
                    "kind": "client_capture_measurement",
                    "run_id": "run",
                    "session_id": f"worker-{index}",
                    "record_count": 10,
                    "client_txn_count": 9,
                    "read_event_count": index,
                    "encoded_payload_bytes": 100,
                    "transaction_type_counts": {"NewOrder": 9},
                    "session_done_counters": {
                        "final_session_seq": 10,
                        "logical_transactions_started": 9,
                        "successful_transactions": 9,
                        "terminal_aborted_transactions": 0,
                        "physical_attempts_started": 9,
                        "definitively_aborted_attempts": 0,
                        "published_transactions": 9,
                        "retry_attempts": 0,
                        "ambiguous_attempts": 0,
                    },
                }
                for index in (1, 2)
            ]
            log.write_text(
                "".join(
                    f"prefix BLACKBOX_MEASUREMENT {json.dumps(item)}\n"
                    for item in summaries
                ),
                encoding="utf-8",
            )
            result = report.capture_measurement_summaries(
                log,
                expected_run_id="run",
                expected_sessions=("worker-1", "worker-2"),
            )
            self.assertEqual(result["client_txn_count"], 18)
            self.assertEqual(result["read_event_count"], 3)
            self.assertEqual(result["transaction_type_counts"], {"NewOrder": 18})

    def test_checksum_writer_excludes_its_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            (root / "large.bin").write_bytes(b"x" * (2 * 1024 * 1024))
            output = root / "SHA256SUMS"
            report.write_checksums(root, output)
            text = output.read_text(encoding="utf-8")
            self.assertIn("  large.bin\n", text)
            self.assertNotIn("SHA256SUMS", text)


if __name__ == "__main__":
    unittest.main()
