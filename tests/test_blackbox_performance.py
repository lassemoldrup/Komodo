#!/usr/bin/env python3

from __future__ import annotations

import csv
import importlib.util
import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "scripts" / "blackbox_performance.py"
SPEC = importlib.util.spec_from_file_location("blackbox_performance", TOOL)
assert SPEC is not None and SPEC.loader is not None
performance = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(performance)


def redis_entry(stream_id: str, field: str, value: dict[str, object]) -> list[object]:
    return [
        stream_id,
        [field, json.dumps(value, ensure_ascii=False, separators=(",", ":"))],
    ]


def proc_stat(
    pid: int, command: str, parent: int, user_ticks: int, system_ticks: int, rss_pages: int
) -> str:
    # Fields starts at proc(5)'s field 3 (state).  The parser uses ppid (4),
    # utime/stime (14/15), and rss (24).
    fields = ["0"] * 50
    fields[0] = "S"
    fields[1] = str(parent)
    fields[11] = str(user_ticks)
    fields[12] = str(system_ticks)
    fields[21] = str(rss_pages)
    return f"{pid} ({command}) " + " ".join(fields) + "\n"


class BenchBaseAnalysisTest(unittest.TestCase):
    def setUp(self) -> None:
        self.raw_text = "\n".join(
            [
                ",".join(performance.BENCHBASE_RAW_FIELDS),
                "1,read,1700000000.001000,10,0,0",
                "2,update,1700000000.002000,20,1,0",
                "1,read,1700000000.003000,30,0,0",
                "2,update,1700000000.004000,40,1,0",
            ]
        ) + "\n"
        self.summary = {
            "Measured Requests": 4,
            "Throughput (requests/second)": 2.0,
            "Goodput (requests/second)": 2.0,
            "Final State": "EXIT",
            "Elapsed Time (nanoseconds)": 2_000_000_000,
            # BenchBase currently records this before warmup, so the raw
            # transaction timestamps are the measured-window authority.
            "Start timestamp (milliseconds)": 1000,
        }

    def test_raw_samples_define_counts_latency_and_measured_window(self) -> None:
        rows = performance.read_benchbase_raw(io.StringIO(self.raw_text))
        result = performance.analyze_benchbase(rows, self.summary)
        self.assertEqual(result["measured_count"], 4)
        self.assertEqual(result["elapsed_time_nanoseconds"], 2_000_000_000)
        self.assertEqual(result["throughput_requests_per_second"], 2.0)
        self.assertEqual(result["goodput_requests_per_second"], 2.0)
        self.assertEqual(result["final_state"], "EXIT")
        self.assertEqual(
            result["latency_microseconds"],
            {"mean": 25.0, "p50": 30, "p95": 40, "p99": 40, "max": 40},
        )
        self.assertEqual(result["measured_start_epoch_ms"], 1_700_000_000_001)
        self.assertEqual(result["measured_end_epoch_ms"], 1_700_000_000_005)
        self.assertEqual(result["transaction_counts"], {"read": 2, "update": 2})
        self.assertEqual(result["phase_ids"], [0])
        self.assertEqual(result["transaction_types"]["read"]["logical_invocation_count"], 2)
        self.assertEqual(result["transaction_types"]["update"]["latency_microseconds"]["p95"], 40)

    def test_empty_measurement_uses_summary_window_and_na_latency(self) -> None:
        summary = dict(self.summary)
        summary["Measured Requests"] = 0
        summary["Throughput (requests/second)"] = 0
        result = performance.analyze_benchbase([], summary)
        self.assertEqual(result["measured_start_epoch_ms"], 1000)
        self.assertEqual(result["measured_end_epoch_ms"], 3000)
        self.assertEqual(
            result["latency_microseconds"],
            {"mean": "N/A", "p50": "N/A", "p95": "N/A", "p99": "N/A", "max": "N/A"},
        )

    def test_raw_and_summary_count_mismatch_is_rejected(self) -> None:
        rows = performance.read_benchbase_raw(io.StringIO(self.raw_text))
        summary = dict(self.summary)
        summary["Measured Requests"] = 3
        with self.assertRaisesRegex(performance.PerformanceDataError, "differs"):
            performance.analyze_benchbase(rows, summary)

    def test_benchbase_cli_can_write_json_and_flat_csv(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            raw = directory / "run.raw.csv"
            summary = directory / "run.summary.json"
            output_json = directory / "metrics.json"
            output_csv = directory / "metrics.csv"
            raw.write_text(self.raw_text, encoding="utf-8")
            summary.write_text(json.dumps(self.summary), encoding="utf-8")
            result = subprocess.run(
                [
                    "python3",
                    str(TOOL),
                    "analyze-benchbase",
                    "--raw-csv",
                    str(raw),
                    "--summary-json",
                    str(summary),
                    "--output-json",
                    str(output_json),
                    "--output-csv",
                    str(output_csv),
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(output_json.read_text())["measured_count"], 4)
            with output_csv.open(newline="") as stream:
                metric_rows = list(csv.DictReader(stream))
            self.assertIn(
                {"metric": "latency_microseconds.p95", "value": "40"}, metric_rows
            )


class TransactionOutcomeAnalysisTest(unittest.TestCase):
    def setUp(self) -> None:
        self.benchbase = {
            "measured_count": 4,
            "elapsed_time_nanoseconds": 2_000_000_000,
            "phase_ids": [1],
            "transaction_counts": {"NewOrder": 3, "Payment": 1},
            "transaction_types": {
                "NewOrder": {
                    "logical_invocation_count": 3,
                    "latency_microseconds": {
                        "mean": 20.0,
                        "p50": 20,
                        "p95": 30,
                        "p99": 30,
                        "max": 30,
                    },
                },
                "Payment": {
                    "logical_invocation_count": 1,
                    "latency_microseconds": {
                        "mean": 40.0,
                        "p50": 40,
                        "p95": 40,
                        "p99": 40,
                        "max": 40,
                    },
                },
            },
        }
        zero = {name: 0 for name in performance.OUTCOME_COUNTERS}
        self.document = {
            "kind": "transaction_outcomes",
            "benchmark": "tpcc",
            "scope": "measurement_window",
            "window_semantics": "fully_contained_logical_invocations",
            "elapsed_nanoseconds": 2_000_000_000,
            "measured_requests": 4,
            "phase_ids": [1],
            "transaction_types": {
                "NewOrder": {
                    **zero,
                    "logical_transactions_started": 3,
                    "successful_transactions": 2,
                    "terminal_aborted_transactions": 1,
                    "physical_attempts_started": 4,
                    "definitively_aborted_attempts": 2,
                    "retry_attempts": 1,
                    "retryable_failure_events": 1,
                },
                "Payment": {
                    **zero,
                    "logical_transactions_started": 1,
                    "successful_transactions": 1,
                    "physical_attempts_started": 1,
                },
                "OrderStatus": dict(zero),
                "Delivery": dict(zero),
                "StockLevel": dict(zero),
            },
        }
        self.document["totals"] = {
            name: sum(
                counters[name]
                for counters in self.document["transaction_types"].values()
            )
            for name in performance.OUTCOME_COUNTERS
        }

    def test_exact_window_algebra_rates_and_zero_type_roster(self) -> None:
        result = performance.analyze_transaction_outcomes(self.document, self.benchbase)
        self.assertEqual(result["totals"]["successful_transactions"], 3)
        self.assertEqual(result["totals"]["terminal_aborted_transactions_per_second"], 0.5)
        self.assertEqual(result["totals"]["failed_transactions_per_second"], 0.0)
        self.assertEqual(result["transaction_types"]["NewOrder"]["physical_attempts_per_second"], 2.0)
        self.assertEqual(result["transaction_types"]["OrderStatus"]["latency_microseconds"]["p95"], "N/A")
        self.assertEqual(result["new_order_per_minute"], 60.0)
        self.assertEqual(result["new_order_rate_label"], "NewOrder/min")

    def test_wrong_window_or_counter_algebra_fails_closed(self) -> None:
        document = json.loads(json.dumps(self.document))
        document["window_semantics"] = "whole_process"
        with self.assertRaisesRegex(performance.PerformanceDataError, "window semantics"):
            performance.analyze_transaction_outcomes(document, self.benchbase)

        document = json.loads(json.dumps(self.document))
        document["transaction_types"]["NewOrder"]["error_transactions"] = 1
        document["totals"]["error_transactions"] = 1
        with self.assertRaisesRegex(performance.PerformanceDataError, "logical outcome algebra"):
            performance.analyze_transaction_outcomes(document, self.benchbase)

    def test_retry_and_raw_count_mutations_fail_closed(self) -> None:
        document = json.loads(json.dumps(self.document))
        document["transaction_types"]["NewOrder"]["retry_different_events"] = 2
        document["totals"]["retry_different_events"] = 2
        with self.assertRaisesRegex(performance.PerformanceDataError, "retry failure event algebra"):
            performance.analyze_transaction_outcomes(document, self.benchbase)

        document = json.loads(json.dumps(self.document))
        document["measured_requests"] = 5
        with self.assertRaisesRegex(performance.PerformanceDataError, "measured request"):
            performance.analyze_transaction_outcomes(document, self.benchbase)

    def test_exhausted_retry_failure_count_includes_terminal_event(self) -> None:
        document = json.loads(json.dumps(self.document))
        new_order = document["transaction_types"]["NewOrder"]
        new_order["logical_transactions_started"] = 1
        new_order["successful_transactions"] = 0
        new_order["terminal_aborted_transactions"] = 1
        new_order["physical_attempts_started"] = 3
        new_order["definitively_aborted_attempts"] = 3
        new_order["retry_attempts"] = 2
        new_order["retryable_failure_events"] = 0
        new_order["retry_different_events"] = 3
        self.benchbase["measured_count"] = 2
        self.benchbase["transaction_counts"] = {"NewOrder": 1, "Payment": 1}
        self.benchbase["transaction_types"]["NewOrder"]["logical_invocation_count"] = 1
        document["measured_requests"] = 2
        document["totals"] = {
            name: sum(
                counters[name]
                for counters in document["transaction_types"].values()
            )
            for name in performance.OUTCOME_COUNTERS
        }

        result = performance.analyze_transaction_outcomes(document, self.benchbase)

        self.assertEqual(result["transaction_types"]["NewOrder"]["retry_attempts"], 2)
        self.assertEqual(result["transaction_types"]["NewOrder"]["retry_different_events"], 3)

    def test_outcome_cli_writes_machine_readable_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            outcomes = root / "run.outcomes.json"
            benchbase = root / "benchbase_metrics.json"
            output = root / "outcome_metrics.json"
            outcomes.write_text(json.dumps(self.document), encoding="utf-8")
            benchbase.write_text(json.dumps(self.benchbase), encoding="utf-8")
            result = subprocess.run(
                [
                    "python3",
                    str(TOOL),
                    "analyze-outcomes",
                    "--outcomes-json",
                    str(outcomes),
                    "--benchbase-metrics-json",
                    str(benchbase),
                    "--output-json",
                    str(output),
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            document = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(document["new_order_rate_label"], "NewOrder/min")
            self.assertEqual(document["transaction_types"]["Delivery"]["logical_transactions_started"], 0)


class RedisAnalysisTest(unittest.TestCase):
    def setUp(self) -> None:
        self.document = [
            redis_entry(
                "1000-0",
                '{"id":"tx-1"}',
                {
                    "status": "BEGIN",
                    "id": "tx-1",
                    "event_count": None,
                    "data_collections": None,
                    "ts_ms": 900,
                },
            ),
            redis_entry(
                "1010-0",
                '{"ycsb_key":1}',
                {
                    "before": None,
                    "after": {"ycsb_key": 1},
                    "source": {"ts_ms": 1000, "db": "benchbase", "table": "usertable"},
                    "op": "c",
                    "ts_ms": 1008,
                },
            ),
            redis_entry(
                "1020-0",
                '{"ycsb_key":1}',
                {
                    "before": {"ycsb_key": 1},
                    "after": {"ycsb_key": 1},
                    "source": {"ts_ms": 1000, "db": "benchbase", "table": "usertable"},
                    "op": "u",
                    "ts_ms": 1017,
                },
            ),
            redis_entry(
                "1030-0",
                '{"id":"tx-1"}',
                {
                    "status": "END",
                    "id": "tx-1",
                    "event_count": 2,
                    "data_collections": [],
                    "ts_ms": 1018,
                },
            ),
            redis_entry(
                "2000-0",
                '{"ycsb_key":2}',
                {
                    "before": None,
                    "after": {"ycsb_key": 2},
                    "source": {"ts_ms": 1900, "db": "benchbase", "table": "usertable"},
                    "op": "c",
                    "ts_ms": 1990,
                },
            ),
        ]

    def test_cdc_bytes_boundaries_and_two_lag_endpoints(self) -> None:
        result = performance.analyze_redis_xrange(
            self.document,
            window_after_stream_id="1000-0",
            window_through_stream_id="1030-0",
        )
        self.assertEqual(result["input_entry_count"], 5)
        self.assertGreater(result["input_logical_utf8_bytes"], result["logical_utf8_bytes"])
        self.assertEqual(result["entry_count"], 3)
        expected_bytes = sum(
            len(field.encode("utf-8")) + len(value.encode("utf-8"))
            for _, fields in performance.parse_xrange_entries(self.document[1:4])
            for field, value in fields
        )
        self.assertEqual(result["logical_utf8_bytes"], expected_bytes)
        self.assertEqual(result["cdc_row_count"], 2)
        self.assertEqual(result["cdc_row_counts_by_operation"], {"c": 1, "u": 1})
        self.assertEqual(result["cdc_row_counts_by_table"], {"benchbase.usertable": 2})
        self.assertEqual(result["begin_count"], 0)
        self.assertEqual(result["end_count"], 1)
        self.assertEqual(result["completed_transaction_count"], 0)
        self.assertEqual(result["cdc_delivery_lag_sample_count"], 2)
        self.assertEqual(
            result["cdc_delivery_lag_ms"],
            {"current": 3, "max": 3, "mean": 2.5, "p50": 3, "p95": 3, "p99": 3},
        )
        self.assertEqual(result["cdc_source_lag_sample_count"], 2)
        self.assertEqual(
            result["cdc_source_lag_ms"],
            {"current": 20, "max": 20, "mean": 15.0, "p50": 20, "p95": 20, "p99": 20},
        )

    def test_cdc_lag_is_na_when_window_has_no_rows(self) -> None:
        result = performance.analyze_redis_xrange(
            [self.document[0], self.document[3]],
            window_after_stream_id="1000-0",
            window_through_stream_id="1030-0",
        )
        expected = {
            "current": "N/A",
            "max": "N/A",
            "mean": "N/A",
            "p50": "N/A",
            "p95": "N/A",
            "p99": "N/A",
        }
        self.assertEqual(result["cdc_delivery_lag_sample_count"], 0)
        self.assertEqual(result["cdc_delivery_lag_ms"], expected)
        self.assertEqual(result["cdc_source_lag_sample_count"], 0)
        self.assertEqual(result["cdc_source_lag_ms"], expected)

    def test_exact_stream_id_window_prevents_same_millisecond_leakage(self) -> None:
        document = [
            redis_entry("1000-0", "boundary", {"status": "END", "id": "before"}),
            redis_entry(
                "1000-1",
                "row",
                {
                    "source": {"ts_ms": 999, "table": "usertable"},
                    "op": "c",
                    "ts_ms": 999,
                },
            ),
            redis_entry("1000-2", "boundary", {"status": "END", "id": "load"}),
            redis_entry(
                "1000-3",
                "row",
                {
                    "source": {"ts_ms": 999, "table": "usertable"},
                    "op": "u",
                    "ts_ms": 999,
                },
            ),
            redis_entry("1000-4", "boundary", {"status": "END", "id": "end"}),
        ]

        result = performance.analyze_redis_xrange(
            document,
            window_after_stream_id="1000-2",
            window_through_stream_id="1000-4",
        )

        self.assertEqual(result["entry_count"], 2)
        self.assertEqual(result["cdc_workload_row_count"], 1)
        self.assertEqual(result["cdc_row_counts_by_operation"], {"u": 1})
        self.assertEqual(
            result["selection_window"],
            {
                "bounded": True,
                "after_stream_id": "1000-2",
                "through_stream_id": "1000-4",
                "start_exclusive": True,
                "end_inclusive": True,
            },
        )

    def test_cdc_workload_metrics_exclude_lifecycle_fence_rows(self) -> None:
        document = list(self.document)
        document.append(
            redis_entry(
                "2010-0",
                '{"run_id":"run"}',
                {
                    "before": None,
                    "after": {"run_id": "run", "phase": "END_FENCE"},
                    "source": {
                        "ts_ms": 2000,
                        "db": "benchbase",
                        "table": "checker_run_fence",
                    },
                    "op": "c",
                    "ts_ms": 2005,
                },
            )
        )

        result = performance.analyze_redis_xrange(document)

        self.assertEqual(result["cdc_row_count"], 4)
        self.assertEqual(result["cdc_control_row_count"], 1)
        self.assertEqual(result["cdc_workload_row_count"], 3)
        self.assertEqual(result["cdc_workload_delivery_lag_sample_count"], 3)
        self.assertEqual(result["cdc_workload_source_lag_sample_count"], 3)
        self.assertEqual(result["cdc_workload_delivery_lag_ms"]["max"], 10)
        self.assertEqual(result["cdc_workload_source_lag_ms"]["max"], 100)

    def test_xrange_rejects_non_increasing_ids(self) -> None:
        with self.assertRaisesRegex(performance.PerformanceDataError, "strictly increasing"):
            performance.parse_xrange_entries([self.document[1], self.document[0]])


class ClientAnalysisTest(unittest.TestCase):
    def test_client_counts_by_kind_session_type_and_session_done_counters(self) -> None:
        document = [
            redis_entry("1-0", "payload", {"kind": "run_manifest"}),
            redis_entry(
                "2-0",
                "payload",
                {
                    "kind": "client_txn",
                    "session_id": "worker/b",
                    "txn_type": "ycsb_read",
                    "read_count": 2,
                    "reads": [{"event_seq": 0}, {"event_seq": 1}],
                },
            ),
            redis_entry(
                "3-0",
                "payload",
                {
                    "kind": "client_txn",
                    "session_id": "worker/a",
                    "txn_type": "ycsb_update",
                    "read_count": 0,
                    "reads": [],
                },
            ),
            redis_entry(
                "4-0",
                "payload",
                {
                    "kind": "session_done",
                    "session_id": "worker/a",
                    "final_session_seq": 10,
                    "logical_transactions_started": 10,
                    "successful_transactions": 8,
                    "physical_attempts_started": 11,
                    "published_transactions": 8,
                    "retry_attempts": 1,
                    "terminal_aborted_transactions": 2,
                    "definitively_aborted_attempts": 3,
                    "ambiguous_attempts": 0,
                },
            ),
            redis_entry(
                "5-0",
                "payload",
                {
                    "kind": "session_done",
                    "session_id": "worker/b",
                    "final_session_seq": 20,
                    "logical_transactions_started": 20,
                    "successful_transactions": 15,
                    "physical_attempts_started": 24,
                    "published_transactions": 15,
                    "retry_attempts": 4,
                    "terminal_aborted_transactions": 5,
                    "definitively_aborted_attempts": 6,
                    "ambiguous_attempts": 0,
                },
            ),
        ]
        result = performance.analyze_client_xrange(document)
        self.assertEqual(
            result["kind_counts"],
            {"client_txn": 2, "run_manifest": 1, "session_done": 2},
        )
        self.assertEqual(result["session_counts"], {"worker/a": 2, "worker/b": 2})
        self.assertEqual(
            result["client_transaction_counts_by_session"], {"worker/a": 1, "worker/b": 1}
        )
        self.assertEqual(
            result["transaction_type_counts"], {"ycsb_read": 1, "ycsb_update": 1}
        )
        self.assertEqual(
            result["transaction_type_counts_by_session"],
            {
                "worker/a": {"ycsb_update": 1},
                "worker/b": {"ycsb_read": 1},
            },
        )
        self.assertEqual(result["read_event_count"], 2)
        self.assertEqual(result["read_event_counts_by_session"], {"worker/a": 0, "worker/b": 2})
        self.assertEqual(result["session_done_count"], 2)
        self.assertEqual(
            result["session_done_counters"],
            {
                "final_session_seq": 30,
                "logical_transactions_started": 30,
                "successful_transactions": 23,
                "physical_attempts_started": 35,
                "published_transactions": 23,
                "retry_attempts": 5,
                "terminal_aborted_transactions": 7,
                "definitively_aborted_attempts": 9,
                "ambiguous_attempts": 0,
            },
        )


class ResourceSamplingTest(unittest.TestCase):
    def test_proc_snapshot_and_recursive_process_tree(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            proc_root = Path(directory_name)
            fixtures = {
                100: proc_stat(100, "root process", 1, 10, 5, 2),
                101: proc_stat(101, "child", 100, 6, 4, 3),
                102: proc_stat(102, "grandchild", 101, 2, 1, 4),
                200: proc_stat(200, "unrelated", 1, 100, 100, 20),
            }
            for pid, stat in fixtures.items():
                directory = proc_root / str(pid)
                directory.mkdir()
                (directory / "stat").write_text(stat, encoding="utf-8")
            snapshot = performance.read_proc_snapshot(proc_root)
        self.assertEqual(performance.process_tree_totals(snapshot, 100), (28, 9))
        self.assertIsNone(performance.process_tree_totals(snapshot, 999))

    def test_one_sample_records_proc_docker_and_explicit_unavailable_rows(self) -> None:
        snapshot = {
            100: (1, 20, 2),
            101: (100, 8, 3),
        }
        docker_values = {"redis": (12.5, 4096)}
        rows, next_ticks = performance.sample_resource_rows(
            123456,
            [("benchbase", 100), ("missing-process", 999)],
            [("redis", "redis"), ("missing-container", "absent")],
            previous_process_ticks={"benchbase": (18, 9.0)},
            monotonic_seconds=10.0,
            proc_snapshot=snapshot,
            clock_ticks_per_second=100,
            page_size=4096,
            docker_sampler=lambda names: {name: docker_values.get(name) for name in names},
        )
        self.assertEqual(
            rows[0],
            {
                "timestamp": 123456,
                "component": "benchbase",
                "cpu_percent": 10.0,
                "rss_bytes": 5 * 4096,
                "source": "proc",
            },
        )
        self.assertEqual(next_ticks, {"benchbase": (28, 10.0)})
        self.assertEqual(rows[1]["source"], "unavailable")
        self.assertEqual(rows[1]["cpu_percent"], "")
        self.assertEqual(rows[2]["source"], "docker")
        self.assertEqual(rows[3]["source"], "unavailable")

    def test_docker_stats_memory_units(self) -> None:
        cpu, memory = performance.parse_docker_stats(
            '{"CPUPerc":"12.50%","MemUsage":"1.5MiB / 2GiB"}\n'
        )
        self.assertEqual(cpu, 12.5)
        self.assertEqual(memory, 1_572_864)
        self.assertEqual(performance.parse_memory_bytes("2.5MB / 1GB"), 2_500_000)

    def test_resource_cli_is_bounded_and_emits_absent_component(self) -> None:
        result = subprocess.run(
            [
                "python3",
                str(TOOL),
                "sample-resources",
                "--process",
                "absent:999999999",
                "--interval-seconds",
                "1",
                "--duration-seconds",
                "0",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = list(csv.DictReader(io.StringIO(result.stdout)))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["component"], "absent")
        self.assertEqual(rows[0]["source"], "unavailable")

    def test_existing_stop_file_ends_before_first_sample(self) -> None:
        with tempfile.NamedTemporaryFile() as stop_file:
            output = io.StringIO()
            count = performance.sample_resources(
                output,
                [("unused", 999999999)],
                [],
                interval_seconds=10,
                duration_seconds=100,
                stop_file=Path(stop_file.name),
            )
        self.assertEqual(count, 0)
        self.assertEqual(output.getvalue(), "timestamp,component,cpu_percent,rss_bytes,source\n")


class TrialSummaryTest(unittest.TestCase):
    def test_wide_trials_use_median_range_mad_and_documented_overhead(self) -> None:
        rows = [
            {
                "mode": "baseline",
                "trial": "1",
                "throughput": "100",
                "latency_p95": "10",
                "redis_bytes": "N/A",
                "optional": "",
            },
            {
                "mode": "baseline",
                "trial": "2",
                "throughput": "120",
                "latency_p95": "12",
                "redis_bytes": "N/A",
                "optional": "",
            },
            {
                "mode": "capture",
                "trial": "1",
                "throughput": "80",
                "latency_p95": "14",
                "redis_bytes": "1000",
                "optional": "",
            },
            {
                "mode": "capture",
                "trial": "2",
                "throughput": "90",
                "latency_p95": "16",
                "redis_bytes": "1100",
                "optional": "",
            },
        ]
        summary = performance.summarize_trials(rows, baseline_mode="baseline")
        by_key = {(row["mode"], row["metric"]): row for row in summary}
        baseline_throughput = by_key[("baseline", "throughput")]
        self.assertEqual(
            [
                baseline_throughput["median"],
                baseline_throughput["min"],
                baseline_throughput["max"],
                baseline_throughput["mad"],
            ],
            [110.0, 100.0, 120.0, 10.0],
        )
        self.assertEqual(baseline_throughput["overhead_vs_baseline_percent"], 0.0)
        capture_throughput = by_key[("capture", "throughput")]
        self.assertAlmostEqual(
            capture_throughput["overhead_vs_baseline_percent"],
            (110.0 - 85.0) / 110.0 * 100.0,
        )
        self.assertEqual(capture_throughput["overhead_direction"], "decrease_from_baseline")
        capture_latency = by_key[("capture", "latency_p95")]
        self.assertAlmostEqual(
            capture_latency["overhead_vs_baseline_percent"],
            (15.0 - 11.0) / 11.0 * 100.0,
        )
        self.assertEqual(capture_latency["overhead_direction"], "increase_from_baseline")
        self.assertEqual(by_key[("baseline", "redis_bytes")]["median"], "N/A")
        self.assertEqual(
            by_key[("capture", "redis_bytes")]["overhead_vs_baseline_percent"], "N/A"
        )
        self.assertEqual(by_key[("baseline", "optional")]["median"], "")
        self.assertEqual(by_key[("capture", "optional")]["overhead_vs_baseline_percent"], "")

    def test_long_form_trials_are_supported(self) -> None:
        rows = [
            {"mode": "baseline", "trial": "1", "metric": "throughput", "value": "10"},
            {"mode": "baseline", "trial": "2", "metric": "throughput", "value": "12"},
            {"mode": "capture", "trial": "1", "metric": "throughput", "value": "9"},
            {"mode": "capture", "trial": "2", "metric": "throughput", "value": "11"},
        ]
        summary = performance.summarize_trials(rows, baseline_mode="baseline")
        self.assertEqual(len(summary), 2)
        self.assertEqual(summary[0]["median"], 11.0)
        self.assertEqual(summary[1]["median"], 10.0)

    def test_trials_without_baseline_keep_descriptive_statistics(self) -> None:
        rows = [
            {"mode": "benchbase_uninstrumented", "trial": "1", "throughput": "100"},
            {"mode": "benchbase_uninstrumented", "trial": "2", "throughput": "120"},
            {
                "mode": "full_e2e",
                "trial": "1",
                "throughput": "90",
            },
            {
                "mode": "full_e2e",
                "trial": "2",
                "throughput": "110",
            },
        ]
        summary = performance.summarize_trials(rows)
        by_mode = {row["mode"]: row for row in summary}
        self.assertEqual(by_mode["benchbase_uninstrumented"]["median"], 110.0)
        self.assertEqual(by_mode["full_e2e"]["median"], 100.0)
        for row in summary:
            self.assertEqual(row["overhead_vs_baseline_percent"], "")
            self.assertEqual(row["overhead_direction"], "")
            self.assertEqual(row["overhead_formula"], "")

    def test_explicit_absent_trial_baseline_is_rejected(self) -> None:
        rows = [{"mode": "benchbase_uninstrumented", "trial": "1", "throughput": "100"}]
        with self.assertRaisesRegex(
            performance.PerformanceDataError, "baseline mode 'missing' is absent"
        ):
            performance.summarize_trials(rows, baseline_mode="missing")

    def test_summarize_command_allows_baseline_free_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            input_csv = root / "trials.csv"
            output_csv = root / "summary.csv"
            output_json = root / "summary.json"
            input_csv.write_text(
                "mode,trial,throughput\n"
                "benchbase_uninstrumented,1,100\n"
                "full_e2e,1,90\n",
                encoding="utf-8",
            )
            result = subprocess.run(
                [
                    "python3",
                    str(TOOL),
                    "summarize-trials",
                    "--input-csv",
                    str(input_csv),
                    "--output-csv",
                    str(output_csv),
                    "--output-json",
                    str(output_json),
                ],
                capture_output=True,
                check=False,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            document = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertIsNone(document["baseline_mode"])
            self.assertEqual(document["overhead_formulas"], {})
            with output_csv.open(encoding="utf-8", newline="") as stream:
                for row in csv.DictReader(stream):
                    self.assertEqual(row["overhead_vs_baseline_percent"], "")

    def test_summarize_command_rejects_legacy_stage_vocabulary(self) -> None:
        for legacy_arguments in (
            ("--baseline-stage", "baseline"),
            ("--stage-column", "stage"),
        ):
            with self.subTest(arguments=legacy_arguments):
                result = subprocess.run(
                    [
                        "python3",
                        str(TOOL),
                        "summarize-trials",
                        "--input-csv",
                        "/does/not/matter.csv",
                        *legacy_arguments,
                    ],
                    capture_output=True,
                    check=False,
                    text=True,
                )
                self.assertEqual(result.returncode, 2)
                self.assertIn("unrecognized arguments", result.stderr)

    def test_bounded_copy_is_limited_and_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            accepted = root / "accepted.bin"
            result = subprocess.run(
                [
                    "python3",
                    str(TOOL),
                    "bounded-copy",
                    "--output",
                    str(accepted),
                    "--max-bytes",
                    "4",
                ],
                input=b"abcd",
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(accepted.read_bytes(), b"abcd")

            rejected = root / "rejected.bin"
            result = subprocess.run(
                [
                    "python3",
                    str(TOOL),
                    "bounded-copy",
                    "--output",
                    str(rejected),
                    "--max-bytes",
                    "4",
                ],
                input=b"abcde",
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(rejected.exists())

            existing = root / "existing.bin"
            existing.write_bytes(b"keep")
            result = subprocess.run(
                [
                    "python3",
                    str(TOOL),
                    "bounded-copy",
                    "--output",
                    str(existing),
                    "--max-bytes",
                    "4",
                ],
                input=b"new",
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(existing.read_bytes(), b"keep")


if __name__ == "__main__":
    unittest.main()
