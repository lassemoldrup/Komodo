from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "registered_performance_result.py"


def load_module():
    spec = importlib.util.spec_from_file_location("registered_performance_result", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RegisteredPerformanceResultTest(unittest.TestCase):
    def test_monitor_peak_memory_and_gc_cost_are_reported(self) -> None:
        module = load_module()
        monitor = {
            "limits": {"checker": {"history_gc": True}},
            "process": {"max_rss_kib": 2048, "user_cpu_nanoseconds": 2000000000,
                        "system_cpu_nanoseconds": 1000000000},
            "checker": {"consumed_transactions": 1000,
                        "consume_total_nanoseconds": 1000000000,
                        "history_gc_total_nanoseconds": 500000000,
                        "engine": {"history_logical_bytes": 0, "reclaimed_history_bytes": 1234}},
        }
        result = module.monitor_memory_and_gc_metrics(monitor, True)
        self.assertEqual(result["monitor_peak_rss_mib"], 2)
        self.assertEqual(result["monitor_cpu_seconds"], 3)
        self.assertEqual(result["monitor_cpu_us_per_consumed_transaction"], 3000)
        self.assertEqual(result["monitor_checker_total_seconds_including_gc"], 1.5)
        self.assertEqual(result["monitor_checker_us_per_consumed_transaction_including_gc"], 1500)
        self.assertEqual(result["monitor_reclaimed_history_bytes"], 1234)
        with self.assertRaisesRegex(ValueError, "history_gc differs"):
            module.monitor_memory_and_gc_metrics(monitor, False)
        monitor["process"].update({
            "workload_rss_sampling_enabled": True,
            "workload_rss_sampling_started": True,
            "workload_rss_sampling_valid": True,
            "workload_sampled_peak_rss_kib": 1024,
            "rss_sample_interval_ms": 100,
            "workload_rss_samples": 4,
            "workload_rss_sample_errors": 0,
        })
        sampled = module.monitor_memory_and_gc_metrics(monitor, True, True)
        self.assertEqual(sampled["monitor_workload_sampled_peak_rss_mib"], 1)
        self.assertEqual(sampled["monitor_peak_rss_mib"], 2)
        monitor["process"]["workload_rss_sample_errors"] = 1
        with self.assertRaisesRegex(ValueError, "incomplete or failed"):
            module.monitor_memory_and_gc_metrics(monitor, True, True)

    @staticmethod
    def cdc_entry(stream_id: str, field: object, value: object) -> list[object]:
        return [
            stream_id,
            [
                json.dumps(field, separators=(",", ":")),
                json.dumps(value, separators=(",", ":")),
            ],
        ]

    def test_operations_per_transaction_is_parsed_from_declared_parameters(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "workload-parameters.txt"
            path.write_text(
                "BENCHBASE_MEASUREMENT_OPERATIONS_PER_TRANSACTION=7\n"
                "BENCHBASE_MEASUREMENT_TERMINALS=4\n",
                encoding="utf-8",
            )
            parameters = module.workload_parameters(path)
            self.assertEqual(module.declared_operations_per_transaction(parameters), 7)

    def test_operations_per_transaction_rejects_noncanonical_values(self) -> None:
        module = load_module()
        with self.assertRaisesRegex(ValueError, "operations per transaction is invalid"):
            module.declared_operations_per_transaction(
                {module.OPERATIONS_PER_TRANSACTION: "0"}
            )

    def test_completed_fence_boundaries_match_full_and_compact_evidence(self) -> None:
        module = load_module()
        reconciliation = {
            "load_finished_cdc_stream_id": "1000-1",
            "end_fence_cdc_stream_id": "2000-1",
        }
        full_stream = [
            self.cdc_entry("1000-1", {"id": "load"}, {"status": "END"}),
            self.cdc_entry("1500-0", {"id": "work"}, {"status": "BEGIN"}),
            self.cdc_entry(
                "1500-1", {"id": "work"}, {"source": {}, "op": "u", "after": {}}
            ),
            self.cdc_entry("1500-2", {"id": "work"}, {"status": "END"}),
            self.cdc_entry("1900-0", {"id": "end"}, {"status": "BEGIN"}),
            self.cdc_entry(
                "1900-1",
                {"id": "end"},
                {
                    "source": {},
                    "op": "c",
                    "after": {"run_id": "run", "phase": "END_FENCE"},
                },
            ),
            self.cdc_entry("2000-1", {"id": "end"}, {"status": "END"}),
        ]
        compact_boundaries = [full_stream[0], full_stream[-1]]
        self.assertEqual(
            module.fence_window(full_stream, reconciliation),
            module.fence_window(compact_boundaries, reconciliation),
        )
        counts = module.cdc_window_counts(full_stream, "1000-1", "2000-1")
        self.assertEqual(counts["entries"], 6)
        self.assertEqual(counts["rows"], 2)
        self.assertEqual(counts["control_rows"], 1)
        self.assertEqual(counts["transaction_begins"], 2)
        self.assertEqual(counts["transaction_ends"], 2)
        self.assertGreater(counts["logical_utf8_bytes"], 0)
        self.assertEqual(module.compact_transport_entries(1, 1), counts["entries"])

    def test_completed_fence_boundaries_fail_closed(self) -> None:
        module = load_module()
        entries = [
            self.cdc_entry("1000-1", {}, {}),
            self.cdc_entry("2000-1", {}, {}),
        ]
        with self.assertRaisesRegex(ValueError, "absent or duplicated"):
            module.fence_window(
                entries,
                {
                    "load_finished_cdc_stream_id": "1000-2",
                    "end_fence_cdc_stream_id": "2000-1",
                },
            )
        with self.assertRaisesRegex(ValueError, "order is invalid"):
            module.fence_window(
                entries,
                {
                    "load_finished_cdc_stream_id": "2000-1",
                    "end_fence_cdc_stream_id": "1000-1",
                },
            )
        with self.assertRaisesRegex(ValueError, "boundary is absent"):
            module.cdc_window_counts(entries, "1000-0", "2000-1")

    def test_compact_transport_count_uses_contiguous_debezium_framing(self) -> None:
        module = load_module()
        self.assertEqual(module.compact_transport_entries(10, 35), 58)
        with self.assertRaisesRegex(ValueError, "transaction count is negative"):
            module.compact_transport_entries(-1, 35)
        with self.assertRaisesRegex(ValueError, "raw row count is negative"):
            module.compact_transport_entries(10, -1)


if __name__ == "__main__":
    unittest.main()
