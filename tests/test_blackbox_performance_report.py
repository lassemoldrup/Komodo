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
    def write_trial(self, root: Path, stage: str, position: int) -> None:
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
            "warmup_seconds": 1,
            "requested_duration_seconds": 2,
            "random_seed": 20260818,
            "configured_repetitions": 1,
            "redis_maxmemory_bytes": 8589934592,
            "monitor_max_retained_identity_bytes": 2147483648,
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
        (trial / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
        (trial / "benchbase_metrics.json").write_text(
            json.dumps(benchbase), encoding="utf-8"
        )
        (trial / "bootstrap_manifest.json").write_text(
            json.dumps(bootstrap), encoding="utf-8"
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
                    "joiner": {"max_retained_identity_bytes": 2147483648}
                },
                "joiner": {
                    "missing_client_head_wait_current_nanoseconds": 2_000_000,
                    "missing_client_head_wait_max_nanoseconds": 5_000_000,
                    "join_buffer_high_water_bytes": 4096,
                    "joined_workload_transactions": 100,
                    "joined_read_events": 50,
                    "joined_write_events": 50,
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
