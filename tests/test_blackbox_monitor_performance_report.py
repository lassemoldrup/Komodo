#!/usr/bin/env python3

from __future__ import annotations

import csv
import importlib.util
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "scripts" / "blackbox_monitor_performance_report.py"
SPEC = importlib.util.spec_from_file_location("blackbox_monitor_performance_report", TOOL)
assert SPEC is not None and SPEC.loader is not None
report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(report)


class MonitorPerformanceReportTest(unittest.TestCase):
    seed = 20260818
    source_transactions = 1000

    def write_trial(
        self,
        root: Path,
        *,
        stage: str,
        repetition: int,
        position: int,
        roster: tuple[str, ...] = report.STAGES,
        repetitions: int = 3,
        wall_nanoseconds: int = 10_000_000_000,
        user_cpu_nanoseconds: int = 4_000_000_000,
        system_cpu_nanoseconds: int = 1_000_000_000,
        max_rss_kib: int = 262144,
    ) -> Path:
        trial = root / "trials" / f"repetition-{repetition:02d}" / f"{position:02d}-{stage}"
        trial.mkdir(parents=True)
        run_id = "11111111-2222-3333-4444-555555555555"
        metadata = {
            "kind": "monitor_performance_trial",
            "stage": stage,
            "repetition": repetition,
            "stage_order_position": position,
            "configured_repetitions": repetitions,
            "configured_stages": list(roster),
            "random_seed": self.seed,
            "trial_id": f"trial-{repetition}-{position}",
            "run_id": run_id,
            "source_trial_dir": "/media/newhd/yifan/db-monitoring-builds/source-trial",
            "source_requested_duration_seconds": 120,
            "source_trial_manifest_sha256": "a" * 64,
            "source_cdc_sha256": "b" * 64,
            "source_client_sha256": "c" * 64,
            "source_cdc_entries": 3003,
            "source_client_entries": 2005,
            "source_workload_transactions": self.source_transactions,
            "redis_maxmemory_bytes": 8 * 1024**3,
            "monitor_max_pending_client_bytes": 3 * 1024**3,
            "monitor_max_retained_identity_bytes": 2 * 1024**3,
            "si_monitoring_head": "d" * 40,
            "co_checker_head": "e" * 40,
            "si_monitoring_dirty": False,
            "co_checker_dirty": False,
        }
        audited = stage == "assembly_join_check"
        monitor = {
            "kind": "performance_measurement",
            "run_id": run_id,
            "processing_mode": "audited" if audited else "join-only",
            "completed": True,
            "completion_status": "completed",
            "evidence_reconciled": True,
            "monitor_wall_time_nanoseconds": wall_nanoseconds,
            "process": {
                "user_cpu_nanoseconds": user_cpu_nanoseconds,
                "system_cpu_nanoseconds": system_cpu_nanoseconds,
                "max_rss_kib": max_rss_kib,
            },
            "limits": {
                "joiner": {
                    "max_pending_client_bytes": 3 * 1024**3,
                    "max_retained_identity_bytes": 2 * 1024**3,
                }
            },
            "joiner": {"joined_workload_transactions": self.source_transactions},
            "checker": {
                "bootstrap_transactions": self.source_transactions // 10 if audited else 0,
                "workload_transactions": self.source_transactions if audited else 0,
                "phase_anchor_transactions": 4 if audited else 0,
                "read_events": self.source_transactions // 2 if audited else 0,
                "write_events": self.source_transactions // 2 if audited else 0,
                "consumed_transactions": self.source_transactions + 12 if audited else 0,
                "mapped_identity_records": self.source_transactions * 2 if audited else 0,
                "mapped_identity_bytes": self.source_transactions * 16 if audited else 0,
                "next_commit_seq": self.source_transactions + 12 if audited else 0,
                "consume_total_nanoseconds": 500_000_000 if audited else 0,
                "consume_max_nanoseconds": 1_000_000 if audited else 0,
            },
            "reconciliation": {
                "evidence_reconciled": True,
                "joined_workload_transactions": self.source_transactions,
            },
            "diagnostics": [],
        }
        if audited:
            monitor["outcome"] = "PASS"
        (trial / "metadata.json").write_text(
            json.dumps(metadata, separators=(",", ":")) + "\n", encoding="utf-8"
        )
        (trial / "monitor_metrics.json").write_text(
            json.dumps(monitor, separators=(",", ":")) + "\n", encoding="utf-8"
        )
        return trial

    def write_complete_roster(self, root: Path) -> None:
        wall_times = {
            "assembly_join": (10_000_000_000, 8_000_000_000, 12_000_000_000),
            "assembly_join_check": (8_000_000_000, 8_000_000_000, 8_000_000_000),
        }
        for repetition in range(1, 4):
            offset = (self.seed + repetition - 1) % len(report.STAGES)
            for position in range(1, len(report.STAGES) + 1):
                stage = report.STAGES[(offset + position - 1) % len(report.STAGES)]
                self.write_trial(
                    root,
                    stage=stage,
                    repetition=repetition,
                    position=position,
                    wall_nanoseconds=wall_times[stage][repetition - 1],
                )

    @staticmethod
    def mutate(path: Path, mutator: Callable[[dict[str, Any]], None]) -> None:
        document = json.loads(path.read_text(encoding="utf-8"))
        mutator(document)
        path.write_text(json.dumps(document) + "\n", encoding="utf-8")

    def test_writes_raw_summary_and_concise_headline_with_negative_loss(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_complete_roster(root)
            report.write_reports(root)

            expected = {
                "raw_monitor_trials.csv",
                "monitor_summary_statistics.csv",
                "monitor_summary_statistics.json",
                "monitor_headline_results.csv",
                "monitor_headline_results.md",
            }
            self.assertTrue(all((root / name).is_file() for name in expected))

            with (root / "raw_monitor_trials.csv").open(encoding="utf-8", newline="") as stream:
                raw = list(csv.DictReader(stream))
            self.assertEqual(len(raw), 6)
            self.assertEqual(
                [(row["repetition"], row["stage_order_position"], row["stage"]) for row in raw],
                [
                    ("1", "1", "assembly_join"),
                    ("1", "2", "assembly_join_check"),
                    ("2", "1", "assembly_join_check"),
                    ("2", "2", "assembly_join"),
                    ("3", "1", "assembly_join"),
                    ("3", "2", "assembly_join_check"),
                ],
            )
            self.assertEqual(raw[0]["monitor_wall_seconds"], "10")
            self.assertEqual(raw[0]["throughput_transactions_per_second"], "100")
            self.assertEqual(raw[0]["process_cpu_seconds"], "5")
            self.assertEqual(raw[0]["transactions_per_cpu_second"], "200")
            self.assertEqual(raw[0]["max_rss_mib"], "256")

            with (root / "monitor_summary_statistics.csv").open(
                encoding="utf-8", newline=""
            ) as stream:
                summaries = list(csv.DictReader(stream))
            self.assertEqual(len(summaries), 2 * len(report.METRICS))
            join_throughput = next(
                row
                for row in summaries
                if row["stage"] == "assembly_join"
                and row["metric"] == "throughput_transactions_per_second"
            )
            self.assertEqual(join_throughput["median"], "100")
            self.assertEqual(join_throughput["min"], "83.33333333333333333333333333")
            self.assertEqual(join_throughput["max"], "125")
            self.assertEqual(join_throughput["mad"], "16.66666666666666666666666667")

            document = json.loads(
                (root / "monitor_summary_statistics.json").read_text(encoding="utf-8")
            )
            self.assertEqual(document["kind"], "monitor_performance_summary")
            self.assertEqual(document["configured_repetitions"], 3)
            self.assertEqual(document["source_corpus"]["source_workload_transactions"], 1000)

            with (root / "monitor_headline_results.csv").open(
                encoding="utf-8", newline=""
            ) as stream:
                headline = list(csv.DictReader(stream))
            self.assertEqual([row["mode"] for row in headline], ["Assembly/join", "Full checking"])
            self.assertEqual(headline[0]["throughput_loss_vs_join_only_percent"], "")
            self.assertEqual(headline[1]["throughput_transactions_per_second"], "125")
            self.assertEqual(headline[1]["throughput_loss_vs_join_only_percent"], "-25")

            markdown = (root / "monitor_headline_results.md").read_text(encoding="utf-8")
            self.assertIn("| Assembly/join | 1,000 | 10.000 | 100.0 |", markdown)
            self.assertIn("| Full checking | 1,000 | 8.000 | 125.0 |", markdown)
            self.assertIn("| -25.00% |", markdown)

    def test_single_selected_mode_has_no_comparison(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for repetition in range(1, 3):
                self.write_trial(
                    root,
                    stage="assembly_join_check",
                    repetition=repetition,
                    position=1,
                    roster=("assembly_join_check",),
                    repetitions=2,
                )
            report.write_reports(root)
            with (root / "monitor_headline_results.csv").open(
                encoding="utf-8", newline=""
            ) as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["mode"], "Full checking")
            self.assertEqual(rows[0]["throughput_loss_vs_join_only_percent"], "")
            self.assertIn("| Full checking |", (root / "monitor_headline_results.md").read_text())
            self.assertIn("| — |", (root / "monitor_headline_results.md").read_text())

    def test_rejects_invalid_roster_completeness_and_rotation(self) -> None:
        cases: tuple[tuple[str, Callable[[Path], None], str], ...] = (
            (
                "unknown stage",
                lambda root: self.mutate(
                    next(root.rglob("metadata.json")),
                    lambda value: value["configured_stages"].append("unknown"),
                ),
                "unknown monitor stage",
            ),
            (
                "duplicate stage",
                lambda root: self.mutate(
                    next(root.rglob("metadata.json")),
                    lambda value: value["configured_stages"].append("assembly_join_check"),
                ),
                "duplicates",
            ),
            (
                "truncated repetitions",
                lambda root: (root / "trials" / "repetition-03" / "01-assembly_join").rename(
                    root / "removed-trial"
                ),
                "trial count",
            ),
            (
                "duplicate position",
                lambda root: self.mutate(
                    root
                    / "trials"
                    / "repetition-01"
                    / "02-assembly_join_check"
                    / "metadata.json",
                    lambda value: value.__setitem__("stage_order_position", 1),
                ),
                "positions",
            ),
            (
                "wrong rotation",
                lambda root: (
                    self.mutate(
                        root / "trials" / "repetition-02" / "01-assembly_join_check" / "metadata.json",
                        lambda value: value.__setitem__("stage_order_position", 2),
                    ),
                    self.mutate(
                        root / "trials" / "repetition-02" / "02-assembly_join" / "metadata.json",
                        lambda value: value.__setitem__("stage_order_position", 1),
                    ),
                ),
                "rotation",
            ),
        )
        for name, mutate, message in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.write_complete_roster(root)
                mutate(root)
                with self.assertRaisesRegex(report.MonitorReportError, message):
                    report.collect_trials(root)

    def test_rejects_corpus_capacity_and_head_invariant_mismatches(self) -> None:
        for field, replacement in (
            ("source_trial_dir", "/media/newhd/yifan/db-monitoring-builds/another-source"),
            ("source_requested_duration_seconds", 121),
            ("source_trial_manifest_sha256", "f" * 64),
            ("source_cdc_sha256", "f" * 64),
            ("source_client_sha256", "f" * 64),
            ("source_cdc_entries", 3004),
            ("source_client_entries", 2006),
            ("source_workload_transactions", 1001),
            ("redis_maxmemory_bytes", 16 * 1024**3),
            ("monitor_max_pending_client_bytes", 4 * 1024**3),
            ("monitor_max_retained_identity_bytes", 3 * 1024**3),
            ("si_monitoring_head", "f" * 40),
            ("co_checker_head", "f" * 40),
        ):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.write_complete_roster(root)
                path = root / "trials" / "repetition-01" / "01-assembly_join" / "metadata.json"
                self.mutate(path, lambda value, field=field, replacement=replacement: value.__setitem__(field, replacement))
                with self.assertRaisesRegex(report.MonitorReportError, field):
                    report.collect_trials(root)

    def test_rejects_dirty_or_malformed_metadata(self) -> None:
        cases = (
            ("wrong kind", lambda value: value.__setitem__("kind", "performance_trial"), "kind"),
            ("dirty", lambda value: value.__setitem__("si_monitoring_dirty", True), "dirty worktree"),
            ("bad digest", lambda value: value.__setitem__("source_cdc_sha256", "A" * 64), "digest"),
            ("zero source", lambda value: value.__setitem__("source_workload_transactions", 0), "less than 1"),
            ("missing field", lambda value: value.pop("random_seed"), "missing required"),
        )
        for name, mutate, message in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                trial = self.write_trial(
                    root,
                    stage="assembly_join",
                    repetition=1,
                    position=1,
                    roster=("assembly_join",),
                    repetitions=1,
                )
                self.mutate(trial / "metadata.json", mutate)
                with self.assertRaisesRegex(report.MonitorReportError, message):
                    report.collect_trials(root)

    def test_rejects_incomplete_unreconciled_or_diagnostic_monitor(self) -> None:
        cases = (
            ("incomplete", lambda value: value.__setitem__("completed", False), "did not complete"),
            (
                "failed status",
                lambda value: value.__setitem__("completion_status", "failed"),
                "failed completion",
            ),
            (
                "unreconciled",
                lambda value: value.__setitem__("evidence_reconciled", False),
                "did not reconcile",
            ),
            (
                "diagnostic",
                lambda value: value["diagnostics"].append({"code": "INTERNAL_ERROR"}),
                "contains diagnostics",
            ),
            ("wrong run", lambda value: value.__setitem__("run_id", "another"), "run_id differs"),
            (
                "wrong mode",
                lambda value: value.__setitem__("processing_mode", "audited"),
                "processing_mode",
            ),
        )
        for name, mutate, message in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                trial = self.write_trial(
                    root,
                    stage="assembly_join",
                    repetition=1,
                    position=1,
                    roster=("assembly_join",),
                    repetitions=1,
                )
                self.mutate(trial / "monitor_metrics.json", mutate)
                with self.assertRaisesRegex(report.MonitorReportError, message):
                    report.collect_trials(root)

    def test_rejects_join_only_checker_activity_and_outcome(self) -> None:
        cases = (
            (
                "checker workload",
                lambda value: value["checker"].__setitem__("workload_transactions", 1),
                "invoked the checker",
            ),
            ("outcome", lambda value: value.__setitem__("outcome", "PASS"), "contain an outcome"),
        )
        for name, mutate, message in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                trial = self.write_trial(
                    root,
                    stage="assembly_join",
                    repetition=1,
                    position=1,
                    roster=("assembly_join",),
                    repetitions=1,
                )
                self.mutate(trial / "monitor_metrics.json", mutate)
                with self.assertRaisesRegex(report.MonitorReportError, message):
                    report.collect_trials(root)

    def test_rejects_nonpassing_or_mismatched_audited_counts(self) -> None:
        cases = (
            ("non-pass", lambda value: value.__setitem__("outcome", "VIOLATION"), "not PASS"),
            (
                "checker mismatch",
                lambda value: value["checker"].__setitem__("workload_transactions", 999),
                "differs from joined",
            ),
            (
                "joined mismatch",
                lambda value: value["joiner"].__setitem__("joined_workload_transactions", 999),
                "differs from source_workload_transactions",
            ),
            (
                "reconciliation mismatch",
                lambda value: value["reconciliation"].__setitem__("joined_workload_transactions", 999),
                "reconciliation workload",
            ),
        )
        for name, mutate, message in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                trial = self.write_trial(
                    root,
                    stage="assembly_join_check",
                    repetition=1,
                    position=1,
                    roster=("assembly_join_check",),
                    repetitions=1,
                )
                self.mutate(trial / "monitor_metrics.json", mutate)
                with self.assertRaisesRegex(report.MonitorReportError, message):
                    report.collect_trials(root)

    def test_rejects_invalid_wall_cpu_rss_or_limit_measurement(self) -> None:
        cases = (
            (
                "zero wall",
                lambda value: value.__setitem__("monitor_wall_time_nanoseconds", 0),
                "less than 1",
            ),
            (
                "zero cpu",
                lambda value: (
                    value["process"].__setitem__("user_cpu_nanoseconds", 0),
                    value["process"].__setitem__("system_cpu_nanoseconds", 0),
                ),
                "CPU time is zero",
            ),
            (
                "zero rss",
                lambda value: value["process"].__setitem__("max_rss_kib", 0),
                "less than 1",
            ),
            (
                "limit mismatch",
                lambda value: value["limits"]["joiner"].__setitem__(
                    "max_pending_client_bytes", 1
                ),
                "differs from trial metadata",
            ),
        )
        for name, mutate, message in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                trial = self.write_trial(
                    root,
                    stage="assembly_join",
                    repetition=1,
                    position=1,
                    roster=("assembly_join",),
                    repetitions=1,
                )
                self.mutate(trial / "monitor_metrics.json", mutate)
                with self.assertRaisesRegex(report.MonitorReportError, message):
                    report.collect_trials(root)

    def test_missing_or_malformed_artifacts_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(report.MonitorReportError, "trial directory is missing"):
                report.collect_trials(root)

            trial = self.write_trial(
                root,
                stage="assembly_join",
                repetition=1,
                position=1,
                roster=("assembly_join",),
                repetitions=1,
            )
            (trial / "monitor_metrics.json").write_text("{", encoding="utf-8")
            with self.assertRaisesRegex(report.MonitorReportError, "valid JSON"):
                report.collect_trials(root)


if __name__ == "__main__":
    unittest.main()
