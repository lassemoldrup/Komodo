#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "backends" / "yugabytedb_ysql_telemetry.py"
SPEC = importlib.util.spec_from_file_location("yugabytedb_ysql_telemetry", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
telemetry = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(telemetry)


class YugabyteTelemetryTest(unittest.TestCase):
    def test_prometheus_parser_keeps_selected_series_and_labels(self) -> None:
        document = """
# HELP ignored ignored
intentsdb_rocksdb_db_iter_bytes_read{table_id="t1",table_name="usertable"} 12 1000
intentsdb_rocksdb_db_iter_bytes_read{table_id="t2",table_name="session_txn"} 7 1000
unrelated_metric 999 1000
"""
        series = telemetry.parse_prometheus(document)
        self.assertEqual(len(series), 2)
        self.assertEqual(telemetry.aggregate_series(series), {
            "intentsdb_rocksdb_db_iter_bytes_read": 19.0
        })
        self.assertEqual(series[0]["labels"]["table_id"], "t1")

    def test_json_parser_preserves_tablet_identity(self) -> None:
        document = json.dumps(
            [
                {
                    "type": "tablet",
                    "id": "tablet-a",
                    "attributes": {
                        "table_id": "table-a",
                        "table_name": "usertable",
                    },
                    "metrics": [
                        {
                            "name": "intentsdb_rocksdb_db_iter_bytes_read",
                            "value": 41,
                        },
                        {
                            "name": "log_append_latency",
                            "total_count": 7,
                            "total_sum": 19,
                        },
                        {"name": "unrelated_metric", "value": 99},
                    ],
                }
            ]
        )
        series = telemetry.parse_json_metrics(document)
        self.assertEqual(len(series), 3)
        self.assertEqual(series[0]["labels"]["metric_id"], "tablet-a")
        self.assertEqual(series[0]["labels"]["table_name"], "usertable")
        self.assertEqual(
            telemetry.aggregate_series(series),
            {
                "intentsdb_rocksdb_db_iter_bytes_read": 41.0,
                "log_append_latency_count": 7.0,
                "log_append_latency_sum": 19.0,
            },
        )

    def test_summary_reports_aggregate_and_per_tablet_deltas(self) -> None:
        samples = [
            {
                "aggregates": {"intentsdb_rocksdb_bytes_read": 10},
                "series": [
                    {
                        "name": "intentsdb_rocksdb_bytes_read",
                        "labels": {"metric_id": "tablet-a"},
                        "value": 10,
                    }
                ],
            },
            {
                "aggregates": {"intentsdb_rocksdb_bytes_read": 34},
                "series": [
                    {
                        "name": "intentsdb_rocksdb_bytes_read",
                        "labels": {"metric_id": "tablet-a"},
                        "value": 34,
                    }
                ],
            },
        ]
        summary = telemetry.summarize_samples(samples)
        self.assertEqual(
            summary["metrics"]["intentsdb_rocksdb_bytes_read"]["delta"], 24
        )
        self.assertEqual(summary["series"][0]["labels"]["metric_id"], "tablet-a")
        self.assertEqual(summary["series"][0]["delta"], 24)

    def test_varz_parser_is_strictly_allowlisted(self) -> None:
        flags = telemetry.parse_varz(
            "--cdc_enable_intra_transactional_before_image=true\n"
            "--cdc_send_null_before_image_if_not_exists=false\n"
            "--ysql_hba_conf_csv=secret\n"
        )
        self.assertEqual(
            flags,
            {
                "cdc_enable_intra_transactional_before_image": "true",
                "cdc_send_null_before_image_if_not_exists": "false",
            },
        )


if __name__ == "__main__":
    unittest.main()
