from __future__ import annotations

import json
import unittest
from pathlib import Path


WORKSPACE = Path(__file__).resolve().parents[2]
MATRIX = Path(__file__).with_name("failure_injection_coverage.json")

EXPECTED_CASES = {
    "begin_missing", "begin_duplicate", "begin_reordered", "end_missing",
    "end_duplicate_or_mismatched", "row_missing", "row_duplicate",
    "row_reordered", "total_count_wrong", "per_table_count_wrong",
    "total_ordinal_wrong", "collection_ordinal_wrong", "non_full_identity",
    "before_image_missing", "after_image_missing", "null_fallback_rejected",
    "malformed_image", "marker_missing", "marker_multiple", "marker_malformed",
    "marker_non_final", "marker_duplicate", "marker_chain_broken", "slot_wrong",
    "plugin_wrong", "lsn_type_wrong", "publication_wrong", "table_roster_wrong",
    "connector_source_wrong", "connector_version_wrong", "connector_task_wrong",
    "streaming_mode_wrong", "schema_wrong", "search_path_wrong",
    "startup_attestation_missing_or_wrong", "final_attestation_missing_or_drifted",
    "pre_cdc_rows", "snapshot_rows", "extra_writer", "live_ddl",
    "workload_after_end_fence", "definitive_retry", "terminal_abort",
    "rollback_failure", "connection_loss", "commit_ambiguity",
    "post_commit_redis_failure", "dropped_client_entry", "dropped_cdc_entry",
    "redis_oom", "redis_noeviction_wrong", "connector_stall",
    "resource_high_water", "session_done_missing", "fence_missing",
    "attestation_missing", "equivalent_backend_traces",
    "operational_invalid_overrides_violation",
    "violation_final_only_after_reconciliation",
}


class FailureInjectionCoverageTest(unittest.TestCase):
    def test_every_required_case_has_an_executable_test_anchor(self) -> None:
        document = json.loads(MATRIX.read_text(encoding="utf-8"))
        self.assertEqual(document["kind"], "yugabytedb_ysql_failure_injection_coverage")
        cases = document["cases"]
        self.assertEqual({case["id"] for case in cases}, EXPECTED_CASES)
        self.assertEqual(len(cases), len(EXPECTED_CASES))
        self.assertEqual(
            {case["layer"] for case in cases},
            {"unit", "fixture", "integration", "live"},
        )
        for case in cases:
            with self.subTest(case=case["id"]):
                path = WORKSPACE / case["path"]
                self.assertTrue(path.is_file(), path)
                self.assertIn(case["anchor"], path.read_text(encoding="utf-8"))
                self.assertIn(
                    case["expected"],
                    {
                        "INVALID",
                        "VIOLATION",
                        "NO_COMMITTED_EVIDENCE",
                        "IDENTICAL_NORMALIZED_INPUT",
                        "PASS_OR_VIOLATION_AFTER_RECONCILIATION",
                    },
                )


if __name__ == "__main__":
    unittest.main()
