#!/usr/bin/env python3

from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "protocol" / "tools" / "verify_protocol.py"
SPEC = importlib.util.spec_from_file_location("verify_protocol", TOOL)
if SPEC is None or SPEC.loader is None:  # pragma: no cover - import setup invariant
    raise RuntimeError(f"cannot load {TOOL}")
VERIFY_PROTOCOL = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFY_PROTOCOL)


class SessionAttemptProgressTest(unittest.TestCase):
    def test_accepts_terminal_abort_gap_and_stable_retry_surplus(self) -> None:
        VERIFY_PROTOCOL.validate_session_attempt_progress(
            [
                {"session_seq": 1, "txn_id": 2},
                {"session_seq": 3, "txn_id": 4},
            ],
            "test session",
        )

    def test_rejects_marker_behind_logical_progress(self) -> None:
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "txn_id is behind session_seq"
        ):
            VERIFY_PROTOCOL.validate_session_attempt_progress(
                [
                    {"session_seq": 1, "txn_id": 1},
                    {"session_seq": 3, "txn_id": 2},
                ],
                "test session",
            )

    def test_rejects_decreasing_retry_surplus(self) -> None:
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "retry surplus decreased"
        ):
            VERIFY_PROTOCOL.validate_session_attempt_progress(
                [
                    {"session_seq": 1, "txn_id": 3},
                    {"session_seq": 3, "txn_id": 4},
                ],
                "test session",
            )

    def test_accepts_empty_and_sufficient_remaining_attempt_histories(self) -> None:
        VERIFY_PROTOCOL.validate_final_attempt_feasibility([], 0, 0, "empty")
        VERIFY_PROTOCOL.validate_final_attempt_feasibility(
            [{"session_seq": 1, "txn_id": 2}], 3, 4, "test session"
        )

    def test_rejects_attempts_claimed_by_an_empty_session(self) -> None:
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "after logical completion"
        ):
            VERIFY_PROTOCOL.validate_final_attempt_feasibility([], 0, 1, "empty")

    def test_rejects_attempts_after_the_final_published_logical_transaction(self) -> None:
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "after logical completion"
        ):
            VERIFY_PROTOCOL.validate_final_attempt_feasibility(
                [{"session_seq": 1, "txn_id": 1}], 1, 2, "test session"
            )

    def test_rejects_too_few_attempts_for_remaining_logical_transactions(self) -> None:
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "too few remaining attempts"
        ):
            VERIFY_PROTOCOL.validate_final_attempt_feasibility(
                [{"session_seq": 1, "txn_id": 2}], 3, 3, "test session"
            )


class ProtocolArtifactAlignmentTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.validator, cls.schemas = VERIFY_PROTOCOL.load_schema_validator()
        cls.contracts = VERIFY_PROTOCOL.load_adapter_contracts(cls.schemas)
        cls.backends = VERIFY_PROTOCOL.load_backend_contracts(cls.schemas)

    @staticmethod
    def example(name: str) -> dict[str, object]:
        return VERIFY_PROTOCOL.load_json(VERIFY_PROTOCOL.EXAMPLE_ROOT / name)

    def test_schema_rejects_noncanonical_table_identifier(self) -> None:
        manifest = self.example("run-manifest.json")
        manifest["application_table_allowlist"] = ["BenchBase.usertable"]
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "schema validation failed"
        ):
            VERIFY_PROTOCOL.verify_record_schema(self.validator, manifest, "manifest")

    def test_wire_names_are_bounded_but_not_globally_enumerated(self) -> None:
        manifest = self.example("run-manifest.json")
        manifest["benchmark"] = "FutureAdapter_1"
        VERIFY_PROTOCOL.verify_record_schema(self.validator, manifest, "manifest")
        manifest["benchmark"] = "1invalid"
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "schema validation failed"
        ):
            VERIFY_PROTOCOL.verify_record_schema(self.validator, manifest, "manifest")

        client = self.example("client-txn-second.json")
        client["txn_type"] = "FutureTransaction_1"
        VERIFY_PROTOCOL.verify_record_schema(self.validator, client, "client")
        client["txn_type"] = "A" * 65
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "schema validation failed"
        ):
            VERIFY_PROTOCOL.verify_record_schema(self.validator, client, "client")

    def test_selected_adapter_rejects_unknown_transaction_membership(self) -> None:
        records = VERIFY_PROTOCOL.verify_schemas_and_examples()
        records["client_txn"][0]["txn_type"] = "NewOrder"
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "not owned by selected adapter"
        ):
            VERIFY_PROTOCOL.verify_cross_record_semantics(records, self.contracts)

    def test_selected_adapter_rejects_table_roster_mismatch(self) -> None:
        records = VERIFY_PROTOCOL.verify_schemas_and_examples()
        records["run_manifest"][0]["application_table_allowlist"] = [
            "benchbase.warehouse"
        ]
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "table roster differs"
        ):
            VERIFY_PROTOCOL.verify_cross_record_semantics(records, self.contracts)

    def test_tpcc_contract_has_exact_names_roster_and_capture_matrix(self) -> None:
        contract = self.contracts["tpcc"]
        self.assertEqual(
            tuple(contract["transaction_types"]), VERIFY_PROTOCOL.TPCC_TRANSACTION_TYPES
        )
        self.assertEqual(
            tuple(contract["application_tables"]), VERIFY_PROTOCOL.TPCC_APPLICATION_TABLES
        )
        classifications = [item["classification"] for item in contract["result_sets"]]
        self.assertEqual(classifications.count("snapshot"), 13)
        self.assertEqual(classifications.count("local"), 2)
        self.assertEqual(classifications.count("omit"), 4)
        aggregates = [
            item
            for item in contract["result_sets"]
            if item["result_shape"] == "aggregate_scalar"
        ]
        self.assertEqual(len(aggregates), 2)
        self.assertTrue(all(item["classification"] == "omit" for item in aggregates))

    def test_tpcc_contract_rejects_capture_matrix_drift(self) -> None:
        contract = copy.deepcopy(self.contracts["tpcc"])
        contract["result_sets"][0]["classification"] = "local"
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "capture/omit matrix"
        ):
            VERIFY_PROTOCOL.verify_tpcc_contract(contract)

    def test_tpcc_contract_freezes_history_key_and_empty_new_order_value(self) -> None:
        tables = {
            table["name"]: table for table in self.contracts["tpcc"]["tables"]
        }
        self.assertEqual(tables["benchbase.history"]["key_columns"], ["h_id"])
        self.assertEqual(
            tables["benchbase.history"]["columns"][0],
            {
                "name": "h_id",
                "sql_type": "BIGINT",
                "nullable": False,
                "canonical_type": "i64",
                "generated": "auto_increment",
            },
        )
        self.assertEqual(tables["benchbase.new_order"]["value_columns"], [])

    def test_tpcc_executable_vectors_cover_every_table_and_boundary(self) -> None:
        VERIFY_PROTOCOL.verify_tpcc_executable_vectors(self.contracts, self.schemas)

    def test_tpcc_adapter_record_examples_are_contract_validated(self) -> None:
        self.assertEqual(
            VERIFY_PROTOCOL.verify_adapter_record_examples(
                self.contracts, self.schemas
            ),
            2,
        )

    def test_monitor_readiness_is_weaker_than_end_to_end_readiness(self) -> None:
        ycsb = VERIFY_PROTOCOL.adapter_runtime_readiness(self.contracts["ycsb"])
        self.assertTrue(ycsb["monitor_ready"])
        self.assertTrue(ycsb["end_to_end_ready"])
        tpcc = VERIFY_PROTOCOL.adapter_runtime_readiness(self.contracts["tpcc"])
        self.assertTrue(tpcc["bootstrap_ready"])
        self.assertTrue(tpcc["monitor_ready"])
        self.assertFalse(tpcc["client_ready"])
        self.assertFalse(tpcc["end_to_end_ready"])

    def test_mariadb_backend_contract_freezes_session_and_schema_defaults(self) -> None:
        contract = self.backends["mariadb"]
        self.assertEqual(contract["server_defaults"]["sql_mode"], VERIFY_PROTOCOL.MARIADB_SQL_MODE)
        self.assertEqual(
            contract["session_requirements"]["set_names"],
            "SET NAMES utf8mb4 COLLATE utf8mb4_bin",
        )
        self.assertEqual(
            contract["application_table_requirements"]["char_read_semantics"],
            "pad_to_declared_length",
        )

    def test_golden_key_encoder_rejects_noncanonical_identifiers(self) -> None:
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "canonical lowercase identifier"
        ):
            VERIFY_PROTOCOL.encode_ycsb_key("BenchBase", "usertable", 1)

    def test_schema_and_verifier_reject_consistency_outcome_as_failure_code(self) -> None:
        failed = self.example("run-failed.json")
        failed["reason_code"] = "PASS"
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "schema validation failed"
        ):
            VERIFY_PROTOCOL.verify_record_schema(self.validator, failed, "RunFailed")

        records = VERIFY_PROTOCOL.verify_schemas_and_examples()
        records["run_failed"][0]["reason_code"] = "PASS"
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "undocumented"
        ):
            VERIFY_PROTOCOL.verify_cross_record_semantics(records)

    def test_session_and_isolation_mismatches_have_distinct_stable_codes(self) -> None:
        codes = VERIFY_PROTOCOL.documented_failure_codes()
        self.assertIn("SESSION_CONTRACT_MISMATCH", codes)
        self.assertIn("ISOLATION_MISMATCH", codes)

    def test_verifier_rejects_duplicate_session_done(self) -> None:
        records = VERIFY_PROTOCOL.verify_schemas_and_examples()
        records["session_done"].append(copy.deepcopy(records["session_done"][0]))
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "duplicate SessionDone"
        ):
            VERIFY_PROTOCOL.verify_cross_record_semantics(records)

    def test_cross_record_verifier_rejects_impossible_final_attempt_history(self) -> None:
        records = VERIFY_PROTOCOL.verify_schemas_and_examples()
        empty_done = next(
            record
            for record in records["session_done"]
            if record["logical_transactions_started"] == 0
        )
        empty_done["physical_attempts_started"] = 1
        empty_done["definitively_aborted_attempts"] = 1
        empty_done["retry_attempts"] = 1
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "after logical completion"
        ):
            VERIFY_PROTOCOL.verify_cross_record_semantics(records)

        records = VERIFY_PROTOCOL.verify_schemas_and_examples()
        nonempty_done = next(
            record
            for record in records["session_done"]
            if record["logical_transactions_started"] != 0
        )
        nonempty_done["physical_attempts_started"] += 1
        nonempty_done["definitively_aborted_attempts"] += 1
        nonempty_done["retry_attempts"] += 1
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "after logical completion"
        ):
            VERIFY_PROTOCOL.verify_cross_record_semantics(records)

    def test_schema_rejects_clean_verdict_without_validated_bootstrap(self) -> None:
        status = self.example("monitor-status.json")
        status["reconciliation"]["bootstrap_manifest_validated"] = False
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "schema validation failed"
        ):
            VERIFY_PROTOCOL.verify_record_schema(self.validator, status, "MonitorStatus")

    def test_verifier_rejects_malformed_final_reconciliation_algebra(self) -> None:
        mutations = (
            ("evidence_reconciled", False, "lacks reconciliation"),
            ("bootstrap_manifest_validated", False, "validated bootstrap"),
            ("end_fence_seen", False, "END_FENCE"),
            ("bootstrap_checker_transactions", 0, "bootstrap counts differ"),
            ("phase_anchor_transactions", 1, "phase-anchor count"),
            ("checker_consumed_transactions", 4, "checker transaction algebra"),
        )
        for field, value, message in mutations:
            with self.subTest(field=field):
                status = self.example("monitor-status.json")
                status["reconciliation"][field] = value
                with self.assertRaisesRegex(VERIFY_PROTOCOL.VerificationError, message):
                    VERIFY_PROTOCOL.verify_final_verdict(status, 2)

        status = self.example("monitor-status.json")
        status["assembled_cdc_transactions"] = 5
        with self.assertRaisesRegex(
            VERIFY_PROTOCOL.VerificationError, "assembled CDC counter"
        ):
            VERIFY_PROTOCOL.verify_final_verdict(status, 2)

if __name__ == "__main__":
    unittest.main()
