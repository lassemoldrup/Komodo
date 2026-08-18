#!/usr/bin/env python3

from __future__ import annotations

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


if __name__ == "__main__":
    unittest.main()
