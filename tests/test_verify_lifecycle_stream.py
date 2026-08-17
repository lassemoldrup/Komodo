#!/usr/bin/env python3

from __future__ import annotations

import copy
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
import verify_lifecycle_stream as verifier  # noqa: E402


RUN_ID = "084cc5a1-3cbe-4fef-bb5c-43559a1c13a0"
FIXTURE = (
    ROOT
    / "monitor/tests/fixtures/mariadb_debezium_3_6_1_full_lifecycle.json"
)


def load_fixture() -> list[object]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def envelope_at(entries: list[object], index: int) -> dict[str, object]:
    return json.loads(entries[index][1][1])


def set_envelope(
    entries: list[object], index: int, envelope: dict[str, object]
) -> None:
    entries[index][1][1] = json.dumps(envelope, separators=(",", ":"))


def renumber(entries: list[object]) -> None:
    for index, entry in enumerate(entries, start=1):
        entry[0] = f"{index}-0"


class LifecycleStreamVerifierTest(unittest.TestCase):
    def assert_rejected(self, entries: list[object]) -> None:
        with self.assertRaises(verifier.StreamError):
            verifier.verify_stream(entries, RUN_ID, "mariadb-main")

    def test_full_captured_lifecycle_is_accepted(self) -> None:
        self.assertEqual(
            len(verifier.verify_stream(load_fixture(), RUN_ID, "mariadb-main")),
            11,
        )

    def test_duplicate_and_reordered_snapshot_tables_are_rejected(self) -> None:
        duplicate = load_fixture()
        duplicate.insert(2, copy.deepcopy(duplicate[1]))
        renumber(duplicate)
        self.assert_rejected(duplicate)

        reordered = load_fixture()
        reordered[1][1], reordered[2][1] = reordered[2][1], reordered[1][1]
        self.assert_rejected(reordered)

    def test_missing_snapshot_prefix_is_rejected(self) -> None:
        self.assert_rejected(load_fixture()[10:])

    def test_stringified_snapshot_number_is_rejected(self) -> None:
        entries = load_fixture()
        envelope = envelope_at(entries, 0)
        envelope["ts_ms"] = str(envelope["ts_ms"])
        set_envelope(entries, 0, envelope)
        self.assert_rejected(entries)

    def test_malformed_nested_schema_column_is_rejected(self) -> None:
        entries = load_fixture()
        envelope = envelope_at(entries, 7)
        envelope["tableChanges"][0]["table"]["columns"][0]["optional"] = "false"
        set_envelope(entries, 7, envelope)
        self.assert_rejected(entries)


if __name__ == "__main__":
    unittest.main()
