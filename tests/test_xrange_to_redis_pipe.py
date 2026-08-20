#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import io
import json
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "scripts" / "xrange_to_redis_pipe.py"
SPEC = importlib.util.spec_from_file_location("xrange_to_redis_pipe", TOOL)
assert SPEC is not None and SPEC.loader is not None
replay = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(replay)


class CountingOutput:
    def __init__(self) -> None:
        self.total = 0
        self.largest_write = 0

    def write(self, value: bytes) -> int:
        self.total += len(value)
        self.largest_write = max(self.largest_write, len(value))
        return len(value)


class XrangeToRedisPipeTest(unittest.TestCase):
    def test_emits_explicit_ids_and_utf8_resp_lengths(self) -> None:
        document = [
            ["1-0", ["payload", "é🙂", "名", "值"]],
            ["1-2", ["empty", ""]],
        ]
        output = io.BytesIO()

        self.assertEqual(replay.write_replay(document, "測試", output), 2)

        self.assertEqual(
            output.getvalue(),
            b"*7\r\n"
            b"$4\r\nXADD\r\n"
            b"$6\r\n\xe6\xb8\xac\xe8\xa9\xa6\r\n"
            b"$3\r\n1-0\r\n"
            b"$7\r\npayload\r\n"
            b"$6\r\n\xc3\xa9\xf0\x9f\x99\x82\r\n"
            b"$3\r\n\xe5\x90\x8d\r\n"
            b"$3\r\n\xe5\x80\xbc\r\n"
            b"*5\r\n"
            b"$4\r\nXADD\r\n"
            b"$6\r\n\xe6\xb8\xac\xe8\xa9\xa6\r\n"
            b"$3\r\n1-2\r\n"
            b"$5\r\nempty\r\n"
            b"$0\r\n\r\n",
        )

    def test_empty_xrange_is_a_valid_noop(self) -> None:
        output = io.BytesIO()
        self.assertEqual(replay.write_replay([], "cdc", output), 0)
        self.assertEqual(output.getvalue(), b"")

    def test_complete_capture_is_validated_before_output(self) -> None:
        output = io.BytesIO()
        document = [
            ["1-0", ["key", "value"]],
            ["2-0", ["field", "value", "field-only"]],
        ]

        with self.assertRaisesRegex(replay.XrangeReplayError, "unmatched"):
            replay.write_replay(document, "cdc", output)

        self.assertEqual(output.getvalue(), b"")

    def test_rejects_malformed_entry_and_field_shapes(self) -> None:
        cases = (
            ({}, "root is not an array"),
            (["not-an-entry"], r"not \[stream_id, fields\]"),
            ([["1-0", []]], "no field/value pair"),
            ([["1-0", ["field", "value", "extra"]]], "unmatched"),
            ([["1-0", {"field": "value"}]], "not a flat array"),
            ([["1-0", [1, "value"]]], "field 0 is not text"),
            ([["1-0", ["field", None]]], "value 0 is not text"),
        )
        for document, message in cases:
            with self.subTest(document=document):
                with self.assertRaisesRegex(replay.XrangeReplayError, message):
                    replay.write_replay(document, "cdc", io.BytesIO())

    def test_rejects_noncanonical_or_unsupported_stream_ids(self) -> None:
        invalid_ids = (
            "0-0",
            "01-0",
            "1-00",
            "+1-0",
            "1",
            "1-2-3",
            f"{1 << 64}-0",
            f"1-{1 << 64}",
        )
        for stream_id in invalid_ids:
            with self.subTest(stream_id=stream_id):
                with self.assertRaises(replay.XrangeReplayError):
                    replay.write_replay(
                        [[stream_id, ["key", "value"]]], "cdc", io.BytesIO()
                    )

    def test_rejects_duplicate_and_regressing_stream_ids(self) -> None:
        for second_id in ("1-0", "1-9"):
            with self.subTest(second_id=second_id):
                document = [
                    ["2-0", ["key", "first"]],
                    [second_id, ["key", "second"]],
                ]
                with self.assertRaisesRegex(replay.XrangeReplayError, "strictly increasing"):
                    replay.write_replay(document, "cdc", io.BytesIO())

    def test_rejects_non_utf8_unicode_and_empty_stream_name(self) -> None:
        with self.assertRaisesRegex(replay.XrangeReplayError, "valid UTF-8"):
            replay.write_replay([["1-0", ["key", "\ud800"]]], "cdc", io.BytesIO())
        with self.assertRaisesRegex(replay.XrangeReplayError, "stream name is empty"):
            replay.write_replay([["1-0", ["key", "value"]]], "", io.BytesIO())

    def test_output_writes_are_bounded_by_one_argument(self) -> None:
        value = "x" * 1024
        document = [
            [f"{entry_number + 1}-0", ["payload", value]]
            for entry_number in range(5000)
        ]
        output = CountingOutput()

        self.assertEqual(replay.write_replay(document, "cdc", output), 5000)
        self.assertGreater(output.total, 5_000_000)
        self.assertEqual(output.largest_write, 1024)

    def test_cli_accepts_stdin_and_emits_only_resp_on_stdout(self) -> None:
        document = [["9-7", ["payload", "value"]]]
        result = subprocess.run(
            ["python3", str(TOOL), "--input-json", "-", "--stream", "client"],
            input=json.dumps(document).encode("utf-8"),
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(result.stderr, b"")
        self.assertIn(b"$3\r\n9-7\r\n", result.stdout)
        self.assertIn(b"$6\r\nclient\r\n", result.stdout)

    def test_cli_failure_emits_no_partial_resp(self) -> None:
        document = [
            ["1-0", ["key", "value"]],
            ["1-0", ["key", "duplicate"]],
        ]
        result = subprocess.run(
            ["python3", str(TOOL), "--input-json", "-", "--stream", "cdc"],
            input=json.dumps(document).encode("utf-8"),
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b"")
        self.assertIn(b"strictly increasing", result.stderr)

    def test_json_loader_rejects_nonstandard_constants(self) -> None:
        with self.assertRaisesRegex(replay.XrangeReplayError, "nonstandard constant"):
            replay.load_xrange_json(io.StringIO('[["1-0",["field",NaN]]]'))


if __name__ == "__main__":
    unittest.main()
