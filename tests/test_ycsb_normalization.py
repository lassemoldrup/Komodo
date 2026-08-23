from __future__ import annotations

import base64
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "protocol" / "tools"
sys.path.insert(0, str(TOOLS))

from bootstrap_adapters.ycsb import (
    YcsbBootstrapAdapter,
)
from bootstrap_adapter_registry import BootstrapAdapterError


class YcsbNormalizationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = json.loads(
            (ROOT / "protocol" / "adapters" / "ycsb.json").read_text(encoding="utf-8")
        )
        self.keys = json.loads(
            (ROOT / "protocol" / "golden" / "ycsb_keys.json").read_text(encoding="utf-8")
        )
        self.values = json.loads(
            (ROOT / "protocol" / "golden" / "ycsb_values.json").read_text(encoding="utf-8")
        )

    @staticmethod
    def token(value: str | None) -> str:
        if value is None:
            return "N"
        return "P" + base64.b64encode(value.encode("utf-8")).decode("ascii")

    def encode(self, lines: list[str]):
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as scan:
            scan.write("\n".join(lines))
            if lines:
                scan.write("\n")
            scan.flush()
            return YcsbBootstrapAdapter().encode_scan(
                self.contract,
                Path(scan.name),
                max_rows=100,
                max_input_bytes=1_000_000,
                max_canonical_bytes=1_000_000,
                options={"schema": "benchbase", "table": "usertable"},
            )

    def test_bootstrap_uses_shared_boundary_and_value_goldens(self) -> None:
        selected_keys = self.keys["vectors"]
        selected_values = self.values["vectors"]
        lines = [
            "\t".join(
                [
                    str(key["key"]),
                    *[self.token(value) for value in vector["fields"]],
                ]
            )
            for key, vector in zip(selected_keys, selected_values, strict=True)
        ]

        rows, counts = self.encode(lines)

        expected = sorted(
            (
                base64.b64decode(key["base64"], validate=True),
                base64.b64decode(value["base64"], validate=True),
            )
            for key, value in zip(selected_keys, selected_values, strict=True)
        )
        self.assertEqual(rows, expected)
        self.assertEqual(counts, {"benchbase.usertable": 4})

    def test_overlength_malformed_utf8_and_noncanonical_base64_fail_closed(self) -> None:
        valid_tail = ["N"] * 9
        cases = (
            ["0", "P" + base64.b64encode(b"x" * 101).decode("ascii"), *valid_tail],
            ["0", "P" + base64.b64encode(b"\x80").decode("ascii"), *valid_tail],
            ["0", "PZE==", *valid_tail],
        )
        for columns in cases:
            with self.subTest(columns=columns[1]):
                with self.assertRaises(BootstrapAdapterError):
                    self.encode(["\t".join(columns)])


if __name__ == "__main__":
    unittest.main()
