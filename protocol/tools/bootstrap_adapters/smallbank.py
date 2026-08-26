#!/usr/bin/env python3
"""SmallBank bootstrap registration using the strict typed row codec."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from bootstrap_adapter_registry import BootstrapAdapterError
from smallbank_codec import SmallBankCodec, SmallBankCodecError, scan_json_lines


class SmallBankBootstrapAdapter:
    benchmark = "smallbank"

    def build_queries(self, contract: dict[str, Any]) -> dict[str, Any]:
        try:
            return {
                "benchmark": self.benchmark,
                "format": "smallbank_jsonl",
                "queries": SmallBankCodec(contract).query_plan(),
            }
        except SmallBankCodecError as error:
            raise BootstrapAdapterError(f"SmallBank query codec failed: {error}") from error

    def encode_scan(
        self,
        contract: dict[str, Any],
        path: Path,
        *,
        max_rows: int,
        max_input_bytes: int,
        max_canonical_bytes: int,
        options: dict[str, Any],
    ) -> tuple[list[tuple[bytes, bytes]], dict[str, int]]:
        del options
        try:
            return scan_json_lines(
                SmallBankCodec(contract),
                path,
                max_rows,
                max_input_bytes,
                max_canonical_bytes,
            )
        except (OSError, UnicodeError, json.JSONDecodeError, SmallBankCodecError) as error:
            raise BootstrapAdapterError(f"SmallBank scan codec failed: {error}") from error


ADAPTER = SmallBankBootstrapAdapter()
