#!/usr/bin/env python3
"""TPC-C bootstrap adapter using the shared strict typed row codec."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bootstrap_adapter_registry import BootstrapAdapterError
from tpcc_codec import CodecError, TpccCodec, bootstrap_select_queries, scan_json_lines


class TpccBootstrapAdapter:
    benchmark = "tpcc"

    def build_queries(self, contract: dict[str, Any]) -> dict[str, Any]:
        try:
            return {
                "benchmark": self.benchmark,
                "format": "tpcc_jsonl",
                "queries": bootstrap_select_queries(TpccCodec(contract)),
            }
        except CodecError as error:
            raise BootstrapAdapterError(f"TPC-C query codec failed: {error.code}: {error}") from error

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
                TpccCodec(contract),
                path,
                max_rows,
                max_input_bytes,
                max_canonical_bytes,
            )
        except CodecError as error:
            raise BootstrapAdapterError(f"TPC-C scan codec failed: {error.code}: {error}") from error


ADAPTER = TpccBootstrapAdapter()
