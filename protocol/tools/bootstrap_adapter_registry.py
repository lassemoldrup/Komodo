#!/usr/bin/env python3
"""Discovery boundary for benchmark-owned bootstrap query and scan adapters."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Protocol


class BootstrapAdapterError(ValueError):
    pass


class BootstrapAdapter(Protocol):
    benchmark: str

    def build_queries(self, contract: dict[str, Any]) -> dict[str, Any]: ...

    def encode_scan(
        self,
        contract: dict[str, Any],
        path: Path,
        *,
        max_rows: int,
        max_input_bytes: int,
        max_canonical_bytes: int,
        options: dict[str, Any],
    ) -> tuple[list[tuple[bytes, bytes]], dict[str, int]]: ...


class BootstrapAdapterRegistry:
    def __init__(self, adapters: dict[str, BootstrapAdapter]):
        self._adapters = adapters

    @classmethod
    def discover(cls, directory: Path) -> "BootstrapAdapterRegistry":
        adapters: dict[str, BootstrapAdapter] = {}
        for path in sorted(directory.glob("*.py")):
            if path.name.startswith("_"):
                continue
            module_name = f"db_monitoring_bootstrap_adapter_{path.stem}"
            spec = importlib.util.spec_from_file_location(module_name, path)
            if spec is None or spec.loader is None:
                raise BootstrapAdapterError(f"cannot load bootstrap adapter module: {path}")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            adapter = getattr(module, "ADAPTER", None)
            benchmark = getattr(adapter, "benchmark", None)
            if not isinstance(benchmark, str) or not benchmark:
                raise BootstrapAdapterError(f"bootstrap adapter lacks benchmark name: {path}")
            if benchmark in adapters:
                raise BootstrapAdapterError(f"duplicate bootstrap adapter: {benchmark}")
            adapters[benchmark] = adapter
        return cls(adapters)

    def require(self, benchmark: str) -> BootstrapAdapter:
        adapter = self._adapters.get(benchmark)
        if adapter is None:
            raise BootstrapAdapterError(f"no bootstrap adapter registered for: {benchmark}")
        return adapter

    @property
    def benchmarks(self) -> tuple[str, ...]:
        return tuple(sorted(self._adapters))
