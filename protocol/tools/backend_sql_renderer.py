#!/usr/bin/env python3
"""Dynamic backend SQL renderer registry for logical loader/bootstrap plans."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Protocol


class BackendSqlRendererError(ValueError):
    pass


class BackendSqlRenderer(Protocol):
    backend: str

    def loader_queries(self, adapter: dict[str, Any]) -> list[dict[str, Any]]: ...

    def bootstrap_queries(
        self, adapter: dict[str, Any], logical_plan: dict[str, Any]
    ) -> list[dict[str, Any]]: ...


class BackendSqlRendererRegistry:
    def __init__(self, renderers: dict[str, BackendSqlRenderer]):
        self._renderers = renderers

    @classmethod
    def discover(cls, directory: Path) -> "BackendSqlRendererRegistry":
        renderers: dict[str, BackendSqlRenderer] = {}
        for path in sorted(directory.glob("*.py")):
            if path.name.startswith("_"):
                continue
            spec = importlib.util.spec_from_file_location(
                f"db_monitoring_backend_sql_{path.stem}", path
            )
            if spec is None or spec.loader is None:
                raise BackendSqlRendererError(f"cannot load backend SQL renderer: {path}")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            renderer = getattr(module, "RENDERER", None)
            backend = getattr(renderer, "backend", None)
            if not isinstance(backend, str) or not backend:
                raise BackendSqlRendererError(f"renderer lacks backend name: {path}")
            if backend in renderers:
                raise BackendSqlRendererError(f"duplicate backend SQL renderer: {backend}")
            renderers[backend] = renderer
        return cls(renderers)

    def require(self, backend: str) -> BackendSqlRenderer:
        renderer = self._renderers.get(backend)
        if renderer is None:
            raise BackendSqlRendererError(f"no SQL renderer registered for backend: {backend}")
        return renderer

    @property
    def backends(self) -> tuple[str, ...]:
        return tuple(sorted(self._renderers))
