#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "protocol" / "tools" / "bootstrap_adapter_registry.py"
sys.path.insert(0, str(MODULE_PATH.parent))
SPEC = importlib.util.spec_from_file_location("bootstrap_adapter_registry", MODULE_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover
    raise RuntimeError(f"cannot load {MODULE_PATH}")
REGISTRY_MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REGISTRY_MODULE)


class BootstrapAdapterRegistryTest(unittest.TestCase):
    def test_discovers_registered_adapters_without_coordinator_branches(self) -> None:
        registry = REGISTRY_MODULE.BootstrapAdapterRegistry.discover(
            ROOT / "protocol" / "tools" / "bootstrap_adapters"
        )
        self.assertEqual(
            registry.benchmarks,
            ("smallbank", "tpcc", "ycsb", "ycsb_fractured_read_probe"),
        )
        self.assertEqual(registry.require("tpcc").benchmark, "tpcc")
        self.assertEqual(registry.require("ycsb").benchmark, "ycsb")
        self.assertEqual(
            registry.require("ycsb_fractured_read_probe").benchmark,
            "ycsb_fractured_read_probe",
        )
        source = (ROOT / "scripts" / "blackbox_record.py").read_text(encoding="utf-8")
        self.assertNotIn('args.benchmark == "tpcc"', source)
        self.assertNotIn('args.benchmark == "ycsb"', source)

    def test_discovers_synthetic_adapter_without_registry_edit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "synthetic.py"
            path.write_text(
                """
class Synthetic:
    benchmark = "synthetic"
    def build_queries(self, contract):
        return {"benchmark": self.benchmark, "format": "none", "queries": []}
    def encode_scan(self, contract, path, **kwargs):
        return [], {"benchbase.synthetic": 0}
ADAPTER = Synthetic()
""",
                encoding="utf-8",
            )
            registry = REGISTRY_MODULE.BootstrapAdapterRegistry.discover(Path(directory))
            self.assertEqual(registry.benchmarks, ("synthetic",))
            self.assertEqual(registry.require("synthetic").build_queries({})["queries"], [])

    def test_unknown_adapter_fails_closed(self) -> None:
        registry = REGISTRY_MODULE.BootstrapAdapterRegistry.discover(
            ROOT / "protocol" / "tools" / "bootstrap_adapters"
        )
        with self.assertRaisesRegex(
            REGISTRY_MODULE.BootstrapAdapterError, "no bootstrap adapter registered"
        ):
            registry.require("missing")


if __name__ == "__main__":
    unittest.main()
