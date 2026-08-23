from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "protocol" / "tools"
MODULE_PATH = TOOLS / "backend_sql_renderer.py"
sys.path.insert(0, str(TOOLS))
SPEC = importlib.util.spec_from_file_location("backend_sql_renderer", MODULE_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover
    raise RuntimeError(f"cannot load {MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class BackendSqlRendererTest(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = MODULE.BackendSqlRendererRegistry.discover(
            TOOLS / "backend_sql_renderers"
        )
        self.ycsb = json.loads((ROOT / "protocol/adapters/ycsb.json").read_text())
        self.tpcc = json.loads((ROOT / "protocol/adapters/tpcc.json").read_text())

    def test_registered_backends_render_same_logical_ycsb_plan(self) -> None:
        from bootstrap_adapters.ycsb import YcsbBootstrapAdapter

        plan = YcsbBootstrapAdapter().build_queries(self.ycsb)
        mariadb = self.registry.require("mariadb").bootstrap_queries(self.ycsb, plan)[0]["sql"]
        yugabyte = self.registry.require("yugabytedb_ysql").bootstrap_queries(
            self.ycsb, plan
        )[0]["sql"]
        self.assertIn("TO_BASE64", mariadb)
        self.assertIn("convert_to", yugabyte)
        self.assertIn("replace(replace(encode", yugabyte)
        self.assertIn('FROM "benchbase"."usertable"', yugabyte)
        self.assertNotIn("TO_BASE64", yugabyte)
        logical_source = (
            TOOLS / "bootstrap_adapters" / "ycsb.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("TO_BASE64", logical_source)
        self.assertNotIn("`", logical_source)

    def test_yugabyte_loader_and_tpcc_scan_have_no_mysql_syntax(self) -> None:
        from bootstrap_adapters.tpcc import TpccBootstrapAdapter

        loader = self.registry.require("yugabytedb_ysql").loader_queries(self.tpcc)
        self.assertEqual(len(loader), 11)
        self.assertIn('COUNT(DISTINCT c."ol_number")', loader[-1]["sql"])
        plan = TpccBootstrapAdapter().build_queries(self.tpcc)
        queries = self.registry.require("yugabytedb_ysql").bootstrap_queries(
            self.tpcc, plan
        )
        customer = next(item["sql"] for item in queries if item["table"] == "benchbase.customer")
        self.assertIn('CAST("c_discount" AS TEXT)', customer)
        self.assertIn("to_char", customer)
        self.assertNotIn("DATE_FORMAT", customer)
        self.assertNotIn("`", customer)

    def test_synthetic_renderer_discovers_without_generic_registry_edit(self) -> None:
        with tempfile.TemporaryDirectory(prefix="backend-sql-renderer-") as temporary:
            path = Path(temporary) / "synthetic.py"
            path.write_text(
                """
class Synthetic:
    backend = "synthetic"
    def loader_queries(self, adapter): return []
    def bootstrap_queries(self, adapter, logical_plan): return []
RENDERER = Synthetic()
""",
                encoding="utf-8",
            )
            registry = MODULE.BackendSqlRendererRegistry.discover(Path(temporary))
            self.assertEqual(registry.backends, ("synthetic",))
            self.assertEqual(registry.require("synthetic").loader_queries({}), [])


if __name__ == "__main__":
    unittest.main()
