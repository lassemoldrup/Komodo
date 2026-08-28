from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "registered_performance_result.py"


def load_module():
    spec = importlib.util.spec_from_file_location("registered_performance_result", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RegisteredPerformanceResultTest(unittest.TestCase):
    def test_operations_per_transaction_is_parsed_from_declared_parameters(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "workload-parameters.txt"
            path.write_text(
                "BENCHBASE_MEASUREMENT_OPERATIONS_PER_TRANSACTION=7\n"
                "BENCHBASE_MEASUREMENT_TERMINALS=4\n",
                encoding="utf-8",
            )
            parameters = module.workload_parameters(path)
            self.assertEqual(module.declared_operations_per_transaction(parameters), 7)

    def test_operations_per_transaction_rejects_noncanonical_values(self) -> None:
        module = load_module()
        with self.assertRaisesRegex(ValueError, "operations per transaction is invalid"):
            module.declared_operations_per_transaction(
                {module.OPERATIONS_PER_TRANSACTION: "0"}
            )


if __name__ == "__main__":
    unittest.main()
