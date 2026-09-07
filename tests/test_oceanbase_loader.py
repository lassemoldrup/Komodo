"""Exercise the inline loader URL policy without starting a database."""
import os
from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]


class OceanBaseLoaderTest(unittest.TestCase):
    def test_loader_url_scope_and_quoting(self):
        cases = [
            ("jdbc:oceanbase://host/db", "jdbc:oceanbase://host/db?rewriteBatchedStatements=true"),
            ("jdbc:oceanbase://host/db?useSSL=false", "jdbc:oceanbase://host/db?useSSL=false&rewriteBatchedStatements=true"),
            ("jdbc:oceanbase://host/db?rewriteBatchedStatements=false&x=1&rewriteBatchedStatements=true",
             "jdbc:oceanbase://host/db?x=1&rewriteBatchedStatements=true"),
            ("jdbc:oceanbase://host/db?x=a'$(exit 91); ",
             "jdbc:oceanbase://host/db?x=a'$(exit 91); &rewriteBatchedStatements=true"),
        ]
        e2e = (ROOT / "scripts/e2e.sh").read_text()
        e2e = e2e[e2e.index('  if [[ $label == loader &&'):]
        e2e = e2e[:e2e.index('  blackbox_log "phase $label: starting"')]
        cell = (ROOT / "scripts/run_registered_performance_cell.sh").read_text()
        cell = cell[cell.index('if [[ $BACKEND == oceanbase_mysql ]]; then', cell.index('LOADER_COMMAND="cd ')):]
        cell = cell[:cell.index('WORKLOAD_COMMAND="cd ')]
        for snippet, variable in ((e2e, "command"), (cell, "LOADER_COMMAND")):
            for backend in ("oceanbase_mysql", "mariadb", "oracle", "yugabytedb_ysql"):
                for url, expected in cases:
                    for label in (("loader", "workload") if variable == "command" else ("loader",)):
                        with self.subTest(entry=variable, backend=backend, url=url, label=label):
                            program = (
                                'blackbox_log() { :; }; test_policy() {\n'
                                + variable + '=\'printf "%s" "$BLACKBOX_JDBC_URL"\'\n'
                                + snippet + '\nbash -c "$' + variable + '"\n'
                                + 'printf "\\n%s" "$BLACKBOX_JDBC_URL"\n}; test_policy'
                            )
                            output = subprocess.check_output(
                                ["bash", "-eu", "-c", program], text=True,
                                env={**os.environ, "BLACKBOX_BACKEND": backend,
                                     "BACKEND": backend, "label": label,
                                     "BLACKBOX_JDBC_URL": url},
                            )
                            optimized = backend == "oceanbase_mysql" and label == "loader"
                            self.assertEqual(output, (expected if optimized else url) + "\n" + url)


if __name__ == "__main__":
    unittest.main()
