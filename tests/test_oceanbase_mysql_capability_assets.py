from __future__ import annotations

import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "connector-contract" / "oceanbase_mysql"


class OceanBaseMysqlCapabilityAssetsTest(unittest.TestCase):
    def test_provenance_pins_every_candidate_artifact(self) -> None:
        provenance = json.loads(
            (CONTRACT / "provenance.lock.json").read_text(encoding="utf-8")
        )
        artifacts = provenance["artifacts"]
        self.assertEqual(artifacts["oceanbase"]["version"], "4.4.2.1")
        self.assertEqual(artifacts["odp"]["version"], "4.3.6.1")
        self.assertEqual(artifacts["obbinlog"]["version"], "4.3.6.4")
        self.assertEqual(artifacts["debezium_server"]["version"], "3.6.1.Final")
        self.assertEqual(
            artifacts["oceanbase_connector_j"]["coordinates"],
            "com.oceanbase:oceanbase-client:2.4.18",
        )
        for artifact in (
            artifacts["oceanbase"]["rpm_sha256"],
            artifacts["oceanbase"]["libs_rpm_sha256"],
            artifacts["odp"]["rpm_sha256"],
            artifacts["obbinlog"]["rpm_sha256"],
            artifacts["debezium_server"]["mysql_connector_jar_sha256"],
            artifacts["oceanbase_connector_j"]["sha256"],
        ):
            self.assertRegex(artifact, r"\A[0-9a-f]{64}\Z")

    def test_disposable_stack_uses_only_the_selected_source_path(self) -> None:
        compose = (CONTRACT / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertIn(
            "quay.io/debezium/server@sha256:11124f362db7c99096ead22237374075f42737c68487cb790e69bdc4ce2e817f",
            compose,
        )
        self.assertIn(
            "redis@sha256:9702d01c1f10c3ea9f48211b4362e44f154ff02d063e6f7268eba804059f53bf",
            compose,
        )
        self.assertNotRegex(compose, r"(?i)(kafka|flink|oms|latest)")
        self.assertEqual(compose.count("debezium_server:"), 1)

    def test_stock_connector_configuration_is_fail_closed(self) -> None:
        properties: dict[str, str] = {}
        for line in (CONTRACT / "application.properties").read_text(
            encoding="utf-8"
        ).splitlines():
            line = line.strip()
            if not line:
                continue
            key, separator, value = line.partition("=")
            self.assertTrue(separator)
            self.assertNotIn(key, properties)
            properties[key] = value
        expected = {
            "debezium.source.connector.class":
                "io.debezium.connector.mysql.MySqlConnector",
            "debezium.source.tasks.max": "1",
            "debezium.source.database.port": "2883",
            "debezium.source.database.user": "blackbox_connector@blackbox",
            "debezium.source.snapshot.mode": "no_data",
            "debezium.source.snapshot.locking.mode": "none",
            "debezium.source.provide.transaction.metadata": "true",
            "debezium.source.event.processing.failure.handling.mode": "fail",
            "debezium.transforms.route.replacement": "cdc",
        }
        for key, value in expected.items():
            self.assertEqual(properties.get(key), value)
        self.assertFalse((CONTRACT / "diagnostics").exists())

    def test_schema_is_empty_utf8_and_least_privilege(self) -> None:
        schema = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (
                CONTRACT / "setup" / "create-principals.sql",
                CONTRACT / "setup" / "create-schema.sql",
            )
        )
        self.assertNotRegex(schema, r"(?im)^\s*INSERT\s+INTO")
        self.assertIn("DEFAULT COLLATE utf8mb4_bin", schema)
        self.assertIn("PARTITION BY HASH(ycsb_key) PARTITIONS 4", schema)
        self.assertIn("GRANT SELECT ON benchbase.* TO blackbox_connector", schema)
        self.assertNotRegex(
            schema,
            r"(?i)GRANT\s+(ALL|CREATE|ALTER|DROP).*blackbox_(application|connector)",
        )

    def test_passing_spike_registers_only_backend_owned_assets(self) -> None:
        self.assertTrue((ROOT / "protocol" / "backends" / "oceanbase_mysql.json").is_file())
        monitor_registry = (ROOT / "monitor" / "src" / "backend_adapter.cpp").read_text(
            encoding="utf-8"
        )
        self.assertIn('return "oceanbase_mysql"', monitor_registry)
        backend_files = {
            path.name for path in (ROOT / "scripts" / "backends").glob("*.sh")
        }
        self.assertIn("oceanbase_mysql.sh", backend_files)

    def test_registered_stack_uses_odp_and_the_pinned_stock_connector(self) -> None:
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        connector = (ROOT / "debezium" / "application-oceanbase-mysql.properties").read_text(
            encoding="utf-8"
        )
        self.assertIn("profiles: [oceanbase_mysql]", compose)
        self.assertIn('"2883:2883"', compose)
        self.assertIn("database.port=2883", connector)
        self.assertIn("database.user=blackbox_connector@blackbox", connector)
        self.assertIn("snapshot.locking.mode=none", connector)
        self.assertIn("gtid.ignore.on.recovery=true", connector)
        self.assertNotIn("bootstrap.servers", connector)
        self.assertNotRegex(connector, r"(?i)(flink|oms)")

    def test_pre_binlog_schema_gate_uses_the_adapter_table_roster(self) -> None:
        initializer = (
            CONTRACT / "source" / "initialize-before-binlog.sh"
        ).read_text(encoding="utf-8")
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertIn(
            "${BLACKBOX_OCEANBASE_MYSQL_CAPTURE_TABLES:?}", initializer
        )
        self.assertIn("expected_capture_tables", initializer)
        self.assertNotIn("usertable", initializer)
        self.assertNotIn("accounts", initializer)
        self.assertNotIn("warehouse", initializer)
        self.assertEqual(
            compose.count(
                "BLACKBOX_OCEANBASE_MYSQL_CAPTURE_TABLES: "
                "${BLACKBOX_OCEANBASE_MYSQL_CAPTURE_TABLES:-}"
            ),
            1,
        )
        self.assertIn(
            "DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST: "
            "${BLACKBOX_OCEANBASE_MYSQL_CAPTURE_TABLES:-}",
            compose,
        )

    def test_ycsb_and_semantic_probe_use_identical_lowercase_schema(self) -> None:
        adapter_root = ROOT / "oceanbase_mysql" / "initdb" / "adapters"
        ycsb = (adapter_root / "ycsb.sql").read_text(encoding="utf-8")
        self.assertEqual(
            ycsb,
            (adapter_root / "ycsb_fractured_read_probe.sql").read_text(encoding="utf-8"),
        )
        self.assertIn("PARTITION BY HASH(ycsb_key) PARTITIONS 4", ycsb)
        self.assertNotIn("YCSB_KEY", ycsb)

    def test_opt_in_benchmark_assets_are_backend_owned(self) -> None:
        adapter_root = ROOT / "oceanbase_mysql" / "initdb" / "adapters"
        config_root = ROOT.parent / "benchbase" / "config" / "oceanbase"
        for benchmark in ("ycsb", "tpcc", "smallbank"):
            schema = adapter_root / f"{benchmark}.sql"
            config = config_root / f"blackbox_{benchmark}_performance_config.xml"
            self.assertTrue(schema.is_file())
            self.assertTrue(config.is_file())
            config_text = config.read_text(encoding="utf-8")
            self.assertIn("<backend>oceanbase_mysql</backend>", config_text)
            self.assertIn(f"<benchmark>{benchmark}</benchmark>", config_text)
        for benchmark in ("tpcc", "smallbank"):
            schema_text = (adapter_root / f"{benchmark}.sql").read_text(
                encoding="utf-8"
            )
            self.assertIn("CREATE TABLE session_txn", schema_text)
            self.assertIn("CREATE TABLE checker_run_fence", schema_text)

    def test_identifiers_are_descriptive(self) -> None:
        sources = "\n".join(
            path.read_text(encoding="utf-8")
            for path in CONTRACT.rglob("*")
            if path.is_file()
            and path.suffix in {".json", ".properties", ".py", ".sh", ".sql", ".yml"}
            and "evidence" not in path.parts
        )
        self.assertIsNone(re.search(r"(?i)(?<![a-z0-9])m14(?![a-z0-9])", sources))


if __name__ == "__main__":
    unittest.main()
