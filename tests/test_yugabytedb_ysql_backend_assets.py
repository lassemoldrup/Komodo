from __future__ import annotations

import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
YSQL = ROOT / "yugabytedb_ysql"
DEBEZIUM = ROOT / "debezium"


class YugabyteYsqlBackendAssetsTest(unittest.TestCase):
    def test_obsolete_exploratory_assets_are_removed(self) -> None:
        for path in (
            ROOT / "yugabyte" / "init.sh",
            ROOT / "yugabyte" / "init.sql",
            DEBEZIUM / "Dockerfile.yugabyte",
            DEBEZIUM / "application-yugabyte.properties",
        ):
            self.assertFalse(path.exists(), path)

        accepted = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (
                ROOT / "docker-compose.yml",
                ROOT / "README.md",
                DEBEZIUM / "Dockerfile.yugabytedb-ysql",
                DEBEZIUM / "application-yugabytedb-ysql.properties",
                YSQL / "init.sh",
                YSQL / "initdb" / "02-common-schema.sql",
                YSQL / "initdb" / "adapters" / "ycsb.sql",
            )
        )
        for obsolete in (
            "yugabytedb/yugabyte:latest",
            "public.registers",
            "dbz_si",
            "yb_lsn_type=HYBRID_TIME",
            "FOR ALL TABLES",
        ):
            self.assertNotIn(obsolete, accepted)

    def test_compose_uses_pinned_single_slot_logical_stack(self) -> None:
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertIn("profiles: [yugabytedb_ysql]", compose)
        self.assertIn(
            "docker.io/yugabytedb/yugabyte@sha256:3dfa310a7a595bbf1ec5e6e6e58e31821aa86a8444b65d800fcc2ef6b3953cbb",
            compose,
        )
        self.assertIn(
            "docker.io/library/redis@sha256:9702d01c1f10c3ea9f48211b4362e44f154ff02d063e6f7268eba804059f53bf",
            compose,
        )
        self.assertIn("cdc_enable_intra_transactional_before_image=true", compose)
        self.assertIn("cdc_send_null_before_image_if_not_exists=false", compose)
        self.assertEqual(compose.count("container_name: cdc-debezium-yugabytedb-ysql"), 1)

    def test_connector_and_image_match_the_contract_gate(self) -> None:
        properties = {}
        for line in (DEBEZIUM / "application-yugabytedb-ysql.properties").read_text(
            encoding="utf-8"
        ).splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                key, separator, value = line.partition("=")
                self.assertTrue(separator)
                self.assertNotIn(key, properties)
                properties[key] = value
        expected = {
            "debezium.source.connector.class": "io.debezium.connector.postgresql.YugabyteDBConnector",
            "debezium.source.tasks.max": "1",
            "debezium.source.plugin.name": "pgoutput",
            "debezium.source.slot.lsn.type": "SEQUENCE",
            "debezium.source.streaming.mode": "default",
            "debezium.source.snapshot.mode": "never",
            "debezium.source.provide.transaction.metadata": "true",
            "debezium.source.publication.autocreate.mode": "disabled",
            "debezium.source.event.processing.failure.handling.mode": "fail",
            "debezium.transforms.route.replacement": "cdc",
        }
        for key, value in expected.items():
            self.assertEqual(properties.get(key), value)

        dockerfile = (DEBEZIUM / "Dockerfile.yugabytedb-ysql").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "4e83132cbebdec53e553f69d4cfdbe48dac563105aabc5db4b880cdcf0118d98",
            dockerfile,
        )
        self.assertIn("debezium-connector-postgres-*.jar", dockerfile)
        self.assertIn("postgresql-*.jar", dockerfile)

    def test_schema_is_empty_full_image_and_least_privilege(self) -> None:
        schema_text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (
                YSQL / "initdb" / "02-common-schema.sql",
                YSQL / "initdb" / "adapters" / "ycsb.sql",
            )
        )
        self.assertNotRegex(schema_text, r"(?im)^\s*INSERT\s+INTO")
        self.assertEqual(schema_text.upper().count("REPLICA IDENTITY FULL"), 3)
        self.assertIn("SPLIT INTO 2 TABLETS", schema_text)

        init = (YSQL / "init.sh").read_text(encoding="utf-8")
        self.assertIn("CREATE ROLE blackbox_setup", init)
        self.assertIn("CREATE ROLE blackbox_application", init)
        self.assertIn("CREATE ROLE blackbox_replication LOGIN REPLICATION", init)
        self.assertNotIn("SUPERUSER", init)
        self.assertIn("CREATE PUBLICATION $publication_name FOR TABLE $capture_tables", init)
        self.assertIn("'pgoutput', false, false, 'SEQUENCE'", init)

        connector = (
            DEBEZIUM / "application-yugabytedb-ysql.properties"
        ).read_text(encoding="utf-8")
        self.assertNotIn("blackbox_setup", connector)
        self.assertNotIn("database.user=yugabyte", connector)

    def test_privilege_attestation_covers_effective_ddl_authority(self) -> None:
        driver = (
            ROOT / "scripts" / "backends" / "yugabytedb_ysql.sh"
        ).read_text(encoding="utf-8")
        for required in (
            "pg_auth_members",
            "pg_default_acl",
            "database.datacl",
            "namespace.nspacl",
            "relation.relacl",
            "relation.relowner",
            "has_database_privilege",
            "has_schema_privilege",
            "unauthorized_ddl_logins",
            "activity.usesysid IN (SELECT role_id FROM capable_logins)",
            "activity.backend_type = 'client backend'",
            "activity.usename <> 'blackbox_replication'",
        ):
            self.assertIn(required, driver)
        self.assertNotIn("slot_active_consumers", driver)

        backend = json.loads(
            (ROOT / "protocol" / "backends" / "yugabytedb_ysql.json").read_text()
        )
        stable_facts = {
            fact["name"] for fact in backend["attestation_fact_contract"]
        }
        self.assertNotIn("slot_active_consumers", stable_facts)
        self.assertIn("dedicated_privileges_sha256", stable_facts)

    def test_generic_lifecycle_has_no_backend_name_or_physical_sql_branch(self) -> None:
        generic = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (
                ROOT / "scripts" / "e2e.sh",
                ROOT / "scripts" / "blackbox_lifecycle.sh",
                ROOT / "scripts" / "blackbox_record.py",
            )
        )
        for forbidden in (
            "cdc-mariadb",
            "yugabytedb_ysql",
            "MariaDB",
            "TO_BASE64",
            "`benchbase`",
        ):
            self.assertNotIn(forbidden, generic)

    def test_provenance_and_backend_contract_agree(self) -> None:
        provenance = json.loads(
            (ROOT / "connector-contract" / "yugabytedb_ysql" / "provenance.lock.json").read_text()
        )
        backend = json.loads(
            (ROOT / "protocol" / "backends" / "yugabytedb_ysql.json").read_text()
        )
        connector = provenance["artifacts"]["connector"]
        self.assertEqual(
            backend["connector_contract"]["source_version"], connector["version"]
        )
        fact = next(
            item
            for item in backend["attestation_fact_contract"]
            if item["name"] == "connector_sha256"
        )
        self.assertEqual(fact["expected_value"], connector["sha256"])
        self.assertRegex(connector["sha256"], re.compile(r"^[0-9a-f]{64}$"))


if __name__ == "__main__":
    unittest.main()
