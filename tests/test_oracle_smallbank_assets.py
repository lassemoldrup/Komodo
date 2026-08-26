from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "monitor/tests/fixtures/oracle_logminer_3_6_1_smallbank.json"


class OracleSmallBankAssetsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.replay = Path(
            os.environ.get(
                "BLACKBOX_CDC_CODEC_REPLAY_BIN",
                "/tmp/db-monitoring-blackbox-monitor-build/blackbox_cdc_codec_replay",
            )
        )

    def replay_fixture(self, document: list, *, expect_success: bool) -> subprocess.CompletedProcess:
        with tempfile.NamedTemporaryFile("w", suffix=".json", encoding="utf-8") as stream:
            json.dump(document, stream, separators=(",", ":"))
            stream.flush()
            result = subprocess.run(
                [
                    str(self.replay),
                    "--input",
                    stream.name,
                    "--backend",
                    "oracle",
                    "--benchmark",
                    "smallbank",
                    "--backend-source-id",
                    "oracle_blackbox_main",
                ],
                text=True,
                capture_output=True,
            )
        self.assertEqual(result.returncode == 0, expect_success, result.stderr)
        return result

    def test_real_fixture_and_before_variants_normalize_identically(self) -> None:
        original = json.loads(FIXTURE.read_text(encoding="utf-8"))
        baseline = self.replay_fixture(original, expect_success=True).stdout
        for replacement in (None, "absent"):
            mutated = json.loads(json.dumps(original))
            for entry in mutated:
                envelope = json.loads(entry[1][1])
                if envelope.get("op") == "u":
                    if replacement == "absent":
                        envelope.pop("before")
                    else:
                        envelope["before"] = None
                    entry[1][1] = json.dumps(envelope, separators=(",", ":"))
            self.assertEqual(
                self.replay_fixture(mutated, expect_success=True).stdout,
                baseline,
            )

    def test_sparse_after_key_mismatch_and_extra_source_field_fail_closed(self) -> None:
        original = json.loads(FIXTURE.read_text(encoding="utf-8"))
        for mutation in ("sparse", "key_mismatch", "extra_source"):
            mutated = json.loads(json.dumps(original))
            envelope = json.loads(mutated[2][1][1])
            if mutation == "sparse":
                envelope["after"].pop("BAL")
            elif mutation == "key_mismatch":
                envelope["after"]["CUSTID"] = "2"
            else:
                envelope["source"]["unexpected"] = "value"
            mutated[2][1][1] = json.dumps(envelope, separators=(",", ":"))
            self.replay_fixture(mutated, expect_success=False)

    def test_exploratory_schema_and_fixed_include_list_are_replaced(self) -> None:
        self.assertFalse((ROOT / "oracle/startdb.d/01-cdc-setup.sh").exists())
        self.assertFalse((ROOT / "oracle/startdb.d/02-cdc-setup.sql").exists())
        accepted = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (
                ROOT / "debezium/application-oracle.properties",
                ROOT / "oracle/init.sh",
                ROOT / "oracle/initdb/02-common-schema.sql",
                ROOT / "scripts/backends/oracle.sh",
            )
        )
        self.assertNotIn("SITEST", accepted)
        self.assertNotIn("REGISTERS", accepted)
        self.assertNotIn("c##dbzuser", accepted)
        properties = (ROOT / "debezium/application-oracle.properties").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("debezium.source.table.include.list=", properties)
        self.assertNotIn("debezium.source.database.password=", properties)

    def test_oracle_prerequisites_pin_a_logminer_sized_redo_ring(self) -> None:
        prerequisites = (ROOT / "oracle/startdb.d/01-logminer-prerequisites.sh").read_text(
            encoding="utf-8"
        )
        for group in range(3, 7):
            self.assertIn(f"add_group({group},", prerequisites)
        self.assertIn("SIZE 512M", prerequisites)
        backend = json.loads((ROOT / "protocol/backends/oracle.json").read_text(encoding="utf-8"))
        facts = {
            item["name"]: item.get("expected_value")
            for item in backend["attestation_fact_contract"]
        }
        self.assertEqual(facts["online_redo_log_groups"], "6")
        self.assertEqual(facts["online_redo_large_groups"], "4")
        self.assertEqual(facts["online_redo_large_group_bytes"], "536870912")


if __name__ == "__main__":
    unittest.main()
