import re
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


class DefaultCdcProfilesTest(unittest.TestCase):
    def test_oracle_uses_unmodified_pinned_connector(self):
        compose = (ROOT / "docker-compose.yml").read_text()
        block = re.search(r"(?ms)^  debezium-oracle:\n(.*?)(?=^  [\w-]+:|\Z)", compose).group(1)
        self.assertIn("image: quay.io/debezium/server@sha256:8d3a3ad6e16ccc52c2efbb76b2e8198697f1e4bfe67ed5557b405a5a01c85da5", block)
        self.assertNotIn("pause100ms", block)
        config = (ROOT / "debezium/application-oracle.properties").read_text()
        self.assertIn("debezium.sink.redis.batch.size=1000", config)
        self.assertIn("debezium.source.offset.flush.interval.ms=1000", config)
        self.assertNotIn("debezium.source.query.fetch.size=2000", config)
        self.assertNotIn("debezium.source.log.mining.query.filter.mode=in", config)

    def test_oceanbase_uses_validated_batching(self):
        config = (ROOT / "debezium/application-oceanbase-mysql.properties").read_text()
        for setting in ("debezium.source.max.batch.size=512",
                        "debezium.source.max.queue.size=2048",
                        "debezium.sink.redis.batch.size=256",
                        "debezium.source.offset.flush.interval.ms=0"):
            self.assertIn(setting, config)


if __name__ == "__main__":
    unittest.main()
