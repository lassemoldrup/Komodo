from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

TEST_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(TEST_ROOT))

from yugabytedb_ysql_acceptance_evidence import (  # noqa: E402
    cdc_frames,
    validate_client_cdc_correlations,
    validate_runtime_provenance,
)


RUN_ID = "00112233-4455-6677-8899-aabbccddeeff"


def entry(stream_id: str, key: dict[str, object], value: dict[str, object]) -> list[object]:
    return [
        stream_id,
        [
            json.dumps(key, separators=(",", ":")),
            json.dumps(value, separators=(",", ":")),
        ],
    ]


class YugabyteYsqlAcceptanceEvidenceTest(unittest.TestCase):
    def test_client_marker_correlation_and_frame_counts_fail_closed(self) -> None:
        cdc = [
            entry("1-0", {"id": "a"}, {"status": "BEGIN", "id": "a", "event_count": None, "data_collections": None, "ts_ms": 1}),
            entry(
                "2-0",
                {"session_id": "ycsb/worker/1"},
                {
                    "before": None,
                    "after": {"session_id": "ycsb/worker/1", "txn_id": 1},
                    "source": {"schema": "benchbase", "table": "session_txn"},
                    "op": "c",
                    "transaction": {"total_order": 1, "data_collection_order": 1},
                },
            ),
            entry(
                "3-0",
                {"id": "b"},
                {
                    "status": "END",
                    "id": "b",
                    "event_count": 1,
                    "data_collections": [
                        {"data_collection": "benchbase.session_txn", "event_count": 1}
                    ],
                    "ts_ms": 1,
                },
            ),
        ]
        client = [
            [
                "1-0",
                [
                    "payload",
                    json.dumps(
                        {
                            "kind": "client_txn",
                            "run_id": RUN_ID,
                            "session_id": "ycsb/worker/1",
                            "correlation_id": {
                                "run_id": RUN_ID,
                                "session_id": "ycsb/worker/1",
                                "txn_id": 1,
                            },
                        },
                        separators=(",", ":"),
                    ),
                ],
            ]
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cdc_path = root / "cdc.json"
            client_path = root / "client.json"
            cdc_path.write_text(json.dumps(cdc), encoding="utf-8")
            client_path.write_text(json.dumps(client), encoding="utf-8")
            records, marker_frames = validate_client_cdc_correlations(
                RUN_ID, client_path, cdc_path
            )
            self.assertEqual(len(records), 1)
            self.assertEqual(marker_frames, 1)

            client_path.write_text("[]", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "no ClientTxn"):
                validate_client_cdc_correlations(RUN_ID, client_path, cdc_path)

            cdc[-1][1][1] = json.dumps(
                {
                    "status": "END",
                    "id": "b",
                    "event_count": 2,
                    "data_collections": [
                        {"data_collection": "benchbase.session_txn", "event_count": 1}
                    ],
                    "ts_ms": 1,
                }
            )
            cdc_path.write_text(json.dumps(cdc), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "event_count"):
                cdc_frames(cdc_path)

    def test_runtime_connector_image_must_equal_locked_identity(self) -> None:
        image_id = "sha256:" + "1" * 64
        image_tag = "db-monitoring/yugabytedb-ysql:test"
        provenance = {
            "artifacts": {
                "runtime_connector_image": {
                    "image": image_tag,
                    "local_image_id": image_id,
                },
                "connector": {"sha256": "2" * 64},
            }
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            provenance_path = root / "provenance.json"
            container_path = root / "container.json"
            image_path = root / "image.json"
            identity_path = root / "identity.txt"
            provenance_path.write_text(json.dumps(provenance), encoding="utf-8")
            container_path.write_text(
                json.dumps([{"Image": image_id, "Config": {"Image": image_tag}}]),
                encoding="utf-8",
            )
            image_path.write_text(
                json.dumps([{"Id": image_id, "RepoTags": [image_tag]}]),
                encoding="utf-8",
            )
            identity_path.write_text("2" * 64, encoding="utf-8")
            self.assertEqual(
                validate_runtime_provenance(
                    provenance_path,
                    [container_path],
                    connector_image_path=image_path,
                    connector_identity_paths=[identity_path],
                ),
                image_id,
            )

            container_path.write_text(
                json.dumps([{"Image": "sha256:" + "3" * 64, "Config": {"Image": image_tag}}]),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "container image differs"):
                validate_runtime_provenance(provenance_path, [container_path])


if __name__ == "__main__":
    unittest.main()
