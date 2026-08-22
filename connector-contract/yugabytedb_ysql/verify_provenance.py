#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
SHA256 = re.compile(r"^[0-9a-f]{64}$")


def require(condition: bool, diagnostic: str) -> None:
    if not condition:
        raise SystemExit(diagnostic)


def verify_remote_artifact(artifact: dict[str, Any], expected_member: str | None = None) -> None:
    with tempfile.TemporaryDirectory(prefix="yugabytedb-connector-contract-") as temporary:
        target = Path(temporary) / "artifact"
        with urllib.request.urlopen(artifact["url"], timeout=60) as response:
            target.write_bytes(response.read())
        actual = hashlib.sha256(target.read_bytes()).hexdigest()
        require(actual == artifact["sha256"], f"download checksum differs for {artifact['url']}")
        require(target.stat().st_size == artifact["bytes"], f"download size differs for {artifact['url']}")
        if expected_member is not None:
            with zipfile.ZipFile(target) as archive:
                members = set(archive.namelist())
            require(expected_member in members, f"connector class is absent: {expected_member}")
            require(
                "io/debezium/connector/postgresql/PostgresConnector.class" not in members,
                "connector release unexpectedly contains the stock PostgresConnector",
            )


def parse_properties(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, separator, value = stripped.partition("=")
        require(bool(separator), f"invalid property line: {line}")
        require(key not in result, f"duplicate property: {key}")
        result[key] = value
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--online", action="store_true")
    args = parser.parse_args()

    lock = json.loads((ROOT / "provenance.lock.json").read_text(encoding="utf-8"))
    require(lock["platform"] == "linux/amd64", "only the frozen linux/amd64 platform is admitted")
    artifacts = lock["artifacts"]
    for name in ("database", "debezium_server", "java_runtime", "redis"):
        image = artifacts[name]["image"]
        require("@sha256:" in image, f"{name} image is not digest-addressed")
        require(":latest" not in image, f"{name} image uses latest")
        digest = artifacts[name]["manifest_digest"].removeprefix("sha256:")
        require(bool(SHA256.fullmatch(digest)), f"{name} manifest digest is malformed")
    for name in ("connector", "postgresql_jdbc"):
        require(bool(SHA256.fullmatch(artifacts[name]["sha256"])), f"{name} checksum is malformed")
        require(artifacts[name]["url"].startswith("https://"), f"{name} URL is not HTTPS")

    configuration = lock["connector_configuration"]
    require(configuration["connector_class"] == "io.debezium.connector.postgresql.YugabyteDBConnector", "wrong connector class")
    require(configuration["primary_plugin"] == "pgoutput", "primary plugin is not pgoutput")
    require(configuration["slot_lsn_type"] == "SEQUENCE", "slot is not SEQUENCE")
    require(configuration["streaming_mode"] == "default", "parallel streaming is forbidden")
    require(configuration["tasks"] == 1 and configuration["logical_slots"] == 1, "exactly one task and slot are required")

    properties = parse_properties(ROOT / "application.properties")
    expected_properties = {
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
    for name, expected in expected_properties.items():
        require(properties.get(name) == expected, f"property {name} is not frozen to {expected}")
    serialized = json.dumps(lock).lower()
    require("grpc" not in serialized, "gRPC connector is forbidden")
    require("hybrid_time" not in serialized, "HYBRID_TIME is forbidden")

    schema = (ROOT / "schema.sql").read_text(encoding="utf-8").lower()
    require("for all tables" not in schema, "publication may not use FOR ALL TABLES")
    require("replica identity full" in schema, "FULL replica identity is absent")
    require("insert into" not in schema, "schema contains pre-CDC seed data")

    if args.online:
        verify_remote_artifact(
            artifacts["connector"],
            "io/debezium/connector/postgresql/YugabyteDBConnector.class",
        )
        verify_remote_artifact(artifacts["postgresql_jdbc"])
    print("provenance contract verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
