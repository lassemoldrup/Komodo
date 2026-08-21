#!/usr/bin/env python3

import argparse
import json
import subprocess
import tempfile
from pathlib import Path


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def command(executable, capture, fixture, benchmark="tpcc"):
    return [
        executable,
        "--input",
        str(capture),
        "--run-id",
        fixture["run_id"],
        "--backend",
        fixture["backend"],
        "--benchmark",
        benchmark,
        "--backend-source-id",
        "mariadb-main",
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay", required=True)
    parser.add_argument("--fixture", required=True)
    args = parser.parse_args()

    fixture_path = Path(args.fixture)
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    capture = list(fixture["startup_schema"])
    for transaction in fixture["transactions"]:
        capture.extend(transaction["raw_entries"])

    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".json"
    ) as temporary:
        json.dump(capture, temporary, separators=(",", ":"))
        temporary.flush()
        result = subprocess.run(
            command(args.replay, temporary.name, fixture),
            capture_output=True,
            text=True,
            check=False,
        )
        require(result.returncode == 0, result.stdout + result.stderr)
        replayed = json.loads(result.stdout)
        expected_transactions = fixture["transactions"]
        actual_transactions = replayed["transactions"]
        require(
            replayed["completed_transactions"] == len(expected_transactions),
            "replay transaction count differs",
        )
        require(
            len(actual_transactions) == len(expected_transactions),
            "replay transaction array differs",
        )
        for index, (actual, fixture_transaction) in enumerate(
            zip(actual_transactions, expected_transactions, strict=True)
        ):
            expected = fixture_transaction["expected"]
            label = f"transaction {index} ({fixture_transaction['name']})"
            require(actual["cdc_seq"] == expected["cdc_seq"], f"{label} cdc_seq")
            require(actual["kind"] == expected["kind"], f"{label} kind")
            if expected["fence_phase"] is None:
                require("fence_phase" not in actual, f"{label} unexpected fence")
            else:
                require(
                    actual.get("fence_phase") == expected["fence_phase"],
                    f"{label} fence",
                )
            if expected["correlation_id"] is None:
                require("correlation_id" not in actual, f"{label} correlation")
            else:
                require(
                    actual.get("correlation_id") == expected["correlation_id"],
                    f"{label} correlation",
                )
            expected_writes = expected["writes"]
            actual_writes = actual["writes"]
            require(len(actual_writes) == len(expected_writes), f"{label} writes")
            for write_index, (actual_write, expected_write) in enumerate(
                zip(actual_writes, expected_writes, strict=True)
            ):
                write_label = f"{label} write {write_index}"
                require(
                    actual_write["key_hex"] == expected_write["key_hex"],
                    f"{write_label} key",
                )
                require(
                    actual_write["value_state"] == expected_write["state"],
                    f"{write_label} state",
                )
                require(
                    actual_write.get("value_hex") == expected_write.get("value_hex"),
                    f"{write_label} value",
                )

        wrong_adapter = subprocess.run(
            command(args.replay, temporary.name, fixture, benchmark="ycsb"),
            capture_output=True,
            text=True,
            check=False,
        )
        require(wrong_adapter.returncode != 0, "wrong adapter replay succeeded")
        require(
            "DDL_OBSERVED" in wrong_adapter.stderr
            and "missing a configured table" in wrong_adapter.stderr,
            "wrong adapter replay did not fail at the table boundary: "
            + wrong_adapter.stderr,
        )

    print("TPC-C offline CDC replay matches every normalized fixture transaction")


if __name__ == "__main__":
    main()
