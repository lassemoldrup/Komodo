#!/usr/bin/env python3
"""Decompose the audited-transaction cost on a live YugabyteDB YSQL stack.

Shapes measured per connection (all against the production evidence schema):
  autocommit_update      single-statement UPDATE (single-shard fast path)
  explicit_update        BEGIN; UPDATE usertable; COMMIT
  audited_update         BEGIN; UPDATE usertable; marker upsert; COMMIT
  audited_read           BEGIN; SELECT usertable; marker upsert; COMMIT
  marker_only            BEGIN; marker upsert; COMMIT

Each shape runs for a fixed duration on N concurrent connections and reports
throughput plus latency percentiles, isolating the explicit-transaction cost,
the marker's extra tablet, and concurrency queuing.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import statistics
import time

import psycopg2


def run_shape(dsn: str, shape: str, seconds: float, worker: int) -> list[float]:
    connection = psycopg2.connect(dsn)
    connection.autocommit = shape == "autocommit_update"
    cursor = connection.cursor()
    cursor.execute("SET search_path TO benchbase")
    if not connection.autocommit:
        connection.commit()
    key = 1000 + worker
    session = f"probe/worker/{worker}"
    latencies: list[float] = []
    deadline = time.monotonic() + seconds
    sequence = 0
    while time.monotonic() < deadline:
        sequence += 1
        start = time.monotonic()
        if shape == "autocommit_update":
            cursor.execute(
                "UPDATE usertable SET field1 = %s WHERE ycsb_key = %s",
                (f"v{sequence}", key),
            )
        else:
            if shape in ("explicit_update", "audited_update"):
                cursor.execute(
                    "UPDATE usertable SET field1 = %s WHERE ycsb_key = %s",
                    (f"v{sequence}", key),
                )
            if shape == "audited_read":
                cursor.execute(
                    "SELECT * FROM usertable WHERE ycsb_key = %s", (key,)
                )
                cursor.fetchall()
            if shape in ("audited_update", "audited_read", "marker_only"):
                cursor.execute(
                    "INSERT INTO session_txn (session_id, txn_id) VALUES (%s, %s) "
                    "ON CONFLICT (session_id) DO UPDATE SET txn_id = EXCLUDED.txn_id",
                    (session, sequence),
                )
            connection.commit()
        latencies.append((time.monotonic() - start) * 1000.0)
    connection.close()
    return latencies


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--seconds", type=float, default=10.0)
    parser.add_argument("--connections", type=int, action="append", default=None)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    connection_counts = args.connections or [1, 4]

    shapes = [
        "autocommit_update",
        "explicit_update",
        "audited_update",
        "audited_read",
        "marker_only",
    ]
    report = {"kind": "audited_transaction_cost_probe", "results": []}
    for shape in shapes:
        for count in connection_counts:
            with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
                futures = [
                    pool.submit(run_shape, args.dsn, shape, args.seconds, worker)
                    for worker in range(count)
                ]
                latencies = [
                    value for future in futures for value in future.result()
                ]
            latencies.sort()
            entry = {
                "shape": shape,
                "connections": count,
                "transactions": len(latencies),
                "throughput_per_second": len(latencies) / args.seconds,
                "latency_ms": {
                    "mean": round(statistics.fmean(latencies), 3),
                    "p50": round(latencies[len(latencies) // 2], 3),
                    "p95": round(latencies[int(len(latencies) * 0.95)], 3),
                    "p99": round(latencies[int(len(latencies) * 0.99)], 3),
                },
            }
            report["results"].append(entry)
            print(json.dumps(entry, sort_keys=True))
    with open(args.output, "w", encoding="utf-8") as output:
        json.dump(report, output, indent=2, sort_keys=True)
        output.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
