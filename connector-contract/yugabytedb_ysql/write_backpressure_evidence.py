#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def positive(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def nonnegative(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be nonnegative")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pause-ms", type=positive, required=True)
    parser.add_argument("--pause-start-ns", type=positive, required=True)
    parser.add_argument("--workload-committed-ns", type=positive, required=True)
    parser.add_argument("--connector-write-observed-ns", type=positive, required=True)
    parser.add_argument("--unpause-ns", type=positive, required=True)
    parser.add_argument("--stream-length-before", type=nonnegative, required=True)
    parser.add_argument("--stream-length-during", type=nonnegative, required=True)
    parser.add_argument("--stream-length-after", type=positive, required=True)
    parser.add_argument("--client-list", type=Path, required=True)
    args = parser.parse_args()

    if not (
        args.pause_start_ns
        <= args.workload_committed_ns
        <= args.connector_write_observed_ns
        <= args.unpause_ns
    ):
        raise SystemExit("backpressure event timestamps are not ordered")
    if args.stream_length_before != args.stream_length_during:
        raise SystemExit("Redis stream advanced while WRITE pause was active")
    client_list = args.client_list.read_text(encoding="utf-8")
    if "cmd=xadd" not in client_list.lower():
        raise SystemExit("connector XADD was not observed while WRITE pause was active")

    print(
        json.dumps(
            {
                "mechanism": "redis_client_pause_write",
                "requested_pause_ms": args.pause_ms,
                "pause_started_monotonic_ns": args.pause_start_ns,
                "workload_commits_completed_monotonic_ns": args.workload_committed_ns,
                "connector_xadd_observed_monotonic_ns": args.connector_write_observed_ns,
                "explicit_unpause_monotonic_ns": args.unpause_ns,
                "connector_xadd_observed_during_pause": True,
                "stream_length_before_pause": args.stream_length_before,
                "stream_length_while_connector_blocked": args.stream_length_during,
                "stream_length_after_unpause": args.stream_length_after,
                "transport_errors_observed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
