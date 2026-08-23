#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


HEADER = [
    "epoch_ms",
    "monotonic_ns",
    "object_name",
    "queue_total_capacity",
    "queue_remaining_capacity",
    "current_queue_size_bytes",
    "max_queue_size_bytes",
]


def positive(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--expected-capacity", type=positive, required=True)
    args = parser.parse_args()

    lines = args.metrics.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0].split("\t") != HEADER:
        raise SystemExit("queue metrics header differs")
    samples = []
    for line in lines[1:]:
        fields = line.split("\t")
        if len(fields) != len(HEADER):
            raise SystemExit(f"malformed queue metric sample: {line!r}")
        total = int(fields[3])
        remaining = int(fields[4])
        if total != args.expected_capacity or not 0 <= remaining <= total:
            raise SystemExit("queue capacity telemetry differs from configuration")
        samples.append(
            {
                "epoch_ms": int(fields[0]),
                "monotonic_ns": int(fields[1]),
                "object_name": fields[2],
                "queue_total_capacity": total,
                "queue_remaining_capacity": remaining,
                "current_queue_size_bytes": int(fields[5]),
                "max_queue_size_bytes": int(fields[6]),
            }
        )
    if not samples:
        raise SystemExit("queue capacity telemetry has no samples")
    minimum_remaining = min(sample["queue_remaining_capacity"] for sample in samples)
    if minimum_remaining != 0:
        raise SystemExit(
            f"connector queue did not saturate: minimum remaining capacity={minimum_remaining}"
        )
    zero_samples = sum(
        sample["queue_remaining_capacity"] == 0 for sample in samples
    )
    result = {
        "kind": "debezium_connector_queue_saturation_evidence",
        "status": "PASSED",
        "metric_source": "remote_jmx_streaming_connector_mbean",
        "object_names": sorted({sample["object_name"] for sample in samples}),
        "configured_and_observed_total_capacity": args.expected_capacity,
        "minimum_remaining_capacity": minimum_remaining,
        "maximum_observed_occupancy": args.expected_capacity - minimum_remaining,
        "saturated_samples": zero_samples,
        "sample_count": len(samples),
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
