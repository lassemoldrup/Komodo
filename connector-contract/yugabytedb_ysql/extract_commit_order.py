#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


ACKNOWLEDGEMENT = re.compile(r"^ACK ([A-Za-z0-9][A-Za-z0-9._:/-]*) ([1-9][0-9]*)$")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("log", nargs="+", type=Path)
    args = parser.parse_args()

    completed: list[dict[str, object]] = []
    sources: list[str] = []
    for path in args.log:
        sources.append(path.name)
        matches = []
        for line in path.read_text(encoding="utf-8").splitlines():
            match = ACKNOWLEDGEMENT.fullmatch(line.strip())
            if match is not None:
                matches.append(
                    {"session_id": match.group(1), "txn_id": int(match.group(2))}
                )
        if not matches:
            raise SystemExit(f"commit acknowledgement is absent from {path}")
        completed.extend(matches)

    print(
        json.dumps(
            {
                "source": "observed_ysqlsh_commit_acknowledgements",
                "source_logs_in_completion_order": sources,
                "completed_transactions": completed,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
