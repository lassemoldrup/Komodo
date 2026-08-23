#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


TABLET = re.compile(
    r"^([0-9a-f]{32})\s+(.+?)\s+(\S+:\d+)\s+([0-9a-f]{32})$"
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", action="append", default=[], metavar="NAME=PATH")
    parser.add_argument("--key-hashes", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    tables: dict[str, list[dict[str, str]]] = {}
    for value in args.table:
        name, separator, raw_path = value.partition("=")
        if not separator or not name or name in tables:
            raise SystemExit(f"invalid or duplicate --table value: {value!r}")
        rows = []
        for line in Path(raw_path).read_text(encoding="utf-8").splitlines()[1:]:
            match = TABLET.fullmatch(line.strip())
            if match is not None:
                rows.append(
                    {
                        "tablet_id": match.group(1),
                        "range": match.group(2),
                        "leader_address": match.group(3),
                        "leader_id": match.group(4),
                    }
                )
        if not rows:
            raise SystemExit(f"no tablet rows parsed for {name}")
        tables[name] = rows

    key_hashes: dict[str, int] = {}
    for line in args.key_hashes.read_text(encoding="utf-8").splitlines():
        fields = line.rstrip("\r").split("\t")
        if len(fields) != 2 or not fields[0] or fields[0] in key_hashes:
            raise SystemExit(f"invalid key hash row: {line!r}")
        key_hashes[fields[0]] = int(fields[1])

    document = {
        "kind": "yugabytedb_tablet_placement",
        "tables": tables,
        "key_hashes": key_hashes,
    }
    args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
