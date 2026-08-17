#!/usr/bin/env bash
# Read the CDC stream. Backend-agnostic: handles both the MariaDB (Debezium
# 3.2) and YugabyteDB (Debezium 2.5.2) envelope shapes.
#
#   ./scripts/stream.sh              # dump everything so far, readable
#   ./scripts/stream.sh -f           # follow live (Ctrl-C to stop)
#   ./scripts/stream.sh --raw        # raw envelopes, one JSON per line
set -euo pipefail

STREAM="${STREAM:-cdc}"
FOLLOW=0; RAW=0
for a in "$@"; do
  case "$a" in
    -f|--follow) FOLLOW=1 ;;
    --raw)       RAW=1 ;;
    *) echo "unknown arg: $a" >&2; exit 2 ;;
  esac
done

if [ "$FOLLOW" = "1" ]; then
  LAST='$'
  while true; do
    OUT=$(docker exec -i cdc-redis redis-cli XREAD BLOCK 0 COUNT 1000 STREAMS "$STREAM" "$LAST" 2>/dev/null || true)
    [ -z "$OUT" ] && continue
    NEW=$(printf '%s\n' "$OUT" | grep -oE '^[0-9]+-[0-9]+$' | tail -1 || true)
    [ -n "$NEW" ] && LAST="$NEW"
    printf '%s\n' "$OUT"
  done
else
  docker exec -i cdc-redis redis-cli XRANGE "$STREAM" - + 2>/dev/null
fi | python3 -u -c '
import json, sys

RAW = '"$RAW"'
OPS = {"r": "read", "c": "insert", "u": "update", "d": "delete"}

def unwrap(v):
    # YugabyteDB wraps every column as {"value": X, "set": true}
    if isinstance(v, dict) and "value" in v and "set" in v:
        return v["value"]
    return v

def row(d):
    if not isinstance(d, dict):
        return d
    return {k: unwrap(v) for k, v in d.items()}

for line in sys.stdin:
    line = line.strip()
    if not line.startswith("{"):
        continue
    try:
        e = json.loads(line)
    except ValueError:
        continue
    if not isinstance(e, dict):
        continue
    # Each Redis entry has one field: the field NAME is the Debezium message
    # key, the field VALUE is the envelope. Only envelopes carry "status"
    # (transaction marker) or "source" (change event); skip the keys.
    if "status" not in e and "source" not in e:
        continue

    if RAW:
        print(json.dumps(e, separators=(",", ":")))
        continue

    if "status" in e:                 # transaction marker
        if e["status"] == "BEGIN":
            print("BEGIN %s" % e["id"])
        else:
            dc = " ".join("%s=%s" % (d["data_collection"], d["event_count"])
                          for d in (e.get("data_collections") or []))
            print("END   %s  events=%s  %s" % (e["id"], e["event_count"], dc))
        continue

    src = e.get("source") or {}
    before, after = row(e.get("before")), row(e.get("after"))
    table = src.get("table")

    # Surface the exploratory YugabyteDB/Oracle session marker. The frozen
    # MariaDB path joins directly on GTID and never emits this table.
    if (table or "").lower() == "session_txn":
        def fld(d, name):
            d = d or {}
            for k, v in d.items():
                if k.lower() == name:
                    return v
            return None
        print("  MARKER  session=%s txn_id=%s (prev=%s)" % (
            fld(after, "session_id"), fld(after, "txn_id"),
            fld(before, "txn_id")))
        continue

    print("  %-6s %-14s before=%s after=%s" % (
        OPS.get(e.get("op"), e.get("op")), table,
        json.dumps(before, separators=(",", ":")),
        json.dumps(after, separators=(",", ":"))))
'
