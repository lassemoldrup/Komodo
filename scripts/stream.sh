#!/usr/bin/env bash
# Read the CDC stream.
#
#   ./scripts/stream.sh              # dump everything so far, readable
#   ./scripts/stream.sh -f           # follow live (Ctrl-C to stop)
#   ./scripts/stream.sh --raw        # raw envelopes, one JSON per line
#   ./scripts/stream.sh -f --raw
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

# Emit "<redis-id>\t<envelope-json>" per entry, then format in python.
emit() {
  if [ "$FOLLOW" = "1" ]; then
    # $ = only entries added after we start blocking.
    docker exec -i cdc-redis redis-cli --timeout 0 XREAD BLOCK 0 COUNT 1000 STREAMS "$STREAM" '$' 2>/dev/null
    return
  fi
  docker exec -i cdc-redis redis-cli XRANGE "$STREAM" - + 2>/dev/null
}

if [ "$FOLLOW" = "1" ]; then
  # redis-cli's blocking XREAD returns one batch then exits, so loop it and
  # track the last id we saw.
  LAST='$'
  while true; do
    OUT=$(docker exec -i cdc-redis redis-cli XREAD BLOCK 0 COUNT 1000 STREAMS "$STREAM" "$LAST" 2>/dev/null || true)
    [ -z "$OUT" ] && continue
    NEW=$(printf '%s\n' "$OUT" | grep -oE '^[0-9]+-[0-9]+$' | tail -1 || true)
    [ -n "$NEW" ] && LAST="$NEW"
    printf '%s\n' "$OUT"
  done
else
  emit
fi | python3 -u -c '
import json, sys

RAW = '"$RAW"'
OPS = {"r": "read", "c": "insert", "u": "update", "d": "delete"}

for line in sys.stdin:
    line = line.strip()
    if not line.startswith("{"):
        continue                      # redis-cli id / stream-name lines
    try:
        e = json.loads(line)
    except ValueError:
        continue
    if not isinstance(e, dict):
        continue
    if "id" in e and len(e) == 1:
        continue                      # the message-key field name

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
    print("  %-6s %-16s before=%s after=%s" % (
        OPS.get(e.get("op"), e.get("op")),
        src.get("table"),
        json.dumps(e.get("before"), separators=(",", ":")),
        json.dumps(e.get("after"), separators=(",", ":")),
    ))
'
