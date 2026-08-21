#!/usr/bin/env bash
# Live one-warehouse RR acceptance run for the standard TPC-C transaction mix.

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
TPCC_ACCEPTANCE_ROOT=$ROOT
TPCC_ACCEPTANCE_ENV_PREFIX=BLACKBOX_TPCC_FULL_MIX
TPCC_ACCEPTANCE_SLUG=full-mix
TPCC_ACCEPTANCE_CONFIG_KIND=tpcc_full_mix_acceptance_config
TPCC_ACCEPTANCE_DEFAULT_VALIDATOR="$ROOT/tests/verify_tpcc_full_mix_acceptance.py"
TPCC_ACCEPTANCE_DEFAULT_WEIGHTS=45,43,4,4,4
TPCC_ACCEPTANCE_DEFAULT_TERMINALS=4
TPCC_ACCEPTANCE_DEFAULT_RANDOM_SEED=3
TPCC_ACCEPTANCE_DEFAULT_DURATION_SECONDS=20
TPCC_ACCEPTANCE_DEFAULT_LOADER_THREADS=1
# shellcheck source=tests/tpcc_live_acceptance.sh
. "$ROOT/tests/tpcc_live_acceptance.sh"
tpcc_acceptance_init

MINIMUM_PER_TYPE=$(tpcc_acceptance_setting MINIMUM_PER_TYPE 1)
REPLAY_CAPTURE_MAX_BYTES=$(tpcc_acceptance_setting REPLAY_CAPTURE_MAX_BYTES 4294967296)
RESOURCE_EXHAUSTION_METRICS=$(tpcc_acceptance_setting RESOURCE_EXHAUSTION_METRICS "")


usage() {
  cat <<'EOF'
Usage: run_tpcc_full_mix_acceptance.sh [options]

Required:
  --benchbase-dist PATH
  --monitor-bin PATH
  --resource-exhaustion-metrics PATH

Profile controls:
  --minimum-per-type N
  --replay-capture-max-bytes N

Common controls (matching BLACKBOX_TPCC_FULL_MIX_* environment variables):
EOF
  tpcc_acceptance_common_usage
  printf '  --help\n'
}


while (( $# > 0 )); do
  if tpcc_acceptance_parse_common_option "$@"; then
    shift "$TPCC_ACCEPTANCE_PARSED_ARGUMENTS"
    continue
  fi
  case "$1" in
    --minimum-per-type)
      tpcc_acceptance_need_value "$@"; MINIMUM_PER_TYPE=$2; shift 2 ;;
    --replay-capture-max-bytes)
      tpcc_acceptance_need_value "$@"; REPLAY_CAPTURE_MAX_BYTES=$2; shift 2 ;;
    --resource-exhaustion-metrics)
      tpcc_acceptance_need_value "$@"; RESOURCE_EXHAUSTION_METRICS=$2; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done


tpcc_acceptance_validate_profile_controls() {
  [[ $TPCC_WAREHOUSES == 1 && $TPCC_TERMINALS == 4 ]] || {
    printf 'standard-mix acceptance requires exactly one warehouse and four terminals\n' >&2
    exit 2
  }
  [[ $TPCC_WEIGHTS == 45,43,4,4,4 ]] || {
    printf 'weights must be exactly 45,43,4,4,4\n' >&2
    exit 2
  }
  tpcc_acceptance_require_positive minimum-per-type "$MINIMUM_PER_TYPE"
  tpcc_acceptance_require_positive replay-capture-max-bytes "$REPLAY_CAPTURE_MAX_BYTES"
  [[ -f $RESOURCE_EXHAUSTION_METRICS ]] || {
    printf 'resource-exhaustion metrics file is missing: %s\n' "$RESOURCE_EXHAUSTION_METRICS" >&2
    exit 2
  }
}


tpcc_acceptance_config_extras() {
  local resource_sha
  resource_sha=$(sha256sum "$RESOURCE_EXHAUSTION_METRICS" | awk '{print $1}')
  python3 - "$MINIMUM_PER_TYPE" "$REPLAY_CAPTURE_MAX_BYTES" \
    "$RESOURCE_EXHAUSTION_METRICS" "$resource_sha" <<'PY'
import json
import sys

minimum, maximum, path, checksum = sys.argv[1:]
print(json.dumps({
    "minimum_per_type": int(minimum),
    "accepted_outcomes": ["PASS", "VIOLATION"],
    "replay_capture_max_bytes": int(maximum),
    "resource_exhaustion_metrics": {"path": path, "sha256": checksum},
}, separators=(",", ":")))
PY
}


tpcc_acceptance_capture_profile_artifacts() {
  local output_dir=$1
  local full_cdc="$output_dir/cdc-full.json"
  local manifest="$output_dir/replay-manifest.json"
  blackbox_redis --json XRANGE cdc - + >"$full_cdc"
  local cdc_bytes
  cdc_bytes=$(stat --format=%s "$full_cdc")
  (( cdc_bytes <= REPLAY_CAPTURE_MAX_BYTES )) || {
    printf 'full CDC replay capture exceeds configured byte limit\n' >&2
    return 1
  }
  python3 - "$manifest" "$output_dir/run-id.txt" "$full_cdc" \
    "$output_dir/client-stream.json" "$REPLAY_CAPTURE_MAX_BYTES" <<'PY'
import hashlib
import json
import os
import sys
from pathlib import Path

manifest, run_id_path, cdc_path, client_path = map(Path, sys.argv[1:5])
maximum_bytes = int(sys.argv[5])
def metadata(path):
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return {
        "path": str(path),
        "sha256": digest.hexdigest(),
        "bytes": size,
    }
document = {
    "kind": "tpcc_full_mix_replay",
    "run_id": run_id_path.read_text(encoding="ascii").strip(),
    "capture_max_bytes": maximum_bytes,
    "streams": {"cdc": metadata(cdc_path), "client": metadata(client_path)},
}
if any(item["bytes"] > maximum_bytes for item in document["streams"].values()):
    raise SystemExit("replay stream exceeds configured capture bound")
with manifest.open("x", encoding="utf-8") as output:
    json.dump(document, output, sort_keys=True, separators=(",", ":"))
    output.write("\n")
PY
  TPCC_ACCEPTANCE_VALIDATOR_EXTRA_ARGS=(
    --benchbase-log "$output_dir/e2e.log"
    --loader-validation "$output_dir/loader-validation.json"
    --replay-manifest "$manifest"
  )
}


tpcc_acceptance_run
