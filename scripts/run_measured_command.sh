#!/usr/bin/env bash
# Run one workload command while sampling its process tree and relevant containers.

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
ANALYZER="$ROOT/scripts/blackbox_performance.py"

RESOURCE_OUTPUT=
NETWORK_OUTPUT=
SAMPLE_DURATION_SECONDS=
SAMPLE_INTERVAL_SECONDS=0.25
PROCESS_LABEL=benchbase
REDIS_HOST=${BLACKBOX_REDIS_HOST:-host.docker.internal}
REDIS_PORT=${BLACKBOX_REDIS_PORT:-6379}
CONTAINERS=()
COMMAND_PID=
SAMPLER_PID=
STOP_FILE=


usage() {
  cat >&2 <<'EOF'
usage: run_measured_command.sh --resource-output PATH --network-output PATH
       --sample-duration-seconds N [--sample-interval-seconds N]
       [--process-label LABEL] [--container [LABEL=]NAME]... -- COMMAND [ARG]...
EOF
}


# shellcheck disable=SC2317
cleanup() {
  local status=$?
  if [[ -n $STOP_FILE ]]; then
    : >"$STOP_FILE"
  fi
  if [[ -n $SAMPLER_PID ]]; then
    wait "$SAMPLER_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n $COMMAND_PID ]] && kill -0 "$COMMAND_PID" >/dev/null 2>&1; then
    kill "$COMMAND_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n $STOP_FILE ]]; then
    rm -f -- "$STOP_FILE"
  fi
  return "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM


redis_counter() {
  local name=$1
  { redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" --raw INFO stats 2>/dev/null || true; } |
    awk -F: -v key="$name" '$1 == key { gsub(/\r/, "", $2); print $2; found=1 } END { if (!found) print 0 }'
}


redis_available() {
  [[ $(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" --raw PING 2>/dev/null || true) == PONG ]]
}


write_network_metrics() {
  local available_before=$1
  local available_after=$2
  local input_before=$3
  local output_before=$4
  local input_after=$5
  local output_after=$6
  python3 - "$NETWORK_OUTPUT" "$available_before" "$available_after" \
    "$input_before" "$output_before" "$input_after" "$output_after" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
available_before = sys.argv[2] == "true"
available_after = sys.argv[3] == "true"
available = available_before and available_after
values = [int(value) if value else None for value in sys.argv[4:]]
document = {
    "available": available,
    "available_before": available_before,
    "available_after": available_after,
    "total_net_input_bytes_before": values[0],
    "total_net_output_bytes_before": values[1],
    "total_net_input_bytes_after": values[2],
    "total_net_output_bytes_after": values[3],
    "input_bytes_delta": values[2] - values[0] if available else None,
    "output_bytes_delta": values[3] - values[1] if available else None,
}
path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}


while (( $# > 0 )); do
  case "$1" in
    --resource-output)
      RESOURCE_OUTPUT=${2:?missing --resource-output value}
      shift 2
      ;;
    --network-output)
      NETWORK_OUTPUT=${2:?missing --network-output value}
      shift 2
      ;;
    --sample-duration-seconds)
      SAMPLE_DURATION_SECONDS=${2:?missing --sample-duration-seconds value}
      shift 2
      ;;
    --sample-interval-seconds)
      SAMPLE_INTERVAL_SECONDS=${2:?missing --sample-interval-seconds value}
      shift 2
      ;;
    --process-label)
      PROCESS_LABEL=${2:?missing --process-label value}
      shift 2
      ;;
    --container)
      CONTAINERS+=("${2:?missing --container value}")
      shift 2
      ;;
    --)
      shift
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

[[ -n $RESOURCE_OUTPUT && -n $NETWORK_OUTPUT && -n $SAMPLE_DURATION_SECONDS && $# -gt 0 ]] || {
  usage
  exit 2
}
[[ $SAMPLE_DURATION_SECONDS =~ ^[0-9]+([.][0-9]+)?$ ]]
[[ $SAMPLE_INTERVAL_SECONDS =~ ^[0-9]+([.][0-9]+)?$ ]]
mkdir -p -- "$(dirname "$RESOURCE_OUTPUT")" "$(dirname "$NETWORK_OUTPUT")"
STOP_FILE="$RESOURCE_OUTPUT.stop"
rm -f -- "$STOP_FILE"

available_before=false
input_before=
output_before=
if redis_available; then
  available_before=true
  input_before=$(redis_counter total_net_input_bytes)
  output_before=$(redis_counter total_net_output_bytes)
fi

# The lifecycle coordinator owns the surrounding process session and can
# therefore terminate this command and all of its descendants at its deadline.
"$@" &
COMMAND_PID=$!

sampler_args=(
  sample-resources
  --process "$PROCESS_LABEL:$COMMAND_PID"
  --interval-seconds "$SAMPLE_INTERVAL_SECONDS"
  --duration-seconds "$SAMPLE_DURATION_SECONDS"
  --stop-file "$STOP_FILE"
  --output "$RESOURCE_OUTPUT"
)
for container in "${CONTAINERS[@]}"; do
  sampler_args+=(--container "$container")
done
python3 "$ANALYZER" "${sampler_args[@]}" &
SAMPLER_PID=$!

command_status=0
wait "$COMMAND_PID" || command_status=$?
COMMAND_PID=
: >"$STOP_FILE"
wait "$SAMPLER_PID" || {
  sampler_status=$?
  SAMPLER_PID=
  exit "$sampler_status"
}
SAMPLER_PID=

available_after=false
input_after=
output_after=
if redis_available; then
  available_after=true
  input_after=$(redis_counter total_net_input_bytes)
  output_after=$(redis_counter total_net_output_bytes)
fi
write_network_metrics "$available_before" "$available_after" \
  "$input_before" "$output_before" "$input_after" "$output_after"
exit "$command_status"
