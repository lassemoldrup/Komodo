#!/usr/bin/env bash

set -Eeuo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
HELPER="$ROOT/scripts/blackbox_lifecycle.sh"
TEST_ROOT=$(mktemp -d /tmp/blackbox-live-lock.XXXXXX)
LOCK_TARGET="$TEST_ROOT/target"
OWNER_READY="$TEST_ROOT/owner-ready"
OWNER_RELEASE="$TEST_ROOT/owner-release"
OWNER_PID=


fail() {
  printf 'live lock test failed: %s\n' "$*" >&2
  exit 1
}


cleanup() {
  : >"$OWNER_RELEASE"
  if [[ -n $OWNER_PID ]]; then
    wait "$OWNER_PID" >/dev/null 2>&1 || true
  fi
  rm -rf -- "$TEST_ROOT"
}
trap cleanup EXIT

mkdir -p "$LOCK_TARGET"

(
  export BLACKBOX_LIVE_LOCK_TARGET=$LOCK_TARGET
  # shellcheck source=scripts/blackbox_lifecycle.sh
  . "$HELPER"
  blackbox_acquire_live_stack_lock
  : >"$OWNER_READY"
  while [[ ! -e $OWNER_RELEASE ]]; do
    sleep 0.01
  done
) &
OWNER_PID=$!

for _ in {1..500}; do
  if [[ -e $OWNER_READY ]]; then
    break
  fi
  if ! kill -0 "$OWNER_PID" >/dev/null 2>&1; then
    fail 'lock owner exited before reporting readiness'
  fi
  sleep 0.01
done
[[ -e $OWNER_READY ]] || fail 'timed out waiting for lock owner'

# The single-quoted program must expand its positional parameters in the child.
# shellcheck disable=SC2016
if timeout --foreground 2 bash -c '
    set -Eeuo pipefail
    export BLACKBOX_LIVE_LOCK_TARGET=$1
    . "$2"
    blackbox_acquire_live_stack_lock
  ' _ "$LOCK_TARGET" "$HELPER" 2>"$TEST_ROOT/contender.err"; then
  fail 'independent contender acquired an owned lock'
fi
grep -Fq 'another destructive live suite owns the repository stack lock' \
  "$TEST_ROOT/contender.err" || fail 'contention diagnostic was not emitted'

: >"$OWNER_RELEASE"
wait "$OWNER_PID"
OWNER_PID=

# The outer and inner programs both expand positional parameters in their own
# shells while sharing the exported lock descriptor.
# shellcheck disable=SC2016
timeout --foreground 2 bash -c '
  set -Eeuo pipefail
  export BLACKBOX_LIVE_LOCK_TARGET=$1
  . "$2"
  blackbox_acquire_live_stack_lock
  bash -c '\''
    set -Eeuo pipefail
    . "$1"
    blackbox_acquire_live_stack_lock
  '\'' _ "$2"
' _ "$LOCK_TARGET" "$HELPER"

printf 'live stack lock tests passed\n'
