#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
export BLACKBOX_BACKEND=mariadb
export BLACKBOX_CORRECTNESS_DIAGNOSTIC_TIMEOUT_SECONDS=1
export BLACKBOX_CORRECTNESS_DIAGNOSTIC_KILL_AFTER_SECONDS=1
# shellcheck source=tests/test_semantic_correctness.sh
. "$ROOT/tests/test_semantic_correctness.sh"

hanging_function() {
  sleep 30
}

started=$SECONDS
set +e
run_bounded_diagnostic bash -c 'sleep 30'
diagnostic_status=$?
run_bounded_function hanging_function
function_status=$?
set -e

[[ $diagnostic_status == 124 ]]
[[ $function_status == 124 ]]
(( SECONDS - started < 8 ))
echo "semantic evidence timeout tests passed"
