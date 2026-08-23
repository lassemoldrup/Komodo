#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
repo_root=$(cd -- "$script_dir/../.." && pwd)
readonly repo_root
readonly output_dir=${1:-"$script_dir/fixtures/redis_audited_failure_gate"}
readonly expected_maxmemory=268435456
: "${BLACKBOX_MONITOR_BIN:?set BLACKBOX_MONITOR_BIN to blackbox_run_monitor}"
: "${BLACKBOX_EVIDENCE_REPLAY_BIN:?set BLACKBOX_EVIDENCE_REPLAY_BIN}"
: "${BLACKBOX_BENCHBASE_DIST:?set BLACKBOX_BENCHBASE_DIST}"
[[ -x $BLACKBOX_MONITOR_BIN ]]
[[ -x $BLACKBOX_EVIDENCE_REPLAY_BIN ]]
[[ -f $BLACKBOX_BENCHBASE_DIST/benchbase.jar ]]
[[ -d $BLACKBOX_BENCHBASE_DIST/lib ]]
if [[ -e $output_dir ]]; then
  echo "Redis audited failure output already exists: $output_dir" >&2
  exit 2
fi
mkdir -p -- "$output_dir"

exec {BLACKBOX_LIVE_LOCK_FD}<"$repo_root"
export BLACKBOX_LIVE_LOCK_FD
flock --nonblock "$BLACKBOX_LIVE_LOCK_FD" || {
  echo "another destructive live suite owns the repository stack lock" >&2
  exit 1
}

contract_compose() {
  docker compose --file "$script_dir/docker-compose.yml" "$@"
}

main_compose() {
  docker compose --file "$repo_root/docker-compose.yml" \
    --profile yugabytedb_ysql "$@"
}

cleanup() {
  contract_compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  main_compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Prove independently that admission rejects a valid Redis server configured
# with the wrong eviction policy. Remove this stack before the audited run.
main_compose down --volumes --remove-orphans
contract_compose down --volumes --remove-orphans
contract_compose up --detach redis
deadline=$((SECONDS + 60))
until [[ $(contract_compose exec -T redis redis-cli --raw PING 2>/dev/null || true) == PONG ]]; do
  (( SECONDS < deadline )) || {
    echo "Redis did not become ready" >&2
    exit 1
  }
  sleep 1
done
contract_compose exec -T redis redis-cli --raw CONFIG SET maxmemory-policy allkeys-lru \
  >"$output_dir/wrong-policy-injection.txt"
(
  export BLACKBOX_BACKEND=yugabytedb_ysql
  # shellcheck source=scripts/blackbox_lifecycle.sh
  . "$repo_root/scripts/blackbox_lifecycle.sh"
  blackbox_redis() {
    contract_compose exec -T redis redis-cli "$@"
  }
  if blackbox_verify_redis_configuration "$expected_maxmemory"; then
    echo "wrong Redis maxmemory policy unexpectedly passed admission" >&2
    exit 1
  fi
) >"$output_dir/wrong-policy-admission.log" 2>&1
grep -q 'Redis maxmemory-policy must be noeviction, found: allkeys-lru' \
  "$output_dir/wrong-policy-admission.log"
contract_compose config >"$output_dir/policy-docker-compose-config.yml"
docker inspect connector-contract-redis >"$output_dir/policy-redis-container.json"
contract_compose down --volumes --remove-orphans

# The OOM now occurs inside the audited coordinator/connector/monitor
# lifecycle. This returns success only after capturing one monitor FINAL INVALID.
BLACKBOX_BACKEND=yugabytedb_ysql \
BLACKBOX_CORRECTNESS_RUN_OUTPUT=$output_dir \
BLACKBOX_MONITOR_BIN=$BLACKBOX_MONITOR_BIN \
BLACKBOX_EVIDENCE_REPLAY_BIN=$BLACKBOX_EVIDENCE_REPLAY_BIN \
BLACKBOX_BENCHBASE_DIST=$BLACKBOX_BENCHBASE_DIST \
  "$repo_root/tests/test_semantic_correctness.sh" --run-case \
    redis_oom_invalid REPEATABLE_READ clean '' INVALID 1 old old \
    ycsb_fractured_read_probe redis_oom

final_status=$(jq -cer '
  [.[] | (.[1][1] | fromjson) | select(.lifecycle_state == "FINAL")] |
  if length == 1 then .[0] else error("expected one FINAL INVALID") end
' "$output_dir/redis_oom_invalid-status.json")
jq -e '
  .outcome == "INVALID" and
  .reconciliation.evidence_reconciled == false and
  any(.diagnostics[]; .code == "RESOURCE_EXHAUSTED") and
  any(.diagnostics[]; .code == "SCHEMA_ATTESTATION_MISSING") and
  any(.diagnostics[]; .code == "RECONCILIATION_MISMATCH")
' <<<"$final_status" >/dev/null
grep -q 'OOM command not allowed' "$output_dir/redis_oom_invalid-operation.txt"

cp -- "$script_dir/run_redis_failure_gate.sh" "$output_dir/runner.sh"
cp -- "$repo_root/tests/operational_failure_client.sh" \
  "$output_dir/operational-failure-client.sh"
cp -- "$repo_root/tests/test_semantic_correctness.sh" \
  "$output_dir/semantic-correctness-runner.sh"
cp -- "$repo_root/scripts/e2e.sh" "$output_dir/e2e-coordinator.sh"
git -C "$repo_root" rev-parse HEAD >"$output_dir/si-monitoring-head.txt"
sha256sum "$BLACKBOX_MONITOR_BIN" "$BLACKBOX_EVIDENCE_REPLAY_BIN" \
  "$BLACKBOX_BENCHBASE_DIST/benchbase.jar" \
  >"$output_dir/runtime-artifact-sha256.txt"
jq -n --argjson final "$final_status" '{
  kind: "redis_audited_failure_acceptance",
  status: "PASSED",
  wrong_noeviction_policy_rejected: true,
  actual_server_xadd_oom_observed: true,
  authoritative_final: $final,
  consistency_verdict_claimed: false
}' >"$output_dir/redis-failure-report.json"

(
  cd -- "$output_dir"
  find . -type f ! -name SHA256SUMS -printf '%P\0' \
    | LC_ALL=C sort -z | xargs -0 sha256sum >SHA256SUMS
  sha256sum --check SHA256SUMS
)

main_compose down --volumes --remove-orphans
trap - EXIT
echo "audited Redis failure evidence preserved at $output_dir"
