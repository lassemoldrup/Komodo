#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
repo_root=$(cd -- "$script_dir/../.." && pwd)
readonly repo_root
readonly output_dir=${1:-"$script_dir/fixtures/redis_server_failure_gate"}
readonly expected_maxmemory=268435456

if [[ -e $output_dir ]]; then
  echo "Redis failure gate output already exists: $output_dir" >&2
  exit 2
fi
mkdir -p -- "$output_dir"

exec {live_lock_fd}<"$repo_root"
flock --nonblock "$live_lock_fd" || {
  echo "another destructive live suite owns the repository stack lock" >&2
  exit 1
}

compose() {
  docker compose --file "$script_dir/docker-compose.yml" "$@"
}

redis() {
  compose exec -T redis redis-cli "$@"
}

cleanup() {
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

compose down --volumes --remove-orphans
compose up --detach redis
deadline=$((SECONDS + 60))
until [[ $(redis --raw PING 2>/dev/null || true) == PONG ]]; do
  (( SECONDS < deadline )) || {
    echo "Redis did not become ready" >&2
    exit 1
  }
  sleep 1
done

compose config >"$output_dir/docker-compose-config.yml"
docker inspect connector-contract-redis >"$output_dir/redis-container.json"
docker image inspect \
  docker.io/library/redis@sha256:9702d01c1f10c3ea9f48211b4362e44f154ff02d063e6f7268eba804059f53bf \
  >"$output_dir/redis-image.json"
redis --raw INFO memory >"$output_dir/redis-memory-before.txt"
redis --raw CONFIG GET maxmemory maxmemory-policy \
  >"$output_dir/redis-configuration-before.txt"

redis --raw CONFIG SET maxmemory-policy allkeys-lru \
  >"$output_dir/wrong-policy-injection.txt"
(
  export BLACKBOX_BACKEND=yugabytedb_ysql
  # shellcheck source=scripts/blackbox_lifecycle.sh
  . "$repo_root/scripts/blackbox_lifecycle.sh"
  blackbox_redis() {
    compose exec -T redis redis-cli "$@"
  }
  if blackbox_verify_redis_configuration "$expected_maxmemory"; then
    echo "wrong Redis maxmemory policy unexpectedly passed admission" >&2
    exit 1
  fi
) >"$output_dir/wrong-policy-admission.log" 2>&1
grep -q 'Redis maxmemory-policy must be noeviction, found: allkeys-lru' \
  "$output_dir/wrong-policy-admission.log"

redis --raw CONFIG SET maxmemory-policy noeviction \
  >"$output_dir/noeviction-restore.txt"
used_memory=$(redis --raw INFO memory | tr -d '\r' | awk -F: '$1 == "used_memory" {print $2}')
[[ $used_memory =~ ^[1-9][0-9]*$ ]]
readonly oom_maxmemory=1
redis --raw CONFIG SET maxmemory "$oom_maxmemory" \
  >"$output_dir/oom-capacity-injection.txt"
set +e
redis --raw XADD cdc '*' payload actual-server-oom \
  >"$output_dir/oom-xadd-response.txt" 2>&1
oom_status=$?
set -e
grep -q '^OOM command not allowed when used memory > .maxmemory' \
  "$output_dir/oom-xadd-response.txt"
[[ $(redis --raw XLEN cdc | tr -d '[:space:]') == 0 ]]
redis --raw INFO memory >"$output_dir/redis-memory-at-oom.txt"

redis --raw CONFIG SET maxmemory "$expected_maxmemory" \
  >"$output_dir/capacity-restore.txt"
(
  export BLACKBOX_BACKEND=yugabytedb_ysql
  # shellcheck source=scripts/blackbox_lifecycle.sh
  . "$repo_root/scripts/blackbox_lifecycle.sh"
  blackbox_redis() {
    compose exec -T redis redis-cli "$@"
  }
  blackbox_verify_redis_configuration "$expected_maxmemory"
) >"$output_dir/restored-admission.log" 2>&1
redis --raw CONFIG GET maxmemory maxmemory-policy \
  >"$output_dir/redis-configuration-restored.txt"

cp -- "$script_dir/run_redis_failure_gate.sh" "$output_dir/runner.sh"
cp -- "$repo_root/scripts/blackbox_lifecycle.sh" "$output_dir/blackbox_lifecycle.sh"
git -C "$repo_root" rev-parse HEAD >"$output_dir/si-monitoring-head.txt"
jq -n \
  --argjson configured_maxmemory "$expected_maxmemory" \
  --argjson used_memory_before_oom "$used_memory" \
  --argjson oom_injected_maxmemory "$oom_maxmemory" \
  --argjson oom_command_exit_status "$oom_status" \
  --rawfile oom_response "$output_dir/oom-xadd-response.txt" \
  '{
    kind: "redis_server_failure_acceptance",
    status: "PASSED",
    wrong_noeviction_policy_rejected: true,
    actual_server_xadd_oom_observed: true,
    stream_length_after_rejected_xadd: 0,
    configured_maxmemory: $configured_maxmemory,
    used_memory_before_oom: $used_memory_before_oom,
    oom_injected_maxmemory: $oom_injected_maxmemory,
    oom_command_exit_status: $oom_command_exit_status,
    oom_response: ($oom_response | rtrimstr("\n"))
  }' >"$output_dir/redis-failure-report.json"

(
  cd -- "$output_dir"
  find . -type f ! -name SHA256SUMS -printf '%P\0' \
    | LC_ALL=C sort -z | xargs -0 sha256sum >SHA256SUMS
  sha256sum --check SHA256SUMS
)

compose down --volumes --remove-orphans
trap - EXIT
echo "Redis server failure evidence preserved at $output_dir"
