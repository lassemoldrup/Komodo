#!/usr/bin/env bash
# Measure commit throughput and commit->stream latency.
#
#   ./scripts/bench.sh [N]       # default 1000 transactions
#
# Latency is measured server-side on both ends to avoid docker-exec overhead:
#   * commit time  = SYSDATE(3) read on the same session right after COMMIT
#   * marker arrival time = the Redis stream entry ID for the final transaction's
#     session_txn row, which is a millisecond epoch stamped by Redis at XADD.
# Both clocks come from the same kernel, so the difference is real.
set -euo pipefail

N="${1:-1000}"
cd "$(dirname "$0")/.."

python3 - "$N" <<'PY'
import json, subprocess, sys, time, uuid

N = int(sys.argv[1])
if N <= 0:
    raise SystemExit("transaction count must be positive")

def mariadb(sql, user="app", pw="app_pw"):
    completed = subprocess.run(
        ["docker", "exec", "-i", "cdc-mariadb", "mariadb", "--skip-ssl",
         f"-u{user}", f"-p{pw}", "benchbase", "-N", "-B"],
        input=sql, capture_output=True, text=True)
    completed.check_returncode()
    return completed.stdout

def redis(*args, json_output=False):
    command = ["docker", "exec", "cdc-redis", "redis-cli"]
    if json_output:
        command.append("--json")
    completed = subprocess.run([*command, *args], capture_output=True, text=True)
    completed.check_returncode()
    return completed.stdout

def final_marker(entries, session_id, txn_id):
    for stream_id, fields in entries:
        if len(fields) != 2:
            continue
        try:
            envelope = json.loads(fields[1])
        except (TypeError, json.JSONDecodeError):
            continue
        source = envelope.get("source") or {}
        after = envelope.get("after") or {}
        if (source.get("table") == "session_txn"
                and after.get("session_id") == session_id
                and int(after.get("txn_id", -1)) == txn_id):
            return stream_id
    return None

# --- Throughput -----------------------------------------------------------
run_token = uuid.uuid4()
session_id = f"bench/session/{run_token}"
seed_session_id = f"bench/seed/{run_token}"
initial = ["START TRANSACTION;"]
for key in range(1, 5):
    initial.append(
        "INSERT INTO usertable (ycsb_key, field1) VALUES (%d,'a') "
        "ON DUPLICATE KEY UPDATE field1='a';" % key)
initial.append(
    "INSERT INTO session_txn (session_id, txn_id) VALUES ('%s', 1); COMMIT;"
    % seed_session_id)
mariadb("\n".join(initial))

stmts = []
for i in range(N):
    stmts.append(
        "START TRANSACTION; UPDATE usertable "
        "SET field1=IF(field1='a','b','a') WHERE ycsb_key=%d; "
        "INSERT INTO session_txn (session_id, txn_id) VALUES ('%s', %d); COMMIT;"
        % (i % 4 + 1, session_id, i + 1))
stmts.append("SELECT ROUND(UNIX_TIMESTAMP(SYSDATE(3))*1000);")

t0 = time.time()
out = mariadb("\n".join(stmts))
t1 = time.time()

last_commit_ms = float(out.strip())

# --- Wait for the stream to catch up --------------------------------------
deadline = time.time() + 60
marker_stream_id = None
while time.time() < deadline:
    tail = json.loads(redis("XREVRANGE", "cdc", "+", "-", "COUNT", "100", json_output=True))
    marker_stream_id = final_marker(tail, session_id, N)
    if marker_stream_id is not None:
        break
    time.sleep(0.02)
t2 = time.time()
arrival_ms = float(marker_stream_id.split("-", 1)[0]) if marker_stream_id else None

wall = t1 - t0
print(f"transactions          : {N}")
print(f"commit throughput     : {N/wall:,.0f} txn/s  ({wall:.2f}s, single session)")
print(f"stream entries        : {redis('XLEN','cdc').strip()}")
print(f"backlog drain         : {t2-t1:.2f}s after last commit")
if arrival_ms:
    print(f"commit -> marker (last txn): {arrival_ms - last_commit_ms:.0f} ms")
print(f"final correlation     : session_id={session_id} txn_id={N}")
PY
