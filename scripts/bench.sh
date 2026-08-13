#!/usr/bin/env bash
# Measure commit throughput and commit->stream latency.
#
#   ./scripts/bench.sh [N]       # default 1000 transactions
#
# Latency is measured server-side on both ends to avoid docker-exec overhead:
#   * commit time  = SYSDATE(3) read on the same session right after COMMIT
#   * arrival time = the Redis stream entry ID, which is a millisecond epoch
#     stamped by Redis at XADD.
# Both clocks come from the same kernel, so the difference is real.
set -euo pipefail

N="${1:-1000}"
cd "$(dirname "$0")/.."

python3 - "$N" <<'PY'
import json, subprocess, sys, time

N = int(sys.argv[1])

def mariadb(sql, user="app", pw="app_pw"):
    return subprocess.run(
        ["docker", "exec", "-i", "cdc-mariadb", "mariadb", "--skip-ssl",
         f"-u{user}", f"-p{pw}", "sitest", "-N", "-B"],
        input=sql, capture_output=True, text=True).stdout

def redis(*args):
    return subprocess.run(["docker", "exec", "cdc-redis", "redis-cli", *args],
                          capture_output=True, text=True).stdout

# --- Throughput -----------------------------------------------------------
stmts = []
for i in range(N):
    stmts.append(
        "START TRANSACTION; UPDATE registers SET val=val+1 WHERE id=%d; COMMIT;"
        % (i % 4 + 1))
# Report each commit's GTID and the wall-clock ms at which it committed.
stmts.append("SELECT CONCAT(@@session.last_gtid,' ',ROUND(UNIX_TIMESTAMP(SYSDATE(3))*1000));")

t0 = time.time()
out = mariadb("\n".join(stmts))
t1 = time.time()

last_gtid, last_commit_ms = out.strip().split()
last_commit_ms = float(last_commit_ms)

# --- Wait for the stream to catch up --------------------------------------
deadline = time.time() + 60
while time.time() < deadline:
    tail = redis("XREVRANGE", "cdc", "+", "-", "COUNT", "1")
    if last_gtid in tail:
        break
    time.sleep(0.02)
t2 = time.time()

# --- Locate the END marker for the final transaction ----------------------
raw = redis("XREVRANGE", "cdc", "+", "-", "COUNT", "40").splitlines()
arrival_ms = None
for i, line in enumerate(raw):
    if '"status":"END"' in line and f'"id":"{last_gtid}"' in line:
        for j in range(i, -1, -1):
            s = raw[j].strip()
            if "-" in s and s.split("-")[0].isdigit():
                arrival_ms = float(s.split("-")[0])
                break
        break

wall = t1 - t0
print(f"transactions          : {N}")
print(f"commit throughput     : {N/wall:,.0f} txn/s  ({wall:.2f}s, single session)")
print(f"stream entries        : {redis('XLEN','cdc').strip()}")
print(f"backlog drain         : {t2-t1:.2f}s after last commit")
if arrival_ms:
    print(f"commit -> stream (last txn): {arrival_ms - last_commit_ms:.0f} ms")
print(f"final txn GTID        : {last_gtid}")
PY
