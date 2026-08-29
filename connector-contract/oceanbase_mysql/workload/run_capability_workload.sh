#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
output=${1:?output directory is required}
source_container=${OCEANBASE_MYSQL_SOURCE_CONTAINER:-oceanbase-mysql-capability-source}
redis_container=${OCEANBASE_MYSQL_REDIS_CONTAINER:-oceanbase-mysql-capability-redis}
debezium_container=${OCEANBASE_MYSQL_DEBEZIUM_CONTAINER:-oceanbase-mysql-capability-debezium}
mkdir -p "$output/workload"

application_client=(docker exec "$source_container" obclient -N -s -A
  -h127.0.0.1 -P2883 -ublackbox_application@blackbox
  -pOceanBaseApplication_2026!)
setup_client=(docker exec "$source_container" obclient -N -s -A
  -h127.0.0.1 -P2883 -ublackbox_setup@blackbox
  -pOceanBaseSetup_2026!)
session_prefix="SET NAMES utf8mb4 COLLATE utf8mb4_bin; SET time_zone='+00:00'; SET SESSION ob_read_consistency='STRONG'; SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ; USE benchbase;"

[[ $("${setup_client[@]}" -e 'SELECT COUNT(*) FROM benchbase.usertable;') == 0 ]]
[[ $("${setup_client[@]}" -e 'SELECT COUNT(*) FROM benchbase.session_txn;') == 0 ]]
[[ $("${setup_client[@]}" -e 'SELECT COUNT(*) FROM benchbase.checker_run_fence;') == 0 ]]
[[ $(docker exec "$redis_container" redis-cli XLEN cdc) == 0 ]]

docker exec -i "$source_container" obclient -h127.0.0.1 -P2881 \
  -uroot@blackbox -pOceanBaseContract_2026 -A \
  <"$root/setup/capture-catalog.sql" >"$output/startup-catalog-attestation.txt"

"${application_client[@]}" -e "$session_prefix START TRANSACTION; INSERT INTO checker_run_fence VALUES ('00000000-0000-0000-0000-000000000140','RUN_START','00000000-0000-0000-0000-000000000141'); COMMIT;"

"${application_client[@]}" -e "$session_prefix START TRANSACTION; INSERT INTO session_txn VALUES ('shape/marker-only',1); COMMIT;"

"${application_client[@]}" -e "$session_prefix START TRANSACTION; INSERT INTO usertable VALUES (1,NULL,'','OceanBase','汉字','🙂','f6','f7','f8','f9','f10'); INSERT INTO session_txn VALUES ('shape/insert',1); COMMIT;"

"${application_client[@]}" -e "$session_prefix START TRANSACTION; UPDATE usertable SET FIELD1='first',FIELD2=NULL WHERE YCSB_KEY=1; UPDATE usertable SET FIELD1='second',FIELD2='' WHERE YCSB_KEY=1; INSERT INTO session_txn VALUES ('shape/repeated-update',1); COMMIT;"

"${application_client[@]}" -e "$session_prefix START TRANSACTION; UPDATE usertable SET FIELD1='temporary' WHERE YCSB_KEY=1; UPDATE usertable SET FIELD1='second' WHERE YCSB_KEY=1; INSERT INTO session_txn VALUES ('shape/update-to-original',1); COMMIT;"

"${application_client[@]}" -e "$session_prefix START TRANSACTION; INSERT INTO usertable (YCSB_KEY,FIELD1,FIELD2) VALUES (2,'temporary',''); DELETE FROM usertable WHERE YCSB_KEY=2; INSERT INTO session_txn VALUES ('shape/insert-delete',1); COMMIT;"

"${application_client[@]}" -e "$session_prefix START TRANSACTION; UPDATE usertable SET YCSB_KEY=101 WHERE YCSB_KEY=1; INSERT INTO session_txn VALUES ('shape/primary-key-update',1); COMMIT;"

"${application_client[@]}" -e "$session_prefix START TRANSACTION; DELETE FROM usertable WHERE YCSB_KEY=101; INSERT INTO session_txn VALUES ('shape/delete',1); COMMIT;"

large_values=
for key in $(seq 1000 1019); do
  if [[ -n $large_values ]]; then
    large_values+=,
  fi
  large_values+="($key,'large-$key')"
done
"${application_client[@]}" -e "$session_prefix START TRANSACTION; INSERT INTO usertable (YCSB_KEY,FIELD1) VALUES $large_values; INSERT INTO session_txn VALUES ('shape/large-transaction',1); COMMIT;"

set +e
"${application_client[@]}" -e "$session_prefix START TRANSACTION; INSERT INTO usertable (YCSB_KEY,FIELD1) VALUES (3001,'A'); DO SLEEP(3); INSERT INTO session_txn VALUES ('order/commit-a-after-b',1); COMMIT;" \
  >"$output/workload/commit-a-after-b.txt" 2>&1 &
commit_a_pid=$!
set -e
sleep 1
"${application_client[@]}" -e "$session_prefix START TRANSACTION; INSERT INTO usertable (YCSB_KEY,FIELD1) VALUES (3002,'B'); INSERT INTO session_txn VALUES ('order/commit-b-before-a',1); COMMIT;" \
  >"$output/workload/commit-b-before-a.txt" 2>&1
set +e
wait "$commit_a_pid"
commit_a_status=$?
set -e
printf '%s\n' "$commit_a_status" >"$output/workload/commit-a-after-b.status"

"${application_client[@]}" -e "$session_prefix START TRANSACTION; INSERT INTO usertable (YCSB_KEY,FIELD1) VALUES (3101,'A'); INSERT INTO session_txn VALUES ('order/realtime-a',1); COMMIT;" \
  >"$output/workload/realtime-a.txt" 2>&1
"${application_client[@]}" -e "$session_prefix START TRANSACTION; INSERT INTO usertable (YCSB_KEY,FIELD1) VALUES (3102,'B'); INSERT INTO session_txn VALUES ('order/realtime-b',1); COMMIT;" \
  >"$output/workload/realtime-b.txt" 2>&1

concurrent_pids=()
for worker in $(seq 1 8); do
  key=$((4000 + worker))
  "${application_client[@]}" -e "$session_prefix START TRANSACTION; INSERT INTO usertable (YCSB_KEY,FIELD1) VALUES ($key,'concurrent-$worker'); INSERT INTO session_txn VALUES ('order/concurrent-$worker',1); COMMIT;" \
    >"$output/workload/concurrent-$worker.txt" 2>&1 &
  concurrent_pids+=("$!")
done
for pid in "${concurrent_pids[@]}"; do
  wait "$pid"
done

docker exec "$redis_container" redis-cli CLIENT PAUSE 2000 WRITE >/dev/null
backpressure_pids=()
for worker in $(seq 1 8); do
  key=$((5000 + worker))
  "${application_client[@]}" -e "$session_prefix START TRANSACTION; INSERT INTO usertable (YCSB_KEY,FIELD1) VALUES ($key,'backpressure-$worker'); INSERT INTO session_txn VALUES ('order/backpressure-$worker',1); COMMIT;" \
    >"$output/workload/backpressure-$worker.txt" 2>&1 &
  backpressure_pids+=("$!")
done
for pid in "${backpressure_pids[@]}"; do
  wait "$pid"
done

"${application_client[@]}" -e "$session_prefix START TRANSACTION; INSERT INTO checker_run_fence VALUES ('00000000-0000-0000-0000-000000000140','END_FENCE','00000000-0000-0000-0000-000000000142'); COMMIT;"

for attempt in $(seq 1 120); do
  docker exec "$redis_container" redis-cli --json XRANGE cdc - + \
    >"$output/raw-redis-cdc.json"
  if python3 - "$output/raw-redis-cdc.json" <<'PY'
import json
import sys
entries = json.load(open(sys.argv[1], encoding="utf-8"))
if len(entries) < 2:
    raise SystemExit(1)
key = json.loads(entries[-2][1][0])
value = json.loads(entries[-1][1][1])
raise SystemExit(0 if key.get("phase") == "END_FENCE" and value.get("status") == "END" else 1)
PY
  then
    break
  fi
  if [[ $attempt == 120 ]]; then
    echo 'END_FENCE did not drain to Redis' >&2
    exit 1
  fi
  sleep 1
done

docker exec "$redis_container" redis-cli XINFO STREAM cdc \
  >"$output/redis-cdc-info.txt"
docker logs "$debezium_container" >"$output/debezium-server.log" 2>&1
"${setup_client[@]}" -e \
  "SELECT COUNT(*) FROM benchbase.usertable; SELECT COUNT(*) FROM benchbase.session_txn; SELECT run_id,phase,nonce FROM benchbase.checker_run_fence ORDER BY phase;" \
  >"$output/final-database-state.txt"
docker exec "$source_container" obclient -A -c -h127.0.0.1 -P2883 \
  -ublackbox_connector@blackbox -pOceanBaseConnector_2026! \
  -e 'SHOW MASTER STATUS; SHOW BINARY LOGS;' >"$output/final-binlog-status.txt"

docker exec -i "$source_container" obclient -h127.0.0.1 -P2881 \
  -uroot@blackbox -pOceanBaseContract_2026 -A \
  <"$root/setup/capture-catalog.sql" >"$output/final-catalog-attestation.txt"
cmp "$output/startup-catalog-attestation.txt" "$output/final-catalog-attestation.txt"

python3 "$root/verify_raw_capability.py" "$output" >"$output/verifier-output.json"
