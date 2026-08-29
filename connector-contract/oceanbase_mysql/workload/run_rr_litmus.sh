#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
output=${1:?output directory is required}
source_container=${OCEANBASE_MYSQL_SOURCE_CONTAINER:-oceanbase-mysql-capability-source}
mkdir -p "$output"

application_client=(docker exec "$source_container" obclient -N -s -A
  -h127.0.0.1 -P2883 -ublackbox_application@blackbox
  -pOceanBaseApplication_2026!)
setup_client=(docker exec "$source_container" obclient -N -s -A
  -h127.0.0.1 -P2883 -ublackbox_setup@blackbox
  -pOceanBaseSetup_2026!)

cleanup() {
  "${setup_client[@]}" -e 'DELETE FROM benchbase.usertable;' >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${setup_client[@]}" -e \
  "DELETE FROM benchbase.usertable; INSERT INTO benchbase.usertable (YCSB_KEY,FIELD1) VALUES (9001,'old'),(9002,'old'),(9101,'initial'),(9201,'1'),(9202,'1');"

"${application_client[@]}" -e \
  "SET SESSION ob_read_consistency='STRONG'; SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT @@transaction_isolation,@@ob_read_consistency;" \
  >"$output/session-contract.txt"

set +e
"${application_client[@]}" -e \
  "SET SESSION ob_read_consistency='STRONG'; SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION; SELECT FIELD1 FROM benchbase.usertable WHERE YCSB_KEY=9001; DO SLEEP(3); SELECT FIELD1 FROM benchbase.usertable WHERE YCSB_KEY=9002; COMMIT;" \
  >"$output/snapshot-reader.txt" 2>&1 &
snapshot_reader_pid=$!
set -e
sleep 1
"${application_client[@]}" -e \
  "SET SESSION ob_read_consistency='STRONG'; SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION; UPDATE benchbase.usertable SET FIELD1='new' WHERE YCSB_KEY IN (9001,9002); COMMIT;" \
  >"$output/snapshot-writer.txt" 2>&1
set +e
wait "$snapshot_reader_pid"
snapshot_reader_status=$?
set -e
printf '%s\n' "$snapshot_reader_status" >"$output/snapshot-reader.status"

set +e
"${application_client[@]}" -e \
  "SET SESSION ob_read_consistency='STRONG'; SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION; UPDATE benchbase.usertable SET FIELD1='winner' WHERE YCSB_KEY=9101; DO SLEEP(3); COMMIT;" \
  >"$output/write-conflict-first.txt" 2>&1 &
first_writer_pid=$!
set -e
sleep 1
set +e
"${application_client[@]}" -e \
  "SET SESSION ob_read_consistency='STRONG'; SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION; SELECT FIELD1 FROM benchbase.usertable WHERE YCSB_KEY=9101; UPDATE benchbase.usertable SET FIELD1='loser' WHERE YCSB_KEY=9101; COMMIT;" \
  >"$output/write-conflict-second.txt" 2>&1
second_writer_status=$?
wait "$first_writer_pid"
first_writer_status=$?
set -e
printf '%s\n' "$first_writer_status" >"$output/write-conflict-first.status"
printf '%s\n' "$second_writer_status" >"$output/write-conflict-second.status"
"${setup_client[@]}" -e \
  "SELECT FIELD1 FROM benchbase.usertable WHERE YCSB_KEY=9101;" \
  >"$output/write-conflict-final.txt"

set +e
"${application_client[@]}" -e \
  "SET SESSION ob_read_consistency='STRONG'; SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION; SELECT FIELD1 FROM benchbase.usertable WHERE YCSB_KEY=9202; DO SLEEP(3); UPDATE benchbase.usertable SET FIELD1='0' WHERE YCSB_KEY=9201; COMMIT;" \
  >"$output/write-skew-first.txt" 2>&1 &
skew_first_pid=$!
"${application_client[@]}" -e \
  "SET SESSION ob_read_consistency='STRONG'; SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION; SELECT FIELD1 FROM benchbase.usertable WHERE YCSB_KEY=9201; DO SLEEP(3); UPDATE benchbase.usertable SET FIELD1='0' WHERE YCSB_KEY=9202; COMMIT;" \
  >"$output/write-skew-second.txt" 2>&1 &
skew_second_pid=$!
wait "$skew_first_pid"
skew_first_status=$?
wait "$skew_second_pid"
skew_second_status=$?
set -e
printf '%s\n' "$skew_first_status" >"$output/write-skew-first.status"
printf '%s\n' "$skew_second_status" >"$output/write-skew-second.status"
"${setup_client[@]}" -e \
  "SELECT YCSB_KEY,FIELD1 FROM benchbase.usertable WHERE YCSB_KEY IN (9201,9202) ORDER BY YCSB_KEY;" \
  >"$output/write-skew-final.txt"

python3 "$root/verify_rr_litmus.py" "$output" >"$output/verifier-output.json"
