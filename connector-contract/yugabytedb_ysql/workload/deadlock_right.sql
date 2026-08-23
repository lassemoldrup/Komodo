\set ON_ERROR_STOP on
\set VERBOSITY verbose
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
UPDATE usertable SET field1 = 'deadlock-right-first' WHERE ycsb_key = 601;
SELECT pg_sleep(2);
UPDATE usertable SET field1 = 'deadlock-right-second' WHERE ycsb_key = 600;
INSERT INTO session_txn (session_id, txn_id) VALUES ('deadlock_right', 1);
COMMIT;
\echo ACK deadlock_right 1
