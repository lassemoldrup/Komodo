\set ON_ERROR_STOP on
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
UPDATE usertable SET field1 = 'serialization-winner' WHERE ycsb_key = 500;
SELECT pg_sleep(4);
INSERT INTO session_txn (session_id, txn_id) VALUES ('serialization_winner', 1);
COMMIT;
\echo ACK serialization_winner 1
