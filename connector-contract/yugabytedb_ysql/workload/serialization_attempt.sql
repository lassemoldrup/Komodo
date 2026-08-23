\set ON_ERROR_STOP on
\set VERBOSITY verbose
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT field1 FROM usertable WHERE ycsb_key = 500;
SELECT pg_sleep(5);
UPDATE usertable SET field1 = 'serialization-attempt-one' WHERE ycsb_key = 500;
INSERT INTO session_txn (session_id, txn_id) VALUES ('serialization_retry', 1);
COMMIT;
\echo ACK serialization_retry 1
