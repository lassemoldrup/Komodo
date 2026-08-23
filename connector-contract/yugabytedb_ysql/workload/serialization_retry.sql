\set ON_ERROR_STOP on
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
UPDATE usertable SET field1 = 'serialization-retry-two' WHERE ycsb_key = 500;
INSERT INTO session_txn (session_id, txn_id) VALUES ('serialization_retry', 2);
COMMIT;
\echo ACK serialization_retry 2
