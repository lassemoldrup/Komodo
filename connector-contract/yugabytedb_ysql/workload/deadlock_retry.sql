\set ON_ERROR_STOP on
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
UPDATE usertable SET field1 = :'payload' WHERE ycsb_key IN (600, 601);
INSERT INTO session_txn (session_id, txn_id)
VALUES (:'session_id', 2)
ON CONFLICT (session_id) DO UPDATE SET txn_id = EXCLUDED.txn_id;
COMMIT;
\echo ACK :session_id 2
