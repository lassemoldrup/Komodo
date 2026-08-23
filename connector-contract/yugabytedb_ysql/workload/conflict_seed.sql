\set ON_ERROR_STOP on
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES
    (500, 'serialization'),
    (600, 'deadlock-left'),
    (601, 'deadlock-right');
INSERT INTO session_txn (session_id, txn_id) VALUES ('conflict_seed', 1);
COMMIT;
\echo ACK conflict_seed 1
