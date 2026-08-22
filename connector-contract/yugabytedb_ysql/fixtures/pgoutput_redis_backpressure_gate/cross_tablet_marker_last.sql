\set ON_ERROR_STOP on
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1, field2)
VALUES (1, 'alpha', NULL);
INSERT INTO usertable (ycsb_key, field1, field2)
VALUES (2, 'beta', '');
INSERT INTO checker_run_fence (run_id, phase, nonce)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    'CONTRACT_SPIKE',
    '00000000-0000-0000-0000-000000000002'
);
INSERT INTO session_txn (session_id, txn_id)
VALUES ('cross_tablet', 1);
COMMIT;
