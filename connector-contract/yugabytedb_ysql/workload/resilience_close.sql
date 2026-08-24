\set ON_ERROR_STOP on
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO checker_run_fence (run_id, phase, nonce)
VALUES (
    '00000000-0000-0000-0000-000000000010',
    'END_FENCE',
    '00000000-0000-0000-0000-000000000011'
);
INSERT INTO session_txn (session_id, txn_id)
VALUES ('resilience_transition', 5);
COMMIT;
\echo ACK resilience_transition 5
