\set ON_ERROR_STOP on
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO contract_account_a (account_id, payload)
SELECT value, repeat('x', 96) || lpad(value::text, 4, '0')
FROM generate_series(1, 8192) AS value;
INSERT INTO session_txn (session_id, txn_id)
VALUES ('resilience_transition', 1);
COMMIT;
\echo ACK resilience_transition 1
