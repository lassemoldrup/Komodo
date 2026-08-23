\set ON_ERROR_STOP on
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO contract_account_a (account_id, payload)
SELECT value, 'large-a-' || value FROM generate_series(10000, 10063) AS value;
INSERT INTO contract_account_b (account_id, payload)
SELECT value, 'large-b-' || value FROM generate_series(20000, 20063) AS value;
INSERT INTO contract_account_c (account_id, payload)
SELECT value, 'large-c-' || value FROM generate_series(30000, 30063) AS value;
INSERT INTO contract_account_d (account_id, payload)
SELECT value, 'large-d-' || value FROM generate_series(40000, 40063) AS value;
INSERT INTO session_txn (session_id, txn_id) VALUES ('large_transaction', 1);
COMMIT;
\echo ACK large_transaction 1
