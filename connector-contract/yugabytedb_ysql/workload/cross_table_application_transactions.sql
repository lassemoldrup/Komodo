\set ON_ERROR_STOP on
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO contract_account_a (account_id, payload) VALUES (1, 'test-adapter-a');
INSERT INTO contract_account_b (account_id, payload) VALUES (1, 'test-adapter-b');
INSERT INTO session_txn (session_id, txn_id) VALUES ('cross_table_test_adapter', 1);
COMMIT;
\echo ACK cross_table_test_adapter 1

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO warehouse (
    w_id, w_ytd, w_tax, w_name, w_street_1, w_street_2, w_city, w_state, w_zip
) VALUES (1, 300000.00, 0.1234, 'warehouse', 'street one', 'street two', 'city', 'CA', '123456789');
INSERT INTO district (
    d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name,
    d_street_1, d_street_2, d_city, d_state, d_zip
) VALUES (1, 1, 30000.00, 0.2345, 3001, 'district',
          'street one', 'street two', 'city', 'CA', '123456789');
INSERT INTO session_txn (session_id, txn_id) VALUES ('cross_table_tpcc', 1);
COMMIT;
\echo ACK cross_table_tpcc 1
