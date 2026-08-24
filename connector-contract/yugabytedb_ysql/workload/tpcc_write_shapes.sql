\set ON_ERROR_STOP on
SET search_path TO benchbase;

-- Write shapes: one marker-terminated transaction exercising update-update,
-- update-to-original, insert-delete, primary-key change, and a decimal update
-- with intra-transaction before images.
BEGIN ISOLATION LEVEL REPEATABLE READ;
UPDATE stock SET s_quantity = 77 WHERE s_w_id = 1 AND s_i_id = 1;
UPDATE stock SET s_quantity = 91 WHERE s_w_id = 1 AND s_i_id = 1;
INSERT INTO item (i_id, i_name, i_price, i_data, i_im_id)
VALUES (600, 'ephemeral item', 5.55, 'insert then delete', 3);
DELETE FROM item WHERE i_id = 600;
UPDATE item SET i_id = 501 WHERE i_id = 500;
UPDATE customer SET c_balance = 25.10
 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
INSERT INTO session_txn (session_id, txn_id)
VALUES ('tpcc/capture/1', 2)
ON CONFLICT (session_id) DO UPDATE SET txn_id = EXCLUDED.txn_id;
COMMIT;
\echo ACK tpcc_write_shapes 2
