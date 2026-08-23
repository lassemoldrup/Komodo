\set ON_ERROR_STOP on
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES
    (10, 'update-update-start'),
    (20, 'return-original'),
    (30, 'unrelated');
INSERT INTO session_txn (session_id, txn_id) VALUES ('write_shape_seed', 1);
COMMIT;
\echo ACK write_shape_seed 1

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES (100, 'insert-start');
UPDATE usertable SET field1 = 'insert-updated' WHERE ycsb_key = 100;
UPDATE usertable SET field1 = 'update-one' WHERE ycsb_key = 10;
UPDATE usertable SET field1 = 'update-two' WHERE ycsb_key = 10;
UPDATE usertable SET field1 = 'changed' WHERE ycsb_key = 20;
UPDATE usertable SET field1 = 'return-original' WHERE ycsb_key = 20;
INSERT INTO usertable (ycsb_key, field1) VALUES (200, 'insert-delete');
DELETE FROM usertable WHERE ycsb_key = 200;
INSERT INTO usertable (ycsb_key, field1) VALUES (300, 'primary-key-change');
UPDATE usertable SET ycsb_key = 301 WHERE ycsb_key = 300;
INSERT INTO session_txn (session_id, txn_id) VALUES ('write_shape_matrix', 1);
COMMIT;
\echo ACK write_shape_matrix 1
