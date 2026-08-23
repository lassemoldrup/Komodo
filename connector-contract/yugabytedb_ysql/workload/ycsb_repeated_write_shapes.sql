\set ON_ERROR_STOP on
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
UPDATE usertable SET field3 = 'changed' WHERE ycsb_key = 42;
UPDATE usertable SET field3 = 'A' WHERE ycsb_key = 42;
INSERT INTO usertable (ycsb_key, field1) VALUES (7, 'temporary');
DELETE FROM usertable WHERE ycsb_key = 7;
INSERT INTO usertable (ycsb_key, field1) VALUES (100, 'primary-key-change');
UPDATE usertable SET ycsb_key = 101 WHERE ycsb_key = 100;
INSERT INTO session_txn (session_id, txn_id)
VALUES ('ycsb_repeated_write_shapes', 1);
COMMIT;
\echo ACK ycsb_repeated_write_shapes 1
