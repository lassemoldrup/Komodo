\set ON_ERROR_STOP on
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key)
VALUES (-2147483648);
INSERT INTO usertable (
    ycsb_key, field1, field2, field3, field4, field5,
    field6, field7, field8, field9, field10
) VALUES (
    42, NULL, '', 'A', 'é', '中', '0', 'hello', '🙂', 'embedded', 'last'
);
INSERT INTO usertable (ycsb_key, field1)
VALUES (2147483647, repeat('x', 100));
INSERT INTO session_txn (session_id, txn_id)
VALUES ('ycsb_normalization', 1);
COMMIT;
\echo ACK ycsb_normalization 1
