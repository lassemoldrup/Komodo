\set ON_ERROR_STOP on
SET search_path TO benchbase;
SELECT pg_sleep(:start_delay_seconds);

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES (1001, 'backpressure-01');
INSERT INTO session_txn (session_id, txn_id) VALUES ('backpressure', 1);
COMMIT;
\echo ACK backpressure 1

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES (1002, 'backpressure-02');
INSERT INTO session_txn (session_id, txn_id) VALUES ('backpressure', 2);
COMMIT;
\echo ACK backpressure 2

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES (1003, 'backpressure-03');
INSERT INTO session_txn (session_id, txn_id) VALUES ('backpressure', 3);
COMMIT;
\echo ACK backpressure 3

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES (1004, 'backpressure-04');
INSERT INTO session_txn (session_id, txn_id) VALUES ('backpressure', 4);
COMMIT;
\echo ACK backpressure 4

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES (1005, 'backpressure-05');
INSERT INTO session_txn (session_id, txn_id) VALUES ('backpressure', 5);
COMMIT;
\echo ACK backpressure 5

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES (1006, 'backpressure-06');
INSERT INTO session_txn (session_id, txn_id) VALUES ('backpressure', 6);
COMMIT;
\echo ACK backpressure 6

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES (1007, 'backpressure-07');
INSERT INTO session_txn (session_id, txn_id) VALUES ('backpressure', 7);
COMMIT;
\echo ACK backpressure 7

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES (1008, 'backpressure-08');
INSERT INTO session_txn (session_id, txn_id) VALUES ('backpressure', 8);
COMMIT;
\echo ACK backpressure 8

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES (1009, 'backpressure-09');
INSERT INTO session_txn (session_id, txn_id) VALUES ('backpressure', 9);
COMMIT;
\echo ACK backpressure 9

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES (1010, 'backpressure-10');
INSERT INTO session_txn (session_id, txn_id) VALUES ('backpressure', 10);
COMMIT;
\echo ACK backpressure 10

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES (1011, 'backpressure-11');
INSERT INTO session_txn (session_id, txn_id) VALUES ('backpressure', 11);
COMMIT;
\echo ACK backpressure 11

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1) VALUES (1012, 'backpressure-12');
INSERT INTO session_txn (session_id, txn_id) VALUES ('backpressure', 12);
COMMIT;
\echo ACK backpressure 12

SELECT 'backpressure' AS committed_session_id,
       12 AS committed_txn_id,
       clock_timestamp() AT TIME ZONE 'UTC' AS final_commit_acknowledged_at_utc;
