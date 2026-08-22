\set ON_ERROR_STOP on
SET search_path TO benchbase;

BEGIN ISOLATION LEVEL REPEATABLE READ;
INSERT INTO usertable (ycsb_key, field1)
VALUES (:ycsb_key, :'field1');
SELECT pg_sleep(:delay_seconds);
INSERT INTO session_txn (session_id, txn_id)
VALUES (:'session_id', :txn_id)
ON CONFLICT (session_id) DO UPDATE SET txn_id = EXCLUDED.txn_id;
COMMIT;

SELECT :'session_id' AS committed_session_id,
       :txn_id AS committed_txn_id,
       clock_timestamp() AT TIME ZONE 'UTC' AS commit_acknowledged_at_utc;
