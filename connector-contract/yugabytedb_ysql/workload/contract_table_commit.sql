\set ON_ERROR_STOP on
SET search_path TO benchbase;

SELECT pg_sleep(GREATEST(0.0, :start_epoch - EXTRACT(EPOCH FROM clock_timestamp())));
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT format(
    'INSERT INTO %I (account_id, payload) VALUES (%s, %L)',
    'contract_account_' || :'table_family',
    :account_id,
    :'payload'
)
\gexec
SELECT pg_sleep(:delay_seconds);
INSERT INTO session_txn (session_id, txn_id)
VALUES (:'session_id', :txn_id);
COMMIT;
\echo ACK :session_id :txn_id
