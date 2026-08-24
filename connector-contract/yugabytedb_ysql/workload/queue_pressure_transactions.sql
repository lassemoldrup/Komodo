\set ON_ERROR_STOP on
\pset tuples_only on
\pset format unaligned
SET search_path TO benchbase;
SELECT pg_sleep(:start_delay_seconds);

SELECT format(
    'BEGIN ISOLATION LEVEL REPEATABLE READ; '
    'INSERT INTO contract_account_%s (account_id, payload) VALUES (%s, %L); '
    'INSERT INTO session_txn (session_id, txn_id) VALUES (%L, %s); '
    'COMMIT; SELECT %L;',
    chr(97 + ((value - 1) % 4)),
    50000 + value,
    'queue-' || value,
    'queue_pressure',
    value,
    'ACK queue_pressure ' || value
)
FROM generate_series(1, 96) AS value
\gexec
