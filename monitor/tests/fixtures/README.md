# MariaDB CDC fixtures

`mariadb_debezium_3_6_1_full_lifecycle.json` is the production replay fixture.
It is the unedited JSON emitted by:

```bash
redis-cli --json XRANGE cdc - +
```

It was captured from the pinned `mariadb:12.3` / Debezium Server
`3.6.1.Final` Compose stack by `tests/test_mariadb_lifecycle.sh`, using run ID
`084cc5a1-3cbe-4fef-bb5c-43559a1c13a0`. Its 55 real Redis entries contain the
complete ten-record startup schema snapshot followed by all three fences, a
two-row bootstrap transaction, marker insert and update chains, a marker-only
transaction, repeated same-key writes, an insert followed by delete, a delete,
and MariaDB's delete-plus-insert encoding of a primary-key update. Its SHA-256
is `6148236e8806c8fc6f3693a3390771a8e905ffbc603d403a3b1a858387b48185`.

`mariadb_debezium_3_6_1_lifecycle.json` is the earlier 45-entry real capture of
the same transactional lifecycle before schema changes were enabled. It is
retained only as a transaction-mutation fixture; unit helpers prepend a valid
captured startup prefix before feeding any of its transactions to the
assembler. It is not a valid standalone production replay. Its SHA-256 is
`dc202702ce0c1d39aa945d38c4f772b5202335e36378ff59f8d8186505b81eeb`.

`mariadb_debezium_3_6_1_snapshot_schema.json` is a separate unedited
`redis-cli --json XRANGE cdc - +` capture from the same pinned stack with
`include.schema.changes=true` and `snapshot.mode=no_data`. It contains the ten
schema envelopes emitted during connector startup before live binlog streaming:
the server character-set statement, synthetic database/table drops, database
setup, and the three table creates. Their `source.snapshot` values are `true`
and `last`; they are retained as pre-run connector evidence, not deleted from
the stream. Its SHA-256 is
`4be5e5dbb70beffd2acb558708e52482d348a0b0e19983dc34c4e775f38f1301`.

To capture the same semantic workload again inside `db-monitoring-dev`:

```bash
cd /home/yifan/code/db-monitoring/si-monitoring
make monitor-build
BLACKBOX_ACTIVE_RUN_ID=084cc5a1-3cbe-4fef-bb5c-43559a1c13a0 \
BLACKBOX_CAPTURE_PATH=/tmp/mariadb-cdc-capture.json \
BLACKBOX_CDC_REPLAY_BIN=/tmp/db-monitoring-blackbox-monitor-build/blackbox_cdc_replay \
BLACKBOX_CDC_INGEST_BIN=/tmp/db-monitoring-blackbox-monitor-build/blackbox_cdc_ingest \
  ./tests/test_mariadb_lifecycle.sh
```

Stream IDs, GTIDs, binlog positions, nonces, and timestamps naturally differ
between captures. Negative unit cases mutate an in-memory copy of this raw
positive fixture; no synthetic file is presented as captured evidence.
