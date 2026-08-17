# MariaDB CDC fixtures

`mariadb_debezium_3_6_1_lifecycle.json` is the unedited JSON emitted by:

```bash
redis-cli --json XRANGE cdc - +
```

It was captured from the pinned `mariadb:12.3` / Debezium Server
`3.6.1.Final` Compose stack by `tests/test_mariadb_lifecycle.sh`, using run ID
`084cc5a1-3cbe-4fef-bb5c-43559a1c13a0`. The capture contains real Redis
entries for all three fences, a two-row bootstrap transaction, marker insert
and update chains, a marker-only transaction, repeated same-key writes, an
insert followed by delete, a delete, and MariaDB's delete-plus-insert encoding
of a primary-key update.

The checked-in raw capture has SHA-256
`dc202702ce0c1d39aa945d38c4f772b5202335e36378ff59f8d8186505b81eeb`.

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
