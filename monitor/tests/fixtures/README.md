# CDC fixtures

`yugabytedb_pgoutput_2_5_2_raw_cross_tablet.json` is an append-only-marker
regeneration of the six Redis entries captured by the pinned YugabyteDB contract experiment
`pgoutput_opaque_transaction_id_mismatch_20260822T153917Z`. The source capture
uses YugabyteDB `2025.2.5.2-b5`, connector `dz.2.5.2.yb.2026.1`, `pgoutput`, and
a `SEQUENCE` slot. It proves the millisecond-only row envelope and deliberately
different `BEGIN`, row, and `END` connector IDs. The unedited source capture's
SHA-256 is `09de81a9da680e1754faa20ebd874553321334e91181a0e05d626995282b47bb`;
this regenerated fixture's SHA-256 is
`7c3eecf40252fc12ef074445b3f5a1d7384b2a3db81e304589af23c5b5629579`.

`yugabytedb_pgoutput_2_5_2_lifecycle.json` is a deterministic lifecycle fixture
using that captured pinned envelope profile. It is not presented as an unedited
live capture. It supplies legal run fences and benchmark rows so the same
assembler can compare backend-neutral transactions with the real MariaDB
lifecycle fixture. Its SHA-256 is
`afd4da9f220ca46cd6a358bea7c4620773c272efccfaea5fe1bbcaae270b5735`.

## MariaDB

`mariadb_debezium_3_6_1_full_lifecycle.json` is the production replay fixture,
regenerated to the append-only marker shape from JSON emitted by:

```bash
redis-cli --json XRANGE cdc - +
```

It was captured from the pinned `mariadb:12.3` / Debezium Server
`3.6.1.Final` Compose stack by `tests/test_mariadb_lifecycle.sh`, using run ID
`084cc5a1-3cbe-4fef-bb5c-43559a1c13a0`. Its 55 real Redis entries contain the
complete ten-record startup schema snapshot followed by all three fences, a
two-row bootstrap transaction, append-only marker inserts, a marker-only
transaction, repeated same-key writes, an insert followed by delete, a delete,
and MariaDB's delete-plus-insert encoding of a primary-key update. Its SHA-256
is `2b2a9f9dc25fc32467b72fee76c89ad80fff3cecc1449442527358b53b06d94a`.

`mariadb_debezium_3_6_1_lifecycle.json` is the earlier 45-entry real capture of
the same transactional lifecycle before schema changes were enabled. It is
retained only as a transaction-mutation fixture; unit helpers prepend a valid
captured startup prefix before feeding any of its transactions to the
assembler. It is not a valid standalone production replay. Its SHA-256 is
`aa27fb89de21508c404242f8234eb40fc432de1d3751035a59c3787b01896325`.

`mariadb_debezium_3_6_1_snapshot_schema.json` is a separately regenerated
`redis-cli --json XRANGE cdc - +` capture from the same pinned stack with
`include.schema.changes=true` and `snapshot.mode=no_data`. It contains the ten
schema envelopes emitted during connector startup before live binlog streaming:
the server character-set statement, synthetic database/table drops, database
setup, and the three table creates. Their `source.snapshot` values are `true`
and `last`; they are retained as pre-run connector evidence, not deleted from
the stream. Its SHA-256 is
`24d1be95f4fb45811bf31d36dee325b4f3bc840471d732e37a9182aae0295306`.

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
