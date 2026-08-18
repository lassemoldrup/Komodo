# si-monitoring

CDC harness for monitoring isolation anomalies under Snapshot Isolation.

Debezium captures the **commit order** of transactions and the **values written**
by each write, into a single ordered Redis stream. The workload client logs its
own **read events**. The two halves are joined on a per-transaction key.

Three interchangeable backends, one shared downstream half. Exactly one backend
runs at a time and all write to the same stream, so the monitor reads one place.

```
   MariaDB     --binlog---------\
   YugabyteDB  --logical repl----+-- Debezium Server --> Redis "cdc" --> C++ monitor
   Oracle      --LogMiner-------/
```

All three use the **same** client-supplied transaction id, so the monitor never
branches on backend.

| Backend | Version | Capture latency |
|---|---|---|
| MariaDB | 12.3 | ~10 ms |
| YugabyteDB | latest | ~40 ms |
| Oracle | 23.26 (26ai) | ~2 s |

## Quick start

```bash
make up                  # MariaDB (default); blocks until Debezium is streaming
make up DB=yugabyte
make up DB=oracle

make txn                 # one write transaction, prints its correlation marker
make stream              # dump the CDC stream
make follow              # tail it live
make status              # health of the active backend
make sql                 # SQL shell
make reset               # wipe everything for the active backend
```

Every target takes `DB=`, defaulting to `mariadb`. Scripts infer the backend
from the running containers, so `./scripts/*.sh` need no flag.

**Use `make reset`, not `make down`, between runs.** All three databases are
ephemeral, and a stale connector offset against a fresh database silently
yields transactions with zero row events.

## Stream contract

One Redis stream, `cdc`. Each entry has a single field: the field **name** is
the Debezium message key, the field **value** is the envelope. Stream order is
commit order; Redis stream IDs are monotonic and usable as a resume cursor.

MariaDB produces four record shapes:

| Kind | Recognised by | Carries |
|---|---|---|
| BEGIN | `status == "BEGIN"` | transaction id |
| row change | has `op` | `before`, `after`, `source`, `transaction.total_order` |
| END | `status == "END"` | transaction id, `event_count`, per-table counts |
| schema change | has `ddl` | connector startup schema proof or unsupported live DDL |

`op` is `c` insert, `u` update, `d` delete. The END marker's `event_count` tells
the monitor a write set is complete without waiting for the next transaction.

Connector-side table filtering is deliberately disabled for MariaDB: every
table in the fenced `benchbase` database and all schema changes reach `cdc`.
The monitor, not Debezium, enforces the manifest allowlists. A fresh `no_data`
connector emits an ordered schema-snapshot prefix, which the assembler requires
and validates before transactional evidence. Only that strictly marked startup
prefix is metadata; later DDL or a row from an unallowlisted table invalidates
the run.

Per-backend envelope differences:

| | MariaDB | YugabyteDB | Oracle |
|---|---|---|---|
| identifier case | lower | lower | UPPER (table and columns) |
| column values | plain | wrapped `{"value":…,"set":…}` | plain, numbers as strings |
| timestamps | ms + µs + ns | ms | ms |

Transaction identity is the `session_txn` marker and nothing else. Boundaries
come from the BEGIN/END markers, commit order from stream order. Debezium also
emits its own server-side transaction id in `source` (GTID, commit hybrid time,
XID) — that is default connector behaviour, and this harness neither uses nor
relies on it.

## Client protocol

One protocol everywhere. The client mints its own `(session_id, txn_id)` and
upserts it into its session's row in `session_txn` as the **last statement** of
the transaction, so the marker travels inside that transaction's own change set
and arrives after the workload writes. `session_txn` starts empty; sessions
create their own row on first use.

For the black-box framework, `txn_id` is the dense session-local physical-attempt
counter starting at one. A logical retry keeps its logical/session sequence but
receives a new attempt ID and `txn_id`. Aborted attempts may therefore create
gaps between committed marker IDs. The run-scoped join identity is
`(run_id, session_id, txn_id)`; `run_id` comes from the one-active-run fenced
lifecycle and is not stored in the marker row.

No isolation statement is issued: all three backends default to Snapshot
Isolation server-side. Only the upsert dialect differs.

```sql
-- YugabyteDB
BEGIN;
  -- reads and writes; client logs each read
INSERT INTO session_txn (session_id, txn_id) VALUES ('<sid>', <counter>)
  ON CONFLICT (session_id) DO UPDATE SET txn_id = EXCLUDED.txn_id;
COMMIT;

-- MariaDB
START TRANSACTION;
  -- reads and writes; client logs each read
INSERT INTO session_txn (session_id, txn_id) VALUES ('<sid>', <counter>)
  ON DUPLICATE KEY UPDATE txn_id = VALUES(txn_id);
COMMIT;

-- Oracle (no ON CONFLICT; MERGE is the upsert)
  -- reads and writes; client logs each read
MERGE INTO session_txn t
  USING (SELECT '<sid>' AS session_id, <counter> AS txn_id FROM DUAL) s
  ON (t.session_id = s.session_id)
  WHEN MATCHED THEN UPDATE SET t.txn_id = s.txn_id
  WHEN NOT MATCHED THEN INSERT (session_id, txn_id) VALUES (s.session_id, s.txn_id);
COMMIT;
```

The marker write and the workload writes must be **atomic**: a transaction that
commits its workload without a marker is uncorrelatable. On Oracle this needs
`WHENEVER SQLERROR EXIT ROLLBACK` — sqlplus otherwise continues past a failed
marker and still runs `COMMIT`. `scripts/txn.sh` demonstrates the upsert dialect
for all three; its timestamp default is not a valid
framework allocator. BenchBase must use the dense per-session attempt counter
defined above (manual concurrent tests should set `TXN_ID` explicitly).

Because the before-image carries the previous `txn_id`, a per-session chain
check comes for free: if `prev` is not the last `txn_id` you saw for that
session, a transaction went missing.

The monitor waits for the enclosing Debezium `END`, requires exactly one marker
as the final row event of every post-load workload transaction, validates this
before-image chain, and removes the marker row from the normalized application
write set. A marker-only physical transaction is still joined and checked with
zero application writes, so successful read-only/no-application-write workload
transactions remain in scope.

Conflicts abort — `ER_CHECKREAD` (MariaDB), `could not serialize access`
(YugabyteDB), `ORA-08177` (Oracle) — so the client needs a retry path. Aborted
transactions produce no stream events at all.

## Reading the stream from C++

```cpp
// "0-0" replays from the beginning; "$" is live-only.
std::string last_id = "0-0";
while (true) {
    redisReply* r = (redisReply*)redisCommand(
        ctx, "XREAD BLOCK 0 COUNT 1000 STREAMS cdc %s", last_id.c_str());
    if (!r) break;
    redisReply* entries = r->element[0]->element[1];
    for (size_t i = 0; i < entries->elements; i++) {
        redisReply* e = entries->element[i];
        last_id = e->element[0]->str;            // "<ms>-<seq>"
        handle(e->element[1]->element[1]->str);  // envelope JSON
    }
    freeReplyObject(r);
}
```

Do not trim `cdc`, client, or status streams during an audited run. Restart
recovery, coordinated checkpoints, and safe trimming are later features.

The monitor implementation lives in `monitor/`. `BlockingRedisReader` issues
binary-safe blocking `XREAD` calls with one explicit cursor per stream;
`DebeziumTxnAssembler` validates MariaDB transaction structure and emits a
normalized `CdcTransaction` only after a valid `END`. Connector transaction
IDs are kept only inside the assembler for `BEGIN`/row/`END` consistency. The
only exposed workload correlation is the unified
`(run_id, session_id, txn_id)` marker identity.

`ClientTxnParser` strictly validates the client schema, canonical Base64 and
YCSB bytes, and the read digest before admission. `TransactionJoiner` retains
bounded client and CDC buffers and releases only the longest ready CDC prefix.
It reconstructs bootstrap state, checks the independent row-count/digest proof
at the load fence, creates the per-session phase anchors, and calls the
unchanged `SnapshotSOWWEngine` synchronously through `CheckerAdapter`. Dense
checker order therefore covers bootstrap transactions, anchors, and joined
workload transactions only; controls never create a gap.

Build and run the C++ codec, reader, captured-fixture, and assembler tests from
the canonical development container with:

```bash
cd /home/yifan/code/db-monitoring/si-monitoring
make test-monitor
```

`blackbox_cdc_replay` validates an `XRANGE --json` capture, while
`blackbox_cdc_ingest` exercises bounded live multi-stream CDC ingestion.
`blackbox_prefix_ingest` additionally consumes client transactions after an
explicit client cursor and requires the exact validated bootstrap prefix,
table count, and digest before releasing anchors. `make test-lifecycle-live`
runs all three paths against fresh MariaDB/Debezium/Redis evidence. The prefix
tool deliberately leaves `END_FENCE` queued: session shutdown, final
reconciliation, and terminal-result emission remain absent.

## Constraints

**All backends** — durability is deliberately disabled (tmpfs datadirs,
`sync_binlog=0`). This affects crash recovery only, never isolation semantics.

**MariaDB** — clients need `--skip-ssl`; the 11.8+ client requires TLS and the
listeners here are plaintext.

**YugabyteDB** — all schema must exist before the replication slot is created;
DDL against a live slot permanently breaks before-image computation and the
connector then retries forever without failing. `yugabyte/init.sh` enforces the
order. The slot must be `HYBRID_TIME`, and the tserver runs with
`cdc_enable_intra_transactional_before_image=true`; without it, touching a row
again after inserting it in the same transaction stalls the connector.

**Oracle** — needs ARCHIVELOG, force logging, and `SUPPLEMENTAL LOG DATA (ALL)
COLUMNS` per table (the equivalent of `REPLICA IDENTITY FULL`);
`oracle/startdb.d/` applies all of it on every start. Single instance only:
under RAC each node keeps its own redo thread and commit ordering across threads
is not guaranteed. Long-running transactions can be dropped silently by
`log.mining.transaction.retention`.

**Version pins** — MariaDB and Oracle run Debezium 3.6.1. YugabyteDB is pinned
to Debezium 2.5.2 because Yugabyte publishes only `dz.2.5.2.yb.*` builds of
their connector; a `dz.3.x.yb.*` tag would collapse the stack to one version.

## Database parameters

Everything below differs from the stock server default. Settings that merely
restate a default are marked `[pin]` in the config files and omitted here.

**MariaDB** (`mariadb/conf.d/cdc.cnf`) — derived by diffing `SHOW GLOBAL
VARIABLES` against an unconfigured `mariadb:12.3`.

| Parameter | Default | Here | Why |
|---|---|---|---|
| `log_bin` | OFF | `mariadb-bin` | CDC source |
| `binlog_row_metadata` | `NO_LOG` | `FULL` | column names in the table-map event |
| `binlog_annotate_row_events` | ON | OFF | SQL text is dead weight |
| `innodb_flush_log_at_trx_commit` | 1 | 0 | durability only, not isolation |
| `innodb_buffer_pool_size` | 128M | 1G | throughput |
| `innodb_log_file_size` | 96M | 256M | throughput |
| `max_connections` | 151 | 512 | concurrent workload sessions |
| `max_binlog_size` | 1G | 256M | smaller files on a tmpfs datadir |
| `expire_logs_days` | 10 | 1 | same |
| `server_id` | – | 1 | required for binlog |

MariaDB's `REPEATABLE READ` + `innodb_snapshot_isolation=ON` is already SI and
already the 12.3 default, so no isolation change is needed there. That pair,
along with `binlog_format=ROW`, `binlog_row_image=FULL` and `sync_binlog=0`,
is pinned rather than changed. Raising
`max_connections`, `max_binlog_size` and the buffer pool also moves derived
values (`back_log`, `thread_cache_size`, `max_relay_log_size`,
`innodb_adaptive_hash_index_cells`).

**YugabyteDB** (tserver flags in `docker-compose.yml`) — all three default to
`false`.

| Flag | Default | Here | Why |
|---|---|---|---|
| `ysql_yb_enable_replication_commands` | false | true | logical replication slots |
| `ysql_yb_enable_replica_identity` | false | true | per-table replica identity |
| `cdc_enable_intra_transactional_before_image` | false | true | before-images for rows written more than once in a transaction |

Plus `default_transaction_isolation` raised from `read committed` to
`repeatable read` (= SI) with `ALTER DATABASE`.

Per-table: `REPLICA IDENTITY FULL` (server default for new tables is `CHANGE`),
and the slot is created with `yb_lsn_type=HYBRID_TIME` (default `SEQUENCE`).

**Oracle** (`oracle/startdb.d/`)

| Parameter | Default | Here |
|---|---|---|
| archive log mode | `NOARCHIVELOG` | `ARCHIVELOG` |
| force logging | NO | YES |
| minimal supplemental logging | NO | YES |
| per-table supplemental logging | none | `ALL COLUMNS` on both tables |
| default isolation | READ COMMITTED | SERIALIZABLE (= SI), via an `AFTER LOGON` trigger on the `SITEST` schema |

**Redis** — `--save ""` (default has RDB save points), `--appendonly no`
(default), `--maxmemory 2gb` (default unlimited). It is a transport, not a
system of record.

## Layout

```
docker-compose.yml                        # profiles: mariadb | yugabyte | oracle
mariadb/conf.d/cdc.cnf                    # binlog, isolation, durability, throughput
mariadb/initdb/*.sql                      # accounts + schema
yugabyte/init.sql, init.sh                # schema first, HYBRID_TIME slot last
oracle/startdb.d/                         # ARCHIVELOG, suppl. logging, user, schema
debezium/application*.properties          # one per backend
debezium/Dockerfile.yugabyte              # YB connector on a Java 17 base
scripts/lib.sh                            # backend detection, shared SQL helpers
scripts/e2e.sh                            # sole MariaDB lifecycle coordinator
scripts/blackbox_lifecycle.sh             # fences, Redis/status, scan helpers
scripts/blackbox_record.py                # manifests and bootstrap digest
scripts/txn.sh                            # client protocol reference, all backends
scripts/stream.sh                         # read/follow, handles all envelopes
scripts/bench.sh, sql.sh, status.sh
monitor/                                  # C++ CDC reader, assembler, fixtures, tests
```

The audited MariaDB path uses an initially empty `benchbase.usertable`,
`benchbase.checker_run_fence`, and `benchbase.session_txn`, all created before
CDC starts. The `registers` table remains the exploratory workload table for the
other backend profiles.

## Credentials

Development defaults, plaintext. MariaDB `root`/`rootpw`, `debezium`/`debezium_pw`,
`app`/`app_pw`. YugabyteDB `yugabyte`/`yugabyte`. Oracle `sys`/`oraclepw`,
`c##dbzuser`/`dbz`, `sitest`/`sitestpw`.
