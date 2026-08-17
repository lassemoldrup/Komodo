# si-monitoring

CDC harness for monitoring isolation anomalies under Snapshot Isolation.

Debezium captures the **commit order** of transactions and the **values written**
by each write, into a single ordered Redis stream. The workload client logs its
own **read events**. The two halves are joined on a per-transaction key.

Three interchangeable backends, one shared downstream half. Exactly one backend
runs at a time and all write to the same stream, so the monitor reads one place.

```
   MariaDB     --binlog (GTID)--------\
   YugabyteDB  --logical repl (HT)-----+-- Debezium Server --> Redis "cdc" --> C++ monitor
   Oracle      --LogMiner (commit SCN)/
```

| Backend | Version | Join key | Capture latency |
|---|---|---|---|
| MariaDB | 12.3 | GTID (`@@session.last_gtid`) | ~10 ms |
| YugabyteDB | latest | client marker table | ~40 ms |
| Oracle | 23.26 (26ai) | XID (`DBMS_TRANSACTION.LOCAL_TRANSACTION_ID`) | ~2 s |

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

Three record kinds:

| Kind | Recognised by | Carries |
|---|---|---|
| BEGIN | `status == "BEGIN"` | transaction id |
| row change | has `op` | `before`, `after`, `source`, `transaction.total_order` |
| END | `status == "END"` | transaction id, `event_count`, per-table counts |

`op` is `c` insert, `u` update, `d` delete. The END marker's `event_count` tells
the monitor a write set is complete without waiting for the next transaction.

Per-backend envelope differences:

| | MariaDB | YugabyteDB | Oracle |
|---|---|---|---|
| transaction identity | `source.gtid` | `source.lsn` (commit hybrid time) | `source.txId` |
| column values | plain | wrapped `{"value":…,"set":…}` | plain, numbers as strings |
| timestamps | ms + µs + ns | ms | ms |

On YugabyteDB, `transaction.id` is a composite `txId:lsn` and `source.txId` is a
per-slot counter that resets — use `source.lsn` as the identity. On Oracle,
`source.txId` is the XID as little-endian hex of `usn.slot.sqn`.

## Client protocol

All three backends default to Snapshot Isolation server-side, so the client
does not set an isolation level. It only has to tag its read log with the same
key the stream carries.

**MariaDB** — read the GTID after a successful `COMMIT`, on the same session.
After a read-only, rolled-back, or aborted transaction it still holds the
previous value, so only read it after a commit that succeeded.

```sql
START TRANSACTION;
  -- reads and writes; client logs each read
COMMIT;
SELECT @@session.last_gtid;
```

**Oracle** — read the XID inside the transaction. Conflicts surface as
`ORA-08177`; the client needs a retry path.

```sql
  -- reads and writes; client logs each read
SELECT DBMS_TRANSACTION.LOCAL_TRANSACTION_ID FROM DUAL;
COMMIT;
```

**YugabyteDB** — no server-side key exists, so the client supplies one.
`session_txn` holds one row per session, upserted as the **last** statement of
the transaction, so the marker travels inside that transaction's change set.
The table starts empty; sessions need no provisioning. `REPLICA IDENTITY FULL`
means the before-image carries the previous `txn_id`, giving a per-session chain
check for free.

```sql
BEGIN;
  -- reads and writes; client logs each read
INSERT INTO session_txn (session_id, txn_id) VALUES ('<sid>', <counter>)
  ON CONFLICT (session_id) DO UPDATE SET txn_id = EXCLUDED.txn_id;
COMMIT;
```

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

Trim consumed history with `XTRIM cdc MINID <id>` to keep memory flat.

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
| `gtid_strict_mode` | OFF | ON | GTID is the join key |
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
| per-table supplemental logging | none | `ALL COLUMNS` |
| default isolation | READ COMMITTED | SERIALIZABLE (= SI), via an `AFTER LOGON` trigger on the `SITEST` schema |

**Redis** — `--save ""` (default has RDB save points), `--appendonly no`
(default), `--maxmemory 2gb` (default unlimited). It is a transport, not a
system of record.

## Layout

```
docker-compose.yml                        # profiles: mariadb | yugabyte | oracle
mariadb/conf.d/cdc.cnf                    # binlog, GTID, isolation, durability
mariadb/initdb/*.sql                      # accounts + schema
yugabyte/init.sql, init.sh                # schema first, HYBRID_TIME slot last
oracle/startdb.d/                         # ARCHIVELOG, suppl. logging, user, schema
debezium/application*.properties          # one per backend
debezium/Dockerfile.yugabyte              # YB connector on a Java 17 base
scripts/lib.sh                            # backend detection, shared SQL helpers
scripts/txn.sh                            # client protocol reference, all backends
scripts/stream.sh                         # read/follow, handles all envelopes
scripts/bench.sh, sql.sh, status.sh
```

`registers` is a placeholder workload table in all three backends — a bank of
integer registers, the usual write-skew testbed. Replace it with your schema and
update the connector's table include list.

## Credentials

Development defaults, plaintext. MariaDB `root`/`rootpw`, `debezium`/`debezium_pw`,
`app`/`app_pw`. YugabyteDB `yugabyte`/`yugabyte`. Oracle `sys`/`oraclepw`,
`c##dbzuser`/`dbz`, `sitest`/`sitestpw`.
