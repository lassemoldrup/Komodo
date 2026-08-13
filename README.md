# maria-db-cdc

CDC harness for monitoring isolation anomalies under Snapshot Isolation.

Debezium captures the **commit order** of transactions and the **values written**
by each write. The client logs its own **read events**. The two halves are
joined on the MariaDB **GTID**.

```
                    binlog (ROW, GTID)          single ordered stream
   MariaDB  ──────────────────────────▶  Debezium Server  ──────────▶  Redis "cdc"  ──▶  C++ monitor
      ▲                                                                                      ▲
      │ SQL + @@session.last_gtid                                                            │
      └──────────────────────────  workload client  ─────────────────────────────────────────┘
                                    (logs read events, tagged by GTID)
```

Three containers. No Kafka, no Kafka Connect, no proxy.

| Service    | Container      | Port | Role |
|------------|----------------|------|------|
| `mariadb`  | `cdc-mariadb`  | 3306 | Source. MariaDB 12.3, ROW binlog + GTID, datadir on tmpfs |
| `debezium` | `cdc-debezium` | –    | Debezium Server 3.2, embedded engine |
| `redis`    | `cdc-redis`    | 6379 | Single ordered stream `cdc` |

## Quick start

```bash
make up        # start; blocks until Debezium is streaming
make txn       # run a write transaction, print its GTID
make stream    # show what Debezium captured
make bench     # throughput + latency
make reset     # wipe everything
```

## The join key

`SELECT @@session.last_gtid` on the **same session**, immediately after
`COMMIT`, returns the GTID that transaction committed under. Every Debezium
record for that transaction carries the identical string in `source.gtid`,
`transaction.id`, and the `id` of its BEGIN/END markers.

```console
$ ./scripts/txn.sh
0-1-8

$ ./scripts/stream.sh
BEGIN 0-1-8
  update registers    before={"id":1,"val":0} after={"id":1,"val":1}
  update registers    before={"id":2,"val":0} after={"id":2,"val":-1}
END   0-1-8  events=2  sitest.registers=2
```

So the client protocol is just:

```sql
START TRANSACTION;
  -- ... reads and writes; the client logs each read locally ...
COMMIT;
SELECT @@session.last_gtid;   -- tag this transaction's read log with this
```

One caveat worth knowing: `last_gtid` is a *session* variable that only
advances when the session writes to the binlog. After a read-only or
rolled-back transaction it still holds the previous write transaction's GTID
rather than an empty string. That is harmless here because only committed
write transactions are correlated with Debezium, but a client that queried it
after a read-only transaction would silently get the wrong ID.

## Ordering guarantee

All events land in **one Redis stream** (`cdc`), which is what makes commit
order recoverable. Debezium would otherwise emit one topic per table, and
per-table ordering cannot be merged back into a global order. A `RegexRouter`
transform collapses every topic — row changes *and* transaction markers —
into a single stream:

```properties
debezium.transforms.route.regex=.*
debezium.transforms.route.replacement=cdc
```

Stream order = binlog order = commit order. Redis stream IDs are monotonic, so
the monitor can also use them as a cursor and resume from any point.

Note that the binlog records transactions **at commit time**, in commit order.
It carries no information about when a transaction *began* or what snapshot it
read. That is precisely the half the client-side read log supplies.

## Stream layout

Each entry has exactly one field, where the **field name** is the Debezium
message key and the **field value** is the envelope:

```
1786627669428-0
  field name : {"id":1}
  field value: {"before":{...},"after":{...},"source":{...,"gtid":"0-1-8"},
                "transaction":{"id":"0-1-8","total_order":1,...},"op":"u",...}
```

Three record kinds, in commit order:

| Kind | Recognised by | Carries |
|------|---------------|---------|
| BEGIN | `status == "BEGIN"` | `id` (GTID) |
| row change | has `op` | `before`, `after`, `source.gtid`, `transaction.total_order` |
| END | `status == "END"` | `id`, `event_count`, per-table counts |

`op` is `c` insert, `u` update, `d` delete. The END marker's `event_count` lets
the monitor know a write set is complete without waiting for the next
transaction to arrive.

## Reading the stream from C++

`XREAD BLOCK 0` gives a blocking, ordered, resumable read. With hiredis:

```cpp
// last_id starts at "0-0" to replay from the beginning, or "$" for live-only.
std::string last_id = "0-0";
while (true) {
    redisReply* r = (redisReply*)redisCommand(
        ctx, "XREAD BLOCK 0 COUNT 1000 STREAMS cdc %s", last_id.c_str());
    if (!r) break;
    // r->element[0]->element[1] is the array of entries
    redisReply* entries = r->element[0]->element[1];
    for (size_t i = 0; i < entries->elements; i++) {
        redisReply* e   = entries->element[i];
        last_id         = e->element[0]->str;              // "<ms>-<seq>"
        redisReply* kv  = e->element[1];                   // [key, value]
        const char* env = kv->element[1]->str;             // envelope JSON
        handle(env);
    }
    freeReplyObject(r);
}
```

Trim consumed history with `XTRIM cdc MINID <id>` so Redis memory stays flat
during long runs.

## Measured performance

On an M-series laptop, single-session workload:

```
$ make bench
transactions          : 2000
commit throughput     : 4,200 txn/s  (0.48s, single session)
backlog drain         : 0.09s after last commit
commit -> stream (last txn): 13 ms
```

Latency is measured server-side on both ends — MariaDB's `SYSDATE(3)` right
after `COMMIT` versus the Redis entry ID, which is a millisecond epoch stamped
at `XADD`. Measuring from the host instead reports ~67 ms, essentially all of
it `docker exec` overhead.

Throughput scales with concurrent clients; 4,200 txn/s is one session issuing
transactions serially, so it is a latency-bound floor, not a ceiling.

## What was tuned, and why

**MariaDB** (`mariadb/conf.d/cdc.cnf`)

| Setting | Value | Reason |
|---|---|---|
| `binlog_row_image` | `FULL` | The monitor needs written values and the overwritten version. Must not be reduced. |
| `binlog_row_metadata` | `FULL` | Column names, emitted per table per transaction, not per row. |
| `binlog_annotate_row_events` | `OFF` | SQL text in the binlog is dead weight; the client logs its own ops. |
| `log_slave_updates` | `OFF` | No replicas in this topology. |
| `sync_binlog` / `innodb_flush_log_at_trx_commit` | `0` / `0` | Removes fsync from every commit. Affects crash durability only — **never isolation semantics**. |
| datadir | tmpfs | Throwaway harness; keeps disk I/O out of the measured workload. |

**Debezium** (`debezium/application.properties`)

| Setting | Value | Reason |
|---|---|---|
| `snapshot.mode` | `no_data` | Only the history the workload generates matters, not prior state. |
| `provide.transaction.metadata` | `true` | BEGIN/END markers and per-event transaction ordinals. |
| `tombstones.on.delete` | `false` | Nothing compacts by key; tombstones are pure overhead. |
| `include.schema.changes` | `false` | DDL events not needed. |
| `decimal.handling.mode` | `string` | Exact values instead of base64 unscaled bytes. |
| `poll.interval.ms` | `10` | Low latency. |
| schema history / offsets | local files | No Kafka topics required. |

**Redis**: `--save "" --appendonly no`. The stream is transport, not a system
of record, so fsync is off the hot path.

### Further trimming, if you need it

The envelope is verbose (~500 bytes; most of it `source`). An
`ExtractNewRecordState` transform can flatten it to the after-image plus
chosen fields:

```properties
debezium.transforms=route,flatten
debezium.transforms.flatten.type=io.debezium.transforms.ExtractNewRecordState
debezium.transforms.flatten.add.fields=op,source.gtid,transaction.id
```

This drops `before`, which is likely information you want for version
tracking — hence it is off by default.

Setting `provide.transaction.metadata=false` saves two entries per
transaction. Boundaries remain recoverable because events of one transaction
are contiguous and share a GTID, but the monitor then cannot tell that the
*most recent* transaction is complete until the next one arrives.

## Layout

```
docker-compose.yml
mariadb/conf.d/cdc.cnf            # binlog, GTID, durability, throughput
mariadb/initdb/01-users.sql       # debezium + app accounts
mariadb/initdb/02-schema.sql      # placeholder registers table
debezium/application.properties   # source, routing, sink, tuning
scripts/txn.sh                    # correlation protocol reference
scripts/stream.sh                 # read/follow the stream
scripts/bench.sh                  # throughput + latency
scripts/sql.sh, scripts/status.sh
```

`mariadb/initdb/02-schema.sql` is a placeholder — a bank of integer registers,
the usual write-skew testbed. Replace it with your workload schema and update
`debezium.source.database.include.list` if you rename the database. Nothing
else depends on the table definitions.

## MariaDB version

Pinned to `mariadb:12.3`, the current LTS (`:lts` and `:latest` both resolve
to it). Pinned rather than tracking `:lts` so that experiment runs stay
reproducible when the alias advances.

Override per run without editing anything:

```bash
MARIADB_IMAGE=mariadb:11.8 make reset up
```

11.8.8 and 12.3.2 were both verified against the full pipeline — same SI
semantics, same Debezium compatibility, throughput equal within noise.

## Credentials

Development defaults, plaintext. `root`/`rootpw`, `debezium`/`debezium_pw`,
`app`/`app_pw`.

## Isolation level

This harness runs **textbook Snapshot Isolation**. MariaDB's
`innodb_snapshot_isolation` is ON by default and is pinned explicitly in
`cdc.cnf`, along with `transaction_isolation=REPEATABLE-READ` (the setting
only applies at RR).

Verified behaviour on 12.3.2 (identical results on 11.8.8):

| Scenario | Result |
|---|---|
| Two transactions write the **same row** from overlapping snapshots | second is **aborted**: `ERROR 1020 (HY000): Record has changed since last read in table ...` |
| Write skew — **disjoint rows**, overlapping reads, shared invariant | both commit; invariant violated |

The first is first-committer-wins, and the error is `ER_CHECKREAD` (1020), not
`ER_LOCK_WAIT_TIMEOUT` (1205) — an immediate snapshot conflict rather than
lock contention. The second is the SI-defining anomaly and is exactly what the
monitoring algorithm is meant to detect.

Two consequences for the workload client:

- **Transactions can now abort**, so the client needs a retry path on error
  1020. Historically InnoDB RR would have blocked instead.
- **Aborted transactions produce no binlog entry**, hence no Debezium events
  and no new GTID. Combined with the `last_gtid` staleness noted above, a
  client that reads `@@session.last_gtid` after a *failed* transaction gets
  the previous successful one. Only read it after a successful `COMMIT`.

Prefer `START TRANSACTION WITH CONSISTENT SNAPSHOT` in the workload. With a
plain `START TRANSACTION` the read view is deferred to the first read, so the
snapshot point is not well defined at the statement the client logs as the
transaction's start.
