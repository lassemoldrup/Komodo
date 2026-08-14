# si-monitoring

CDC harness for monitoring isolation anomalies under Snapshot Isolation.

Debezium captures the **commit order** of transactions and the **values written**
by each write. The client logs its own **read events**. The two halves are
joined on a per-transaction key.

Two interchangeable backends share one downstream half:

```
   MariaDB     --binlog (ROW, GTID)-----\
                                         >-- Debezium Server --> Redis "cdc" --> C++ monitor
   YugabyteDB  --logical repl (HT LSN)--/                                             ^
                       ^                                                              |
                       └──────────────── workload client ─────────────────────────────┘
                                   (logs read events, tagged by txn key)
```

Exactly one backend runs at a time and both write to the same Redis stream, so
the monitor reads one place regardless of backend.

| Service       | Container         | Port | Role |
|---------------|-------------------|------|------|
| `redis`       | `cdc-redis`       | 6379 | Single ordered stream `cdc` (shared) |
| `mariadb`     | `cdc-mariadb`     | 3306 | MariaDB 12.3, ROW binlog + GTID, tmpfs datadir |
| `debezium`    | `cdc-debezium`    | –    | Debezium Server 3.2 |
| `yugabyte`    | `cdc-yugabyte`    | 5433 | YugabyteDB, logical replication |
| `debezium-yb` | `cdc-debezium-yb` | –    | Debezium Server 2.5.2 + YugabyteDB connector |

## Quick start

```bash
make up                  # MariaDB (default); blocks until Debezium is streaming
make up DB=yugabyte      # YugabyteDB instead

make txn                 # run a write transaction, print its correlation marker
make stream              # show what Debezium captured
make status              # health of the active backend
make reset               # wipe everything for the active backend
```

Every target takes `DB=`; it defaults to `mariadb`. The scripts infer the
backend from the running containers, so `./scripts/*.sh` need no flag.

## The join key

The two backends differ here, because MariaDB hands out a usable key and
YugabyteDB does not.

### MariaDB — server-supplied GTID

`SELECT @@session.last_gtid` on the **same session**, immediately after
`COMMIT`, returns the GTID that transaction committed under. Every Debezium
record for that transaction carries the identical string in `source.gtid`,
`transaction.id`, and the `id` of its BEGIN/END markers.

```console
$ ./scripts/txn.sh
marker: gtid=0-1-9

$ ./scripts/stream.sh
BEGIN 0-1-9
  update registers    before={"id":1,"val":1} after={"id":1,"val":2}
  update registers    before={"id":2,"val":-1} after={"id":2,"val":-2}
END   0-1-9  events=2  sitest.registers=2
```

```sql
START TRANSACTION;
  -- ... reads and writes; the client logs each read locally ...
COMMIT;
SELECT @@session.last_gtid;   -- tag this transaction's read log with this
```

Caveat: `last_gtid` only advances when the session writes to the binlog. After
a read-only, rolled-back, or conflict-aborted transaction it still holds the
*previous* write transaction's GTID rather than an empty string. Harmless
here, since only committed write transactions are correlated — but read it
only after a successful `COMMIT`.

### YugabyteDB — client-supplied marker table

YugabyteDB exposes no equivalent. Every server-side channel was tested and
rejected (see *Why YugabyteDB needs a marker table* below), so the client
supplies the key itself.

`session_txn` holds **one row per client session, upserted in place**. The
marker is the **last statement before `COMMIT`**, so the workload runs first
and the bookkeeping write cannot perturb it:

```sql
BEGIN ISOLATION LEVEL REPEATABLE READ;
  -- ... reads and writes; the client logs each read locally ...
INSERT INTO session_txn (session_id, txn_id) VALUES ('<sid>', <client counter>)
  ON CONFLICT (session_id) DO UPDATE SET txn_id = EXCLUDED.txn_id;
COMMIT;
```

```console
$ SESSION=sessA TXN_ID=102 ./scripts/txn.sh
marker: session_id=sessA txn_id=102

$ ./scripts/stream.sh
BEGIN 3:7318325031662411776
  update registers      before={"id":1,"val":1} after={"id":1,"val":2}
  update registers      before={"id":2,"val":-1} after={"id":2,"val":-2}
  MARKER  session=sessA txn_id=102 (prev=101)  txn=7318325031662411776
END   3:7318325031662411777  events=3  public.session_txn=1 public.registers=2
```

The marker always arrives with the **highest `transaction.total_order`** in
its transaction, after every workload event.

Why a fixed row per session rather than an inserted row per transaction:

- the table never grows
- sessions only touch their **own** row, so the marker introduces no
  cross-session write conflicts under SI
- `REPLICA IDENTITY FULL` means the before-image carries the *previous*
  `txn_id`, giving a free per-session chain check: if `prev` is not the last
  `txn_id` you saw for that session, a transaction went missing
- the workload is unconstrained: with
  `cdc_enable_intra_transactional_before_image` on (see below), it may insert,
  update and delete the same row within one transaction

Upsert rather than plain `UPDATE` means **sessions need no provisioning**:
`session_txn` starts empty and a new session id creates its own row on first
use. That first marker is `op=c` with `before=null`; every later one is `op=u`
carrying the previous `txn_id`.

Verified on a clean stack, 4 concurrent sessions × 6 transactions:

```
committed transactions        : 24
commit-HT strictly increasing : True
transactions WITHOUT a marker : 0
duplicate (session,txn_id)    : 0
marker is LAST event in txn   : True
op=c count (first use)        : 4
op=u count (subsequent)       : 20
per-session prev-chain intact : True (all 4)
```

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

## YugabyteDB specifics

### Why a marker table

Four server-side correlation channels were tested against YugabyteDB 2026.1.
All four fail, which is why the client supplies the key:

| Channel | Outcome |
|---|---|
| `pg_current_xact_id()` | Global, not per-session. Offset vs `source.txId` **drifts** (16→18 over 40 txns) and the two orders disagree — xid is assigned at transaction start, `txId` at commit |
| `yb_get_current_transaction()` UUID | Client-visible but **never appears in the stream**; also returns NULL during the first write statement |
| `pg_logical_emit_message()` | Exists and returns an LSN, but is a **silent no-op** — the message is never decoded |
| Replication origins | Work, but concurrent origin sessions are capped by `max_replication_slots` (10, shared with real slots — 16 sessions → 6 succeeded), and origin-tagged changes are by convention filterable downstream |

The mapping you'd actually want — transaction UUID → commit hybrid time —
does exist, inside the `System.transactions` status tablets. It is not
queryable at any level (`relation "transactions" does not exist`; `yb-admin`
has only management commands), and the records are garbage-collected once the
transaction resolves.

### Two operational rules

**1. All schema before the replication slot.** DDL against a table with an
active slot permanently breaks before-image computation for that tablet:

```
ERROR: Failed to get the beforeimage for tablet_id: …
Where: Catalog Version Mismatch: A DDL occurred while processing this query.
```

The connector then **retries forever** rather than failing. Recreating the
slot does not fix it; recreating the table does not fix it. `yugabyte/init.sh`
enforces the order — schema, seed, then slot last.

**2. Enable `cdc_enable_intra_transactional_before_image`.** It is **off by
default**, and with it off, touching a row again after `INSERT`ing it in the
same transaction makes the CDC service fail to compute a before-image:

```
ERROR:  Failed to get the beforeimage for tablet_id: ce9dff026bcd465b912ee67ee796dd22
CONTEXT:  Catalog Version Mismatch: A DDL occurred while processing this query. Try again.
```

This is a **YugabyteDB server-side limitation, not a connector bug** —
`pg_recvlogical`, with Debezium entirely out of the picture, reproduces it
identically. (The `CONTEXT` line is spurious: no DDL is involved. That
misattribution is much of why it is hard to diagnose.)

The compose file sets the flag on the tserver, which fixes it properly:

| Pattern | flag off (default) | flag on |
|---|---|---|
| INSERT then UPDATE same row | **stalls permanently** | works |
| INSERT then DELETE same row | **stalls permanently** | works |
| everything else | works | works |

The flag does more than suppress the error — it changes before-images to be
**intra-transactional**, which is the semantics a version-chain reconstruction
wants:

```
                                   flag off              flag on
UPDATE ... val=10   before=  {"val":0}             {"val":0}
UPDATE ... val=20   before=  {"val":0}   <-- snapshot   {"val":10}  <-- predecessor
```

So with the flag on, `before` is the immediately preceding value *within the
transaction*; with it off, every write in a transaction reports the value as
of the transaction's read snapshot.

There is also a weaker flag, `cdc_send_null_before_image_if_not_exists`, which
merely returns a null before-image instead of erroring. Prefer the
intra-transactional flag: it yields real values rather than nulls.

**Why this matters operationally:** the YB fork's `PostgresErrorHandler`
classifies every error as retriable —

```java
protected boolean isCustomRetriable(Throwable throwable) {
    // YB Note: Yes, all the errors are custom retriable.
    return true;
}
```

— and `errors.max.retries = -1`. So without the flag, a permanent server-side
error becomes an infinite retry loop: the connector never fails, never
advances, and emits nothing, while the stack still looks healthy. Watch the
error count in `make status`, not just liveness.

### The slot must be HYBRID_TIME

`init.sh` creates it explicitly because Debezium's auto-creation defaults to
`SEQUENCE`:

```sql
SELECT pg_create_logical_replication_slot('dbz_si','yboutput',false,false,'HYBRID_TIME');
```

Under `HYBRID_TIME`, `source.lsn` **is** the commit hybrid time: identical
across every event of a transaction, unique, and monotonic in commit order —
verified across tablets under concurrency. Under `SEQUENCE` it increments per
row and loses that property. `debezium.source.publication.autocreate.mode` is
`disabled` so Debezium cannot replace the slot with a `SEQUENCE` one.

Do not use `source.txId` as an identifier: it is a per-slot 32-bit counter
read straight off the pgoutput BEGIN message, and it resets when the slot is
recreated.

## Layout

```
docker-compose.yml                        # profiles: mariadb | yugabyte
mariadb/conf.d/cdc.cnf                    # binlog, GTID, isolation, durability
mariadb/initdb/*.sql                      # accounts + placeholder schema
yugabyte/init.sql                         # schema + session_txn marker table
yugabyte/init.sh                          # schema first, HYBRID_TIME slot last
debezium/application.properties           # MariaDB source (Debezium 3.2)
debezium/application-yugabyte.properties  # YugabyteDB source (Debezium 2.5.2)
debezium/Dockerfile.yugabyte              # YB connector on a Java 17 base
(no vendored jars — Dockerfile pulls them in a build stage)
scripts/lib.sh                            # backend detection, shared SQL helpers
scripts/txn.sh                            # correlation protocol, both backends
scripts/stream.sh                         # read/follow, handles both envelopes
scripts/bench.sh, sql.sh, status.sh
```

The `registers` table is a placeholder in both backends — a bank of integer
registers, the usual write-skew testbed. Replace it with your workload schema;
only `table.include.list` / `database.include.list` need updating.

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
