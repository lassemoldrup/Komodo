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

`blackbox_run_monitor` owns one complete audited run. It first requires the
client stream's first record to be the authoritative `RunManifest`, then replays
CDC from `0-0` while consuming the remaining client records from the manifest's
exact stream cursor. It validates the `BootstrapManifest`, every `SessionDone`
counter set and rolling marker digest, and any `RunFailed` notification. The
monitor continues CDC ingestion behind a blocked checker head, remembers but
does not process `END_FENCE` out of order, and waits for an input-idle boundary
after closure so an already-queued post-done or post-fence record cannot be
skipped.

Before any status append or evidence replay, the monitor atomically claims
`blackbox:monitor-owner:<run_id>` only if both that key and the run's status
stream are absent. The permanent claim makes a concurrent or replacement
monitor fail with `COMPONENT_RESTARTED`; it is removed only when the fresh Redis
stack is destroyed. Redis connection, ownership, blocking-read, and status-write
operations all have deadlines bounded by the run's overall timeout.

At shutdown it reconciles the expected session roster, accepted client and
marker-bearing CDC counts, joined transactions, bootstrap transactions, phase
anchors, checker totals, unmatched halves, and ambiguous attempts. A clean run
emits exactly one final `PASS` or `VIOLATION`; any protocol or operational
invalidity emits `INVALID`, even if the checker previously observed a
violation. The earlier attributed violation remains in the final diagnostics.
Lifecycle acknowledgements and the terminal record are written to
`blackbox:status:<run_id>` with acknowledged `XADD`s, and the process prints a
human-readable final report.

When the coordinator cannot prove client/database quiescence or a fence commit
has an ambiguous outcome, it cannot create an audited closing barrier. That
path publishes best-effort `RunFailed`, reports timeout-invalid locally,
terminates its owned monitor process, and deliberately does not wait for or
claim a terminal monitor record.

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
runs those diagnostic paths plus the complete one-run monitor against fresh
MariaDB/Debezium/Redis evidence. The prefix tool deliberately leaves
`END_FENCE` queued so its output continues to expose the pre-closure ordered
prefix; the one-run monitor performs the audited terminal reconciliation.

`make test-correctness-live` builds the BenchBase `FracturedReadProbe` client
and runs four fresh audited stacks: a clean repeatable-read pass, a coordinated
read-committed fractured-read violation, the same coordinated probe passing
under repeatable read, and an intentionally omitted client record producing an
invalid result. The configured `BENCHBASE_ROOT`, `PROBE_BUILD_DIR`, and `MAVEN`
are used for both compilation and execution. The harness validates both worker
roles, the canonical client values, the final two-row database state, strict
head-of-line blocking in the omitted-record case, and terminal reconciliation.
Every case and evidence-capture command has a hard wall-clock bound. A failed
run retains its raw CDC, client, and status streams plus Compose state and
container logs before teardown. These runs generate fresh run IDs and do not
replace or modify the checked-in captured CDC fixtures.

Run the focused audited TPC-C transaction acceptances against a fresh stack with:

```bash
./tests/run_tpcc_order_status_acceptance.sh \
  --benchbase-dist /path/to/benchbase-mariadb \
  --monitor-bin /path/to/blackbox_run_monitor \
  --output-dir /path/to/new-output-directory

./tests/run_tpcc_new_order_acceptance.sh \
  --benchbase-dist /path/to/benchbase-mariadb \
  --monitor-bin /path/to/blackbox_run_monitor \
  --output-dir /path/to/another-new-output-directory

./tests/run_tpcc_payment_acceptance.sh \
  --benchbase-dist /path/to/benchbase-mariadb \
  --monitor-bin /path/to/blackbox_run_monitor \
  --output-dir /path/to/a-third-new-output-directory

./tests/run_tpcc_delivery_acceptance.sh \
  --benchbase-dist /path/to/benchbase-mariadb \
  --monitor-bin /path/to/blackbox_run_monitor \
  --output-dir /path/to/a-fourth-new-output-directory

./tests/run_tpcc_stock_level_acceptance.sh \
  --benchbase-dist /path/to/benchbase-mariadb \
  --monitor-bin /path/to/blackbox_run_monitor \
  --output-dir /path/to/a-fifth-new-output-directory
```

The OrderStatus default uses one warehouse, one terminal, RR, seed 3, and weights
`0,0,100,0,0`. It retains a machine-readable run configuration, independent
customer last-name cardinalities, client/status stream captures, the CDC tail
strictly after `LOAD_FINISHED_SEEN`, monitor metrics, and an acceptance report.
Every control has a command-line or corresponding
`BLACKBOX_TPCC_ORDER_STATUS_*`, `BLACKBOX_TPCC_NEW_ORDER_*`, or
`BLACKBOX_TPCC_PAYMENT_*`, `BLACKBOX_TPCC_DELIVERY_*`, or
`BLACKBOX_TPCC_STOCK_LEVEL_*` environment form;
`--print-config` validates and prints the resolved non-destructive run
configuration. NewOrder defaults to one warehouse, two terminals, RR, seed 144,
and weights `100,0,0,0,0`; its report reconciles invalid-item aborts, retries,
repeated item/stock keys, and exact application writes. A remote item cannot be
selected with one warehouse, so that path remains fixture-only and the live
report records `remote_item_live_applicable=false`. The live drivers are
destructive to the repository Compose stack and tear that stack down unless
`--keep-stack` is explicitly selected.

Payment uses weights `0,100,0,0,0` and independently validates ID/name and
good-/bad-credit reads, post-update local rows, four-table CDC writes, and
generated history identities. A remote customer warehouse is impossible at
one warehouse, so its report records `remote_customer_live_applicable=false`;
the deterministic fixture covers that branch.

Delivery uses weights `0,0,0,100,0` with at least two terminals. Its report
proves empty and active district cases, snapshot pairs, marker-only empty work,
multi-district and multi-line updates, conservative retries, exact deletes and
updates, and terminal reconciliation without treating the omitted aggregate as
a read. Delivery preserves BenchBase's production loop from district 1 through
each worker's upper bound; overlapping worker ranges are intentional and drive
the live zero-delete/1020 race coverage.

StockLevel uses weights `0,0,0,0,100`. Every accepted client must contain one
district snapshot Present read, while its workload CDC transaction remains
marker-only; stock rows and aggregate-derived evidence are rejected by the
focused acceptance validator.

Run the standard one-warehouse/four-terminal mix and retain its bounded replay
corpus with:

```bash
./tests/run_tpcc_full_mix_acceptance.sh \
  --benchbase-dist /path/to/benchbase-mariadb \
  --monitor-bin /path/to/blackbox_run_monitor \
  --resource-exhaustion-metrics /path/to/recorded-invalid-monitor-metrics.json \
  --output-dir /path/to/new-full-mix-output
```

The profile fixes weights `45,43,4,4,4`, requires positive results for every
transaction type, validates all four sessions and all nine bootstrap tables,
and accepts only reconciled `PASS` or checker-attributed `VIOLATION`. It records
stream checksums and a size-bounded full CDC/client replay manifest. Run the
required offline fail-closed injections against that capture with:

```bash
./tests/run_tpcc_full_mix_mutations.sh \
  --corpus-dir /path/to/new-full-mix-output \
  --monitor-bin /path/to/blackbox_run_monitor \
  --output-dir /path/to/new-mutation-output
```

The mutation runner drops a client, a workload CDC transaction, and a workload
CDC row, then injects a wrong JSON-typed client value and a RunManifest table
roster mismatch. Every replay must end `INVALID`; aggregate, predicate,
implicit-DML, and locking-read exclusions are not used as negative tests.

## Performance measurement

The formal performance experiment has exactly two modes. Both use the current
BenchBase checkout, the same RR database/server contract, evidence-compatible
schema, deterministic loaded bytes, scale, mix, seed, terminal count, rate,
warmup, and measured duration:

- `benchbase_uninstrumented` omits `<blackbox>` completely. It uses ordinary
  benchmark SQL, `Worker`, and retry behavior, with no client capture/encoding,
  marker, Redis publication, Debezium consumer, or C++ monitor.
- `full_e2e` enables audited client capture and canonical encoding, marker and
  Redis publication, MariaDB CDC collection by external Debezium Server, and
  the C++ monitor's assembly/normalization of raw Debezium records, joiner, and
  checker.

Debezium belongs to the full black-box system but is not part of the C++
Monitor. The initial TPC-C target is one warehouse, four terminals, 120 seconds,
three repetitions, and mix `45,43,4,4,4`:

```bash
./scripts/run_blackbox_performance.sh \
  --benchmark tpcc \
  --warehouses 1 \
  --terminals 4 \
  --duration-seconds 120 \
  --repetitions 3 \
  --mix 45,43,4,4,4 \
  --loader-timestamp-utc 2000-01-01T00:00:00Z \
  --rate unlimited \
  --drain-timeout-seconds 30 \
  --status-timeout-seconds 900 \
  --high-water-threshold-percent 70 \
  --monitor-bootstrap-state-bytes 1073741824 \
  --output-root /media/newhd/yifan/db-monitoring-builds/blackbox-performance/tpcc-w1-t4
```

Backlog and head-of-line measurements remain in the reports as diagnostic
telemetry. Acceptance requires terminal queues and unmatched evidence to be
zero, the end-to-verdict tail to stay within `--drain-timeout-seconds`, and
resource high-water marks to remain below
`--high-water-threshold-percent`; it does not impose a backlog-trend rule.

By default, the smoke target executes both formal modes once with scale one and
two requested measurement seconds. YCSB smoke uses two terminals and one second
of warmup; TPC-C smoke uses four terminals and no warmup so the standard mix is
exercised without adding another workload interval after its larger bootstrap.
The default measured YCSB target uses three rotated repetitions, four terminals,
scale ten, five seconds of warmup, and twenty requested measurement seconds:

```bash
docker exec db-monitoring-dev bash -lc '
  cd /home/yifan/code/db-monitoring/si-monitoring
  make performance-smoke \
    PERFORMANCE_OUTPUT_ROOT=/media/newhd/yifan/db-monitoring-builds/blackbox-performance/smoke-20260818
  make performance-measure \
    PERFORMANCE_OUTPUT_ROOT=/media/newhd/yifan/db-monitoring-builds/blackbox-performance/run-20260818
'
```

Pass `--mode NAME` directly to `scripts/run_blackbox_performance.sh` to run
only one mode. Selection is set-like: names must be unique, the selected roster
is normalized to canonical order, and each repetition rotates only that roster.
Omitting `--mode` runs both modes. For example, this runs only full E2E:

```bash
./scripts/run_blackbox_performance.sh \
  --benchmark tpcc \
  --output-root /media/newhd/yifan/db-monitoring-builds/blackbox-performance/full-e2e-only \
  --mode full_e2e
```

Selected-mode runs record their roster in each trial's metadata and in
`mode_order.csv`. Client overhead is available only when both modes are
present. Full-E2E artifacts always keep client, external CDC, C++ Monitor, and
complete evidence-path metrics distinct.

The generated BenchBase configurations use loader batch size 128 by default.
Pass `--loader-batch-size N` to select another positive value. The selection is
written to configuration, trial metadata, and `loader_validation.json`; loader
acceptance never assumes 128. Every current-loader trial independently compares
the exact loader-declared table counts with SQL counts before creating the
codec-dependent bootstrap manifest.

The generated configurations also record a fixed loader timestamp. It defaults
to `2000-01-01T00:00:00Z` and can be selected with
`--loader-timestamp-utc YYYY-MM-DDTHH:MM:SSZ` or the
`BLACKBOX_PERFORMANCE_LOADER_TIMESTAMP_UTC` environment variable. The runner
rejects offsets, fractional seconds, whitespace, and invalid calendar values.
For TPC-C this fixes `c_since`, `h_date`, `o_entry_d`, and non-null
`ol_delivery_d` loader values across modes and repetitions; workload-generated
timestamps remain real-time. The exact value is preserved in both generated
profiles, system provenance, and every trial's metadata, and the report rejects
cross-trial differences.

Both formal modes use the current BenchBase loader and atomically publish a
`loader_declaration` JSON only after successful completion. Loading is outside
the measured workload. The runner independently validates the declaration
against SQL state and requires the canonical bootstrap state to agree across
modes; this validation does not enable client tracing in the uninstrumented
workload.

Captured `redis-cli --json XRANGE` evidence can be restored with its original
stream IDs through a bounded RESP producer:

```bash
set -o pipefail
python3 scripts/xrange_to_redis_pipe.py \
  --input-json /media/newhd/yifan/db-monitoring-builds/path/to/cdc_stream.json \
  --stream cdc | redis-cli --pipe
```

The converter validates the complete capture, including canonical increasing
IDs and every UTF-8 field/value pair, before writing any command. It preserves
flat-array field order and writes RESP one argument at a time, so output memory
is bounded by the largest field. It uses standard-library `json.load`, so the
parsed input document itself must fit in memory.

Measure whole-process monitor capacity by replaying one complete captured
client/CDC corpus into a fresh Redis before starting the monitor:

```bash
./scripts/run_blackbox_monitor_performance.sh \
  --corpus-dir /media/newhd/yifan/db-monitoring-builds/blackbox-performance/measurements/final-120s-20260819T143836Z/trials/repetition-01/06-assembly_join_check \
  --output-root /media/newhd/yifan/db-monitoring-builds/blackbox-performance/monitor-throughput-120s \
  --stage assembly_join \
  --stage assembly_join_check
```

This separate replay microbenchmark retains its own repeatable `--stage`
selector for `assembly_join` and `assembly_join_check`; omit it to run both.
The runner verifies the source trial checksum and clean audited PASS, restores
the same stream IDs and bytes for every trial, and rotates only the selected
microbenchmark stages.
The default source-duration contract is 120 seconds. That describes how long
the captured workload ran: the preloaded monitor must drain the corpus as fast
as possible and is not delayed to fill 120 seconds.

Monitor throughput is workload transactions divided by the monitor's monotonic
full-lifecycle wall time. It includes Redis reading, protocol parsing, CDC
assembly, joining, optional checking, and terminal reconciliation; it excludes
corpus preload. The reports are `raw_monitor_trials.csv`,
`monitor_summary_statistics.csv/json`, and the concise
`monitor_headline_results.csv/md`. Join-only and checked trials use the exact
same numerator corpus, while checker core consume time remains diagnostic and
is not reported as whole-process throughput.

An output root must be below `/media/newhd/yifan/db-monitoring-builds` and must
not already contain files. Normal measured runs require clean current BenchBase,
monitor, and checker worktrees; `--smoke` is the explicit exception used to test
uncommitted harness changes. The runner also requires that its own process is
inside `db-monitoring-dev`.

The performance runner explicitly starts Redis with an 8 GiB no-eviction limit
and exposes every assembler, pending-client, pending-CDC, retained-identity,
per-transaction, bootstrap, and checker-state capacity as a command-line
option; `--help` lists their defaults. The full-E2E trial verifies Redis's
effective `maxmemory` and policy and every monitor-reported
limit against trial metadata, requires terminal backlogs to be zero, and
requires
all observed high-water marks to remain strictly below the selected threshold
(70 percent by default).

Performance output is restricted to `/media/newhd/yifan/db-monitoring-builds`
and defaults to a 107374182400-byte (100 GiB) run budget. The coordinator checks
the aggregate at mode boundaries, supervises it during the run, and streams
large Redis captures through a bounded writer. `--output-max-bytes` can lower
the budget but should not be raised for the canonical run.

Throughput, request latency, the authoritative actual elapsed time, and the
measured epoch window come from BenchBase's raw CSV and summary JSON. Logical
transactions/s is the primary client comparison because ordinary and audited
retry behavior differs. The atomic measurement-window outcome artifact supplies
exact overall and per-type successes, attempts, retries, terminal aborts, and
errors; successful TPC-C NewOrder/min is derived from that same interval. Redis
logical bytes count UTF-8 stream field and value bytes; per-stream allocation is
`MEMORY USAGE`, and network bytes are workload-window `INFO stats` counter
deltas from workload-process start through pipeline drain. CDC delivery lag is Redis stream time minus Debezium envelope time;
source lag is Redis stream time minus database source time. Head-of-line wait,
join-buffer high-water bytes, checker consume time/rate, checker retained state,
event counts, reconciliation, and full-lifecycle monitor CPU/RSS come from the
monitor metrics endpoint. BenchBase process-tree and MariaDB/Redis/Debezium
CPU/RSS use the raw `workload_process_window` resource sample CSV (JVM startup,
warmup, measured requests, and teardown). CDC component selection begins
strictly after the exact Redis stream ID of the `LOAD_FINISHED` transaction
`END` and ends inclusively at the exact `END_FENCE` transaction `END` ID. This
prevents bootstrap entries sharing the same millisecond from leaking into the
workload scope. Both IDs and their epoch milliseconds are recorded. The
component window includes warmup and measured transactions; its totals
reconcile to the complete client stream, not just BenchBase's smaller
measurement window. CDC ingestion metrics are attributed to external Debezium
Server; normalized, joined, and checked rates are attributed to the C++
Monitor. Raw CDC workload rows reconcile only to assembler raw rows; normalized
application writes separately reconcile across assembler, joiner, and checker.
They are not equal because normalization removes markers and may collapse
repeated writes or expand key changes. Complete evidence-path throughput starts at the `LOAD_FINISHED`
transaction-end Redis time and ends at the Redis XADD time of the run-scoped
`FINAL` monitor-status record. Drain tail uses that final-status time minus the
`END_FENCE` transaction-end time. The final status's exact stream ID and
milliseconds are retained, keeping every interval endpoint on the Redis server
clock rather than mixing in the monitor process clock.
`component_fence_window.json` preserves the two extracted fence transaction-end
IDs/timestamps, and `final_status_boundary.json` preserves the run-scoped
`FINAL` status ID/timestamp. `analysis_window.json` records the independent
BenchBase client measurement window, the exact-ID component window, and the
final-status boundary together.

Each output root contains generated input configurations, build logs and exact
commands, host/JVM/container/image provenance, `mode_order.csv`, and one
checksummed directory per raw trial. The aggregate deliverables are
`headline_results.md`, its machine-readable `headline_results.csv`,
`raw_trials.csv`, `transaction_results.csv`, `component_results.csv`,
`summary_statistics.csv` (median,
min, max, and MAD without outlier removal), `overhead.csv`,
`artifact_index.csv`, and the root `SHA256SUMS`. `transaction_results.csv`
contains one overall/per-type row per mode and repetition. The headline compares
uninstrumented and full-E2E BenchBase logical throughput, then reports full-E2E
client outcome metrics, external CDC ingestion, C++ Monitor throughput,
complete evidence-path throughput, drain, and resources without crossing
component boundaries. `overhead.csv` contains only the full-E2E logical-TPS
effect relative to uninstrumented BenchBase; it does not attribute that effect
to individual components. The CSV retains unrounded values and canonical mode
identifiers; Markdown applies concise display rounding and labels. Existing
output roots can be backfilled without rereading captured streams:

```bash
python3 scripts/blackbox_performance_report.py headline \
  --input-csv OUTPUT_ROOT/raw_trials.csv \
  --csv-output OUTPUT_ROOT/headline_results.csv \
  --markdown-output OUTPUT_ROOT/headline_results.md
python3 scripts/blackbox_performance_report.py checksums \
  --root OUTPUT_ROOT --output OUTPUT_ROOT/SHA256SUMS
(cd OUTPUT_ROOT && sha256sum --check SHA256SUMS)
```

When backfilling a preserved result, also record the headline command and the
reporter commit in that result's derivation/provenance before regenerating its
root manifest. Trial artifacts and their manifests remain unchanged.

Large distributions, Redis captures, BenchBase result sets, Compose logs, and
resource samples remain under that output root on the artifact volume.

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
| `character_set_server` | image default | `utf8mb4` | canonical strict UTF-8 text |
| `collation_server` | image default | `utf8mb4_bin` | binary text comparison |
| `default_time_zone` | `SYSTEM` | `+00:00` | host-independent timestamps |
| `sql_mode` | image default | pinned contract | strict writes and full-width `CHAR` reads |

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
mariadb/initdb/*.sql                      # accounts + benchmark-neutral controls
mariadb/initdb/adapters/*.sql             # one selected application schema
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

The audited MariaDB path creates the selected adapter's exact initially empty
application-table roster plus `benchbase.checker_run_fence` and
`benchbase.session_txn` before CDC starts. `BLACKBOX_BENCHMARK` selects the
adapter contract and matching init SQL; the default remains `ycsb`. The
`registers` table remains the exploratory workload table for the other backend
profiles.

## Credentials

Development defaults, plaintext. MariaDB `root`/`rootpw`, `debezium`/`debezium_pw`,
`app`/`app_pw`. YugabyteDB `yugabyte`/`yugabyte`. Oracle `sys`/`oraclepw`,
`c##dbzuser`/`dbz`, `sitest`/`sitestpw`.
