# Black-box protocol decisions

Status: frozen branch-local contract.

These decisions turn the open questions in the end-to-end design into one
implementable contract. Changing one before implementation is inexpensive;
changing one after client/monitor code lands requires updating both sides and
their golden tests in the same branch.

## Scope and result

- MariaDB is the first backend.
- Every successful supported workload transaction writes one `session_txn`
  marker and is therefore CDC-visible, including read-only and
  no-application-write transactions.
- The marker write is filtered from the normalized application write set.
- `PASS` means the complete supported point-event projection satisfies SOWW; it
  is not a predicate/phantom or arbitrary-SQL SI claim.
- Ordinary non-locking, non-aggregate result rows are retained as whole-row
  points. Predicate, range, order, limit, and phantom semantics are not retained.
- Aggregate scalars, implicit DML reads, and locking/current reads such as
  `SELECT ... FOR UPDATE` are omitted completely. A true read-own-write result
  may be retained with `source=local`.
- The result describes the execution with marker instrumentation enabled. The
  marker adds one per-session physical write to every workload transaction.

## D1: All backends use the session marker

Use one backend-independent join identity:

```text
(run_id, session_id, txn_id)
```

Each session owns one row in `session_txn`. Each physical attempt increments a
session-local dense attempt counter starting at one; that counter is `txn_id`.
The attempt writes it as the last statement before `commit()` and retains the
same logical `session_seq` across retries. Consequently, the highest allocated
`txn_id` equals `physical_attempts_started`, while committed marker IDs may have
gaps caused by aborted attempts.

The marker requirements are:

- exactly one marker in every successful workload transaction;
- no marker in a definitively aborted transaction;
- marker is the final row event in the transaction;
- first committed marker for a session has a null before-image;
- every later marker's `before.txn_id` equals the previous committed marker's
  `after.txn_id` for that session;
- `txn_id` changes on every physical attempt, so a committed retry cannot become
  a no-op marker update;
- every published record has `txn_id >= session_seq`, and the per-session
  retry surplus `txn_id - session_seq` never decreases;
- marker/fence writes never enter the application write set.

MariaDB uses `INSERT ... ON DUPLICATE KEY UPDATE`; YugabyteDB uses
`INSERT ... ON CONFLICT DO UPDATE`; Oracle uses `MERGE`. Connector/server
transaction identifiers are ignored for correlation.

Rationale: the same client/monitor contract works on every backend, every
successful transaction is observable in CDC order, and the marker before-image
provides an independent per-session loss check.

## D2: Every successful transaction is published

BenchBase publishes one `ClientTxn` after every acknowledged successful commit,
including marker-only physical transactions whose normalized application write
set is empty. There is no ignored-success counter.

`SessionDone` uses unambiguous logical-transaction and physical-attempt counters:

```text
final_session_seq
logical_transactions_started
successful_transactions
terminal_aborted_transactions
physical_attempts_started
definitively_aborted_attempts
retry_attempts
published_transactions
ambiguous_attempts
rolling_correlation_digest
```

A clean session satisfies:

```text
final_session_seq == logical_transactions_started

logical_transactions_started
    == successful_transactions + terminal_aborted_transactions

successful_transactions
    == published_transactions

physical_attempts_started
    == successful_transactions
     + definitively_aborted_attempts
     + ambiguous_attempts

retry_attempts
    == physical_attempts_started - logical_transactions_started

ambiguous_attempts == 0
```

Published `session_seq` values may have gaps only for terminally aborted logical
transactions. The rolling correlation digest covers accepted marker identities
in increasing `session_seq`; its number of steps equals
`published_transactions`.

## D3: The coordinator is external

Place run orchestration in `si-monitoring/scripts/e2e.sh` (with small reusable
helpers), not directly in `DBWorkload`.

The coordinator owns:

- component startup/readiness;
- `RunManifest` publication;
- `RUN_START`, `LOAD_FINISHED`, and `END_FENCE` database transactions;
- the post-load `BootstrapManifest` scan;
- waiting for monitor acknowledgements and the final result.

BenchBase owns worker/attempt capture and `SessionDone`. It exposes configuration
and exits nonzero on a tracing failure.

Rationale: database-specific lifecycle control stays outside the general
benchmark runner, while transaction evidence remains at the only layer that can
observe it correctly.

## D4: Explicit results use the point-event projection

For every adapter-classified ordinary result, emit one complete canonical row
point for every returned row. An exact-primary-key miss emits one `Absent`
point. A zero-row predicate/range result emits no synthetic miss. Predicates,
range completeness, ordering, limits, and phantoms are not represented.

The selected adapter classifies every result-producing statement as `snapshot`,
`local`, or `omit`. Aggregate scalars have no synthetic-key representation.
Omitted results do not enter `ClientTxn.reads` or its digest.

YCSB may retain its adapter-specific scan-weight admission rule. That rule is
not a wire-protocol restriction and is not applied to other adapters.

## D5: JSON transport with canonical binary payloads

- Redis black-box records use JSON.
- Canonical keys/values are binary and encoded as standard padded Base64 in JSON.
- The C++ monitor uses `hiredis` and `nlohmann_json`.
- BenchBase uses a pinned Java Redis client dependency.
- SHA-256 comes from standard Java APIs and OpenSSL on C++.

Dependency revisions are pinned by the branch's build files. There is no
separate protocol-version field.

Rationale: JSON keeps the prototype observable and debuggable; canonical binary
payloads prevent JSON/JDBC/Debezium representation differences from defining
checker equality.

## D6: Best-effort failure closure

After a BenchBase failure, the coordinator writes `END_FENCE` only after it can
prove every workload process has stopped and all database connections have been
closed. It then waits for the monitor to return `INVALID` with reconciled
diagnostics.

If quiescence cannot be proven, do not write a misleading end fence. Stop the
run and report timeout-invalid.

Rationale: an end fence is a database-order closure proof. It must never race an
unknown client that can still commit.

## D7: Branch-controlled compatibility

Code/API identifiers are unsuffixed (`ClientTxn`, `RunManifest`, and so on), and
records contain no application protocol-version field. The coordinator must
launch compatible client and monitor builds from the same development branch.

JSON Schema's `$schema` keyword identifies the JSON Schema standard and is not
an application protocol version.

## D8: Adapter-owned semantic membership

`RunManifest.benchmark` and `ClientTxn.txn_type` are bounded strings matching
`[A-Za-z][A-Za-z0-9_]{0,63}`. JSON Schema validates only that syntax. The
adapter selected by the run manifest validates the transaction name and exact
application-table roster.

The shared contracts are under `protocol/adapters/`. A contract owns its
transaction names, table/column metadata, statement-result classification, and
loader acceptance rules. Generic lifecycle, marker, CDC framing, joining,
reconciliation, and checker records do not enumerate benchmark names.

Runtime-capability flags are fail-closed implementation facts, not wire fields.
They prevent a contract from admitting a run before its client trace, CDC
normalizer, and bootstrap scanner implementations are all available.

## D9: TPC-C adapter contract

The stable benchmark name is `tpcc`. Its exact wire transaction names are
`NewOrder`, `Payment`, `OrderStatus`, `Delivery`, and `StockLevel`.

The application roster is `warehouse`, `item`, `stock`, `district`, `customer`,
`history`, `oorder`, `new_order`, and `order_line` in schema `benchbase`.
Columns retain the MariaDB `ddl-mysql.sql` order, type, and nullability. The
black-box-compatible schema inserts
`h_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY` as the first `history` column;
the standard eight history columns are its canonical value.

`protocol/adapters/tpcc.json` is the machine-checkable source for the complete
column/key/value roster, all 19 result-producing BenchBase statement IDs and
their snapshot/local/omit classification, and loader counts/relationships. The
TPC-C adapter accepts only declared repeatable-read runs. Its bootstrap scanner
and monitor-side CDC normalization capabilities are enabled with the executable
typed codec; client tracing remains disabled until that integration lands.

No post-run TPC-C application-invariant suite is part of this protocol or its
result claim. Bootstrap loader cardinality and relationship validation remains
required and is distinct from a post-workload invariant check.

## D10: TPC-C executable typed vectors

`protocol/golden/tpcc_rows.json` freezes typed source rows and exact key/value
bytes for every table, signed key boundaries, composite keys, declared-scale
decimals, finite raw database `FLOAT` bits, UTC-second timestamps, `CHAR`
padding, strict UTF-8, SQL `NULL`, the empty `new_order` value, and the generated
history key. Its malformed vectors fail closed in the Python, Java, and C++
implementations. Real MariaDB/JDBC/Debezium fixtures must match these bytes
before the corresponding TPC-C runtime capabilities can become true.

The pinned real fixture is `protocol/fixtures/mariadb_tpcc_typed_rows.json`,
captured from MariaDB 12.3 and Debezium 3.6.1 under the shared session contract.
Its checksum, exact 11-row roster, source versions, raw row images, and expected
bytes are verifier-enforced.

## D11: MariaDB session and schema contract

Backend/session requirements remain independent of benchmark adapters and live
in `protocol/backends/mariadb.json`. The MariaDB server, `benchbase` database,
and every selected-adapter application table use `utf8mb4` with
`utf8mb4_bin`. Connections use
`SET NAMES utf8mb4 COLLATE utf8mb4_bin`, UTC (`+00:00`), repeatable read with
snapshot isolation enabled, and exactly this ordered SQL-mode string:

```text
STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,PAD_CHAR_TO_FULL_LENGTH
```

The client verifies `character_set_client`, `character_set_connection`,
`character_set_results`, `collation_connection`, time zone, SQL mode, isolation,
and snapshot isolation on every initial or replacement connection. Exact SQL
mode membership enforces `PAD_CHAR_TO_FULL_LENGTH`; stored-column padding is
proved later by a real MariaDB/JDBC codec fixture rather than an expression
probe. Transaction/snapshot failures use `ISOLATION_MISMATCH`; all other
connection/session failures use `SESSION_CONTRACT_MISMATCH`.

MariaDB initialization mounts the common marker/fence schema and exactly one
adapter schema selected by the validated benchmark name. Before CDC starts, the
coordinator compares server/session metadata, the exact table roster, column
order/types/nullability, primary keys, engine, charset, collation, and zero row
counts against the backend and adapter contracts. TPC-C schema validation is
available independently, while full TPC-C run admission remains disabled until
all adapter runtime capabilities are present.

## D12: Loader declarations and independent validation

Before codec-dependent bootstrap scanning, the coordinator validates one exact
loader-declared count for every selected-adapter table. The loader publishes its
declaration atomically with create/no-overwrite semantics only after every loader
thread returns successfully; orchestrator-synthesized counts are not accepted as
loader evidence. Constant and
scale-multiplied declarations must match the adapter contract before loading.
After loader quiescence, independent SQL counts must match every declaration.
Adapter-owned derived queries then enforce `sum_column` and relationship rules.

For TPC-C, `W` supplies the `warehouses` scale parameter. The independent SQL
gate enforces all fixed/multiplied counts, requires `order_line` row count to
equal `SUM(oorder.o_ol_cnt)`, and rejects every order whose `o_ol_cnt` is outside
5 through 15 or whose `ol_number` values are not exactly contiguous from 1
through `o_ol_cnt`.

Loader batch size is a positive command-line/configuration value with default
128. It is passed into generated BenchBase configuration and recorded in loader
validation/performance artifacts, but it is not an acceptance constant.
Canonical scans use the same shared row contract and globally byte-sort encoded
keys before computing the bootstrap digest.

## D13: Adapter readiness is boundary-specific

Runtime capability flags are interpreted by boundary rather than collapsed into
one ambiguous ready bit. `monitor_ready` requires both CDC normalization and
bootstrap scanning. `end_to_end_ready` additionally requires client tracing.
Consequently, a monitor path may become ready from captured normalization
fixtures while the live workload remains fail-closed until its client trace
adapter is independently enabled. TPC-C now satisfies both boundaries.

Before `LOAD_FINISHED` can be acknowledged, the monitor's final normalized
bootstrap map is independently counted, globally key-sorted, and hashed with the
same bootstrap framing as the coordinator scan. Its exact table roster, counts,
and digest must equal `BootstrapManifest`; a count or byte mismatch is
`BOOTSTRAP_STATE_MISMATCH`.

The monitor-ready boundary is pinned by
`protocol/fixtures/mariadb_tpcc_transactions.json`. Its raw startup schema and
BEGIN/row/END entries retain workload markers; independent reference and C++
normalizers must agree on control/bootstrap/workload classification, marker
filtering, correlation, repeated-write collapse, primary-key splits, touched
net-no-change keys, deletes, and the final bootstrap digest.

## D14: TPC-C trace SQL variants and client evidence

The ordinary BenchBase statement IDs and SQL remain unchanged. Each of the 15
captured TPC-C result sets owns a stable opt-in `trace_statement_id` whose SQL
returns the complete canonical row; the four locking/aggregate omissions have
no trace variant and remain `trace_projection=omitted`. The adapter contract
must enumerate all 19 original statements and this exact 15/4 coverage split.

Committed client fixtures must be reachable executions and keep the wire
`ClientTxn` shape unchanged. In particular, no standard committed TPC-C
transaction can contain an exact-key Absent read: those misses abort and their
attempt-local buffers are discarded, while an empty Delivery predicate emits no
synthetic event. Absent encoding/digest coverage therefore uses an explicitly
discarded, never-published attempt vector rather than inventing committed
evidence. `client_trace` becomes true only after production instrumentation and
all statement paths pass. TPC-C passed that gate, so its client, monitor, and
end-to-end readiness flags are now true.

The shared evidence inventory is
`protocol/fixtures/mariadb_tpcc_client_txns.json`. Its five semantic clients map
Payment, NewOrder, Delivery, OrderStatus, and StockLevel to real captured marker
transactions 1, 2, 3, 4, and 8 respectively. Separate clients 5 through 7 are
explicitly labeled synthetic diagnostics and exist only to pair with the
captured primary-key-change, insert/delete, and net-no-change CDC shapes. The
single `SessionDone` counts all eight accepted clients; discarded-attempt reads
are excluded from both publication and reconciliation.
