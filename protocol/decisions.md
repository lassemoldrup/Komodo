# Milestone 0 protocol decisions

Status: proposed freeze for review on the current branch.

These decisions turn the open questions in the end-to-end design into one
implementable contract. Changing one before implementation is inexpensive;
changing one after client/monitor code lands requires updating both sides and
their golden tests in the same branch.

## Scope and result

- MariaDB is the first backend.
- The checked population contains committed transactions with at least one
  captured application CDC write.
- Successful read-only and successful no-application-write transactions are
  outside the checked population.
- `PASS` therefore means write-projected SOWW, not full SI.
- V0 accepts non-locking point reads only. Scans, predicates, phantom claims,
  `SELECT ... FOR UPDATE`, and unclassified read-own-write behavior are rejected.

## D1: MariaDB correlation uses GTID change

Use the GTID returned by `@@session.last_gtid` on the same exclusive physical
connection after an acknowledged commit. Do not add a marker write to MariaDB.

The client tracks a per-connection baseline:

- changed GTID: publish one `ClientTxn` keyed by
  `(run_id, backend_source_id, GTID)`;
- unchanged GTID: count an intentionally omitted successful no-write
  transaction;
- failed GTID lookup: stop and invalidate the run;
- exception from `commit()`: unknown outcome, stop and invalidate the run.

Procedure type and write intent are never used as proof of CDC membership.

Rationale: this preserves the measured MariaDB workload while still providing
an authoritative join key for physical writes. The no-op/update behavior must be
qualified by integration tests.

## D2: Omitted transactions use aggregate session accounting

Do not publish a per-transaction record for successful read-only/no-write
transactions in V0. Allocate `session_seq` for every logical transaction, allow
gaps among published write transactions, and reconcile aggregate omitted counts
through `SessionDone`.

`SessionDone` uses unambiguous logical-transaction and physical-attempt counters:

```text
final_session_seq
logical_transactions_started
successful_transactions
terminal_aborted_transactions
physical_attempts_started
definitively_aborted_attempts
retry_attempts
published_write_transactions
ignored_successful_no_write_transactions
ambiguous_attempts
rolling_correlation_digest
```

A clean session satisfies:

```text
final_session_seq == logical_transactions_started

logical_transactions_started
    == successful_transactions + terminal_aborted_transactions

successful_transactions
    == published_write_transactions
     + ignored_successful_no_write_transactions

physical_attempts_started
    == successful_transactions
     + definitively_aborted_attempts
     + ambiguous_attempts

retry_attempts
    == physical_attempts_started - logical_transactions_started

ambiguous_attempts == 0
```

Rationale: exact omitted positions do not affect the explicitly declared
write-transaction projection. This avoids one Redis publication for every
read-only transaction while retaining run-level accounting.

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

## D4: Scans are disabled

Reject a workload configuration that gives YCSB scans nonzero weight. Do not
silently reduce a range query to returned point reads in V0.

Rationale: a returned-point projection cannot represent an empty range, missing
interior keys, ordering, or phantoms, and would make the result easy to
overinterpret.

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
