# Milestone 0 protocol decisions

Status: proposed freeze for review on the current branch.

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
- `PASS` means the complete supported point-transaction history satisfies SOWW;
  it is not a predicate/phantom or arbitrary-SQL SI claim.
- V0 accepts non-locking point reads only. Scans, predicates, phantom claims,
  `SELECT ... FOR UPDATE`, and unclassified read-own-write behavior are rejected.
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
