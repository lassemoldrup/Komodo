# Results, live status, and error codes

All codes are stable uppercase strings. They are used in logs, `RunFailed`, and
the monitor's final diagnostic. They describe evidence/protocol failures, not
database consistency violations.

## Consistency outcomes

| Code | Terminal | Meaning |
|---|---:|---|
| `PASS_SO_FAR` | no | Every transaction in the currently checked complete prefix passes. |
| `VIOLATION_OBSERVED` | no | The checker observed a violation in the current complete prefix; reconciliation is still running. |
| `PASS` | yes | Reconciliation succeeded and every successful supported point transaction passes. |
| `VIOLATION` | yes | Reconciliation succeeded and at least one captured transaction violates SOWW. |
| `INVALID` | yes | Evidence/lifecycle validation failed; no consistency verdict is available. |

If a violation is observed before a later protocol failure, the final outcome is
`INVALID`; the violation remains attached as diagnostic evidence.

## Lifecycle states

```text
STARTING
START_FENCE_SEEN
BOOTSTRAP
LOAD_FINISHED_SEEN
RUNNING
DRAINING
RECONCILED
FINAL
```

## Error codes

### Configuration and scope

| Code | Meaning |
|---|---|
| `CONFIG_INVALID` | Required configuration is missing or contradictory. |
| `BRANCH_CONTRACT_MISMATCH` | Client/monitor artifacts do not implement the same branch contract. |
| `ISOLATION_MISMATCH` | Configured, declared, or observed transaction/snapshot isolation differs from the contract. |
| `SESSION_CONTRACT_MISMATCH` | Character set, collation, time zone, SQL mode, or CHAR-padding behavior differs from the backend session contract. |
| `UNSUPPORTED_TRANSACTION` | The transaction name is not owned by the selected adapter, or its required adapter capability is unavailable. |
| `UNSUPPORTED_READ_SOURCE` | A read cannot be classified safely as snapshot or local. |
| `UNEXPECTED_TABLE` | CDC contains a table outside the application/control allowlists. |
| `DDL_OBSERVED` | DDL occurred after run start. |
| `EXTERNAL_WRITER_OBSERVED` | An identifiable non-workload writer modified modeled data. |
| `SCHEMA_ATTESTATION_MISSING` | STARTUP or FINAL schema/capture attestation is absent at its required lifecycle boundary. |
| `SCHEMA_ATTESTATION_DUPLICATE` | More than one attestation exists for one phase. |
| `SCHEMA_ATTESTATION_MALFORMED` | An attestation has an invalid outer shape, normalized table description, fact value, or digest. |
| `SCHEMA_ATTESTATION_MISMATCH` | An attestation disagrees with the selected run/backend/benchmark contract. |
| `SCHEMA_ATTESTATION_DRIFT` | Valid STARTUP and FINAL normalized schema/capture evidence differs. |
| `BACKEND_FACT_MISMATCH` | Backend facts are missing, extra, reordered, or violate their registered value rules. |

### Client evidence

| Code | Meaning |
|---|---|
| `CLIENT_RECORD_MALFORMED` | Client JSON fails schema or semantic validation. |
| `CLIENT_READ_COUNT_MISMATCH` | `read_count` differs from the reads array length. |
| `CLIENT_READ_DIGEST_MISMATCH` | Recomputed read digest differs from the record. |
| `CLIENT_DUPLICATE_CORRELATION` | More than one client record uses a marker identity. |
| `LOGICAL_TRANSACTION_DUPLICATE` | A logical transaction identity is reused. |
| `ATTEMPT_DUPLICATE` | A committed attempt identity is reused. |
| `MULTIPLE_COMMITTED_ATTEMPTS` | More than one attempt of one logical transaction committed. |
| `SESSION_UNKNOWN` | Evidence names a session absent from the run manifest. |
| `SESSION_SEQUENCE_NON_INCREASING` | A retained session sequence repeats or decreases. |
| `SESSION_ATTEMPT_PROGRESS_INVALID` | A marker attempt counter is behind logical progress or its retry surplus decreases. |
| `SESSION_DONE_MISSING` | An expected worker never publishes `SessionDone`. |
| `SESSION_DONE_MISMATCH` | Session counters/digest do not reconcile. |
| `CLIENT_RECORD_AFTER_DONE` | A session publishes transaction evidence after `SessionDone`. |
| `COMMIT_OUTCOME_AMBIGUOUS` | A commit call returned an unknown outcome. |
| `MARKER_WRITE_FAILED` | The last-statement marker insert failed; the attempt must roll back. |
| `REDIS_PUBLICATION_FAILED` | A required client/status record was not acknowledged by Redis. |

### CDC evidence

| Code | Meaning |
|---|---|
| `CDC_RECORD_MALFORMED` | A Debezium envelope cannot be decoded or validated. |
| `CDC_BEGIN_MISSING` | A row/end appears without a legal transaction begin. |
| `CDC_END_MISSING` | A transaction never receives its required end record. |
| `CDC_TRANSACTION_ID_MISMATCH` | The key and envelope copies of an opaque ID disagree within the same BEGIN or END record. Cross-record ID differences are expected diagnostics. |
| `CDC_EVENT_ORDINAL_INVALID` | Event ordinals repeat, regress, or contain a gap. |
| `CDC_EVENT_COUNT_MISMATCH` | Observed rows disagree with `END.event_count` or table counts. |
| `CDC_DUPLICATE_CONFLICT` | A stable CDC identity is replayed with different content. |
| `MARKER_MISSING` | A post-load workload transaction has no marker row event. |
| `MARKER_MULTIPLE` | A workload transaction contains more than one marker event. |
| `MARKER_MALFORMED` | The marker is not an INSERT with a null before-image, or its key/after session and transaction fields are absent, inconsistent, or out of range. |
| `MARKER_NOT_LAST` | The marker is not the final row event in its CDC transaction. |
| `MARKER_DUPLICATE` | A run/session/transaction marker identity is reused. |
| `UNMARKED_APPLICATION_TRANSACTION` | A post-load application transaction commits without exactly one marker. |
| `CDC_APPLICATION_AFTER_END` | An application commit appears after `END_FENCE`. |
| `CDC_ORDER_INVALID` | The source stream violates the backend CO contract. |

### Joining and normalization

| Code | Meaning |
|---|---|
| `CLIENT_HALF_MISSING` | A workload marker slot has no client half at closure. |
| `CDC_HALF_MISSING` | A successful client transaction has no marker-bearing CDC half at closure. |
| `CORRELATION_MISMATCH` | Joined halves disagree on run/session/transaction marker identity. |
| `CANONICAL_ENCODING_FAILED` | A key/value cannot be represented by the frozen codec. |
| `PRIMARY_KEY_IMAGE_INCOMPLETE` | A key-changing CDC event lacks a complete before/after key. |
| `COMMIT_SEQUENCE_INVALID` | Dense checker sequence assignment is inconsistent. |
| `CHECKER_INPUT_REJECTED` | `SnapshotSOWWEngine` rejects normalized input. |

### Bootstrap and lifecycle

| Code | Meaning |
|---|---|
| `RUN_MANIFEST_MISSING` | `RUN_START` cannot proceed without its manifest. |
| `RUN_MANIFEST_DUPLICATE` | More than one manifest exists for a run. |
| `RUN_ID_MISMATCH` | Evidence belongs to another run epoch. |
| `FENCE_MISSING` | A required run/load/end fence is absent. |
| `FENCE_DUPLICATE` | A phase fence is repeated. |
| `FENCE_ORDER_INVALID` | Fences occur in an illegal database order. |
| `BOOTSTRAP_MANIFEST_MISSING` | `LOAD_FINISHED` has no independent state manifest. |
| `BOOTSTRAP_STATE_MISMATCH` | Loader declarations, independent SQL validation, reconstructed state, counts, or the bootstrap manifest disagree. |
| `PHASE_ANCHOR_FAILED` | A required per-session load anchor cannot be consumed. |
| `QUIESCENCE_UNPROVEN` | The coordinator cannot prove all clients stopped before closure. |
| `COMPONENT_RESTARTED` | A forbidden database/CDC/Redis/client/monitor restart occurred. |
| `RECONCILIATION_MISMATCH` | Final cross-component equalities do not hold. |

### Resources and internal failures

| Code | Meaning |
|---|---|
| `JOIN_BUFFER_LIMIT_EXCEEDED` | Unmatched/ordered evidence exceeds its byte limit. |
| `CHECKER_STATE_LIMIT_EXCEEDED` | Retained checker state exceeds its byte limit. |
| `RESOURCE_EXHAUSTED` | Allocation or another configured resource fails. |
| `INTERNAL_ERROR` | An invariant failed without a more specific stable code. |

Errors are sticky for one run. Once one occurs, admission stops and the final
outcome cannot be `PASS` or `VIOLATION`.
