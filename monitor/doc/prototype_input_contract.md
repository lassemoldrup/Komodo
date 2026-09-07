# Prototype input contract

The paper prototype checks consistency of an ordered, uniquely identified
transaction history. It is not a general-purpose deduplication or recovery
service. The default policy is recorded as `ordered_unique_input_v1` in monitor
metrics and registered performance results.

## Preconditions

- Logical transaction and attempt UUIDs are unique within a run, and upstream
  retry handling emits at most one committed result per logical transaction.
- Client records are published in increasing logical session sequence order.
  Marker transaction IDs include attempts, so gaps for aborted attempts are
  allowed; retry surplus must not regress.
- CDC transaction frames are delivered in database commit order. Within a
  session, committed marker transaction IDs increase strictly.
- A run does not resume with at-least-once redelivery of old committed evidence.
  A fresh offline replay of a complete, unique captured log is still supported.
- The declared session roster and existing run/fence/schema contracts apply.

UUID uniqueness is a precondition, not a global validation pass. Reusing an
opaque UUID under a fresh session sequence is no longer diagnosed by retaining
all earlier UUIDs. A PASS verdict is conditional on these input assumptions.

## State retained

The assembler retains active BEGIN/row/END frames and constant-size lifecycle,
fence and stream-order state. Completed structural IDs and historical marker
strings are not retained. Marker shape is checked here; cross-transaction
per-session marker order is checked by the joiner.

The joiner retains unmatched client/CDC evidence indexed by correlation, plus
the latest client and CDC progress for each declared session. Pending duplicate
correlations, client sequence regression, retry-surplus regression, and CDC
marker sequence regression remain errors. Matching consumes and removes pending
evidence. It does not retain completed logical UUIDs, attempt UUIDs, correlations
or copies of synthetic checker IDs.

The checker adapter maps UUIDs to engine metadata without retaining a global
seen-UUID map. A stateless tag check keeps external UUIDs out of the reserved
synthetic namespaces. Dense commit sequence, SO/WW state and consistency checking,
history/value GC, final reconciliation and bootstrap validation are unchanged.

No TTL, lossy filter, disk-backed index or new acknowledgement protocol is used.
This bounds monitor identity bookkeeping by sessions and pending evidence, not
by the number of completed workload transactions. It does not bound live key/
value state or history pinned by a slow session.

## Metrics and compatibility

`joiner.retained_identity_*` now accounts for the small per-session progress
maps. Pending identity storage is already included in pending-evidence charges.
Assembler retained-identity and checker mapped-identity diagnostic counters
remain present and zero. Their unused capacity configuration fields and CLI
options have been removed; no historical table is allocated.
The joiner's identity limits still bound its per-session metadata, and existing
active/pending/state limits and the assembler's total transaction count limit
remain in force.

Earlier metrics lacking `identity_policy` describe the previous
`strict_history_audit_v1` behavior. Use separate result directories when comparing
the policies, and state the input assumptions in paper results.

## Internal normalization boundary

Live and offline replay both decode raw CDC through the assembler and registered
row codecs. The joiner therefore accepts trusted, already-normalized writes and
does not decode each canonical key/value again. Cheap write shape/order checks
are performed while the checker adapter builds engine events, without a separate
validation traversal. Raw JSON, CDC framing, codecs and engine checks remain.

Oracle redo-ring sizing and LogMiner batch/sleep settings are recorded metadata,
not fixed-value semantic admission conditions. Isolation, required logging and
encoding/capture contracts remain enforced. Resource utilization percentages are
reported but are not admission thresholds; actual runtime capacity limits remain.
