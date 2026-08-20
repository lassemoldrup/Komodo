# Black-box monitoring protocol

This directory is the machine-checkable contract shared by BenchBase and the
C++ black-box monitor.

The contract is branch-local:

- record/type names do not carry release or internal-step suffixes;
- Redis records do not carry an application protocol-version field;
- BenchBase, the monitor, schemas, and golden vectors must come from compatible
  branches;
- an incompatible protocol change is developed on a different branch and
  updates all artifacts together.

## Contract artifacts

| Path | Purpose |
|---|---|
| `decisions.md` | Frozen design choices and their rationale. |
| `canonical_encoding.md` | Exact key, value, and digest byte encodings. |
| `result_error_codes.md` | Live/final outcomes and stable error identifiers. |
| `adapters/` | Machine-checkable benchmark names, transaction membership, table contracts, result classification, loader rules, and runtime capabilities. |
| `backends/` | Machine-checkable database, connection/session, isolation, and application-table requirements. |
| `schema/` | JSON Schemas for Redis records. |
| `examples/` | Valid example records used by schema checks. |
| `fixtures/` | Version-pinned real database/JDBC/CDC typed-row, transaction, and client-evidence fixtures. |
| `golden/` | Executable language-neutral row, codec, and digest vectors. |
| `tools/verify_protocol.py` | Schema, cross-record, and golden-vector verifier. |

## Redis streams

```text
cdc                              Debezium-owned raw stream
blackbox:client:<run_id>         BenchBase/coordinator records
blackbox:status:<run_id>         monitor status and final result
blackbox:monitor-owner:<run_id>  permanent one-monitor ownership claim
```

Every black-box stream entry has exactly one field named `payload`. Its value is
one complete UTF-8 JSON object. A record is never split across Redis entries.

Every successful supported workload transaction is correlated by
`(run_id, session_id, txn_id)`. The database marker row stores only
`(session_id, txn_id)`; the active fenced run supplies `run_id`. Connector/server
transaction identifiers are not part of the join contract.

The `cdc` stream retains Debezium's existing key/envelope field layout and is
validated by the CDC assembler rather than these client-record schemas.

Before consuming evidence or appending status, a monitor atomically requires
both its owner key and status stream to be absent, then creates the owner key.
The claim has no TTL and is never released during the run; a concurrent or
replacement monitor is `INVALID/COMPONENT_RESTARTED`. Fresh-stack teardown is
the only supported way to remove it.

## Validation

Run validation inside the canonical development container:

```bash
docker exec db-monitoring-dev bash -lc '
  cd /home/yifan/code/db-monitoring
  python3 si-monitoring/protocol/tools/verify_protocol.py
'
```

The protocol freeze is complete only when the schemas, examples, golden vectors,
and this verifier agree, and Java/C++/bootstrap tests consume the same shared
adapter contracts and vectors. A false adapter runtime-capability flag means the
contract is available for tests but the corresponding live path must fail
closed.

Readiness is boundary-specific: CDC normalization plus bootstrap scanning makes
an adapter monitor-ready, while end-to-end readiness additionally requires
client tracing.
