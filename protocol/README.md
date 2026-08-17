# Black-box monitoring protocol

This directory is the machine-checkable contract shared by BenchBase and the
C++ black-box monitor.

The contract is branch-local:

- record/type names do not carry `V0`, `V1`, or similar suffixes;
- Redis records do not carry an application protocol-version field;
- BenchBase, the monitor, schemas, and golden vectors must come from compatible
  branches;
- an incompatible protocol change is developed on a different branch and
  updates all artifacts together.

The design label “V0” may still be used in documentation to describe the first
prototype's feature scope.

## Contract artifacts

| Path | Purpose |
|---|---|
| `decisions.md` | Frozen design choices and their rationale. |
| `canonical_encoding.md` | Exact key, value, and digest byte encodings. |
| `result_error_codes.md` | Live/final outcomes and stable error identifiers. |
| `schema/` | JSON Schemas for Redis records. |
| `examples/` | Valid example records used by schema checks. |
| `golden/` | Language-neutral codec and digest test vectors. |
| `tools/verify_protocol.py` | Schema, cross-record, and golden-vector verifier. |

## Redis streams

```text
cdc                              Debezium-owned raw stream
blackbox:client:<run_id>         BenchBase/coordinator records
blackbox:status:<run_id>         monitor status and final result
```

Every black-box stream entry has exactly one field named `payload`. Its value is
one complete UTF-8 JSON object. A record is never split across Redis entries.

The `cdc` stream retains Debezium's existing key/envelope field layout and is
validated by the CDC assembler rather than these client-record schemas.

## Validation

Run validation inside the canonical development container:

```bash
docker exec db-monitoring-dev bash -lc '
  cd /home/yifan/code/db-monitoring
  python3 si-monitoring/protocol/tools/verify_protocol.py
'
```

Milestone 0 is complete only when the schemas, examples, golden vectors, and
this verifier agree, and Java/C++ codec tests consume the same vectors.
