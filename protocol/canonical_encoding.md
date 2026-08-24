# Canonical byte and digest encoding

This file freezes the language-neutral byte contract shared by BenchBase and the
C++ monitor. Hashes never depend on JSON serialization, object field order,
JDBC `toString()`, or raw Debezium JSON.

## Common primitives

```text
U8(n)       one byte
U32_BE(n)   four-byte unsigned big-endian integer
U64_BE(n)   eight-byte unsigned big-endian integer
I32_BE(n)   four-byte signed two's-complement big-endian integer
I64_BE(n)   eight-byte signed two's-complement big-endian integer
LP(bytes)   U32_BE(byte_length) || bytes
```

- Reject an `LP` payload longer than `2^32 - 1` bytes.
- UUID bytes are the 16 bytes in RFC textual order. For
  `00112233-4455-6677-8899-aabbccddeeff`, the bytes are
  `00112233445566778899aabbccddeeff`; do not use GUID mixed-endian order.
- Text is strict UTF-8. Reject malformed UTF-8 and Java unpaired surrogates.
- Do not perform Unicode normalization on application values.
- Canonical schema/table identifiers are lowercase ASCII matching
  `[a-z][a-z0-9_]{0,63}`.
- Base64 uses the RFC 4648 standard alphabet with required padding and no
  whitespace. Validation decodes, re-encodes, and requires byte-for-byte equality.
- SHA-256 JSON fields use exactly 64 lowercase hexadecimal characters.
- Domain strings below include their terminating `00` byte. They are fixed
  codec constants, not transmitted protocol-version fields.
- JSON counters are limited to `0..9007199254740991` for exact interoperability.

## Canonical row prefix

Every adapter row key begins with:

```text
RowPrefix = LP(UTF8(canonical_schema_name))
         || LP(UTF8(canonical_table_name))
```

The selected adapter appends its declared primary-key components in primary-key
order. A present row value contains every non-key column in the table's declared
schema order and never repeats a key column.

## Canonical YCSB key

```text
YcsbKey =
    RowPrefix
 || I32_BE(ycsb_key)
```

`ycsb_key` is in `[-2147483648, 2147483647]`.

For schema `benchbase` and table `usertable`:

| Key | Hex | Base64 | SHA-256 of key bytes |
|---|---|---|---|
| `INT32_MIN` | `0000000962656e63686261736500000009757365727461626c6580000000` | `AAAACWJlbmNoYmFzZQAAAAl1c2VydGFibGWAAAAA` | `5f5dc3f005a569e4c88e073fb663dea82c8bef67e12ac875bb1efcd2ad947049` |
| `-1` | `0000000962656e63686261736500000009757365727461626c65ffffffff` | `AAAACWJlbmNoYmFzZQAAAAl1c2VydGFibGX/////` | `2b6461927129ed264bdce703c9a48d363aa01dc7bd5858e7af115da2e4440853` |
| `42` | `0000000962656e63686261736500000009757365727461626c650000002a` | `AAAACWJlbmNoYmFzZQAAAAl1c2VydGFibGUAAAAq` | `57e4bd034b2329163f26a3241480f65b1d65dd4fdd08d79caacafb94e48f0453` |
| `INT32_MAX` | `0000000962656e63686261736500000009757365727461626c657fffffff` | `AAAACWJlbmNoYmFzZQAAAAl1c2VydGFibGV/////` | `621deb60b507dccd917145eb6a78ebb7c8d12a4acd88eb7aa602c944f4dab6af` |

`I32_BE` is ordinary two's-complement encoding, so unsigned bytewise order is
not signed numeric order. Bootstrap sorting uses the bytewise rule below.

## Canonical YCSB value

A present row contains exactly ten fields in this order:
`FIELD1, ..., FIELD10`.

```text
NullField    = 00
PresentField = 01 || U32_BE(len(utf8_value)) || utf8_value

YcsbValue = Encode(FIELD1) || ... || Encode(FIELD10)
```

There is no field count or field name in the bytes. Decoding requires exactly
ten fields and complete input exhaustion.

These states are distinct:

- SQL `NULL`: `00`;
- present empty string: `0100000000`;
- absent row: surrounding `value_state = absent`, with no value bytes.

Representative mixed ten-field value:

```text
FIELD1  = SQL NULL
FIELD2  = ""
FIELD3  = "A"
FIELD4  = "é"
FIELD5  = "中"
FIELD6  = "0"
FIELD7  = "hello"
FIELD8  = "🙂"
FIELD9  = "a\u0000b"
FIELD10 = "last"

length = 69

hex =
0001000000000100000001410100000002c3a90100000003e4b8ad010000000130010000000568656c6c6f0100000004f09f9982010000000361006201000000046c617374

base64 =
AAEAAAAAAQAAAAFBAQAAAALDqQEAAAAD5LitAQAAAAEwAQAAAAVoZWxsbwEAAAAE8J+ZggEAAAADYQBiAQAAAARsYXN0

sha256 =
4d953fbb87c4ac6d920410774a9c8669dac28678b9a940738e82fa2dcb43c1c0
```

All ten fields null:

```text
hex    = 00000000000000000000
base64 = AAAAAAAAAAAAAA==
sha256 = 01d448afd928065458cf670b60f5a594d735af0172c8d67f22a81680132681ca
```

## TPC-C row contract

`protocol/adapters/tpcc.json` freezes the exact nine-table schema, primary-key
order, and non-key value order shared by JDBC capture, CDC normalization, and
bootstrap scanning.

```text
TpccKey = RowPrefix || Encode(key_column_1) || ... || Encode(key_column_n)

ordinary key column = I32_BE(value)
history h_id        = I64_BE(value)

TpccValue = Encode(value_column_1) || ... || Encode(value_column_n)
```

`new_order` has no non-key columns, so its present canonical value is exactly
zero bytes. That present empty value remains distinct from an absent read or a
delete, which carries no value payload.

The adapter contract records each column's MariaDB type, nullability, and
canonical type category. Non-key columns encode in exact schema order:

```text
INT                  = I32_BE(value)
BIGINT               = I64_BE(value)
DECIMAL(p, s)        = I64_BE(exact declared-scale unscaled value)
FLOAT                = U32_BE(raw finite IEEE-754 binary32 bits)
TIMESTAMP            = I64_BE(signed UTC Unix epoch seconds)
VARCHAR(n)           = LP(strict UTF-8), at most n Unicode characters
CHAR(n)              = LP(strict UTF-8 after right-padding with ASCII spaces
                          to exactly n Unicode characters)
nullable SQL NULL    = 00
nullable present     = 01 || Encode(non-null value)
non-nullable present = Encode(non-null value)
```

TPC-C `TIMESTAMP` columns have DDL precision zero; fractional seconds are
invalid. Decimal input must have exactly the declared scale and fit both the SQL
precision and signed I64. Float infinities and NaNs are invalid. Text is counted
in Unicode characters before strict UTF-8 encoding; it is never normalized.

`protocol/golden/tpcc_rows.json` contains executable typed source rows, expected
key/value hex, and malformed vectors shared by Java, C++, and the bootstrap
reference implementation.

`protocol/fixtures/mariadb_tpcc_typed_rows.json` pins real MariaDB 12.3 and
Debezium 3.6.1 row images for all nine tables, including numeric `FLOAT`,
trimmed `CHAR`, nullable present/NULL, timestamps, decimals, UTF-8, and the empty
`new_order` value. Its encoded bytes must match the executable goldens' codec.

`protocol/fixtures/mariadb_tpcc_transactions.json` pins the raw startup-schema
prefix and 12 BEGIN/row/END transactions covering bootstrap, fences,
append-only markers including two marker-only workload transactions,
Payment/NewOrder/Delivery write shapes,
primary-key changes,
insert-then-delete, and net-no-change writes. The protocol verifier independently
parses and normalizes those raw envelopes before comparing their expected
classification, correlation, fences, final writes, and bootstrap manifest.

`protocol/fixtures/mariadb_tpcc_client_txns.json` pins eight `ClientTxn` records
against the eight workload markers in that raw fixture. Five semantic records
cover the exact transaction roster, snapshot and local sources, multiple rows
from one statement, nullable values, and a present row whose canonical value is
empty. Three explicitly synthetic diagnostic clients supply the remaining CDC
halves for reconciliation and carry no reads. The separate discarded-attempt
vector covers Absent framing without publishing or correlating it, and the
Delivery empty-result assertion requires zero emitted events. The verifier
independently recomputes every row encoding, read digest, marker correlation,
and final session rolling digest.

## Read digest

Binary tags:

```text
PRESENT  = 01
ABSENT   = 02
SNAPSHOT = 01
LOCAL    = 02
```

`event_seq` is zero-based and dense in the JSON `reads` array:
`0, 1, ..., read_count - 1`.

```text
READ_DOMAIN = ASCII("db-monitoring/read-digest") || 00

PresentRead =
    U32_BE(event_seq)
 || LP(key)
 || U8(PRESENT)
 || LP(value)
 || U8(source)

AbsentRead =
    U32_BE(event_seq)
 || LP(key)
 || U8(ABSENT)
 || U8(source)

ReadDigestPreimage =
    READ_DOMAIN
 || U32_BE(read_count)
 || Encode(reads[0])
 || ...
 || Encode(reads[read_count - 1])

read_digest = lowercase_hex(SHA256(ReadDigestPreimage))
```

For an absent read, `value_bytes_base64` is absent from JSON. For a present read
it exists even when the decoded value is empty.

Golden two-read digest:

```text
read 0: key 42, mixed present value, snapshot
read 1: key -1, absent, local

preimage length = 183

preimage hex =
64622d6d6f6e69746f72696e672f726561642d6469676573740000000002000000000000001e0000000962656e63686261736500000009757365727461626c650000002a01000000450001000000000100000001410100000002c3a90100000003e4b8ad010000000130010000000568656c6c6f0100000004f09f9982010000000361006201000000046c61737401000000010000001e0000000962656e63686261736500000009757365727461626c65ffffffff0202

preimage base64 =
ZGItbW9uaXRvcmluZy9yZWFkLWRpZ2VzdAAAAAACAAAAAAAAAB4AAAAJYmVuY2hiYXNlAAAACXVzZXJ0YWJsZQAAACoBAAAARQABAAAAAAEAAAABQQEAAAACw6kBAAAAA+S4rQEAAAABMAEAAAAFaGVsbG8BAAAABPCfmYIBAAAAA2EAYgEAAAAEbGFzdAEAAAABAAAAHgAAAAliZW5jaGJhc2UAAAAJdXNlcnRhYmxl/////wIC

read_digest =
813a7ce7676b4952e778f4abf96fceae0364f63b5385d31f872aa711d991c501
```

Empty reads:

```text
preimage hex = 64622d6d6f6e69746f72696e672f726561642d6469676573740000000000
read_digest  = 3f251669e36904bc1a3bc472991c9ff89f7f23f6b98bad9dd33f0e66884a06ce
```

## Bootstrap state digest

The state is the final map of present rows only. Deletes remove an entry.
Duplicate canonical keys are invalid.

Sort records by unsigned lexicographic comparison of the complete canonical key
bytes; if one key is a prefix of another, the shorter key sorts first. Do not use
locale, database collation, signed integer order, or Base64 text order.

```text
BOOTSTRAP_DOMAIN =
    ASCII("db-monitoring/bootstrap-state-digest") || 00

BootstrapDigestPreimage =
    BOOTSTRAP_DOMAIN
 || U64_BE(total_present_row_count)
 || LP(sorted_row[0].key)
 || LP(sorted_row[0].value)
 || ...
 || LP(sorted_row[n - 1].key)
 || LP(sorted_row[n - 1].value)

canonical_state_digest =
    lowercase_hex(SHA256(BootstrapDigestPreimage))
```

The digest excludes `run_id`; the independent database scan and CDC
reconstruction hash the same application state. `table_row_counts` is validated
separately. Its sum equals `total_present_row_count`, and it contains exactly one
entry for every allowlisted application table, including empty tables.

Golden state:

- key `42` maps to the mixed value;
- key `-1` maps to the ten-null value;
- bytewise order is `42`, then `-1`.

```text
preimage length = 200

preimage hex =
64622d6d6f6e69746f72696e672f626f6f7473747261702d73746174652d6469676573740000000000000000020000001e0000000962656e63686261736500000009757365727461626c650000002a000000450001000000000100000001410100000002c3a90100000003e4b8ad010000000130010000000568656c6c6f0100000004f09f9982010000000361006201000000046c6173740000001e0000000962656e63686261736500000009757365727461626c65ffffffff0000000a00000000000000000000

preimage base64 =
ZGItbW9uaXRvcmluZy9ib290c3RyYXAtc3RhdGUtZGlnZXN0AAAAAAAAAAACAAAAHgAAAAliZW5jaGJhc2UAAAAJdXNlcnRhYmxlAAAAKgAAAEUAAQAAAAABAAAAAUEBAAAAAsOpAQAAAAPkuK0BAAAAATABAAAABWhlbGxvAQAAAATwn5mCAQAAAANhAGIBAAAABGxhc3QAAAAeAAAACWJlbmNoYmFzZQAAAAl1c2VydGFibGX/////AAAACgAAAAAAAAAAAAA=

canonical_state_digest =
71749720e062cf361dbad1e139c88d7d39b10debeddf18562d35cd80ae53afa8
```

Empty state:

```text
preimage hex =
64622d6d6f6e69746f72696e672f626f6f7473747261702d73746174652d646967657374000000000000000000

canonical_state_digest =
944dd6136dbad7047e31af3b6419eb6a632d4dea64bec8159e93307163810bc2
```

## Rolling session-marker digest

The join key is `(run_id, session_id, txn_id)`. `txn_id` is a fresh positive
monotonic physical-attempt counter owned by one session. It is stored in the
session's marker row as a signed database `BIGINT` and is restricted to the
JSON-safe positive range by the protocol schema.

```text
MARKER_SEED_DOMAIN =
    ASCII("db-monitoring/session-marker-seed") || 00

MARKER_STEP_DOMAIN =
    ASCII("db-monitoring/session-marker-step") || 00

D0 = SHA256(
    MARKER_SEED_DOMAIN
 || UUID_BYTES(run_id)
 || LP(UTF8(session_id))
)

MarkerEntry =
    U64_BE(session_seq)
 || U64_BE(txn_id)

Di = SHA256(
    MARKER_STEP_DOMAIN
 || D(i - 1)
 || MarkerEntry_i
)

rolling_correlation_digest = lowercase_hex(Dn)
```

Update the client chain only after the corresponding `ClientTxn` `XADD` is
acknowledged. The monitor recomputes it from accepted records in increasing
`session_seq`. Gaps are legal only for terminally aborted logical transactions.
Chain steps equal `published_transactions`.

Golden chain:

```text
run_id     = 00112233-4455-6677-8899-aabbccddeeff
session_id = ycsb/worker/1

records:
  session_seq 1, txn_id 1
  session_seq 3, txn_id 3

seed preimage hex =
64622d6d6f6e69746f72696e672f73657373696f6e2d6d61726b65722d736565640000112233445566778899aabbccddeeff0000000d796373622f776f726b65722f31

D0 = 20c58f889d34c7f08ec308d2737b9bccb498868e19773cec3b2b696f43a12170

entry 1 hex =
00000000000000010000000000000001

D1 = c0e8ea544d438285925a25fcdf53dee3e673b00f8887526e09871d5fb69a4029

entry 2 hex =
00000000000000030000000000000003

D2 / rolling_correlation_digest =
69d3511b963663dd9215557af81380e3b6c779cf97505da7204f2e510a5d5dea
```
