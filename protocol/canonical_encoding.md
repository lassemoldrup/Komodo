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

## Canonical YCSB key

```text
YcsbKey =
    LP(UTF8(canonical_schema_name))
 || LP(UTF8(canonical_table_name))
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

## Rolling session correlation digest

Parse a canonical MariaDB GTID into `(domain_id, server_id, sequence)`. The JSON
spelling is `domain-server-sequence`, with no leading zeroes except the number
zero. Domain/server fit `U32`; sequence is a nonzero `U64`.

```text
MARIADB_GTID = 01

CORRELATION_SEED_DOMAIN =
    ASCII("db-monitoring/session-correlation-seed") || 00

CORRELATION_STEP_DOMAIN =
    ASCII("db-monitoring/session-correlation-step") || 00

D0 = SHA256(
    CORRELATION_SEED_DOMAIN
 || UUID_BYTES(run_id)
)

CorrelationEntry =
    U64_BE(session_seq)
 || U8(MARIADB_GTID)
 || LP(UTF8(backend_source_id))
 || U32_BE(gtid.domain_id)
 || U32_BE(gtid.server_id)
 || U64_BE(gtid.sequence)

Di = SHA256(
    CORRELATION_STEP_DOMAIN
 || D(i - 1)
 || CorrelationEntry_i
)

rolling_correlation_digest = lowercase_hex(Dn)
```

Update the client chain only after the corresponding `ClientTxn` `XADD` is
acknowledged. The monitor recomputes it from accepted records in increasing
`session_seq`. Gaps are legal. Chain steps equal
`published_write_transactions`.

Golden chain:

```text
run_id            = 00112233-4455-6677-8899-aabbccddeeff
backend_source_id = mariadb-main

records:
  session_seq 1, GTID 0-1-17
  session_seq 3, GTID 0-1-19

D0 = 131c321cfe8be47e97969aeaf5e848ec807a081218905a49e26d736a885a6ea6

entry 1 hex =
0000000000000001010000000c6d6172696164622d6d61696e00000000000000010000000000000011

D1 = 037c67d980a8ff16adf29649df4ee427d8f6740d17d05e99cca6122b17d8984b

entry 2 hex =
0000000000000003010000000c6d6172696164622d6d61696e00000000000000010000000000000013

D2 / rolling_correlation_digest =
f9c4629a6758d0ee002aa29c46a47d952eebab35a2dd9397a4f44d1cc1961522
```
