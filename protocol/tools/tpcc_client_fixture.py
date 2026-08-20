#!/usr/bin/env python3
"""Independent validator for shared TPC-C client read/correlation fixtures."""

from __future__ import annotations

import base64
import hashlib
import re
import struct
import uuid
from typing import Any

from tpcc_codec import CodecError, TpccCodec
from tpcc_transaction_fixture import TransactionFixtureError, verify_transaction_fixture


READ_DOMAIN = b"db-monitoring/read-digest\x00"
MARKER_SEED_DOMAIN = b"db-monitoring/session-marker-seed\x00"
MARKER_STEP_DOMAIN = b"db-monitoring/session-marker-step\x00"
BASE64 = re.compile(r"^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$")


class ClientFixtureError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ClientFixtureError(message)


def lp(value: bytes) -> bytes:
    require(len(value) <= 0xFFFFFFFF, "fixture value exceeds U32")
    return struct.pack(">I", len(value)) + value


def decode_base64(value: Any, label: str) -> bytes:
    require(isinstance(value, str) and BASE64.fullmatch(value) is not None, f"{label} Base64")
    try:
        decoded = base64.b64decode(value, validate=True)
    except ValueError as error:
        raise ClientFixtureError(f"{label} Base64 is invalid: {error}") from error
    require(base64.b64encode(decoded).decode("ascii") == value, f"{label} Base64 is noncanonical")
    return decoded


def read_digest(reads: list[dict[str, Any]]) -> str:
    preimage = bytearray(READ_DOMAIN + struct.pack(">I", len(reads)))
    for expected_sequence, read in enumerate(reads):
        require(read.get("event_seq") == expected_sequence, "fixture read sequence is not dense")
        key = decode_base64(read.get("key_bytes_base64"), "read key")
        preimage.extend(struct.pack(">I", expected_sequence) + lp(key))
        state = read.get("value_state")
        if state == "present":
            value = decode_base64(read.get("value_bytes_base64"), "read value")
            preimage.extend(b"\x01" + lp(value))
        elif state == "absent":
            require("value_bytes_base64" not in read, "absent fixture read carries value")
            preimage.extend(b"\x02")
        else:
            raise ClientFixtureError("fixture read has unknown value state")
        source = read.get("source")
        require(source in {"snapshot", "local"}, "fixture read has unknown source")
        preimage.extend(b"\x01" if source == "snapshot" else b"\x02")
    return hashlib.sha256(preimage).hexdigest()


def rolling_digest(run_id: str, session_id: str, clients: list[dict[str, Any]]) -> str:
    seed = (
        MARKER_SEED_DOMAIN
        + uuid.UUID(run_id).bytes
        + lp(session_id.encode("utf-8", "strict"))
    )
    digest = hashlib.sha256(seed).digest()
    for client in clients:
        entry = struct.pack(">Q", client["session_seq"]) + struct.pack(
            ">Q", client["correlation_id"]["txn_id"]
        )
        digest = hashlib.sha256(MARKER_STEP_DOMAIN + digest + entry).digest()
    return digest.hex()


def statement_contracts(adapter: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (contract["transaction_type"], contract["statement_id"]): contract
        for contract in adapter["result_sets"]
    }


def transaction_state_views(
    raw_transactions: dict[str, Any], codec: TpccCodec
) -> dict[tuple[str, str, int], tuple[dict[bytes, bytes], dict[bytes, bytes]]]:
    try:
        transactions = verify_transaction_fixture(raw_transactions, codec)
    except (TransactionFixtureError, CodecError) as error:
        raise ClientFixtureError(f"raw transaction fixture: {error}") from error
    state: dict[bytes, bytes] = {}
    views: dict[tuple[str, str, int], tuple[dict[bytes, bytes], dict[bytes, bytes]]] = {}
    for transaction in transactions:
        before = state.copy()
        for write in transaction["writes"]:
            key = bytes.fromhex(write["key_hex"])
            if write["state"] == "present":
                state[key] = bytes.fromhex(write["value_hex"])
            else:
                state.pop(key, None)
        correlation = transaction["correlation_id"]
        if correlation is not None:
            identity = (
                correlation["run_id"],
                correlation["session_id"],
                correlation["txn_id"],
            )
            require(identity not in views, "duplicate raw transaction correlation")
            views[identity] = (before, state.copy())
    return views


def verify_client_fixture(
    document: dict[str, Any],
    codec: TpccCodec,
    raw_transactions: dict[str, Any],
) -> list[dict[str, Any]]:
    run_manifest = document["run_manifest"]
    run_id = run_manifest["run_id"]
    session_ids = run_manifest["expected_session_ids"]
    require(
        run_manifest
        == {
            "kind": "run_manifest",
            "run_id": run_id,
            "benchmark": "tpcc",
            "backend": "mariadb",
            "backend_source_id": "mariadb-main",
            "declared_isolation": "REPEATABLE_READ",
            "expected_session_ids": ["tpcc/worker/1"],
            "application_table_allowlist": codec.contract["application_tables"],
            "control_table_allowlist": ["benchbase.checker_run_fence"],
            "transaction_marker_table": "benchbase.session_txn",
            "result_scope": "point_transaction_soww",
        },
        "client fixture RunManifest differs from adapter/run contract",
    )
    require(session_ids == ["tpcc/worker/1"], "client fixture session roster mismatch")
    session_id = session_ids[0]
    require(
        run_id == raw_transactions["run_id"],
        "client fixture run ID differs from raw transaction fixture",
    )
    row_catalog: dict[str, tuple[str, dict[str, Any], bytes, bytes]] = {}
    for entry in document["row_catalog"]:
        require(
            isinstance(entry, dict) and set(entry) == {"name", "table", "row"},
            "client row catalog entry has wrong fields",
        )
        name = entry["name"]
        require(isinstance(name, str) and name not in row_catalog, "duplicate client row name")
        try:
            key, value = codec.encode_row(entry["table"], entry["row"])
        except CodecError as error:
            raise ClientFixtureError(f"client row {name}: {error.code}: {error}") from error
        row_catalog[name] = (entry["table"], entry["row"], key, value)

    raw_by_name = {transaction["name"]: transaction for transaction in raw_transactions["transactions"]}
    state_views = transaction_state_views(raw_transactions, codec)
    contracts = statement_contracts(codec.contract)
    semantic_mapping = {
        "Payment": ("payment_shape", 1),
        "NewOrder": ("new_order_shape", 2),
        "Delivery": ("delivery_shape", 3),
        "OrderStatus": ("order_status_marker_only", 4),
        "StockLevel": ("stock_level_marker_only", 8),
    }
    synthetic_mapping = {
        "primary_key_change_join": ("primary_key_change", "Payment", 5),
        "insert_then_delete_join": ("insert_then_delete", "Payment", 6),
        "net_no_change_join": ("net_no_change", "NewOrder", 7),
    }
    require(
        all(entry.get("fixture_role") == "semantic" for entry in document["semantic_transactions"])
        and all(
            entry.get("fixture_role") == "synthetic_diagnostic"
            for entry in document["synthetic_join_clients"]
        ),
        "client fixture semantic/synthetic partition mismatch",
    )
    all_entries = [*document["semantic_transactions"], *document["synthetic_join_clients"]]
    clients: list[dict[str, Any]] = []
    semantic_types: set[str] = set()
    semantic_sources: set[str] = set()
    has_multirow_statement = False
    has_empty_value = False
    nullable_null_columns: set[tuple[str, str]] = set()
    has_composite_key = False
    for entry in all_entries:
        require(
            isinstance(entry, dict)
            and set(entry)
            == {"name", "fixture_role", "cdc_transaction", "client_txn", "read_provenance"},
            "client fixture transaction has wrong fields",
        )
        role = entry["fixture_role"]
        require(role in {"semantic", "synthetic_diagnostic"}, "unknown client fixture role")
        client = entry["client_txn"]
        require(
            isinstance(client, dict)
            and set(client)
            == {
                "kind",
                "run_id",
                "session_id",
                "session_seq",
                "txn_type",
                "logical_txn_id",
                "attempt_id",
                "correlation_kind",
                "correlation_id",
                "read_count",
                "reads",
                "read_digest",
            },
            "client fixture ClientTxn shape",
        )
        require(
            client["kind"] == "client_txn"
            and client["correlation_kind"] == "session_txn"
            and client["txn_type"] in codec.contract["transaction_types"],
            "client fixture ClientTxn identity",
        )
        cdc_name = entry["cdc_transaction"]
        require(cdc_name in raw_by_name, "client fixture names unknown CDC transaction")
        raw_correlation = raw_by_name[cdc_name]["expected"]["correlation_id"]
        require(raw_correlation == client["correlation_id"], "client/CDC fixture correlation mismatch")
        state_identity = (
            raw_correlation["run_id"],
            raw_correlation["session_id"],
            raw_correlation["txn_id"],
        )
        require(state_identity in state_views, "client correlation lacks raw state views")
        snapshot_state, local_state = state_views[state_identity]
        require(client["run_id"] == run_id and client["session_id"] == session_id, "client fixture IDs")
        require(client["correlation_id"]["session_id"] == session_id, "nested client session")
        require(client["read_count"] == len(client["reads"]), "client fixture read count")
        require(read_digest(client["reads"]) == client["read_digest"], "client read digest mismatch")
        provenance = entry["read_provenance"]
        require(len(provenance) == len(client["reads"]), "client read provenance count")
        statement_counts: dict[str, int] = {}
        for index, (read, source) in enumerate(zip(client["reads"], provenance, strict=True)):
            require(
                isinstance(source, dict)
                and set(source)
                == {"event_seq", "statement_id", "trace_statement_id", "table", "row_name"},
                "client read provenance shape",
            )
            require(source["event_seq"] == index == read["event_seq"], "provenance sequence mismatch")
            contract = contracts.get((client["txn_type"], source["statement_id"]))
            require(contract is not None, "client provenance names unregistered statement")
            require(
                contract["trace_statement_id"] == source["trace_statement_id"]
                and contract["table"] == source["table"]
                and contract["classification"] == read["source"],
                "client provenance differs from statement contract",
            )
            row_name = source["row_name"]
            require(row_name in row_catalog, "client provenance names unknown row")
            table, row, key, value = row_catalog[row_name]
            require(table == source["table"], "client provenance row table mismatch")
            require(read["value_state"] == "present", "committed TPC-C fixture read is absent")
            actual_key = decode_base64(read["key_bytes_base64"], "client key")
            actual_value = decode_base64(read["value_bytes_base64"], "client value")
            require(actual_key == key, "client key bytes")
            require(actual_value == value, "client value bytes")
            source_state = snapshot_state if read["source"] == "snapshot" else local_state
            require(
                source_state.get(actual_key) == actual_value,
                "client read differs from raw CDC state at correlation",
            )
            if role == "semantic":
                semantic_sources.add(read["source"])
                statement_counts[source["statement_id"]] = (
                    statement_counts.get(source["statement_id"], 0) + 1
                )
                has_empty_value = has_empty_value or value == b""
                table_contract = codec.table(table)
                has_composite_key = has_composite_key or len(table_contract["key_columns"]) > 1
                for column in table_contract["columns"]:
                    if not column["nullable"]:
                        continue
                    if row[column["name"]] is None:
                        nullable_null_columns.add((table, column["name"]))
        if role == "semantic":
            expected_cdc_name, expected_txn_id = semantic_mapping[client["txn_type"]]
            require(
                cdc_name == expected_cdc_name
                and client["correlation_id"]["txn_id"] == expected_txn_id,
                "semantic client/CDC mapping mismatch",
            )
            require(client["txn_type"] not in semantic_types, "duplicate semantic transaction type")
            semantic_types.add(client["txn_type"])
            has_multirow_statement = has_multirow_statement or any(
                count > 1 for count in statement_counts.values()
            )
        else:
            require(client["read_count"] == 0, "synthetic join client unexpectedly has reads")
            require(
                synthetic_mapping.get(entry["name"])
                == (cdc_name, client["txn_type"], client["correlation_id"]["txn_id"]),
                "synthetic client/CDC mapping mismatch",
            )
        clients.append(client)
    require(
        semantic_types == {"NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"},
        "semantic client fixture lacks transaction type",
    )
    require(
        semantic_sources == {"snapshot", "local"},
        "semantic client fixture lacks snapshot/local coverage",
    )
    require(has_multirow_statement, "semantic client fixture lacks multirow coverage")
    require(has_empty_value, "semantic client fixture lacks empty canonical value coverage")
    require(
        {
            ("benchbase.oorder", "o_carrier_id"),
            ("benchbase.order_line", "ol_delivery_d"),
        }
        <= nullable_null_columns,
        "semantic client fixture lacks nullable carrier/delivery coverage",
    )
    require(has_composite_key, "semantic client fixture lacks composite-key coverage")
    clients.sort(key=lambda client: client["session_seq"])
    require(
        [client["session_seq"] for client in clients] == list(range(1, 9)),
        "client fixture session sequence is not dense",
    )
    require(
        [client["correlation_id"]["txn_id"] for client in clients] == list(range(1, 9)),
        "client fixture marker sequence is not dense",
    )

    discarded = document["discarded_attempt_reads"]
    require(isinstance(discarded, list) and len(discarded) == 1, "discarded attempt fixture count")
    discarded_attempt = discarded[0]
    require(
        isinstance(discarded_attempt, dict)
        and set(discarded_attempt)
        == {
            "name",
            "txn_type",
            "discarded",
            "published",
            "reads",
            "read_digest",
            "read_provenance",
        },
        "discarded attempt fixture shape",
    )
    require(
        discarded_attempt["discarded"] is True
        and discarded_attempt["published"] is False
        and discarded_attempt["txn_type"] == "NewOrder",
        "discarded attempt fixture semantics",
    )
    require(
        read_digest(discarded_attempt["reads"]) == discarded_attempt["read_digest"],
        "discarded attempt digest mismatch",
    )
    require(
        any(read["value_state"] == "absent" for read in discarded_attempt["reads"]),
        "discarded attempt lacks Absent read",
    )
    discarded_provenance = discarded_attempt["read_provenance"]
    require(
        len(discarded_provenance) == len(discarded_attempt["reads"]),
        "discarded attempt provenance count",
    )
    absent_count = 0
    for index, (read, source) in enumerate(
        zip(discarded_attempt["reads"], discarded_provenance, strict=True)
    ):
        require(
            isinstance(source, dict)
            and set(source)
            == {
                "event_seq",
                "statement_id",
                "trace_statement_id",
                "table",
                "row_name",
                "key",
            },
            "discarded attempt provenance shape",
        )
        require(source["event_seq"] == index == read["event_seq"], "discarded sequence")
        contract = contracts.get((discarded_attempt["txn_type"], source["statement_id"]))
        require(contract is not None, "discarded provenance names unregistered statement")
        require(
            contract["trace_statement_id"] == source["trace_statement_id"]
            and contract["table"] == source["table"]
            and contract["classification"] == read["source"],
            "discarded provenance differs from statement contract",
        )
        if read["value_state"] == "present":
            require(
                source["row_name"] in row_catalog and source["key"] is None,
                "discarded present provenance",
            )
            table, _row, key, value = row_catalog[source["row_name"]]
            require(table == source["table"], "discarded present row table")
            require(decode_base64(read["key_bytes_base64"], "discarded key") == key, "discarded key bytes")
            require(
                decode_base64(read["value_bytes_base64"], "discarded value") == value,
                "discarded value bytes",
            )
        else:
            absent_count += 1
            require(
                source["row_name"] is None and isinstance(source["key"], dict),
                "discarded absent provenance",
            )
            try:
                key = codec.encode_key(source["table"], source["key"])
            except CodecError as error:
                raise ClientFixtureError(
                    f"discarded absent key: {error.code}: {error}"
                ) from error
            require(
                decode_base64(read["key_bytes_base64"], "discarded absent key") == key,
                "discarded absent key bytes",
            )
    require(absent_count == 1, "discarded attempt Absent read count differs")

    assertions = document["empty_result_assertions"]
    require(isinstance(assertions, list) and len(assertions) == 1, "empty result assertion count")
    empty = assertions[0]
    require(
        empty
        == {
            "name": "delivery_empty_district",
            "txn_type": "Delivery",
            "statement_id": "delivGetOrderIdSQL",
            "returned_row_count": 0,
            "emitted_reads": [],
        },
        "Delivery empty-result assertion mismatch",
    )
    require(
        contracts[("Delivery", "delivGetOrderIdSQL")]["empty_result"] == "no_event",
        "Delivery empty-result contract changed",
    )

    done = document["session_done"]
    require(done["run_id"] == run_id and done["session_id"] == session_id, "SessionDone IDs")
    require(
        done["final_session_seq"] == 8
        and done["logical_transactions_started"] == 8
        and done["successful_transactions"] == 8
        and done["terminal_aborted_transactions"] == 0
        and done["physical_attempts_started"] == 8
        and done["definitively_aborted_attempts"] == 0
        and done["published_transactions"] == 8
        and done["retry_attempts"] == 0
        and done["ambiguous_attempts"] == 0,
        "SessionDone counters differ from eight accepted fixture clients",
    )
    require(
        done["rolling_correlation_digest"] == rolling_digest(run_id, session_id, clients),
        "SessionDone rolling digest mismatch",
    )
    return clients
