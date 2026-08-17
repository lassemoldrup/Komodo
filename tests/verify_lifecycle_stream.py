#!/usr/bin/env python3
"""Verify a captured pinned-stack CDC lifecycle stream."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


STREAM_ID = re.compile(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)\Z")
CANONICAL_IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
SCHEMA_SOURCE_FIELDS = {
    "version",
    "connector",
    "name",
    "ts_ms",
    "snapshot",
    "db",
    "sequence",
    "ts_us",
    "ts_ns",
    "table",
    "server_id",
    "gtid",
    "file",
    "pos",
    "row",
    "thread",
    "query",
}
CONFIGURED_TABLE_IDS = {
    '"benchbase"."usertable"',
    '"benchbase"."checker_run_fence"',
    '"benchbase"."session_txn"',
}


@dataclass
class StartupSchemaState:
    stage: str = "server_settings"
    dropped_table_ids: list[str] = field(default_factory=list)
    create_index: int = 0

    @property
    def finished(self) -> bool:
        return self.stage == "complete"


class StreamError(ValueError):
    pass


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise StreamError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def require(condition: bool, message: str) -> None:
    if not condition:
        raise StreamError(message)


def load_json_text(value: str, label: str) -> Any:
    try:
        return json.loads(value, object_pairs_hook=reject_duplicate_keys)
    except json.JSONDecodeError as error:
        raise StreamError(f"{label} is not JSON: {error}") from error


def integer(value: Any, label: str) -> int:
    require(type(value) is int, f"{label} is not a JSON integer: {value!r}")
    return value


def uint64(value: Any, label: str) -> int:
    converted = integer(value, label)
    require(0 <= converted <= (1 << 64) - 1, f"{label} is outside uint64")
    return converted


def nullable_string(value: Any, label: str) -> None:
    require(value is None or isinstance(value, str), f"{label} is not null/string")


def verify_schema_column(column: Any) -> None:
    require(isinstance(column, dict), "startup schema column is not an object")
    require(
        set(column)
        == {
            "name",
            "jdbcType",
            "nativeType",
            "typeName",
            "typeExpression",
            "charsetName",
            "length",
            "scale",
            "position",
            "optional",
            "autoIncremented",
            "generated",
            "comment",
            "defaultValueExpression",
            "enumValues",
        },
        "startup schema column shape is invalid",
    )
    require(isinstance(column["name"], str) and column["name"], "schema column name is empty")
    integer(column["jdbcType"], "schema column jdbcType")
    nullable_string(column["nativeType"], "schema column nativeType")
    require(isinstance(column["typeName"], str), "schema column typeName is not a string")
    require(
        isinstance(column["typeExpression"], str),
        "schema column typeExpression is not a string",
    )
    nullable_string(column["charsetName"], "schema column charsetName")
    for name in ("length", "scale"):
        require(
            column[name] is None or type(column[name]) is int,
            f"schema column {name} is not null/integer",
        )
    require(
        1 <= integer(column["position"], "schema column position") <= (1 << 32) - 1,
        "schema column position is outside uint32",
    )
    for name in ("optional", "autoIncremented", "generated"):
        require(type(column[name]) is bool, f"schema column {name} is not boolean")
    nullable_string(column["comment"], "schema column comment")
    nullable_string(
        column["defaultValueExpression"], "schema column defaultValueExpression"
    )
    enum_values = column["enumValues"]
    require(
        enum_values is None
        or (isinstance(enum_values, list) and all(isinstance(item, str) for item in enum_values)),
        "schema column enumValues is malformed",
    )


def verify_schema_table(table: Any) -> None:
    require(isinstance(table, dict), "startup CREATE lacks a table")
    require(
        set(table) == {"defaultCharsetName", "primaryKeyColumnNames", "columns", "comment"},
        "startup schema table shape is invalid",
    )
    nullable_string(table["defaultCharsetName"], "schema table defaultCharsetName")
    primary_keys = table["primaryKeyColumnNames"]
    columns = table["columns"]
    require(
        isinstance(primary_keys, list) and all(isinstance(item, str) for item in primary_keys),
        "schema table primaryKeyColumnNames is malformed",
    )
    require(isinstance(columns, list) and columns, "schema table columns is empty/malformed")
    for column in columns:
        verify_schema_column(column)
    nullable_string(table["comment"], "schema table comment")


def verify_startup_schema_snapshot(
    key: Any,
    envelope: dict[str, Any],
    backend_source_id: str,
    state: StartupSchemaState,
) -> None:
    require(not state.finished, "schema envelope follows the snapshot last record")
    require(
        isinstance(key, dict) and set(key) == {"databaseName"},
        "startup schema key shape is invalid",
    )
    require(
        set(envelope)
        == {"source", "ts_ms", "databaseName", "schemaName", "ddl", "tableChanges"},
        "startup schema envelope shape is invalid",
    )
    database = envelope["databaseName"]
    require(
        isinstance(database, str) and database in {"", "benchbase"},
        "startup schema database is invalid",
    )
    require(key["databaseName"] == database, "startup schema key/database mismatch")
    require(envelope["schemaName"] is None, "startup schema schemaName is non-null")
    require(isinstance(envelope["ddl"], str) and envelope["ddl"], "startup DDL is empty")
    uint64(envelope["ts_ms"], "startup schema ts_ms")
    changes = envelope["tableChanges"]
    require(isinstance(changes, list), "startup schema tableChanges is not an array")

    source = envelope["source"]
    require(isinstance(source, dict), "startup schema source is not an object")
    require(set(source) == SCHEMA_SOURCE_FIELDS, "startup schema source shape is invalid")
    require(source["version"] == "3.6.1.Final", "startup schema version mismatch")
    require(source["connector"] == "mariadb", "startup schema connector mismatch")
    require(source["name"] == backend_source_id, "startup schema source identity mismatch")
    require(
        isinstance(source["snapshot"], str)
        and source["snapshot"] in {"true", "last"},
        "non-snapshot DDL was observed",
    )
    require(source["db"] == database, "startup schema source database mismatch")
    require(uint64(source["server_id"], "startup schema server_id") == 0, "server_id is not zero")
    require(
        all(source[field] is None for field in ("sequence", "gtid", "thread", "query")),
        "startup schema nullable source metadata is non-null",
    )
    require(isinstance(source["file"], str) and source["file"], "startup schema file is empty")
    for field in ("ts_ms", "ts_us", "ts_ns", "pos", "row"):
        uint64(source[field], f"startup schema source {field}")
    table = source["table"]
    require(
        table is None
        or (isinstance(table, str) and CANONICAL_IDENTIFIER.fullmatch(table) is not None),
        "startup schema source table is invalid",
    )

    ddl = envelope["ddl"]
    snapshot = source["snapshot"]
    if not changes:
        require(table is None, "startup database schema record unexpectedly names a table")
        if state.stage == "server_settings":
            require(database == "" and ddl.startswith("SET "), "server-settings record malformed")
            require(snapshot == "true", "server-settings snapshot marker is invalid")
            state.stage = "drop_tables"
            return
        if state.stage == "drop_tables":
            require(
                database == "benchbase"
                and ddl.startswith("DROP DATABASE ")
                and state.dropped_table_ids,
                "DROP DATABASE record is out of order",
            )
            require(snapshot == "true", "DROP DATABASE snapshot marker is invalid")
            require(
                CONFIGURED_TABLE_IDS <= set(state.dropped_table_ids),
                "startup schema snapshot is missing a configured table",
            )
            state.stage = "create_database"
            return
        if state.stage == "create_database":
            require(
                database == "benchbase" and ddl.startswith("CREATE DATABASE "),
                "CREATE DATABASE record is out of order",
            )
            require(snapshot == "true", "CREATE DATABASE snapshot marker is invalid")
            state.stage = "use_database"
            return
        if state.stage == "use_database":
            require(
                database == "benchbase" and ddl.startswith("USE "),
                "USE DATABASE record is out of order",
            )
            require(snapshot == "true", "USE DATABASE snapshot marker is invalid")
            state.stage = "create_tables"
            return
        raise StreamError("empty startup schema record is out of order")

    require(
        len(changes) == 1 and database == "benchbase" and table is not None,
        "startup table schema record is malformed",
    )
    change = changes[0]
    require(
        isinstance(change, dict) and set(change) == {"type", "id", "table"},
        "startup schema table change shape is invalid",
    )
    identifier = change["id"]
    require(isinstance(identifier, str) and identifier, "startup schema change id is empty")
    require(
        identifier == f'"benchbase"."{table}"',
        "startup schema table-change/source identity mismatch",
    )
    if state.stage == "drop_tables":
        require(
            change["type"] == "DROP"
            and change["table"] is None
            and ddl.startswith("DROP TABLE "),
            "startup DROP TABLE record is malformed",
        )
        require(snapshot == "true", "DROP TABLE snapshot marker is invalid")
        require(
            not state.dropped_table_ids or identifier > state.dropped_table_ids[-1],
            "startup DROP TABLE identifiers are duplicated or reordered",
        )
        state.dropped_table_ids.append(identifier)
        return

    require(
        state.stage == "create_tables"
        and change["type"] == "CREATE"
        and ddl.startswith("CREATE TABLE "),
        "startup CREATE TABLE record is out of order",
    )
    verify_schema_table(change["table"])
    require(
        state.create_index < len(state.dropped_table_ids)
        and identifier == state.dropped_table_ids[state.create_index],
        "startup CREATE TABLE identifiers do not match ordered drops",
    )
    final_table = state.create_index + 1 == len(state.dropped_table_ids)
    require(
        snapshot == ("last" if final_table else "true"),
        "startup CREATE TABLE snapshot marker is out of order",
    )
    state.create_index += 1
    if final_table:
        require(
            CONFIGURED_TABLE_IDS <= set(state.dropped_table_ids),
            "startup schema snapshot lost a configured table",
        )
        state.stage = "complete"


def transaction_summary(
    transaction: dict[str, Any], run_id: str, marker_state: dict[str, int]
) -> dict[str, Any]:
    rows = transaction["rows"]
    tables = {row["table"] for row in rows}
    if tables == {"checker_run_fence"}:
        require(len(rows) == 1, "a fence transaction must contain one row event")
        after = rows[0]["after"]
        require(isinstance(after, dict), "fence row has no after image")
        require(after.get("run_id") == run_id, "fence run_id mismatch")
        phase = after.get("phase")
        require(phase in {"RUN_START", "LOAD_FINISHED", "END_FENCE"}, "unknown fence phase")
        return {
            "classification": "fence",
            "phase": phase,
            "event_count": len(rows),
        }
    if tables == {"usertable"}:
        return {
            "classification": "application",
            "operations": [row["op"] for row in rows],
            "event_count": len(rows),
        }
    require(
        tables in ({"session_txn"}, {"session_txn", "usertable"}),
        f"unexpected or mixed tables: {sorted(tables)}",
    )
    marker_indexes = [
        index for index, row in enumerate(rows) if row["table"] == "session_txn"
    ]
    require(marker_indexes == [len(rows) - 1], "workload marker is not exactly one final row event")
    marker = rows[-1]
    after = marker["after"]
    require(isinstance(after, dict), "workload marker lacks an after image")
    require(after.get("session_id") == "ycsb/worker/1", "workload marker session mismatch")
    session_id = after["session_id"]
    txn_id = integer(after.get("txn_id"), "marker txn_id")
    previous = marker_state.get(session_id)
    if previous is None:
        require(marker["op"] == "c", "first workload marker is not an insert")
        require(marker["before"] is None, "first workload marker has a before image")
    else:
        require(marker["op"] == "u", "later workload marker is not an update")
        before = marker["before"]
        require(isinstance(before, dict), "marker update lacks a before image")
        require(before.get("session_id") == session_id, "marker before session mismatch")
        require(
            integer(before.get("txn_id"), "marker before txn_id") == previous,
            "marker before-image chain mismatch",
        )
        require(txn_id > previous, "marker txn_id did not advance")
    marker_state[session_id] = txn_id
    return {
        "classification": "workload_marked",
        "correlation_id": {"session_id": session_id, "txn_id": txn_id},
        "operations": [row["op"] for row in rows if row["table"] == "usertable"],
        "event_count": len(rows),
    }


def verify_stream(entries: Any, run_id: str, backend_source_id: str) -> list[dict[str, Any]]:
    require(isinstance(entries, list), "XRANGE JSON root is not an array")
    active: dict[str, Any] | None = None
    completed: list[dict[str, Any]] = []
    marker_state: dict[str, int] = {}
    previous_stream_id: tuple[int, int] | None = None
    startup_schema = StartupSchemaState()

    for entry_index, entry in enumerate(entries):
        require(isinstance(entry, list) and len(entry) == 2, f"entry {entry_index} shape is invalid")
        stream_id, fields = entry
        match = STREAM_ID.fullmatch(stream_id) if isinstance(stream_id, str) else None
        require(match is not None, f"entry {entry_index} has invalid stream ID")
        numeric_stream_id = (int(match.group(1)), int(match.group(2)))
        require(
            previous_stream_id is None or numeric_stream_id > previous_stream_id,
            "Redis stream IDs are not increasing",
        )
        previous_stream_id = numeric_stream_id
        require(
            isinstance(fields, list) and len(fields) == 2,
            f"entry {stream_id} must contain exactly one Debezium key/envelope pair",
        )
        key = load_json_text(fields[0], f"entry {stream_id} Debezium key")
        envelope = load_json_text(fields[1], f"entry {stream_id} envelope")
        require(isinstance(envelope, dict), f"entry {stream_id} envelope is not an object")

        if "ddl" in envelope:
            require(active is None and not completed, "DDL appears after transactional evidence")
            verify_startup_schema_snapshot(key, envelope, backend_source_id, startup_schema)
            continue

        if "status" in envelope:
            require(
                startup_schema.finished,
                "transactional evidence begins without a complete startup schema snapshot",
            )
            status = envelope["status"]
            transaction_id = envelope.get("id")
            require(isinstance(transaction_id, str) and transaction_id, "metadata lacks transaction id")
            if status == "BEGIN":
                require(active is None, "nested Debezium BEGIN")
                active = {"id": transaction_id, "rows": []}
                continue
            require(status == "END", f"unknown transaction status: {status}")
            require(active is not None, "Debezium END without BEGIN")
            require(transaction_id == active["id"], "BEGIN/END transaction id mismatch")
            event_count = integer(envelope.get("event_count"), "END.event_count")
            require(event_count == len(active["rows"]), "END.event_count mismatch")
            ordinals = [row["total_order"] for row in active["rows"]]
            require(ordinals == list(range(1, event_count + 1)), "row ordinals are not dense")
            collection_counts = Counter(row["collection"] for row in active["rows"])
            declared_counts = Counter()
            for collection in envelope.get("data_collections") or []:
                require(isinstance(collection, dict), "END data_collection is not an object")
                declared_counts[collection.get("data_collection")] += integer(
                    collection.get("event_count"), "END data_collection event_count"
                )
            require(collection_counts == declared_counts, "END per-table counts mismatch")
            completed.append(transaction_summary(active, run_id, marker_state))
            active = None
            continue

        require(active is not None, f"row event {stream_id} appears outside a transaction")
        require(envelope.get("op") in {"c", "u", "d"}, f"unsupported row op at {stream_id}")
        source = envelope.get("source")
        transaction = envelope.get("transaction")
        require(isinstance(source, dict), f"row {stream_id} lacks source metadata")
        require(isinstance(transaction, dict), f"row {stream_id} lacks transaction metadata")
        require(transaction.get("id") == active["id"], "row transaction id mismatch")
        require(source.get("gtid") == active["id"], "row GTID mismatch")
        require(source.get("name") == backend_source_id, "row backend source identity mismatch")
        database = source.get("db")
        table = source.get("table")
        require(database == "benchbase", f"row has unexpected database: {database}")
        require(isinstance(table, str), "row lacks table")
        active["rows"].append(
            {
                "table": table,
                "collection": f"{database}.{table}",
                "op": envelope["op"],
                "before": envelope.get("before"),
                "after": envelope.get("after"),
                "total_order": integer(transaction.get("total_order"), "row total_order"),
            }
        )

    require(active is None, "stream ends with an incomplete transaction")
    require(
        startup_schema.finished,
        "stream ends without a complete startup schema snapshot",
    )
    shape = [
        (item["classification"], item.get("phase"))
        for item in completed
    ]
    require(
        shape
        == [
            ("fence", "RUN_START"),
            ("application", None),
            ("fence", "LOAD_FINISHED"),
            ("workload_marked", None),
            ("workload_marked", None),
            ("workload_marked", None),
            ("workload_marked", None),
            ("workload_marked", None),
            ("workload_marked", None),
            ("workload_marked", None),
            ("fence", "END_FENCE"),
        ],
        f"unexpected completed transaction order: {shape}",
    )
    require(completed[1]["operations"] == ["c", "c"], "loader transaction is not two inserts")
    expected_operations = [
        ["u"],
        [],
        ["c", "u", "u", "d"],
        ["c"],
        ["u", "u"],
        ["d"],
        ["d", "c"],
    ]
    actual_operations = [item["operations"] for item in completed[3:10]]
    require(actual_operations == expected_operations, "unexpected workload row operations")
    require(
        [item["correlation_id"]["txn_id"] for item in completed[3:10]]
        == list(range(1, 8)),
        "workload marker chain is not 1..7",
    )
    return completed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--backend-source-id", default="mariadb-main")
    parser.add_argument("--summary-output")
    args = parser.parse_args()
    try:
        outer = load_json_text(Path(args.input).read_text(encoding="utf-8"), "XRANGE capture")
        summary = verify_stream(outer, args.run_id, args.backend_source_id)
        rendered = json.dumps(summary, indent=2, sort_keys=True) + "\n"
        if args.summary_output:
            Path(args.summary_output).write_text(rendered, encoding="utf-8")
        sys.stdout.write(rendered)
    except (OSError, StreamError, UnicodeError) as error:
        print(f"lifecycle stream verification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
