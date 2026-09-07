#!/usr/bin/env python3

import argparse
import copy
import hashlib
import json
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path


RUN_ID = "11111111-2222-3333-4444-555555555555"
STALL = object()
CLOSE = object()


def resp_integer(value):
    return f":{value}\r\n".encode()


def resp_bulk(value):
    if isinstance(value, str):
        value = value.encode()
    return b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"


def resp_array(elements):
    return b"*" + str(len(elements)).encode() + b"\r\n" + b"".join(elements)


def manifest_payload():
    return json.dumps(
        {
            "kind": "run_manifest",
            "run_id": RUN_ID,
            "backend": "mariadb",
            "backend_source_id": "blackbox-mariadb",
            "declared_isolation": "REPEATABLE_READ",
            "benchmark": "ycsb",
            "expected_session_ids": ["ycsb/worker/1"],
            "application_table_allowlist": ["benchbase.usertable"],
            "transaction_marker_table": "benchbase.session_txn",
            "control_table_allowlist": ["benchbase.checker_run_fence"],
            "result_scope": "point_transaction_soww",
        },
        separators=(",", ":"),
    )


def manifest_xread_reply(payload=None):
    stream = f"blackbox:client:{RUN_ID}"
    if payload is None:
        payload = manifest_payload()
    entry = resp_array(
        [resp_bulk("1-0"), resp_array([resp_bulk("payload"), resp_bulk(payload)])]
    )
    return resp_array([resp_array([resp_bulk(stream), resp_array([entry])])])


def tpcc_manifest_payload():
    manifest = json.loads(manifest_payload())
    manifest["benchmark"] = "tpcc"
    manifest["expected_session_ids"] = ["tpcc/worker/1"]
    manifest["application_table_allowlist"] = [
        "benchbase.warehouse",
        "benchbase.item",
        "benchbase.stock",
        "benchbase.district",
        "benchbase.customer",
        "benchbase.history",
        "benchbase.oorder",
        "benchbase.new_order",
        "benchbase.order_line",
    ]
    return json.dumps(manifest, separators=(",", ":"))


def read_resp_command(connection):
    def read_line():
        line = bytearray()
        while True:
            byte = connection.recv(1)
            if not byte:
                return None
            line.extend(byte)
            if line.endswith(b"\r\n"):
                return bytes(line[:-2])

    root = read_line()
    if root is None:
        return None
    if not root.startswith(b"*"):
        raise AssertionError(f"expected RESP array, got {root!r}")
    count = int(root[1:])
    command = []
    for _ in range(count):
        length = read_line()
        if length is None or not length.startswith(b"$"):
            raise AssertionError(f"expected RESP bulk length, got {length!r}")
        size = int(length[1:])
        value = bytearray()
        while len(value) != size + 2:
            chunk = connection.recv(size + 2 - len(value))
            if not chunk:
                raise AssertionError("connection closed in a RESP bulk value")
            value.extend(chunk)
        if not value.endswith(b"\r\n"):
            raise AssertionError("RESP bulk value has no terminator")
        command.append(bytes(value[:-2]))
    return command


class ScriptedRedis:
    def __init__(self, handler):
        self.handler = handler
        self.commands = []
        self.command_condition = threading.Condition()
        self.stop_event = threading.Event()
        self.connections = []
        self.connection_lock = threading.Lock()
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.accept_thread = threading.Thread(target=self._accept, daemon=True)
        self.accept_thread.start()

    def _accept(self):
        connection_id = 0
        while not self.stop_event.is_set():
            try:
                connection, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self.connection_lock:
                self.connections.append(connection)
            thread = threading.Thread(
                target=self._serve, args=(connection_id, connection), daemon=True
            )
            connection_id += 1
            thread.start()

    def _serve(self, connection_id, connection):
        try:
            while not self.stop_event.is_set():
                command = read_resp_command(connection)
                if command is None:
                    return
                with self.command_condition:
                    self.commands.append(command)
                    self.command_condition.notify_all()
                response = self.handler(connection_id, command)
                if response is STALL:
                    self.stop_event.wait()
                    return
                if response is CLOSE:
                    return
                connection.sendall(response)
        except (ConnectionError, OSError):
            return
        finally:
            connection.close()

    def wait_for(self, predicate, timeout=2.0):
        deadline = time.monotonic() + timeout
        with self.command_condition:
            while not predicate(self.commands):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError("timed out waiting for a Redis command")
                self.command_condition.wait(remaining)

    def close(self):
        self.stop_event.set()
        self.listener.close()
        with self.connection_lock:
            for connection in self.connections:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                connection.close()
        self.accept_thread.join(timeout=1.0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def monitor_command(executable, port, timeout_ms, extra_arguments=None):
    command = [
        executable,
        "--redis-host",
        "127.0.0.1",
        "--redis-port",
        str(port),
        "--run-id",
        RUN_ID,
        "--overall-timeout-ms",
        str(timeout_ms),
    ]
    if extra_arguments:
        command.extend(extra_arguments)
    return command


def run_monitor(
    executable, port, timeout_ms, process_timeout=3.0, extra_arguments=None
):
    started = time.monotonic()
    result = subprocess.run(
        monitor_command(executable, port, timeout_ms, extra_arguments),
        capture_output=True,
        text=True,
        timeout=process_timeout,
        check=False,
    )
    return result, time.monotonic() - started


def output(result):
    return result.stdout + result.stderr


def require_invalid(result, code):
    assert result.returncode == 4, result
    assert f"outcome=INVALID code={code}" in output(result), output(result)


def check_ownership(executable):
    lock = threading.Lock()
    claimed = False

    def handler(_connection_id, command):
        nonlocal claimed
        if command[0] == b"EVAL":
            assert command[2:] == [
                b"2",
                f"blackbox:monitor-owner:{RUN_ID}".encode(),
                f"blackbox:status:{RUN_ID}".encode(),
                RUN_ID.encode(),
            ]
            assert b"EXISTS" in command[1]
            assert b"SET" in command[1]
            with lock:
                if claimed:
                    return resp_integer(0)
                claimed = True
                return resp_integer(1)
        if command[0] == b"XREAD":
            return STALL
        raise AssertionError(f"unexpected Redis command: {command!r}")

    with ScriptedRedis(handler) as server:
        first = subprocess.Popen(
            monitor_command(executable, server.port, 5000),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            server.wait_for(lambda commands: any(c[0] == b"EVAL" for c in commands))
            concurrent, elapsed = run_monitor(executable, server.port, 5000)
            require_invalid(concurrent, "COMPONENT_RESTARTED")
            assert elapsed < 1.0, elapsed
            assert not any(c[0] == b"XADD" for c in server.commands)

            first.terminate()
            first.communicate(timeout=2.0)
            replacement, elapsed = run_monitor(executable, server.port, 5000)
            require_invalid(replacement, "COMPONENT_RESTARTED")
            assert elapsed < 1.0, elapsed
            assert not any(c[0] == b"XADD" for c in server.commands)
        finally:
            if first.poll() is None:
                first.kill()
                first.communicate(timeout=2.0)
    print("ownership: concurrent and replacement monitors rejected without XADD")


def check_claim_timeout(executable):
    def handler(_connection_id, command):
        assert command[0] == b"EVAL"
        return STALL

    with ScriptedRedis(handler) as server:
        result, elapsed = run_monitor(executable, server.port, 5000)
        require_invalid(result, "COMPONENT_RESTARTED")
        assert elapsed < 2.0, elapsed
        assert not any(c[0] == b"XADD" for c in server.commands)
    print("ownership claim: silent peer bounded by the operation timeout cap")


def check_manifest_timeout(executable):
    def handler(_connection_id, command):
        if command[0] == b"EVAL":
            return resp_integer(1)
        if command[0] == b"XREAD":
            return STALL
        if command[0] == b"XADD":
            return resp_bulk("1-0")
        raise AssertionError(f"unexpected Redis command: {command!r}")

    with ScriptedRedis(handler) as server:
        result, elapsed = run_monitor(executable, server.port, 800)
        require_invalid(result, "COMPONENT_RESTARTED")
        assert elapsed < 1.5, elapsed
        assert any(c[0] == b"XADD" for c in server.commands)
    print("manifest XREAD: silent peer bounded and INVALID published")


def check_main_read_timeout(executable):
    xreads = 0

    def handler(_connection_id, command):
        nonlocal xreads
        if command[0] == b"EVAL":
            return resp_integer(1)
        if command[0] == b"XREAD":
            xreads += 1
            if xreads == 1:
                return manifest_xread_reply()
            return STALL
        if command[0] == b"XADD":
            return resp_bulk("1-0")
        raise AssertionError(f"unexpected Redis command: {command!r}")

    with ScriptedRedis(handler) as server:
        result, elapsed = run_monitor(executable, server.port, 1000)
        assert result.returncode == 4, result
        assert elapsed < 1.75, elapsed
        assert xreads == 2, xreads
        assert any(c[0] == b"XADD" for c in server.commands)
    print("main XREAD: silent peer bounded and terminal INVALID published")


def check_status_timeout(executable):
    xreads = 0

    def handler(_connection_id, command):
        nonlocal xreads
        if command[0] == b"EVAL":
            return resp_integer(1)
        if command[0] == b"XREAD":
            xreads += 1
            if xreads == 1:
                return manifest_xread_reply()
            return CLOSE
        if command[0] == b"XADD":
            return STALL
        raise AssertionError(f"unexpected Redis command: {command!r}")

    with ScriptedRedis(handler) as server:
        result, elapsed = run_monitor(executable, server.port, 5000)
        require_invalid(result, "REDIS_PUBLICATION_FAILED")
        assert elapsed < 2.0, elapsed
        assert any(c[0] == b"XADD" for c in server.commands)
    print("status XADD: silent peer bounded by the operation timeout cap")


def check_tpcc_adapter_enters_main_read(executable):
    xreads = 0

    def handler(_connection_id, command):
        nonlocal xreads
        if command[0] == b"EVAL":
            return resp_integer(1)
        if command[0] == b"XREAD":
            xreads += 1
            if xreads == 1:
                return manifest_xread_reply(tpcc_manifest_payload())
            return STALL
        if command[0] == b"XADD":
            return resp_bulk("1-0")
        raise AssertionError(f"unexpected Redis command: {command!r}")

    with ScriptedRedis(handler) as server:
        result, _ = run_monitor(executable, server.port, 1000)
        assert result.returncode == 4, result
        assert "BRANCH_CONTRACT_MISMATCH" not in output(result)
        assert xreads == 2
        assert any(command[0] == b"XADD" for command in server.commands)
    print("adapter capability: TPC-C enters the monitor-owned CDC path")


def check_tpcc_roster_mismatch_is_invalid(executable):
    manifest = json.loads(tpcc_manifest_payload())
    manifest["application_table_allowlist"].remove("benchbase.order_line")
    payload = json.dumps(manifest, separators=(",", ":"))
    xreads = 0

    def handler(_connection_id, command):
        nonlocal xreads
        if command[0] == b"EVAL":
            return resp_integer(1)
        if command[0] == b"XREAD":
            xreads += 1
            assert xreads == 1
            return manifest_xread_reply(payload)
        if command[0] == b"XADD":
            return resp_bulk("1-0")
        raise AssertionError(f"unexpected Redis command: {command!r}")

    with ScriptedRedis(handler) as server:
        result, _ = run_monitor(executable, server.port, 1000)
        require_invalid(result, "UNEXPECTED_TABLE")
        assert xreads == 1
        assert any(command[0] == b"XADD" for command in server.commands)
    print("adapter roster: incomplete TPC-C manifest publishes INVALID")


def assert_no_audited_result(value):
    if isinstance(value, dict):
        assert "outcome" not in value, value
        assert "verdict" not in value, value
        for child in value.values():
            assert_no_audited_result(child)
    elif isinstance(value, list):
        for child in value:
            assert_no_audited_result(child)
    elif isinstance(value, str):
        assert value not in {"PASS", "VIOLATION", "INVALID"}, value


def check_join_only_requires_metrics(executable):
    result = subprocess.run(
        monitor_command(
            executable,
            1,
            1000,
            ["--processing-mode", "join-only"],
        ),
        capture_output=True,
        text=True,
        timeout=2.0,
        check=False,
    )
    assert result.returncode == 1, result
    assert "join-only processing requires --metrics-output" in output(result)
    print("join-only configuration: metrics output is mandatory")


def check_join_only_early_failure_artifact(executable):
    def handler(_connection_id, command):
        assert command[0] == b"EVAL"
        return resp_integer(0)

    with tempfile.TemporaryDirectory() as directory:
        metrics_path = Path(directory) / "monitor-metrics.json"
        with ScriptedRedis(handler) as server:
            result, elapsed = run_monitor(
                executable,
                server.port,
                5000,
                extra_arguments=[
                    "--processing-mode",
                    "join-only",
                    "--metrics-output",
                    str(metrics_path),
                ],
            )
            assert result.returncode == 1, result
            assert elapsed < 1.0, elapsed
            assert not any(c[0] == b"XADD" for c in server.commands)
        measurement = json.loads(metrics_path.read_text(encoding="utf-8"))
        assert measurement["kind"] == "performance_measurement"
        assert measurement["processing_mode"] == "join-only"
        assert measurement["completed"] is False
        assert measurement["completion_status"] == "failed"
        assert measurement["evidence_reconciled"] is False
        assert measurement["diagnostics"][0]["code"] == "COMPONENT_RESTARTED"
        assert set(("assembler", "joiner", "checker", "process")) <= set(
            measurement
        )
        assert measurement["limits"]["assembler"] == {
            "max_active_transactions": 1024,
            "max_events_per_transaction": 100000,
            "max_active_bytes": 268435456,
            "max_completed_transactions": 10000000,
        }
        assert measurement["limits"]["joiner"] == {
            "max_pending_client_records": 1000000,
            "max_pending_client_bytes": 268435456,
            "max_pending_cdc_slots": 1000000,
            "max_pending_cdc_bytes": 268435456,
            "max_retained_identity_records": 10000000,
            "max_retained_identity_bytes": 536870912,
            "max_events_per_transaction": 101000,
            "max_bootstrap_state_rows": 10000000,
            "max_bootstrap_state_bytes": 268435456,
            "max_checker_state_bytes": 536870912,
        }
        assert measurement["limits"]["checker"] == {
            "history_gc": False,
            "max_retained_state_bytes": 536870912,
            "max_events_per_transaction": 101000,
        }
        assert {
            "active_transactions_high_water",
            "observed_events_per_transaction_high_water",
            "active_assembly_high_water_bytes",
            "retained_identity_high_water_records",
            "retained_identity_high_water_bytes",
            "workload_normalized_write_events",
            "workload_raw_row_events",
            "accept_total_nanoseconds",
            "accept_max_nanoseconds",
            "workload_processing_total_nanoseconds",
            "workload_processing_max_nanoseconds",
        } <= measurement["assembler"].keys()
        assert {
            "pending_client_high_water_records",
            "pending_client_high_water_bytes",
            "pending_cdc_high_water_slots",
            "pending_cdc_high_water_bytes",
            "join_buffer_high_water_records",
            "join_buffer_high_water_bytes",
            "released_events_per_transaction_high_water",
            "retained_identity_high_water_records",
            "retained_identity_high_water_bytes",
            "bootstrap_state_high_water_rows",
            "bootstrap_state_high_water_bytes",
            "missing_client_head_wait_max_nanoseconds",
            "drain_invocations",
            "drain_total_nanoseconds",
            "drain_max_nanoseconds",
            "end_fence_drain_nanoseconds",
            "workload_processing_total_nanoseconds",
            "workload_processing_max_nanoseconds",
        } <= measurement["joiner"].keys()
        assert {
            "mapped_identity_high_water_records",
            "mapped_identity_high_water_bytes",
            "events_per_transaction_high_water",
            "workload_read_events",
            "workload_write_events",
            "workload_consume_total_nanoseconds",
            "workload_consume_max_nanoseconds",
        } <= measurement["checker"].keys()
        assert "peak_retained_state_bytes" in measurement["checker"]["engine"]
        assert_no_audited_result(measurement)
        assert "outcome=INVALID" not in output(result)
    print("join-only early failure: failed measurement written without verdict")


def check_capacity_options(executable):
    for option in (
        "--max-assembler-active-transactions",
        "--max-assembler-events-per-transaction",
        "--max-assembler-active-bytes",
        "--max-assembler-completed-transactions",
        "--max-pending-client-records",
        "--max-pending-client-bytes",
        "--max-pending-cdc-slots",
        "--max-pending-cdc-bytes",
        "--max-retained-identity-records",
        "--max-retained-identity-bytes",
        "--max-events-per-transaction",
        "--max-bootstrap-state-rows",
        "--max-bootstrap-state-bytes",
        "--max-checker-state-bytes",
    ):
        for invalid in ("0", "18446744073709551616"):
            result = subprocess.run(
                monitor_command(executable, 1, 1000, [option, invalid]),
                capture_output=True,
                text=True,
                timeout=2.0,
                check=False,
            )
            assert result.returncode == 1, result
            assert "must be a positive decimal count" in output(result), output(result)

    def handler(_connection_id, command):
        assert command[0] == b"EVAL"
        return resp_integer(0)

    with tempfile.TemporaryDirectory() as directory:
        metrics_path = Path(directory) / "monitor-metrics.json"
        with ScriptedRedis(handler) as server:
            result, _ = run_monitor(
                executable,
                server.port,
                5000,
                extra_arguments=[
                    "--processing-mode",
                    "join-only",
                    "--metrics-output",
                    str(metrics_path),
                    "--max-assembler-active-transactions",
                    "2048",
                    "--max-assembler-events-per-transaction",
                    "200000",
                    "--max-assembler-active-bytes",
                    "536870912",
                    "--max-assembler-completed-transactions",
                    "20000000",
                    "--max-pending-client-records",
                    "2000000",
                    "--max-pending-client-bytes",
                    "1073741824",
                    "--max-pending-cdc-slots",
                    "2000000",
                    "--max-pending-cdc-bytes",
                    "1073741824",
                    "--max-retained-identity-records",
                    "20000000",
                    "--max-retained-identity-bytes",
                    "2147483648",
                    "--max-events-per-transaction",
                    "202000",
                    "--max-bootstrap-state-rows",
                    "20000000",
                    "--max-bootstrap-state-bytes",
                    "1073741824",
                    "--max-checker-state-bytes",
                    "2147483648",
                    "--history-gc",
                    "on",
                    "--workload-rss-sampling",
                    "on",
                ],
            )
            assert result.returncode == 1, result
        measurement = json.loads(metrics_path.read_text(encoding="utf-8"))
        assert measurement["limits"]["assembler"] == {
            "max_active_transactions": 2048,
            "max_events_per_transaction": 200000,
            "max_active_bytes": 536870912,
            "max_completed_transactions": 20000000,
        }
        assert measurement["limits"]["joiner"] == {
            "max_pending_client_records": 2000000,
            "max_pending_client_bytes": 1073741824,
            "max_pending_cdc_slots": 2000000,
            "max_pending_cdc_bytes": 1073741824,
            "max_retained_identity_records": 20000000,
            "max_retained_identity_bytes": 2147483648,
            "max_events_per_transaction": 202000,
            "max_bootstrap_state_rows": 20000000,
            "max_bootstrap_state_bytes": 1073741824,
            "max_checker_state_bytes": 2147483648,
        }
        assert measurement["limits"]["checker"] == {
            "history_gc": True,
            "max_retained_state_bytes": 2147483648,
            "max_events_per_transaction": 202000,
        }
        assert measurement["process"]["workload_rss_sampling_enabled"] is True
        assert measurement["process"]["workload_rss_sampling_started"] is False
        assert measurement["process"]["workload_rss_sampling_valid"] is False
        assert measurement["process"]["workload_sampled_peak_rss_kib"] is None
        assert measurement["process"]["workload_rss_samples"] == 0
        assert_no_audited_result(measurement)
    print("monitor capacities: defaults, overrides, zero, and overflow validated")


def check_rss_sampling_options(executable):
    for arguments, message in (
        (["--workload-rss-sampling", "invalid"], "must be on or off"),
        (monitor_command(executable, 1, 1000, ["--workload-rss-sampling", "on"])[1:],
         "workload RSS sampling requires --metrics-output"),
    ):
        result = subprocess.run([executable, *arguments], capture_output=True,
                                text=True, timeout=2, check=False)
        assert result.returncode == 1 and message in output(result), result


def check_workload_rss_window(executable):
    root = Path(__file__).resolve().parents[2]
    attestation = json.loads((root / "protocol/examples/schema-attestation-startup.json").read_text())
    attestation.update(run_id=RUN_ID, backend_source_id="blackbox-mariadb")
    body = {key: attestation[key] for key in ("backend", "backend_source_id", "tables", "backend_facts")}
    attestation["canonical_attestation_digest"] = hashlib.sha256(
        b"db-monitoring/schema-attestation\x00" +
        json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    bootstrap = {"kind": "bootstrap_manifest", "run_id": RUN_ID,
                 "table_row_counts": [{"table": "benchbase.usertable", "row_count": 0}],
                 "canonical_state_digest": "944dd6136dbad7047e31af3b6419eb6a632d4dea64bec8159e93307163810bc2"}
    done = json.loads((root / "protocol/examples/session-done.json").read_text())
    for key, value in done.items():
        if isinstance(value, int): done[key] = 0
    done["run_id"] = RUN_ID
    session = done["session_id"].encode()
    done["rolling_correlation_digest"] = hashlib.sha256(
        b"db-monitoring/session-marker-seed\x00" + bytes.fromhex(RUN_ID.replace("-", "")) +
        len(session).to_bytes(4, "big") + session).hexdigest()
    final = dict(attestation, phase="FINAL")
    template = json.loads((root / "monitor/tests/fixtures/mariadb_debezium_3_6_1_lifecycle.json").read_text())[:3]
    schema = json.loads((root / "monitor/tests/fixtures/mariadb_debezium_3_6_1_snapshot_schema.json").read_text().replace("mariadb-main", "blackbox-mariadb"))
    for index, entry in enumerate(schema):
        entry[0] = f"{10 + index}-0"

    def fence(phase, number):
        entries = copy.deepcopy(template)
        txid = f"0-1-{number + 9}"
        for index, entry in enumerate(entries):
            entry[0] = f"{100 + number}-{index}"
            key, value = (json.loads(x) for x in entry[1])
            if index == 1:
                key.update(run_id=RUN_ID, phase=phase)
                value["after"].update(run_id=RUN_ID, phase=phase)
                value["source"].update(name="blackbox-mariadb", gtid=txid, pos=345 + number * 100)
                value["transaction"]["id"] = txid
            else:
                key["id"] = value["id"] = txid
            entry[1] = [json.dumps(key), json.dumps(value)]
        return entries

    def reply(client_records, cdc_records):
        streams = []
        for name, entries in ((f"blackbox:client:{RUN_ID}", client_records), ("cdc", cdc_records)):
            if entries:
                streams.append(resp_array([resp_bulk(name), resp_array([
                    resp_array([resp_bulk(sid), resp_array([resp_bulk(x) for x in fields])])
                    for sid, fields in entries])]))
        return resp_array(streams)

    for enabled, mode in ((False, "audited"), (True, "audited"), (True, "join-only")):
        xreads = 0
        statuses = []

        def handler(_connection_id, command):
            nonlocal xreads
            if command[0] == b"EVAL": return resp_integer(1)
            if command[0] == b"XADD":
                statuses.append(json.loads(command[-1]))
                return resp_bulk(f"{len(statuses)}-0")
            assert command[0] == b"XREAD", command
            xreads += 1
            if xreads == 1: return manifest_xread_reply()
            if xreads == 2:
                return reply([["2-0", ["payload", json.dumps(attestation)]],
                              ["3-0", ["payload", json.dumps(bootstrap)]]],
                             schema + fence("RUN_START", 0) + fence("LOAD_FINISHED", 1))
            if xreads == 3:
                assert any(s["lifecycle_state"] == "LOAD_FINISHED_SEEN" for s in statuses)
                time.sleep(0.35) # Worker must sample while the main thread waits on Redis.
                return reply([["4-0", ["payload", json.dumps(done)]],
                              ["5-0", ["payload", json.dumps(final)]]], fence("END_FENCE", 2))
            return b"*-1\r\n"

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metrics.json"
            with ScriptedRedis(handler) as server:
                result, _ = run_monitor(executable, server.port, 5000, extra_arguments=[
                    "--metrics-output", str(path), "--processing-mode", mode,
                    "--workload-rss-sampling", "on" if enabled else "off"])
            metrics = json.loads(path.read_text())
            assert result.returncode == 0, (output(result), metrics["diagnostics"])
            process = metrics["process"]
            assert metrics["evidence_reconciled"] is True
            assert process["workload_rss_sampling_enabled"] is enabled
            assert process["workload_rss_sampling_started"] is enabled
            assert process["workload_rss_sampling_valid"] is enabled
            assert process["workload_rss_sample_errors"] == 0
            if enabled:
                assert process["workload_sampled_peak_rss_kib"] > 0
                assert process["workload_rss_samples"] >= 3
                assert process["rss_sample_interval_ms"] == 100
            else:
                assert process["workload_sampled_peak_rss_kib"] is None
                assert process["workload_rss_samples"] == 0
    print("workload RSS: admission, idle sampling, final drain, and disabled mode verified")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--monitor", required=True)
    args = parser.parse_args()
    result = subprocess.run(
        [args.monitor, "--history-gc", "invalid"],
        capture_output=True, text=True, timeout=2.0, check=False,
    )
    assert result.returncode == 1, result
    assert "history-gc must be on or off" in output(result), output(result)
    check_ownership(args.monitor)
    check_claim_timeout(args.monitor)
    check_manifest_timeout(args.monitor)
    check_main_read_timeout(args.monitor)
    check_status_timeout(args.monitor)
    check_tpcc_adapter_enters_main_read(args.monitor)
    check_tpcc_roster_mismatch_is_invalid(args.monitor)
    check_join_only_requires_metrics(args.monitor)
    check_join_only_early_failure_artifact(args.monitor)
    check_capacity_options(args.monitor)
    check_rss_sampling_options(args.monitor)
    check_workload_rss_window(args.monitor)


if __name__ == "__main__":
    main()
