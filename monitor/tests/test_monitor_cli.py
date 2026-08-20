#!/usr/bin/env python3

import argparse
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


def check_incomplete_adapter_is_rejected(executable):
    xreads = 0

    def handler(_connection_id, command):
        nonlocal xreads
        if command[0] == b"EVAL":
            return resp_integer(1)
        if command[0] == b"XREAD":
            xreads += 1
            assert xreads == 1
            return manifest_xread_reply(tpcc_manifest_payload())
        if command[0] == b"XADD":
            return resp_bulk("1-0")
        raise AssertionError(f"unexpected Redis command: {command!r}")

    with ScriptedRedis(handler) as server:
        result, _ = run_monitor(executable, server.port, 1000)
        require_invalid(result, "BRANCH_CONTRACT_MISMATCH")
        assert xreads == 1
        assert any(command[0] == b"XADD" for command in server.commands)
    print("adapter capability: incomplete TPC-C runtime rejected before CDC admission")


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
        assert (
            measurement["limits"]["joiner"]["max_pending_client_bytes"]
            == 268435456
        )
        assert (
            measurement["limits"]["joiner"]["max_retained_identity_bytes"]
            == 536870912
        )
        assert_no_audited_result(measurement)
        assert "outcome=INVALID" not in output(result)
    print("join-only early failure: failed measurement written without verdict")


def check_joiner_byte_limit_options(executable):
    for option in ("--max-pending-client-bytes", "--max-retained-identity-bytes"):
        for invalid in ("0", "18446744073709551616"):
            result = subprocess.run(
                monitor_command(executable, 1, 1000, [option, invalid]),
                capture_output=True,
                text=True,
                timeout=2.0,
                check=False,
            )
            assert result.returncode == 1, result
            assert "must be a positive decimal byte count" in output(result), output(
                result
            )

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
                    "--max-pending-client-bytes",
                    "1073741824",
                    "--max-retained-identity-bytes",
                    "2147483648",
                ],
            )
            assert result.returncode == 1, result
        measurement = json.loads(metrics_path.read_text(encoding="utf-8"))
        assert (
            measurement["limits"]["joiner"]["max_pending_client_bytes"]
            == 1073741824
        )
        assert (
            measurement["limits"]["joiner"]["max_retained_identity_bytes"]
            == 2147483648
        )
        assert_no_audited_result(measurement)
    print("joiner byte capacities: defaults, overrides, zero, and overflow validated")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--monitor", required=True)
    args = parser.parse_args()
    check_ownership(args.monitor)
    check_claim_timeout(args.monitor)
    check_manifest_timeout(args.monitor)
    check_main_read_timeout(args.monitor)
    check_status_timeout(args.monitor)
    check_incomplete_adapter_is_rejected(args.monitor)
    check_join_only_requires_metrics(args.monitor)
    check_join_only_early_failure_artifact(args.monitor)
    check_joiner_byte_limit_options(args.monitor)


if __name__ == "__main__":
    main()
