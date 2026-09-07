"""Regression tests for EXIT cleanup; only test-owned child sessions are used."""
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class CleanupTest(unittest.TestCase):
    def run_cleanup(self, ignore_term, slot):
        source = (ROOT / "scripts/e2e.sh").read_text()
        cleanup = source[source.index("cleanup_owned_processes() {"):source.index("trap cleanup_owned_processes EXIT")]
        with tempfile.TemporaryDirectory(prefix="e2e-cleanup-") as directory:
            child = '''import signal, time
from pathlib import Path
def stop(*args):
    time.sleep(0.3)
    Path("stopped").touch()
    raise SystemExit(0)
signal.signal(signal.SIGTERM, SIGNAL_HANDLER)
Path("ready").touch()
while True: time.sleep(0.05)
'''.replace("SIGNAL_HANDLER", "signal.SIG_IGN" if ignore_term else "stop")
            program = '''set -eu
PHASE_SESSION_ID=
MONITOR_SESSION_ID=
BOOTSTRAP_SCAN_FILE=
BLACKBOX_TERMINATION_GRACE_SECONDS=1
blackbox_session_has_processes() { pgrep --session "$1" >/dev/null 2>&1; }
''' + cleanup + '''
trap cleanup_owned_processes EXIT
setsid python3 -c CHILD &
SLOT=$!
child_pid=$!
for ((i=0; i<200; i++)); do
  [[ -f ready ]] && break
  sleep 0.01
done
[[ -f ready ]]
cleanup_owned_processes
! kill -0 "$child_pid" 2>/dev/null
[[ -z $PHASE_SESSION_ID && -z $MONITOR_SESSION_ID ]]
cleanup_owned_processes
exit 23
'''.replace("CHILD", shlex.quote(child)).replace("SLOT", slot)
            result = subprocess.run(["bash", "-c", program], cwd=directory,
                                    capture_output=True, text=True, timeout=8)
            self.assertEqual(result.returncode, 23, result.stderr)
            self.assertEqual((Path(directory) / "stopped").exists(), not ignore_term)

    def test_waits_for_delayed_signal_handler(self):
        for slot in ("PHASE_SESSION_ID", "MONITOR_SESSION_ID"):
            with self.subTest(slot=slot):
                self.run_cleanup(False, slot)

    def test_kills_and_reaps_child_ignoring_term(self):
        self.run_cleanup(True, "MONITOR_SESSION_ID")


if __name__ == "__main__":
    unittest.main()
