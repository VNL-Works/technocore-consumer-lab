"""Process and temp-dir hygiene: nothing a cycle or a worker started outlives it.

- `run_client` kills the client's process group after every cycle, not only on timeout.
- A worker that times out leaves no temp dir behind (or, with `--keep-root`, a kept dir that
  says why). This one needs the upstream tree, like the lab tests, and is skipped without it.
"""

import contextlib
import io
import json
import os
import shutil
import signal
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from consumer_lab import cli  # noqa: E402
from consumer_lab.scenarios import run_client  # noqa: E402

TREE = Path(os.environ.get("CONSUMER_LAB_UPSTREAM", "~/consumer-lab-upstream-0e47f77")).expanduser()
HAVE_TREE = (TREE / "src" / "store.py").is_file()


def alive(pid: int, grace: float = 2.0) -> bool:
    """Whether `pid` is still a running process after up to `grace` seconds.

    A killed process whose parent already exited stays a zombie until init reaps it; a zombie
    runs nothing, so it counts as gone.
    """
    deadline = time.monotonic() + grace
    while True:
        try:
            with open(f"/proc/{pid}/stat", encoding="ascii", errors="replace") as f:
                state = f.read().rsplit(")", 1)[1].split()[0]
        except (FileNotFoundError, ProcessLookupError, IndexError):
            return False
        if state == "Z":
            return False
        if time.monotonic() >= deadline:
            return True
        time.sleep(0.05)


def kill_quietly(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


@unittest.skipUnless(hasattr(os, "killpg") and Path("/proc").is_dir(), "needs POSIX process groups and /proc")
class ProcessGroupKilledAfterEveryCycle(unittest.TestCase):
    def test_background_helper_dies_after_a_normal_exit(self):
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-hygiene-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        pidfile = tmp / "helper.pid"
        # The client starts a helper in its own process group and exits 0 at once.
        argv = ["sh", "-c", f'sleep 30 & echo $! > "{pidfile}"; exit 0']
        exit_code, timed_out = run_client(argv, dict(os.environ), tmp / "client.stderr", 10.0)
        self.assertEqual((exit_code, timed_out), (0, False))
        pid = int(pidfile.read_text().strip())
        self.addCleanup(kill_quietly, pid)  # never leave the helper behind, even on failure
        self.assertFalse(alive(pid), f"helper {pid} outlived the cycle")


SLEEPER = "import time\ntime.sleep(8)\n"


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class WorkerTimeoutLeavesNoStrayDir(unittest.TestCase):
    TIMEOUT = 3

    def w0_dirs(self) -> set[str]:
        return {p.name for p in Path(tempfile.gettempdir()).glob("consumer-lab-w0-*")}

    def run_main(self, *extra: str) -> tuple[int, str, set[str]]:
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-hclient-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        script = tmp / "client.py"
        script.write_text(SLEEPER)
        before = self.w0_dirs()
        out, err = io.StringIO(), io.StringIO()
        argv = ["run", "--upstream", str(TREE), "--client", f"{sys.executable} -B {script}",
                "--scenario", "W0", *extra]
        with mock.patch.object(cli, "WORKER_TIMEOUT", self.TIMEOUT), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(argv)
        new = self.w0_dirs() - before
        for name in new:  # whatever this run left, the test removes
            self.addCleanup(shutil.rmtree, Path(tempfile.gettempdir()) / name, True)
        return code, err.getvalue(), new

    def test_timeout_without_keep_root_removes_the_dir(self):
        code, err, new = self.run_main()
        self.assertEqual(code, 2, err)
        self.assertIn(f"W0: worker did not finish within {self.TIMEOUT}s", err)
        self.assertEqual(new, set(), err)

    def test_timeout_with_keep_root_keeps_a_dir_that_says_why(self):
        code, err, new = self.run_main("--keep-root")
        self.assertEqual(code, 2, err)
        kept = [ln.removeprefix("kept ") for ln in err.splitlines() if ln.startswith("kept ")]
        self.assertEqual(len(kept), 1, err)
        kept_dir = Path(kept[0])
        self.addCleanup(shutil.rmtree, kept_dir, True)
        self.assertEqual({kept_dir.name}, new, err)
        result = json.loads((kept_dir / "result.json").read_text())
        self.assertEqual(
            result,
            {"id": "W0", "exit_code": 2, "setup_error": f"W0: worker did not finish within {self.TIMEOUT}s"},
        )


if __name__ == "__main__":
    unittest.main()
