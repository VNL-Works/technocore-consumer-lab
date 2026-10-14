"""Process and temp-dir hygiene: nothing a cycle or a worker started outlives it.

- `run_client` kills the client's process group after every cycle, not only on timeout.
- A worker that times out leaves no temp dir behind (or, with `--keep-root`, a kept dir that
  says why). This one needs the upstream tree, like the lab tests, and is skipped without it.
- An in-group SIGINT to the worker does not stop the group kill; a worker killed outright
  still leaves, with `--keep-root`, a kept dir that says why (also needs the tree).
- v0.2.4: with `--keep-root`, result.json is never written through a link the client planted
  or into a workdir it swapped for a link, and the parent says when it did not write one
  (silent only for the worker's own file after exit -2); the asynchronous terminating signals
  a client sends its worker (SIGTERM, SIGALRM, a real-time signal, or a SIGINT that lands just
  before the kill) are deferred until the group is killed; `run_client` off the main thread is
  a setup error.
"""

import contextlib
import io
import json
import os
import shutil
import signal
import subprocess
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
        helper_alive = alive(pid)
        if helper_alive:  # never leave the helper behind; never signal a pid already seen dead
            self.addCleanup(kill_quietly, pid)
        self.assertFalse(helper_alive, f"helper {pid} outlived the cycle")


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


SIGINT_CLIENT = r'''
import os, signal, time
# Starts an in-group helper that records its pid, SIGINTs the worker (this client's parent)
# while the worker waits for this cycle, then sleeps; the client sleeps too.
worker = os.getppid()
if os.fork() == 0:
    with open(HELPER_PID, "w") as f:
        f.write(str(os.getpid()))
    time.sleep(0.3)
    os.kill(worker, signal.SIGINT)
    time.sleep(30)
    os._exit(0)
with open(CLIENT_PID, "w") as f:
    f.write(str(os.getpid()))
time.sleep(20)
'''

KILL_WORKER = r'''
import os, signal
# Kills its worker outright: the worker cannot write its own result.json.
os.kill(os.getppid(), signal.SIGKILL)
'''


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class SignalledWorker(unittest.TestCase):
    def w0_dirs(self) -> set[str]:
        return {p.name for p in Path(tempfile.gettempdir()).glob("consumer-lab-w0-*")}

    def run_kit(self, source: str, *extra: str) -> tuple[int, dict | None, str, Path]:
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-sclient-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        script = tmp / "client.py"
        script.write_text(f"HELPER_PID = {str(tmp / 'helper.pid')!r}\nCLIENT_PID = {str(tmp / 'client.pid')!r}\n" + source)
        out = tmp / "report.json"
        before = self.w0_dirs()
        cmd = [sys.executable, "-m", "consumer_lab", "run", "--upstream", str(TREE),
               "--client", f"{sys.executable} -B {script}", "--scenario", "W0", "--json", str(out), *extra]
        proc = subprocess.run(cmd, cwd=REPO, capture_output=True, timeout=300)
        for name in self.w0_dirs() - before:  # whatever this run left, the test removes
            self.addCleanup(shutil.rmtree, Path(tempfile.gettempdir()) / name, True)
        report = json.loads(out.read_text()) if out.exists() else None
        output = (proc.stdout + proc.stderr).decode("utf-8", errors="replace")
        return proc.returncode, report, output, tmp

    def test_sigint_from_the_group_still_kills_the_group(self):
        code, report, output, tmp = self.run_kit(SIGINT_CLIENT)
        pids = [int((tmp / name).read_text()) for name in ("helper.pid", "client.pid")]
        seen = {pid: alive(pid) for pid in pids}
        for pid, was_alive in seen.items():
            if was_alive:  # never leave it behind; never signal a pid already seen dead
                self.addCleanup(kill_quietly, pid)
        self.assertEqual(seen, {pid: False for pid in pids}, output)
        self.assertEqual(code, 2, output)
        self.assertEqual(report["setup_error"], "W0: worker exited -2: killed by signal 2", output)

    def test_keep_root_with_a_killed_worker_keeps_a_dir_that_says_why(self):
        code, report, output, _ = self.run_kit(KILL_WORKER, "--keep-root")
        setup_error = "W0: worker exited -9: killed by signal 9"
        self.assertEqual(code, 2, output)
        self.assertEqual(report["setup_error"], setup_error, output)
        kept = [ln.removeprefix("kept ") for ln in output.splitlines() if ln.startswith("kept ")]
        self.assertEqual(len(kept), 1, output)
        kept_dir = Path(kept[0])
        self.addCleanup(shutil.rmtree, kept_dir, True)
        result = json.loads((kept_dir / "result.json").read_text())
        self.assertEqual(result, {"id": "W0", "exit_code": 2, "setup_error": setup_error})



def remove_entry(path: Path) -> None:
    """Remove what a run left at `path`: a link itself (never its target), or a dir tree."""
    if path.is_symlink():
        path.unlink()
    else:
        shutil.rmtree(path, ignore_errors=True)


NOT_WRITING = "not writing result.json in {}: it was replaced or already exists"

# Set OUTSIDE before this source. Plants result.json in its workdir (known from TC_EVENTS) as a
# link to OUTSIDE, which does not exist, then kills its worker outright: only the parent can
# write result.json now.
PLANT_LINK_THEN_KILL = r"""
import os, signal
os.symlink(OUTSIDE, os.path.join(os.path.dirname(os.environ["TC_EVENTS"]), "result.json"))
os.kill(os.getppid(), signal.SIGKILL)
"""

# Set OUTSIDE before this source. Plants result.json as a link to OUTSIDE, an existing file, then
# exits normally: the worker writes its own result.json in its finally.
PLANT_LINK_THEN_EXIT = r"""
import os
os.symlink(OUTSIDE, os.path.join(os.path.dirname(os.environ["TC_EVENTS"]), "result.json"))
"""

# Set AWAY and OUTSIDE before this source. Renames its workdir to AWAY, puts a link to the dir
# OUTSIDE in its place, then kills its worker outright.
SWAP_WORKDIR_THEN_KILL = r"""
import os, signal
workdir = os.path.dirname(os.environ["TC_EVENTS"])
os.rename(workdir, AWAY)
os.symlink(OUTSIDE, workdir)
os.kill(os.getppid(), signal.SIGKILL)
"""

# Plants result.json as a regular file of its own, then kills its worker outright.
PLANT_FILE_THEN_KILL = r"""
import os, signal
with open(os.path.join(os.path.dirname(os.environ["TC_EVENTS"]), "result.json"), "w") as f:
    f.write("PLANTED\n")
os.kill(os.getppid(), signal.SIGKILL)
"""

# Set OUTSIDE and HELPER_PID before this source. Plants result.json as a hard link to OUTSIDE (a
# regular file, two links), then an in-group helper SIGINTs the worker: exit -2.
HARDLINK_THEN_SIGINT = r"""
import os, signal, time
os.link(OUTSIDE, os.path.join(os.path.dirname(os.environ["TC_EVENTS"]), "result.json"))
worker = os.getppid()
if os.fork() == 0:
    with open(HELPER_PID, "w") as f:
        f.write(str(os.getpid()))
    time.sleep(0.3)
    os.kill(worker, signal.SIGINT)
    time.sleep(30)
    os._exit(0)
time.sleep(2)
"""


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class KeptFilesNeverWrittenThroughLinks(unittest.TestCase):
    """--keep-root: result.json is created only as a new file in the real workdir (v0.2.4)."""

    def w0_entries(self) -> set[str]:
        return {p.name for p in Path(tempfile.gettempdir()).glob("consumer-lab-w0-*")}

    def run_kit(self, tmp: Path, source: str) -> tuple[int, dict | None, str, list[str]]:
        script = tmp / "client.py"
        script.write_text(source)
        out = tmp / "report.json"
        before = self.w0_entries()
        cmd = [sys.executable, "-m", "consumer_lab", "run", "--upstream", str(TREE),
               "--client", f"{sys.executable} -B {script}", "--scenario", "W0", "--json", str(out),
               "--keep-root"]
        proc = subprocess.run(cmd, cwd=REPO, capture_output=True, timeout=300)
        for name in self.w0_entries() - before:  # whatever this run left (a link, too), the test removes
            self.addCleanup(remove_entry, Path(tempfile.gettempdir()) / name)
        report = json.loads(out.read_text()) if out.exists() else None
        output = (proc.stdout + proc.stderr).decode("utf-8", errors="replace")
        kept = [ln.removeprefix("kept ") for ln in output.splitlines() if ln.startswith("kept ")]
        return proc.returncode, report, output, kept

    def new_tmp(self) -> Path:
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-lclient-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        return tmp

    def test_parent_never_writes_through_a_planted_link(self):
        tmp = self.new_tmp()
        outside = tmp / "outside" / "target.json"
        outside.parent.mkdir()
        code, report, output, kept = self.run_kit(tmp, f"OUTSIDE = {str(outside)!r}\n" + PLANT_LINK_THEN_KILL)
        self.assertEqual(code, 2, output)
        self.assertEqual(report["setup_error"], "W0: worker exited -9: killed by signal 9", output)
        self.assertEqual(len(kept), 1, output)
        link = Path(kept[0]) / "result.json"
        self.assertFalse(os.path.lexists(outside), output)  # nothing created through the link
        self.assertTrue(link.is_symlink(), output)  # the planted link is left as it was
        self.assertEqual(os.readlink(link), str(outside))
        self.assertIn(NOT_WRITING.format(kept[0]), output.splitlines())

    def test_worker_never_overwrites_through_a_planted_link(self):
        tmp = self.new_tmp()
        outside = tmp / "outside.txt"
        outside.write_text("ORIGINAL\n")
        code, report, output, kept = self.run_kit(tmp, f"OUTSIDE = {str(outside)!r}\n" + PLANT_LINK_THEN_EXIT)
        self.assertIsNotNone(report, output)
        self.assertIsNone(report["setup_error"], output)
        self.assertEqual(code, 1, output)  # W0 judged: this client posts nothing
        self.assertEqual(outside.read_text(), "ORIGINAL\n", output)
        self.assertEqual(len(kept), 1, output)
        self.assertTrue((Path(kept[0]) / "result.json").is_symlink(), output)
        self.assertIn(NOT_WRITING.format(kept[0]), output.splitlines())

    def test_parent_never_writes_into_a_swapped_workdir(self):
        tmp = self.new_tmp()
        outside = tmp / "outside"
        outside.mkdir()
        source = f"AWAY = {str(tmp / 'away')!r}\nOUTSIDE = {str(outside)!r}\n" + SWAP_WORKDIR_THEN_KILL
        code, report, output, kept = self.run_kit(tmp, source)
        self.assertEqual(code, 2, output)
        self.assertEqual(report["setup_error"], "W0: worker exited -9: killed by signal 9", output)
        self.assertEqual(list(outside.iterdir()), [], output)  # no result.json in the outside dir
        self.assertFalse((tmp / "away" / "result.json").exists(), output)
        self.assertEqual(len(kept), 1, output)
        self.assertTrue(Path(kept[0]).is_symlink(), output)
        self.assertIn(NOT_WRITING.format(kept[0]), output.splitlines())

    def test_parent_reports_a_planted_regular_file_after_a_kill(self):
        # Not exit -2: the worker's own `finally` never ran, so the file is not its own and the
        # parent says it did not write, then where the kept dir is.
        tmp = self.new_tmp()
        code, report, output, kept = self.run_kit(tmp, PLANT_FILE_THEN_KILL)
        self.assertEqual(code, 2, output)
        self.assertEqual(report["setup_error"], "W0: worker exited -9: killed by signal 9", output)
        self.assertEqual(len(kept), 1, output)
        self.assertIn(NOT_WRITING.format(kept[0]), output.splitlines())
        planted = Path(kept[0]) / "result.json"
        self.assertFalse(planted.is_symlink())
        self.assertEqual(planted.read_text(), "PLANTED\n")  # left as the client wrote it

    def test_hardlinked_result_json_after_sigint_is_not_taken_as_the_workers(self):
        tmp = self.new_tmp()
        outside = tmp / "outside.txt"
        outside.write_text("ORIGINAL\n")
        helper_pid = tmp / "helper.pid"
        source = f"OUTSIDE = {str(outside)!r}\nHELPER_PID = {str(helper_pid)!r}\n" + HARDLINK_THEN_SIGINT
        code, report, output, kept = self.run_kit(tmp, source)
        pid = int(helper_pid.read_text())
        helper_alive = alive(pid)
        if helper_alive:  # never leave it behind; never signal a pid already seen dead
            self.addCleanup(kill_quietly, pid)
        self.assertFalse(helper_alive, output)
        self.assertEqual(code, 2, output)
        self.assertEqual(report["setup_error"], "W0: worker exited -2: killed by signal 2", output)
        self.assertEqual(outside.read_text(), "ORIGINAL\n", output)  # never written through
        self.assertEqual(outside.stat().st_nlink, 2, output)
        # Exit -2 but two links: not the worker's own file, so the parent is not quiet. Its
        # lines come last, before its setup error: `not writing`, then one `kept`. (The worker
        # printed its own `not writing` and `kept` earlier.)
        lines = output.splitlines()
        at = lines.index("setup error: W0: worker exited -2: killed by signal 2")
        self.assertEqual(len(set(kept)), 1, output)
        self.assertEqual(lines[at - 2:at], [NOT_WRITING.format(kept[0]), f"kept {kept[0]}"], output)
        self.assertEqual(len(kept), 2, output)  # one from the worker, one from the parent


SIGTERM_CLIENT = r"""
import os, signal, time
# Starts an in-group helper that records its pid, SIGTERMs the worker (this client's parent)
# while the worker waits for this cycle, then sleeps; the client exits after the signal was sent.
worker = os.getppid()
if os.fork() == 0:
    with open(HELPER_PID, "w") as f:
        f.write(str(os.getpid()))
    time.sleep(0.3)
    os.kill(worker, signal.SIGTERM)
    time.sleep(30)
    os._exit(0)
with open(CLIENT_PID, "w") as f:
    f.write(str(os.getpid()))
time.sleep(2)
"""


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class DeferredSignalFromTheGroup(unittest.TestCase):
    w0_dirs = SignalledWorker.w0_dirs
    run_kit = SignalledWorker.run_kit

    def test_sigterm_from_the_group_is_deferred_until_the_group_is_killed(self):
        code, report, output, tmp = self.run_kit(SIGTERM_CLIENT)
        pids = [int((tmp / name).read_text()) for name in ("helper.pid", "client.pid")]
        seen = {pid: alive(pid) for pid in pids}
        for pid, was_alive in seen.items():
            if was_alive:  # never leave it behind; never signal a pid already seen dead
                self.addCleanup(kill_quietly, pid)
        self.assertEqual(seen, {pid: False for pid in pids}, output)
        self.assertEqual(code, 2, output)
        self.assertEqual(report["setup_error"], "W0: worker exited -15: killed by signal 15", output)

    def check_deferred(self, expr: str, signum: int):
        source = SIGTERM_CLIENT.replace("signal.SIGTERM", expr)
        code, report, output, tmp = self.run_kit(source)
        pids = [int((tmp / name).read_text()) for name in ("helper.pid", "client.pid")]
        seen = {pid: alive(pid) for pid in pids}
        for pid, was_alive in seen.items():
            if was_alive:  # never leave it behind; never signal a pid already seen dead
                self.addCleanup(kill_quietly, pid)
        self.assertEqual(seen, {pid: False for pid in pids}, output)
        self.assertEqual(code, 2, output)
        self.assertEqual(report["setup_error"], f"W0: worker exited -{signum}: killed by signal {signum}", output)

    def test_sigalrm_from_the_group_is_deferred_until_the_group_is_killed(self):
        self.check_deferred("signal.SIGALRM", int(signal.SIGALRM))

    def test_realtime_signal_from_the_group_is_deferred_until_the_group_is_killed(self):
        self.check_deferred("signal.SIGRTMIN", int(signal.SIGRTMIN))

    def test_sigint_with_keep_root_keeps_the_workers_own_result_with_one_kept_line(self):
        # The worker writes its own result.json (KeyboardInterrupt); the parent's O_EXCL create
        # then finds a regular file and stays silent: one `kept` line, no `not writing` line.
        source = SIGTERM_CLIENT.replace("signal.SIGTERM", "signal.SIGINT").replace("SIGTERMs", "SIGINTs")
        code, report, output, tmp = self.run_kit(source, "--keep-root")
        pids = [int((tmp / name).read_text()) for name in ("helper.pid", "client.pid")]
        seen = {pid: alive(pid) for pid in pids}
        for pid, was_alive in seen.items():
            if was_alive:  # never leave it behind; never signal a pid already seen dead
                self.addCleanup(kill_quietly, pid)
        kept = [ln.removeprefix("kept ") for ln in output.splitlines() if ln.startswith("kept ")]
        for d in set(kept):
            self.addCleanup(shutil.rmtree, d, True)
        self.assertEqual(seen, {pid: False for pid in pids}, output)
        self.assertEqual(code, 2, output)
        self.assertEqual(report["setup_error"], "W0: worker exited -2: killed by signal 2", output)
        self.assertEqual(len(kept), 1, output)
        self.assertFalse([ln for ln in output.splitlines() if ln.startswith("not writing result.json")], output)
        result_json = Path(kept[0]) / "result.json"
        self.assertFalse(result_json.is_symlink())
        self.assertEqual(json.loads(result_json.read_text()), {"error": "KeyboardInterrupt()", "exit_code": 1, "id": "W0"})


@unittest.skipUnless(hasattr(os, "killpg") and Path("/proc").is_dir(), "needs POSIX process groups and /proc")
class RunClientDefersSignals(unittest.TestCase):
    def test_sigint_just_before_the_group_kill_does_not_skip_it(self):
        # Hits the window deterministically: the SIGINT is raised in this process after the
        # wait returned, as the first thing the group kill does, before the real killpg.
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-hygiene-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        pidfile = tmp / "helper.pid"
        argv = ["sh", "-c", f'sleep 30 & echo $! > "{pidfile}"; exit 0']
        real_killpg = os.killpg
        calls = []

        def sigint_then_killpg(pgid, sig):
            calls.append(sig)
            os.kill(os.getpid(), signal.SIGINT)
            real_killpg(pgid, sig)

        before = signal.getsignal(signal.SIGINT)
        with mock.patch.object(os, "killpg", side_effect=sigint_then_killpg):
            with self.assertRaises(KeyboardInterrupt):  # re-delivered after the kill
                run_client(argv, dict(os.environ), tmp / "client.stderr", 10.0)
        self.assertEqual(calls, [signal.SIGKILL])
        pid = int(pidfile.read_text().strip())
        helper_alive = alive(pid)
        if helper_alive:  # never leave the helper behind; never signal a pid already seen dead
            self.addCleanup(kill_quietly, pid)
        self.assertFalse(helper_alive, f"helper {pid} outlived the cycle")
        self.assertIs(signal.getsignal(signal.SIGINT), before)  # the previous handler is back

    def test_off_the_main_thread_is_a_setup_error(self):
        from consumer_lab.server import SetupError

        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-hygiene-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        marker = tmp / "ran"
        got: list[BaseException | tuple] = []

        def target():
            try:
                got.append(run_client(["sh", "-c", f'touch "{marker}"'], dict(os.environ), tmp / "e", 10.0))
            except BaseException as exc:  # noqa: BLE001 - inspected below
                got.append(exc)

        import threading

        t = threading.Thread(target=target)
        t.start()
        t.join(30)
        self.assertEqual(len(got), 1, got)
        self.assertIsInstance(got[0], SetupError)
        self.assertIn("main thread", str(got[0]))
        self.assertFalse(marker.exists())  # no client was started


if __name__ == "__main__":
    unittest.main()
