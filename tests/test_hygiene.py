"""Process and temp-dir hygiene: nothing a cycle or a worker started outlives it.

- `run_client` kills the client's process group after every cycle, not only on timeout.
- A worker that times out leaves no temp dir behind (or, with `--keep-root`, a kept dir that
  says why). This one needs the upstream tree, like the lab tests, and is skipped without it.
- An in-group SIGINT to the worker does not stop the group kill; a worker killed outright
  still leaves, with `--keep-root`, a kept dir that says why (also needs the tree).
- v0.2.4: with `--keep-root`, result.json is never written through a link the client planted
  or into a workdir it swapped for a link, and the parent says when it did not write one
  (silent only for the worker's own file after exit -2); the asynchronous terminating signals
  and stop signals a client sends its worker (SIGTERM, SIGALRM, SIGTSTP, a real-time signal, or
  a SIGINT that lands just before the kill) are deferred until the group is killed (a stop
  signal is then dropped with a kit line, and cannot mask a terminating one), and SIGINT's
  handler is restored last; `run_client` off the main thread is a setup error.
- The deferred signals stay blocked in the main thread while their handlers are restored, and
  the kit's own threads (the upstream server's, the fault proxy's and the threads they start)
  block them for their whole life, so one sent to the process then stays pending and reaches
  its restored handler once (a KeyboardInterrupt it raises comes after the re-delivery); a
  partial line the client leaves on its worker's stderr never joins a kit line (`kept`, the
  stop-signal line, the parent's lines after its echo).
- A client that replaces its workdir with a link to an outside dir and exits 0 ends the run
  with a setup error right after that cycle (reader and writer); the kit creates nothing there,
  not even upstream's shutdown counter flush, and `--keep-root` writes no result.json there.
  One that swaps and then SIGINTs its worker (the cycle then ends in KeyboardInterrupt, before
  the check) ends with the worker's -2, and the kit creates nothing there either.
- C12: replacing only the store root `<workdir>/root` with a link is caught the same way (reader
  and writer); SIGTERM or SIGHUP to `run` during a cycle stops the worker, removes the temp dirs,
  writes the report as a setup error and ends the parent by that signal, leaving no process.
"""

import contextlib
import io
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from consumer_lab import cli  # noqa: E402
from consumer_lab.scenarios import DEFERRED_SIGNALS, run_client  # noqa: E402

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


# Set AWAY and OUTSIDE before this source. Renames its workdir to AWAY, puts a link to the dir
# OUTSIDE in its place, then exits 0 (the worker is not killed: the kit would go on by path).
SWAP_WORKDIR_THEN_EXIT = r"""
import os
workdir = os.path.dirname(os.environ["TC_EVENTS"])
os.rename(workdir, AWAY)
os.symlink(OUTSIDE, workdir)
"""

# Appended to SWAP_WORKDIR_THEN_EXIT: then SIGINT the worker (this client's parent), whose
# deferred re-delivery after the cycle raises KeyboardInterrupt out of run_client.
THEN_SIGINT_THE_WORKER = r"""
import signal
os.kill(os.getppid(), signal.SIGINT)
"""


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class WorkdirReplacedDuringACycle(unittest.TestCase):
    """A client that swaps its workdir for a link to an outside dir and exits 0 ends the run
    with a setup error right after that cycle, and the kit creates nothing in the outside dir
    (no root/, root/.counters, root/.counters.lock, events.jsonl, state/, client.stderr,
    result.json: its listing at every depth must stay empty)."""

    def check(self, scenario: str, *extra: str, sigint: bool = False) -> str:
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-wclient-"))
        self.addCleanup(shutil.rmtree, tmp, True)  # holds AWAY (the renamed workdir) and OUTSIDE
        outside = tmp / "outside"
        outside.mkdir()
        away = tmp / "away"
        script = tmp / "client.py"
        source = f"AWAY = {str(away)!r}\nOUTSIDE = {str(outside)!r}\n" + SWAP_WORKDIR_THEN_EXIT
        script.write_text(source + (THEN_SIGINT_THE_WORKER if sigint else ""))
        out = tmp / "report.json"
        pattern = f"consumer-lab-{scenario.lower()}-*"
        before = {p.name for p in Path(tempfile.gettempdir()).glob(pattern)}
        cmd = [sys.executable, "-m", "consumer_lab", "run", "--upstream", str(TREE),
               "--client", f"{sys.executable} -B {script}", "--scenario", scenario, "--json", str(out),
               *extra]
        proc = subprocess.run(cmd, cwd=REPO, capture_output=True, timeout=300)
        left = sorted({p.name for p in Path(tempfile.gettempdir()).glob(pattern)} - before)
        for name in left:  # the link the client left at the workdir path; never its target
            self.addCleanup(remove_entry, Path(tempfile.gettempdir()) / name)
        listing = sorted(str(p.relative_to(outside)) for p in outside.rglob("*"))
        output = (proc.stdout + proc.stderr).decode("utf-8", errors="replace")
        detail = f"{output}\noutside dir holds: {listing}"
        self.assertEqual(listing, [], detail)  # nothing created through the link, at any depth
        self.assertTrue(away.is_dir(), detail)  # the swap did happen
        self.assertEqual(proc.returncode, 2, detail)
        self.assertEqual(len(left), 1, detail)
        workdir = Path(tempfile.gettempdir()) / left[0]
        self.assertTrue(workdir.is_symlink(), detail)
        self.assertEqual(os.readlink(workdir), str(outside), detail)
        report = json.loads(out.read_text())
        if sigint:  # the check never ran: the worker ended on the client's signal
            expected = f"{scenario}: worker exited -2: killed by signal 2"
        else:
            expected = f"{scenario}: worker exited 2: workdir {workdir} was replaced during the client cycle"
        self.assertEqual(report["setup_error"], expected, detail)
        return output

    def test_reader_stops_when_the_workdir_is_replaced(self):
        self.check("S1")

    def test_writer_stops_when_the_workdir_is_replaced(self):
        self.check("W0")

    def test_keep_root_writes_no_result_json_into_a_replaced_workdir(self):
        output = self.check("W0", "--keep-root")  # the outside dir stays empty: no result.json
        kept = [ln.removeprefix("kept ") for ln in output.splitlines() if ln.startswith("kept ")]
        self.assertEqual(len(kept), 1, output)  # the worker's own; the parent writes none on exit 2
        self.assertIn(NOT_WRITING.format(kept[0]), output.splitlines())

    def test_reader_sigint_after_the_swap_creates_nothing_outside(self):
        self.check("S1", sigint=True)

    def test_writer_sigint_after_the_swap_creates_nothing_outside(self):
        self.check("W0", sigint=True)


# Set WHERE, AWAY and OUTSIDE before this source. Keeps its workdir but renames the store root
# `<workdir>/root` to AWAY and puts a link to the dir OUTSIDE in its place, then exits 0; WHERE
# gets the workdir's path.
SWAP_ROOT_THEN_EXIT = r"""
import os
workdir = os.path.dirname(os.environ["TC_EVENTS"])
with open(WHERE, "w") as f:
    f.write(workdir)
os.rename(os.path.join(workdir, "root"), AWAY)
os.symlink(OUTSIDE, os.path.join(workdir, "root"))
"""


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class StoreRootReplacedDuringACycle(unittest.TestCase):
    """v0.2.4 C12: a client that keeps its workdir but replaces `<workdir>/root` with a link to
    an outside dir and exits 0 ends the run with a setup error right after that cycle, naming the
    root; upstream stops without its shutdown hooks and nothing is created in the outside dir."""

    def check(self, scenario: str) -> None:
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-rclient-"))
        self.addCleanup(shutil.rmtree, tmp, True)  # holds AWAY (the renamed root) and OUTSIDE
        outside = tmp / "outside"
        outside.mkdir()
        away = tmp / "away"
        where = tmp / "workdir.txt"
        script = tmp / "client.py"
        script.write_text(
            f"WHERE = {str(where)!r}\nAWAY = {str(away)!r}\nOUTSIDE = {str(outside)!r}\n" + SWAP_ROOT_THEN_EXIT
        )
        out = tmp / "report.json"
        pattern = f"consumer-lab-{scenario.lower()}-*"
        before = {p.name for p in Path(tempfile.gettempdir()).glob(pattern)}
        cmd = [sys.executable, "-m", "consumer_lab", "run", "--upstream", str(TREE),
               "--client", f"{sys.executable} -B {script}", "--scenario", scenario, "--json", str(out)]
        proc = subprocess.run(cmd, cwd=REPO, capture_output=True, timeout=300)
        left = sorted({p.name for p in Path(tempfile.gettempdir()).glob(pattern)} - before)
        for name in left:  # whatever this run left (a link, too), the test removes; never a target
            self.addCleanup(remove_entry, Path(tempfile.gettempdir()) / name)
        listing = sorted(str(p.relative_to(outside)) for p in outside.rglob("*"))
        output = (proc.stdout + proc.stderr).decode("utf-8", errors="replace")
        detail = f"{output}\noutside dir holds: {listing}"
        self.assertEqual(listing, [], detail)  # nothing created through the link, at any depth
        self.assertTrue(away.is_dir(), detail)  # the swap did happen
        self.assertEqual(proc.returncode, 2, detail)
        self.assertEqual(left, [], detail)  # the worker removed its workdir (and the link in it)
        workdir = where.read_text()
        expected = f"{scenario}: worker exited 2: store root {workdir}/root was replaced during the client cycle"
        self.assertEqual(json.loads(out.read_text())["setup_error"], expected, detail)

    def test_reader_stops_when_the_store_root_is_replaced(self):
        self.check("S1")

    def test_writer_stops_when_the_store_root_is_replaced(self):
        self.check("W0")


# Set STARTED before this source. Says it started, then sleeps through most of its cycle.
SLEEPY_CLIENT = r"""
import time
open(STARTED, "w").close()
time.sleep(3)
"""


def processes_with(*needles: str) -> list[int]:
    """Pids of the running processes (zombies excluded: empty cmdline) whose command line
    contains any of `needles`."""
    found = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or int(entry.name) == os.getpid():
            continue
        try:
            cmdline = (entry / "cmdline").read_bytes()
        except OSError:
            continue
        text = cmdline.replace(b"\0", b" ").decode("utf-8", errors="replace")
        if cmdline and any(n in text for n in needles):
            found.append(int(entry.name))
    return found


def _default_stop_signals() -> None:  # in the kit's child: as from a shell, not ignored
    for sig in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, signal.SIG_DFL)


@unittest.skipUnless(HAVE_TREE and Path("/proc").is_dir(), f"needs /proc and the upstream tree at {TREE}")
class ParentSignalled(unittest.TestCase):
    """v0.2.4 C12: SIGTERM or SIGHUP to `run` while a client cycle runs: the parent stops the
    worker (which ends after its cycle, the client's group killed), removes its temp dirs,
    writes the report as a setup error and ends by that signal; nothing outlives it."""

    # Seconds the parent may take after the signal (getattr: the module also loads on a kit
    # without the constant, so such a kit fails these tests rather than the import).
    BOUND = getattr(cli, "WORKER_STOP_GRACE", 45) + 15

    def check(self, signum: int) -> None:
        name = signal.Signals(signum).name
        base = Path(tempfile.mkdtemp(prefix="consumer-lab-psig-"))
        self.addCleanup(shutil.rmtree, base, True)
        kit_tmp = base / "tmp"  # the kit's TMPDIR: its workdir and output dir go here
        kit_tmp.mkdir()
        files = base / "files"
        files.mkdir()
        marker = f"consumer-lab-psig-marker-{os.getpid()}-{name}"
        started = files / "started"
        script = files / "client.py"
        script.write_text(f"STARTED = {str(started)!r}\n" + SLEEPY_CLIENT)
        out = files / "report.json"
        cmd = [sys.executable, "-B", "-m", "consumer_lab", "run", "--upstream", str(TREE),
               "--client", f"{sys.executable} -B {script} {marker}", "--scenario", "W0", "--json", str(out)]
        proc = subprocess.Popen(cmd, cwd=REPO, env={**os.environ, "TMPDIR": str(kit_tmp)},
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                start_new_session=True, preexec_fn=_default_stop_signals)
        self.addCleanup(lambda: proc.poll() is None and kill_quietly(proc.pid))
        deadline = time.monotonic() + 60
        while not started.exists() and proc.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertTrue(started.exists(), "the client never started")
        os.kill(proc.pid, signum)
        sent = time.monotonic()
        try:
            stdout, stderr = proc.communicate(timeout=self.BOUND)
        except subprocess.TimeoutExpired:
            kill_quietly(proc.pid)
            stdout, stderr = proc.communicate()
        took = time.monotonic() - sent
        output = (stdout + stderr).decode("utf-8", errors="replace")
        remaining = processes_with(str(kit_tmp), marker)
        deadline = time.monotonic() + 2.0
        while remaining and time.monotonic() < deadline:
            time.sleep(0.05)
            remaining = processes_with(str(kit_tmp), marker)
        for pid in remaining:  # never leave one behind
            self.addCleanup(kill_quietly, pid)
        detail = f"{output}\nafter {took:.1f}s; still running: {remaining}"
        self.assertEqual(proc.returncode, -signum, detail)
        self.assertLess(took, self.BOUND, detail)
        self.assertEqual(remaining, [], detail)
        self.assertEqual(sorted(p.name for p in kit_tmp.iterdir() if p.name.startswith("consumer-lab")), [], detail)
        self.assertTrue(out.exists(), detail)
        report = json.loads(out.read_text())
        self.assertEqual(report["setup_error"], f"W0: interrupted by {name}", detail)
        self.assertEqual(report["scenarios"], {}, detail)
        self.assertIn(f"setup error: W0: interrupted by {name}", output.splitlines(), detail)

    def test_sigterm_to_the_parent_cleans_up_and_ends_by_sigterm(self):
        self.check(signal.SIGTERM)

    def test_sighup_to_the_parent_cleans_up_and_ends_by_sighup(self):
        self.check(signal.SIGHUP)


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


# Set HELPER_PID, CLIENT_PID, WORKER_PID and SIGNALS (names) before this source. An in-group
# helper sends the worker each of SIGNALS in turn during the cycle, then sleeps; the client
# records the worker's pid and exits after 2 s.
SIGTSTP_CLIENT = r"""
import os, signal, time
worker = os.getppid()
with open(WORKER_PID, "w") as f:
    f.write(str(worker))
if os.fork() == 0:
    with open(HELPER_PID, "w") as f:
        f.write(str(os.getpid()))
    time.sleep(0.3)
    for name in SIGNALS:
        os.kill(worker, getattr(signal, name))
        time.sleep(0.2)
    time.sleep(30)
    os._exit(0)
with open(CLIENT_PID, "w") as f:
    f.write(str(os.getpid()))
time.sleep(2)
"""

# The parent `run` with a short WORKER_TIMEOUT (as WorkerTimeoutLeavesNoStrayDir patches it).
SHORT_TIMEOUT_RUN = r"""
import sys
from consumer_lab import cli
cli.WORKER_TIMEOUT = 8
sys.exit(cli.main(sys.argv[1:]))
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

    STOP_LINE = "stop signal during the client cycle not re-delivered: SIGTSTP"

    def run_signals(self, *signals: str) -> tuple[int, dict | None, str]:
        """Run W0 with SIGTSTP_CLIENT sending `signals`; assert the client's group and the worker
        are gone afterwards. The run gets its own process group in this session, so the group is
        not orphaned and a stop signal re-delivered to the worker would really stop it (the
        kernel discards stop signals to an orphaned group); WORKER_TIMEOUT is 8 s, so such a
        stop fails the test in seconds rather than minutes."""
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-sclient-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        script = tmp / "client.py"
        names = ("helper.pid", "client.pid", "worker.pid")
        script.write_text(
            "".join(f"{n.split('.')[0].upper()}_PID = {str(tmp / n)!r}\n" for n in names)
            + f"SIGNALS = {list(signals)!r}\n" + SIGTSTP_CLIENT
        )
        out = tmp / "report.json"
        before = self.w0_dirs()
        cmd = [sys.executable, "-B", "-c", SHORT_TIMEOUT_RUN, "run", "--upstream", str(TREE),
               "--client", f"{sys.executable} -B {script}", "--scenario", "W0", "--json", str(out)]
        proc = subprocess.run(cmd, cwd=REPO, capture_output=True, timeout=120, process_group=0)
        for name in self.w0_dirs() - before:
            self.addCleanup(shutil.rmtree, Path(tempfile.gettempdir()) / name, True)
        output = (proc.stdout + proc.stderr).decode("utf-8", errors="replace")
        report = json.loads(out.read_text()) if out.exists() else None
        helper, client, worker = (int((tmp / n).read_text()) for n in names)
        seen = {pid: alive(pid) for pid in (helper, client, worker)}
        for pid, was_alive in seen.items():
            if was_alive:  # never leave it behind (stopped or not); never signal a pid seen dead
                self.addCleanup(kill_quietly, pid)
        self.assertEqual(seen, {helper: False, client: False, worker: False}, output)
        return proc.returncode, report, output

    def test_sigtstp_from_the_group_is_deferred_and_dropped(self):
        # Deferred, then dropped with a kit line: the run gives the verdict it would have given
        # without the signal (this client posts nothing: W0 FAIL), not a stall to the timeout.
        code, report, output = self.run_signals("SIGTSTP")
        self.assertIn(code, (0, 1), output)
        self.assertIsNone(report["setup_error"], output)
        self.assertIn(self.STOP_LINE, output)

    def test_stop_then_sigterm_ends_by_sigterm(self):
        code, report, output = self.run_signals("SIGTSTP", "SIGTERM")
        self.assertEqual(code, 2, output)
        self.assertEqual(report["setup_error"], "W0: worker exited -15: killed by signal 15", output)
        self.assertIn(self.STOP_LINE, output)

    def test_sigint_then_stop_ends_by_sigint(self):
        code, report, output = self.run_signals("SIGINT", "SIGTSTP")
        self.assertEqual(code, 2, output)
        self.assertEqual(report["setup_error"], "W0: worker exited -2: killed by signal 2", output)
        self.assertIn(self.STOP_LINE, output)

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


# Writes PARTIAL (no newline) to its worker's stderr, once per scenario (TC_STATE_DIR keeps a
# marker across the cycles of a reader scenario), then exits 0 without events.
PARTIAL_LINE = r"""
import os
marker = os.path.join(os.environ["TC_STATE_DIR"], "partial-written")
if not os.path.exists(marker):
    open(marker, "w").close()
    fd = os.open(f"/proc/{os.getppid()}/fd/2", os.O_WRONLY | os.O_APPEND)
    os.write(fd, b"PARTIAL")
    os.close(fd)
"""

# Writes PARTIAL to its worker's stderr, then kills the worker outright: the stderr tail the
# parent echoes ends without a newline, and the parent's own `kept` line comes next.
PARTIAL_THEN_KILL = r"""
import os, signal
fd = os.open(f"/proc/{os.getppid()}/fd/2", os.O_WRONLY | os.O_APPEND)
os.write(fd, b"PARTIAL")
os.close(fd)
os.kill(os.getppid(), signal.SIGKILL)
"""

# Writes PARTIAL to its worker's stderr, then sends the worker SIGTSTP (deferred, then dropped
# with the kit's stop-signal line).
PARTIAL_THEN_SIGTSTP = r"""
import os, signal
fd = os.open(f"/proc/{os.getppid()}/fd/2", os.O_WRONLY | os.O_APPEND)
os.write(fd, b"PARTIAL")
os.close(fd)
os.kill(os.getppid(), signal.SIGTSTP)
"""


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class KitLinesStartOnAFreshLine(unittest.TestCase):
    """A partial line the client leaves on its worker's stderr never joins a kit line (v0.2.4):
    the worker's `kept` and stop-signal lines and the parent's lines after its echo start on a
    fresh line, and a run without a partial line gains no empty line."""

    def kept_entries(self) -> set[str]:
        return {p.name for p in Path(tempfile.gettempdir()).glob("consumer-lab-[sw][0-9]-*")}

    def run_kit(self, source: str, scenario: str, *extra: str, short: bool = False) -> tuple[int, str]:
        """Run `scenario` with the client `source`; return the exit code and the kit's stderr
        alone (stdout holds the verdict lines). With `short`, WORKER_TIMEOUT is 8 s (a stop
        signal re-delivered to the worker would fail in seconds). The run gets its own process
        group, as in `DeferredSignalFromTheGroup.run_signals`. Whatever the run left in the
        temp dir is removed afterwards."""
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-nclient-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        script = tmp / "client.py"
        script.write_text(source)
        before = self.kept_entries()
        head = [sys.executable, "-B", "-c", SHORT_TIMEOUT_RUN] if short else [sys.executable, "-m", "consumer_lab"]
        cmd = [*head, "run", "--upstream", str(TREE),
               "--client", f"{sys.executable} -B {script}", "--scenario", scenario, *extra]
        proc = subprocess.run(cmd, cwd=REPO, capture_output=True, timeout=300, process_group=0)
        for name in self.kept_entries() - before:
            self.addCleanup(remove_entry, Path(tempfile.gettempdir()) / name)
        return proc.returncode, proc.stderr.decode("utf-8", errors="replace")

    def assert_kept_line(self, stderr: str, scenario: str) -> None:
        lines = stderr.splitlines()
        kept = [ln for ln in lines if ln.startswith("kept ")]
        self.assertTrue(kept, stderr)
        for line in kept:
            path = Path(line.removeprefix("kept "))
            self.assertTrue(path.name.startswith(f"consumer-lab-{scenario.lower()}-"), stderr)
            self.assertTrue(path.is_dir(), stderr)
            self.assertEqual(line, f"kept {path}", stderr)
        self.assertIn("PARTIAL", lines, stderr)  # the client's text, on a line of its own

    def test_worker_kept_line_after_a_partial_line_reader(self):
        code, stderr = self.run_kit(PARTIAL_LINE, "S1", "--keep-root")
        self.assertIn(code, (0, 1), stderr)
        self.assert_kept_line(stderr, "S1")
        self.assertEqual(len([ln for ln in stderr.splitlines() if ln.startswith("kept ")]), 1, stderr)

    def test_worker_kept_line_after_a_partial_line_writer(self):
        code, stderr = self.run_kit(PARTIAL_LINE, "W0", "--keep-root")
        self.assertIn(code, (0, 1), stderr)
        self.assert_kept_line(stderr, "W0")
        self.assertEqual(len([ln for ln in stderr.splitlines() if ln.startswith("kept ")]), 1, stderr)

    def test_parent_kept_line_after_a_partial_tail(self):
        code, stderr = self.run_kit(PARTIAL_THEN_KILL, "W0", "--keep-root")
        self.assertEqual(code, 2, stderr)
        self.assert_kept_line(stderr, "W0")
        self.assertEqual(stderr.splitlines()[-1], "setup error: W0: worker exited -9: killed by signal 9", stderr)

    def test_stop_signal_line_after_a_partial_line(self):
        code, stderr = self.run_kit(PARTIAL_THEN_SIGTSTP, "W0", short=True)
        self.assertIn(code, (0, 1), stderr)
        lines = stderr.splitlines()
        self.assertIn(DeferredSignalFromTheGroup.STOP_LINE, lines, stderr)
        self.assertIn("PARTIAL", lines, stderr)

    def test_runs_without_a_partial_line_gain_no_empty_line(self):
        for scenario in ("S1", "W0"):
            with self.subTest(scenario=scenario):
                code, stderr = self.run_kit("", scenario, "--keep-root")
                self.assertIn(code, (0, 1), stderr)
                self.assertEqual(len([ln for ln in stderr.splitlines() if ln.startswith("kept ")]), 1, stderr)
                self.assertNotIn("\n\n", stderr)
                self.assertFalse(stderr.startswith("\n"), stderr)


def sigblk(status_text: str) -> set[int]:
    """The blocked signal numbers from the `SigBlk:` line of a /proc/.../status text."""
    for line in status_text.splitlines():
        if line.startswith("SigBlk:"):
            bits = int(line.split()[1], 16)
            return {n for n in range(1, 65) if bits >> (n - 1) & 1}
    raise AssertionError(f"no SigBlk line in {status_text!r}")


def blockable_deferred_signals() -> set[int]:
    """The deferred signals a thread can block, as a fresh thread that blocks them sees its
    mask (the C library's reserved numbers, 32 and 33 on glibc, are not valid signals)."""
    out: dict[str, set[int]] = {}

    def probe():
        valid = set(DEFERRED_SIGNALS) & signal.valid_signals()
        before = {int(s) for s in signal.pthread_sigmask(signal.SIG_BLOCK, valid)}
        out["added"] = {int(s) for s in signal.pthread_sigmask(signal.SIG_BLOCK, [])} - before

    t = threading.Thread(target=probe)
    t.start()
    t.join(10)
    return out["added"]


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

    def test_sigint_handler_is_restored_last(self):
        # A SIGINT arriving mid-restore must find every other handler already back.
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-hygiene-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        real_signal = signal.signal
        installs: list[int] = []

        def logging_signal(sig, handler):
            previous = real_signal(sig, handler)  # a refused number raises before it is logged
            installs.append(int(sig))
            return previous

        before = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM, signal.SIGUSR1)}
        with mock.patch.object(signal, "signal", side_effect=logging_signal):
            run_client(["true"], dict(os.environ), tmp / "client.stderr", 10.0)
        set_ = len(installs) // 2  # every handler is set once, then restored once
        self.assertEqual(len(installs), 2 * set_)
        restores = installs[set_:]
        self.assertEqual(restores[-1], int(signal.SIGINT), restores)
        self.assertEqual(sorted(restores), sorted(installs[:set_]))
        self.assertEqual({s: signal.getsignal(s) for s in before}, before)

    def restore_hook(self, after_first_restore):
        """A `signal.signal` stand-in that calls `after_first_restore()` once, right after the
        first handler `run_client` restores (the second call for a signal number), and records
        the blocked mask at every restore."""
        real_signal = signal.signal
        installed: set[int] = set()
        masks: list[tuple[int, set[int]]] = []

        def hooked(sig, handler):
            restoring = int(sig) in installed
            if restoring:
                masks.append((int(sig), {int(s) for s in signal.pthread_sigmask(signal.SIG_BLOCK, [])}))
            previous = real_signal(sig, handler)  # a refused number raises before it is logged
            if not restoring:
                installed.add(int(sig))
            elif len(masks) == 1:
                after_first_restore()
            return previous

        return hooked, masks

    def keep_handler(self, sig, handler):
        before = signal.signal(sig, handler)
        self.addCleanup(signal.signal, sig, before)

    def test_signal_during_restore_reaches_the_restored_handler_once(self):
        # A SIGUSR1 sent while the handlers are being restored (after the first one is back)
        # reaches SIGUSR1's previous handler exactly once: not lost, not delivered twice.
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-hygiene-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        got: list[int] = []
        self.keep_handler(signal.SIGUSR1, lambda signum, _frame: got.append(signum))
        hooked, masks = self.restore_hook(lambda: os.kill(os.getpid(), signal.SIGUSR1))
        with mock.patch.object(signal, "signal", side_effect=hooked):
            run_client(["true"], dict(os.environ), tmp / "client.stderr", 10.0)
        self.assertTrue(masks)  # the hook ran
        self.assertEqual(got, [signal.SIGUSR1])

    def test_deferred_signals_stay_blocked_while_handlers_are_restored(self):
        # The window inside signal.signal() is in C and cannot be hit from Python; what closes
        # it is that no deferred signal can be delivered while any handler is being restored.
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-hygiene-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        hooked, masks = self.restore_hook(lambda: None)
        mask_before = signal.pthread_sigmask(signal.SIG_BLOCK, [])
        with mock.patch.object(signal, "signal", side_effect=hooked):
            run_client(["true"], dict(os.environ), tmp / "client.stderr", 10.0)
        restored = {sig for sig, _ in masks}
        self.assertIn(int(signal.SIGINT), restored)
        self.assertIn(int(signal.SIGUSR1), restored)
        unblocked = [(sig, sorted(restored - mask)) for sig, mask in masks if not restored <= mask]
        self.assertEqual(unblocked, [])
        self.assertEqual(signal.pthread_sigmask(signal.SIG_BLOCK, []), mask_before)  # mask is back

    def test_handler_raising_when_the_mask_is_restored_still_redelivers(self):
        # A SIGINT that arrives mid-restore raises KeyboardInterrupt from the default handler;
        # the SIGUSR2 the client sent during the cycle is still re-delivered first, once.
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-hygiene-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        got: list[int] = []
        self.keep_handler(signal.SIGUSR2, lambda signum, _frame: got.append(signum))
        self.keep_handler(signal.SIGINT, signal.default_int_handler)
        hooked, masks = self.restore_hook(lambda: os.kill(os.getpid(), signal.SIGINT))
        argv = ["sh", "-c", "kill -USR2 $PPID"]
        with mock.patch.object(signal, "signal", side_effect=hooked):
            with self.assertRaises(KeyboardInterrupt):
                run_client(argv, dict(os.environ), tmp / "client.stderr", 10.0)
        self.assertTrue(masks)
        self.assertEqual(got, [signal.SIGUSR2])
        self.assertIs(signal.getsignal(signal.SIGINT), signal.default_int_handler)

    def kit_threads_alive(self) -> list[threading.Thread]:
        """Start the kit's fault proxy (its serve thread is started the way the upstream thread
        is) and keep one of its request threads busy with a connection that sends nothing.
        Returns the threads this added; they end in the test's cleanup."""
        from consumer_lab.proxy import FaultProxy, Plan

        before = set(threading.enumerate())
        proxy = FaultProxy("http://127.0.0.1:9", Plan(room="lab-c9"))  # the origin is never used
        self.addCleanup(proxy.stop)
        conn = socket.create_connection(("127.0.0.1", proxy.port), timeout=5)
        self.addCleanup(conn.close)  # runs before proxy.stop: the request thread reads EOF
        deadline = time.monotonic() + 5
        while len(set(threading.enumerate()) - before) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        added = sorted(set(threading.enumerate()) - before, key=lambda t: t.name)
        self.assertEqual(len(added), 2, added)  # fault-proxy and one request thread
        return added

    def test_kit_threads_block_the_deferred_signals(self):
        # Every thread the kit starts (here the fault proxy's serve thread and a request thread
        # it started) has every blockable deferred signal blocked; starting them leaves the
        # starting thread's mask as it was.
        mask_before = signal.pthread_sigmask(signal.SIG_BLOCK, [])
        added = self.kit_threads_alive()
        self.assertEqual(signal.pthread_sigmask(signal.SIG_BLOCK, []), mask_before)
        want = blockable_deferred_signals()
        self.assertIn(int(signal.SIGTERM), want)
        for t in added:
            with self.subTest(thread=t.name):
                status = Path(f"/proc/self/task/{t.native_id}/status").read_text()
                self.assertEqual(sorted(want - sigblk(status)), [])

    def test_signal_to_the_process_during_restore_stays_pending_with_kit_threads_alive(self):
        # A process-directed SIGUSR1 sent mid-restore, while the main thread blocks it and kit
        # threads are alive, is not taken by them: it is still pending after a pause, and only
        # reaches SIGUSR1's restored handler, exactly once, when run_client puts the mask back.
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-hygiene-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        self.kit_threads_alive()
        main_tid = threading.main_thread().native_id
        others = [tid for tid in os.listdir("/proc/self/task") if int(tid) != main_tid]
        self.assertGreaterEqual(len(others), 2, others)
        got: list[int] = []
        self.keep_handler(signal.SIGUSR1, lambda signum, _frame: got.append(signum))
        seen: dict[str, object] = {}

        def send_to_the_process():
            os.kill(os.getpid(), signal.SIGUSR1)  # to the process: an unblocked thread takes it
            time.sleep(0.2)
            seen["pending"] = signal.SIGUSR1 in signal.sigpending()
            seen["handled"] = list(got)

        hooked, masks = self.restore_hook(send_to_the_process)
        with mock.patch.object(signal, "signal", side_effect=hooked):
            run_client(["true"], dict(os.environ), tmp / "client.stderr", 10.0)
        self.assertTrue(masks)  # the hook ran
        self.assertEqual(seen, {"pending": True, "handled": []})
        self.assertEqual(got, [signal.SIGUSR1])
        self.assertNotIn(signal.SIGUSR1, signal.sigpending())

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


# Records, at every cycle, each thread of its worker (/proc/<ppid>/task/*): its tid and its
# whole status text (Name, SigBlk, ...). The test prepends OUT_DIR.
THREAD_MASKS = r"""
import json, os
ppid = os.getppid()
tasks = []
for tid in sorted(os.listdir(f"/proc/{ppid}/task"), key=int):
    try:
        with open(f"/proc/{ppid}/task/{tid}/status") as f:
            status = f.read()
    except FileNotFoundError:
        continue  # the thread ended meanwhile
    tasks.append({"tid": int(tid), "status": status})
n = len(os.listdir(OUT_DIR))
with open(os.path.join(OUT_DIR, f".cycle-{n}"), "w") as f:
    json.dump({"ppid": ppid, "tasks": tasks}, f)
os.rename(os.path.join(OUT_DIR, f".cycle-{n}"), os.path.join(OUT_DIR, f"cycle-{n}.json"))
"""


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class KitThreadsBlockTheDeferredSignals(unittest.TestCase):
    """In a real worker, during every client cycle, every thread but the main one blocks the
    deferred signals (the upstream server's thread, the fault proxy's and the threads they
    started), and the main thread blocks none of SIGINT, SIGTERM, SIGUSR1."""

    def cycles(self, scenario: str) -> list[dict]:
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-tclient-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        out = tmp / "cycles"
        out.mkdir()
        script = tmp / "client.py"
        script.write_text(f"OUT_DIR = {str(out)!r}\n" + THREAD_MASKS)
        cmd = [sys.executable, "-m", "consumer_lab", "run", "--upstream", str(TREE),
               "--client", f"{sys.executable} -B {script}", "--scenario", scenario]
        proc = subprocess.run(cmd, cwd=REPO, capture_output=True, timeout=300)
        output = (proc.stdout + proc.stderr).decode("utf-8", errors="replace")
        self.assertIn(proc.returncode, (0, 1), output)  # no events: a verdict, not a setup error
        found = sorted(out.glob("cycle-*.json"), key=lambda p: int(p.stem.split("-")[1]))
        self.assertTrue(found, output)
        return [json.loads(p.read_text()) for p in found]

    def check(self, scenario: str, other_threads_at_least: int) -> None:
        want = blockable_deferred_signals()
        main_must_take = {int(signal.SIGINT), int(signal.SIGTERM), int(signal.SIGUSR1)}
        for i, cycle in enumerate(self.cycles(scenario)):
            main = [t for t in cycle["tasks"] if t["tid"] == cycle["ppid"]]
            others = [t for t in cycle["tasks"] if t["tid"] != cycle["ppid"]]
            with self.subTest(cycle=i):
                self.assertEqual(len(main), 1, cycle)
                self.assertEqual(main_must_take & sigblk(main[0]["status"]), set(), cycle)
                self.assertGreaterEqual(len(others), other_threads_at_least, cycle)
                unblocked = {t["tid"]: sorted(want - sigblk(t["status"])) for t in others}
                summary = [ln for t in cycle["tasks"] for ln in t["status"].splitlines()
                           if ln.startswith(("Name:", "Pid:", "SigBlk:"))]
                self.assertEqual({tid: s for tid, s in unblocked.items() if s}, {}, summary)

    def test_reader_worker_threads_block_the_deferred_signals(self):
        self.check("S1", 1)  # at least the upstream thread

    def test_writer_worker_threads_block_the_deferred_signals(self):
        self.check("W0", 2)  # at least the upstream and fault-proxy threads



@unittest.skipUnless(hasattr(os, "pidfd_open") and Path("/proc").is_dir(), "needs pidfd_open and /proc")
class WorkerStopperReapedMeanwhile(unittest.TestCase):
    """v0.2.4 C12 fix round 1: when the worker is gone on the stopper's re-check (reaped, the pid
    maybe reused, between the scan and pidfd_open), the stopper signals nothing and closes its
    pidfd exactly once, with no exception in its thread."""

    def test_pidfd_closed_once_when_the_worker_is_gone_on_the_recheck(self):
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        self.addCleanup(child.wait)
        self.addCleanup(kill_quietly, child.pid)  # runs first: cleanups run last-in, first-out
        scans, opened, closed, errors = [], [], [], []
        real_open, real_close = os.pidfd_open, os.close

        def child_pids():  # the child on the first scan, gone on the re-check
            scans.append(1)
            return [child.pid] if len(scans) == 1 else []

        def pidfd_open(pid, *rest):
            fd = real_open(pid, *rest)
            opened.append(fd)
            return fd

        def close(fd):
            if fd in opened:
                closed.append(fd)
            real_close(fd)

        stop = cli._ParentSignals()
        stop.signum = signal.SIGTERM  # recorded; nothing is installed
        with mock.patch.object(cli, "_child_pids", child_pids), \
                mock.patch.object(cli.os, "pidfd_open", pidfd_open), \
                mock.patch.object(cli.os, "close", close), \
                mock.patch.object(threading, "excepthook", lambda a: errors.append(repr(a.exc_value))):
            stopper = cli._WorkerStopper(stop)
            stopper.start()
            stopper.join(10)
        self.assertFalse(stopper.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(scans), 2)
        self.assertEqual(len(opened), 1)
        self.assertEqual(closed, opened)  # exactly one close, of the pidfd it opened
        self.assertIsNone(child.poll(), "the child was signalled")


class ParentSignalDecidedLate(unittest.TestCase):
    """v0.2.4 C12 fix round 1, in-process (no upstream, no worker: the tree checks, the worker
    and `upstream_commit` are patched; `_ParentSignals.end` records instead of ending the
    process). A signal recorded before a setup error leaves `_run`, or after the scenarios but
    before the report is written, still gives the report and the lines before the end."""

    def setUp(self):
        self.base = Path(tempfile.mkdtemp(prefix="consumer-lab-plate-"))
        self.addCleanup(shutil.rmtree, self.base, True)
        self.out = self.base / "report.json"
        self.err = io.StringIO()
        self.ends = []  # at each `end`: (stderr so far, the report then, or None)
        self.signals = []
        real_install = cli._ParentSignals.install

        def install(signals):
            self.signals.append(signals)
            real_install(signals)

        def end(signals):
            report = json.loads(self.out.read_text()) if self.out.exists() else None
            self.ends.append((signals.name(), self.err.getvalue(), report))

        for patch in (
            mock.patch.object(cli, "marker_ok", lambda tree: (True, "")),
            mock.patch.object(cli, "missing_upstream_api", lambda tree: []),
            mock.patch.object(cli._ParentSignals, "install", install),
            mock.patch.object(cli._ParentSignals, "end", end),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    def record(self, signum: int) -> None:
        """What the installed handler does when the signal arrives."""
        self.signals[0]._handler(signum, None)

    def main(self) -> int:
        argv = ["run", "--upstream", str(self.base), "--client", "true", "--scenario", "W0",
                "--json", str(self.out)]
        with contextlib.redirect_stderr(self.err), contextlib.redirect_stdout(io.StringIO()):
            return cli.main(argv)

    def test_a_setup_error_after_a_signal_is_printed_and_reported_before_the_end(self):
        def new_workdir(sid):
            self.record(signal.SIGTERM)
            raise cli.SetupError("injected after the signal")

        with mock.patch.object(cli, "_new_workdir", new_workdir):
            code = self.main()
        self.assertEqual(code, cli.EXIT_SETUP)
        self.assertEqual(len(self.ends), 1, self.ends)
        name, err, report = self.ends[0]
        self.assertEqual(name, "SIGTERM")
        lines = err.splitlines()
        self.assertIn("setup error: injected after the signal", lines, err)
        self.assertIn("setup error: W0: interrupted by SIGTERM", lines, err)
        self.assertIsNotNone(report, err)
        self.assertEqual(report["setup_error"], "W0: interrupted by SIGTERM")
        self.assertEqual(report["scenarios"], {})
        self.assertFalse(report["all_pass"])
        self.assertEqual(sorted(p.name for p in self.base.iterdir()), ["report.json"])

    def test_a_signal_after_the_scenarios_before_the_report_is_reported(self):
        workdirs = []

        def new_workdir(sid):
            workdirs.append(Path(tempfile.mkdtemp(prefix="consumer-lab-w0-", dir=self.base)))
            return workdirs[-1]

        result = {"id": "W0", "name": "seed", "verdict": "PASS", "reason": "ok"}

        def run_worker(cmd, env, stop=None):
            return 0, json.dumps(result) + "\n", "", 0

        def commit(tree):  # read after the last scenario, before the report is written
            self.record(signal.SIGHUP)
            return "unknown"

        with mock.patch.object(cli, "_new_workdir", new_workdir), \
                mock.patch.object(cli, "_run_worker", run_worker), \
                mock.patch.object(cli, "upstream_commit", commit):
            code = self.main()
        self.assertEqual(code, cli.EXIT_SETUP)
        self.assertEqual(len(self.ends), 1, self.ends)
        name, err, report = self.ends[0]
        self.assertEqual(name, "SIGHUP")
        self.assertIn("setup error: W0: interrupted by SIGHUP", err.splitlines(), err)
        self.assertIsNotNone(report, err)
        self.assertEqual(report["setup_error"], "W0: interrupted by SIGHUP")
        self.assertEqual(report["scenarios"], {"W0": result})
        self.assertFalse(report["all_pass"])
        self.assertEqual(len(workdirs), 1)
        self.assertEqual(sorted(p.name for p in self.base.iterdir()), ["report.json"])


if __name__ == "__main__":
    unittest.main()
