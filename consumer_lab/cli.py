"""`python -m consumer_lab run --upstream <tree> --client "<cmd>" [--scenario S1..S5|W0..W7|all|writers] [--json out.json]`

Reader scenarios S1..S5 (`all`) and writer scenarios W0..W7 (`writers`) have different client
contracts, so one run selects from one family only. Each scenario runs in its own worker
process (`python -m consumer_lab _worker ...`) with its own temp dir (created by the parent,
so a worker that times out leaves none behind) and its own in-process upstream server,
because upstream reads its env knobs and caches per-root state at import; a writer scenario
also runs its fault-injecting proxy there. Exit codes: 0 all selected scenarios PASS, 1 any
FAIL, 2 setup error (bad tree, missing private functions, server failed to start, a scenario
precondition that did not hold, S and W ids mixed in one run).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from dataclasses import replace
from pathlib import Path

from consumer_lab import __version__
from consumer_lab.server import (
    PINNED_COMMIT,
    SetupError,
    marker_ok,
    missing_upstream_api,
    upstream_commit,
)

EXIT_PASS, EXIT_FAIL, EXIT_SETUP = 0, 1, 2
WORKER_TIMEOUT = 600
# After SIGTERM/SIGHUP to `run`: how long the worker gets to end after its SIGTERM before it is
# killed. The worker defers that SIGTERM while a client cycle runs (at most CYCLE_TIMEOUT, 30 s),
# then kills the client's group and ends by it; 30 s plus a margin of 15 s.
WORKER_STOP_GRACE = 45
WORKER_POLL = 0.2  # seconds between checks for a parent signal while a worker runs
OUTPUT_TAIL_BYTES = 1 << 20  # how much of each worker stream the parent reads (the end of it)


def _split_needs(needs: tuple[str, ...]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    names = tuple(n for n in needs if n.isupper() or n.lstrip("_").isupper())
    functions = tuple(n for n in needs if n not in names)
    return functions, names


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m consumer_lab", description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="command", required=True)
    for name in ("run", "_worker"):
        s = sub.add_parser(name, help="run reader or writer fault scenarios" if name == "run" else argparse.SUPPRESS)
        s.add_argument("--upstream", required=True, help="upstream technocore-chat working tree")
        s.add_argument("--client", required=True, help="client command, run once per cycle")
        s.add_argument(
            "--scenario",
            default="all",
            help="reader ids S1..S5, writer ids W0..W7, a comma list of either family, "
            "all (S1..S5) or writers (W0..W7); default: all",
        )
        s.add_argument("--keep-root", action="store_true", help="keep each scenario's temp dir")
        if name == "run":
            s.add_argument("--json", dest="json_out", help="write a JSON report here")
        else:
            s.add_argument("--workdir", help=argparse.SUPPRESS)  # the scenario's temp dir, from `_run`
    return p


def _selected(arg: str) -> list[str]:
    from consumer_lab.scenarios import SCENARIOS
    from consumer_lab.writers import WRITER_SCENARIOS

    if arg == "all":
        return list(SCENARIOS)
    if arg == "writers":
        return list(WRITER_SCENARIOS)
    wanted = [s.strip().upper() for s in arg.split(",") if s.strip()]
    unknown = [s for s in wanted if s not in SCENARIOS and s not in WRITER_SCENARIOS]
    if unknown or not wanted:
        raise SetupError(
            f"unknown scenario {', '.join(unknown) or arg!r}; choose S1..S5, W0..W7, all or writers"
        )
    if any(s in SCENARIOS for s in wanted) and any(s in WRITER_SCENARIOS for s in wanted):
        raise SetupError(
            "reader (S) and writer (W) scenarios cannot be mixed in one run: their client "
            "contracts differ; run them separately"
        )
    return wanted


def _new_workdir(sid: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=f"consumer-lab-{sid.lower()}-"))


def _workdir(args, sid: str) -> Path:
    """The dir `_run` created for this scenario; a worker started by hand makes its own."""
    return Path(args.workdir) if args.workdir else _new_workdir(sid)


def _write_kept_result(workdir: Path, outcome: dict, quiet_if_own: bool = False) -> bool:
    """Write `outcome` to <workdir>/result.json for `--keep-root`, never through a link.

    The client can create, rename and replace entries in its workdir (it knows the path from
    TC_EVENTS), so: the workdir is opened without following a symlink, and result.json is
    created relative to it, exclusively and without following one. If the workdir was replaced
    or result.json already exists in any form, nothing is written, overwritten, followed or
    deleted, and a fixed kit line says so; except, with `quiet_if_own` (the parent's call after
    a worker exit of -2, the one signal exit where the worker's own `finally` ran and printed
    `kept`), when result.json exists as a regular file with one link (taken as the worker's
    own): then nothing is printed and False is returned. Otherwise True. (The workdir's parent,
    the system temp dir, is not defended.)"""
    text = json.dumps(outcome, sort_keys=True, indent=2) + "\n"
    dir_fd = None
    try:
        dir_fd = os.open(workdir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        fd = os.open(
            "result.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=dir_fd
        )
    except OSError:
        if quiet_if_own and dir_fd is not None:
            try:
                existing = os.lstat("result.json", dir_fd=dir_fd)  # never follows
            except OSError:
                existing = None
            if existing is not None and stat.S_ISREG(existing.st_mode) and existing.st_nlink == 1:
                return False
        print(f"not writing result.json in {workdir}: it was replaced or already exists", file=sys.stderr)
        return True
    finally:
        if dir_fd is not None:
            os.close(dir_fd)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
    except OSError:
        pass  # a full disk, say: the kept dir stays, without a complete result.json
    return True


def _print_result(result: dict) -> None:
    """The worker's one JSON result line. The client shares this stdout (it can write to
    /proc/<ppid>/fd/1): start a fresh line, so that a partial line it left cannot be joined
    to the result."""
    sys.stdout.write("\n" + json.dumps(result, sort_keys=True) + "\n")
    sys.stdout.flush()


def _worker(args) -> int:
    """Run one scenario in this process; print one JSON line with the result."""
    from consumer_lab.judge import judge
    from consumer_lab.scenarios import SCENARIOS, Kit, start_fresh_stderr_line, workdir_identity, workdir_unchanged
    from consumer_lab.server import Upstream
    from consumer_lab.writers import WRITER_SCENARIOS

    if args.scenario in WRITER_SCENARIOS:
        return _writer_worker(args, WRITER_SCENARIOS[args.scenario])
    scenario = SCENARIOS[args.scenario]
    functions, names = _split_needs(scenario.needs)
    missing = missing_upstream_api(args.upstream, functions, names)
    if missing:
        raise SetupError(f"{scenario.id} needs upstream {', '.join(missing)}, missing from store.py")

    workdir = _workdir(args, scenario.id)
    kit = None
    # How this worker ended, written to <workdir>/result.json when the dir is kept, so a kept
    # root always says whether it holds a verdict, a setup error or a crash of the kit.
    outcome: dict = {"id": scenario.id, "exit_code": 1, "error": "worker stopped before a verdict"}
    try:
        if str(workdir.resolve()).startswith("/mnt/"):
            raise SetupError(f"temp dir {workdir} is on /mnt/; set TMPDIR to a Linux path")
        root = workdir / "root"
        root.mkdir()
        workdir_id = workdir_identity(workdir)  # the workdir and its root
        up = Upstream(args.upstream, root)
        try:
            kit = Kit(up, args.client, workdir, scenario.room)
            truth = scenario.run(kit)
        finally:
            # After a replaced workdir or root, no shutdown hooks: upstream's hook flushes its
            # counters by path, and that path now leads wherever the client chose. Asked of the
            # dirs themselves, not of the exception: a client's SIGINT can end the cycle first.
            up.stop(shutdown_hooks=workdir_unchanged(workdir, workdir_id))
        cycles = list(kit.cycles)
        if cycles and not kit.log_intact():  # changed after the last cycle ended
            cycles[-1] = replace(cycles[-1], log_intact=False)
        lines = kit.events_path.read_text(encoding="utf-8", errors="replace").splitlines()
        verdict = judge(scenario.id, lines, truth, cycles)
        kinds: Counter[str] = Counter()
        for line in lines:
            try:
                kinds[str(json.loads(line).get("event"))] += 1
            except (ValueError, RecursionError, AttributeError):
                kinds["malformed"] += 1
        result = {
            "id": scenario.id,
            "name": scenario.name,
            "verdict": verdict.verdict,
            "reason": verdict.reason,
            "cycles": len(kit.cycles),
            "cycle_exit_codes": [c.exit_code for c in kit.cycles],
            "events": {
                "deliver": kinds.get("deliver", 0),
                "loss": kinds.get("loss", 0),
                "other": sum(v for k, v in kinds.items() if k not in ("deliver", "loss")),
                "total": len(lines),
            },
        }
        _print_result(result)
        outcome = {**result, "exit_code": EXIT_PASS}
        return EXIT_PASS
    except SetupError as exc:
        outcome = {"id": scenario.id, "exit_code": EXIT_SETUP, "setup_error": str(exc)}
        raise
    except BaseException as exc:
        outcome = {"id": scenario.id, "exit_code": 1, "error": repr(exc)}
        raise
    finally:
        if args.keep_root:
            if kit is not None:
                outcome["cycle_exit_codes"] = [c.exit_code for c in kit.cycles]
            start_fresh_stderr_line()  # the client may have left a partial line on this stderr
            _write_kept_result(workdir, outcome)
            print(f"kept {workdir}", file=sys.stderr)
        else:
            shutil.rmtree(workdir, ignore_errors=True)


def _writer_worker(args, scenario) -> int:
    """The writer twin of `_worker`: one scenario, one client cycle through the proxy."""
    from consumer_lab.scenarios import start_fresh_stderr_line, workdir_identity, workdir_unchanged
    from consumer_lab.server import Upstream
    from consumer_lab.writer_judge import judge_write
    from consumer_lab.writers import run_writer

    functions, names = _split_needs(scenario.needs)
    missing = missing_upstream_api(args.upstream, functions, names)
    if missing:
        raise SetupError(f"{scenario.id} needs upstream {', '.join(missing)}, missing from store.py")

    workdir = _workdir(args, scenario.id)
    run = None
    outcome: dict = {"id": scenario.id, "exit_code": 1, "error": "worker stopped before a verdict"}
    try:
        if str(workdir.resolve()).startswith("/mnt/"):
            raise SetupError(f"temp dir {workdir} is on /mnt/; set TMPDIR to a Linux path")
        root = workdir / "root"
        root.mkdir()
        workdir_id = workdir_identity(workdir)  # the workdir and its root
        up = Upstream(args.upstream, root)
        try:
            run = run_writer(scenario, up, args.client, workdir)
        finally:
            # After a replaced workdir or root, no shutdown hooks: upstream's hook flushes its
            # counters by path, and that path now leads wherever the client chose. Asked of the
            # dirs themselves, not of the exception: a client's SIGINT can end the cycle first.
            up.stop(shutdown_hooks=workdir_unchanged(workdir, workdir_id))
        verdict = judge_write(scenario.id, run.lines, run.truth, run.cycle)
        kinds: Counter[str] = Counter()
        for line in run.lines:
            if not line.strip():
                continue
            try:
                kinds[str(json.loads(line).get("event"))] += 1
            except (ValueError, RecursionError, AttributeError):
                kinds["malformed"] += 1
        # Never the write text: only counts, the claimed outcome and the proxy's log as
        # `writers.report_log` reduced it (no client-chosen path or method).
        result = {
            "id": scenario.id,
            "name": scenario.name,
            "verdict": verdict.verdict,
            "reason": verdict.reason,
            "cycles": 1,
            "cycle_exit_codes": [run.cycle.exit_code],
            "outcome": run.outcome,
            "stored": run.truth.stored,
            "other_records": run.truth.other_records,
            "proxy_log": run.truth.proxy_log,
            "events": {
                "write": kinds.get("write", 0),
                "other": sum(v for k, v in kinds.items() if k != "write"),
                "total": sum(kinds.values()),
            },
        }
        _print_result(result)
        outcome = {**result, "exit_code": EXIT_PASS}
        return EXIT_PASS
    except SetupError as exc:
        outcome = {"id": scenario.id, "exit_code": EXIT_SETUP, "setup_error": str(exc)}
        raise
    except BaseException as exc:
        outcome = {"id": scenario.id, "exit_code": 1, "error": repr(exc)}
        raise
    finally:
        if args.keep_root:
            if run is not None:
                outcome["cycle_exit_codes"] = [run.cycle.exit_code]
            start_fresh_stderr_line()  # the client may have left a partial line on this stderr
            _write_kept_result(workdir, outcome)
            print(f"kept {workdir}", file=sys.stderr)
        else:
            shutil.rmtree(workdir, ignore_errors=True)


def _worker_failure(returncode: int, stderr: str) -> str:
    """Kit-written text for a failed worker, never the client's: the client shares the
    worker's stderr (it can write to /proc/<ppid>/fd/2). On EXIT_SETUP the worker's own last
    `setup error: ` line, which `main` prints after the client's process group was killed, so
    a line the client forged earlier is not the last; otherwise fixed text."""
    if returncode == EXIT_SETUP:
        lines = [ln for ln in stderr.splitlines() if ln.startswith("setup error: ")]
        return lines[-1].removeprefix("setup error: ") if lines else "no detail"
    if returncode < 0:
        return f"killed by signal {-returncode}"
    return "kit error, see stderr"


RESULT_KEYS = ("id", "name", "verdict", "reason")  # what `_run` and the report read


def _worker_result(stdout: str) -> dict | None:
    """The result from the last non-empty line of a worker's stdout, or None when that line
    is missing, not JSON, not a JSON object, or lacks a key `_run` reads (name, verdict and
    reason must be strings: they are printed with a format spec)."""
    lines = [ln for ln in stdout.splitlines() if ln.strip()]
    if not lines:
        return None
    try:
        result = json.loads(lines[-1])
    except (ValueError, RecursionError):
        return None
    if not isinstance(result, dict) or any(k not in result for k in RESULT_KEYS):
        return None
    if not all(isinstance(result[k], str) for k in ("name", "verdict", "reason")):
        return None
    return result


def _keep_result_json(workdir: Path, sid: str, setup_error: str, worker_wrote_its_own: bool = False) -> None:
    """`--keep-root` for a worker that could not write its own result.json. With
    `worker_wrote_its_own` (exit -2: its `finally` ran and printed `kept`), a regular
    single-link result.json already there is left as it is, silently, with no second `kept`
    line; in every other case a refused write prints the `not writing` line and one `kept`."""
    outcome = {"id": sid, "exit_code": EXIT_SETUP, "setup_error": setup_error}
    if _write_kept_result(workdir, outcome, quiet_if_own=worker_wrote_its_own):
        print(f"kept {workdir}", file=sys.stderr)


def _tail(f) -> tuple[str, int]:
    """The last OUTPUT_TAIL_BYTES of the open binary file `f`, decoded with replacement, and the
    number of earlier bytes not read. The cut may land mid-line or mid-character."""
    size = f.seek(0, os.SEEK_END)
    start = max(0, size - OUTPUT_TAIL_BYTES)
    f.seek(start)
    return f.read(OUTPUT_TAIL_BYTES).decode("utf-8", errors="replace"), start


class _ParentSignals:
    """SIGTERM and SIGHUP to `run`, for the duration of `_run`.

    The handler only records the first one (later ones change nothing), so it never interrupts
    the kit's own steps or the cleanup. While a worker runs, a `_WorkerStopper` thread acts on it
    (the worker's own wait stays in `subprocess.run`); `_run` checks `signum` after each worker,
    between scenarios and once more immediately before the report is written, cleans up, prints
    its lines, and `end` then ends the process by that signal. A signal
    whose disposition is SIG_IGN when `_run` starts (under `nohup`, say) stays ignored. Off the
    main thread nothing is installed (Python handlers run only there)."""

    SIGNALS = (signal.SIGTERM, signal.SIGHUP)

    def __init__(self) -> None:
        self.signum: int | None = None
        self._previous: dict[int, object] = {}

    def _handler(self, signum, _frame) -> None:
        if self.signum is None:
            self.signum = signum

    def install(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        for sig in self.SIGNALS:
            if signal.getsignal(sig) == signal.SIG_IGN:
                continue
            self._previous[sig] = signal.signal(sig, self._handler)

    def restore(self) -> None:
        for sig, handler in self._previous.items():
            signal.signal(sig, signal.SIG_DFL if handler is None else handler)
        self._previous.clear()

    def installed(self) -> bool:
        return bool(self._previous)

    def name(self) -> str:
        return signal.Signals(self.signum).name

    def end(self) -> None:
        """End this process by the recorded signal, with its default disposition, so the caller
        sees termination by that signal (after the kit's output is flushed)."""
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.flush()
            except (OSError, ValueError):
                pass
        signal.signal(self.signum, signal.SIG_DFL)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {self.signum})
        os.kill(os.getpid(), self.signum)
        os._exit(128 + self.signum)  # not reached: the default action ends the process first


def _child_pids() -> list[int]:
    """This process's children, from /proc (the parent starts no process but the worker)."""
    me, found = os.getpid(), []
    try:
        entries = os.listdir("/proc")
    except OSError:
        return found
    for name in entries:
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat", "rb") as f:
                fields = f.read().rsplit(b")", 1)[1].split()
        except (OSError, IndexError):
            continue
        if len(fields) > 1 and fields[1] == str(me).encode():
            found.append(int(name))
    return found


class _WorkerStopper(threading.Thread):
    """Runs beside `subprocess.run` while one worker runs. Once `stop` has recorded a signal it
    finds the worker (this process's child), sends it SIGTERM (the worker defers that during a
    client cycle, kills the client's group, then ends by it) and, if `finish` has not been called
    WORKER_STOP_GRACE seconds later, SIGKILL. The worker is addressed through a pidfd taken while
    it is an unreaped child, so a signal sent after `subprocess.run` reaped it reaches no other
    process (without pidfd support, by pid, and only while it is still this process's child)."""

    def __init__(self, stop: _ParentSignals) -> None:
        super().__init__(name="consumer-lab-worker-stopper", daemon=True)
        self._signals = stop
        self._done = False

    def finish(self) -> None:
        self._done = True
        self.join()

    def _send(self, pid: int, pidfd: int | None, sig: int) -> None:
        try:
            if pidfd is not None:
                signal.pidfd_send_signal(pidfd, sig)
            elif pid in _child_pids():
                os.kill(pid, sig)
        except (ProcessLookupError, PermissionError):
            pass

    def run(self) -> None:
        pid = pidfd = None
        try:
            while not self._done and pid is None:
                if self._signals.signum is not None:
                    children = _child_pids()
                    if children:
                        pid = children[0]
                        try:
                            pidfd = os.pidfd_open(pid)
                        except (AttributeError, OSError):
                            pidfd = None
                        if pidfd is not None and pid not in _child_pids():
                            # Reaped (and maybe reused) meanwhile: not ours. `finally` closes
                            # the pidfd, once.
                            return
                        self._send(pid, pidfd, signal.SIGTERM)
                        break
                time.sleep(WORKER_POLL)
            if pid is None:
                return
            kill_at = time.monotonic() + WORKER_STOP_GRACE
            while not self._done and time.monotonic() < kill_at:
                time.sleep(WORKER_POLL)
            if not self._done:
                self._send(pid, pidfd, signal.SIGKILL)
        finally:
            if pidfd is not None:
                os.close(pidfd)


def _run_worker(
    cmd: list[str], env: dict[str, str], stop: _ParentSignals | None = None
) -> tuple[int, str, str, int] | None:
    """Run one worker; return (exit code, stdout tail, stderr tail, earlier stderr bytes not
    read), or None when it did not finish within WORKER_TIMEOUT. A signal `stop` records
    meanwhile stops the worker (`_WorkerStopper`); the result is then returned as usual.

    The client shares the worker's stdout and stderr (it can write to /proc/<ppid>/fd/1 and 2),
    so either may hold anything, of any size. They go to two files in a temp dir of the
    parent's, whose path the worker is never given, opened for append, so the worker's own
    writes land at the end, after what the client wrote. The parent reads at most
    OUTPUT_TAIL_BYTES of each, through its own descriptors, and removes the dir in every case."""
    out_dir = tempfile.mkdtemp(prefix="consumer-lab-out-")
    try:
        with open(os.path.join(out_dir, "stdout"), "a+b") as out, \
                open(os.path.join(out_dir, "stderr"), "a+b") as err:
            stopper = None
            if stop is not None and stop.installed():
                stopper = _WorkerStopper(stop)
                stopper.start()
            try:
                proc = subprocess.run(cmd, env=env, stdout=out, stderr=err, timeout=WORKER_TIMEOUT)
            except subprocess.TimeoutExpired:
                return None
            finally:
                if stopper is not None:
                    stopper.finish()
            stdout, _ = _tail(out)
            stderr, skipped = _tail(err)
            return proc.returncode, stdout, stderr, skipped
    finally:
        shutil.rmtree(out_dir, ignore_errors=True)


def _run(args) -> int:
    tree = Path(args.upstream).expanduser().resolve()
    ok, reason = marker_ok(tree)
    if not ok:
        raise SetupError(reason)
    missing = missing_upstream_api(tree)
    if missing:
        raise SetupError("pinned upstream API missing from store.py: " + ", ".join(missing))
    selected = _selected(args.scenario)
    if args.json_out:
        out_dir = Path(args.json_out).expanduser().resolve().parent
        if not out_dir.is_dir():
            raise SetupError(f"--json: directory {out_dir} does not exist")

    pkg_parent = str(Path(__file__).resolve().parent.parent)
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    env["PYTHONPATH"] = pkg_parent + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")

    results = {}
    setup_error = None
    stop = _ParentSignals()  # SIGTERM/SIGHUP: stop the worker, clean up, report, end by it
    stop.install()
    raised = None  # a SetupError leaving `_run`
    try:
        last = selected[0]
        try:
            for sid in selected:
                last = sid
                if stop.signum is not None:  # arrived between scenarios
                    setup_error = f"{sid}: interrupted by {stop.name()}"
                    break
                workdir = _new_workdir(sid)
                cmd = [sys.executable, "-m", "consumer_lab", "_worker", "--upstream", str(tree),
                       "--client", args.client, "--scenario", sid, "--workdir", str(workdir)]
                if args.keep_root:
                    cmd.append("--keep-root")
                ran = _run_worker(cmd, env, stop)
                if stop.signum is not None:  # the worker was stopped (or had just ended)
                    setup_error = f"{sid}: interrupted by {stop.name()}"
                    if ran is not None:
                        _echo_worker_stderr(ran[2], ran[3])
                    if args.keep_root:
                        # Its `finally` ran (and printed `kept`) unless a signal other than SIGINT
                        # ended it.
                        own = ran is not None and (ran[0] >= 0 or ran[0] == -signal.SIGINT)
                        _keep_result_json(workdir, sid, setup_error, worker_wrote_its_own=own)
                    else:
                        shutil.rmtree(workdir, ignore_errors=True)
                    break
                if ran is None:
                    setup_error = f"{sid}: worker did not finish within {WORKER_TIMEOUT}s"
                    if args.keep_root:
                        _keep_result_json(workdir, sid, setup_error)
                    else:
                        shutil.rmtree(workdir, ignore_errors=True)
                    break
                returncode, stdout, stderr, skipped = ran
                if not args.keep_root:
                    shutil.rmtree(workdir, ignore_errors=True)  # the worker removes it; this is the backstop
                _echo_worker_stderr(stderr, skipped)
                if returncode != EXIT_PASS:
                    detail = _worker_failure(returncode, stderr)
                    setup_error = f"{sid}: worker exited {returncode}: {detail}"
                    if args.keep_root and returncode < 0:
                        # Most likely killed before it wrote one; O_EXCL keeps one it did write.
                        own = returncode == -signal.SIGINT  # its `finally` ran and printed `kept`
                        _keep_result_json(workdir, sid, setup_error, worker_wrote_its_own=own)
                    break
                result = _worker_result(stdout)
                if result is None:
                    setup_error = f"{sid}: worker printed no valid result"
                    break
                results[sid] = result
                print(f"{sid} {result['name']:<26} {result['verdict']:<22} {result['reason']}")
        except SetupError as exc:
            if stop.signum is None:
                raise
            # A signal was recorded: this error's line now, then the report as interrupted.
            print(f"setup error: {exc}", file=sys.stderr)
            setup_error = f"{last}: interrupted by {stop.name()}"

        upstream = upstream_commit(tree)
        # The final decision about a recorded signal, immediately before the report is written
        # (after the last scenario, too). A signal recorded from here on still ends the process
        # by it, with the report and the lines as written.
        if setup_error is None and stop.signum is not None:
            setup_error = f"{last}: interrupted by {stop.name()}"
        report = {
            "kit_version": __version__,
            "pinned_commit": PINNED_COMMIT,
            "upstream_commit": upstream,
            "client": args.client,
            "scenarios": results,
            "selected": selected,
            "setup_error": setup_error,
            "all_pass": setup_error is None and all(r["verdict"] == "PASS" for r in results.values()),
        }
        if args.json_out:
            try:
                Path(args.json_out).expanduser().write_text(
                    json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8"
                )
            except OSError as exc:
                error = SetupError(f"--json: cannot write {args.json_out}: {exc.strerror}")
                if stop.signum is None:
                    raise error from exc
                print(f"setup error: {error}", file=sys.stderr)  # this run ends by the signal below
        if setup_error:
            print(f"setup error: {setup_error}", file=sys.stderr)
            return EXIT_SETUP
        passed = sum(r["verdict"] == "PASS" for r in results.values())
        print(f"{passed}/{len(results)} PASS (upstream {report['upstream_commit']})")
        return EXIT_PASS if report["all_pass"] else EXIT_FAIL
    except SetupError as exc:
        raised = exc
        raise
    finally:
        stop.restore()
        if stop.signum is not None:
            if raised is not None:  # `main` would print it only after this process ended
                print(f"setup error: {raised}", file=sys.stderr)
            stop.end()


def _echo_worker_stderr(stderr: str, skipped: int) -> None:
    if skipped:
        sys.stderr.write(f"[worker stderr: {skipped} earlier bytes not shown]\n")
    if stderr.strip():
        sys.stderr.write(stderr if stderr.endswith("\n") else stderr + "\n")  # own lines start fresh


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "_worker":
            return _worker(args)
        return _run(args)
    except SetupError as exc:
        if args.command == "_worker":
            # The client shares this stderr: start a fresh line, so that a partial line it left
            # cannot become the start of the line `_worker_failure` reads.
            sys.stderr.write("\n")
        print(f"setup error: {exc}", file=sys.stderr)
        return EXIT_SETUP
