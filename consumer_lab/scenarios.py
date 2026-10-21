"""The 5 reader fault scenarios. Each drives one room through HTTP writes plus a few private
store calls, runs client cycles in between, and returns the truth computed from store state.

Private upstream calls used (pinned at upstream commit 4a013e5; see server.PINNED_COMMIT):
`store._compact` (S2), `store._cutoff` via `store.EPHEMERAL_TTL_SECONDS` (S3, S4),
`store._reap_pass` with `store.IDLE_SECONDS` (S5), plus the public-but-internal
`store.export_room`, `store.read_messages`, `store.room_generation`, `store.room_path` for truth.
"""

from __future__ import annotations

import errno
import json
import os
import shlex
import signal
import stat
import subprocess
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

from consumer_lab.judge import Cycle, Gap, Truth
from consumer_lab.server import SetupError, Upstream

CYCLE_TIMEOUT = 30.0
_NO_PROXY = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class NotDeterministic(SetupError):
    """A scenario precondition did not hold in store state: the run proves nothing."""


class WorkdirReplaced(SetupError):
    """The client replaced the scenario's workdir or its `root` during a cycle (`check_workdir`).
    Whether the worker stops upstream without its shutdown hooks (which would write the store by
    path) is not decided by this exception: the workers' `finally` asks `workdir_unchanged`,
    whatever ended the run."""


@dataclass(frozen=True)
class Scenario:
    id: str
    name: str
    room: str
    run: Callable[["Kit"], Truth]
    needs: tuple[str, ...]  # private store names this scenario relies on


class Kit:
    """What a scenario gets: the live upstream, HTTP writes, and client cycles."""

    def __init__(self, up: Upstream, client_cmd: str, workdir: Path, room: str, cycle_timeout: float = CYCLE_TIMEOUT):
        self.workdir = workdir
        self._workdir_id = workdir_identity(workdir)  # checked after every client cycle
        self.up = up
        self.store = up.store
        self.root = up.root
        self.room = room
        self.client_argv = client_argv(client_cmd)
        self.state_dir = workdir / "state"
        self.state_dir.mkdir()
        self.events_path = workdir / "events.jsonl"
        self.events_path.touch()
        self.stderr_path = workdir / "client.stderr"  # kept with --keep-root, for debugging
        self.cycle_timeout = cycle_timeout
        self.cycles: list[Cycle] = []
        self._log_prefix = b""  # the log's bytes as of the end of the last cycle
        self._n = 0

    # ------------------------------------------------------------------ writes (HTTP)

    def post(self, count: int) -> list[dict]:
        """Append `count` records over HTTP. Returns [{seq, ts}] for each, from the reply."""
        out = []
        for _ in range(count):
            self._n += 1
            body = json.dumps({"from": "lab", "text": f"consumer-lab record {self._n}"}).encode()
            req = urllib.request.Request(
                f"{self.up.url}/r/{self.room}?format=json",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with _NO_PROXY.open(req, timeout=10) as resp:
                posted = json.loads(resp.read())["posted"]
            out.append({"seq": posted["seq"], "ts": posted["ts"]})
        return out

    # ------------------------------------------------------------------ cycles

    def cycle(self) -> Cycle:
        before = self.events_path.read_bytes()
        intact_before = before.startswith(self._log_prefix)
        first = _line_count(self.events_path)
        env = client_env(
            {
                "TECHNOCORE_URL": self.up.url,
                "TC_ROOM": self.room,
                "TC_STATE_DIR": str(self.state_dir),
                "TC_EVENTS": str(self.events_path),
            }
        )
        exit_code, timed_out = run_client(self.client_argv, env, self.stderr_path, self.cycle_timeout)
        check_workdir(self.workdir, self._workdir_id)  # before anything else touches it
        after = self.events_path.read_bytes()
        # Append-only: what the log held before this cycle must still be its prefix, byte for
        # byte. (A rewrite to identical bytes is indistinguishable from an append, and harmless.)
        intact = intact_before and after.startswith(before)
        self._log_prefix = after
        c = Cycle(exit_code, timed_out, first, _line_count(self.events_path), log_intact=intact)
        self.cycles.append(c)
        return c

    # ------------------------------------------------------------------ store truth

    def log_intact(self) -> bool:
        """Whether the log still starts with what it held at the end of the last cycle."""
        return self.events_path.read_bytes().startswith(self._log_prefix)

    def export_keys(self) -> list[tuple[int, int]]:
        """(generation, seq) of every record the store would export now."""
        generation, chunks = self.store.export_room(self.root, self.room)
        data = b"".join(chunks)
        return [(generation, json.loads(line)["seq"]) for line in data.splitlines() if line.strip()]

    def readable_after(self, since: int) -> list[int]:
        view = self.store.read_messages(self.root, self.room, limit=self.store.MAX_LIMIT, since=since)
        return [m["seq"] for m in view["messages"]]

    def generation(self) -> int:
        return self.store.room_generation(self.root, self.room)

    def require(self, condition: bool, what: str) -> None:
        if not condition:
            raise NotDeterministic(f"{self.room}: precondition failed: {what}")

    def freeze_cutoff(self, cutoff: float) -> None:
        """Make `store._cutoff` return exactly `cutoff` for every `e-` room, until reset.

        At the pin, `_cutoff` is `time.time() - EPHEMERAL_TTL_SECONDS if is_ephemeral(room)`,
        and it reads the module global at call time. A float TTL would let the cutoff drift
        forward with the wall clock while a client cycle runs, so the verdict would depend on
        how long the cycle took. `_FrozenTTL` is a float whose reflected subtraction returns
        the fixed cutoff, so `time.time() - TTL` is the same instant on every call.
        """
        self.store.EPHEMERAL_TTL_SECONDS = _FrozenTTL(cutoff)
        got = self.store._cutoff(self.room)
        self.require(got == cutoff, f"_cutoff did not honour the frozen TTL ({got!r} != {cutoff!r})")


# ---------------------------------------------------------------------- client launch (shared)


def client_argv(client_cmd: str) -> list[str]:
    """The `--client` command split like a shell would, `~` expanded; never run by a shell."""
    argv = [os.path.expanduser(a) for a in shlex.split(client_cmd)]
    if not argv:
        raise SetupError("empty --client command")
    return argv


WorkdirId = tuple[tuple[int, int], tuple[int, int]]


def _dir_id(path: Path) -> tuple[int, int]:
    """(st_dev, st_ino) of the directory `path` itself, never following a link; a SetupError
    if it is not a directory."""
    try:
        st = os.lstat(path)
    except OSError as exc:
        raise SetupError(f"workdir {path} cannot be read: {exc.strerror}") from exc
    if not stat.S_ISDIR(st.st_mode):
        raise SetupError(f"workdir {path} is not a directory")
    return st.st_dev, st.st_ino


def _same_dir(path: Path, identity: tuple[int, int]) -> bool:
    try:
        st = os.lstat(path)
    except OSError:
        return False
    return stat.S_ISDIR(st.st_mode) and (st.st_dev, st.st_ino) == identity


def workdir_identity(workdir: Path) -> WorkdirId:
    """(st_dev, st_ino) of the scenario's workdir and of its store root `<workdir>/root`, each
    the directory itself, never following a link; recorded before the first client cycle, after
    `root` was created. A SetupError if either is not a directory."""
    return _dir_id(workdir), _dir_id(workdir / "root")


def check_workdir(workdir: Path, identity: WorkdirId) -> None:
    """Right after a client cycle: a SetupError unless `workdir` and `<workdir>/root` are still
    the directories `workdir_identity` recorded (not missing, not a link, same st_dev and
    st_ino). The client knows the path (from TC_EVENTS) and could rename either and leave a link
    to somewhere else; the store and the kit write by path, so the run stops here
    (`WorkdirReplaced`, naming what was replaced). This detects a swap after the cycle; it does
    not prevent what the store writes there because of the client's own requests."""
    if not _same_dir(workdir, identity[0]):
        raise WorkdirReplaced(f"workdir {workdir} was replaced during the client cycle")
    if not _same_dir(workdir / "root", identity[1]):
        raise WorkdirReplaced(f"store root {workdir / 'root'} was replaced during the client cycle")


def workdir_unchanged(workdir: Path, identity: WorkdirId) -> bool:
    """Whether `workdir` and `<workdir>/root` are still the directories `workdir_identity`
    recorded (not missing, not a link, same st_dev and st_ino). Also asked by the worker before
    it stops upstream, however the run ended: its shutdown hooks write the store by path."""
    return _same_dir(workdir, identity[0]) and _same_dir(workdir / "root", identity[1])


def client_env(contract: dict[str, str]) -> dict[str, str]:
    """The kit's env minus every CHAT_* server knob, plus the contract's variables and a
    loopback NO_PROXY, so a client never sees the server's config or routes via a proxy."""
    return {
        **{k: v for k, v in os.environ.items() if not k.startswith("CHAT_")},
        **contract,
        "NO_PROXY": "127.0.0.1,localhost",
        "no_proxy": "127.0.0.1,localhost",
    }


# The asynchronous, catchable signals that end or stop a process by default (SIGINT: by
# KeyboardInterrupt), by name where the platform has them, plus every real-time signal and every
# unnamed number below SIGRTMIN. A client can send them to its worker; during a cycle they are
# recorded, and after the group kill the terminating ones are re-delivered and the stop signals
# (STOP_SIGNALS) are dropped, with a kit line on stderr. `run_client` tries each one: a number the
# platform refuses a handler for (EINVAL, or ValueError) is skipped and added to UNDEFERRABLE;
# these are the signals reserved by the C library (32 and 33 on glibc). Not here: SIGKILL and
# SIGSTOP (cannot be caught), and the fault signals SIGSEGV, SIGBUS, SIGFPE, SIGILL, SIGTRAP,
# SIGABRT and SIGSYS (a real fault under a recording handler could loop or hide a crash). Those,
# and the C library's reserved signals, can still end (or stop) the worker before the group kill.
_DEFERRED_NAMES = (
    "SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT", "SIGUSR1", "SIGUSR2",
    "SIGALRM", "SIGVTALRM", "SIGPROF", "SIGXCPU", "SIGIO", "SIGPOLL", "SIGPWR", "SIGSTKFLT",
    "SIGTSTP", "SIGTTIN", "SIGTTOU",
)


def _deferred_signals() -> tuple[int, ...]:
    sigs = [int(getattr(signal, name)) for name in _DEFERRED_NAMES if hasattr(signal, name)]
    if hasattr(signal, "SIGRTMIN") and hasattr(signal, "SIGRTMAX"):
        named = {int(s) for s in signal.Signals}
        sigs += [n for n in range(1, int(signal.SIGRTMIN)) if n not in named]  # e.g. 32, 33 on Linux
        sigs += range(int(signal.SIGRTMIN), int(signal.SIGRTMAX) + 1)
    return tuple(dict.fromkeys(sigs))  # SIGIO and SIGPOLL can be one number


DEFERRED_SIGNALS = _deferred_signals()  # the candidates; minus UNDEFERRABLE, the deferred set
UNDEFERRABLE: set[int] = set()  # candidates the platform refused a handler for, as found
# Recorded like the others but never re-delivered: stopping the worker after the cycle would only
# stall the run until WORKER_TIMEOUT, and could hide a terminating signal recorded with it.
STOP_SIGNALS = frozenset(int(getattr(signal, n)) for n in ("SIGTSTP", "SIGTTIN", "SIGTTOU") if hasattr(signal, n))


def start_kit_thread(thread: threading.Thread) -> None:
    """Start `thread` with every signal in `DEFERRED_SIGNALS` blocked in it for its whole life.

    A new thread inherits the signal mask of the thread that starts it, so the signals are
    blocked in the calling thread around `thread.start()` and the caller's own mask is put back
    afterwards (a signal that arrived meanwhile is delivered then). Threads that `thread` starts
    in turn inherit its mask. The upstream server thread and the fault proxy's serve thread are
    started this way, so they and the threads they start (asyncio's and anyio's worker threads,
    the proxy's request threads) never take a process-directed deferred signal; of the kit's
    threads only the main thread can. While `run_client` restores the handlers the main thread
    blocks them too, so such a signal then stays pending for the process, provided no thread
    started by other means (not covered here) leaves it unblocked. A deferred signal sent to
    one of these threads itself (tgkill) stays pending in that thread and is never handled.
    The C library's reserved numbers (32 and 33 on glibc) are left out: they are not valid
    signals to block.
    """
    old = signal.pthread_sigmask(signal.SIG_BLOCK, set(DEFERRED_SIGNALS) & signal.valid_signals())
    try:
        thread.start()
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, old)


def run_client(argv: list[str], env: dict[str, str], stderr_path: Path, timeout: float) -> tuple[int | None, bool]:
    """Run one client cycle in its own session. Returns (exit code or None, timed out).

    stdout is discarded and stderr appended to `stderr_path`. After every cycle the whole
    process group gets SIGKILL before this returns: on timeout, after a normal exit, and also
    when the wait raises, so a helper the client started in its group cannot keep running into
    the next step (a process that left the group, e.g. via `setsid`, is not reached).

    While the cycle runs, the asynchronous terminating and stop signals in `DEFERRED_SIGNALS`
    (which a client can send to this process) are only recorded. After the group kill and the
    reap, the previous handlers come back, SIGINT's last. Meanwhile this thread blocks the
    deferred signals, and the kit's other threads block them for their whole life
    (`start_kit_thread`), so one sent to this thread or to the process then stays pending
    (unless a thread started by other means leaves it unblocked) and reaches its restored
    handler once the mask is back. Then each recorded terminating signal is re-delivered once,
    in the order this process handled them (not necessarily the order they were sent), so this
    process ends as it would have, but only after the client's group is dead. A recorded stop
    signal (`STOP_SIGNALS`) is dropped, with a kit line on stderr naming it. Not deferred:
    SIGKILL and SIGSTOP, which cannot be caught; the fault signals SIGSEGV, SIGBUS, SIGFPE,
    SIGILL, SIGTRAP, SIGABRT and SIGSYS; and the signals reserved by the C library (32 and 33 on
    glibc), which refuse a handler (`UNDEFERRABLE`). A client that sends one of those can
    still end (or stop) this worker before the kill and leave its group alive.
    Must run on the main thread (Python handlers are set and run there), else a SetupError.
    """
    if threading.current_thread() is not threading.main_thread():
        raise SetupError("run_client must run on the main thread: it defers the client's signals there")
    arrived: list[int] = []
    previous: dict[int, object] = {}
    try:
        for sig in DEFERRED_SIGNALS:
            try:
                # Minimal on purpose: runs on the main thread between bytecodes, at any point.
                previous[sig] = signal.signal(sig, lambda signum, _frame: arrived.append(signum))
            except ValueError:  # not a signal Python can handle here (the thread was checked)
                UNDEFERRABLE.add(sig)
            except OSError as exc:
                if exc.errno != errno.EINVAL:
                    raise SetupError(f"cannot defer signal {sig}: {exc.strerror}") from exc
                UNDEFERRABLE.add(sig)  # reserved by the C library (32 and 33 on glibc)
        try:
            with open(stderr_path, "ab") as stderr:
                proc = subprocess.Popen(
                    argv,
                    env=env,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=stderr,
                    start_new_session=True,
                )
        except OSError as exc:
            raise SetupError(f"cannot start client {argv[0]!r}: {exc.strerror}") from exc
        try:
            exit_code, timed_out = proc.wait(timeout=timeout), False
        except subprocess.TimeoutExpired:
            exit_code, timed_out = None, True
        finally:
            # Also when the wait raised: the group dies before the exception goes on.
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass  # the group is already empty (or its id now belongs to someone else)
            proc.wait()
    finally:
        # This thread blocks every deferred signal while the handlers are restored. The kit's
        # other threads block them for their whole life (start_kit_thread), so one sent to this
        # thread or to the process meanwhile (even inside signal.signal(), between its pending
        # check and the swap) stays pending and reaches the restored handler, once, when the
        # old mask is back. A thread started by other means that leaves it unblocked could
        # still take it. SIGINT's handler is still restored last.
        old_mask = signal.pthread_sigmask(signal.SIG_BLOCK, previous) if previous else None
        raised: BaseException | None = None
        try:
            for sig in sorted(previous, key=lambda s: s == signal.SIGINT):
                handler = previous[sig]
                signal.signal(sig, signal.SIG_DFL if handler is None else handler)
        finally:
            if old_mask is not None:
                try:
                    signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)  # pending ones run here
                except BaseException as exc:  # noqa: BLE001 - KeyboardInterrupt; raised below
                    raised = exc
        dropped = [sig for sig in dict.fromkeys(arrived) if sig in STOP_SIGNALS]
        if dropped:
            names = ", ".join(signal.Signals(sig).name for sig in dropped)
            start_fresh_stderr_line()
            sys.stderr.write(f"stop signal during the client cycle not re-delivered: {names}\n")
            sys.stderr.flush()
        try:
            _redeliver([sig for sig in arrived if sig not in STOP_SIGNALS])
        except BaseException as exc:  # noqa: BLE001 - raised below, after the rest
            raised = raised or exc
        if raised is not None:
            raise raised
    return exit_code, timed_out


def start_fresh_stderr_line() -> None:
    r"""Write one "\n" to stderr if fd 2 is a regular file that already holds bytes and whose
    last byte is not a newline. The client shares the worker's stderr (it can write to
    /proc/<ppid>/fd/2), so a partial line it left would otherwise become the start of the
    kit's next line. When fd 2 is not a regular file (or cannot be read back), nothing extra
    is written; output that already ends with a newline gains no blank line."""
    try:
        sys.stderr.flush()
        st = os.fstat(2)
        if not stat.S_ISREG(st.st_mode) or st.st_size == 0:
            return
        last = os.pread(2, 1, st.st_size - 1)
    except (OSError, ValueError):
        return
    if last and last != b"\n":
        sys.stderr.write("\n")
        sys.stderr.flush()


def _redeliver(arrived: list[int]) -> None:
    """Send each recorded signal once more to this process, in the order this process handled
    them (not necessarily the order they were sent), now that the client's group is dead and
    the previous handlers are back. A default-terminating one ends
    the process here; an exception a handler raises (KeyboardInterrupt) goes on after the rest
    were re-delivered."""
    raised: BaseException | None = None
    for sig in dict.fromkeys(arrived):
        try:
            os.kill(os.getpid(), sig)
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            raised = raised or exc
    if raised is not None:
        raise raised


class _FrozenTTL(float):
    def __new__(cls, cutoff: float) -> "_FrozenTTL":
        obj = super().__new__(cls, time.time() - cutoff)  # the nominal TTL, for display
        obj.cutoff = cutoff
        return obj

    def __rsub__(self, now: float) -> float:  # time.time() - self
        return self.cutoff


def _ts(record: dict) -> float:
    """A record's `ts` as epoch seconds, parsed the way upstream's `_expired` parses it."""
    return datetime.strptime(record["ts"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC).timestamp()


def _line_count(path: Path) -> int:
    with open(path, "rb") as f:
        return sum(1 for _ in f)


def _keys(generation: int, seqs) -> list[tuple[int, int]]:
    return [(generation, s) for s in seqs]


# ---------------------------------------------------------------------- the scenarios


def s1_lag_within_ring(kit: Kit) -> Truth:
    kit.post(10)
    kit.cycle()
    kit.post(300)
    kit.require(kit.store.MAX_LIMIT == 200, "MAX_LIMIT is 200")
    retained = kit.export_keys()
    gen = kit.generation()
    kit.require(retained == _keys(gen, range(1, 311)), "all 310 records retained")
    kit.cycle()
    return Truth(expected=retained)


def s2_lag_beyond_retention(kit: Kit) -> Truth:
    kit.post(10)
    kit.cycle()
    kit.post(300)
    gen = kit.generation()
    # Keep exactly the newest 250 records (61..310): more than one MAX_LIMIT window, so a
    # reader must use /export to see them all, and starting well above cursor + 1 = 11.
    path = kit.store.room_path(kit.root, kit.room)
    lines = path.read_bytes().splitlines()
    keep = sum(len(line) + 1 for line in lines[-250:])
    # `_compact` documents "caller holds the lock". No other writer exists here: the kit is
    # the only poster and no client cycle is running.
    kit.store._compact(path, keep=keep)
    retained = kit.export_keys()
    kit.require(retained == _keys(gen, range(61, 311)), "compaction kept exactly seq 61..310")
    kit.cycle()
    return Truth(
        expected=_keys(gen, range(1, 11)) + retained,
        lost=set(_keys(gen, range(11, 61))),
        gaps=[Gap(after_seq=10, generation=gen, observable_cycle=1)],
    )


def _ephemeral_common(kit: Kit, empty_poll_first: bool) -> Truth:
    first = kit.post(3)
    kit.cycle()
    second = kit.post(3)
    gen = kit.generation()
    ts6 = _ts(second[-1])
    if empty_poll_first:
        # Every record so far is expired: the cutoff sits a second after record 6.
        kit.freeze_cutoff(ts6 + 1.0)
        kit.require(kit.readable_after(3) == [], "records 4..6 are expired")
        kit.require(kit.export_keys() == [], "export holds nothing live")
        kit.cycle()
    live = kit.post(1)
    ts7 = _ts(live[0])
    kit.require(ts7 > ts6, "record 7 is stamped after record 6")
    # The cutoff between records 6 and 7: 4..6 stay expired, 7 is live.
    kit.freeze_cutoff((ts6 + ts7) / 2)
    kit.require(kit.readable_after(3) == [7], "only record 7 is readable after seq 3")
    kit.require(kit.export_keys() == [(gen, 7)], "export holds only record 7")
    kit.cycle()
    kit.require([r["seq"] for r in first + second + live] == list(range(1, 8)), "seqs 1..7")
    return Truth(
        expected=_keys(gen, [1, 2, 3, 7]),
        lost=set(_keys(gen, [4, 5, 6])),
        # The gap exists from the first cycle after the kit expired 4..6, i.e. the second cycle,
        # in both S3 and S4. In S3 the room read is an empty window with last_seq 3 there, but
        # `/rooms?format=json` already lists the room's on-disk head as 6, so a client can see
        # it. S4's second cycle sees first_seq 7.
        gaps=[Gap(after_seq=3, generation=gen, observable_cycle=1)],
    )


def s3_ephemeral_all_expired(kit: Kit) -> Truth:
    return _ephemeral_common(kit, empty_poll_first=True)


def s4_ephemeral_prefix_expired(kit: Kit) -> Truth:
    return _ephemeral_common(kit, empty_poll_first=False)


def s5_room_recreated(kit: Kit) -> Truth:
    kit.post(5)
    gen_old = kit.generation()
    old = kit.export_keys()
    kit.require(old == _keys(gen_old, range(1, 6)), "5 records in the first generation")
    kit.cycle()
    kit.store._reap_pass(kit.root, time.time() + kit.store.IDLE_SECONDS + 1)
    path = kit.store.room_path(kit.root, kit.room)
    kit.require(not path.exists() or kit.generation() != gen_old, "the reap removed the room")
    new = kit.post(3)
    gen_new = kit.generation()
    kit.require(gen_new != gen_old, f"generation changed on recreate ({gen_old} -> {gen_new})")
    recreated = kit.export_keys()
    kit.require(recreated == _keys(gen_new, [r["seq"] for r in new]), "export holds the 3 new records")
    kit.cycle()
    return Truth(
        expected=old + recreated,
        gaps=[
            Gap(
                after_seq=old[-1][1],
                generation=gen_old,
                reason_any=("generation", "recreated"),
                observable_cycle=1,
            )
        ],
    )


SCENARIOS: dict[str, Scenario] = {
    s.id: s
    for s in (
        Scenario("S1", "lag-within-ring", "lab-s1", s1_lag_within_ring, ("MAX_LIMIT", "export_room")),
        Scenario("S2", "lag-beyond-retention", "lab-s2", s2_lag_beyond_retention, ("_compact", "room_path", "export_room")),
        Scenario("S3", "ephemeral-all-expired", "e-lab-s3", s3_ephemeral_all_expired, ("_cutoff", "EPHEMERAL_TTL_SECONDS", "read_messages", "export_room")),
        Scenario("S4", "ephemeral-prefix-expired", "e-lab-s4", s4_ephemeral_prefix_expired, ("_cutoff", "EPHEMERAL_TTL_SECONDS", "read_messages", "export_room")),
        Scenario("S5", "room-recreated", "lab-s5", s5_room_recreated, ("_reap_pass", "IDLE_SECONDS", "room_generation", "room_path", "export_room")),
    )
}
