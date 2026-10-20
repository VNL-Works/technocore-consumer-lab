"""The 8 writer fault scenarios (W0..W7): one message, one client cycle, through the proxy.

Per scenario: a fresh plain room `lab-wN`; one seed record posted straight to upstream (so
the room exists and a read that follows a redirect reads a real room); then one client cycle
through a `FaultProxy` whose plan injects the fault; then `proxy.wait_idle`, and the truth:
how many records in the room hold exactly the text the client was given, and how many other
non-seed records it holds, read in-process with `store.read_messages`, never through the proxy.
The seed is the record with the `seq` upstream gave the kit's seed POST, not any record with the
seed text: a client that re-posts the seed text has posted another record.
The proxy's log goes into the report only through `report_log`, which keeps no client-chosen
path or method.

Only public-but-internal upstream calls are used here (`store.read_messages`,
`store.clean_text`, `store.MAX_LIMIT`); no private function, no store mutation.
"""

from __future__ import annotations

import json
import secrets
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from consumer_lab import proxy as P
from consumer_lab.judge import Cycle
from consumer_lab.scenarios import (
    _NO_PROXY,
    CYCLE_TIMEOUT,
    NotDeterministic,
    client_argv,
    check_workdir,
    client_env,
    run_client,
    workdir_identity,
)
from consumer_lab.server import Upstream
from consumer_lab.writer_judge import WriteTruth, parse_write_events

WRITE_TIMEOUT = 3  # seconds, given to the client as TC_WRITE_TIMEOUT
WRITE_FROM = "lab-writer"
SEED_FROM = "lab"
SEED_TEXT = "consumer-lab seed"
IDLE_TIMEOUT = 10.0


@dataclass(frozen=True)
class WriterScenario:
    id: str
    name: str
    room: str
    plan: P.Plan
    # The precondition on the first POST /r/{room}, when there was one: True = it must have
    # been forwarded and answered 200 by the origin; False = it must not have been forwarded;
    # None = no precondition (W0, the clean control).
    first_post_forwarded: bool | None
    needs: tuple[str, ...] = ("read_messages", "clean_text", "MAX_LIMIT")


@dataclass
class WriterRun:
    truth: WriteTruth
    cycle: Cycle
    lines: list[str]  # the event log as it stood when the cycle ended
    outcome: str | None  # the client's claimed outcome, for the report (None if no valid event)


def _plan(room: str, first: P.Action, **later) -> P.Plan:
    return P.Plan(room=room, first_post=first, **later)


def _scenarios() -> dict[str, WriterScenario]:
    out = {}
    for sid, name, first, forwarded, later in (
        ("W0", "clean", P.PASS, None, {}),
        ("W1", "redirect-303-after-write", P.redirect_after_write(303), True, {}),
        ("W2", "redirect-307-after-write", P.redirect_after_write(307), True, {}),
        ("W3", "timeout-after-write", P.stall_after_write(10), True, {}),
        ("W4", "truncated-after-write", P.truncate_after_write(), True, {}),
        ("W5", "reset-before-write", P.reset_before_write(), False, {}),
        ("W6", "redirect-without-write", P.redirect_without_write(303), False, {}),
        (
            "W7",
            "unreadable-after-write",
            P.truncate_after_write(),
            True,
            {"later_posts": P.reset_before_write(), "room_reads": P.unavailable(503)},
        ),
    ):
        room = f"lab-{sid.lower()}"
        out[sid] = WriterScenario(sid, name, room, _plan(room, first, **later), forwarded)
    return out


WRITER_SCENARIOS: dict[str, WriterScenario] = _scenarios()


def _seed(up: Upstream, room: str) -> int:
    """One record straight to upstream (not through the proxy), so the room exists.

    Returns the `seq` upstream gave it, from the POST response: the kit tells its seed from
    every other record by that seq (a client can read the seed text and post it again).
    """
    body = json.dumps({"from": SEED_FROM, "text": SEED_TEXT}).encode()
    req = urllib.request.Request(
        f"{up.url}/r/{room}?format=json",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with _NO_PROXY.open(req, timeout=10) as resp:
        posted = json.loads(resp.read())["posted"]
    if posted.get("text") != SEED_TEXT:
        raise NotDeterministic(f"{room}: precondition failed: the seed was stored as another text")
    seq = posted.get("seq")
    if not isinstance(seq, int) or isinstance(seq, bool):
        raise NotDeterministic(f"{room}: precondition failed: the seed POST's response carries no integer seq")
    return seq


def _records(up: Upstream, room: str) -> list[tuple[object, object]]:
    """`(seq, text)` of every record in the room, in store order."""
    limit = up.store.MAX_LIMIT
    view = up.store.read_messages(up.root, room, limit=limit)
    messages = view["messages"]
    if len(messages) >= limit:
        # The read may be cut off at the limit, so no count taken from it can be trusted.
        raise NotDeterministic(
            f"{room}: the room holds too many records to count ({len(messages)} read, limit {limit})"
        )
    return [(m.get("seq"), m.get("text")) for m in messages]


REPORT_METHODS = frozenset(("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "?"))
OTHER = "<other>"


def report_entry(entry: dict, room: str) -> dict:
    """One proxy log entry as the report shows it: nothing the client chose verbatim.

    The path is reported as upstream routes it (`room_request_path`) when that is exactly
    `/r/{room}`, `/r/{room}/export` or `/rooms`, else as `<other>`; the method is kept only if
    it is a standard one (or the proxy's `?` for a request it could not parse), else `<other>`.
    `raw_path_differs` says whether the client's raw path differed from the reported one.
    """
    raw = entry["path_without_query"]
    routed = P.room_request_path(raw)
    path = routed if routed in (f"/r/{room}", f"/r/{room}/export", "/rooms") else OTHER
    method = entry["method"] if entry["method"] in REPORT_METHODS else OTHER
    return {
        "method": method,
        "path_without_query": path,
        "raw_path_differs": raw != path,
        "action": entry["action"],
        "forwarded": entry["forwarded"],
        "origin_status": entry["origin_status"],
    }


def report_log(log: list[dict], room: str) -> list[dict]:
    """The proxy log for the report (`report_entry` on each entry). The raw log stays internal."""
    return [report_entry(e, room) for e in log]


def _require(cond: bool, room: str, what: str) -> None:
    if not cond:
        raise NotDeterministic(f"{room}: precondition failed: {what}")


def _read_events(path: Path) -> bytes:
    """The event log's bytes; a log the client deleted or made unreadable reads as empty, so
    the client's fault is judged (crash / bad_events), not reported as the kit's."""
    try:
        return path.read_bytes()
    except OSError:
        return b""


def _room_posts(log: list[dict], room: str) -> list[dict]:
    """Proxy log entries for POSTs to the room, matched as upstream routes the path."""
    want = f"/r/{room}"
    return [e for e in log if e["method"] == "POST" and P.room_request_path(e["path_without_query"]) == want]


def check_fault_applied(scenario: WriterScenario, log: list[dict], stored: int) -> None:
    """The preconditions on the first POST, from the proxy log (raises NotDeterministic).

    - W1..W7: once the message is stored, the scenario's fault must have been applied to a
      room POST. A write that reached upstream without the fault proves nothing (fail closed).
    - W1..W4, W7: if a room POST arrived, the first one was forwarded, answered 200, and stored.
    - W5, W6: if a room POST arrived, the first one was not forwarded.
    With no room POST and nothing stored (a client that crashed first), there is nothing to
    check: the cycle is judged as is.
    """
    room = scenario.room
    posts = _room_posts(log, room)
    first_post = posts[0] if posts else None
    if scenario.first_post_forwarded is not None and stored >= 1:
        label = scenario.plan.first_post.label
        _require(
            any(e["action"] == label for e in posts),
            room,
            f"the message was stored without passing the scenario's fault ({label})",
        )
    if first_post is not None and scenario.first_post_forwarded is True:
        _require(
            first_post["forwarded"] and first_post["origin_status"] == 200,
            room,
            f"the first POST was forwarded and answered 200 (forwarded={first_post['forwarded']}, "
            f"origin_status={first_post['origin_status']})",
        )
        _require(stored >= 1, room, "the forwarded first POST is stored")
    if first_post is not None and scenario.first_post_forwarded is False:
        _require(not first_post["forwarded"], room, "the first POST was not forwarded")


def run_writer(scenario: WriterScenario, up: Upstream, client_cmd: str, workdir: Path) -> WriterRun:
    """Seed, run one client cycle through the proxy, wait for the proxy, compute the truth."""
    argv = client_argv(client_cmd)
    room = scenario.room
    write_id = secrets.token_hex(8)
    text = f"consumer-lab write {scenario.id} {secrets.token_hex(8)}"
    # The upstream sweep must store this text unchanged, or `stored` would count nothing.
    _require(up.store.clean_text(text) == text, room, "upstream clean_text keeps the write text unchanged")

    workdir_id = workdir_identity(workdir)  # checked right after the client cycle
    state_dir = workdir / "state"
    state_dir.mkdir()
    events_path = workdir / "events.jsonl"
    events_path.touch()
    stderr_path = workdir / "client.stderr"  # kept with --keep-root, for debugging

    seed_seq = _seed(up, room)
    before = _records(up, room)
    _require(before == [(seed_seq, SEED_TEXT)], room, "the room holds exactly the seed before the cycle")

    proxy = P.FaultProxy(up.url, scenario.plan)
    try:
        env = client_env(
            {
                "TECHNOCORE_URL": proxy.url,
                "TC_ROOM": room,
                "TC_STATE_DIR": str(state_dir),
                "TC_EVENTS": str(events_path),
                "TC_WRITE_ID": write_id,
                "TC_WRITE_FROM": WRITE_FROM,
                "TC_WRITE_TEXT": text,
                "TC_WRITE_TIMEOUT": str(WRITE_TIMEOUT),
            }
        )
        exit_code, timed_out = run_client(argv, env, stderr_path, CYCLE_TIMEOUT)
        check_workdir(workdir, workdir_id)  # before anything else touches it
        at_end = _read_events(events_path)
        n_lines = len(at_end.splitlines())
        # A forward still running would be missed by the count below: wait for the proxy.
        _require(proxy.wait_idle(IDLE_TIMEOUT), room, f"proxy idle within {IDLE_TIMEOUT:g}s of the cycle")
        log = [dict(e) for e in proxy.log]
    finally:
        proxy.stop()
    # A connection accepted between the wait above and the shutdown could still forward a
    # write: wait again (the counter outlives the listener) and require that nothing new came.
    _require(proxy.wait_idle(IDLE_TIMEOUT), room, "proxy idle after it stopped")
    _require(len(proxy.log) == len(log), room, "no request reached the proxy after the cycle's snapshot")
    # Same entries, now with their final `forwarded` / `origin_status`: use those from here on.
    log = [dict(e) for e in proxy.log]

    records = _records(up, room)
    seed = [t for s, t in records if s == seed_seq]
    _require(
        seed == [SEED_TEXT],
        room,
        f"the seed record (seq {seed_seq}) is still stored exactly once, with the seed text",
    )
    others = [t for s, t in records if s != seed_seq]  # a re-posted seed text is one of these
    stored = others.count(text)
    other_records = len(others) - stored  # every non-seed record that is not the write text
    check_fault_applied(scenario, log, stored)

    # The log as it stood when the cycle ended is what gets judged. A change made after that
    # and before this second read (a helper that left the client's process group, say) makes
    # the log untrustworthy; later changes are not seen, but they cannot alter the verdict.
    intact = _read_events(events_path) == at_end
    cycle = Cycle(exit_code, timed_out, 0, n_lines, log_intact=intact)
    lines = at_end.decode("utf-8", errors="replace").splitlines()
    event, _ = parse_write_events(lines, write_id)
    truth = WriteTruth(
        scenario=scenario.id,
        write_id=write_id,
        stored=stored,
        other_records=other_records,
        proxy_log=report_log(log, room),
    )
    return WriterRun(truth=truth, cycle=cycle, lines=lines, outcome=event["outcome"] if event else None)
