"""The 8 writer fault scenarios (W0..W7): one message, one client cycle, through the proxy.

Per scenario: a fresh plain room `lab-wN`; one seed record posted straight to upstream (so
the room exists and a read that follows a redirect reads a real room); then one client cycle
through a `FaultProxy` whose plan injects the fault; then `proxy.wait_idle`, and the truth:
how many records in the room hold exactly the text the client was given, read in-process
with `store.read_messages`, never through the proxy.

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
    client_env,
    run_client,
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


def _seed(up: Upstream, room: str) -> None:
    """One record straight to upstream (not through the proxy), so the room exists."""
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


def _texts(up: Upstream, room: str) -> list[str]:
    view = up.store.read_messages(up.root, room, limit=up.store.MAX_LIMIT)
    return [m.get("text") for m in view["messages"]]


def _require(cond: bool, room: str, what: str) -> None:
    if not cond:
        raise NotDeterministic(f"{room}: precondition failed: {what}")


def run_writer(scenario: WriterScenario, up: Upstream, client_cmd: str, workdir: Path) -> WriterRun:
    """Seed, run one client cycle through the proxy, wait for the proxy, compute the truth."""
    argv = client_argv(client_cmd)
    room = scenario.room
    write_id = secrets.token_hex(8)
    text = f"consumer-lab write {scenario.id} {secrets.token_hex(8)}"
    # The upstream sweep must store this text unchanged, or `stored` would count nothing.
    _require(up.store.clean_text(text) == text, room, "upstream clean_text keeps the write text unchanged")

    state_dir = workdir / "state"
    state_dir.mkdir()
    events_path = workdir / "events.jsonl"
    events_path.touch()
    stderr_path = workdir / "client.stderr"  # kept with --keep-root, for debugging

    _seed(up, room)
    before = _texts(up, room)
    _require(before == [SEED_TEXT], room, "the room holds exactly the seed before the cycle")

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
        at_end = events_path.read_bytes()
        n_lines = len(at_end.splitlines())
        # A forward still running would be missed by the count below: wait for the proxy.
        _require(proxy.wait_idle(IDLE_TIMEOUT), room, f"proxy idle within {IDLE_TIMEOUT:g}s of the cycle")
        log = [dict(e) for e in proxy.log]
    finally:
        proxy.stop()

    texts = _texts(up, room)
    _require(texts.count(SEED_TEXT) == 1, room, "the seed is stored exactly once")
    stored = texts.count(text)

    first_post = next(
        (e for e in log if e["method"] == "POST" and e["path_without_query"] == f"/r/{room}"), None
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

    # The log as it stood when the cycle ended is what gets judged; any change since (a
    # helper the client left behind, say) makes the log untrustworthy.
    intact = events_path.read_bytes() == at_end
    cycle = Cycle(exit_code, timed_out, 0, n_lines, log_intact=intact)
    lines = at_end.decode("utf-8", errors="replace").splitlines()
    event, _ = parse_write_events(lines, write_id)
    truth = WriteTruth(scenario=scenario.id, write_id=write_id, stored=stored, proxy_log=log)
    return WriterRun(truth=truth, cycle=cycle, lines=lines, outcome=event["outcome"] if event else None)
