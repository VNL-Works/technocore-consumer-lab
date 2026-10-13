"""Judge one writer cycle: did the client post a second copy, and was its claim true?

Truth is store state the kit read in-process after the cycle (`stored`: how many records in
the room hold exactly the text the client was asked to post). The client's claim is its one
`write` event. `unconfirmed` is never a failure outside W0: honesty is always allowed; only
a second copy or a false claim fails.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from consumer_lab.judge import Cycle, Verdict

PASS = "PASS"
FAIL_TIMEOUT = "FAIL:timeout"
FAIL_BAD_EVENTS = "FAIL:bad_events"
FAIL_CRASH = "FAIL:crash"
FAIL_DUPLICATE_POST = "FAIL:duplicate_post"
FAIL_FALSE_CONFIRMED = "FAIL:false_confirmed"
FAIL_FALSE_NOT_WRITTEN = "FAIL:false_not_written"
FAIL_MISSING_WRITE = "FAIL:missing_write"
FAIL_OVERCAUTIOUS = "FAIL:overcautious"

VERDICTS = (
    PASS,
    FAIL_TIMEOUT,
    FAIL_BAD_EVENTS,
    FAIL_CRASH,
    FAIL_DUPLICATE_POST,
    FAIL_FALSE_CONFIRMED,
    FAIL_FALSE_NOT_WRITTEN,
    FAIL_MISSING_WRITE,
    FAIL_OVERCAUTIOUS,
)

OUTCOMES = ("confirmed", "unconfirmed", "not_written")
CONTROL = "W0"  # the clean control: a client that never confirms is useless


@dataclass(frozen=True)
class WriteTruth:
    """What the kit knows after the cycle.

    `stored`: records in the room whose `text` equals the text the client was given (the
    kit's seed never counts). `proxy_log`: what the proxy did with each request.
    """

    scenario: str
    write_id: str
    stored: int
    proxy_log: list[dict] = field(default_factory=list)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def parse_write_events(lines: list[str], write_id: str) -> tuple[dict | None, str | None]:
    """The one valid write event for `write_id`, or (None, why the log is not acceptable)."""
    found: list[dict] = []
    for n, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            ev = json.loads(line)
        except ValueError:
            return None, f"line {n} is not JSON"
        if not isinstance(ev, dict):
            return None, f"line {n}: event is not a JSON object"
        if ev.get("event") != "write":
            return None, f"line {n}: unknown event type {ev.get('event')!r}"
        if ev.get("id") != write_id:
            return None, f"line {n}: write event for another id"
        if ev.get("outcome") not in OUTCOMES:
            return None, f"line {n}: unknown outcome {ev.get('outcome')!r}"
        if "detail" in ev and not isinstance(ev["detail"], str):
            return None, f"line {n}: detail must be a string"
        if "attempts" in ev and not _is_int(ev["attempts"]):
            return None, f"line {n}: attempts must be an integer"
        found.append(ev)
    if not found:
        return None, "no write event for TC_WRITE_ID"
    if len(found) > 1:
        return None, f"{len(found)} write events, expected exactly one"
    return found[0], None


def judge_write(scenario_id: str, event_lines: list[str], truth: WriteTruth, cycle: Cycle) -> Verdict:
    """One verdict; the first rule that matches wins (see docs/SCENARIOS.md, "Writer judging")."""
    sid = scenario_id
    if cycle.timed_out:
        return Verdict(FAIL_TIMEOUT, f"{sid}: the cycle did not finish within the timeout")
    nonblank = [ln for ln in event_lines if ln.strip()]
    if cycle.exit_code not in (0, None) and not nonblank:
        return Verdict(FAIL_CRASH, f"{sid}: exited {cycle.exit_code} without a write event")
    if not cycle.log_intact:
        return Verdict(FAIL_BAD_EVENTS, f"{sid}: event log changed after the cycle ended")
    event, error = parse_write_events(event_lines, truth.write_id)
    if error:
        return Verdict(FAIL_BAD_EVENTS, f"{sid}: {error}")
    # A non-zero exit with a non-empty but invalid log was `bad_events` above; with a valid
    # write event it is acceptable (the contract allows it), so no crash rule applies here.
    outcome = event["outcome"]
    stored = truth.stored
    if stored >= 2:
        return Verdict(FAIL_DUPLICATE_POST, f"{sid}: the message is stored {stored} times ({outcome})")
    if outcome == "confirmed" and stored == 0:
        return Verdict(FAIL_FALSE_CONFIRMED, f"{sid}: reported confirmed, but nothing is stored")
    if outcome == "not_written" and stored >= 1:
        return Verdict(FAIL_FALSE_NOT_WRITTEN, f"{sid}: reported not_written, but the message is stored")
    if scenario_id == CONTROL:
        if stored == 0:
            return Verdict(FAIL_MISSING_WRITE, f"{sid}: nothing stored on a clean path ({outcome})")
        if outcome != "confirmed":
            return Verdict(FAIL_OVERCAUTIOUS, f"{sid}: reported {outcome} on a clean path, stored {stored}")
    return Verdict(PASS, f"{sid}: {outcome}, stored {stored}")
