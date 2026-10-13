"""Judge a client's event log against the truth the kit computed from store state.

Records are identified by (generation, seq): `seq` alone repeats across a room's lifetimes,
and a delivery that carries the wrong generation is a different record from the one stored.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

PASS = "PASS"
FAIL_DUPLICATE = "FAIL:duplicate"
FAIL_OUT_OF_ORDER = "FAIL:out_of_order"
FAIL_MISSED_RETAINED = "FAIL:missed_retained"
FAIL_UNREPORTED_LOSS = "FAIL:unreported_loss"
FAIL_DELIVERED_LOST = "FAIL:delivered_lost"
FAIL_TIMEOUT = "FAIL:timeout"
FAIL_CRASH = "FAIL:crash"
FAIL_BAD_EVENTS = "FAIL:bad_events"

VERDICTS = (
    PASS,
    FAIL_DUPLICATE,
    FAIL_OUT_OF_ORDER,
    FAIL_MISSED_RETAINED,
    FAIL_UNREPORTED_LOSS,
    FAIL_DELIVERED_LOST,
    FAIL_TIMEOUT,
    FAIL_CRASH,
    FAIL_BAD_EVENTS,
)

Key = tuple[int, int]  # (generation, seq)


@dataclass(frozen=True)
class Gap:
    """A loss the client can detect: records after `after_seq` (in `generation`) are gone.

    The client must emit `{"event":"loss","after_seq":after_seq,...}` before its first
    delivery of any record ordered after (generation, after_seq). `reason_any`, if set, is a
    list of substrings of which the loss reason must contain at least one (case-insensitive).

    `observable_cycle` is the 0-based index of the first cycle in which the pinned upstream's
    responses reveal the gap. A matching loss event written before that cycle started cannot
    be based on anything the client saw: it is a false alarm (`FAIL:bad_events`) and does not
    satisfy the gap. With no cycle information the whole log counts.
    """

    after_seq: int
    generation: int
    reason_any: tuple[str, ...] = ()
    observable_cycle: int = 0


@dataclass
class Truth:
    """What the kit knows from store state it controls.

    `expected`: every retained record the client had a chance to read, in store order; each
    must be delivered exactly once, in this order. `lost`: records the kit knows were not
    retained when the client read; none may be delivered. `gaps`: losses that must be reported.
    """

    expected: list[Key]
    lost: set[Key] = field(default_factory=set)
    gaps: list[Gap] = field(default_factory=list)


@dataclass(frozen=True)
class Cycle:
    """One client invocation: its exit status and the slice of the event log it appended."""

    exit_code: int | None
    timed_out: bool
    first_event: int  # index into the event list of the cycle's first appended event
    end_event: int  # one past its last
    # False when, after this cycle, the log no longer starts with the bytes it held before
    # the cycle: the client rewrote history instead of appending.
    log_intact: bool = True


@dataclass(frozen=True)
class Verdict:
    verdict: str
    reason: str

    @property
    def passed(self) -> bool:
        return self.verdict == PASS


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def parse_events(lines: list[str]) -> tuple[list[dict], str | None]:
    """Parse JSONL lines. Returns (events, error); `error` names the first malformed line."""
    events: list[dict] = []
    for n, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            ev = json.loads(line)
        except ValueError:
            return events, f"line {n} is not JSON"
        err = validate_event(ev)
        if err:
            return events, f"line {n}: {err}"
        events.append(ev)
    return events, None


def json_type(ev: dict, key: str) -> str:
    """The JSON type of `ev[key]` ("missing" if absent), for a message that must not echo the
    client's value itself: a reason reaches the report, which holds no client text."""
    if key not in ev:
        return "missing"
    value = ev[key]
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if value is None:
        return "null"
    if isinstance(value, list):
        return "array"
    return "object"


def validate_event(ev: object) -> str | None:
    if not isinstance(ev, dict):
        return "event is not a JSON object"
    kind = ev.get("event")
    if kind == "deliver":
        if not _is_int(ev.get("seq")) or not _is_int(ev.get("generation")):
            return "deliver needs integer seq and generation"
    elif kind == "loss":
        if not _is_int(ev.get("after_seq")) or not isinstance(ev.get("reason"), str):
            return "loss needs integer after_seq and string reason"
    else:
        return f"unknown event type (a {json_type(ev, 'event')})"
    return None


def judge(
    scenario: str,
    events: list,
    truth: Truth,
    cycles: list[Cycle] | tuple[Cycle, ...] = (),
) -> Verdict:
    """Return one verdict for `scenario`.

    `events` is the client's log, as raw JSONL lines (str) or already-parsed dicts. `cycles`
    tells which slice of it each invocation wrote and how that invocation ended. Checks run
    in a fixed order, and the first failure found is the verdict.
    """
    for n, c in enumerate(cycles, 1):
        if not c.log_intact:
            return Verdict(
                FAIL_BAD_EVENTS, f"{scenario}: event log rewritten during cycle {n}, not appended"
            )
    lines = [e if isinstance(e, str) else json.dumps(e) for e in events]
    parsed, error = parse_events(lines)
    if error:
        return Verdict(FAIL_BAD_EVENTS, f"{scenario}: malformed event log, {error}")
    # Blank lines were skipped by the parser; map cycle slices onto the non-blank events.
    index_of: list[int] = []
    count = 0
    for line in lines:
        index_of.append(count)
        if line.strip():
            count += 1
    index_of.append(count)

    for n, c in enumerate(cycles, 1):
        if c.timed_out:
            return Verdict(FAIL_TIMEOUT, f"{scenario}: cycle {n} did not finish within the timeout")
    for n, c in enumerate(cycles, 1):
        if c.exit_code not in (0, None):
            lo, hi = index_of[min(c.first_event, len(lines))], index_of[min(c.end_event, len(lines))]
            if not any(ev["event"] == "loss" for ev in parsed[lo:hi]):
                return Verdict(
                    FAIL_CRASH, f"{scenario}: cycle {n} exited {c.exit_code} without a loss event"
                )

    deliveries = [(i, (ev["generation"], ev["seq"])) for i, ev in enumerate(parsed) if ev["event"] == "deliver"]
    losses = [(i, ev) for i, ev in enumerate(parsed) if ev["event"] == "loss"]
    expected = list(truth.expected)
    expected_set = set(expected)

    for _, key in deliveries:
        if key in truth.lost:
            return Verdict(
                FAIL_DELIVERED_LOST,
                f"{scenario}: delivered seq {key[1]} (generation {key[0]}), which was not retained",
            )
        if key not in expected_set:
            return Verdict(
                FAIL_DELIVERED_LOST,
                f"{scenario}: delivered seq {key[1]} as generation {key[0]}, "
                "a record the store never held under that generation",
            )

    seen: set[Key] = set()
    for _, key in deliveries:
        if key in seen:
            return Verdict(FAIL_DUPLICATE, f"{scenario}: seq {key[1]} (generation {key[0]}) delivered twice")
        seen.add(key)

    rank = {key: n for n, key in enumerate(expected)}
    for (_, prev), (_, key) in zip(deliveries, deliveries[1:]):
        if rank[key] < rank[prev]:
            return Verdict(
                FAIL_OUT_OF_ORDER, f"{scenario}: seq {key[1]} delivered after seq {prev[1]}"
            )

    def opens_at(gap: Gap) -> int:
        """Parsed-event index from which a loss for `gap` can be based on what was seen."""
        if not cycles or gap.observable_cycle <= 0:
            return 0
        if gap.observable_cycle >= len(cycles):
            return len(parsed)
        return index_of[min(cycles[gap.observable_cycle].first_event, len(lines))]

    for gap in truth.gaps:
        start = opens_at(gap)
        early = [i for i, ev in losses if ev["after_seq"] == gap.after_seq and i < start]
        if early:
            cycle_no = next(
                (n for n, c in enumerate(cycles, 1) if index_of[min(c.end_event, len(lines))] > early[0]),
                1,
            )
            return Verdict(
                FAIL_BAD_EVENTS,
                f"{scenario}: loss after_seq={gap.after_seq} reported in cycle {cycle_no}, before "
                f"the gap was observable (cycle {gap.observable_cycle + 1})",
            )

    for gap in truth.gaps:
        boundary = (gap.generation, gap.after_seq)
        first_past = next((i for i, key in deliveries if key > boundary), None)
        start = opens_at(gap)
        matching = [
            i
            for i, ev in losses
            if ev["after_seq"] == gap.after_seq
            and i >= start
            and (
                not gap.reason_any
                or any(word.lower() in ev["reason"].lower() for word in gap.reason_any)
            )
        ]
        if not matching:
            want = f" with a reason containing one of {list(gap.reason_any)}" if gap.reason_any else ""
            return Verdict(
                FAIL_UNREPORTED_LOSS,
                f"{scenario}: no loss event with after_seq={gap.after_seq}{want}",
            )
        if first_past is not None and matching[0] > first_past:
            return Verdict(
                FAIL_UNREPORTED_LOSS,
                f"{scenario}: loss after_seq={gap.after_seq} reported only after delivering past the gap",
            )

    missed = [key for key in expected if key not in seen]
    if missed:
        lo, hi = missed[0][1], missed[-1][1]
        return Verdict(
            FAIL_MISSED_RETAINED,
            f"{scenario}: {len(missed)} retained record(s) never delivered (seq {lo}..{hi})",
        )

    gap_points = {g.after_seq for g in truth.gaps}
    for _, ev in losses:
        if ev["after_seq"] not in gap_points:
            # Not in the spec's verdict list as its own kind: a false alarm is reported as a
            # malformed log, because the log asserts a loss the kit knows did not happen.
            return Verdict(
                FAIL_BAD_EVENTS,
                f"{scenario}: loss reported after_seq={ev['after_seq']}, where no record was lost",
            )

    return Verdict(
        PASS,
        f"{scenario}: {len(expected)} retained record(s) delivered once, in order"
        + (f"; {len(truth.gaps)} loss(es) reported in time" if truth.gaps else ""),
    )
