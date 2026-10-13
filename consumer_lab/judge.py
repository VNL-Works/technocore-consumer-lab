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
    """

    after_seq: int
    generation: int
    reason_any: tuple[str, ...] = ()


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
        return f"unknown event type {kind!r}"
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

    for gap in truth.gaps:
        boundary = (gap.generation, gap.after_seq)
        first_past = next((i for i, key in deliveries if key > boundary), None)
        matching = [
            i
            for i, ev in losses
            if ev["after_seq"] == gap.after_seq
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
