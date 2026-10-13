"""Unit tests of the judging rules on synthetic event logs. No upstream needed."""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from consumer_lab import judge as J  # noqa: E402
from consumer_lab.judge import Cycle, Gap, Truth, judge  # noqa: E402


def d(seq, gen=1):
    return {"event": "deliver", "seq": seq, "generation": gen}


def loss(after, reason="gap"):
    return {"event": "loss", "after_seq": after, "reason": reason}


def ok_cycles(events, exits=(0,)):
    """One cycle per exit code; the last one owns every event."""
    n = len(events)
    return [Cycle(code, False, 0 if i == 0 else n, n) for i, code in enumerate(exits)]


# S2-shaped truth: 1..10 delivered, 11..60 gone, 61..70 retained.
GAP_TRUTH = Truth(
    expected=[(1, s) for s in list(range(1, 11)) + list(range(61, 71))],
    lost={(1, s) for s in range(11, 61)},
    gaps=[Gap(after_seq=10, generation=1)],
)
GAP_OK = [d(s) for s in range(1, 11)] + [loss(10)] + [d(s) for s in range(61, 71)]

# S5-shaped truth: generation 1 holds 1..5, generation 2 holds 6..8.
RECREATE_TRUTH = Truth(
    expected=[(1, s) for s in range(1, 6)] + [(2, s) for s in range(6, 9)],
    gaps=[Gap(after_seq=5, generation=1, reason_any=("generation", "recreated"))],
)


class JudgeVerdicts(unittest.TestCase):
    def verdict(self, events, truth, cycles=None):
        v = judge("SX", events, truth, ok_cycles(events) if cycles is None else cycles)
        self.assertTrue(v.reason.startswith("SX: "), v.reason)
        self.assertNotIn("\n", v.reason)
        return v.verdict

    # PASS ---------------------------------------------------------------------------
    def test_pass_contiguous(self):
        truth = Truth(expected=[(1, s) for s in range(1, 6)])
        self.assertEqual(self.verdict([d(s) for s in range(1, 6)], truth), J.PASS)

    def test_pass_with_loss_reported_before_gap(self):
        self.assertEqual(self.verdict(GAP_OK, GAP_TRUTH), J.PASS)

    def test_pass_recreate(self):
        events = [d(s) for s in range(1, 6)] + [loss(5, "room recreated: generation 1 -> 2")]
        events += [d(s, 2) for s in range(6, 9)]
        self.assertEqual(self.verdict(events, RECREATE_TRUTH), J.PASS)

    def test_pass_accepts_raw_jsonl_lines_and_blank_lines(self):
        lines = [json.dumps(e) for e in GAP_OK]
        lines.insert(3, "")
        self.assertEqual(self.verdict(lines, GAP_TRUTH, ok_cycles(lines)), J.PASS)

    def test_pass_nonzero_exit_with_loss_event(self):
        # A client may stop on a gap (as #545's loop raises) as long as it reported it.
        truth = Truth(expected=[(1, s) for s in range(1, 11)], gaps=[Gap(10, 1)])
        events = [d(s) for s in range(1, 11)] + [loss(10)]
        cycles = [Cycle(0, False, 0, 10), Cycle(1, False, 10, 11)]
        self.assertEqual(self.verdict(events, truth, cycles), J.PASS)

    # FAIL:duplicate -------------------------------------------------------------------
    def test_duplicate(self):
        truth = Truth(expected=[(1, s) for s in range(1, 4)])
        self.assertEqual(self.verdict([d(1), d(2), d(2), d(3)], truth), J.FAIL_DUPLICATE)

    def test_duplicate_across_cycles(self):
        truth = Truth(expected=[(1, s) for s in range(1, 4)])
        events = [d(1), d(2), d(1), d(2), d(3)]
        cycles = [Cycle(0, False, 0, 2), Cycle(0, False, 2, 5)]
        self.assertEqual(self.verdict(events, truth, cycles), J.FAIL_DUPLICATE)

    # FAIL:out_of_order ----------------------------------------------------------------
    def test_out_of_order(self):
        truth = Truth(expected=[(1, s) for s in range(1, 4)])
        self.assertEqual(self.verdict([d(1), d(3), d(2)], truth), J.FAIL_OUT_OF_ORDER)

    def test_new_generation_before_old(self):
        events = [loss(5, "generation changed")] + [d(s, 2) for s in range(6, 9)]
        events += [d(s) for s in range(1, 6)]
        self.assertEqual(self.verdict(events, RECREATE_TRUTH), J.FAIL_OUT_OF_ORDER)

    # FAIL:missed_retained -------------------------------------------------------------
    def test_missed_retained(self):
        truth = Truth(expected=[(1, s) for s in range(1, 311)])
        events = [d(s) for s in range(1, 11)] + [d(s) for s in range(111, 311)]
        self.assertEqual(self.verdict(events, truth), J.FAIL_MISSED_RETAINED)

    def test_missed_retained_after_reported_gap(self):
        events = [d(s) for s in range(1, 11)] + [loss(10)] + [d(s) for s in range(61, 70)]
        self.assertEqual(self.verdict(events, GAP_TRUTH), J.FAIL_MISSED_RETAINED)

    # FAIL:unreported_loss -------------------------------------------------------------
    def test_unreported_loss_silent(self):
        events = [d(s) for s in range(1, 11)] + [d(s) for s in range(61, 71)]
        self.assertEqual(self.verdict(events, GAP_TRUTH), J.FAIL_UNREPORTED_LOSS)

    def test_unreported_loss_reported_too_late(self):
        events = [d(s) for s in range(1, 11)] + [d(61), loss(10)] + [d(s) for s in range(62, 71)]
        self.assertEqual(self.verdict(events, GAP_TRUTH), J.FAIL_UNREPORTED_LOSS)

    def test_unreported_loss_wrong_after_seq(self):
        events = [d(s) for s in range(1, 11)] + [loss(60)] + [d(s) for s in range(61, 71)]
        self.assertEqual(self.verdict(events, GAP_TRUTH), J.FAIL_UNREPORTED_LOSS)

    def test_unreported_loss_recreate_reason_must_say_so(self):
        events = [d(s) for s in range(1, 6)] + [loss(5, "gap")] + [d(s, 2) for s in range(6, 9)]
        self.assertEqual(self.verdict(events, RECREATE_TRUTH), J.FAIL_UNREPORTED_LOSS)

    def test_unreported_loss_naive_recreate(self):
        events = [d(s) for s in range(1, 6)] + [d(s, 2) for s in range(6, 9)]
        self.assertEqual(self.verdict(events, RECREATE_TRUTH), J.FAIL_UNREPORTED_LOSS)

    # FAIL:delivered_lost --------------------------------------------------------------
    def test_delivered_lost(self):
        events = [d(s) for s in range(1, 11)] + [loss(10), d(11)] + [d(s) for s in range(61, 71)]
        self.assertEqual(self.verdict(events, GAP_TRUTH), J.FAIL_DELIVERED_LOST)

    def test_new_generation_delivered_as_old(self):
        # The recreated room's records presented as a continuation of generation 1.
        events = [d(s) for s in range(1, 6)] + [loss(5, "recreated")] + [d(s, 1) for s in range(6, 9)]
        self.assertEqual(self.verdict(events, RECREATE_TRUTH), J.FAIL_DELIVERED_LOST)

    # FAIL:timeout ---------------------------------------------------------------------
    def test_timeout(self):
        truth = Truth(expected=[(1, 1)])
        cycles = [Cycle(None, True, 0, 1)]
        self.assertEqual(self.verdict([d(1)], truth, cycles), J.FAIL_TIMEOUT)

    # FAIL:crash -----------------------------------------------------------------------
    def test_crash(self):
        truth = Truth(expected=[(1, s) for s in range(1, 4)])
        cycles = [Cycle(1, False, 0, 3)]
        self.assertEqual(self.verdict([d(1), d(2), d(3)], truth, cycles), J.FAIL_CRASH)

    def test_crash_loss_from_another_cycle_does_not_excuse(self):
        truth = Truth(expected=[(1, s) for s in range(1, 11)], gaps=[Gap(10, 1)])
        events = [d(s) for s in range(1, 11)] + [loss(10)]
        cycles = [Cycle(0, False, 0, 11), Cycle(2, False, 11, 11)]
        self.assertEqual(self.verdict(events, truth, cycles), J.FAIL_CRASH)

    # FAIL:bad_events ------------------------------------------------------------------
    def test_bad_events_not_json(self):
        truth = Truth(expected=[(1, 1)])
        self.assertEqual(self.verdict(['{"event":"deliver","seq":1', ], truth, []), J.FAIL_BAD_EVENTS)

    def test_bad_events_missing_generation(self):
        truth = Truth(expected=[(1, 1)])
        self.assertEqual(self.verdict([{"event": "deliver", "seq": 1}], truth), J.FAIL_BAD_EVENTS)

    def test_bad_events_bool_is_not_int(self):
        truth = Truth(expected=[(1, 1)])
        ev = {"event": "deliver", "seq": True, "generation": 1}
        self.assertEqual(self.verdict([ev], truth), J.FAIL_BAD_EVENTS)

    def test_bad_events_unknown_type(self):
        truth = Truth(expected=[(1, 1)])
        self.assertEqual(self.verdict([d(1), {"event": "ack", "seq": 1}], truth), J.FAIL_BAD_EVENTS)

    def test_bad_events_false_alarm(self):
        truth = Truth(expected=[(1, s) for s in range(1, 4)])
        events = [d(1), loss(1), d(2), d(3)]
        self.assertEqual(self.verdict(events, truth), J.FAIL_BAD_EVENTS)

    def test_every_verdict_is_covered(self):
        names = {n for n in dir(self) if n.startswith("test_")}
        for v in J.VERDICTS:
            stem = "pass" if v == J.PASS else v.split(":", 1)[1]
            self.assertTrue(any(n.startswith(f"test_{stem}") for n in names), v)


if __name__ == "__main__":
    unittest.main()
