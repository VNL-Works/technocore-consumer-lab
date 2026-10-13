"""Unit tests of the writer judging rules on synthetic logs. No upstream needed."""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from consumer_lab import writer_judge as WJ  # noqa: E402
from consumer_lab.judge import Cycle  # noqa: E402
from consumer_lab.writer_judge import WriteTruth, judge_write  # noqa: E402

WID = "abc123"


def ev(outcome="confirmed", **extra):
    return json.dumps({"event": "write", "id": WID, "outcome": outcome, **extra})


def cyc(exit_code=0, timed_out=False, intact=True, n=1):
    return Cycle(exit_code, timed_out, 0, n, log_intact=intact)


def truth(stored, sid="W3", other=0):
    return WriteTruth(scenario=sid, write_id=WID, stored=stored, other_records=other)


class WriterVerdicts(unittest.TestCase):
    def v(self, lines, stored, sid="W3", cycle=None, other=0):
        verdict = judge_write(sid, lines, truth(stored, sid, other), cycle or cyc(n=len(lines)))
        self.assertTrue(verdict.reason.startswith(f"{sid}: "), verdict.reason)
        self.assertNotIn("\n", verdict.reason)
        return verdict

    # PASS ------------------------------------------------------------------------
    def test_pass_confirmed(self):
        r = self.v([ev("confirmed", detail="200", attempts=1)], 1)
        self.assertEqual((r.verdict, r.reason), (WJ.PASS, "W3: confirmed, stored 1"))

    def test_pass_unconfirmed_stored_or_not(self):
        self.assertEqual(self.v([ev("unconfirmed")], 1).reason, "W3: unconfirmed, stored 1")
        self.assertEqual(self.v([ev("unconfirmed")], 0).verdict, WJ.PASS)

    def test_pass_not_written_when_nothing_stored(self):
        self.assertEqual(self.v([ev("not_written")], 0, sid="W6").verdict, WJ.PASS)

    def test_pass_nonzero_exit_with_valid_event(self):
        self.assertEqual(self.v([ev("unconfirmed")], 1, cycle=cyc(exit_code=3)).verdict, WJ.PASS)

    def test_blank_lines_ignored(self):
        self.assertEqual(self.v(["", ev("confirmed"), "  "], 1).verdict, WJ.PASS)

    def test_w0_pass(self):
        self.assertEqual(self.v([ev("confirmed")], 1, sid="W0").verdict, WJ.PASS)

    # FAILs -----------------------------------------------------------------------
    def test_timeout(self):
        self.assertEqual(self.v([ev()], 1, cycle=cyc(exit_code=None, timed_out=True)).verdict, WJ.FAIL_TIMEOUT)

    def test_crash_empty_log_nonzero_exit(self):
        self.assertEqual(self.v([], 0, cycle=cyc(exit_code=1, n=0)).verdict, WJ.FAIL_CRASH)
        self.assertEqual(self.v(["", "\n"], 0, cycle=cyc(exit_code=1)).verdict, WJ.FAIL_CRASH)

    def test_bad_events_cases(self):
        cases = {
            "malformed": ["{not json"],
            "not an object": ["[1, 2]"],
            "no event": [],
            "other kind": [ev(), json.dumps({"event": "deliver", "seq": 1, "generation": 1})],
            "wrong id": [json.dumps({"event": "write", "id": "zzz", "outcome": "confirmed"})],
            "two events": [ev(), ev()],
            "unknown outcome": [ev("maybe")],
            "detail not str": [ev(detail=5)],
            "attempts not int": [ev(attempts="2")],
            "attempts bool": [ev(attempts=True)],
        }
        for name, lines in cases.items():
            with self.subTest(name):
                self.assertEqual(self.v(lines, 1).verdict, WJ.FAIL_BAD_EVENTS)

    def test_bad_events_empty_log_exit_0(self):
        self.assertEqual(self.v([], 1, cycle=cyc(exit_code=0, n=0)).verdict, WJ.FAIL_BAD_EVENTS)

    def test_bad_events_nonzero_exit_with_garbage(self):
        # Not a crash: the log is not empty, it is invalid.
        self.assertEqual(self.v(["garbage"], 0, cycle=cyc(exit_code=1)).verdict, WJ.FAIL_BAD_EVENTS)

    def test_bad_events_log_changed_after_cycle(self):
        r = self.v([ev()], 1, cycle=cyc(intact=False))
        self.assertEqual(r.verdict, WJ.FAIL_BAD_EVENTS)
        self.assertIn("changed after", r.reason)

    def test_duplicate_post(self):
        for outcome in ("confirmed", "unconfirmed", "not_written"):
            with self.subTest(outcome):
                self.assertEqual(self.v([ev(outcome)], 2).verdict, WJ.FAIL_DUPLICATE_POST)
        self.assertEqual(self.v([ev()], 3).verdict, WJ.FAIL_DUPLICATE_POST)

    def test_extra_post(self):
        for outcome in ("confirmed", "unconfirmed", "not_written"):
            for stored in (0, 1):
                with self.subTest(outcome=outcome, stored=stored):
                    r = self.v([ev(outcome)], stored, other=1)
                    self.assertEqual(r.verdict, WJ.FAIL_EXTRA_POST)
                    self.assertEqual(r.reason, f"W3: the client posted 1 other record(s) to the room ({outcome})")
        self.assertEqual(self.v([ev()], 1, other=3).reason, "W3: the client posted 3 other record(s) to the room (confirmed)")
        for sid in ("W0", "W1", "W2", "W3", "W4", "W5", "W6", "W7"):
            with self.subTest(sid=sid):
                self.assertEqual(self.v([ev("confirmed")], 1, sid=sid, other=1).verdict, WJ.FAIL_EXTRA_POST)

    def test_no_other_records_is_no_extra_post(self):
        self.assertEqual(self.v([ev("confirmed")], 1, other=0).verdict, WJ.PASS)

    def test_false_confirmed(self):
        self.assertEqual(self.v([ev("confirmed")], 0, sid="W6").verdict, WJ.FAIL_FALSE_CONFIRMED)

    def test_false_not_written(self):
        self.assertEqual(self.v([ev("not_written")], 1, sid="W7").verdict, WJ.FAIL_FALSE_NOT_WRITTEN)

    def test_w0_missing_write(self):
        self.assertEqual(self.v([ev("unconfirmed")], 0, sid="W0").verdict, WJ.FAIL_MISSING_WRITE)
        self.assertEqual(self.v([ev("not_written")], 0, sid="W0").verdict, WJ.FAIL_MISSING_WRITE)

    def test_w0_overcautious(self):
        r = self.v([ev("unconfirmed")], 1, sid="W0")
        self.assertEqual(r.verdict, WJ.FAIL_OVERCAUTIOUS)

    def test_unconfirmed_never_fails_outside_w0(self):
        for sid in ("W1", "W2", "W3", "W4", "W5", "W6", "W7"):
            for stored in (0, 1):
                with self.subTest(sid=sid, stored=stored):
                    self.assertEqual(self.v([ev("unconfirmed")], stored, sid=sid).verdict, WJ.PASS)


class Precedence(unittest.TestCase):
    """The first matching rule wins: timeout > crash(empty) > bad_events > duplicate_post >
    extra_post > false_confirmed > false_not_written > W0 rules > PASS."""

    def j(self, lines, stored, sid="W0", cycle=None, other=0):
        return judge_write(sid, lines, truth(stored, sid, other), cycle or cyc()).verdict

    def test_order(self):
        # timeout beats an empty log with a non-zero exit, and a broken log
        self.assertEqual(self.j([], 0, cycle=cyc(exit_code=1, timed_out=True)), WJ.FAIL_TIMEOUT)
        self.assertEqual(self.j(["junk"], 2, cycle=cyc(timed_out=True)), WJ.FAIL_TIMEOUT)
        # crash (empty log) beats a log changed after the cycle
        self.assertEqual(self.j([], 0, cycle=cyc(exit_code=1, intact=False)), WJ.FAIL_CRASH)
        # bad_events beats duplicate_post
        self.assertEqual(self.j(["junk"], 2), WJ.FAIL_BAD_EVENTS)
        self.assertEqual(self.j([ev()], 2, cycle=cyc(intact=False)), WJ.FAIL_BAD_EVENTS)
        # duplicate_post beats everything about the claim
        self.assertEqual(self.j([ev("not_written")], 2), WJ.FAIL_DUPLICATE_POST)
        # bad_events beats extra_post
        self.assertEqual(self.j(["junk"], 1, other=1), WJ.FAIL_BAD_EVENTS)
        # duplicate_post beats extra_post
        self.assertEqual(self.j([ev("confirmed")], 2, other=1), WJ.FAIL_DUPLICATE_POST)
        # extra_post beats false_confirmed, missing_write and false_not_written
        self.assertEqual(self.j([ev("confirmed")], 0, other=1), WJ.FAIL_EXTRA_POST)
        self.assertEqual(self.j([ev("unconfirmed")], 0, other=1), WJ.FAIL_EXTRA_POST)
        self.assertEqual(self.j([ev("not_written")], 1, other=1), WJ.FAIL_EXTRA_POST)
        self.assertEqual(self.j([ev("confirmed")], 0, sid="W6", other=1), WJ.FAIL_EXTRA_POST)
        self.assertEqual(self.j([ev("not_written")], 1, sid="W7", other=1), WJ.FAIL_EXTRA_POST)
        # false_confirmed beats missing_write (W0, stored 0)
        self.assertEqual(self.j([ev("confirmed")], 0), WJ.FAIL_FALSE_CONFIRMED)
        # false_not_written beats overcautious (W0, stored 1)
        self.assertEqual(self.j([ev("not_written")], 1), WJ.FAIL_FALSE_NOT_WRITTEN)
        # missing_write beats overcautious (W0, stored 0, unconfirmed)
        self.assertEqual(self.j([ev("unconfirmed")], 0), WJ.FAIL_MISSING_WRITE)

    def test_every_verdict_is_covered(self):
        produced = {
            self.j([], 0, cycle=cyc(timed_out=True)),
            self.j([], 0, cycle=cyc(exit_code=1)),
            self.j(["x"], 1),
            self.j([ev()], 2),
            self.j([ev()], 1, other=1),
            self.j([ev("confirmed")], 0),
            self.j([ev("not_written")], 1),
            self.j([ev("unconfirmed")], 0),
            self.j([ev("unconfirmed")], 1),
            self.j([ev("confirmed")], 1),
        }
        self.assertEqual(produced, set(WJ.VERDICTS))


if __name__ == "__main__":
    unittest.main()
