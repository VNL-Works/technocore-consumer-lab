"""End-to-end: both reference writers against W0..W7 on the real upstream, through the proxy.

The kit's proof that the writer scenarios discriminate: the careful writer must pass all
eight, and the naive writer must fail exactly W2, W3, W4, W6 and W7. If the naive writer ever
passes one of those, that scenario proves nothing: do not weaken the assertion, fix the
scenario. Needs Linux, the venv from scripts/setup_wsl.sh and the upstream tree
(CONSUMER_LAB_UPSTREAM, default ~/consumer-lab-upstream-0e47f77); skipped loudly without it.
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
TREE = Path(os.environ.get("CONSUMER_LAB_UPSTREAM", "~/consumer-lab-upstream-0e47f77")).expanduser()
HAVE_TREE = (TREE / "src" / "store.py").is_file()

# id: (naive verdict, careful verdict, careful outcome) -- the spec's table, verbatim.
TABLE = {
    "W0": ("PASS", "PASS", "confirmed"),
    "W1": ("PASS", "PASS", "confirmed"),
    "W2": ("FAIL:duplicate_post", "PASS", "confirmed"),
    "W3": ("FAIL:duplicate_post", "PASS", "confirmed"),
    "W4": ("FAIL:duplicate_post", "PASS", "confirmed"),
    "W5": ("PASS", "PASS", "unconfirmed"),
    "W6": ("FAIL:false_confirmed", "PASS", "unconfirmed"),
    "W7": ("FAIL:false_not_written", "PASS", "unconfirmed"),
}


def run_kit(client_cmd: str, scenario: str = "writers", *extra: str) -> tuple[int, dict | None, str]:
    with tempfile.TemporaryDirectory(prefix="consumer-lab-wtest-") as tmp:
        out = Path(tmp) / "report.json"
        cmd = [sys.executable, "-m", "consumer_lab", "run", "--upstream", str(TREE),
               "--client", client_cmd, "--scenario", scenario, "--json", str(out), *extra]
        proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, timeout=900)
        report = json.loads(out.read_text()) if out.exists() else None
        return proc.returncode, report, proc.stdout + proc.stderr


def run_script(source: str, scenario: str, *extra: str) -> tuple[int, dict | None, str]:
    with tempfile.TemporaryDirectory(prefix="consumer-lab-wclient-") as tmp:
        script = Path(tmp) / "client.py"
        script.write_text(source)
        return run_kit(f"{sys.executable} {script}", scenario, *extra)


def ref(client: str) -> str:
    return f"{sys.executable} {REPO / 'clients' / client}"


def verdicts(report: dict) -> dict[str, str]:
    return {sid: r["verdict"] for sid, r in report["scenarios"].items()}


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class ReferenceWriters(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.careful = run_kit(ref("careful_writer.py"))
        cls.naive = run_kit(ref("naive_writer.py"))

    def test_careful_passes_all_eight(self):
        code, report, output = self.careful
        self.assertEqual(code, 0, output)
        self.assertEqual(verdicts(report), {sid: row[1] for sid, row in TABLE.items()}, output)
        self.assertEqual(
            {sid: r["outcome"] for sid, r in report["scenarios"].items()},
            {sid: row[2] for sid, row in TABLE.items()},
            output,
        )
        self.assertTrue(report["all_pass"])

    def test_naive_verdicts_exactly(self):
        # W1 and W5 pass for the naive writer by design: following a 303 after a committed
        # write, or retrying a write that never arrived, does no harm.
        code, report, output = self.naive
        self.assertEqual(code, 1, output)
        self.assertEqual(verdicts(report), {sid: row[0] for sid, row in TABLE.items()}, output)
        self.assertFalse(report["all_pass"])

    def test_naive_must_fail_the_discriminating_five(self):
        v = verdicts(self.naive[1])
        for sid in ("W2", "W3", "W4", "W6", "W7"):
            self.assertNotEqual(v[sid], "PASS", f"{sid} is too weak: the naive writer passed it")

    def test_report_shape_and_no_text(self):
        for code, report, output in (self.careful, self.naive):
            self.assertEqual(report["upstream_commit"], "0e47f770b13cc27e1e2e199d4cdf70a4778c97cc")
            self.assertEqual(report["selected"], list(TABLE))
            for sid, r in report["scenarios"].items():
                self.assertEqual(set(r["events"]), {"write", "other", "total"}, sid)
                self.assertEqual(r["events"]["write"], 1, sid)
                self.assertIn(r["stored"], (0, 1, 2), sid)
                self.assertTrue(r["proxy_log"], sid)
                for entry in r["proxy_log"]:
                    self.assertEqual(
                        set(entry), {"method", "path_without_query", "action", "forwarded", "origin_status"}
                    )
            text = json.dumps(report)
            self.assertNotIn("consumer-lab write", text)  # never the write text
            self.assertNotIn("format=json", text)  # paths without query

    def test_first_post_truth_in_proxy_log(self):
        report = self.careful[1]
        for sid in ("W1", "W2", "W3", "W4", "W7"):
            first = report["scenarios"][sid]["proxy_log"][0]
            self.assertEqual((first["method"], first["forwarded"], first["origin_status"]), ("POST", True, 200), sid)
        for sid in ("W5", "W6"):
            first = report["scenarios"][sid]["proxy_log"][0]
            self.assertEqual((first["method"], first["forwarded"]), ("POST", False), sid)


ENV_PROBE = r'''
import json, os
# Posts nothing. Reports not_written (true: nothing was posted) unless the env is wrong, in
# which case it writes a malformed line, so the verdict turns into FAIL:bad_events.
ok = (not any(k.startswith("CHAT_") for k in os.environ)
      and os.environ["TC_WRITE_TIMEOUT"] == "3"
      and os.environ["TC_WRITE_TEXT"].startswith("consumer-lab write W5 ")
      and os.environ["TC_WRITE_FROM"] and os.environ["TC_WRITE_ID"]
      and os.environ["TECHNOCORE_URL"].startswith("http://127.0.0.1:")
      and "127.0.0.1" in os.environ["NO_PROXY"] and "127.0.0.1" in os.environ["no_proxy"]
      and os.path.isdir(os.environ["TC_STATE_DIR"]) and os.environ["TC_ROOM"] == "lab-w5")
with open(os.environ["TC_EVENTS"], "a") as f:
    if ok:
        f.write(json.dumps({"event": "write", "id": os.environ["TC_WRITE_ID"], "outcome": "not_written"}) + "\n")
    else:
        f.write("env wrong\n")
'''

CRASH = "raise SystemExit(3)\n"

BAD_BODY = r'''
import json, os, urllib.request
# Posts a body upstream refuses (400), so the first POST is forwarded but not answered 200:
# W1's precondition does not hold and the kit must say so (exit 2), not judge.
req = urllib.request.Request(os.environ["TECHNOCORE_URL"] + "/r/" + os.environ["TC_ROOM"] + "?format=json",
                             data=b"not json", headers={"Content-Type": "application/json"}, method="POST")
try:
    urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req, timeout=3).read()
except Exception:
    pass
with open(os.environ["TC_EVENTS"], "a") as f:
    f.write(json.dumps({"event": "write", "id": os.environ["TC_WRITE_ID"], "outcome": "unconfirmed"}) + "\n")
'''

LATE_APPEND = r'''
import json, os, socket, time
# W3: the parent sends the POST on a raw socket (the proxy forwards it, then stays silent for
# 10 s), forks a child that keeps the socket, writes an honest `unconfirmed` and exits. The
# child waits until the parent has been reaped, then one more second (the kit reads the log
# within milliseconds of the reap), and appends a second line. The proxy is still stalled
# then, and the kit waits for it before it compares the log again, so the append lands after
# the cycle and before the check, with seconds of margin on both sides.
base, room = os.environ["TECHNOCORE_URL"], os.environ["TC_ROOM"]
host, port = base.rsplit("/", 1)[1].split(":")
body = json.dumps({"from": os.environ["TC_WRITE_FROM"], "text": os.environ["TC_WRITE_TEXT"]}).encode()
s = socket.create_connection((host, int(port)))
s.sendall(f"POST /r/{room}?format=json HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n"
          f"Content-Length: {len(body)}\r\n\r\n".encode() + body)
parent = os.getpid()
event = json.dumps({"event": "write", "id": os.environ["TC_WRITE_ID"], "outcome": "unconfirmed"}) + "\n"
if os.fork() == 0:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            os.kill(parent, 0)
        except ProcessLookupError:
            break
        time.sleep(0.01)
    time.sleep(1.0)
    with open(os.environ["TC_EVENTS"], "a") as f:
        f.write(event)
    s.close()
    os._exit(0)
s.close()
with open(os.environ["TC_EVENTS"], "a") as f:
    f.write(event)
'''


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class WriterKit(unittest.TestCase):
    def test_client_env(self):
        code, report, output = run_script(ENV_PROBE, "W5")
        self.assertEqual(code, 0, output)
        self.assertEqual(verdicts(report)["W5"], "PASS", output)
        self.assertEqual(report["scenarios"]["W5"]["stored"], 0)

    def test_crash_before_posting_is_a_verdict(self):
        # No POST reached the proxy, so there is no precondition to break: FAIL:crash.
        code, report, output = run_script(CRASH, "W1")
        self.assertEqual(code, 1, output)
        self.assertEqual(verdicts(report)["W1"], "FAIL:crash", output)
        self.assertEqual(report["scenarios"]["W1"]["cycle_exit_codes"], [3])

    def test_broken_precondition_exits_2(self):
        code, report, output = run_script(BAD_BODY, "W1")
        self.assertEqual(code, 2, output)
        self.assertIn("precondition failed: the first POST was forwarded and answered 200", output)
        self.assertEqual(report["scenarios"], {})
        self.assertIsNotNone(report["setup_error"])

    def test_log_changed_after_cycle_is_bad_events(self):
        code, report, output = run_script(LATE_APPEND, "W3")
        self.assertEqual(code, 1, output)
        self.assertEqual(verdicts(report)["W3"], "FAIL:bad_events", output)
        self.assertIn("changed after the cycle", report["scenarios"]["W3"]["reason"])
        self.assertEqual(report["scenarios"]["W3"]["events"]["write"], 1)  # judged at cycle end
        self.assertEqual(report["scenarios"]["W3"]["stored"], 1)

    def test_mixing_readers_and_writers_exits_2(self):
        code, report, output = run_kit(ref("careful_writer.py"), "S1,W0")
        self.assertEqual(code, 2, output)
        self.assertIn("cannot be mixed", output)
        self.assertIsNone(report)  # refused before anything ran

    def test_unknown_writer_id_exits_2(self):
        code, _, output = run_kit(ref("careful_writer.py"), "W8")
        self.assertEqual(code, 2, output)

ENCODED_NAIVE = r'''
import json, os, urllib.request
# The naive writer's logic (urllib, retry any error up to 2 more times, trust a 2xx), but the
# room is written percent-encoded ("lab%2Dw2"): upstream routes it to the same room, so the
# proxy must apply the scenario's fault to it all the same (review Fix 1).
room = os.environ["TC_ROOM"].replace("-", "%2D")
url = os.environ["TECHNOCORE_URL"] + "/r/" + room + "?format=json"
body = json.dumps({"from": os.environ["TC_WRITE_FROM"], "text": os.environ["TC_WRITE_TEXT"]}).encode()
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
outcome = "not_written"
for attempt in range(1, 4):
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with opener.open(req, timeout=float(os.environ["TC_WRITE_TIMEOUT"])) as resp:
            resp.read()
            if 200 <= resp.status < 300:
                outcome = "confirmed"
                break
    except Exception:
        continue
with open(os.environ["TC_EVENTS"], "a") as f:
    f.write(json.dumps({"event": "write", "id": os.environ["TC_WRITE_ID"], "outcome": outcome}) + "\n")
'''

DELETE_EVENTS = r'''
import json, os
# Writes its event, then deletes the event log: the client's fault, to be judged, not a kit error.
with open(os.environ["TC_EVENTS"], "a") as f:
    f.write(json.dumps({"event": "write", "id": os.environ["TC_WRITE_ID"], "outcome": "unconfirmed"}) + "\n")
os.remove(os.environ["TC_EVENTS"])
'''


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class WriterKitFixRound1(unittest.TestCase):
    def test_encoded_room_cannot_dodge_the_fault(self):
        code, report, output = run_script(ENCODED_NAIVE, "W2,W3,W4,W6,W7")
        self.assertEqual(code, 1, output)
        self.assertEqual(
            verdicts(report),
            {"W2": "FAIL:duplicate_post", "W3": "FAIL:duplicate_post", "W4": "FAIL:duplicate_post",
             "W6": "FAIL:false_confirmed", "W7": "FAIL:false_not_written"},
            output,
        )
        for sid, r in report["scenarios"].items():
            first = r["proxy_log"][0]
            self.assertEqual(first["path_without_query"], f"/r/lab%2D{sid.lower()}", sid)
            self.assertNotEqual(first["action"], "pass", sid)

    def test_deleted_event_log_is_judged(self):
        code, report, output = run_script(DELETE_EVENTS, "W0")
        self.assertEqual(code, 1, output)
        self.assertNotIn("Traceback", output)
        self.assertEqual(verdicts(report)["W0"], "FAIL:bad_events", output)
        self.assertIn("no write event", report["scenarios"]["W0"]["reason"])


class FaultAppliedPrecondition(unittest.TestCase):
    """`check_fault_applied` on synthetic proxy logs; no upstream needed."""

    @staticmethod
    def entry(path, action, forwarded=True, status=200, method="POST"):
        return {"method": method, "path_without_query": path, "action": action,
                "forwarded": forwarded, "origin_status": status}

    def setUp(self):
        sys.path.insert(0, str(REPO))
        from consumer_lab import writers as W
        from consumer_lab.scenarios import NotDeterministic
        self.W, self.ND = W, NotDeterministic

    def check(self, sid, log, stored):
        self.W.check_fault_applied(self.W.WRITER_SCENARIOS[sid], log, stored)

    def test_stored_without_the_fault_fails_closed(self):
        with self.assertRaisesRegex(self.ND, "stored without passing the scenario's fault"):
            self.check("W2", [self.entry("/r/lab-w2-unseen", "pass")], 1)
        with self.assertRaisesRegex(self.ND, "stored without passing the scenario's fault"):
            self.check("W5", [], 1)

    def test_encoded_room_post_counts_as_the_room(self):
        self.check("W2", [self.entry("/r/lab%2Dw2", "redirect_after_write(307)"),
                          self.entry("/r/lab-w2", "pass")], 2)
        with self.assertRaisesRegex(self.ND, "answered 200"):
            self.check("W3", [self.entry("/r/lab%2dw3", "stall_after_write(10)", status=400)], 1)

    def test_no_post_nothing_stored_is_judged(self):
        self.check("W1", [], 0)
        self.check("W0", [], 0)
        self.check("W0", [self.entry("/r/lab-w0", "pass")], 1)

    def test_not_forwarded_first_post(self):
        self.check("W5", [self.entry("/r/lab-w5", "reset_before_write", forwarded=False, status=None),
                          self.entry("/r/lab-w5", "pass")], 1)
        with self.assertRaisesRegex(self.ND, "was not forwarded"):
            self.check("W6", [self.entry("/r/lab-w6", "redirect_without_write(303)")], 0)


if __name__ == "__main__":
    unittest.main()
