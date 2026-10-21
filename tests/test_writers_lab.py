"""End-to-end: both reference writers against W0..W7 on the real upstream, through the proxy.

The kit's proof that the writer scenarios discriminate: the careful writer must pass all
eight, and the naive writer must fail exactly W2, W3, W4, W6 and W7. If the naive writer ever
passes one of those, that scenario proves nothing: do not weaken the assertion, fix the
scenario. Needs Linux, the venv from scripts/setup_wsl.sh and the upstream tree
(CONSUMER_LAB_UPSTREAM, default ~/consumer-lab-upstream-4a013e5); skipped loudly without it.
"""

import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

REPO = Path(__file__).resolve().parent.parent
TREE = Path(os.environ.get("CONSUMER_LAB_UPSTREAM", "~/consumer-lab-upstream-4a013e5")).expanduser()
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
            self.assertEqual(report["upstream_commit"], "4a013e5401f15e56762456d1d48c4e676e2cff2c")
            self.assertEqual(report["selected"], list(TABLE))
            for sid, r in report["scenarios"].items():
                self.assertEqual(set(r["events"]), {"write", "other", "total"}, sid)
                self.assertEqual(r["events"]["write"], 1, sid)
                self.assertIn(r["stored"], (0, 1, 2), sid)
                self.assertEqual(r["other_records"], 0, sid)
                self.assertTrue(r["proxy_log"], sid)
                for entry in r["proxy_log"]:
                    self.assertEqual(
                        set(entry),
                        {"method", "path_without_query", "raw_path_differs", "action", "forwarded", "origin_status"},
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
# child first leaves the client's process group with setsid() (the kit kills that group after
# every cycle; the parent waits on a pipe until the child is out), then waits until the parent
# has been reaped, then one more second (the kit reads the log within milliseconds of the
# reap), and appends a second line. The proxy is still stalled then, and the kit waits for it
# before it compares the log again, so the append lands after the cycle and before the check,
# with seconds of margin on both sides.
base, room = os.environ["TECHNOCORE_URL"], os.environ["TC_ROOM"]
host, port = base.rsplit("/", 1)[1].split(":")
body = json.dumps({"from": os.environ["TC_WRITE_FROM"], "text": os.environ["TC_WRITE_TEXT"]}).encode()
s = socket.create_connection((host, int(port)))
s.sendall(f"POST /r/{room}?format=json HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n"
          f"Content-Length: {len(body)}\r\n\r\n".encode() + body)
parent = os.getpid()
event = json.dumps({"event": "write", "id": os.environ["TC_WRITE_ID"], "outcome": "unconfirmed"}) + "\n"
ready_r, ready_w = os.pipe()
if os.fork() == 0:
    os.close(ready_r)
    os.setsid()
    os.write(ready_w, b"x")
    os.close(ready_w)
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
os.close(ready_w)
os.read(ready_r, 1)  # the child is out of the process group
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
        # The worker's own `setup error:` line is the detail.
        self.assertTrue(report["setup_error"].startswith(
            "W1: worker exited 2: lab-w1: precondition failed: the first POST was forwarded and answered 200"
        ), report["setup_error"])

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
            # Reported as upstream routes it; that the raw path differed is kept as a flag.
            self.assertEqual(first["path_without_query"], f"/r/lab-{sid.lower()}", sid)
            self.assertIs(first["raw_path_differs"], True, sid)
            self.assertNotEqual(first["action"], "pass", sid)

    def test_deleted_event_log_is_judged(self):
        code, report, output = run_script(DELETE_EVENTS, "W0")
        self.assertEqual(code, 1, output)
        self.assertNotIn("Traceback", output)
        self.assertEqual(verdicts(report)["W0"], "FAIL:bad_events", output)
        self.assertIn("no write event", report["scenarios"]["W0"]["reason"])


ALTERED_RETRY = r'''
import json, os, urllib.request
# The naive writer's logic (urllib, retry any error up to 2 more times, trust a 2xx), but each
# retry posts an altered text: never a second copy of the message, always another record.
url = os.environ["TECHNOCORE_URL"] + "/r/" + os.environ["TC_ROOM"] + "?format=json"
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
outcome = "not_written"
for attempt in range(1, 4):
    text = os.environ["TC_WRITE_TEXT"] + ("" if attempt == 1 else f" (retry {attempt - 1})")
    body = json.dumps({"from": os.environ["TC_WRITE_FROM"], "text": text}).encode()
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

HANG = r'''
import os, time
# Records its own pid, then hangs well past the 30 s cycle timeout.
with open(PIDFILE, "w") as f:
    f.write(str(os.getpid()))
time.sleep(40)
'''

LEAKY = r'''
import http.client, json, os, urllib.request
# Writes its event properly on W0, and also sends requests that carry the write text's
# 16-hex token in a sub-path, in a fragment and as the method: none of it may reach the report.
base, room = os.environ["TECHNOCORE_URL"], os.environ["TC_ROOM"]
token = os.environ["TC_WRITE_TEXT"].rsplit(" ", 1)[1]
host, port = base.rsplit("/", 1)[1].split(":")


def send(method, path):
    conn = http.client.HTTPConnection(host, int(port), timeout=5)
    try:
        conn.request(method, path)
        conn.getresponse().read()
    except Exception:
        pass
    finally:
        conn.close()


body = json.dumps({"from": os.environ["TC_WRITE_FROM"], "text": os.environ["TC_WRITE_TEXT"]}).encode()
req = urllib.request.Request(base + "/r/" + room + "?format=json", data=body,
                             headers={"Content-Type": "application/json"}, method="POST")
with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req, timeout=3) as resp:
    resp.read()
    outcome = "confirmed" if resp.status == 200 else "unconfirmed"
send("GET", f"/r/{room}/{token}")
send("GET", f"/r/{room}#{token}")
send(token, "/rooms")
with open(os.environ["TC_EVENTS"], "a") as f:
    f.write(json.dumps({"event": "write", "id": os.environ["TC_WRITE_ID"], "outcome": outcome}) + "\n")
'''


TEXT_AS_EVENT = r'''
import json, os
# Writes the write text where the event type belongs: bad_events, and the reason must not
# carry that text (or its 16-hex token) into the report.
with open(os.environ["TC_EVENTS"], "a") as f:
    f.write(json.dumps({"event": os.environ["TC_WRITE_TEXT"], "id": os.environ["TC_WRITE_ID"]}) + "\n")
'''


NESTED = r'''
import os
# A line of 200000 "[": json.loads raises RecursionError (not ValueError) on it. The kit must
# judge it as a malformed line (FAIL:bad_events), not stop with a setup error.
with open(os.environ["TC_EVENTS"], "a") as f:
    f.write("[" * 200000 + "\n")
'''

HOSTILE_MARKER = "consumer-lab-c1-hostile-marker-3d81"

# Set CHECK before this source: the path where the client records who its parent is.
HOSTILE = r'''
import json, os, signal
# A hostile client: it writes a marker (also as a forged "setup error:" line) to its parent's
# stderr, then SIGKILLs the parent. The report's setup_error must hold only kit text.
MARKER = "consumer-lab-c1-hostile-marker-3d81"
ppid = os.getppid()
with open(f"/proc/{ppid}/cmdline", "rb") as f:
    argv = f.read().split(b"\0")
with open(CHECK, "w") as f:
    json.dump({"ppid": ppid, "parent_is_worker": b"_worker" in argv and b"consumer_lab" in argv}, f)
with open(f"/proc/{ppid}/fd/2", "w") as f:
    f.write(MARKER + "\n" + "setup error: " + MARKER + "\n")
os.kill(ppid, signal.SIGKILL)
'''


PARTIAL_MARKER = "consumer-lab-c1-partial-marker-7e52"

# BAD_BODY (a real W1 setup error), whose last act leaves a partial forged line, with no
# newline, on the worker's stderr: the worker's own `setup error:` line must not be joined to it.
PARTIAL_FORGERY = BAD_BODY + r'''
fd = os.open(f"/proc/{os.getppid()}/fd/2", os.O_WRONLY)
os.write(fd, b"setup error: consumer-lab-c1-partial-marker-7e52 12345678901234567890123 ")
'''


def pid_alive(pid: int, grace: float = 2.0) -> bool:
    """Running (not a zombie) after up to `grace` seconds; the same check as test_hygiene.alive."""
    deadline = time.monotonic() + grace
    while True:
        try:
            with open(f"/proc/{pid}/stat", encoding="ascii", errors="replace") as f:
                state = f.read().rsplit(")", 1)[1].split()[0]
        except (FileNotFoundError, ProcessLookupError, IndexError):
            return False
        if state == "Z":
            return False
        if time.monotonic() >= deadline:
            return True
        time.sleep(0.05)


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class WriterKitV021(unittest.TestCase):
    def test_altered_retry_is_an_extra_post(self):
        code, report, output = run_script(ALTERED_RETRY, "W2,W3,W4")
        self.assertEqual(code, 1, output)
        self.assertEqual(verdicts(report), {sid: "FAIL:extra_post" for sid in ("W2", "W3", "W4")}, output)
        for sid, r in report["scenarios"].items():
            self.assertEqual((r["stored"], r["other_records"]), (1, 1), sid)

    def test_hanging_writer_times_out_and_is_killed(self):
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-hang-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        pidfile = tmp / "client.pid"
        code, report, output = run_script(f"PIDFILE = {str(pidfile)!r}\n" + HANG, "W0")
        self.assertEqual(code, 1, output)
        self.assertEqual(verdicts(report)["W0"], "FAIL:timeout", output)
        pid = int(pidfile.read_text().strip())
        self.assertFalse(pid_alive(pid), f"client {pid} outlived its cycle")

    def test_report_never_carries_client_chosen_paths_or_methods(self):
        code, report, output = run_script(LEAKY, "W0")
        self.assertEqual(code, 0, output)
        self.assertEqual(verdicts(report)["W0"], "PASS", output)
        text = json.dumps(report)
        self.assertIsNone(re.search(r"\b[0-9a-f]{16}\b", text), text)
        self.assertNotIn("consumer-lab write", text)
        log = report["scenarios"]["W0"]["proxy_log"]
        self.assertIn("<other>", [e["path_without_query"] for e in log])  # the sub-path
        self.assertIn("<other>", [e["method"] for e in log])  # the token as the method
        self.assertIn(("GET", "/r/lab-w0", True),  # the fragment
                      [(e["method"], e["path_without_query"], e["raw_path_differs"]) for e in log])

    def test_report_never_carries_a_client_value_from_the_event(self):
        code, report, output = run_script(TEXT_AS_EVENT, "W0")
        self.assertEqual(code, 1, output)
        self.assertEqual(verdicts(report)["W0"], "FAIL:bad_events", output)
        self.assertEqual(report["scenarios"]["W0"]["reason"], "W0: line 1: unknown event type (a string)")
        text = json.dumps(report)
        self.assertIsNone(re.search(r"\b[0-9a-f]{16}\b", text), text)
        self.assertNotIn("consumer-lab write", text)


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class WriterKitV022(unittest.TestCase):
    def test_deeply_nested_line_is_bad_events(self):
        code, report, output = run_script(NESTED, "W0")
        self.assertEqual(code, 1, output)
        self.assertIsNone(report["setup_error"], output)
        self.assertEqual(verdicts(report)["W0"], "FAIL:bad_events", output)
        self.assertEqual(report["scenarios"]["W0"]["reason"], "W0: line 1 is not JSON")
        self.assertEqual(report["scenarios"]["W0"]["events"], {"write": 0, "other": 1, "total": 1})

    def test_setup_error_never_carries_what_a_hostile_client_wrote(self):
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-hostile-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        check = tmp / "parent.json"
        code, report, output = run_script(f"CHECK = {str(check)!r}\n" + HOSTILE, "W0")
        parent = json.loads(check.read_text())
        self.assertTrue(parent["parent_is_worker"], parent)  # /proc/<ppid>/fd/2 is the worker's stderr
        self.assertEqual(code, 2, output)
        self.assertIn(HOSTILE_MARKER, output)  # the worker's stderr is still copied to the kit's
        self.assertEqual(report["setup_error"], "W0: worker exited -9: killed by signal 9")
        self.assertEqual(report["scenarios"], {})
        self.assertNotIn(HOSTILE_MARKER, json.dumps(report))

    def test_setup_error_never_joins_a_partial_line_the_client_left(self):
        code, report, output = run_script(PARTIAL_FORGERY, "W1")
        self.assertEqual(code, 2, output)
        self.assertEqual(report["scenarios"], {})
        self.assertTrue(report["setup_error"].startswith(
            "W1: worker exited 2: lab-w1: precondition failed: the first POST was forwarded and answered 200"
        ), report["setup_error"])
        text = json.dumps(report)
        self.assertNotIn(PARTIAL_MARKER, text)
        self.assertNotIn("12345678901234567890123", text)
        self.assertIn(PARTIAL_MARKER, output)  # still copied to the kit's stderr


UTF8_MARKER = "consumer-lab-c2-utf8-marker-5b19"

# Set FD before this source (1 or 2): writes invalid UTF-8, with no newline, to that fd of the
# worker, then posts nothing and says so honestly.
BAD_UTF8 = r'''
import json, os
fd = os.open(f"/proc/{os.getppid()}/fd/{FD}", os.O_WRONLY)
os.write(fd, b"\xff\xfe consumer-lab-c2-utf8-marker-5b19 \xff\xfe")
with open(os.environ["TC_EVENTS"], "a") as f:
    f.write(json.dumps({"event": "write", "id": os.environ["TC_WRITE_ID"], "outcome": "not_written"}) + "\n")
'''


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class WriterKitV023(unittest.TestCase):
    """Invalid UTF-8 on the worker's stdout or stderr: a report, not a crash of the parent."""

    def check(self, fd: int) -> str:
        code, report, output = run_script(f"FD = {fd}\n" + BAD_UTF8, "W0")
        self.assertIsNotNone(report, output)
        self.assertNotIn("Traceback", output)
        self.assertIsNone(report["setup_error"], output)
        # W0 wants the write; this client posts nothing and says so: judged, not crashed.
        self.assertEqual(verdicts(report), {"W0": "FAIL:missing_write"}, output)
        self.assertEqual(report["scenarios"]["W0"]["reason"], "W0: nothing stored on a clean path (not_written)")
        self.assertEqual(report["scenarios"]["W0"]["outcome"], "not_written", output)
        self.assertEqual(code, 1, output)
        self.assertNotIn(UTF8_MARKER, json.dumps(report))
        return output

    def test_invalid_utf8_on_the_workers_stderr(self):
        output = self.check(2)
        bad = chr(0xFFFD) * 2  # each invalid byte, replaced
        self.assertIn(f"{bad} {UTF8_MARKER} {bad}", output)  # still copied to the kit's stderr

    def test_invalid_utf8_on_the_workers_stdout(self):
        self.check(1)


FLOOD_MIB = 256
# Peak RSS of the parent `run` process with the flood below (RUSAGE_SELF, children excluded).
# Calibrated in the C3 round (v0.2.4): measured 35.4 MiB; the spec's ceiling is 256 MiB, and
# v0.2.3, which held the whole output in memory, went far past it (see the round's red.log).
PARENT_RSS_BOUND_MIB = 128

# Set FLOOD_MIB and DONE before this source. Floods its worker's stdout with FLOOD_MIB MiB of
# 64-byte lines, records how many bytes it wrote, then posts nothing and says so honestly.
FLOOD_STDOUT = r"""
import json, os
fd = os.open(f"/proc/{os.getppid()}/fd/1", os.O_WRONLY)
chunk = (b"consumer-lab flood " + b"x" * 44 + b"\n") * (1 << 14)  # 1 MiB of 64-byte lines
written = 0
for _ in range(FLOOD_MIB):
    view = memoryview(chunk)
    while view:
        n = os.write(fd, view)
        written += n
        view = view[n:]
os.close(fd)
with open(DONE, "w") as f:
    f.write(str(written))
with open(os.environ["TC_EVENTS"], "a") as f:
    f.write(json.dumps({"event": "write", "id": os.environ["TC_WRITE_ID"], "outcome": "not_written"}) + "\n")
"""

# The parent `run`, with its own peak RSS (children excluded) as the last stderr line.
MEASURED_RUN = r"""
import resource, sys
from consumer_lab import cli
code = cli.main(sys.argv[1:])
print(f"PARENT_MAXRSS_KIB {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}", file=sys.stderr)
sys.exit(code)
"""


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class WriterKitV024(unittest.TestCase):
    """A client flooding its worker's stdout: the parent keeps at most 1 MiB of each stream."""

    def test_flooded_stdout_keeps_the_parent_small(self):
        # Writes FLOOD_MIB MiB to a file in the parent's temp dir: needs that much free in /tmp
        # (about 1 s here; v0.2.3 took far longer and far more memory).
        with tempfile.TemporaryDirectory(prefix="consumer-lab-wclient-") as tmp:
            script, out, done = Path(tmp) / "client.py", Path(tmp) / "report.json", Path(tmp) / "done"
            script.write_text(f"FLOOD_MIB = {FLOOD_MIB}\nDONE = {str(done)!r}\n" + FLOOD_STDOUT)
            cmd = [sys.executable, "-B", "-c", MEASURED_RUN, "run", "--upstream", str(TREE),
                   "--client", f"{sys.executable} {script}", "--scenario", "W0", "--json", str(out)]
            started = time.monotonic()
            proc = subprocess.run(cmd, cwd=REPO, capture_output=True, timeout=900)
            took = time.monotonic() - started
            report = json.loads(out.read_text()) if out.exists() else None
            written = int(done.read_text()) if done.exists() else None
        stderr = proc.stderr.decode("utf-8", errors="replace")
        output = proc.stdout.decode("utf-8", errors="replace") + stderr
        rss = [ln for ln in stderr.splitlines() if ln.startswith("PARENT_MAXRSS_KIB ")]
        self.assertEqual(written, FLOOD_MIB << 20, output[-2000:])
        self.assertTrue(rss, output[-2000:])
        rss_mib = int(rss[-1].split()[1]) / 1024
        print(f"\n[flood] {FLOOD_MIB} MiB on the worker's stdout: parent peak RSS {rss_mib:.1f} MiB "
              f"(bound {PARENT_RSS_BOUND_MIB} MiB), exit {proc.returncode}, {took:.1f} s", file=sys.stderr)
        self.assertIsNotNone(report, output[-2000:])
        self.assertNotIn("Traceback", output)
        if report["setup_error"] is None:  # the worker's own last line was read
            self.assertEqual(verdicts(report), {"W0": "FAIL:missing_write"}, output[-2000:])
            self.assertEqual(proc.returncode, 1, output[-2000:])
        else:
            self.assertEqual(report["setup_error"], "W0: worker printed no valid result")
        self.assertLess(rss_mib, PARENT_RSS_BOUND_MIB, output[-2000:])


SEED_REPOST = r'''
import json, os, urllib.request
# A W0 writer that may post the seed text once more before its own write (MODE, set above):
# "read" takes the seed record's text from the room, "literal" posts the literal seed text
# without reading, "none" posts nothing extra (the control). Then it posts its write once and
# reports it confirmed, which is true on the clean path.
base = os.environ["TECHNOCORE_URL"] + "/r/" + os.environ["TC_ROOM"]
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
timeout = float(os.environ["TC_WRITE_TIMEOUT"])

def post(text):
    body = json.dumps({"from": os.environ["TC_WRITE_FROM"], "text": text}).encode()
    req = urllib.request.Request(base + "?format=json", data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    with opener.open(req, timeout=timeout) as resp:
        resp.read()

if MODE == "read":
    with opener.open(base + "?format=json", timeout=timeout) as resp:
        messages = json.loads(resp.read())["messages"]
    assert len(messages) == 1, messages  # the kit's seed, and nothing else yet
    post(messages[0]["text"])
elif MODE == "literal":
    post("consumer-lab seed")
post(os.environ["TC_WRITE_TEXT"])
with open(os.environ["TC_EVENTS"], "a") as f:
    f.write(json.dumps({"event": "write", "id": os.environ["TC_WRITE_ID"], "outcome": "confirmed"}) + "\n")
'''


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class SeedRepost(unittest.TestCase):
    # The kit identifies its seed by the seq upstream gave it, so a client that posts the seed
    # text again has posted another record: a verdict (exit 1), not a setup error (exit 2).

    def check(self, mode: str, verdict: str, other_records: int) -> None:
        code, report, output = run_script(f"MODE = {mode!r}\n" + SEED_REPOST, "W0")
        self.assertIsNotNone(report, output)
        self.assertIsNone(report["setup_error"], output)
        self.assertNotIn("the seed is stored exactly once", output)
        self.assertEqual(verdicts(report), {"W0": verdict}, output)
        r = report["scenarios"]["W0"]
        self.assertEqual((r["stored"], r["other_records"], r["outcome"]), (1, other_records, "confirmed"), output)
        self.assertEqual(code, 0 if verdict == "PASS" else 1, output)

    def test_reposting_the_seed_read_from_the_room_is_an_extra_post(self):
        self.check("read", "FAIL:extra_post", 1)

    def test_posting_the_literal_seed_text_is_an_extra_post(self):
        self.check("literal", "FAIL:extra_post", 1)

    def test_the_same_client_without_the_repost_passes(self):
        self.check("none", "PASS", 0)


class SeedSeqFromThePostResponse(unittest.TestCase):
    """`writers._seed` on a stubbed seed POST response (no upstream needed): the seed is
    identified by the `seq` in it, so a response without an integer seq (a bool included) is a
    failed precondition, never a guess."""

    MESSAGE = r"^lab-w0: precondition failed: the seed POST's response carries no integer seq$"

    def setUp(self):
        sys.path.insert(0, str(REPO))
        from consumer_lab import writers as W
        from consumer_lab.scenarios import NotDeterministic
        self.W, self.ND = W, NotDeterministic

    def seed(self, posted: dict):
        body = json.dumps({"posted": posted}).encode()
        opened = []

        def fake_open(req, timeout=None):
            opened.append(req.full_url)
            return io.BytesIO(body)

        try:
            with mock.patch.object(self.W._NO_PROXY, "open", fake_open):
                return self.W._seed(SimpleNamespace(url="http://127.0.0.1:9"), "lab-w0")
        finally:
            self.assertEqual(opened, ["http://127.0.0.1:9/r/lab-w0?format=json"])  # one POST, stubbed

    def test_an_integer_seq_is_returned(self):
        self.assertEqual(self.seed({"from": "lab", "seq": 7, "text": self.W.SEED_TEXT}), 7)

    def test_a_missing_or_non_integer_seq_is_not_deterministic(self):
        for name, extra in (("missing", {}), ("null", {"seq": None}), ("string", {"seq": "1"}),
                            ("float", {"seq": 1.0})):
            with self.subTest(name), self.assertRaisesRegex(self.ND, self.MESSAGE):
                self.seed({"from": "lab", "text": self.W.SEED_TEXT, **extra})

    def test_a_bool_seq_is_not_deterministic(self):
        with self.assertRaisesRegex(self.ND, self.MESSAGE):
            self.seed({"from": "lab", "seq": True, "text": self.W.SEED_TEXT})


class ReportLog(unittest.TestCase):
    """`writers.report_entry` on synthetic proxy log entries; no upstream needed."""

    def setUp(self):
        sys.path.insert(0, str(REPO))
        from consumer_lab import writers as W
        self.W = W

    def rep(self, path, method="POST", room="lab-w2"):
        raw = {"method": method, "path_without_query": path, "action": "pass",
               "forwarded": True, "origin_status": 200}
        out = self.W.report_entry(raw, room)
        self.assertEqual(raw["path_without_query"], path)  # the raw entry is not touched
        self.assertEqual((out["action"], out["forwarded"], out["origin_status"]), ("pass", True, 200))
        return out

    def test_routed_room_paths(self):
        for path, reported, differs in (
            ("/r/lab%2Dw2", "/r/lab-w2", True),
            ("/r/lab-w2#x", "/r/lab-w2", True),
            ("/r/lab-w2", "/r/lab-w2", False),
            ("/r/lab-w2/export", "/r/lab-w2/export", False),
            ("/rooms", "/rooms", False),
        ):
            with self.subTest(path):
                out = self.rep(path)
                self.assertEqual((out["path_without_query"], out["raw_path_differs"]), (reported, differs))

    def test_other_paths(self):
        for path in ("/r/lab-w2/abc", "/r/lab-w3", "/", "/r/lab-w2/export/x", "?", "/rooms/0123456789abcdef"):
            with self.subTest(path):
                out = self.rep(path)
                self.assertEqual((out["path_without_query"], out["raw_path_differs"]), ("<other>", True))

    def test_methods(self):
        for method in ("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "?"):
            self.assertEqual(self.rep("/rooms", method=method)["method"], method)
        for method in ("ABCDEF0123456789", "get", "PROPFIND"):
            self.assertEqual(self.rep("/rooms", method=method)["method"], "<other>")

    def test_report_log_keeps_order_and_keys(self):
        log = [{"method": "POST", "path_without_query": p, "action": "a", "forwarded": False,
                "origin_status": None} for p in ("/r/lab-w0", "/x")]
        out = self.W.report_log(log, "lab-w0")
        self.assertEqual([e["path_without_query"] for e in out], ["/r/lab-w0", "<other>"])
        self.assertEqual(set(out[0]), {"method", "path_without_query", "raw_path_differs", "action",
                                       "forwarded", "origin_status"})


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
