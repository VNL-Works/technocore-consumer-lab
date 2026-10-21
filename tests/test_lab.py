"""End-to-end: both reference clients against all 5 scenarios on the real upstream service.

This is the kit's proof that it discriminates: the careful client must pass everything and
the naive one must fail the scenarios that lose records. Needs Linux, the venv from
scripts/setup_wsl.sh, and an upstream tree (CONSUMER_LAB_UPSTREAM, default
~/consumer-lab-upstream-4a013e5). Skipped, loudly, when the tree is absent.
"""

import contextlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO = Path(__file__).resolve().parent.parent
TREE = Path(os.environ.get("CONSUMER_LAB_UPSTREAM", "~/consumer-lab-upstream-4a013e5")).expanduser()
HAVE_TREE = (TREE / "src" / "store.py").is_file()


def run_kit(client: str, scenario: str = "all", upstream: Path = TREE) -> tuple[int, dict | None, str]:
    with tempfile.TemporaryDirectory(prefix="consumer-lab-test-") as tmp:
        out = Path(tmp) / "report.json"
        cmd = [sys.executable, "-m", "consumer_lab", "run", "--upstream", str(upstream),
               "--client", f"{sys.executable} {REPO / 'clients' / client}",
               "--scenario", scenario, "--json", str(out)]
        proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, timeout=900)
        report = json.loads(out.read_text()) if out.exists() else None
        return proc.returncode, report, proc.stdout + proc.stderr


def verdicts(report: dict) -> dict[str, str]:
    return {sid: r["verdict"] for sid, r in report["scenarios"].items()}


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class ReferenceClients(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.careful = run_kit("careful_client.py")
        cls.naive = run_kit("naive_client.py")

    def test_careful_passes_all_five(self):
        code, report, output = self.careful
        self.assertEqual(code, 0, output)
        self.assertEqual(verdicts(report), {f"S{n}": "PASS" for n in range(1, 6)}, output)
        self.assertTrue(report["all_pass"])

    def test_naive_fails_and_exits_1(self):
        code, report, output = self.naive
        self.assertEqual(code, 1, output)
        self.assertFalse(report["all_pass"])

    def test_naive_s1(self):
        # interop.md's main loop sends no `limit`, so each poll is the newest 50 records with
        # seq > since. After 300 new records it delivers 261..310 and skips 11..260, which are
        # still retained. Observed: FAIL:missed_retained (the expected verdict in the spec).
        self.assertEqual(verdicts(self.naive[1])["S1"], "FAIL:missed_retained")

    def test_naive_fails_lossy_scenarios(self):
        v = verdicts(self.naive[1])
        for sid in ("S2", "S3", "S5"):
            self.assertIn(v[sid], ("FAIL:unreported_loss", "FAIL:missed_retained"), sid)

    def test_naive_s4(self):
        # Observed: the prefix-expired window returns record 7 alone and the naive loop
        # delivers it with no first_seq check, so the expiry of 4..6 goes unreported.
        self.assertEqual(verdicts(self.naive[1])["S4"], "FAIL:unreported_loss")

    def test_report_shape(self):
        report = self.careful[1]
        self.assertEqual(report["upstream_commit"], "4a013e5401f15e56762456d1d48c4e676e2cff2c")
        for r in report["scenarios"].values():
            self.assertEqual(set(r["events"]), {"deliver", "loss", "other", "total"})
        text = json.dumps(report)
        self.assertNotIn("consumer-lab record", text)  # no record texts in the report


REWRITER = r'''
import json, os, sys
# Cycle 1: deliver 1..10. Cycle 2: rewrite the whole log, 1..310, opened with "w" instead of
# "a" and serialized compactly, so the first cycle's bytes change. The events are right; only
# the append-only rule is broken. (A rewrite to byte-identical content is indistinguishable
# from an append, and harmless.)
state = os.path.join(os.environ["TC_STATE_DIR"], "n")
n = int(open(state).read()) if os.path.exists(state) else 0
seqs = range(1, 11) if n == 0 else range(1, 311)
with open(os.environ["TC_EVENTS"], "a" if n == 0 else "w") as f:
    for s in seqs:
        ev = {"event": "deliver", "seq": s, "generation": 1}
        f.write((json.dumps(ev) if n == 0 else json.dumps(ev, separators=(",", ":"))) + "\n")
open(state, "w").write(str(n + 1))
'''

ENV_PROBE = r'''
import json, os
# A malformed line if any CHAT_* server knob leaked into the client's environment.
if any(k.startswith("CHAT_") for k in os.environ):
    open(os.environ["TC_EVENTS"], "a").write("CHAT_ leaked\n")
'''


EARLY_S3 = r'''
import json, os, runpy, sys
# The careful client, plus a loss for the S3 gap written in the second cycle, the earliest
# cycle in which the gap exists (a /rooms-aware client can see it there). Deterministic: it
# keys on its own invocation count, not on the /rooms cache.
state = os.path.join(os.environ["TC_STATE_DIR"], "invocations")
n = int(open(state).read()) if os.path.exists(state) else 0
open(state, "w").write(str(n + 1))
if n == 1:
    with open(os.environ["TC_EVENTS"], "a") as f:
        f.write(json.dumps({"event": "loss", "after_seq": 3, "reason": "head 6 above cursor 3"}) + "\n")
sys.argv = [CAREFUL]
runpy.run_path(CAREFUL, run_name="__main__")
'''

NESTED = r'''
import os
# A line of 200000 "[": json.loads raises RecursionError (not ValueError) on it. The kit must
# judge it as a malformed line (FAIL:bad_events), not stop with a setup error.
with open(os.environ["TC_EVENTS"], "a") as f:
    f.write("[" * 200000 + "\n")
'''


PARTIAL_STDOUT = r'''
import os
# Leaves a partial line, with no newline, on the worker's stdout (the parent reads the
# worker's last stdout line as its result), then exits normally without events.
fd = os.open(f"/proc/{os.getppid()}/fd/1", os.O_WRONLY)
os.write(fd, b'{"forged": ')
'''


def run_script_client(source: str, scenario: str, *extra: str) -> tuple[int, dict | None, str]:
    with tempfile.TemporaryDirectory(prefix="consumer-lab-client-") as tmp:
        script = Path(tmp) / "client.py"
        script.write_text(source)
        out = Path(tmp) / "report.json"
        cmd = [sys.executable, "-m", "consumer_lab", "run", "--upstream", str(TREE),
               "--client", f"{sys.executable} {script}", "--scenario", scenario,
               "--json", str(out), *extra]
        proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, timeout=300)
        report = json.loads(out.read_text()) if out.exists() else None
        return proc.returncode, report, proc.stdout + proc.stderr


@unittest.skipUnless(HAVE_TREE, f"no upstream tree at {TREE}: run scripts/setup_wsl.sh first")
class KitHardening(unittest.TestCase):
    def test_rewritten_log_is_bad_events(self):
        code, report, output = run_script_client(REWRITER, "S1")
        self.assertEqual(code, 1, output)
        self.assertEqual(verdicts(report)["S1"], "FAIL:bad_events", output)
        self.assertIn("rewritten", report["scenarios"]["S1"]["reason"])

    def test_s3_loss_in_second_cycle_passes(self):
        # Regression (review R-1): S3's gap is observable from the second cycle.
        source = f"CAREFUL = {str(REPO / 'clients' / 'careful_client.py')!r}\n" + EARLY_S3
        code, report, output = run_script_client(source, "S3")
        self.assertEqual(code, 0, output)
        self.assertEqual(verdicts(report)["S3"], "PASS", output)
        self.assertEqual(report["scenarios"]["S3"]["events"]["loss"], 2, output)

    def test_client_gets_no_chat_env(self):
        code, report, output = run_script_client(ENV_PROBE, "S1")
        self.assertEqual(code, 1, output)
        # Silent client: missed records, but no "CHAT_ leaked" malformed line.
        self.assertEqual(verdicts(report)["S1"], "FAIL:missed_retained", output)

    def test_keep_root_writes_result_json(self):
        code, report, output = run_script_client(ENV_PROBE, "S1", "--keep-root")
        kept = [ln.split(" ", 1)[1] for ln in output.splitlines() if ln.startswith("kept ")]
        self.assertEqual(len(kept), 1, output)
        try:
            result = json.loads((Path(kept[0]) / "result.json").read_text())
        finally:
            import shutil
            shutil.rmtree(kept[0], ignore_errors=True)
        self.assertEqual(result["verdict"], "FAIL:missed_retained")
        self.assertEqual(result["exit_code"], 0)
        self.assertEqual(result["cycle_exit_codes"], [0, 0])

    def test_deeply_nested_line_is_bad_events(self):
        code, report, output = run_script_client(NESTED, "S1")
        self.assertEqual(code, 1, output)
        self.assertIsNone(report["setup_error"], output)
        self.assertEqual(verdicts(report)["S1"], "FAIL:bad_events", output)
        self.assertEqual(report["scenarios"]["S1"]["reason"], "S1: malformed event log, line 1 is not JSON")
        events = report["scenarios"]["S1"]["events"]
        self.assertEqual((events["deliver"], events["loss"]), (0, 0), output)
        self.assertEqual(events["other"], events["total"], output)  # each line counted malformed

    def test_partial_line_on_the_workers_stdout_is_not_joined_to_the_result(self):
        # The parent used to json.loads the joined line and crash: no report, exit 1.
        code, report, output = run_script_client(PARTIAL_STDOUT, "S1")
        self.assertIsNotNone(report, output)
        self.assertEqual(code, 1, output)
        self.assertIsNone(report["setup_error"], output)
        self.assertEqual(verdicts(report)["S1"], "FAIL:missed_retained", output)
        self.assertNotIn("Traceback", output)

    def test_json_into_missing_dir_exits_2(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "no-such-dir" / "x.json"
            cmd = [sys.executable, "-m", "consumer_lab", "run", "--upstream", str(TREE),
                   "--client", f"{sys.executable} {REPO / 'clients' / 'careful_client.py'}",
                   "--scenario", "S1", "--json", str(target)]
            proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, timeout=120)
        self.assertEqual(proc.returncode, 2, proc.stderr)
        self.assertNotIn("Traceback", proc.stderr)
        self.assertEqual(proc.stderr.strip().splitlines(), [f"setup error: --json: directory {target.parent} does not exist"])


class InvalidWorkerResult(unittest.TestCase):
    """`_run` with a worker that exits 0 but prints no valid result: a setup error worded by
    the kit, with a report, never a crash. After the worker's fresh-line result (v0.2.3) no
    client recipe reaches this path, so the worker is mocked; no upstream needed."""

    def run_with_stdout(self, stdout: bytes) -> tuple[int, dict | None, str]:
        sys.path.insert(0, str(REPO))
        from consumer_lab import cli
        tmp = Path(tempfile.mkdtemp(prefix="consumer-lab-wres-"))
        self.addCleanup(shutil.rmtree, tmp, True)
        out = tmp / "report.json"
        def done(cmd, **kwargs):  # since v0.2.4 the worker's stdout is a file of the parent's
            if hasattr(kwargs.get("stdout"), "write"):
                kwargs["stdout"].write(stdout)
                kwargs["stdout"].flush()
            return subprocess.CompletedProcess(cmd, 0, stdout=stdout, stderr=b"")

        argv = ["run", "--upstream", str(tmp), "--client", "true", "--scenario", "S1", "--json", str(out)]
        err = io.StringIO()
        with mock.patch.object(cli, "marker_ok", return_value=(True, "")), \
                mock.patch.object(cli, "missing_upstream_api", return_value=[]), \
                mock.patch.object(cli, "upstream_commit", return_value="unknown"), \
                mock.patch.object(cli.subprocess, "run", side_effect=done), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
            code = cli.main(argv)
        report = json.loads(out.read_text()) if out.exists() else None
        return code, report, err.getvalue()

    def test_invalid_result_is_a_setup_error(self):
        for stdout in (
            b"",
            b"\n\n",
            b"not json\n",
            b'{"forged": \n',
            b"[1, 2]\n",
            b'"S1"\n',
            b'{"id": "S1", "name": "x", "verdict": "PASS"}\n',  # no reason
            b'{"name": "x", "verdict": "PASS", "reason": "r"}\n',  # no id
            b'{"id": "S1", "name": null, "verdict": "PASS", "reason": "r"}\n',
            b'{"id": "S1", "name": "x", "verdict": ["PASS"], "reason": "r"}\n',
            b"\xff\xfe\n",
            b"[" * 200000 + b"\n",
        ):
            with self.subTest(stdout=stdout[:60]):
                code, report, err = self.run_with_stdout(stdout)
                self.assertEqual(code, 2, err)
                self.assertIsNotNone(report, err)
                self.assertEqual(report["setup_error"], "S1: worker printed no valid result")
                self.assertEqual(report["scenarios"], {})
                self.assertFalse(report["all_pass"])
                self.assertEqual(err.strip().splitlines()[-1], "setup error: S1: worker printed no valid result")

    def test_valid_result_after_junk_is_taken(self):
        line = b'{"id": "S1", "name": "n", "verdict": "PASS", "reason": "r"}'
        code, report, err = self.run_with_stdout(b'{"forged": \n' + line + b"\n")
        self.assertEqual(code, 0, err)
        self.assertEqual(report["scenarios"]["S1"]["verdict"], "PASS")
        self.assertIsNone(report["setup_error"])


class SetupErrors(unittest.TestCase):
    def test_bad_tree_exits_2(self):
        with tempfile.TemporaryDirectory() as empty:
            code, _, output = run_kit("careful_client.py", "S1", Path(empty))
        self.assertEqual(code, 2, output)
        self.assertIn("not a technocore-chat tree", output)

    def test_missing_private_function_exits_2(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            src.mkdir()
            (src / "store.py").write_text("def _compact(path):\n    pass\n\ndef _reap_pass(root, now):\n    pass\n")
            code, _, output = run_kit("careful_client.py", "S1", Path(tmp))
        self.assertEqual(code, 2, output)
        self.assertIn("missing", output)

    def test_unknown_scenario_exits_2(self):
        if not HAVE_TREE:
            self.skipTest("needs the upstream tree")
        code, _, output = run_kit("careful_client.py", "S9")
        self.assertEqual(code, 2, output)


if __name__ == "__main__":
    unittest.main()
