"""End-to-end: both reference clients against all 5 scenarios on the real upstream service.

This is the kit's proof that it discriminates: the careful client must pass everything and
the naive one must fail the scenarios that lose records. Needs Linux, the venv from
scripts/setup_wsl.sh, and an upstream tree (CONSUMER_LAB_UPSTREAM, default
~/consumer-lab-upstream-0e47f77). Skipped, loudly, when the tree is absent.
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
        self.assertEqual(report["upstream_commit"], "0e47f770b13cc27e1e2e199d4cdf70a4778c97cc")
        for r in report["scenarios"].values():
            self.assertEqual(set(r["events"]), {"deliver", "loss", "other", "total"})
        text = json.dumps(report)
        self.assertNotIn("consumer-lab record", text)  # no record texts in the report


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
