"""The reader scenarios against upstream trees other than the pin: open PRs #800 and #910.

At the pinned commit an empty window reports `last_seq = min(since, head)`, so the careful
client's "empty page whose last_seq is above the cursor" branch can never run there. PRs #800
(head 6ce0852) and #910 (head b72607e) make an empty window report the room head instead, so
with every later `e-` record expired the page carries `last_seq > since`. These tests run both
reference clients against those trees and prove that the branch ran, and that it cannot run at
the pin.

Trees come from CONSUMER_LAB_VARIANT_UPSTREAMS (os.pathsep-separated); the default is
~/consumer-lab-upstream-pr800-6ce0852 and ~/consumer-lab-upstream-pr910-b72607e, made with
`git archive <sha>` from a technocore-chat clone (no `.consumer-lab-upstream` marker, so the
report says "unknown"). An absent tree is skipped, loudly, per tree. The pinned tree is
CONSUMER_LAB_UPSTREAM, as in test_lab.py. These trees are not the pin: a verdict here says
what the kit observed on them, not what a scenario means (docs/SCENARIOS.md is written for the
pin).
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PINNED = Path(os.environ.get("CONSUMER_LAB_UPSTREAM", "~/consumer-lab-upstream-0e47f77")).expanduser()
DEFAULT_VARIANTS = os.pathsep.join(["~/consumer-lab-upstream-pr800-6ce0852", "~/consumer-lab-upstream-pr910-b72607e"])
VARIANTS = [Path(p).expanduser() for p in os.environ.get("CONSUMER_LAB_VARIANT_UPSTREAMS", DEFAULT_VARIANTS).split(os.pathsep) if p]

EMPTY_WINDOW = "empty window with last_seq"
SCENARIOS = [f"S{n}" for n in range(1, 6)]


def have_tree(tree: Path) -> bool:
    return (tree / "src" / "store.py").is_file()


def run_kept(client: str, upstream: Path) -> tuple[int, dict | None, str, dict[str, list[dict]]]:
    """Run `--scenario all --keep-root`; return the exit code, report, output and each
    scenario's events.jsonl. Every kept dir is deleted before this returns, pass or fail."""
    with tempfile.TemporaryDirectory(prefix="consumer-lab-test-") as tmp:
        out = Path(tmp) / "report.json"
        cmd = [sys.executable, "-m", "consumer_lab", "run", "--upstream", str(upstream),
               "--client", f"{sys.executable} {REPO / 'clients' / client}",
               "--scenario", "all", "--keep-root", "--json", str(out)]
        try:
            proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, timeout=900)
            code, stdout, stderr = proc.returncode, proc.stdout, proc.stderr
        except subprocess.TimeoutExpired as exc:
            def text(b):
                return b.decode(errors="replace") if isinstance(b, bytes) else (b or "")
            code, stdout, stderr = -1, text(exc.stdout), text(exc.stderr)
        report = json.loads(out.read_text()) if out.exists() else None
    kept = [ln.split(" ", 1)[1] for ln in stderr.splitlines() if ln.startswith("kept ")]
    events: dict[str, list[dict]] = {}
    try:
        for d in kept:
            m = re.match(r"consumer-lab-(s\d)-", Path(d).name)
            if m is None:
                continue
            log = Path(d) / "events.jsonl"
            lines = log.read_text().splitlines() if log.exists() else []
            events[m.group(1).upper()] = [json.loads(ln) for ln in lines if ln.strip()]
    finally:
        for d in kept:
            shutil.rmtree(d, ignore_errors=True)
    return code, report, stdout + stderr, events


def run_plain(client: str, upstream: Path) -> tuple[int, dict | None, str]:
    with tempfile.TemporaryDirectory(prefix="consumer-lab-test-") as tmp:
        out = Path(tmp) / "report.json"
        cmd = [sys.executable, "-m", "consumer_lab", "run", "--upstream", str(upstream),
               "--client", f"{sys.executable} {REPO / 'clients' / client}",
               "--scenario", "all", "--json", str(out)]
        proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, timeout=900)
        report = json.loads(out.read_text()) if out.exists() else None
        return proc.returncode, report, proc.stdout + proc.stderr


def verdicts(report: dict) -> dict[str, str]:
    return {sid: r["verdict"] for sid, r in report["scenarios"].items()}


def empty_window_losses(events: dict[str, list[dict]]) -> dict[str, list[dict]]:
    return {sid: [e for e in evs if e.get("event") == "loss" and str(e.get("reason", "")).startswith(EMPTY_WINDOW)]
            for sid, evs in events.items()}


class _VariantTree(unittest.TestCase):
    """Both reader clients on one variant tree. Subclassed once per tree below."""

    TREE: Path

    @classmethod
    def setUpClass(cls):
        cls.careful = run_kept("careful_client.py", cls.TREE)
        cls.naive = run_plain("naive_client.py", cls.TREE)

    def test_careful_passes_all_five(self):
        code, report, output, _ = self.careful
        self.assertEqual(code, 0, output)
        self.assertIsNotNone(report, output)
        self.assertEqual(verdicts(report), {sid: "PASS" for sid in SCENARIOS}, output)
        self.assertTrue(report["all_pass"])
        # No marker in a git-archived tree: the report does not claim a commit it cannot check.
        self.assertEqual(report["upstream_commit"], "unknown")

    def test_empty_window_branch_ran(self):
        # The reason of careful_client.py's empty-window branch. Observed on both #800 and
        # #910: it fires in S3 only, in the middle cycle, where every record after the cursor
        # (3) is expired and the empty page now reports the head (6). S4 never has an empty
        # page (record 7 is live), so it is caught by the first_seq check as at the pin.
        code, report, output, events = self.careful
        self.assertEqual(sorted(events), SCENARIOS, output)  # every scenario's log was read
        fired = {sid: losses for sid, losses in empty_window_losses(events).items() if losses}
        self.assertEqual(set(fired), {"S3"}, output)
        self.assertEqual([(e["after_seq"], e["reason"]) for e in fired["S3"]],
                         [(3, "empty window with last_seq 6 > since 3")])

    def test_naive_verdicts(self):
        code, report, output = self.naive
        self.assertEqual(code, 1, output)
        self.assertFalse(report["all_pass"])
        # Observed on both #800 and #910; every verdict equals the pinned-tree README table.
        # S3 fails the same way for a different path: the empty window's last_seq (6) moves
        # the naive cursor past the expired 4..6 in the middle cycle, instead of record 7
        # moving it in the last cycle. Either way no loss is reported.
        self.assertEqual(verdicts(report), {
            "S1": "FAIL:missed_retained",
            "S2": "FAIL:unreported_loss",
            "S3": "FAIL:unreported_loss",
            "S4": "FAIL:unreported_loss",
            "S5": "FAIL:unreported_loss",
        }, output)


def _class_name(tree: Path) -> str:
    return "VariantTree_" + re.sub(r"\W", "_", tree.name.removeprefix("consumer-lab-upstream-"))


for _tree in VARIANTS:
    _cls = type(_class_name(_tree), (_VariantTree,), {"TREE": _tree, "__doc__": f"Variant tree {_tree}."})
    _cls = unittest.skipUnless(have_tree(_tree), f"no variant upstream tree at {_tree}: create it with "
                               "`git archive` from a technocore-chat clone (see the module docstring)")(_cls)
    globals()[_cls.__name__] = _cls
for _name in ("_VariantTree", "_cls", "_tree"):  # the base runs nothing by itself, and a loop
    globals().pop(_name, None)                    # variable left behind would be collected twice


@unittest.skipUnless(have_tree(PINNED), f"no upstream tree at {PINNED}: run scripts/setup_wsl.sh first")
class PinnedTreeContrast(unittest.TestCase):
    def test_empty_window_branch_unreachable_at_pin(self):
        # At 0e47f77 an empty window reports last_seq = min(since, head), never above the
        # cursor, so the branch the variant trees exercise cannot run here. This is why
        # test_empty_window_branch_ran needs the other trees.
        code, report, output, events = run_kept("careful_client.py", PINNED)
        self.assertEqual(code, 0, output)
        self.assertEqual(verdicts(report), {sid: "PASS" for sid in SCENARIOS}, output)
        self.assertEqual(sorted(events), SCENARIOS, output)
        self.assertEqual({sid: losses for sid, losses in empty_window_losses(events).items() if losses}, {}, output)


if __name__ == "__main__":
    unittest.main()
