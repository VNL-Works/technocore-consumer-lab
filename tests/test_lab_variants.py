"""The reader scenarios against upstream trees other than the pin: open PRs #800 and #910.

At the pinned commit an empty window reports `last_seq = min(since, head)`, so the careful
client's "empty page whose last_seq is above the cursor" branch can never run there. PRs #800
(head f686ceb) and #910 (head b72607e) make an empty window report the room head instead, so
with every later `e-` record expired the page carries `last_seq > since`. These tests run both
reference clients against those trees and prove that the branch ran, and that it cannot run at
the pin. #800's earlier heads 6ce0852 and 01cecdc gave the same results, and
CONSUMER_LAB_VARIANT_UPSTREAMS can still point at those trees.

Trees come from CONSUMER_LAB_VARIANT_UPSTREAMS (os.pathsep-separated); the default is
~/consumer-lab-upstream-pr800-f686ceb and ~/consumer-lab-upstream-pr910-b72607e, made with
`git archive <sha>` from a technocore-chat clone (no `.consumer-lab-upstream` marker, so the
report says "unknown"). An absent tree is skipped, loudly, per tree. The pinned tree is
CONSUMER_LAB_UPSTREAM, as in test_lab.py. These trees are not the pin: a verdict here says
what the kit observed on them, not what a scenario means (docs/SCENARIOS.md is written for the
pin).

RuntimeFilesUnchanged guards why #545 (a84a3ff) and the #545 x #800 merge tree (6cd4eb9) get
no runs of their own: the code the kit runs is byte-identical to the pin's and to #800's
respectively. consumer_lab/server.py imports config, store and app from <tree>/src, and app and
store import the other src/ modules (didkey, limit, manifest), so the guard compares every
entry at any depth under src/ (the union of both trees' paths, so an added or removed entry
fails too) plus uv.lock and pyproject.toml. A tree is skipped only when <tree>/src does not
exist by lstat. If src exists (checked in both trees before any skip), it must be a real
directory holding a regular src/store.py, both by lstat, or the test fails: a symlinked src,
even a dangling one, fails (os.walk would follow it), and so does a src whose store.py is
missing, a directory or a symlink (dangling or not).
Each entry's kind (file, directory or symlink, never following a symlink) must match; files
must be byte-identical, and symlinks must have the same target text and must resolve
(os.path.realpath) inside that tree's own src (a dangling target inside src is allowed; a
symlink loop, src/loop -> loop or a -> b -> a, fails), and
not to an existing served-text file the comparison skips (src/limit.py -> limit.md fails,
through a chain of links too).
uv.lock and pyproject.toml must be regular files by lstat (not symlinks) and byte-identical.
__pycache__ entries are compared like any other (CPython can load a matching .pyc instead
of the source, and -B stops writing pycs, not reading them), so they must be absent from
both trees or identical. The only exclusion is the served text files directly in src/ (a
regular file whose name ends in .md, or humans.html; see SERVED_TEXT_SUFFIXES and
SERVED_TEXT_NAMES), since no kit client fetches them. Those trees come from
CONSUMER_LAB_PR545_UPSTREAM (default ~/consumer-lab-upstream-pr545-a84a3ff) and
CONSUMER_LAB_COMBO_UPSTREAM (default ~/consumer-lab-upstream-combo-6cd4eb9), made with
`git archive` of the commit and of the merge's tree id.
"""

import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PINNED = Path(os.environ.get("CONSUMER_LAB_UPSTREAM", "~/consumer-lab-upstream-4a013e5")).expanduser()
PR800 = Path("~/consumer-lab-upstream-pr800-f686ceb").expanduser()
DEFAULT_VARIANTS = os.pathsep.join([str(PR800), "~/consumer-lab-upstream-pr910-b72607e"])
VARIANTS = [Path(p).expanduser() for p in os.environ.get("CONSUMER_LAB_VARIANT_UPSTREAMS", DEFAULT_VARIANTS).split(os.pathsep) if p]
PR545 = Path(os.environ.get("CONSUMER_LAB_PR545_UPSTREAM", "~/consumer-lab-upstream-pr545-a84a3ff")).expanduser()
COMBO = Path(os.environ.get("CONSUMER_LAB_COMBO_UPSTREAM", "~/consumer-lab-upstream-combo-6cd4eb9")).expanduser()
# What the kit runs from an upstream tree: consumer_lab/server.py imports config, store and app
# from <tree>/src, and app and store import the other src/ modules, so every entry under src/
# counts (see src_entries and compared_src_paths); all of it runs under a venv built from
# uv.lock and pyproject.toml, which must be regular files (by lstat) with the same bytes.
LOCK_FILES = ["uv.lock", "pyproject.toml"]
# Served text files: excluded only when a regular file directly in src/ (deeper files with
# these names, and a symlink or directory with these names, count).
SERVED_TEXT_SUFFIXES = (".md",)
SERVED_TEXT_NAMES = ("humans.html",)


def _kind(path: Path) -> str:
    """The kind of path itself, by lstat: a symlink is "symlink" whatever it points at."""
    mode = os.lstat(path).st_mode
    if stat.S_ISLNK(mode):
        return "symlink"
    if stat.S_ISDIR(mode):
        return "dir"
    if stat.S_ISREG(mode):
        return "file"
    return "other"


def _raise(exc: OSError):
    raise exc


def src_entries(tree: Path) -> dict[str, str]:
    """{"src/<POSIX path>": kind} for every entry at any depth under <tree>/src, walked
    without following symlinks (a symlinked directory is one "symlink" entry, not descended;
    a dangling symlink is listed too). __pycache__ directories are walked like any other. A
    walk error raises instead of hiding part of the tree. os.walk follows root itself even
    with followlinks=False, so callers check that <tree>/src is a real directory first."""
    root = tree / "src"
    entries = {}
    for dirpath, dirnames, filenames in os.walk(root, onerror=_raise, followlinks=False):
        for name in dirnames + filenames:
            path = Path(dirpath, name)
            entries[f"src/{path.relative_to(root).as_posix()}"] = _kind(path)
    return entries


def _is_excluded(rel: str, kind: str) -> bool:
    """rel is "src/..."; kind is its kind in one tree. Only the served-text rule."""
    parts = rel.split("/")[1:]
    name = parts[-1]
    return (kind == "file" and len(parts) == 1
            and (name.endswith(SERVED_TEXT_SUFFIXES) or name in SERVED_TEXT_NAMES))


def compared_src_paths(*entries: dict[str, str]) -> list[str]:
    """The sorted union of the src_entries paths, dropping a path only when, in every tree
    where it exists, it is excluded (so a served file replaced by a symlink is compared)."""
    return sorted({rel for e in entries for rel in e
                   if not all(_is_excluded(rel, x[rel]) for x in entries if rel in x)})


EMPTY_WINDOW = "empty window with last_seq"
SCENARIOS = [f"S{n}" for n in range(1, 6)]


def have_tree(tree: Path) -> bool:
    return (tree / "src" / "store.py").is_file()


def run_kept(client: str, upstream: Path) -> tuple[int, dict | None, str, dict[str, list[dict] | None]]:
    """Run `--scenario all --keep-root`; return the exit code, report, output and each
    scenario's events.jsonl (None when the log is missing). Every kept dir is deleted before
    this returns, pass or fail."""
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
    events: dict[str, list[dict] | None] = {}
    try:
        for d in kept:
            m = re.match(r"consumer-lab-(s\d)-", Path(d).name)
            if m is None:
                continue
            log = Path(d) / "events.jsonl"
            # A missing log is None, not []: a check over "no events" must not pass by accident.
            events[m.group(1).upper()] = ([json.loads(ln) for ln in log.read_text().splitlines() if ln.strip()]
                                          if log.exists() else None)
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


def empty_window_losses(events: dict[str, list[dict] | None]) -> dict[str, list[dict]]:
    return {sid: [e for e in evs if e.get("event") == "loss" and str(e.get("reason", "")).startswith(EMPTY_WINDOW)]
            for sid, evs in events.items() if evs is not None}


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
        # At the pin an empty window reports last_seq = min(since, head), never above the
        # cursor (store.py is identical at 0e47f77), so the branch the variant trees exercise
        # cannot run here. This is why
        # test_empty_window_branch_ran needs the other trees.
        code, report, output, events = run_kept("careful_client.py", PINNED)
        self.assertEqual(code, 0, output)
        self.assertEqual(verdicts(report), {sid: "PASS" for sid in SCENARIOS}, output)
        self.assertEqual(sorted(events), SCENARIOS, output)
        # Not vacuous: every scenario's events.jsonl exists and holds events (S1 alone delivers
        # 310 records), so "no empty-window loss" is a statement about real logs.
        for sid in SCENARIOS:
            self.assertTrue(events[sid], f"{sid}: events.jsonl missing or empty\n{output}")
        self.assertEqual({sid: losses for sid, losses in empty_window_losses(events).items() if losses}, {}, output)


class RuntimeFilesUnchanged(unittest.TestCase):
    """The docs say #545 and the #545 x #800 merge need no kit runs of their own, because they
    change no file the kit runs. Every entry under src/ is compared by kind (see src_entries
    and compared_src_paths), never following a symlink: files by bytes, symlinks by target
    text and by where they resolve, directories by presence. So a later head that changes,
    adds or removes one (a module, a package directory, an empty directory, a
    src/pyproject.toml, a compiled extension, a symlink that could shadow an installed
    library, a dangling symlink, a __pycache__ directory or .pyc) fails this instead of the
    docs quietly going stale. A tree is skipped only when its src is absent by lstat. A src
    that exists must be a real directory (a symlinked src fails, dangling or not) holding a
    regular src/store.py by lstat (missing, a directory or a symlink fails). A symlink under
    src must resolve inside that tree's own src (a link to content outside src fails, even
    with the same target text in both trees; a dangling one passes, a symlink loop fails), and
    not to a served-text file the comparison skips (src/limit.py -> limit.md fails, chains
    too). uv.lock and pyproject.toml must be regular files, then byte-identical. The only
    entries skipped are the served text files directly in src/."""

    def _same_runtime_files(self, base: Path, other: Path):
        trees = (base, other)
        # Before the skip, for every src that exists by lstat (in either tree): it must be a
        # real directory holding a regular store.py (by lstat), else fail. The skip below is
        # only for an absent src (have_tree would follow symlinks and skip a src without a
        # regular store.py).
        for tree in trees:
            if os.path.lexists(tree / "src"):
                kind = _kind(tree / "src")
                self.assertEqual(kind, "dir", f"{tree / 'src'} is a {kind}, not a directory")
                store = tree / "src" / "store.py"
                found = _kind(store) if os.path.lexists(store) else None
                self.assertEqual(found, "file", f"{store} is missing" if found is None
                                 else f"{store} is a {found}, not a regular file")
        for tree in trees:
            if not os.path.lexists(tree / "src"):
                self.skipTest(f"no upstream tree at {tree}: create it with `git archive` from a "
                              "technocore-chat clone (see the module docstring)")
        # Walked once per tree; presence below is lstat-based (a dangling symlink is present).
        entries = [src_entries(t) for t in trees]
        for rel in compared_src_paths(*entries):
            with self.subTest(entry=rel):
                kinds = [e.get(rel) for e in entries]
                missing = [str(t) for t, k in zip(trees, kinds) if k is None]
                self.assertEqual(missing, [], f"{rel} is missing from {missing}")
                odd = [str(t / rel) for t, k in zip(trees, kinds) if k == "other"]
                self.assertEqual(odd, [], f"{rel} is not a file, directory or symlink in {odd}")
                self.assertEqual(kinds[0], kinds[1], f"{rel} is a {kinds[0]} in {base} but a "
                                 f"{kinds[1]} in {other}")
                if kinds[0] == "file":
                    self.assertEqual((base / rel).read_bytes(), (other / rel).read_bytes(),
                                     f"{rel} differs between {base} and {other}")
                elif kinds[0] == "symlink":
                    for t in trees:
                        root = os.path.realpath(t / "src")
                        target = os.path.realpath(t / rel)
                        self.assertEqual(os.path.commonpath([root, target]), root,
                                         f"{rel} in {t} resolves to {target}, outside {root}")
                        # Non-strict realpath returns a symlink only for a loop (a -> a,
                        # a -> b -> a); a dangling target is a path that does not exist.
                        self.assertFalse(os.path.islink(target), f"{rel} in {t} is a symlink loop")
                        # Inside src, the target must not be a file the comparison skips
                        # (realpath follows the whole chain).
                        if os.path.lexists(target):
                            served = f"src/{Path(target).relative_to(root).as_posix()}"
                            self.assertFalse(_is_excluded(served, _kind(Path(target))),
                                             f"{rel} in {t} resolves to served text {served}, "
                                             "which is not compared")
                    self.assertEqual(os.readlink(base / rel), os.readlink(other / rel),
                                     f"{rel} points elsewhere in {other} than in {base}")
        for rel in LOCK_FILES:
            with self.subTest(file=rel):
                kinds = [_kind(t / rel) if os.path.lexists(t / rel) else None for t in trees]
                missing = [str(t) for t, k in zip(trees, kinds) if k is None]
                self.assertEqual(missing, [], f"{rel} is missing from {missing}")
                odd = [f"{t / rel} is a {k}" for t, k in zip(trees, kinds) if k != "file"]
                self.assertEqual(odd, [], f"{rel} is not a regular file: {odd}")
                self.assertEqual((base / rel).read_bytes(), (other / rel).read_bytes(),
                                 f"{rel} differs between {base} and {other}")

    def test_pr545_runs_the_pins_code(self):
        self._same_runtime_files(PINNED, PR545)

    def test_combo_runs_pr800s_code(self):
        self._same_runtime_files(PR800, COMBO)


if __name__ == "__main__":
    unittest.main()
