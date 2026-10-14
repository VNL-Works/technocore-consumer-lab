"""`python -m consumer_lab run --upstream <tree> --client "<cmd>" [--scenario S1..S5|W0..W7|all|writers] [--json out.json]`

Reader scenarios S1..S5 (`all`) and writer scenarios W0..W7 (`writers`) have different client
contracts, so one run selects from one family only. Each scenario runs in its own worker
process (`python -m consumer_lab _worker ...`) with its own temp dir (created by the parent,
so a worker that times out leaves none behind) and its own in-process upstream server,
because upstream reads its env knobs and caches per-root state at import; a writer scenario
also runs its fault-injecting proxy there. Exit codes: 0 all selected scenarios PASS, 1 any
FAIL, 2 setup error (bad tree, missing private functions, server failed to start, a scenario
precondition that did not hold, S and W ids mixed in one run).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections import Counter
from dataclasses import replace
from pathlib import Path

from consumer_lab import __version__
from consumer_lab.server import (
    PINNED_COMMIT,
    SetupError,
    marker_ok,
    missing_upstream_api,
    upstream_commit,
)

EXIT_PASS, EXIT_FAIL, EXIT_SETUP = 0, 1, 2
WORKER_TIMEOUT = 600


def _split_needs(needs: tuple[str, ...]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    names = tuple(n for n in needs if n.isupper() or n.lstrip("_").isupper())
    functions = tuple(n for n in needs if n not in names)
    return functions, names


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m consumer_lab", description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="command", required=True)
    for name in ("run", "_worker"):
        s = sub.add_parser(name, help="run reader or writer fault scenarios" if name == "run" else argparse.SUPPRESS)
        s.add_argument("--upstream", required=True, help="upstream technocore-chat working tree")
        s.add_argument("--client", required=True, help="client command, run once per cycle")
        s.add_argument(
            "--scenario",
            default="all",
            help="reader ids S1..S5, writer ids W0..W7, a comma list of either family, "
            "all (S1..S5) or writers (W0..W7); default: all",
        )
        s.add_argument("--keep-root", action="store_true", help="keep each scenario's temp dir")
        if name == "run":
            s.add_argument("--json", dest="json_out", help="write a JSON report here")
        else:
            s.add_argument("--workdir", help=argparse.SUPPRESS)  # the scenario's temp dir, from `_run`
    return p


def _selected(arg: str) -> list[str]:
    from consumer_lab.scenarios import SCENARIOS
    from consumer_lab.writers import WRITER_SCENARIOS

    if arg == "all":
        return list(SCENARIOS)
    if arg == "writers":
        return list(WRITER_SCENARIOS)
    wanted = [s.strip().upper() for s in arg.split(",") if s.strip()]
    unknown = [s for s in wanted if s not in SCENARIOS and s not in WRITER_SCENARIOS]
    if unknown or not wanted:
        raise SetupError(
            f"unknown scenario {', '.join(unknown) or arg!r}; choose S1..S5, W0..W7, all or writers"
        )
    if any(s in SCENARIOS for s in wanted) and any(s in WRITER_SCENARIOS for s in wanted):
        raise SetupError(
            "reader (S) and writer (W) scenarios cannot be mixed in one run: their client "
            "contracts differ; run them separately"
        )
    return wanted


def _new_workdir(sid: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=f"consumer-lab-{sid.lower()}-"))


def _workdir(args, sid: str) -> Path:
    """The dir `_run` created for this scenario; a worker started by hand makes its own."""
    return Path(args.workdir) if args.workdir else _new_workdir(sid)


def _worker(args) -> int:
    """Run one scenario in this process; print one JSON line with the result."""
    from consumer_lab.judge import judge
    from consumer_lab.scenarios import SCENARIOS, Kit
    from consumer_lab.server import Upstream
    from consumer_lab.writers import WRITER_SCENARIOS

    if args.scenario in WRITER_SCENARIOS:
        return _writer_worker(args, WRITER_SCENARIOS[args.scenario])
    scenario = SCENARIOS[args.scenario]
    functions, names = _split_needs(scenario.needs)
    missing = missing_upstream_api(args.upstream, functions, names)
    if missing:
        raise SetupError(f"{scenario.id} needs upstream {', '.join(missing)}, missing from store.py")

    workdir = _workdir(args, scenario.id)
    kit = None
    # How this worker ended, written to <workdir>/result.json when the dir is kept, so a kept
    # root always says whether it holds a verdict, a setup error or a crash of the kit.
    outcome: dict = {"id": scenario.id, "exit_code": 1, "error": "worker stopped before a verdict"}
    try:
        if str(workdir.resolve()).startswith("/mnt/"):
            raise SetupError(f"temp dir {workdir} is on /mnt/; set TMPDIR to a Linux path")
        root = workdir / "root"
        root.mkdir()
        up = Upstream(args.upstream, root)
        try:
            kit = Kit(up, args.client, workdir, scenario.room)
            truth = scenario.run(kit)
        finally:
            up.stop()
        cycles = list(kit.cycles)
        if cycles and not kit.log_intact():  # changed after the last cycle ended
            cycles[-1] = replace(cycles[-1], log_intact=False)
        lines = kit.events_path.read_text(encoding="utf-8", errors="replace").splitlines()
        verdict = judge(scenario.id, lines, truth, cycles)
        kinds: Counter[str] = Counter()
        for line in lines:
            try:
                kinds[str(json.loads(line).get("event"))] += 1
            except (ValueError, RecursionError, AttributeError):
                kinds["malformed"] += 1
        result = {
            "id": scenario.id,
            "name": scenario.name,
            "verdict": verdict.verdict,
            "reason": verdict.reason,
            "cycles": len(kit.cycles),
            "cycle_exit_codes": [c.exit_code for c in kit.cycles],
            "events": {
                "deliver": kinds.get("deliver", 0),
                "loss": kinds.get("loss", 0),
                "other": sum(v for k, v in kinds.items() if k not in ("deliver", "loss")),
                "total": len(lines),
            },
        }
        print(json.dumps(result, sort_keys=True))
        outcome = {**result, "exit_code": EXIT_PASS}
        return EXIT_PASS
    except SetupError as exc:
        outcome = {"id": scenario.id, "exit_code": EXIT_SETUP, "setup_error": str(exc)}
        raise
    except BaseException as exc:
        outcome = {"id": scenario.id, "exit_code": 1, "error": repr(exc)}
        raise
    finally:
        if args.keep_root:
            if kit is not None:
                outcome["cycle_exit_codes"] = [c.exit_code for c in kit.cycles]
            try:
                text = json.dumps(outcome, sort_keys=True, indent=2)
                (workdir / "result.json").write_text(text + "\n", encoding="utf-8")
            except OSError:
                pass
            print(f"kept {workdir}", file=sys.stderr)
        else:
            shutil.rmtree(workdir, ignore_errors=True)


def _writer_worker(args, scenario) -> int:
    """The writer twin of `_worker`: one scenario, one client cycle through the proxy."""
    from consumer_lab.server import Upstream
    from consumer_lab.writer_judge import judge_write
    from consumer_lab.writers import run_writer

    functions, names = _split_needs(scenario.needs)
    missing = missing_upstream_api(args.upstream, functions, names)
    if missing:
        raise SetupError(f"{scenario.id} needs upstream {', '.join(missing)}, missing from store.py")

    workdir = _workdir(args, scenario.id)
    run = None
    outcome: dict = {"id": scenario.id, "exit_code": 1, "error": "worker stopped before a verdict"}
    try:
        if str(workdir.resolve()).startswith("/mnt/"):
            raise SetupError(f"temp dir {workdir} is on /mnt/; set TMPDIR to a Linux path")
        root = workdir / "root"
        root.mkdir()
        up = Upstream(args.upstream, root)
        try:
            run = run_writer(scenario, up, args.client, workdir)
        finally:
            up.stop()
        verdict = judge_write(scenario.id, run.lines, run.truth, run.cycle)
        kinds: Counter[str] = Counter()
        for line in run.lines:
            if not line.strip():
                continue
            try:
                kinds[str(json.loads(line).get("event"))] += 1
            except (ValueError, RecursionError, AttributeError):
                kinds["malformed"] += 1
        # Never the write text: only counts, the claimed outcome and the proxy's log as
        # `writers.report_log` reduced it (no client-chosen path or method).
        result = {
            "id": scenario.id,
            "name": scenario.name,
            "verdict": verdict.verdict,
            "reason": verdict.reason,
            "cycles": 1,
            "cycle_exit_codes": [run.cycle.exit_code],
            "outcome": run.outcome,
            "stored": run.truth.stored,
            "other_records": run.truth.other_records,
            "proxy_log": run.truth.proxy_log,
            "events": {
                "write": kinds.get("write", 0),
                "other": sum(v for k, v in kinds.items() if k != "write"),
                "total": sum(kinds.values()),
            },
        }
        print(json.dumps(result, sort_keys=True))
        outcome = {**result, "exit_code": EXIT_PASS}
        return EXIT_PASS
    except SetupError as exc:
        outcome = {"id": scenario.id, "exit_code": EXIT_SETUP, "setup_error": str(exc)}
        raise
    except BaseException as exc:
        outcome = {"id": scenario.id, "exit_code": 1, "error": repr(exc)}
        raise
    finally:
        if args.keep_root:
            if run is not None:
                outcome["cycle_exit_codes"] = [run.cycle.exit_code]
            try:
                text = json.dumps(outcome, sort_keys=True, indent=2)
                (workdir / "result.json").write_text(text + "\n", encoding="utf-8")
            except OSError:
                pass
            print(f"kept {workdir}", file=sys.stderr)
        else:
            shutil.rmtree(workdir, ignore_errors=True)


def _worker_failure(returncode: int, stderr: str) -> str:
    """Kit-written text for a failed worker, never the client's: the client shares the
    worker's stderr (it can write to /proc/<ppid>/fd/2). On EXIT_SETUP the worker's own last
    `setup error: ` line, which `main` prints after the client's process group was killed, so
    a line the client forged earlier is not the last; otherwise fixed text."""
    if returncode == EXIT_SETUP:
        lines = [ln for ln in stderr.splitlines() if ln.startswith("setup error: ")]
        return lines[-1].removeprefix("setup error: ") if lines else "no detail"
    if returncode < 0:
        return f"killed by signal {-returncode}"
    return "kit error, see stderr"


def _run(args) -> int:
    tree = Path(args.upstream).expanduser().resolve()
    ok, reason = marker_ok(tree)
    if not ok:
        raise SetupError(reason)
    missing = missing_upstream_api(tree)
    if missing:
        raise SetupError("pinned upstream API missing from store.py: " + ", ".join(missing))
    selected = _selected(args.scenario)
    if args.json_out:
        out_dir = Path(args.json_out).expanduser().resolve().parent
        if not out_dir.is_dir():
            raise SetupError(f"--json: directory {out_dir} does not exist")

    pkg_parent = str(Path(__file__).resolve().parent.parent)
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    env["PYTHONPATH"] = pkg_parent + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")

    results = {}
    setup_error = None
    for sid in selected:
        workdir = _new_workdir(sid)
        cmd = [sys.executable, "-m", "consumer_lab", "_worker", "--upstream", str(tree),
               "--client", args.client, "--scenario", sid, "--workdir", str(workdir)]
        if args.keep_root:
            cmd.append("--keep-root")
        try:
            proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=WORKER_TIMEOUT)
        except subprocess.TimeoutExpired:
            setup_error = f"{sid}: worker did not finish within {WORKER_TIMEOUT}s"
            if args.keep_root:
                result_json = {"id": sid, "exit_code": EXIT_SETUP, "setup_error": setup_error}
                try:
                    text = json.dumps(result_json, sort_keys=True, indent=2)
                    (workdir / "result.json").write_text(text + "\n", encoding="utf-8")
                except OSError:
                    pass
                print(f"kept {workdir}", file=sys.stderr)
            else:
                shutil.rmtree(workdir, ignore_errors=True)
            break
        if not args.keep_root:
            shutil.rmtree(workdir, ignore_errors=True)  # the worker removes it; this is the backstop
        if proc.stderr.strip():
            sys.stderr.write(proc.stderr)
        if proc.returncode != EXIT_PASS:
            detail = _worker_failure(proc.returncode, proc.stderr)
            setup_error = f"{sid}: worker exited {proc.returncode}: {detail}"
            break
        result = json.loads(proc.stdout.strip().splitlines()[-1])
        results[sid] = result
        print(f"{sid} {result['name']:<26} {result['verdict']:<22} {result['reason']}")

    report = {
        "kit_version": __version__,
        "pinned_commit": PINNED_COMMIT,
        "upstream_commit": upstream_commit(tree),
        "client": args.client,
        "scenarios": results,
        "selected": selected,
        "setup_error": setup_error,
        "all_pass": setup_error is None and all(r["verdict"] == "PASS" for r in results.values()),
    }
    if args.json_out:
        try:
            Path(args.json_out).expanduser().write_text(
                json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8"
            )
        except OSError as exc:
            raise SetupError(f"--json: cannot write {args.json_out}: {exc.strerror}") from exc
    if setup_error:
        print(f"setup error: {setup_error}", file=sys.stderr)
        return EXIT_SETUP
    passed = sum(r["verdict"] == "PASS" for r in results.values())
    print(f"{passed}/{len(results)} PASS (upstream {report['upstream_commit']})")
    return EXIT_PASS if report["all_pass"] else EXIT_FAIL


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "_worker":
            return _worker(args)
        return _run(args)
    except SetupError as exc:
        if args.command == "_worker":
            # The client shares this stderr: start a fresh line, so that a partial line it left
            # cannot become the start of the line `_worker_failure` reads.
            sys.stderr.write("\n")
        print(f"setup error: {exc}", file=sys.stderr)
        return EXIT_SETUP
