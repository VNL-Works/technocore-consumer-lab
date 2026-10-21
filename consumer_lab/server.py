"""Start and stop the real upstream technocore-chat app in-process, on loopback.

The kit and the server share one process on purpose: patching `store.EPHEMERAL_TTL_SECONDS`
or calling `store._compact` / `store._reap_pass` from a scenario changes exactly the state
the HTTP handlers read. One `Upstream` per process: upstream's modules read their env knobs
once, at import, so the CLI runs each scenario in a fresh worker process.
"""

from __future__ import annotations

import ast
import importlib
import logging
import os
import re
import sys
import threading
import time
from pathlib import Path

PINNED_COMMIT = "4a013e5401f15e56762456d1d48c4e676e2cff2c"
MARKER_FILE = ".consumer-lab-upstream"

# Everything the scenarios touch in upstream's src/store.py. Private names are not a
# contract upstream keeps; the pin is what makes them safe to use.
REQUIRED_FUNCTIONS = (
    "read_messages",
    "export_room",
    "room_generation",
    "room_path",
    "_cutoff",
    "_compact",
    "_reap_pass",
)
REQUIRED_NAMES = ("IDLE_SECONDS", "EPHEMERAL_TTL_SECONDS", "MAX_LIMIT")

ENV = {
    "CHAT_RATE_READ": "1000000",
    "CHAT_RATE_WRITE": "1000000",
    "CHAT_RATE_ROOMS_PER_DAY": "1000000",
    "CHAT_DUPE_FILTER_SECONDS": "0",
    "CHAT_MAX_WAIT": "1",
}


class SetupError(RuntimeError):
    """The tree, the pinned private API or the server is not usable. CLI exit code 2."""


def marker_ok(tree: str | os.PathLike) -> tuple[bool, str]:
    """The git-less check: `<tree>/src/store.py` exists and defines `_compact` and `_reap_pass`."""
    store_py = Path(tree) / "src" / "store.py"
    try:
        source = store_py.read_text(encoding="utf-8")
    except OSError:
        return False, f"{store_py} not found: not a technocore-chat tree"
    for needle in ("def _compact(", "def _reap_pass("):
        if needle not in source:
            return False, f"{store_py} has no `{needle}`: not the pinned upstream"
    return True, "ok"


def _top_level_names(store_py: Path) -> tuple[set[str], set[str]]:
    tree = ast.parse(store_py.read_text(encoding="utf-8"), filename=str(store_py))
    functions: set[str] = set()
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.add(node.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                for sub in ast.walk(target):
                    if isinstance(sub, ast.Name):
                        names.add(sub.id)
    return functions, names


def missing_upstream_api(
    tree: str | os.PathLike,
    functions: tuple[str, ...] = REQUIRED_FUNCTIONS,
    names: tuple[str, ...] = REQUIRED_NAMES,
) -> list[str]:
    """Required store functions/globals absent from `<tree>/src/store.py` (static, no import)."""
    store_py = Path(tree) / "src" / "store.py"
    try:
        have_functions, have_names = _top_level_names(store_py)
    except (OSError, SyntaxError) as exc:
        return [f"<unreadable store.py: {exc.__class__.__name__}>"]
    missing = [f"def {f}" for f in functions if f not in have_functions]
    missing += [n for n in names if n not in have_names]
    return missing


def pinned_upstream_ok(tree: str | os.PathLike) -> bool:
    """True when every private store function and global the scenarios use is present."""
    ok, _ = marker_ok(tree)
    return ok and not missing_upstream_api(tree)


def upstream_commit(tree: str | os.PathLike) -> str:
    """The commit `scripts/setup_wsl.sh` recorded for this tree, or "unknown"."""
    try:
        text = (Path(tree) / MARKER_FILE).read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError):
        return "unknown"
    return text if re.fullmatch(r"[0-9a-f]{40}", text) else "unknown"


class Upstream:
    """The upstream app served by uvicorn on 127.0.0.1 in a background thread.

    `tree` is an upstream working tree; `root` becomes CHAT_ROOT and must be a directory on
    the Linux filesystem (never under /mnt/). Exposes `.url`, `.store`, `.config`, `.stop()`.
    """

    _active: "Upstream | None" = None

    def __init__(self, tree: str | os.PathLike, root: str | os.PathLike, ready_timeout: float = 15.0):
        self.tree = Path(tree).expanduser().resolve()
        self.root = Path(root).resolve()
        ok, reason = marker_ok(self.tree)
        if not ok:
            raise SetupError(reason)
        missing = missing_upstream_api(self.tree)
        if missing:
            raise SetupError("pinned upstream API missing from store.py: " + ", ".join(missing))
        if str(self.root).startswith("/mnt/"):
            raise SetupError(f"CHAT_ROOT {self.root} is on /mnt/: fcntl locks there are unreliable")
        if not self.root.is_dir():
            raise SetupError(f"CHAT_ROOT {self.root} is not a directory")
        if Upstream._active is not None or "store" in sys.modules:
            raise SetupError("one Upstream per process: upstream reads its env once, at import")

        os.environ["CHAT_ROOT"] = str(self.root)
        os.environ.update(ENV)
        src = str(self.tree / "src")
        sys.path.insert(0, src)
        try:
            self.config = importlib.import_module("config")
            self.store = importlib.import_module("store")
            self.app_module = importlib.import_module("app")
        except Exception as exc:  # an import failure is a setup failure, whatever it is
            raise SetupError(f"importing upstream from {src} failed: {exc!r}") from exc
        if Path(self.store.__file__).resolve().parent != Path(src).resolve():
            raise SetupError(f"`store` imported from {self.store.__file__}, not from {src}")
        if Path(self.config.ROOT).resolve() != self.root:
            raise SetupError(f"upstream config.ROOT is {self.config.ROOT}, expected {self.root}")

        import uvicorn  # upstream's locked dependency, installed by scripts/setup_wsl.sh

        cfg = uvicorn.Config(
            self.app_module.app,
            host="127.0.0.1",
            port=0,
            log_level="warning",
            access_log=False,
            lifespan="on",
        )
        from consumer_lab.scenarios import start_kit_thread  # here: scenarios imports this module

        self._server = uvicorn.Server(cfg)
        self._thread = threading.Thread(target=self._server.run, name="upstream", daemon=True)
        # The deferred signals stay blocked in this thread and in every thread it starts, so a
        # deferred signal sent to the worker process is taken by the main thread, not here.
        start_kit_thread(self._thread)
        deadline = time.monotonic() + ready_timeout
        while not self._server.started:
            if not self._thread.is_alive() or time.monotonic() > deadline:
                self._server.should_exit = True
                raise SetupError("upstream server failed to start")
            time.sleep(0.01)
        sockets = [s for srv in self._server.servers for s in srv.sockets]
        host, port = sockets[0].getsockname()[:2]
        if host != "127.0.0.1":
            self.stop()
            raise SetupError(f"server bound to {host}, not 127.0.0.1")
        self.port = port
        self.url = f"http://127.0.0.1:{port}"
        Upstream._active = self

    def stop(self, shutdown_hooks: bool = True) -> None:
        """Stop the server and wait for its thread. With `shutdown_hooks=False`, uvicorn's
        `force_exit`: no wait for open connections and no lifespan shutdown, so upstream's
        hook (which flushes the store's counters by path) does not run. The lifespan task is
        then cancelled at its `yield`; uvicorn's traceback for that is not logged."""
        if not shutdown_hooks:
            logging.getLogger("uvicorn.error").setLevel(logging.CRITICAL)
            self._server.force_exit = True
        self._server.should_exit = True
        self._thread.join(timeout=10)
        if Upstream._active is self:
            Upstream._active = None

    def __enter__(self) -> "Upstream":
        return self

    def __exit__(self, *exc) -> None:
        self.stop()
