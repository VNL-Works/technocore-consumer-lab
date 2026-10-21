#!/usr/bin/env bash
# Create the consumer-lab venv and install upstream technocore-chat's locked runtime deps.
#
#   bash scripts/setup_wsl.sh <upstream-tree> [<upstream-commit>]
#
# <upstream-tree> is a plain working tree of technocore-chat (no .git needed), e.g. produced
# with `git archive <commit> | tar -x -C <dir>`. Keep it on the Linux filesystem, not /mnt/.
#
# Network: this is the only step that touches the network. It installs `uv` from PyPI and
# then exactly the packages pinned (with hashes) in <upstream-tree>/uv.lock. No sudo, nothing
# system-wide: everything lands in $CONSUMER_LAB_VENV (default ~/consumer-lab-venv).
#
# If the tree matches the pinned commit's fingerprint (or <upstream-commit> is given), the
# commit is recorded in <upstream-tree>/.consumer-lab-upstream for the JSON report.
set -euo pipefail

PINNED_COMMIT="4a013e5401f15e56762456d1d48c4e676e2cff2c"
# sha256 of three files at the pinned commit: the module the scenarios patch, the lock the
# deps come from, and src/humans.html. A git-less tree cannot prove its commit; matching all
# three is the fingerprint. humans.html is there because store.py and uv.lock are identical at
# 0e47f77 and 4a013e5: without it a 0e47f77 tree would be recorded as the new pin.
PINNED_STORE_SHA256="0eee2537081c44d34d34b63feeed89a235853c82764168c364ca14655e437249"
PINNED_LOCK_SHA256="407cf73389d48a075a2ad6152e63188d8adb0e0281cd769e5bbc159b681dc8a1"
PINNED_HUMANS_SHA256="b29b2d0e77fc8db975b4ca6389bdc1cdece816abe2084d6c9048f4e639e83025"

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
    echo "usage: bash scripts/setup_wsl.sh <upstream-tree> [<upstream-commit>]" >&2
    exit 2
fi
TREE="$(cd "$1" && pwd)"
VENV="${CONSUMER_LAB_VENV:-$HOME/consumer-lab-venv}"

case "$TREE" in
    /mnt/*) echo "error: keep the upstream tree on the Linux filesystem, not $TREE" >&2; exit 2 ;;
esac
for f in src/store.py src/app.py uv.lock pyproject.toml; do
    if [ ! -f "$TREE/$f" ]; then
        echo "error: $TREE/$f not found: not a technocore-chat tree" >&2
        exit 2
    fi
done

if [ ! -x "$VENV/bin/python" ]; then
    python3 -m venv "$VENV"
fi
PY="$VENV/bin/python"

# Caches inside the venv, so nothing is written to ~/.cache.
export UV_CACHE_DIR="$VENV/.uv-cache"
export UV_PYTHON_DOWNLOADS=never
export UV_NO_CONFIG=1
export PIP_DISABLE_PIP_VERSION_CHECK=1

"$PY" -m pip install --no-cache-dir --quiet "uv>=0.9,<1"
UV="$VENV/bin/uv"

REQ="$(mktemp -d)/requirements.txt"
trap 'rm -rf "$(dirname "$REQ")"' EXIT
# --frozen: read uv.lock as is, never re-resolve. --no-dev: runtime deps only.
# --no-emit-project: upstream is run from its tree via sys.path, not installed.
(cd "$TREE" && "$UV" export --frozen --no-dev --no-emit-project --format requirements-txt \
    --python "$PY" --output-file "$REQ" --quiet)
"$UV" pip install --python "$PY" --require-hashes --quiet -r "$REQ"

MARKER="$TREE/.consumer-lab-upstream"
COMMIT="${2:-}"
if [ -z "$COMMIT" ]; then
    store_sha="$(sha256sum "$TREE/src/store.py" | cut -d' ' -f1)"
    lock_sha="$(sha256sum "$TREE/uv.lock" | cut -d' ' -f1)"
    humans_sha=""
    if [ -f "$TREE/src/humans.html" ]; then
        humans_sha="$(sha256sum "$TREE/src/humans.html" | cut -d' ' -f1)"
    fi
    if [ "$store_sha" = "$PINNED_STORE_SHA256" ] && [ "$lock_sha" = "$PINNED_LOCK_SHA256" ] \
        && [ "$humans_sha" = "$PINNED_HUMANS_SHA256" ]; then
        COMMIT="$PINNED_COMMIT"
    fi
fi
if [ -n "$COMMIT" ]; then
    printf '%s\n' "$COMMIT" > "$MARKER"
    echo "upstream commit: $COMMIT (recorded in $MARKER)"
else
    rm -f "$MARKER"
    echo "upstream commit: unknown (tree does not match the pinned fingerprint)"
fi

"$PY" --version
"$UV" --version
"$PY" - <<'EOF'
from importlib.metadata import version
for name in ("starlette", "uvicorn", "orjson"):
    print(f"{name} {version(name)}")
EOF
