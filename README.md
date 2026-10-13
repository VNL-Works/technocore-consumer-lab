# technocore-consumer-lab

A local test kit for people who write **readers** of [technocore-chat](https://github.com/flop-labs/technocore-chat)
rooms: bridges, archivers, scorers, mirrors. It runs the real upstream service on loopback,
drives a room through 5 reader fault scenarios, and judges whether your client delivered every
retained record exactly once and in order, and whether it reported every loss it could detect.

Why it exists: upstream issue [#481](https://github.com/flop-labs/technocore-chat/issues/481)
collects independent consumers that lost records silently (a scorer, archivers, a reference
client in #919), all with the same few mistakes. None of those cases could be reproduced locally.

## What a PASS means, and what it does not

A PASS means: **your client behaved correctly against the pinned upstream commit under these 5
controlled cases.** Nothing more. It does not mean your client is correct against the live
service, under concurrent writers, under compaction racing a read, against writer-side faults,
or against any future upstream change. A FAIL, on the other hand, is a concrete reproducible
case with a one-line reason.

The kit ships two reference clients and checks itself with them on every test run:

| scenario | `clients/naive_client.py` | `clients/careful_client.py` |
|---|---|---|
| S1 lag-within-ring | FAIL:missed_retained | PASS |
| S2 lag-beyond-retention | FAIL:unreported_loss | PASS |
| S3 ephemeral-all-expired | FAIL:unreported_loss | PASS |
| S4 ephemeral-prefix-expired | FAIL:unreported_loss | PASS |
| S5 room-recreated | FAIL:unreported_loss | PASS |

The naive client is the bridge loop in upstream `src/interop.md` on main (`since =
view["last_seq"]`, no gap check). The careful one follows PR #545's loop (checkpoint
`{generation, seq}`, `/export` recovery, gap and recreate detection, contiguity check), and
reports losses as events instead of raising. If the naive client ever passed S2, S3 or S5,
that scenario would prove nothing; `tests/test_lab.py` fails in that case.

## The pin, and the private functions it relies on

The scenarios are built from HTTP writes plus a few **private** functions of upstream's
`src/store.py`, called in the same process as the server:

- `_compact(path, cutoff=None, keep=...)`: S2 drops the oldest records from a room's ring;
- `_cutoff(room)`, which reads the module global `EPHEMERAL_TTL_SECONDS` on every call: S3
  and S4 expire `e-` records;
- `_reap_pass(root, now)` with `IDLE_SECONDS`: S5 reaps a room so it can be recreated;
- `read_messages`, `export_room`, `room_generation`, `room_path`: the kit's own truth.

Private names are not an upstream contract. The kit is pinned to upstream commit
`0e47f770b13cc27e1e2e199d4cdf70a4778c97cc`; at any other commit a scenario may stop meaning
what `docs/SCENARIOS.md` says. The kit refuses to start (exit 2) when `src/store.py` lacks
`def _compact(` or `def _reap_pass(`, or when any function a scenario needs is missing.
It does not vendor upstream: it runs an upstream tree you point it at.

## S3 and S4 are a policy choice

S3 and S4 require a client to **report** that expired `e-` records were never read. Upstream
treats expiry as the room's promise, not as a fault, so a client that skips expired records
silently is arguably within the service's contract. The kit takes the stricter position on
purpose: a consumer that mirrors or scores a room should be able to say "I missed 4..6", and
the owner's 2026-10-13 finding on PR #545 shows why it matters. Composed with the
empty-window `last_seq` change of #800/#910, #545's loop advances its checkpoint past expired
`e-` records without noticing. If your use case accepts silent expiry, read S3/S4 failures
in that light.

## Setup (WSL Ubuntu or Linux, Python 3.12)

```bash
# 1. An upstream working tree at the pinned commit, on the Linux filesystem (not /mnt/):
mkdir -p ~/consumer-lab-upstream-0e47f77
git -C <technocore-chat clone> archive 0e47f770b13cc27e1e2e199d4cdf70a4778c97cc \
  | tar -x -C ~/consumer-lab-upstream-0e47f77

# 2. The venv (~/consumer-lab-venv) with upstream's locked runtime deps. Network: PyPI only.
bash scripts/setup_wsl.sh ~/consumer-lab-upstream-0e47f77
```

`setup_wsl.sh` uses `python3 -m venv`, installs `uv` from PyPI, installs exactly what
upstream's `uv.lock` pins (`uv export --frozen --no-dev`, hashes required), and prints the
versions. No sudo; nothing system-wide. When the tree matches the pinned commit's fingerprint
it records the commit in `<tree>/.consumer-lab-upstream`, which the JSON report quotes.

Keep the tree and temp roots on the Linux filesystem: `fcntl` locks over drvfs (`/mnt/*`)
are unreliable, and the kit refuses a `CHAT_ROOT` under `/mnt/`.

## Usage

```bash
~/consumer-lab-venv/bin/python -B -m consumer_lab run \
  --upstream ~/consumer-lab-upstream-0e47f77 \
  --client "./my-reader --flag" \
  [--scenario S1..S5|all] [--json report.json] [--keep-root]
```

One line per scenario with its verdict. Exit code 0 when every selected scenario passes, 1 when
any fails, 2 on a setup error (bad tree, missing private function, server failed to start, a
scenario precondition that did not hold). The JSON report has sorted keys, the upstream commit
(or `"unknown"`), each verdict and its reason, and event counts. It holds no record texts.
Kit self-tests: `~/consumer-lab-venv/bin/python -B -m unittest discover -s tests -v`.

## The client contract

The kit runs your command once per **cycle**, several cycles per scenario, with:

| env var | meaning |
|---|---|
| `TECHNOCORE_URL` | base URL of the loopback server, e.g. `http://127.0.0.1:41234` |
| `TC_ROOM` | the room to read |
| `TC_STATE_DIR` | a directory that persists across the cycles of one scenario: keep your cursor here |
| `TC_EVENTS` | an append-only JSONL file: write your events here |

On each cycle, read until you are caught up, append events, and exit 0. Events, one JSON
object per line:

```json
{"event": "deliver", "seq": 12, "generation": 1}
{"event": "loss", "after_seq": 10, "reason": "retention gap after seq 10"}
```

- `deliver` once per record you hand downstream, in the order you hand it, with the room
  generation you believe it belongs to.
- `loss` when you know records after `after_seq` are gone. It must come **before** your
  first delivery past the gap. For a recreated room the reason must contain `generation` or
  `recreated`.
- A cycle has 30 s. A non-zero exit is acceptable only if that cycle wrote a `loss` event.
- Any language. The command is split like a shell would (`~` is expanded), not run by a shell.
- Append only: rewriting lines of an earlier cycle is `FAIL:bad_events` (within one cycle the
  kit cannot see reordering), and so is a loss written before its gap became observable.
- Trust: the client runs as your user, without the server's `CHAT_*` env, but `TC_STATE_DIR`
  sits next to the live store (`../root`). It could read or edit the store; it is trusted not to.

Verdicts: `PASS`, `FAIL:duplicate`, `FAIL:out_of_order`, `FAIL:missed_retained` (a retained
record never delivered), `FAIL:unreported_loss`, `FAIL:delivered_lost` (a record that was not
retained, or a record under the wrong generation), `FAIL:timeout`, `FAIL:crash` (non-zero exit
without a loss event), `FAIL:bad_events` (malformed JSONL, or a loss reported where nothing was
lost, or before it could be seen, or a rewritten log). See `docs/SCENARIOS.md`.

## How it runs

Each scenario runs in a fresh worker process: a `tempfile.mkdtemp()` root becomes `CHAT_ROOT`,
the upstream app is imported from `<tree>/src` and served by uvicorn on `127.0.0.1` (a free
port) in a background thread, with `CHAT_RATE_READ`, `CHAT_RATE_WRITE` and
`CHAT_RATE_ROOMS_PER_DAY` at 1000000, `CHAT_DUPE_FILTER_SECONDS=0` and `CHAT_MAX_WAIT=1`.
Because the kit and the server share that process, the private store calls above change
exactly what the HTTP handlers serve; that is intended. The temp dir (root, `TC_STATE_DIR`,
events) is deleted afterwards unless you pass `--keep-root`, which also leaves a `result.json`
saying how the run ended. The kit makes no outbound connection other than to its own loopback
server. It writes nothing outside the temp dir and the output paths you name, except Python's
own `consumer_lab/__pycache__` (gitignored); run with `python -B` to avoid even that.

Out of scope: writer scenarios, a fault-injecting proxy, a compaction-during-read race, MCP clients, Windows and Docker.
License: Apache-2.0, copyright VNL Works; technocore-chat is a separate Apache-2.0 project. Built with AI assistance (Claude), and reviewed by a separate AI reviewer that did not write the code.
