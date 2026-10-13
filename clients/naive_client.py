"""Reference client 1: the bridge loop from upstream `src/interop.md` on main, as written.

    view = get(f"{BASE}/r/{room}", params={"since": since, "wait": 10, "format": "json"}).json()
    for m in view["messages"]: deliver_to_far_side(m)
    since = view["last_seq"]
    save_cursor(room, since)

No `limit` (so the server default window), no `first_seq` check, no generation check, no
export. The only addition is the exit when a poll comes back empty, which is how one kit cycle
ends. This client is expected to FAIL most scenarios: that is the point of shipping it.
"""

import json
import os
import urllib.parse
import urllib.request
from pathlib import Path

BASE = os.environ["TECHNOCORE_URL"]
ROOM = os.environ["TC_ROOM"]
STATE = Path(os.environ["TC_STATE_DIR"]) / "cursor"
EVENTS = os.environ["TC_EVENTS"]
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def get(path, params):
    with OPENER.open(f"{BASE}{path}?{urllib.parse.urlencode(params)}", timeout=20) as resp:
        return json.loads(resp.read())


def main():
    since = int(STATE.read_text()) if STATE.exists() else 0
    with open(EVENTS, "a", encoding="utf-8") as events:
        while True:
            view = get(f"/r/{ROOM}", {"since": since, "wait": 10, "format": "json"})
            for m in view["messages"]:
                events.write(json.dumps({"event": "deliver", "seq": m["seq"], "generation": view["generation"]}) + "\n")
            events.flush()
            since = view["last_seq"]
            STATE.write_text(str(since))
            if not view["messages"]:
                return 0


if __name__ == "__main__":
    raise SystemExit(main())
