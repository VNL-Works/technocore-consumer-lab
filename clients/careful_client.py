"""Reference client 2: the bridge loop from upstream PR #545's `src/interop.md`, extended.

Keeps a checkpoint {generation, seq} in TC_STATE_DIR and, on every poll:
  - a generation change since the checkpoint = the room was recreated: report the loss,
    then resync from the new generation's export;
  - an empty page whose `last_seq` is above the cursor = records were dropped unread
    (expired or lost): report the loss and move the cursor (the #545 + #800/#910 case);
  - `first_seq > since + 1` = the window was cut: download `/export`, check its
    `X-Room-Generation`, and if the retained ring does not start at since + 1, report the loss;
  - the records about to be delivered must be contiguous, or the client stops.
Unlike #545's loop, which raises on a gap, this one reports the loss as an event and then
delivers what is still retained, so a kit cycle can see both.
"""

import json
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

BASE = os.environ["TECHNOCORE_URL"]
ROOM = os.environ["TC_ROOM"]
STATE = Path(os.environ["TC_STATE_DIR"]) / "checkpoint.json"
EVENTS_PATH = os.environ["TC_EVENTS"]
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
LIMIT = 200  # upstream MAX_LIMIT
MAX_POLLS = 1000


def http_get(path, params=None):
    url = f"{BASE}{path}" + (f"?{urllib.parse.urlencode(params)}" if params else "")
    with OPENER.open(url, timeout=20) as resp:
        return resp.headers, resp.read()


def read_view(since):
    _, body = http_get(f"/r/{ROOM}", {"since": since, "limit": LIMIT, "format": "json"})
    return json.loads(body)


def export():
    headers, body = http_get(f"/r/{ROOM}/export")
    records = [json.loads(line) for line in body.splitlines() if line.strip()]
    return int(headers["X-Room-Generation"]), records


def load_checkpoint():
    try:
        ck = json.loads(STATE.read_text())
        return {"generation": int(ck["generation"]), "seq": int(ck["seq"])}
    except FileNotFoundError:
        return None


def save_checkpoint(ck):
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(ck))
    os.replace(tmp, STATE)


class Events:
    def __init__(self, path):
        self.f = open(path, "a", encoding="utf-8")

    def emit(self, **ev):
        self.f.write(json.dumps(ev) + "\n")
        self.f.flush()

    def deliver(self, seq, generation):
        self.emit(event="deliver", seq=seq, generation=generation)

    def loss(self, after_seq, reason):
        self.emit(event="loss", after_seq=after_seq, reason=reason)


def contiguous(records):
    return all(b["seq"] == a["seq"] + 1 for a, b in zip(records, records[1:]))


def main():
    events = Events(EVENTS_PATH)
    ck = load_checkpoint()
    for _ in range(MAX_POLLS):
        since = ck["seq"] if ck else 0
        view = read_view(since)
        gen = view["generation"]

        if ck and gen != ck["generation"]:
            events.loss(since, f"room recreated: generation {ck['generation']} -> {gen}")
            xgen, records = export()
            if xgen != gen:
                continue  # changed again between the poll and the snapshot: look again
            if not contiguous(records):
                events.loss(since, "non-contiguous export after recreate")
                return 3
            for r in records:
                events.deliver(r["seq"], gen)
            ck = {"generation": gen, "seq": records[-1]["seq"] if records else view["last_seq"]}
            save_checkpoint(ck)
            continue

        messages = view["messages"]
        if not messages:
            if ck and view["last_seq"] > since:
                # The server says the room moved past our cursor but returned nothing to read.
                events.loss(since, f"empty window with last_seq {view['last_seq']} > since {since}")
                ck = {"generation": gen, "seq": view["last_seq"]}
                save_checkpoint(ck)
                continue
            if ck is None:
                save_checkpoint({"generation": gen, "seq": since})
            return 0  # caught up

        if view["first_seq"] > since + 1:
            # The read is the newest window; recover from the retained ring.
            xgen, records = export()
            if xgen != gen:
                continue
            retained = [r for r in records if r["seq"] > since]
            if ck and (not retained or retained[0]["seq"] != since + 1):
                events.loss(since, f"retention gap: records after seq {since} are no longer retained")
            messages = retained
            if not messages:
                continue

        if not contiguous(messages):
            events.loss(since, "non-contiguous records in one read")
            return 3
        for m in messages:
            events.deliver(m["seq"], gen)
        ck = {"generation": gen, "seq": messages[-1]["seq"]}
        save_checkpoint(ck)
    print("careful_client: too many polls without catching up", file=sys.stderr)
    return 4


if __name__ == "__main__":
    raise SystemExit(main())
