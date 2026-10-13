"""Reference writer 1: "retry on error, trust a 2xx". The common writer, as usually written.

    for attempt in 1..3:
        try: urlopen(POST /r/{room}?format=json, timeout); return "confirmed"
        except Exception: continue
    return "not_written"

urllib's default redirect handling follows a POST 303 as a GET and raises on a POST 307.
Any exception, including `http.client.IncompleteRead` and `HTTPError`, is retried with the
same POST. This client is expected to FAIL W2, W3, W4, W6 and W7: that is the point of
shipping it.
"""

import json
import os
import urllib.request

BASE = os.environ["TECHNOCORE_URL"]
ROOM = os.environ["TC_ROOM"]
EVENTS = os.environ["TC_EVENTS"]
WRITE_ID = os.environ["TC_WRITE_ID"]
FROM = os.environ["TC_WRITE_FROM"]
TEXT = os.environ["TC_WRITE_TEXT"]
TIMEOUT = float(os.environ["TC_WRITE_TIMEOUT"])
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
ATTEMPTS = 3  # the first try plus up to 2 retries


def post_once():
    req = urllib.request.Request(
        f"{BASE}/r/{ROOM}?format=json",
        data=json.dumps({"from": FROM, "text": TEXT}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with OPENER.open(req, timeout=TIMEOUT) as resp:
        resp.read()
        return resp.status


def main():
    outcome, detail = "not_written", ""
    attempt = 0
    for attempt in range(1, ATTEMPTS + 1):
        try:
            status = post_once()
        except Exception as exc:  # noqa: BLE001 - this client retries on anything
            detail = f"attempt {attempt}: {type(exc).__name__}"
            continue
        if 200 <= status < 300:
            outcome, detail = "confirmed", f"attempt {attempt}: {status}"
            break
    with open(EVENTS, "a", encoding="utf-8") as events:
        events.write(json.dumps({"event": "write", "id": WRITE_ID, "outcome": outcome,
                                 "detail": detail, "attempts": attempt}) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
