"""Reference writer 2: never re-POST an ambiguous write; read back once and say what you know.

    POST /r/{room}?format=json, follow no redirect, timeout TC_WRITE_TIMEOUT
      2xx, full JSON, posted.text == sent  -> confirmed
      4xx other than 408 / 429             -> not_written (the server refused it)
      anything else (3xx, 408, 429, 5xx, timeout / connection error, a cut-off body,
      bad JSON, a 2xx that does not echo the text) is ambiguous:
        GET /r/{room}?format=json&limit=200 once:
          the text is there               -> confirmed
          absent, or the read failed      -> unconfirmed (never re-POST, never guess)

This is the rule PR #944 words as "this write was not confirmed; check whether it landed
before retrying", applied to every ambiguous answer, including the cut-off body of PR #667
and the timeout of issue #982. Stdlib only; `http.client`, so no redirect is ever followed.
"""

import http.client
import json
import os
import urllib.parse

BASE = urllib.parse.urlsplit(os.environ["TECHNOCORE_URL"])
ROOM = os.environ["TC_ROOM"]
EVENTS = os.environ["TC_EVENTS"]
WRITE_ID = os.environ["TC_WRITE_ID"]
FROM = os.environ["TC_WRITE_FROM"]
TEXT = os.environ["TC_WRITE_TEXT"]
TIMEOUT = float(os.environ["TC_WRITE_TIMEOUT"])
READ_LIMIT = 200  # upstream MAX_LIMIT: the newest 200 records


def request(method, path, body=None):
    """(status, body bytes). Raises OSError / HTTPException on any transport failure."""
    conn = http.client.HTTPConnection(BASE.hostname, BASE.port, timeout=TIMEOUT)
    try:
        headers = {"Content-Type": "application/json"} if body is not None else {}
        conn.request(method, path, body=body, headers=headers)
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def try_post():
    """('confirmed' | 'not_written' | None for ambiguous, what happened)."""
    body = json.dumps({"from": FROM, "text": TEXT}).encode()
    try:
        status, data = request("POST", f"/r/{urllib.parse.quote(ROOM)}?format=json", body)
    except http.client.IncompleteRead:
        return None, "cut-off body after POST"
    except (OSError, http.client.HTTPException) as exc:  # timeouts are OSError
        return None, f"{type(exc).__name__} after POST"
    if 200 <= status < 300:
        try:
            posted = json.loads(data)["posted"]
        except (ValueError, KeyError, TypeError):
            return None, f"{status} with unreadable JSON after POST"
        if isinstance(posted, dict) and posted.get("text") == TEXT:
            return "confirmed", f"{status}; posted seq {posted.get('seq')}"
        return None, f"{status} that does not echo the text"
    if 400 <= status < 500 and status not in (408, 429):
        return "not_written", f"{status}: refused"
    return None, f"{status} after POST"


def read_back():
    """True if the text is in the room, False if it is not, None if the read failed."""
    query = urllib.parse.urlencode({"format": "json", "limit": READ_LIMIT})
    try:
        status, data = request("GET", f"/r/{urllib.parse.quote(ROOM)}?{query}")
        if status != 200:
            return None, f"read-back failed: {status}"
        messages = json.loads(data)["messages"]
    except (OSError, http.client.HTTPException, ValueError, KeyError, TypeError) as exc:
        return None, f"read-back failed: {type(exc).__name__}"
    found = any(isinstance(m, dict) and m.get("text") == TEXT for m in messages)
    return found, "read-back found it" if found else "read-back did not find it"


def main():
    outcome, detail = try_post()
    if outcome is None:  # ambiguous: never re-POST
        found, how = read_back()
        outcome = "confirmed" if found else "unconfirmed"
        detail = f"{detail}; {how}"
    with open(EVENTS, "a", encoding="utf-8") as events:
        events.write(json.dumps({"event": "write", "id": WRITE_ID, "outcome": outcome,
                                 "detail": detail, "attempts": 1}) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
