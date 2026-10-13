"""The fault-injecting proxy against a stdlib loopback origin. No upstream tree needed."""

import json
import socket
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from consumer_lab import proxy as P  # noqa: E402
from consumer_lab.server import SetupError  # noqa: E402

ROOM = "lab-wx"
REPLY = json.dumps({"posted": {"seq": 1, "text": "x" * 80}}).encode()  # well over 2 bytes


class Origin:
    """A loopback origin that records every request and answers with a fixed JSON body."""

    def __init__(self):
        self.requests: list[tuple[str, str, bytes, dict]] = []
        self.release = threading.Event()  # /slow waits for this
        self.entered = threading.Event()  # set when a /slow request arrived
        origin = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def _answer(self, body):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                origin.requests.append(("POST", self.path, body, dict(self.headers)))
                if self.path.startswith("/slow"):
                    origin.entered.set()
                    origin.release.wait(10)
                self._answer(REPLY)

            def do_GET(self):
                origin.requests.append(("GET", self.path, b"", dict(self.headers)))
                self._answer(b'{"get": "ok"}')

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def posts(self):
        return [r for r in self.requests if r[0] == "POST"]

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()


def raw(proxy, method, path, body=b"", headers=b"", timeout=5.0):
    """Send one request on a fresh socket; return (bytes until EOF, exception or None, seconds)."""
    s = socket.create_connection(("127.0.0.1", proxy.port), timeout=timeout)
    t0 = time.monotonic()
    try:
        head = f"{method} {path} HTTP/1.1\r\nHost: x\r\n".encode() + headers
        if body or method == "POST":
            head += f"Content-Length: {len(body)}\r\nContent-Type: application/json\r\n".encode()
        s.sendall(head + b"\r\n" + body)
        data = b""
        while True:
            try:
                chunk = s.recv(65536)
            except OSError as exc:
                return data, exc, time.monotonic() - t0
            if not chunk:
                return data, None, time.monotonic() - t0
            data += chunk
    finally:
        s.close()


def split(data):
    head, _, body = data.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    hdrs = {k.lower(): v.strip() for k, v in (ln.split(":", 1) for ln in lines[1:])}
    return lines[0], hdrs, body


BODY = json.dumps({"from": "w", "text": "t"}).encode()


class ProxyActions(unittest.TestCase):
    def setUp(self):
        self.origin = Origin()
        self.addCleanup(self.origin.close)

    def start(self, **plan):
        proxy = P.FaultProxy(self.origin.url, P.Plan(room=ROOM, **plan))
        self.addCleanup(proxy.stop)
        return proxy

    def post(self, proxy, query="?format=json"):
        return raw(proxy, "POST", f"/r/{ROOM}{query}", BODY)

    def test_pass(self):
        proxy = self.start()
        data, exc, _ = self.post(proxy)
        self.assertIsNone(exc)
        status, hdrs, body = split(data)
        self.assertEqual(status, "HTTP/1.1 200 OK")
        self.assertEqual(body, REPLY)
        self.assertEqual(hdrs["connection"], "close")
        self.assertEqual(len(self.origin.posts()), 1)
        _, path, sent, origin_headers = self.origin.posts()[0]
        self.assertEqual((path, sent), (f"/r/{ROOM}?format=json", BODY))
        self.assertEqual(origin_headers["Connection"], "close")
        self.assertTrue(proxy.wait_idle(5))
        self.assertEqual(proxy.log, [{"method": "POST", "path_without_query": f"/r/{ROOM}",
                                      "action": "pass", "forwarded": True, "origin_status": 200}])

    def _redirect(self, code, reason, action, forwarded):
        proxy = self.start(first_post=action)
        data, exc, _ = self.post(proxy)
        self.assertIsNone(exc)
        self.assertEqual(
            data,
            f"HTTP/1.1 {code} {reason}\r\nLocation: /r/{ROOM}?format=json\r\n"
            "Content-Length: 0\r\nConnection: close\r\n\r\n".encode(),
        )
        self.assertTrue(proxy.wait_idle(5))
        self.assertEqual(len(self.origin.posts()), 1 if forwarded else 0)
        self.assertEqual(proxy.log[0]["action"], action.label)
        self.assertEqual(proxy.log[0]["forwarded"], forwarded)
        self.assertEqual(proxy.log[0]["origin_status"], 200 if forwarded else None)

    def test_redirect_after_write_303(self):
        self._redirect(303, "See Other", P.redirect_after_write(303), True)
        self.assertEqual(self.origin.posts()[0][1], f"/r/{ROOM}?format=json")

    def test_redirect_after_write_307(self):
        self._redirect(307, "Temporary Redirect", P.redirect_after_write(307), True)

    def test_redirect_without_write(self):
        self._redirect(303, "See Other", P.redirect_without_write(303), False)

    def test_stall_after_write_then_close(self):
        proxy = self.start(first_post=P.stall_after_write(1.0))
        data, exc, took = self.post(proxy)
        self.assertIsNone(exc)
        self.assertEqual(data, b"")  # nothing at all, then a close
        self.assertGreaterEqual(took, 0.9)
        self.assertLess(took, 4.0)
        self.assertEqual(len(self.origin.posts()), 1)  # forwarded before the stall
        self.assertTrue(proxy.wait_idle(5))
        self.assertEqual(proxy.log[0]["action"], "stall_after_write(1)")
        self.assertEqual((proxy.log[0]["forwarded"], proxy.log[0]["origin_status"]), (True, 200))

    def test_stop_ends_a_stall(self):
        proxy = P.FaultProxy(self.origin.url, P.Plan(room=ROOM, first_post=P.stall_after_write(30)))
        t = threading.Thread(target=self.post, args=(proxy,), daemon=True)
        t.start()
        deadline = time.monotonic() + 5
        while not self.origin.posts() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertFalse(proxy.wait_idle(0.2))  # stalled handler still in flight
        t0 = time.monotonic()
        proxy.stop()
        t.join(5)
        self.assertLess(time.monotonic() - t0, 3.0)

    def test_stall_ends_when_the_client_hangs_up(self):
        # A client that sends and closes at once: the forward may start after it is gone, and
        # the stall must not keep the proxy busy for its full length after that.
        proxy = self.start(first_post=P.stall_after_write(30))
        s = socket.create_connection(("127.0.0.1", proxy.port), timeout=5)
        s.sendall(f"POST /r/{ROOM}?format=json HTTP/1.1\r\nHost: x\r\n"
                  f"Content-Length: {len(BODY)}\r\n\r\n".encode() + BODY)
        s.close()
        t0 = time.monotonic()
        self.assertTrue(proxy.wait_idle(5))
        self.assertLess(time.monotonic() - t0, 3.0)
        self.assertEqual(len(self.origin.posts()), 1)  # forwarded all the same
        self.assertEqual((proxy.log[0]["forwarded"], proxy.log[0]["origin_status"]), (True, 200))

    def test_truncate_after_write(self):
        proxy = self.start(first_post=P.truncate_after_write())
        data, exc, _ = self.post(proxy)
        self.assertIsNone(exc)
        status, hdrs, body = split(data)
        self.assertEqual(status, "HTTP/1.1 200 OK")
        self.assertEqual(hdrs["content-length"], str(len(REPLY)))  # the origin's, unchanged
        self.assertEqual(hdrs["content-type"], "application/json")
        self.assertEqual(body, REPLY[: len(REPLY) // 2])
        self.assertEqual(len(self.origin.posts()), 1)
        self.assertTrue(proxy.wait_idle(5))
        self.assertEqual(proxy.log[0]["action"], "truncate_after_write")
        self.assertEqual((proxy.log[0]["forwarded"], proxy.log[0]["origin_status"]), (True, 200))

    def test_reset_before_write(self):
        proxy = self.start(first_post=P.reset_before_write())
        data, exc, _ = self.post(proxy)
        self.assertEqual(data, b"")
        self.assertIsInstance(exc, ConnectionResetError)
        self.assertTrue(proxy.wait_idle(5))
        self.assertEqual(self.origin.posts(), [])
        self.assertEqual(proxy.log, [{"method": "POST", "path_without_query": f"/r/{ROOM}",
                                      "action": "reset_before_write", "forwarded": False,
                                      "origin_status": None}])

    def test_only_first_post_gets_the_action(self):
        proxy = self.start(first_post=P.redirect_after_write(303))
        self.post(proxy)
        data, _, _ = self.post(proxy)
        self.assertTrue(data.startswith(b"HTTP/1.1 200 OK\r\n"))
        raw(proxy, "POST", "/r/other-room?format=json", BODY)
        self.assertTrue(proxy.wait_idle(5))
        self.assertEqual([e["action"] for e in proxy.log], ["redirect_after_write(303)", "pass", "pass"])
        self.assertEqual(len(self.origin.posts()), 3)

    def test_w7_shape_later_posts_reset_and_reads_503(self):
        proxy = self.start(
            first_post=P.truncate_after_write(),
            later_posts=P.reset_before_write(),
            room_reads=P.unavailable(503),
        )
        self.post(proxy)
        _, exc, _ = self.post(proxy)
        self.assertIsInstance(exc, ConnectionResetError)
        data, _, _ = raw(proxy, "GET", f"/r/{ROOM}?format=json&limit=200")
        status, hdrs, body = split(data)
        self.assertEqual(status, "HTTP/1.1 503 Service Unavailable")
        self.assertTrue(hdrs["content-type"].startswith("text/plain"))
        self.assertTrue(0 < len(body) < 100)
        data, _, _ = raw(proxy, "GET", f"/r/{ROOM}/export")
        self.assertTrue(data.startswith(b"HTTP/1.1 503 "))
        data, _, _ = raw(proxy, "GET", "/rooms?format=json")
        self.assertTrue(data.startswith(b"HTTP/1.1 200 "))
        self.assertTrue(proxy.wait_idle(5))
        self.assertEqual(
            [(e["method"], e["path_without_query"], e["action"], e["forwarded"]) for e in proxy.log],
            [
                ("POST", f"/r/{ROOM}", "truncate_after_write", True),
                ("POST", f"/r/{ROOM}", "reset_before_write", False),
                ("GET", f"/r/{ROOM}", "unavailable(503)", False),
                ("GET", f"/r/{ROOM}/export", "unavailable(503)", False),
                ("GET", "/rooms", "pass", True),
            ],
        )
        self.assertEqual(len(self.origin.posts()), 1)
        self.assertEqual([r[1] for r in self.origin.requests if r[0] == "GET"], ["/rooms?format=json"])

    def test_percent_encoded_room_gets_the_rules(self):
        # Upstream routes the decoded path, so `/r/lab%2Dwx` is the room `lab-wx`: it must not
        # dodge the first-post action, the later-post action or the room-read action.
        enc = "/r/lab%2Dwx"
        proxy = self.start(
            first_post=P.redirect_after_write(307),
            later_posts=P.reset_before_write(),
            room_reads=P.unavailable(503),
        )
        data, _, _ = raw(proxy, "POST", enc + "?format=json", BODY)
        self.assertTrue(data.startswith(b"HTTP/1.1 307 "))
        _, exc, _ = raw(proxy, "POST", "/r/lab-w%78", BODY)  # %78 = "x"
        self.assertIsInstance(exc, ConnectionResetError)
        data, _, _ = raw(proxy, "GET", enc + "?format=json")
        self.assertTrue(data.startswith(b"HTTP/1.1 503 "))
        data, _, _ = raw(proxy, "GET", "/r/%6cab-wx/export")
        self.assertTrue(data.startswith(b"HTTP/1.1 503 "))
        self.assertTrue(proxy.wait_idle(5))
        self.assertEqual(
            [(e["path_without_query"], e["action"]) for e in proxy.log],
            [
                (enc, "redirect_after_write(307)"),
                ("/r/lab-w%78", "reset_before_write"),
                (enc, "unavailable(503)"),
                ("/r/%6cab-wx/export", "unavailable(503)"),
            ],
        )
        self.assertEqual(len(self.origin.posts()), 1)
        self.assertEqual(P.room_request_path("/r/lab%2Dwx"), "/r/lab-wx")

    def test_fragment_is_dropped_like_upstream(self):
        # httptools drops `#...` from the target before upstream routes it, so `/r/lab-wx#x` is
        # the room `lab-wx` there and must get the room's rules here (review 2, Fix 1).
        proxy = self.start(
            first_post=P.redirect_after_write(303),
            later_posts=P.reset_before_write(),
            room_reads=P.unavailable(503),
        )
        data, _, _ = raw(proxy, "POST", f"/r/{ROOM}#x", BODY)
        self.assertTrue(data.startswith(b"HTTP/1.1 303 "))
        _, exc, _ = raw(proxy, "POST", f"/r/{ROOM}#?format=json", BODY)
        self.assertIsInstance(exc, ConnectionResetError)
        for target in (f"/r/{ROOM}#x", f"/r/{ROOM}#?format=json&limit=200", f"/r/{ROOM}/export#x",
                       f"/r/{ROOM}?format=json#x", "/r/lab%2Dwx#x"):
            with self.subTest(target=target):
                data, _, _ = raw(proxy, "GET", target)
                self.assertTrue(data.startswith(b"HTTP/1.1 503 "), data[:40])
        self.assertTrue(proxy.wait_idle(5))
        self.assertEqual(len(self.origin.posts()), 1)  # only the first POST was forwarded
        self.assertEqual([r for r in self.origin.requests if r[0] == "GET"], [])
        self.assertEqual(P.room_request_path("/r/lab-wx#x"), "/r/lab-wx")
        self.assertEqual(P.room_request_path("/r/lab-wx#a/export"), "/r/lab-wx")
        self.assertEqual(P.room_request_path("/r/lab-wx%23x"), "/r/lab-wx#x")  # encoded: not a fragment

    def test_chunked_body_is_411(self):
        proxy = self.start()
        data, _, _ = raw(proxy, "POST", f"/r/{ROOM}", headers=b"Transfer-Encoding: chunked\r\n")
        self.assertTrue(data.startswith(b"HTTP/1.1 411 Length Required\r\n"))
        self.assertTrue(proxy.wait_idle(5))
        self.assertEqual(self.origin.requests, [])
        self.assertEqual(proxy.log[0]["action"], "reject_411")
        self.assertFalse(proxy.log[0]["forwarded"])

    def test_log_holds_no_bodies(self):
        proxy = self.start()
        raw(proxy, "POST", f"/r/{ROOM}?format=json", json.dumps({"from": "w", "text": "secret-text-123"}).encode())
        self.assertTrue(proxy.wait_idle(5))
        self.assertEqual(set(proxy.log[0]), {"method", "path_without_query", "action", "forwarded", "origin_status"})
        self.assertNotIn("secret-text-123", json.dumps(proxy.log))
        self.assertNotIn("format=json", json.dumps(proxy.log))

    def test_wait_idle_waits_for_an_inflight_forward(self):
        proxy = self.start()
        result = {}
        t = threading.Thread(
            target=lambda: result.update(r=raw(proxy, "POST", "/slow", BODY)), daemon=True
        )
        t.start()
        self.assertTrue(self.origin.entered.wait(5))  # the origin holds the request
        self.assertFalse(proxy.wait_idle(0.3))  # ... so the proxy is not idle
        self.origin.release.set()
        self.assertTrue(proxy.wait_idle(5))
        # Idle means done: the forward finished and the log says so.
        self.assertEqual(proxy.log[0]["origin_status"], 200)
        t.join(5)
        self.assertTrue(result["r"][0].startswith(b"HTTP/1.1 200 OK"))

    def test_binds_loopback_only(self):
        proxy = self.start()
        self.assertTrue(proxy.url.startswith("http://127.0.0.1:"))
        self.assertEqual(proxy._server.server_address[0], "127.0.0.1")


class ProxySetup(unittest.TestCase):
    def test_origin_must_be_loopback_http(self):
        for bad in ("http://localhost:8000", "https://127.0.0.1:8000", "http://10.0.0.1:8000",
                    "http://127.0.0.1", "http://127.0.0.1:8000/x", "ftp://127.0.0.1:21",
                    "http://u:p@127.0.0.1:8000", "http://127.0.0.2:8000"):
            with self.subTest(bad=bad), self.assertRaises(SetupError):
                P.FaultProxy(bad, P.Plan(room=ROOM))

    def test_redirect_codes(self):
        with self.assertRaises(ValueError):
            P.redirect_after_write(302)


if __name__ == "__main__":
    unittest.main()
