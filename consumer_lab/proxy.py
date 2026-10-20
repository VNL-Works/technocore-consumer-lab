"""A fault-injecting HTTP/1.1 relay on loopback, between a writer client and upstream.

It reads one request per connection (request line, headers, a `Content-Length` body),
forwards it to the origin with `Connection: close`, and applies the plan's action to the
**first** `POST /r/{room}`. Every other request passes through, except where the plan says
otherwise for later `POST /r/{room}` and for reads of the room (W7).

The proxy is deterministic by construction: every "after write" action reads the origin's
whole reply before it misbehaves, so the write has committed before the client sees anything.

Not supported, on purpose: keep-alive reuse, chunked request bodies (answered 411 and
logged), TLS, HTTP/2, absolute-form request targets (answered 400 and logged).
"""

from __future__ import annotations

import http.client
import select
import socket
import socketserver
import struct
import threading
import time
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

from consumer_lab.scenarios import start_kit_thread
from consumer_lab.server import SetupError

CLIENT_READ_TIMEOUT = 15.0  # a client that stops sending mid-request is dropped after this
ORIGIN_TIMEOUT = 30.0
MAX_LINE = 65536
MAX_HEADERS = 100
MAX_BODY = 1 << 20

# Hop-by-hop headers (RFC 9110 §7.6.1) plus Host, which is rewritten to the origin's.
_HOP = {
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
}


@dataclass(frozen=True)
class Action:
    """What the proxy does with one request. Build with the functions below."""

    kind: str
    code: int | None = None
    seconds: float | None = None

    @property
    def label(self) -> str:
        if self.kind in ("redirect_after_write", "redirect_without_write", "unavailable"):
            return f"{self.kind}({self.code})"
        if self.kind == "stall_after_write":
            return f"{self.kind}({self.seconds:g})"
        return self.kind

    @property
    def forwards(self) -> bool:
        return self.kind in ("pass", "redirect_after_write", "stall_after_write", "truncate_after_write")


PASS = Action("pass")


def redirect_after_write(code: int) -> Action:
    if code not in (303, 307):
        raise ValueError("redirect_after_write takes 303 or 307")
    return Action("redirect_after_write", code=code)


def stall_after_write(seconds: float) -> Action:
    return Action("stall_after_write", seconds=float(seconds))


def truncate_after_write() -> Action:
    return Action("truncate_after_write")


def reset_before_write() -> Action:
    return Action("reset_before_write")


def redirect_without_write(code: int = 303) -> Action:
    if code not in (303, 307):
        raise ValueError("redirect_without_write takes 303 or 307")
    return Action("redirect_without_write", code=code)


def unavailable(code: int = 503) -> Action:
    """Answer `code` with a short plain-text body, without forwarding."""
    return Action("unavailable", code=code)


@dataclass(frozen=True)
class Plan:
    """Which action applies to which request, for one room.

    `first_post` applies to the first `POST /r/{room}`; `later_posts` to every later one;
    `room_reads` to every GET of `/r/{room}` or of a path under it (`/r/{room}/...`).
    Everything else passes through.
    """

    room: str
    first_post: Action = PASS
    later_posts: Action = PASS
    room_reads: Action = PASS


_REASONS = {
    200: "OK",
    303: "See Other",
    307: "Temporary Redirect",
    400: "Bad Request",
    411: "Length Required",
    502: "Bad Gateway",
    503: "Service Unavailable",
}


class _BadRequest(Exception):
    def __init__(self, code: int, why: str):
        super().__init__(why)
        self.code = code


class _Server(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, proxy: "FaultProxy"):
        self.proxy = proxy
        super().__init__(("127.0.0.1", 0), _Handler)

    def get_request(self):
        # Count the connection as in flight before it leaves the listen backlog, so that
        # `wait_idle` (which also checks the backlog) never sees a gap between the two.
        with self.proxy._cond:
            self.proxy._inflight += 1
        try:
            return super().get_request()
        except BaseException:
            self.proxy._done()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.proxy._done()

    def handle_error(self, request, client_address):
        pass  # a client that hung up mid-reply is not the kit's error; the log says what happened


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        self.server.proxy._handle(self.request)


class FaultProxy:
    """The relay. `.url`, `.log`, `.wait_idle(timeout)`, `.stop()`.

    `origin_url` must be `http://127.0.0.1:<port>`; anything else is a `SetupError`. The proxy
    listens on 127.0.0.1 only, and its only outbound connection is to that origin.
    """

    def __init__(self, origin_url: str, plan: Plan):
        parts = urlsplit(origin_url)
        try:
            port = parts.port
        except ValueError:
            port = None
        if (
            parts.scheme != "http"
            or parts.hostname != "127.0.0.1"
            or not port
            or parts.path not in ("", "/")
            or parts.query
            or parts.fragment
            or parts.username
            or parts.password
        ):
            raise SetupError(f"proxy origin must be http://127.0.0.1:<port>, got {origin_url!r}")
        if not plan.room or "/" in plan.room:
            raise SetupError(f"proxy plan needs a plain room name, got {plan.room!r}")
        self.origin_host, self.origin_port = "127.0.0.1", port
        self.plan = plan
        self.log: list[dict] = []
        self._cond = threading.Condition()
        self._inflight = 0
        self._first_post_seen = False
        self._stopping = threading.Event()
        self._server = _Server(self)
        host, self.port = self._server.server_address[:2]
        if host != "127.0.0.1":
            self._server.server_close()
            raise SetupError(f"proxy bound to {host}, not 127.0.0.1")
        self.url = f"http://127.0.0.1:{self.port}"
        self._thread = threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.05}, name="fault-proxy", daemon=True
        )
        # The deferred signals stay blocked in this thread and in the request threads it starts,
        # so a deferred signal sent to the worker process is taken by the main thread, not here.
        start_kit_thread(self._thread)

    # ------------------------------------------------------------------ lifecycle

    def wait_idle(self, timeout: float) -> bool:
        """True once no handler is in flight and no connection waits in the backlog."""
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                if self._inflight == 0 and not self._backlog():
                    return True
                left = deadline - time.monotonic()
                if left <= 0:
                    return False
                self._cond.wait(min(left, 0.05))

    def _backlog(self) -> bool:
        try:
            readable, _, _ = select.select([self._server.socket], [], [], 0)
        except (OSError, ValueError):  # closed by stop()
            return False
        return bool(readable)

    def _done(self) -> None:
        with self._cond:
            self._inflight -= 1
            self._cond.notify_all()

    def stop(self) -> None:
        self._stopping.set()  # ends any stall at once
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=10)

    def __enter__(self) -> "FaultProxy":
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    # ------------------------------------------------------------------ one connection

    def _choose(self, method: str, path: str) -> Action:
        # Match on the percent-decoded path, as upstream routes it: `/r/lab%2Dw2` is the room
        # `lab-w2` to Starlette, so it must get that room's action here too.
        path = room_request_path(path)
        room_path = f"/r/{self.plan.room}"
        with self._cond:
            if method == "POST" and path == room_path:
                if not self._first_post_seen:
                    self._first_post_seen = True
                    return self.plan.first_post
                return self.plan.later_posts
        if method == "GET" and (path == room_path or path.startswith(room_path + "/")):
            return self.plan.room_reads
        return PASS

    def _handle(self, sock: socket.socket) -> None:
        sock.settimeout(CLIENT_READ_TIMEOUT)
        rfile = sock.makefile("rb")
        entry: dict | None = None
        try:
            try:
                method, target, headers, body = _read_request(rfile)
            except _BadRequest as exc:
                entry = self._log_entry("?", "?", "reject_411" if exc.code == 411 else "reject_400")
                _send_simple(sock, exc.code, f"{exc.code} {exc}\n".encode())
                return
            except (OSError, ValueError):
                return  # the client went away before a whole request arrived: nothing to log
            path = target.split("?", 1)[0]
            action = self._choose(method, path)
            entry = self._log_entry(method, path, action.label)
            if action.kind == "reset_before_write":
                rfile.close()
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                sock.close()
                return
            if action.kind == "unavailable":
                _send_simple(sock, action.code, f"{action.code} consumer-lab: room unavailable\n".encode())
                return
            if action.kind == "redirect_without_write":
                _send_redirect(sock, action.code, self.plan.room)
                return
            # Every remaining action forwards, and reads the whole reply before answering.
            try:
                status, reason, resp_headers, resp_body = self._forward(method, target, headers, body, entry)
            except (OSError, http.client.HTTPException):
                _send_simple(sock, 502, b"502 consumer-lab: origin unreachable\n")
                return
            if action.kind == "pass":
                _send_response(sock, status, reason, resp_headers, resp_body)
            elif action.kind == "redirect_after_write":
                _send_redirect(sock, action.code, self.plan.room)
            elif action.kind == "stall_after_write":
                self._stall(sock, action.seconds)
            elif action.kind == "truncate_after_write":
                _send_truncated(sock, status, reason, resp_headers, resp_body)
        except OSError:
            pass  # the client hung up while we answered; the log already holds the truth
        finally:
            try:
                rfile.close()
            except OSError:
                pass

    def _stall(self, sock: socket.socket, seconds: float) -> None:
        """Send nothing for `seconds`, then return (the caller closes).

        Ends early when `.stop()` is called, or when the client has hung up: once nobody is
        left to be silent to, the silence is over. Without that, a client that posts and
        closes at once (the forward starting after it exited) would keep the handler busy up
        to `seconds` past the cycle, racing the kit's `wait_idle`.
        """
        deadline = time.monotonic() + seconds
        while not self._stopping.is_set():
            left = deadline - time.monotonic()
            if left <= 0:
                return
            try:
                readable, _, _ = select.select([sock], [], [], min(left, 0.1))
                if readable and not sock.recv(4096):
                    return  # EOF: the client hung up
            except OSError:
                return  # reset by the client: also gone

    def _log_entry(self, method: str, path: str, action: str) -> dict:
        entry = {
            "method": method,
            "path_without_query": path,
            "action": action,
            "forwarded": False,
            "origin_status": None,
        }
        with self._cond:
            self.log.append(entry)
        return entry

    def _forward(self, method, target, headers, body, entry):
        conn = http.client.HTTPConnection(self.origin_host, self.origin_port, timeout=ORIGIN_TIMEOUT)
        try:
            out = {k: v for k, v in headers if k.lower() not in _HOP}
            out["Host"] = f"{self.origin_host}:{self.origin_port}"
            out["Connection"] = "close"
            if body or method in ("POST", "PUT", "PATCH"):
                out["Content-Length"] = str(len(body))
            conn.request(method, target, body=body if body else None, headers=out)
            with self._cond:
                entry["forwarded"] = True
            resp = conn.getresponse()
            data = resp.read()
            with self._cond:
                entry["origin_status"] = resp.status
            return resp.status, resp.reason, resp.getheaders(), data
        finally:
            conn.close()


# ---------------------------------------------------------------------- wire helpers


def room_request_path(path: str) -> str:
    """A request path as upstream routes it: any `#fragment` dropped (httptools drops it from
    the target before routing), then percent-decoded (Starlette matches `/r/{room}` on the
    decoded path). The proxy log keeps the raw path the client sent."""
    return unquote(path.split("#", 1)[0])


def _read_request(rfile) -> tuple[str, str, list[tuple[str, str]], bytes]:
    line = rfile.readline(MAX_LINE + 1)
    if not line:
        raise ValueError("no request")
    if len(line) > MAX_LINE:
        raise _BadRequest(400, "request line too long")
    parts = line.decode("latin-1").rstrip("\r\n").split(" ")
    if len(parts) != 3 or not parts[2].startswith("HTTP/1."):
        raise _BadRequest(400, "bad request line")
    method, target, _ = parts
    headers: list[tuple[str, str]] = []
    while True:
        h = rfile.readline(MAX_LINE + 1)
        if not h:
            raise ValueError("eof in headers")
        if h in (b"\r\n", b"\n"):
            break
        if len(headers) >= MAX_HEADERS or len(h) > MAX_LINE or b":" not in h:
            raise _BadRequest(400, "bad header")
        name, value = h.decode("latin-1").split(":", 1)
        headers.append((name.strip(), value.strip()))
    if not target.startswith("/"):
        raise _BadRequest(400, "only origin-form request targets")
    names = {k.lower(): v for k, v in headers}
    if "transfer-encoding" in names:
        raise _BadRequest(411, "chunked request bodies are not supported; send Content-Length")
    length = names.get("content-length", "0")
    if not length.isdigit() or int(length) > MAX_BODY:
        raise _BadRequest(400, "bad Content-Length")
    body = rfile.read(int(length)) if int(length) else b""
    if len(body) != int(length):
        raise ValueError("eof in body")
    return method, target, headers, body


def _status_line(code: int, reason: str | None = None) -> bytes:
    return f"HTTP/1.1 {code} {reason or _REASONS.get(code, '')}\r\n".encode("latin-1")


def _send_simple(sock: socket.socket, code: int, body: bytes) -> None:
    head = _status_line(code) + (
        f"Content-Type: text/plain; charset=utf-8\r\nContent-Length: {len(body)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode()
    sock.sendall(head + body)


def _send_redirect(sock: socket.socket, code: int, room: str) -> None:
    head = _status_line(code) + (
        f"Location: /r/{room}?format=json\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    ).encode("latin-1")
    sock.sendall(head)


def _send_response(sock, status, reason, headers, body) -> None:
    lines = [_status_line(status, reason)]
    for k, v in headers:
        if k.lower() in ("connection", "keep-alive", "transfer-encoding", "content-length"):
            continue
        lines.append(f"{k}: {v}\r\n".encode("latin-1"))
    lines.append(f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode())
    sock.sendall(b"".join(lines) + body)


def _send_truncated(sock, status, reason, headers, body) -> None:
    """The origin's status line and headers unchanged, then at most half of the body."""
    lines = [_status_line(status, reason)]
    has_length = False
    for k, v in headers:
        if k.lower() == "content-length":
            has_length = True
        if k.lower() == "transfer-encoding":
            continue  # http.client already de-chunked the body; the length below replaces it
        lines.append(f"{k}: {v}\r\n".encode("latin-1"))
    if not has_length:
        lines.append(f"Content-Length: {len(body)}\r\n".encode())
    lines.append(b"\r\n")
    sock.sendall(b"".join(lines) + body[: len(body) // 2])
