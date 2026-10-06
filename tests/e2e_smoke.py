"""End-to-end smoke harness for the native Mojo gate proxy.

This is intentionally standalone (run directly, not collected by pytest) because
it needs the compiled binary and real sockets. It starts a small raw-socket
upstream, launches ``gate_mojo``, and asserts observable proxy behaviour.

Usage:
    python tests/e2e_smoke.py
Exit code 0 == all checks passed.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from mojo_gate.compiler import ensure_binary  # noqa: E402

RESULTS: list[tuple[bool, str]] = []


def check(ok: bool, name: str, detail: str = "") -> None:
    RESULTS.append((ok, name))
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {name}" + (f"  -- {detail}" if detail and not ok else ""))


class Upstream:
    """Deterministic HTTP/1.1 upstream that counts requests per method+path."""

    def __init__(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(64)
        self.port = self.sock.getsockname()[1]
        self.counts: dict[str, int] = {}
        self.lock = threading.Lock()
        self._stop = False
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self) -> None:
        while not self._stop:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                break
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn: socket.socket) -> None:
        conn.settimeout(5)
        try:
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = conn.recv(65536)
                if not chunk:
                    return
                data += chunk
            head, _, rest = data.partition(b"\r\n\r\n")
            lines = head.split(b"\r\n")
            method, target, _ = lines[0].split(b" ")
            key = method.decode() + " " + target.decode()
            # read body if content-length present
            cl = 0
            for line in lines[1:]:
                if line.lower().startswith(b"content-length:"):
                    cl = int(line.split(b":", 1)[1].strip())
            while len(rest) < cl:
                rest += conn.recv(65536)
            with self.lock:
                self.counts[key] = self.counts.get(key, 0) + 1
                n = self.counts[key]
            body = b'{"path":"%s","n":%d}' % (target, n)
            resp = (
                b"HTTP/1.1 200 OK\r\n"
                b"content-type: application/json\r\n"
                b"content-length: " + str(len(body)).encode() + b"\r\n"
                b"\r\n" + body
            )
            conn.sendall(resp)
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def count(self, key: str) -> int:
        with self.lock:
            return self.counts.get(key, 0)

    def stop(self) -> None:
        self._stop = True
        try:
            self.sock.close()
        except OSError:
            pass


class RawClient:
    """Minimal HTTP client that lets us send raw (possibly malformed) bytes."""

    def __init__(self, port: int):
        self.port = port

    def request(self, raw: bytes, read: bool = True) -> bytes:
        s = socket.create_connection(("127.0.0.1", self.port), timeout=5)
        try:
            s.sendall(raw)
            if not read:
                return b""
            s.settimeout(5)
            out = b""
            while True:
                try:
                    chunk = s.recv(65536)
                except socket.timeout:
                    break
                if not chunk:
                    break
                out += chunk
                if b"\r\n\r\n" in out:
                    # stop once we have a content-length-satisfying body or it closed
                    head, _, body = out.partition(b"\r\n\r\n")
                    cl = None
                    for line in head.split(b"\r\n")[1:]:
                        if line.lower().startswith(b"content-length:"):
                            cl = int(line.split(b":", 1)[1].strip())
                    if cl is not None and len(body) >= cl:
                        break
                    if cl is None and b"connection: close" in head.lower():
                        break
            return out
        finally:
            s.close()

    def get(self, path: str, extra: str = "") -> bytes:
        return self.request(
            f"GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{extra}\r\n".encode()
        )


def main() -> int:
    binary = ensure_binary()
    if binary is None:
        print("SKIP: no compiled binary / mojo compiler")
        return 2

    up = Upstream()
    port = _free_port()
    rate_port = _free_port()

    base_cmd = [
        str(binary),
        "--host", "127.0.0.1",
        "--upstream-host", "127.0.0.1",
        "--upstream-port", str(up.port),
        "--cache-ttl", "60",
        "--purge-endpoint", "/_mojo_gate/purge",
        "--internal-token", "sekret-token",
        "--no-cache-prefixes", "/_mojo_gate",
        "--analytics-endpoint", "/_mojo_gate/analytics",
    ]
    proc = subprocess.Popen(
        base_cmd + ["--port", str(port), "--no-rate-limit"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    proc2 = subprocess.Popen(
        base_cmd + ["--port", str(rate_port), "--rate-rules", "/api:2:60"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    try:
        _wait_port(port)
        _wait_port(rate_port)
        c = RawClient(port)
        r = RawClient(rate_port)

        # 1. cache miss then hit
        resp1 = c.get("/api/items/1")
        resp2 = c.get("/api/items/1")
        check(resp1.startswith(b"HTTP/1.1 200"), "GET cache miss 200", resp1[:80].decode(errors="replace"))
        check(up.count("GET /api/items/1") == 1, "second GET served from cache", f"count={up.count('GET /api/items/1')}")
        check(resp1 == resp2 or b'{"path"' in resp2, "cache hit body present")

        # 2. write flushes cache
        c.request(b"POST /api/items HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc")
        c.get("/api/items/1")
        check(up.count("GET /api/items/1") == 2, "POST flushed cache (GET re-fetched)", f"count={up.count('GET /api/items/1')}")

        # 3. purge endpoint
        p = c.request(b"POST /_mojo_gate/purge HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        check(p.startswith(b"HTTP/1.1 200"), "loopback purge 200", p[:60].decode(errors="replace"))

        # 4. analytics endpoint blocked from external
        a = c.get("/_mojo_gate/analytics")
        check(a.startswith(b"HTTP/1.1 403"), "direct analytics access forbidden", a[:60].decode(errors="replace"))

        # 5. rate limiting
        r1 = r.get("/api/x")
        r2 = r.get("/api/x")
        r3 = r.get("/api/x")
        check(r3.startswith(b"HTTP/1.1 429"), "rate limit 429 after limit", r3[:60].decode(errors="replace"))
        check(b"retry-after:" in r3.lower(), "429 has retry-after header")
        check(r1.startswith(b"HTTP/1.1 200") and r2.startswith(b"HTTP/1.1 200"), "requests under limit pass")

        # 6. smuggling: CL + TE
        sm = c.request(b"POST /x HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\nabc")
        check(sm.startswith(b"HTTP/1.1 400"), "CL+TE rejected 400", sm[:60].decode(errors="replace"))

        # 7. duplicate content-length
        dcl = c.request(b"POST /x HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\nContent-Length: 4\r\nConnection: close\r\n\r\nabc")
        check(dcl.startswith(b"HTTP/1.1 400"), "duplicate CL rejected 400", dcl[:60].decode(errors="replace"))

        # 8. missing Host on HTTP/1.1
        nh = c.request(b"GET / HTTP/1.1\r\nConnection: close\r\n\r\n")
        check(nh.startswith(b"HTTP/1.1 400"), "missing Host rejected 400", nh[:60].decode(errors="replace"))

        # 9. whitespace before colon
        wc = c.request(b"GET / HTTP/1.1\r\nHost: localhost\r\nBad Header : x\r\nConnection: close\r\n\r\n")
        check(wc.startswith(b"HTTP/1.1 400"), "space-before-colon rejected 400", wc[:60].decode(errors="replace"))

        # 10. CRLF injection in header value
        inj = c.request(b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Bad: a\x00b\r\nConnection: close\r\n\r\n")
        check(inj.startswith(b"HTTP/1.1 400"), "NUL in header rejected 400", inj[:60].decode(errors="replace"))

        # 11. unsupported transfer-encoding
        te = c.request(b"POST /x HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: gzip\r\nConnection: close\r\n\r\n")
        check(te.startswith(b"HTTP/1.1 501"), "unsupported TE -> 501", te[:60].decode(errors="replace"))

        # 12. non-ASCII target not cached
        na = c.request("GET /café HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".encode("utf-8"))
        na2 = c.request("GET /café HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".encode("utf-8"))
        check(up.count("GET /caf\u00e9") >= 2 or up.count("GET /cafÃ©") >= 2, "non-ASCII path not cached")

        # 13. internal token purge from non-loopback impossible here; skip
        # 14. HEAD served
        h = c.request(b"HEAD /api/items/1 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        check(h.startswith(b"HTTP/1.1 200"), "HEAD 200", h[:60].decode(errors="replace"))

    finally:
        for p in (proc, proc2):
            p.terminate()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
        up.stop()

    failed = [n for ok, n in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} checks passed")
    return 1 if failed else 0


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _wait_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)


if __name__ == "__main__":
    sys.exit(main())
