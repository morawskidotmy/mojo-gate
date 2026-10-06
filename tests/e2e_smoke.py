# ruff: noqa: E501  -- raw HTTP byte literals are clearer on one line
"""End-to-end smoke harness for the native Mojo gate proxy.

This is intentionally standalone (run directly, not collected by pytest) because
it needs the compiled binary and real sockets. It starts a small raw-socket
upstream, launches ``gate_mojo``, and asserts observable proxy behaviour.

Usage:
    python tests/e2e_smoke.py
Exit code 0 == all checks passed.
"""

from __future__ import annotations

import contextlib
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from mojo_gate.compiler import ensure_binary

TOKEN = "sekret-token"
RESULTS: list[tuple[bool, str]] = []


def check(ok: bool, name: str, detail: str = "") -> None:
    RESULTS.append((ok, name))
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {name}" + (f"  -- {detail}" if detail and not ok else ""))


class Upstream:
    """Deterministic HTTP/1.1 upstream that counts requests and records the last body."""

    def __init__(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(64)
        self.port = self.sock.getsockname()[1]
        self.counts: dict[str, int] = {}
        self.last_headers: dict[str, str] = {}
        self.last_body: bytes = b""
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
            headers: dict[str, str] = {}
            for line in lines[1:]:
                if b":" in line:
                    k, v = line.split(b":", 1)
                    headers[k.decode("latin-1").lower()] = v.strip().decode("latin-1")
            cl = int(headers.get("content-length", "0") or "0")
            if headers.get("expect", "").lower() == "100-continue":
                conn.sendall(b"HTTP/1.1 100 Continue\r\n\r\n")
            while len(rest) < cl:
                more = conn.recv(65536)
                if not more:
                    break
                rest += more
            with self.lock:
                self.counts[key] = self.counts.get(key, 0) + 1
                n = self.counts[key]
                self.last_headers = headers
                self.last_body = rest[:cl]
            body = b'{"path":"%s","n":%d}' % (target, n)
            vary = b"vary: accept-language\r\n" if b"/vary" in target else b""
            resp = (
                b"HTTP/1.1 200 OK\r\n"
                b"content-type: application/json\r\n"
                + vary
                + b"content-length: " + str(len(body)).encode() + b"\r\n"
                b"\r\n" + body
            )
            conn.sendall(resp)
        except OSError:
            pass
        finally:
            with contextlib.suppress(OSError):
                conn.close()

    def count(self, key: str) -> int:
        with self.lock:
            return self.counts.get(key, 0)

    def stop(self) -> None:
        self._stop = True
        with contextlib.suppress(OSError):
            self.sock.close()


class RawClient:
    """Minimal HTTP client that lets us send raw (possibly malformed) bytes."""

    def __init__(self, port: int, host: str = "127.0.0.1"):
        self.port = port
        self.host = host

    def request(self, raw: bytes, read: bool = True) -> bytes:
        s = socket.create_connection((self.host, self.port), timeout=5)
        try:
            s.sendall(raw)
            if not read:
                return b""
            s.settimeout(5)
            out = b""
            while True:
                try:
                    chunk = s.recv(65536)
                except TimeoutError:
                    break
                if not chunk:
                    break
                out += chunk
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


def _status(resp: bytes) -> bytes:
    return resp.split(b"\r\n", 1)[0]


def main() -> int:
    binary = ensure_binary()
    if binary is None:
        print("SKIP: no compiled binary / mojo compiler")
        return 2

    up = Upstream()
    port = _free_port()
    rate_port = _free_port()
    ext_ip = _nonloopback_ip()
    ext_port = _free_port()

    base_cmd = [
        str(binary),
        "--host", "127.0.0.1",
        "--upstream-host", "127.0.0.1",
        "--upstream-port", str(up.port),
        "--cache-ttl", "60",
        "--purge-endpoint", "/_mojo_gate/purge",
        "--internal-token", TOKEN,
        "--no-cache-prefixes", "/_mojo_gate",
        "--analytics-endpoint", "/_mojo_gate/analytics",
    ]
    proc = subprocess.Popen(
        [*base_cmd, "--port", str(port), "--no-rate-limit"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    proc2 = subprocess.Popen(
        [*base_cmd, "--port", str(rate_port), "--rate-rules", "/api:2:60"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    procs = [proc, proc2]
    ext_proc = None
    if ext_ip:
        ext_proc = subprocess.Popen(
            [str(binary), "--host", ext_ip, "--port", str(ext_port),
             "--upstream-host", "127.0.0.1", "--upstream-port", str(up.port),
             "--cache-ttl", "60", "--no-rate-limit",
             "--no-cache-prefixes", "/_mojo_gate"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )
        procs.append(ext_proc)
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
        check(b'{"path"' in resp2, "cache hit body present")

        # 2. write flushes cache
        c.request(b"POST /api/items HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc")
        c.get("/api/items/1")
        check(up.count("GET /api/items/1") == 2, "POST flushed cache (GET re-fetched)", f"count={up.count('GET /api/items/1')}")

        # 3. purge endpoint requires the internal token (loopback is not enough)
        no_tok = c.request(b"POST /_mojo_gate/purge HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        check(no_tok.startswith(b"HTTP/1.1 403"), "purge without token forbidden", no_tok[:60].decode(errors="replace"))
        tok = c.request(
            b"POST /_mojo_gate/purge HTTP/1.1\r\nHost: localhost\r\nX-Mojo-Gate-Token: " + TOKEN.encode()
            + b"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )
        check(tok.startswith(b"HTTP/1.1 200"), "purge with token 200", tok[:60].decode(errors="replace"))

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

        # 10. NUL in header
        inj = c.request(b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Bad: a\x00b\r\nConnection: close\r\n\r\n")
        check(inj.startswith(b"HTTP/1.1 400"), "NUL in header rejected 400", inj[:60].decode(errors="replace"))

        # 11. non-ASCII (obs-text) header byte rejected
        na_hdr = c.request(b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Bad: \xc3\xa9\r\nConnection: close\r\n\r\n")
        check(na_hdr.startswith(b"HTTP/1.1 400"), "non-ASCII header rejected 400", na_hdr[:60].decode(errors="replace"))

        # 12. unsupported transfer-encoding
        te = c.request(b"POST /x HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: gzip\r\nConnection: close\r\n\r\n")
        check(te.startswith(b"HTTP/1.1 501"), "unsupported TE -> 501", te[:60].decode(errors="replace"))

        # 13. unsupported Expect -> 417 (and proxy stays alive)
        ex417 = c.request(b"POST /x HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nExpect: 200-ok\r\nConnection: close\r\n\r\n")
        check(ex417.startswith(b"HTTP/1.1 417"), "unsupported Expect -> 417", ex417[:60].decode(errors="replace"))

        # 14. Expect: 100-continue with huge CL and NO body must not crash the proxy
        s = socket.create_connection(("127.0.0.1", port), timeout=5)
        try:
            s.sendall(b"POST /big HTTP/1.1\r\nHost: localhost\r\nContent-Length: 9999999\r\nExpect: 100-continue\r\n\r\n")
            time.sleep(0.4)
        finally:
            s.close()
        check(proc.poll() is None, "proxy survives Expect+huge CL with no body")
        check(c.get("/api/items/2").startswith(b"HTTP/1.1 200"), "proxy still serves after Expect probe")

        # 15. Expect with a body: upstream must receive it exactly once
        c.request(
            b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 11\r\n"
            b"Expect: 100-continue\r\nConnection: close\r\n\r\nhello world"
        )
        check(up.last_body == b"hello world", "Expect body delivered exactly once", repr(up.last_body))

        # 16. untrusted forwarding headers are stripped (needs a non-loopback peer)
        if ext_ip:
            _wait_port(ext_port, host=ext_ip)
            ext = RawClient(ext_port, host=ext_ip)
            ext.get("/fwd", extra="X-Forwarded-Host: evil\r\nForwarded: host=evil\r\nX-Real-IP: 6.6.6.6\r\n")
            check("x-forwarded-host" not in up.last_headers, "X-Forwarded-Host stripped", repr(up.last_headers))
            check("forwarded" not in up.last_headers, "Forwarded stripped", repr(up.last_headers))
            check("x-real-ip" not in up.last_headers, "X-Real-IP stripped", repr(up.last_headers))
            check(up.last_headers.get("x-forwarded-for") == ext_ip, "XFF replaced with peer", repr(up.last_headers.get("x-forwarded-for")))
        else:
            print("[SKIP] no non-loopback interface: forwarding-header stripping not exercised")

        # 17. Vary on an unkeyed dimension disables caching
        c.get("/vary/a", extra="Accept-Language: en\r\n")
        c.get("/vary/a", extra="Accept-Language: fr\r\n")
        check(up.count("GET /vary/a") == 2, "Vary: Accept-Language not cached", f"count={up.count('GET /vary/a')}")

        # 18. absolute-form target rejected
        abs_form = c.request(b"GET http://evil/ HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        check(abs_form.startswith(b"HTTP/1.1 400"), "absolute-form target rejected", abs_form[:60].decode(errors="replace"))

        # 19. cache key is derived from the raw target: non-canonical paths get
        # their own entry and never serve another URL's cached response
        rp1 = c.get("/a/../b")
        rp2 = c.get("/b")
        rp3 = c.get("/a/../b")
        check(up.count("GET /a/../b") == 1, "non-canonical path cached under its own key", f"count={up.count('GET /a/../b')}")
        check(b'"/a/../b"' in rp1, "first non-canonical response correct", rp1[-60:].decode(errors="replace"))
        check(b'"/b"' in rp2 and b'"/a/../b"' not in rp2, "distinct target not served other's cache", rp2[-60:].decode(errors="replace"))
        check(b'"/a/../b"' in rp3, "non-canonical cache hit returns own body", rp3[-60:].decode(errors="replace"))

        # 20. huge Content-Length rejected (overflow guard)
        huge = c.request(b"POST /x HTTP/1.1\r\nHost: localhost\r\nContent-Length: 18446744073709551617\r\nConnection: close\r\n\r\n")
        check(huge.startswith((b"HTTP/1.1 400", b"HTTP/1.1 413")), "huge Content-Length rejected", huge[:60].decode(errors="replace"))

        # 21. non-ASCII target not cached
        c.request("GET /café HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".encode())
        c.request("GET /café HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".encode())
        check(up.count("GET /caf\u00e9") >= 2, "non-ASCII path not cached")

        # 22. HEAD served
        h = c.request(b"HEAD /api/items/1 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        check(h.startswith(b"HTTP/1.1 200"), "HEAD 200", h[:60].decode(errors="replace"))

    finally:
        for p in procs:
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


def _wait_port(port: int, timeout: float = 5.0, host: str = "127.0.0.1") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)


def _nonloopback_ip() -> str:
    """Best-effort local non-loopback IPv4, or '' when none is available."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("192.0.2.1", 9))  # TEST-NET-1, no packets actually sent
        ip = s.getsockname()[0]
    except OSError:
        return ""
    finally:
        s.close()
    if ip.startswith("127."):
        return ""
    return ip


if __name__ == "__main__":
    sys.exit(main())
