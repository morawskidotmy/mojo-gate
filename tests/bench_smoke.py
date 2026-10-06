"""Micro-benchmark harness for the native Mojo gate proxy.

Starts a raw upstream + the real binary and measures cache-hit throughput and
latency over persistent keep-alive connections. Run directly:

    python tests/bench_smoke.py [--threads 8] [--requests 3000]

The absolute numbers depend on the Python load generator; the harness is meant
for before/after comparison across native changes.
"""

from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from e2e_smoke import Upstream, _free_port, _wait_port

from mojo_gate.compiler import ensure_binary


def _read_response(sock: socket.socket, buf: bytearray) -> bytes:
    """Read one HTTP response (content-length framed) from a persistent socket."""
    while True:
        head_end = buf.find(b"\r\n\r\n")
        if head_end != -1:
            cl = None
            for line in bytes(buf[:head_end]).split(b"\r\n")[1:]:
                if line.lower().startswith(b"content-length:"):
                    cl = int(line.split(b":", 1)[1].strip())
            if cl is not None and len(buf) - (head_end + 4) >= cl:
                return bytes(buf[: head_end + 4 + cl])
        chunk = sock.recv(65536)
        if not chunk:
            return bytes(buf)
        buf += chunk


def _cpu_seconds(pid: int) -> float:
    """utime+stime of a process in seconds (from /proc/<pid>/stat)."""
    try:
        parts = Path(f"/proc/{pid}/stat").read_text().split()
        ticks = int(parts[13]) + int(parts[14])
        return ticks / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError):
        return 0.0


def _worker(port: int, path: str, count: int, latencies: list[float], errors: list[int]) -> None:
    req = f"GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n".encode()
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=10)
        s.settimeout(10)
    except OSError:
        errors.append(1)
        return
    try:
        buf = bytearray()
        for _ in range(count):
            t0 = time.perf_counter()
            s.sendall(req)
            buf = bytearray(_read_response(s, buf))
            latencies.append(time.perf_counter() - t0)
    except OSError:
        errors.append(1)
    finally:
        s.close()


def run(threads: int, requests: int, path: str = "/api/items/1", binary: str | None = None) -> dict:
    binary = ensure_binary() if binary is None else Path(binary)
    if binary is None:
        print("SKIP: no compiled binary")
        return {}

    up = Upstream()
    port = _free_port()
    proxy_cmd = [
        str(binary), "--host", "127.0.0.1", "--port", str(port),
        "--upstream-host", "127.0.0.1", "--upstream-port", str(up.port),
        "--cache-ttl", "300", "--no-rate-limit",
    ]
    # Pin the proxy to one core so its CPU time is measurable despite the
    # (GIL-bound) Python load generator.
    if shutil.which("taskset"):
        proxy_cmd = ["taskset", "-c", "0", *proxy_cmd]
    proc = subprocess.Popen(
        proxy_cmd,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    try:
        _wait_port(port)
        # Warm the cache and confirm it is populated.
        w = socket.create_connection(("127.0.0.1", port), timeout=5)
        w.sendall(f"GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".encode())
        w.recv(65536)
        w.close()
        time.sleep(0.3)

        latencies: list[float] = []
        errors: list[int] = []
        per_thread = requests // threads
        workers = [
            threading.Thread(target=_worker, args=(port, path, per_thread, latencies, errors))
            for _ in range(threads)
        ]
        cpu0 = _cpu_seconds(proc.pid)
        t0 = time.perf_counter()
        for w in workers:
            w.start()
        for w in workers:
            w.join()
        elapsed = time.perf_counter() - t0
        cpu1 = _cpu_seconds(proc.pid)
        total = len(latencies)
        latencies.sort()
        result = {
            "requests": total,
            "elapsed_s": elapsed,
            "req_per_s": total / elapsed if elapsed else 0.0,
            "p50_ms": latencies[len(latencies) // 2] * 1000 if latencies else 0.0,
            "p99_ms": latencies[int(len(latencies) * 0.99)] * 1000 if latencies else 0.0,
            "errors": len(errors),
            "cpu_s": cpu1 - cpu0,
            "cpu_us_per_req": ((cpu1 - cpu0) / total) * 1e6 if total else 0.0,
            "cpu_pct": ((cpu1 - cpu0) / elapsed) * 100 if elapsed else 0.0,
        }
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        up.stop()
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--requests", type=int, default=4000)
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--binary", default=None, help="Path to a prebuilt gate binary")
    args = ap.parse_args()
    best = None
    for i in range(args.repeat):
        r = run(args.threads, args.requests, binary=args.binary)
        if not r:
            return 2
        print(
            f"run {i}: {r['req_per_s']:,.0f} req/s  "
            f"p50={r['p50_ms']:.3f}ms p99={r['p99_ms']:.3f}ms  "
            f"cpu={r['cpu_us_per_req']:.2f}us/req ({r['cpu_pct']:.0f}% core)  "
            f"({r['requests']} req, {r['errors']} err)"
        )
        if best is None or r["cpu_us_per_req"] < best["cpu_us_per_req"]:
            best = r
    if best:
        print(
            f"BEST: {best['req_per_s']:,.0f} req/s  "
            f"{best['cpu_us_per_req']:.2f}us/req proxy CPU"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
