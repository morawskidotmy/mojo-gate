"""Run any Uvicorn application behind the Mojo front proxy (``src/mojo/gate.mojo``).

The front proxy architecture:
- The Mojo proxy listens on the public port (``--port``) and the Python app on
  an internal upstream port (``--upstream-port``) with ``MOJO_GATE_FRONT_PROXY=1``.
- The proxy enforces rate limits, serves cached responses for safe GET/HEAD requests,
  and reports cache-hit analytics back to the app via loopback.
- Responses are the app's own, so this changes speed, not behaviour.
- If the proxy cannot be used (no Mojo toolchain, build failure, etc.), falls back
  to running the app directly alone.
"""

from __future__ import annotations

import contextlib
import ctypes
import os
import secrets
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from typing import Any

import uvicorn

from mojo_gate.compiler import ensure_binary

_PR_SET_PDEATHSIG = 1


def _log(msg: str) -> None:
    print(f"[mojo-front] {msg}", file=sys.stderr, flush=True)


def _die_with_parent() -> None:
    """Child-side: get SIGTERM if parent process dies so the proxy never holds port alone."""
    with contextlib.suppress(OSError):
        ctypes.CDLL("libc.so.6", use_errno=True).prctl(_PR_SET_PDEATHSIG, signal.SIGTERM)


def _wait_healthy(host: str, port: int, timeout: float = 30.0, health_path: str = "") -> bool:
    """Poll the upstream port until it is accepting connections and answering."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if health_path:
            try:
                url = f"http://{host}:{port}{health_path}"
                with urllib.request.urlopen(url, timeout=2):
                    return True
            except OSError:
                time.sleep(0.25)
        else:
            try:
                with socket.create_connection((host, port), timeout=1.0):
                    return True
            except OSError:
                time.sleep(0.2)
    return False


def serve(
    app: str | Any,
    *,
    port: int = 8000,
    host: str = "127.0.0.1",
    upstream_port: int | None = None,
    upstream_host: str = "127.0.0.1",
    cache_ttl: int = 60,
    cache_max_bytes: int = 256 * 1024 * 1024,
    rate_limit: bool = True,
    rate_rules: list[tuple[str, int, int]] | None = None,
    no_cache_prefixes: list[str] | None = None,
    analytics_endpoint: str = "",
    purge_endpoint: str = "/_mojo_gate/purge",
    fallback_to_uvicorn: bool = True,
    reload: bool = False,
    log_level: str = "info",
    health_path: str = "",
    **uvicorn_kwargs: Any,
) -> bool:
    """Run `app` on `upstream_port` behind the Mojo front proxy on `port`.

    Returns True if run with front proxy, or False if fallen back to running
    the app alone.
    """
    if reload:
        _log("reload enabled: serving the Python app alone without front proxy")
        uvicorn.run(app, host=host, port=port, reload=True, log_level=log_level, **uvicorn_kwargs)
        return False

    up_port = (
        upstream_port if upstream_port is not None else (port + 3 if port < 65530 else port - 100)
    )
    binary = ensure_binary()

    if binary is None:
        if fallback_to_uvicorn:
            _log("front proxy unavailable (no compiled binary): serving the Python app alone")
            uvicorn.run(
                app, host=host, port=port, reload=False, log_level=log_level, **uvicorn_kwargs
            )
            return False
        raise RuntimeError(
            "Mojo front proxy binary is unavailable and fallback_to_uvicorn is False"
        )

    # Inform the app it sits behind the Mojo front proxy
    internal_token = secrets.token_hex(32)
    os.environ["MOJO_GATE_FRONT_PROXY"] = "1"
    os.environ["MOJO_GATE_PORT"] = str(port)
    os.environ["MOJO_GATE_UPSTREAM_PORT"] = str(up_port)
    os.environ["MOJO_GATE_INTERNAL_TOKEN"] = internal_token

    # Build proxy command line arguments
    cmd = [
        str(binary),
        "--host",
        host,
        "--port",
        str(port),
        "--upstream-host",
        upstream_host,
        "--upstream-port",
        str(up_port),
        "--cache-ttl",
        str(cache_ttl),
        "--cache-max-bytes",
        str(cache_max_bytes),
        "--purge-endpoint",
        purge_endpoint,
        "--internal-token",
        internal_token,
    ]
    if not rate_limit:
        cmd.append("--no-rate-limit")
    elif rate_rules:
        rules_str = ",".join(f"{p}:{lim}:{win}" for p, lim, win in rate_rules)
        cmd.extend(["--rate-rules", rules_str])

    if no_cache_prefixes:
        cmd.extend(["--no-cache-prefixes", ",".join(no_cache_prefixes)])

    if analytics_endpoint:
        cmd.extend(["--analytics-endpoint", analytics_endpoint])

    # Configure upstream Uvicorn server on internal loopback port
    config = uvicorn.Config(
        app,
        host=upstream_host,
        port=up_port,
        log_level=log_level,
        **uvicorn_kwargs,
    )
    server = uvicorn.Server(config)
    proxy_proc: list[subprocess.Popen] = []

    def supervise() -> None:
        # Wait until the upstream app answers
        if not _wait_healthy(upstream_host, up_port, timeout=30.0, health_path=health_path):
            _log("upstream app did not become healthy; stopping")
            server.should_exit = True
            return
        if server.should_exit:
            return

        proc = subprocess.Popen(cmd, preexec_fn=_die_with_parent)
        proxy_proc.append(proc)
        _log(f"proxy pid {proc.pid} on {host}:{port} -> app {upstream_host}:{up_port}")

        while proc.poll() is None and not server.should_exit:
            time.sleep(0.5)

        if not server.should_exit:
            _log(f"proxy exited ({proc.returncode}); stopping the app too")
            server.should_exit = True

    watcher = threading.Thread(target=supervise, name="mojo-gate-supervisor", daemon=True)
    watcher.start()

    try:
        server.run()
    finally:
        if proxy_proc and proxy_proc[0].poll() is None:
            _log("stopping front proxy...")
            proxy_proc[0].terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                proxy_proc[0].wait(timeout=2)
            if proxy_proc[0].poll() is None:
                proxy_proc[0].kill()

    return True
