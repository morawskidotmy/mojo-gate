"""ASGI Middleware and helpers for applications running behind Mojo Gate."""

from __future__ import annotations

import hmac
import json
import os
import urllib.parse
import urllib.request
from collections.abc import Callable, Coroutine
from typing import Any

# Pure ASGI types (avoids hard dependency on starlette)
Scope = dict[str, Any]
Receive = Callable[[], Coroutine[Any, Any, dict[str, Any]]]
Send = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]
ASGIApp = Callable[[Scope, Receive, Send], Coroutine[Any, Any, None]]


def is_front_proxy_active() -> bool:
    """Return True if the current application process is running behind Mojo Gate."""
    return os.environ.get("MOJO_GATE_FRONT_PROXY") == "1"


def purge_cache(
    gate_url: str = "http://127.0.0.1:8000", purge_path: str = "/_mojo_gate/purge"
) -> bool:
    """Send a purge request to the Mojo Gate front proxy to immediately flush its cache.

    When running behind the front proxy the internal token is read from
    ``MOJO_GATE_INTERNAL_TOKEN`` and sent so the proxy authorizes the purge.
    """
    try:
        req = urllib.request.Request(f"{gate_url.rstrip('/')}{purge_path}", method="POST")
        token = os.environ.get("MOJO_GATE_INTERNAL_TOKEN", "").strip()
        if token:
            req.add_header("X-Mojo-Gate-Token", token)
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            return resp.status == 200
    except OSError:
        return False


class MojoGateMiddleware:
    """Starlette/FastAPI ASGI middleware for Mojo Gate front proxy integration.

    Features:
    - Automatically handles the internal analytics endpoint (e.g. `/_mojo_gate/analytics`)
      called by the Mojo front proxy for cache hits.
    - Decodes hits and calls `on_analytics(hits)` where hits is a list of:
      `[raw_path, raw_query, client_ip, user_agent]`.
    - Marks `request.state.is_front_proxy = True` for upstream requests.
    """

    def __init__(
        self,
        app: ASGIApp,
        analytics_endpoint: str = "/_mojo_gate/analytics",
        on_analytics: Callable[[list[tuple[str, str, str, str]]], Any] | None = None,
    ) -> None:
        self.app = app
        self.analytics_endpoint = analytics_endpoint
        self.on_analytics = on_analytics

    def _is_loopback_proxy_call(self, scope: Scope) -> bool:
        """Verify the request is a direct authenticated loopback call from Mojo Gate."""
        if not is_front_proxy_active():
            return False
        client = scope.get("client")
        if not client or client[0] not in ("127.0.0.1", "::1", "localhost"):
            return False
        headers = dict(scope.get("headers", []))
        expected_token = os.environ.get("MOJO_GATE_INTERNAL_TOKEN", "").strip().encode("ascii")
        if expected_token:
            token = headers.get(b"x-mojo-gate-token", b"")
            return hmac.compare_digest(token, expected_token)
        # Fallback if no token configured: disallow forwarded headers
        return b"x-forwarded-for" not in headers

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        method = scope.get("method", "GET")

        # Handle batched cache-hit analytics from front proxy
        if (
            path == self.analytics_endpoint
            and method == "POST"
            and self._is_loopback_proxy_call(scope)
        ):
            try:
                body_parts: list[bytes] = []
                while True:
                    message = await receive()
                    body_parts.append(message.get("body", b""))
                    if not message.get("more_body", False):
                        break
                body = b"".join(body_parts)
                data = json.loads(body.decode("utf-8")) if body else []
                if self.on_analytics and isinstance(data, list):
                    res = self.on_analytics(data)
                    if isinstance(res, Coroutine):
                        await res
                await send({
                    "type": "http.response.start",
                    "status": 204,
                    "headers": [(b"content-length", b"0")],
                })
                await send({
                    "type": "http.response.body",
                    "body": b"",
                })
            except Exception:
                err_body = b'{"detail":"invalid analytics payload"}'
                await send({
                    "type": "http.response.start",
                    "status": 400,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", str(len(err_body)).encode("ascii")),
                    ],
                })
                await send({
                    "type": "http.response.body",
                    "body": err_body,
                })
            return

        # Add flag to request state
        scope.setdefault("state", {})
        scope["state"]["is_front_proxy"] = is_front_proxy_active()

        await self.app(scope, receive, send)
