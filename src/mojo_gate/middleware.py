"""ASGI Middleware and helpers for applications running behind Mojo Gate."""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from collections.abc import Callable, Coroutine
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send


def is_front_proxy_active() -> bool:
    """Return True if the current application process is running behind Mojo Gate."""
    return os.environ.get("MOJO_GATE_FRONT_PROXY") == "1"


def purge_cache(
    gate_url: str = "http://127.0.0.1:8000", purge_path: str = "/_mojo_gate/purge"
) -> bool:
    """Send a purge request to the Mojo Gate front proxy to immediately flush its cache."""
    try:
        req = urllib.request.Request(f"{gate_url.rstrip('/')}{purge_path}", method="POST")
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

    def _is_loopback_proxy_call(self, request: Request) -> bool:
        """Verify the request is a direct loopback call from Mojo Gate."""
        return (
            is_front_proxy_active()
            and request.client is not None
            and request.client.host in ("127.0.0.1", "::1", "localhost")
            and "x-forwarded-for" not in request.headers
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        path = request.url.path

        # Handle batched cache-hit analytics from front proxy
        if (
            path == self.analytics_endpoint
            and request.method == "POST"
            and self._is_loopback_proxy_call(request)
        ):
            try:
                body = await request.body()
                data = json.loads(body)
                if self.on_analytics and isinstance(data, list):
                    # Call synchronous or asynchronous handler
                    res = self.on_analytics(data)
                    if isinstance(res, Coroutine):
                        await res
                response = Response(status_code=204)
            except Exception:
                response = JSONResponse({"detail": "invalid analytics payload"}, status_code=400)
            await response(scope, receive, send)
            return

        # Add flag to request state
        scope.setdefault("state", {})
        scope["state"]["is_front_proxy"] = is_front_proxy_active()

        await self.app(scope, receive, send)
