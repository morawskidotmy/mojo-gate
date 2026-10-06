import json
import pytest
from mojo_gate.middleware import MojoGateMiddleware, is_front_proxy_active, purge_cache


def test_is_front_proxy_active(monkeypatch):
    monkeypatch.delenv("MOJO_GATE_FRONT_PROXY", raising=False)
    assert not is_front_proxy_active()

    monkeypatch.setenv("MOJO_GATE_FRONT_PROXY", "1")
    assert is_front_proxy_active()


class _FakeResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_purge_cache_sends_internal_token(monkeypatch):
    monkeypatch.setenv("MOJO_GATE_INTERNAL_TOKEN", "abc123")
    captured: dict[str, str] = {}

    def fake_urlopen(req, timeout=None):
        captured.update({k.lower(): v for k, v in req.header_items()})
        return _FakeResponse()

    monkeypatch.setattr("mojo_gate.middleware.urllib.request.urlopen", fake_urlopen)
    assert purge_cache("http://127.0.0.1:9999") is True
    assert captured.get("x-mojo-gate-token") == "abc123"


def test_purge_cache_without_token_omits_header(monkeypatch):
    monkeypatch.delenv("MOJO_GATE_INTERNAL_TOKEN", raising=False)
    captured: dict[str, str] = {}

    def fake_urlopen(req, timeout=None):
        captured.update({k.lower(): v for k, v in req.header_items()})
        return _FakeResponse()

    monkeypatch.setattr("mojo_gate.middleware.urllib.request.urlopen", fake_urlopen)
    assert purge_cache("http://127.0.0.1:9999") is True
    assert "x-mojo-gate-token" not in captured


@pytest.mark.anyio
async def test_middleware_normal_request(monkeypatch):
    monkeypatch.setenv("MOJO_GATE_FRONT_PROXY", "1")

    async def dummy_app(scope, receive, send):
        assert scope["state"]["is_front_proxy"] is True
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/plain")],
        })
        await send({
            "type": "http.response.body",
            "body": b"ok",
        })

    middleware = MojoGateMiddleware(dummy_app)
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/items",
        "headers": [],
        "client": ("192.168.1.5", 54321),
    }

    messages = []
    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(msg):
        messages.append(msg)

    await middleware(scope, receive, send)
    assert messages[0]["status"] == 200


@pytest.mark.anyio
async def test_middleware_analytics_endpoint(monkeypatch):
    monkeypatch.setenv("MOJO_GATE_FRONT_PROXY", "1")
    received_hits = []

    def on_hits(hits):
        received_hits.extend(hits)

    async def dummy_app(scope, receive, send):
        pytest.fail("dummy_app should not be reached for internal analytics")

    middleware = MojoGateMiddleware(dummy_app, on_analytics=on_hits)
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/_mojo_gate/analytics",
        "headers": [(b"host", b"127.0.0.1")],
        "client": ("127.0.0.1", 12345),
    }

    hits_payload = [["/items", "", "1.2.3.4", "curl/7.88"]]
    body_bytes = json.dumps(hits_payload).encode("utf-8")

    async def receive():
        return {"type": "http.request", "body": body_bytes, "more_body": False}

    messages = []
    async def send(msg):
        messages.append(msg)

    await middleware(scope, receive, send)
    assert messages[0]["status"] == 204
    assert received_hits == hits_payload


@pytest.mark.anyio
async def test_middleware_analytics_rejects_external_xff(monkeypatch):
    monkeypatch.setenv("MOJO_GATE_FRONT_PROXY", "1")

    reached_app = False
    async def dummy_app(scope, receive, send):
        nonlocal reached_app
        reached_app = True
        await send({"type": "http.response.start", "status": 404, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = MojoGateMiddleware(dummy_app)
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/_mojo_gate/analytics",
        "headers": [(b"x-forwarded-for", b"203.0.113.195")],
        "client": ("127.0.0.1", 12345),
    }

    async def receive():
        return {"type": "http.request", "body": b"[]", "more_body": False}

    messages = []
    async def send(msg):
        messages.append(msg)

    await middleware(scope, receive, send)
    assert reached_app is True

