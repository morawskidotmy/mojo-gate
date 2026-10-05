# Mojo Gate

A high-performance reverse proxy and caching front proxy written in **Mojo**, designed to sit directly in front of **Uvicorn** and Python ASGI applications (FastAPI, Starlette, Litestar, etc.).

Extracted and modularized from [`search.matugen`](https://github.com/matura-lol) so any Python web project can use it as a drop-in front proxy.

---

## Why Mojo Gate?

Python ASGI servers like Uvicorn are great for application logic, but handling high request concurrency, per-IP sliding-window rate limiting, and caching deterministic responses in Python incurs Python interpreter overhead and event-loop lag.

**Mojo Gate** sits in front of Uvicorn on loopback (`127.0.0.1`):
1. **Zero-Overhead Epoll Event Loop**: Written in Mojo using Linux `epoll` with non-blocking sockets and `TCP_NODELAY`.
2. **In-Memory Micro-Caching**: Safe `GET` and `HEAD` responses are cached directly in Mojo's memory (configurable TTL, default 60s). Cached responses are replayed in microseconds with freshly computed RFC 7231 `Date` headers.
3. **Instant Cache Invalidation**: Any write request (`POST`, `PUT`, `DELETE`, `PATCH`) passing through the proxy immediately flushes the cache, ensuring data is never stale. Also supports explicit cache purging via `POST /_mojo_gate/purge`.
4. **Transparent Keep-Alive Proxying**: Relays requests to upstream Uvicorn with `Connection: close` while keeping client keep-alive connections alive.
5. **Per-IP Rate Limiting**: Token-bucket sliding window rate limiting based on `X-Forwarded-For` client IPs. Shielding your Python app by answering `429 Too Many Requests` directly in native code.
6. **Batched Analytics Reporting**: Because cached requests never hit Uvicorn, Mojo Gate batches cache hits and reports them periodically to your app via a loopback endpoint (e.g. `/_mojo_gate/analytics`).
7. **Full Protocol Tunneling**: Full bidirectional pass-through for WebSockets (`Upgrade`), Server-Sent Events (SSE), and chunked uploads.

---

## Architecture

```
Client Requests
      │
      ▼
┌────────────────────────────────────────┐
│     Mojo Gate Proxy (:8080)            │
│  - Epoll socket loop                   │
│  - Sliding-window rate limit (429)     │
│  - In-memory cache (GET/HEAD hits)     │
│  - Protocol tunneling (WebSockets)     │
└───────────────────┬────────────────────┘
                    │ (Misses, Writes & Batched Analytics)
                    ▼
┌────────────────────────────────────────┐
│     Uvicorn / FastAPI App (:8083)      │
│  - Pure application business logic     │
│  - MOJO_GATE_FRONT_PROXY=1             │
└────────────────────────────────────────┘
```

---

## Installation

Install via pip:

```bash
cd mojo-gate
pip install .
```

*Prerequisite*: Mojo compiler (installed via Modular or `pixi` or in virtualenv).

---

## Quickstart

### 1. In your Python Code

Use `mojo_gate.serve` instead of `uvicorn.run`:

```python
from fastapi import FastAPI
import mojo_gate

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello from behind Mojo Gate!"}

if __name__ == "__main__":
    mojo_gate.serve(
        "main:app",
        port=8080,                # Public port Mojo Gate listens on
        upstream_port=8083,       # Internal port Uvicorn runs on
        rate_rules=[("/api", 100, 60)], # 100 req/min for /api
        cache_ttl=60,             # Cache safe GET responses for 60s
    )
```

### 2. From the Command Line

Run your ASGI application behind Mojo Gate using the CLI:

```bash
mojo-gate main:app --port 8080 --upstream-port 8083 --cache-ttl 60
```

### 3. Starlette / FastAPI Middleware (Optional)

Receive cache-hit analytics batches and detect front-proxy status:

```python
from fastapi import FastAPI
from mojo_gate import MojoGateMiddleware, is_front_proxy_active

app = FastAPI()

app.add_middleware(
    MojoGateMiddleware,
    analytics_endpoint="/_mojo_gate/analytics",
    on_analytics=lambda hits: print(f"Recorded {len(hits)} cache hits from Mojo Gate!"),
)

@app.get("/")
def index():
    return {"front_proxy": is_front_proxy_active()}
```

---

## Configuration Reference

| Parameter / CLI Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | Public host IP to bind the Mojo proxy |
| `--port` | `8000` | Public port to bind the Mojo proxy |
| `--upstream-host` | `127.0.0.1` | Internal host where Uvicorn runs |
| `--upstream-port` | `port + 3` | Internal port where Uvicorn runs |
| `--cache-ttl` | `60` | Cache time-to-live in seconds for safe GET/HEAD requests |
| `--cache-max-bytes`| `268435456` (256MB) | Maximum in-memory cache capacity |
| `--rate-rule` | None | Rate rule formatted as `prefix:limit:window_s` |
| `--no-rate-limit` | `False` | Disable native rate limiting |
| `--purge-endpoint` | `/_mojo_gate/purge` | Endpoint to instantly flush cache via `POST` |
| `--no-mojo` | `False` | Bypass Mojo Gate and run Uvicorn alone |
| `--reload` | `False` | Enable auto-reload (automatically runs Uvicorn alone) |

---

## License

MIT
