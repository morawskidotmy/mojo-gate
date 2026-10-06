<div align="center">

# ⛩️🔥 Mojo Gate

High-performance reverse proxy and caching front proxy written in **Mojo**, designed to run directly in front of **Uvicorn** and Python ASGI applications (FastAPI, Starlette, Litestar).

</div>

Mojo Gate offloads connection multiplexing, HTTP caching, and sliding-window rate limiting to native code using the Linux `epoll` system call, freeing Python worker event loops to focus strictly on dynamic application logic.

---

## Features

- **Epoll Event Loop**: Zero-overhead I/O multiplexing implemented directly in Mojo with non-blocking sockets and `TCP_NODELAY`.
- **In-Memory Micro-Caching**: Safe `GET` and `HEAD` responses are cached in native memory. Cached hits bypass Python completely and are served in microseconds with dynamically regenerated RFC 7231 `Date` headers.
- **Write-Invalidation & Purge API**: Any write request (`POST`, `PUT`, `DELETE`, `PATCH`) flushes the response cache automatically. Explicit cache purging is supported via `POST /_mojo_gate/purge` (authorized by the internal token when one is configured).
- **Transparent Keep-Alive Proxying**: Relays requests to upstream Uvicorn with `Connection: close` while maintaining persistent client keep-alive connections.
- **Per-IP Rate Limiting**: In-memory token-bucket sliding window rate limiting based on `X-Forwarded-For` client IPs, returning standard `429 Too Many Requests` responses with `Retry-After` headers.
- **Full Protocol Tunneling**: Native pass-through for WebSockets (`Upgrade`), Server-Sent Events (SSE), chunked uploads, and `Expect: 100-continue`.
- **Batched Analytics Reporting**: Hits served from cache never reach Python; Mojo Gate batches hit telemetry and posts it periodically to a loopback endpoint (e.g. `/_mojo_gate/analytics`).

---

## Performance Comparison

We tested Mojo Gate directly against a standard production deployment of [`tiangolo/full-stack-fastapi-template`](https://github.com/fastapi/full-stack-fastapi-template) (Python 3.13, FastAPI 0.142, Uvicorn 0.54) under concurrent keep-alive load on identical hardware:

### Benchmark Results (`full-stack-fastapi-template`)

| Metric | Pure Uvicorn (Alone) | Uvicorn + Mojo Gate | Difference |
|---|---|---|---|
| **Throughput (Requests/sec)** | 1,486.2 req/s | **51,321.8 req/s** | **+3,353% (34.53x faster)** |
| **Average Latency** | 33.57 ms | **0.95 ms** | **-97.2% lower latency** |
| **50th Percentile (p50)** | 33.01 ms | **0.89 ms** | **-97.3%** |
| **99th Percentile (p99)** | 54.36 ms | **2.11 ms** | **-96.1%** |
| **Python CPU / Event-Loop Load** | 100% active per request | Offloaded on cache hits | Substantial CPU reduction |
| **Write Request Handling** | Direct execution | Transparent pass-through & auto-flush | Identical application semantics |

> [!TIP]
> **Why the 34.53x speedup?** On deterministic endpoints (e.g., OpenAPI schemas, health checks, catalog items, static metadata), Mojo Gate serves the exact response bytes directly from native epoll memory without waking Python or touching the asyncio event loop.

---

## Installation

```bash
# uv
uv add git+https://github.com/morawskidotmy/mojo-gate.git

# pip
pip install git+https://github.com/morawskidotmy/mojo-gate.git
```

> [!NOTE]
> Compiling the native proxy binary requires the [Mojo SDK](https://docs.modular.com/mojo/) (detected automatically via `$MOJO`, PATH, `~/.mojo-venv`, or `~/.pixi`). If the compiler is not present, `mojo-gate` gracefully falls back to serving Uvicorn directly.

---

## Architecture

```
             Client Requests
                    │
                    ▼
┌────────────────────────────────────────┐
│        Mojo Gate Proxy (:8080)         │
│  - Non-blocking epoll loop             │
│  - Per-IP rate limiting (429)          │
│  - In-memory cache (GET/HEAD hits)     │
│  - Protocol tunneling (WebSockets)     │
└───────────────────┬────────────────────┘
                    │ (Cache misses, writes, & batched analytics)
                    ▼
┌────────────────────────────────────────┐
│     Uvicorn / FastAPI App (:8083)      │
│  - Pure application business logic     │
│  - MOJO_GATE_FRONT_PROXY=1             │
└────────────────────────────────────────┘
```

> [!NOTE]
> Mojo Gate sits in front of Uvicorn on loopback (`127.0.0.1`). If the Mojo compiler or native binary is unavailable, the supervisor automatically falls back to serving Uvicorn alone without breaking application startup.

---

## Quickstart

### 1. In Python Code

Replace `uvicorn.run(...)` with `mojo_gate.serve(...)`:

```python
from fastapi import FastAPI
import mojo_gate

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello from behind Mojo Gate!"}

@app.get("/api/items/{item_id}")
def get_item(item_id: int):
    # Deterministic GET responses are cached automatically for 60s
    return {"item_id": item_id, "name": f"Item {item_id}"}

@app.post("/api/items")
def create_item(name: str):
    # Non-GET requests automatically flush the cache
    return {"status": "created", "name": name}

if __name__ == "__main__":
    mojo_gate.serve(
        "main:app",
        port=8080,                # Public port Mojo Gate listens on
        upstream_port=8083,       # Internal port Uvicorn runs on
        rate_rules=[("/api", 100, 60)], # Rate limit: 100 req/60s per IP
        cache_ttl=60,             # Cache safe responses for 60 seconds
    )
```

### 2. Using the Command-Line Interface

Run any existing ASGI application directly from your terminal:

```bash
mojo-gate main:app --port 8080 --upstream-port 8083 --cache-ttl 60
```

To pre-compile the binary ahead of time:

```bash
mojo-gate build
```

To inspect compiler detection and binary status:

```bash
mojo-gate info
```

---

## Middleware & Analytics (Optional)

Add `MojoGateMiddleware` to your FastAPI / Starlette app to receive cache-hit analytics batches and check if the app is active behind the gate:

```python
from fastapi import FastAPI
from mojo_gate import MojoGateMiddleware, is_front_proxy_active, purge_cache

app = FastAPI()

app.add_middleware(
    MojoGateMiddleware,
    analytics_endpoint="/_mojo_gate/analytics",
    on_analytics=lambda hits: print(f"Logged {len(hits)} cache hits from Mojo Gate"),
)

@app.get("/")
def status():
    return {"front_proxy_active": is_front_proxy_active()}

@app.post("/admin/clear-cache")
def clear_cache():
    purged = purge_cache("http://127.0.0.1:8080")
    return {"purged": purged}
```

---

## Configuration Reference

| Parameter / CLI Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | Public host IP address to bind the Mojo proxy |
| `--port` | `8000` | Public port to bind the Mojo proxy |
| `--upstream-host` | `127.0.0.1` | Internal host where Uvicorn runs |
| `--upstream-port` | `port + 3` | Internal port where Uvicorn runs |
| `--cache-ttl` | `60` | Cache time-to-live in seconds for safe GET/HEAD requests |
| `--cache-max-bytes`| `268435456` (256MB) | Maximum total in-memory cache capacity |
| `--entry-max-bytes`| `8388608` (8MB) | Maximum size of an individual cached response |
| `--rate-rule` | None | Rate rule formatted as `prefix:limit:window_s` (repeatable) |
| `--rate-limit-msg` | `{"detail":"Too Many Requests","retry_after":{retry}}` | JSON body returned on `429` (`{retry}` placeholder supported) |
| `--no-rate-limit` | `False` | Disable native rate limiting |
| `--purge-endpoint` | `/_mojo_gate/purge` | Endpoint to instantly flush cache via `POST` or `DELETE` |
| `--server-header` | `mojo-gate` | `server:` header value on proxy-generated responses (set to your upstream's value for byte-identical parity) |
| `--idle-timeout` | `30` | Seconds before an idle client or stalled upstream connection is reaped |
| `--env-file` | `.env` | Path to a `.env` file with `MOJO_GATE_*` settings |
| `--no-mojo` | `False` | Bypass Mojo Gate and run Uvicorn alone |
| `--reload` | `False` | Enable auto-reload (automatically runs Uvicorn alone) |

---

## Configuration via `.env`

Instead of a long wall of CLI flags, put the settings in a `.env` file. When
`mojo_gate.serve(...)` runs it loads `.env` from the working directory (or the
path in `--env-file` / `$MOJO_GATE_ENV_FILE`). Only `MOJO_GATE_*` keys are
consumed, existing environment variables always win, and an explicitly passed
argument beats the `.env` value.

```dotenv
# .env
MOJO_GATE_HOST=127.0.0.1
MOJO_GATE_PORT=8095
MOJO_GATE_UPSTREAM_PORT=8098
MOJO_GATE_CACHE_TTL=120
MOJO_GATE_CACHE_MAX_BYTES=268435456
MOJO_GATE_ENTRY_MAX_BYTES=8388608
MOJO_GATE_RATE_RULES=/api/search:90:60,/source:240:60,/api:400:60
MOJO_GATE_RATE_LIMIT_MSG={"detail":"zbyt wiele żądań","retry_after":{retry}}
MOJO_GATE_NO_CACHE_PREFIXES=/_mojo_gate,/question/,/mcp,/source,/pdf
MOJO_GATE_ANALYTICS_ENDPOINT=/_mojo_gate/analytics
MOJO_GATE_PURGE_ENDPOINT=/_mojo_gate/purge
MOJO_GATE_SERVER_HEADER=uvicorn
MOJO_GATE_IDLE_TIMEOUT=30
MOJO_GATE_RATE_LIMIT=true
```

Then the application code is just:

```python
import mojo_gate

mojo_gate.serve("search_matugen.server:app", health_path="/api/health")
```

You can also load the file yourself and read the values:

```python
from mojo_gate import MojoGateConfig, load_dotenv

load_dotenv()                       # populates os.environ from .env
cfg = MojoGateConfig.from_env()     # typed config object
```

The equivalent CLI form is `mojo-gate serve app:app --env-file .env`.

---

## Forwarded headers & trust

Mojo Gate always sets `X-Forwarded-For` to the TCP peer address and, for
non-loopback peers, strips client-supplied `X-Forwarded-Host/Proto/Port/Server`,
`Forwarded`, `X-Real-IP`, `X-Client-IP`, `X-Original-URL` and `X-Rewrite-URL` so
the upstream cannot be tricked into trusting attacker-controlled identity.

`X-Forwarded-For` is only *chained* (appended) when the peer is loopback
(`127.0.0.1`/`::1`), i.e. when Mojo Gate sits behind a trusted local load
balancer / reverse proxy that sanitizes the header. If Mojo Gate is exposed
directly, clients are not loopback and their forwarded headers are replaced.
Make sure any local fronting proxy strips inbound `X-Forwarded-*` before
forwarding, or the app will see spoofed values.

