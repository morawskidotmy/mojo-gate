# AI Agent Implementation Guide for `mojo-gate`

This document provides instructions for AI coding agents integrating `mojo-gate` into existing or new Python services (FastAPI, Starlette, Litestar, or any ASGI/Uvicorn application).

---

## 1. What is `mojo-gate`?

`mojo-gate` is a drop-in middleware/front proxy runner that places a native compiled **Mojo** epoll reverse proxy in front of an internal **Uvicorn** worker.
- **Cache Hits**: Safe `GET` and `HEAD` requests matching cache criteria are served directly by native code in microseconds without waking Python's `asyncio` event loop.
- **Write Requests**: Non-safe HTTP methods (`POST`, `PUT`, `DELETE`, `PATCH`) automatically invalidate/flush the cache and pass through transparently.
- **Fallback**: If the Mojo toolchain or native binary is not available (e.g., non-Linux environments or missing Mojo compiler), it automatically falls back to standard Uvicorn execution with zero downtime or code modification.

---

## 2. Installation One-Liners

Add `mojo-gate` as a git dependency to the project:

### Using `uv` (Recommended)
```bash
uv add git+https://github.com/morawskidotmy/mojo-gate.git
```

### Using `pip`
```bash
pip install git+https://github.com/morawskidotmy/mojo-gate.git
```

### In `pyproject.toml`
```toml
[project]
dependencies = [
    "mojo-gate @ git+https://github.com/morawskidotmy/mojo-gate.git",
]
```

---

## 3. Implementation Patterns

### Pattern A: CLI / Container / Entrypoint (Zero Code Changes)
If the application is launched via a CLI command in a Dockerfile, `Procfile`, or systemd unit:

**Before:**
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

**After:**
```bash
mojo-gate app.main:app --host 0.0.0.0 --port 8000 --upstream-port 8003 --cache-ttl 60
```

### Pattern B: Programmatic Startup Script
If the application starts via `if __name__ == "__main__": uvicorn.run(...)`:

**Before:**
```python
import uvicorn

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000)
```

**After:**
```python
import mojo_gate

if __name__ == "__main__":
    mojo_gate.serve(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        upstream_port=8003,
        cache_ttl=60,
    )
```

### Pattern C: Adding Cache-Hit Analytics (Optional)
If you want the Python app to receive batched telemetry about cache hits served by the native proxy:

```python
from fastapi import FastAPI
from mojo_gate import MojoGateMiddleware, is_front_proxy_active, purge_cache

app = FastAPI()

app.add_middleware(
    MojoGateMiddleware,
    analytics_endpoint="/_mojo_gate/analytics",
    on_analytics=lambda hits: print(f"Received {len(hits)} cache hits from Mojo proxy"),
)
```

---

## 4. Key Configuration Parameters

| Parameter | Default | Purpose |
|---|---|---|
| `port` | `8000` | The public-facing port the Mojo proxy listens on |
| `upstream_port` | `port + 3` | The internal loopback port Uvicorn listens on |
| `cache_ttl` | `60` | Time-to-live (seconds) for cached safe `GET`/`HEAD` responses |
| `rate_rules` | `None` | List of `(path_prefix, limit, window_seconds)` tuples for per-IP rate limiting |
| `no_cache_prefixes`| `["/_mojo_gate"]`| URL prefixes to exclude from native caching |
| `purge_endpoint` | `/_mojo_gate/purge`| Internal endpoint to trigger on-demand cache flush |

---

## 5. Verification Checklist for Agents

After implementing `mojo-gate`:
1. Verify the service boots normally (`curl http://localhost:8000/health` or target endpoint).
2. Confirm repeated `GET` requests return `server: mojo-gate` (or cached headers) with fast response times.
3. Test a `POST` or write request to confirm automatic cache invalidation works as expected.
4. Verify graceful fallback if `mojo` is not present (the app will still start cleanly on the specified port via pure Uvicorn).
