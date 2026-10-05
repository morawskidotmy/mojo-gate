"""Example FastAPI application served behind Mojo Gate.

Usage:
    # 1. From Python:
    python examples/app.py

    # 2. Or using the CLI:
    mojo-gate examples.app:app --port 8080 --upstream-port 8083
"""

from fastapi import FastAPI

from mojo_gate import MojoGateMiddleware, is_front_proxy_active, serve

app = FastAPI(title="Mojo Gate Demo App")

# Optional: Add middleware to capture cache-hit analytics
app.add_middleware(
    MojoGateMiddleware,
    analytics_endpoint="/_mojo_gate/analytics",
    on_analytics=lambda hits: print(f"[analytics] Received {len(hits)} cache hits from Mojo Gate!"),
)


@app.get("/")
def home():
    return {
        "message": "Hello from behind Mojo Gate!",
        "front_proxy_active": is_front_proxy_active(),
    }


@app.get("/api/items/{item_id}")
def get_item(item_id: int):
    # This deterministic GET response will be cached by Mojo Gate for 60s
    return {"item_id": item_id, "name": f"Item {item_id}"}


@app.post("/api/items")
def create_item(name: str):
    # Non-GET requests automatically flush the Mojo Gate response cache
    return {"status": "created", "name": name}


if __name__ == "__main__":
    # Start the app behind the Mojo front proxy
    serve(
        "examples.app:app",
        port=8080,
        upstream_port=8083,
        rate_rules=[("/api", 100, 60)],
    )
