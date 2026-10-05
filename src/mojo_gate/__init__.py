"""Mojo Gate: High-performance reverse proxy and caching front proxy for Uvicorn and ASGI apps."""

from mojo_gate.compiler import ensure_binary, find_mojo_compiler, get_binary_path, get_source_path
from mojo_gate.config import MojoGateConfig
from mojo_gate.front import serve
from mojo_gate.middleware import MojoGateMiddleware, is_front_proxy_active, purge_cache

__version__ = "0.1.0"
__all__ = [
    "MojoGateConfig",
    "MojoGateMiddleware",
    "ensure_binary",
    "find_mojo_compiler",
    "get_binary_path",
    "get_source_path",
    "is_front_proxy_active",
    "purge_cache",
    "serve",
]
