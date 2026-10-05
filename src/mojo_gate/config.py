"""Configuration for Mojo Gate."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class MojoGateConfig:
    """Configuration options for the Mojo Gate proxy and runner."""

    # Network binding
    host: str = "127.0.0.1"
    port: int = 8000
    upstream_host: str = "127.0.0.1"
    upstream_port: int | None = None

    # Caching options
    cache_ttl: int = 60
    cache_max_bytes: int = 256 * 1024 * 1024
    entry_max_bytes: int = 8 * 1024 * 1024
    no_cache_prefixes: list[str] = field(default_factory=lambda: ["/_mojo_gate"])

    # Rate limiting options
    rate_limit: bool = True
    rate_rules: list[tuple[str, int, int]] = field(default_factory=list)
    rate_limit_msg: str = '{"detail":"Too Many Requests","retry_after":{retry}}'

    # Analytics and Purge endpoints
    analytics_endpoint: str = ""
    purge_endpoint: str = "/_mojo_gate/purge"

    # Behavior & Supervision
    fallback_to_uvicorn: bool = True
    reload: bool = False
    log_level: str = "info"

    def effective_upstream_port(self) -> int:
        """Resolve upstream port, defaulting to port + 3 if not explicitly set."""
        if self.upstream_port is not None:
            return self.upstream_port
        return self.port + 3 if self.port < 65530 else self.port - 100

    def rate_rules_string(self) -> str:
        """Serialize rate rules to prefix:limit:window format."""
        if not self.rate_rules:
            return ""
        return ",".join(f"{prefix}:{limit}:{win}" for prefix, limit, win in self.rate_rules)

    def no_cache_prefixes_string(self) -> str:
        """Serialize cache bypass prefixes to comma-separated string."""
        return ",".join(p.strip() for p in self.no_cache_prefixes if p.strip())

    @classmethod
    def from_env(cls, **overrides) -> MojoGateConfig:
        """Load configuration with environment variable fallbacks."""
        kwargs = {}
        if "MOJO_GATE_HOST" in os.environ:
            kwargs["host"] = os.environ["MOJO_GATE_HOST"]
        if "MOJO_GATE_PORT" in os.environ:
            kwargs["port"] = int(os.environ["MOJO_GATE_PORT"])
        if "MOJO_GATE_UPSTREAM_PORT" in os.environ:
            kwargs["upstream_port"] = int(os.environ["MOJO_GATE_UPSTREAM_PORT"])
        if "MOJO_GATE_CACHE_TTL" in os.environ:
            kwargs["cache_ttl"] = int(os.environ["MOJO_GATE_CACHE_TTL"])
        if "MOJO_GATE_NO_RATE_LIMIT" in os.environ:
            kwargs["rate_limit"] = os.environ["MOJO_GATE_NO_RATE_LIMIT"] not in (
                "1",
                "true",
                "True",
            )
        kwargs.update(overrides)
        return cls(**kwargs)
