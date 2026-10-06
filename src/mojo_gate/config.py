"""Configuration for Mojo Gate.

Configuration can come from three places, in order of precedence:

1. Explicit arguments (e.g. ``mojo_gate.serve(...)`` keyword arguments).
2. Environment variables (``MOJO_GATE_*``).
3. A ``.env`` file (loaded by :func:`load_dotenv`; only ``MOJO_GATE_*`` keys
   are consumed).

Loading a ``.env`` file never overrides variables already present in the
environment, so real environment variables and CI secrets always win.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

__all__ = ["MojoGateConfig", "load_dotenv", "parse_dotenv"]


def parse_dotenv(text: str) -> dict[str, str]:
    """Parse ``.env`` text into a dict.

    Supports ``#`` comments (whole-line and trailing on unquoted values),
    blank lines, an optional ``export `` prefix, and single/double quoted
    values. This is intentionally small and dependency-free.
    """
    result: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        key, sep, value = line.partition("=")
        if not sep:
            continue
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()
        result[key] = value
    return result


def load_dotenv(path: str | os.PathLike[str] = ".env", *, override: bool = False) -> dict[str, str]:
    """Load a ``.env`` file into ``os.environ``.

    Returns the parsed values. Existing environment variables are preserved
    unless ``override`` is true. A missing file is not an error.
    """
    p = Path(path)
    if not p.is_file():
        return {}
    values = parse_dotenv(p.read_text(encoding="utf-8"))
    for key, value in values.items():
        if override or key not in os.environ:
            os.environ[key] = value
    return values


def _parse_rate_rules(value: str) -> list[tuple[str, int, int]]:
    rules: list[tuple[str, int, int]] = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) == 3:
            prefix, limit, window = parts[0].strip(), parts[1].strip(), parts[2].strip()
            if prefix and limit.isdigit() and window.isdigit():
                rules.append((prefix, int(limit), int(window)))
        elif len(parts) == 2:
            prefix, limit = parts[0].strip(), parts[1].strip()
            if prefix and limit.isdigit():
                rules.append((prefix, int(limit), 60))
    return rules


def _parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


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

    # Proxy-generated responses
    server_header: str = "mojo-gate"
    idle_timeout: int = 30

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
    def from_env(
        cls, env_file: str | os.PathLike[str] | None = None, **overrides
    ) -> MojoGateConfig:
        """Build a config from ``MOJO_GATE_*`` environment variables.

        If ``env_file`` is given, that ``.env`` file is loaded first (existing
        environment variables are not overwritten). Explicit ``overrides`` win
        over both.
        """
        if env_file:
            load_dotenv(env_file)

        env = os.environ
        kwargs: dict[str, object] = {}
        str_map = {
            "MOJO_GATE_HOST": "host",
            "MOJO_GATE_UPSTREAM_HOST": "upstream_host",
            "MOJO_GATE_RATE_LIMIT_MSG": "rate_limit_msg",
            "MOJO_GATE_ANALYTICS_ENDPOINT": "analytics_endpoint",
            "MOJO_GATE_PURGE_ENDPOINT": "purge_endpoint",
            "MOJO_GATE_SERVER_HEADER": "server_header",
        }
        int_map = {
            "MOJO_GATE_PORT": "port",
            "MOJO_GATE_UPSTREAM_PORT": "upstream_port",
            "MOJO_GATE_CACHE_TTL": "cache_ttl",
            "MOJO_GATE_CACHE_MAX_BYTES": "cache_max_bytes",
            "MOJO_GATE_ENTRY_MAX_BYTES": "entry_max_bytes",
            "MOJO_GATE_IDLE_TIMEOUT": "idle_timeout",
        }
        for env_name, attr in str_map.items():
            if env_name in env:
                kwargs[attr] = env[env_name]
        for env_name, attr in int_map.items():
            if env_name in env:
                kwargs[attr] = int(env[env_name])
        if "MOJO_GATE_RATE_RULES" in env:
            kwargs["rate_rules"] = _parse_rate_rules(env["MOJO_GATE_RATE_RULES"])
        if "MOJO_GATE_NO_CACHE_PREFIXES" in env:
            kwargs["no_cache_prefixes"] = _parse_csv(env["MOJO_GATE_NO_CACHE_PREFIXES"])
        if "MOJO_GATE_RATE_LIMIT" in env:
            kwargs["rate_limit"] = env["MOJO_GATE_RATE_LIMIT"].strip().lower() not in (
                "0",
                "false",
                "no",
            )
        if "MOJO_GATE_NO_RATE_LIMIT" in env:
            kwargs["rate_limit"] = env["MOJO_GATE_NO_RATE_LIMIT"].strip().lower() not in (
                "1",
                "true",
                "yes",
            )
        kwargs.update(overrides)
        return cls(**kwargs)
