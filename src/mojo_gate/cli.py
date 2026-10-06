"""Command-line interface for Mojo Gate."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from mojo_gate.compiler import ensure_binary, find_mojo_compiler, get_binary_path, get_source_path
from mojo_gate.front import serve


def parse_rate_rule(val: str) -> tuple[str, int, int]:
    """Parse a rate rule string like /api:100:60."""
    parts = val.split(":")
    if len(parts) == 3:
        return parts[0], int(parts[1]), int(parts[2])
    if len(parts) == 2:
        return parts[0], int(parts[1]), 60
    raise argparse.ArgumentTypeError(f"Invalid rate rule '{val}'. Expected prefix:limit[:window_s]")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="mojo-gate",
        description="Mojo Gate: High-performance reverse proxy & caching front proxy for Uvicorn.",
    )
    subparsers = parser.add_subparsers(dest="subcommand")

    # Command: build
    p_build = subparsers.add_parser("build", help="Precompile the Mojo front proxy binary")
    p_build.add_argument("--source", type=Path, default=None, help="Path to gate.mojo")
    p_build.add_argument("--output", type=Path, default=None, help="Target path for binary")

    # Command: info
    subparsers.add_parser("info", help="Display Mojo compiler and binary status")

    # Default / Command: serve
    p_serve = subparsers.add_parser("serve", help="Serve a Python ASGI app behind Mojo Gate")
    p_serve.add_argument("app", help="Application import string, e.g. 'main:app'")
    p_serve.add_argument(
        "--host", default=None, help="Public bind address (default: 127.0.0.1)"
    )
    p_serve.add_argument(
        "--port", type=int, default=None, help="Public port to listen on (default: 8000)"
    )
    p_serve.add_argument(
        "--upstream-host", default=None, help="Upstream host (default: 127.0.0.1)"
    )
    p_serve.add_argument(
        "--upstream-port", type=int, default=None, help="Upstream app port (default: port + 3)"
    )
    p_serve.add_argument(
        "--cache-ttl", type=int, default=None, help="Cache TTL in seconds (default: 60)"
    )
    p_serve.add_argument(
        "--cache-max-bytes",
        type=int,
        default=None,
        help="Max cache bytes (default: 256MB)",
    )
    p_serve.add_argument("--no-rate-limit", action="store_true", help="Disable rate limiting")
    p_serve.add_argument(
        "--entry-max-bytes",
        type=int,
        default=None,
        help="Max size of a single cached response (default: 8MB)",
    )
    p_serve.add_argument(
        "--rate-limit-msg",
        default=None,
        help="JSON body returned on 429 ({retry} placeholder supported)",
    )
    p_serve.add_argument(
        "--server-header",
        default=None,
        help="`server:` header value on proxy-generated responses (default: mojo-gate)",
    )
    p_serve.add_argument(
        "--idle-timeout",
        type=int,
        default=None,
        help="Idle client/upstream timeout in seconds (default: 30)",
    )
    p_serve.add_argument(
        "--rate-rule",
        action="append",
        type=parse_rate_rule,
        dest="rate_rules",
        help="Rate limit rule 'prefix:limit:window_s' (repeatable)",
    )
    p_serve.add_argument(
        "--no-cache-prefix",
        action="append",
        dest="no_cache_prefixes",
        help="Path prefix to exclude from caching (repeatable)",
    )
    p_serve.add_argument(
        "--analytics-endpoint", default=None, help="Upstream endpoint to report cache hits"
    )
    p_serve.add_argument(
        "--purge-endpoint", default=None, help="Endpoint to purge cache via POST"
    )
    p_serve.add_argument(
        "--env-file", default=None, help="Path to a .env file with MOJO_GATE_* settings"
    )
    p_serve.add_argument(
        "--reload", action="store_true", help="Enable auto-reload (bypasses front proxy)"
    )
    p_serve.add_argument(
        "--no-mojo", action="store_true", help="Bypass Mojo front proxy and run Uvicorn alone"
    )
    p_serve.add_argument("--log-level", default="info", help="Uvicorn log level (default: info)")

    # Support shorthand: mojo-gate main:app [options]
    args_list = sys.argv[1:] if argv is None else argv
    if args_list and args_list[0] not in ("build", "info", "serve", "-h", "--help"):
        args_list = ["serve", *args_list]

    args = parser.parse_args(args_list)

    if args.subcommand == "info":
        compiler = find_mojo_compiler()
        source = get_source_path()
        binary = get_binary_path()
        print(f"Mojo compiler: {compiler or 'Not found'}")
        print(f"Source file:   {source} ({'exists' if source.exists() else 'missing'})")
        print(f"Binary path:   {binary} ({'compiled' if binary.exists() else 'not compiled'})")
        return 0

    if args.subcommand == "build":
        bin_path = ensure_binary(source_path=args.source, target_path=args.output)
        if bin_path:
            print(f"Successfully built Mojo Gate binary: {bin_path}")
            return 0
        print("Failed to build Mojo Gate binary.", file=sys.stderr)
        return 1

    if args.subcommand == "serve":
        if args.no_mojo:
            import uvicorn

            uvicorn.run(
                args.app,
                host=args.host,
                port=args.port,
                reload=args.reload,
                log_level=args.log_level,
            )
            return 0

        serve(
            args.app,
            host=args.host,
            port=args.port,
            upstream_host=args.upstream_host,
            upstream_port=args.upstream_port,
            cache_ttl=args.cache_ttl,
            cache_max_bytes=args.cache_max_bytes,
            entry_max_bytes=args.entry_max_bytes,
            rate_limit=False if args.no_rate_limit else None,
            rate_rules=args.rate_rules,
            rate_limit_msg=args.rate_limit_msg,
            no_cache_prefixes=args.no_cache_prefixes,
            analytics_endpoint=args.analytics_endpoint,
            purge_endpoint=args.purge_endpoint,
            server_header=args.server_header,
            idle_timeout=args.idle_timeout,
            env_file=args.env_file,
            reload=args.reload,
            log_level=args.log_level,
        )
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
