"""Mojo compiler discovery and binary building for Mojo Gate."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


def _log(msg: str) -> None:
    print(f"[mojo-gate] {msg}", file=sys.stderr, flush=True)


def find_mojo_compiler() -> str | None:
    """Find the Mojo compiler executable in environment, PATH, or virtualenvs."""
    candidates = [
        os.environ.get("MOJO"),
        shutil.which("mojo"),
        str(Path.home() / ".mojo-venv" / "bin" / "mojo"),
        str(Path.home() / ".pixi" / "bin" / "mojo"),
        str(Path.home() / ".pixi" / "envs" / "mojo" / "bin" / "mojo"),
        str(Path(sys.prefix) / "bin" / "mojo"),
    ]
    # Check parent project virtualenv if exists
    cwd_venv_mojo = Path.cwd() / ".venv" / "bin" / "mojo"
    if cwd_venv_mojo.exists():
        candidates.append(str(cwd_venv_mojo))

    for candidate in candidates:
        if candidate and os.access(candidate, os.X_OK):
            return candidate
    return None


def get_source_path() -> Path:
    """Path to the Mojo proxy source file (gate.mojo)."""
    # 1. Look in package directory
    pkg_mojo = Path(__file__).parent.parent / "mojo" / "gate.mojo"
    if pkg_mojo.exists():
        return pkg_mojo
    # 2. Look relative to repo root
    repo_mojo = Path(__file__).parent.parent.parent / "src" / "mojo" / "gate.mojo"
    if repo_mojo.exists():
        return repo_mojo
    # 3. Fallback to bundled
    return pkg_mojo


def get_binary_path() -> Path:
    """Path to the compiled gate binary."""
    cache_dir = Path.home() / ".cache" / "mojo_gate"
    env_cache = os.environ.get("MOJO_GATE_BUILD_DIR")
    if env_cache:
        cache_dir = Path(env_cache)
    return cache_dir / "gate_mojo"


def ensure_binary(source_path: Path | None = None, target_path: Path | None = None) -> Path | None:
    """Return the proxy binary, (re)building it when the source is newer; None if impossible."""
    source = source_path or get_source_path()
    binary = target_path or get_binary_path()

    if not source.exists():
        _log(f"source file not found: {source}")
        return None

    if binary.exists() and binary.stat().st_mtime >= source.stat().st_mtime:
        return binary

    compiler = find_mojo_compiler()
    if compiler is None:
        _log("no mojo compiler found (set MOJO=... or install it into ~/.mojo-venv or pixi)")
        return binary if binary.exists() else None

    binary.parent.mkdir(parents=True, exist_ok=True)
    tmp = binary.with_suffix(".tmp")
    _log(f"building {binary} from {source} ...")

    proc = subprocess.run(
        [compiler, "build", "-O3", str(source), "-o", str(tmp)],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        _log("build failed:\n" + proc.stderr[-2000:])
        return binary if binary.exists() else None

    # Atomic move
    tmp.chmod(0o755)
    tmp.replace(binary)
    _log(f"build successful: {binary}")
    return binary
