import os

import pytest

from mojo_gate.config import MojoGateConfig, load_dotenv, parse_dotenv


@pytest.fixture(autouse=True)
def _restore_environ():
    snapshot = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(snapshot)


def test_default_config():
    cfg = MojoGateConfig(port=8080)
    assert cfg.port == 8080
    assert cfg.effective_upstream_port() == 8083
    assert cfg.cache_ttl == 60
    assert cfg.rate_limit is True
    assert cfg.server_header == "mojo-gate"
    assert cfg.idle_timeout == 30


def test_custom_upstream_port():
    cfg = MojoGateConfig(port=8080, upstream_port=9000)
    assert cfg.effective_upstream_port() == 9000


def test_rate_rules_serialization():
    cfg = MojoGateConfig(rate_rules=[("/api", 100, 60), ("/search", 20, 10)])
    assert cfg.rate_rules_string() == "/api:100:60,/search:20:10"


def test_no_cache_prefixes_serialization():
    cfg = MojoGateConfig(no_cache_prefixes=["/_mojo_gate", "/mcp"])
    assert cfg.no_cache_prefixes_string() == "/_mojo_gate,/mcp"


def test_parse_dotenv_forms():
    text = (
        "# a comment\n"
        "\n"
        "MOJO_GATE_PORT=8095\n"
        "export MOJO_GATE_HOST=0.0.0.0\n"
        "MOJO_GATE_SERVER_HEADER=uvicorn   # trailing comment\n"
        'MOJO_GATE_RATE_LIMIT_MSG={"detail":"zbyt wiele"}\n'
        "MOJO_GATE_QUOTED='a b c'\n"
        "not a pair\n"
    )
    parsed = parse_dotenv(text)
    assert parsed["MOJO_GATE_PORT"] == "8095"
    assert parsed["MOJO_GATE_HOST"] == "0.0.0.0"
    assert parsed["MOJO_GATE_SERVER_HEADER"] == "uvicorn"
    assert parsed["MOJO_GATE_RATE_LIMIT_MSG"] == '{"detail":"zbyt wiele"}'
    assert parsed["MOJO_GATE_QUOTED"] == "a b c"
    assert "not a pair" not in parsed


def test_load_dotenv_does_not_override(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("MOJO_GATE_PORT=8095\nMOJO_GATE_SERVER_HEADER=uvicorn\n")
    monkeypatch.setenv("MOJO_GATE_PORT", "9999")
    values = load_dotenv(env_file)
    assert values["MOJO_GATE_PORT"] == "8095"
    assert os.environ["MOJO_GATE_PORT"] == "9999"  # existing env wins
    assert os.environ["MOJO_GATE_SERVER_HEADER"] == "uvicorn"


def test_load_dotenv_missing_file(tmp_path):
    assert load_dotenv(tmp_path / "nope.env") == {}


def test_from_env_file(tmp_path, monkeypatch):
    for key in (
        "MOJO_GATE_PORT",
        "MOJO_GATE_CACHE_TTL",
        "MOJO_GATE_RATE_RULES",
        "MOJO_GATE_NO_CACHE_PREFIXES",
        "MOJO_GATE_SERVER_HEADER",
        "MOJO_GATE_RATE_LIMIT",
        "MOJO_GATE_ENTRY_MAX_BYTES",
    ):
        monkeypatch.delenv(key, raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "MOJO_GATE_PORT=8095\n"
        "MOJO_GATE_CACHE_TTL=120\n"
        "MOJO_GATE_RATE_RULES=/api/search:90:60,/source:240:60\n"
        "MOJO_GATE_NO_CACHE_PREFIXES=/_mojo_gate,/question/,/pdf\n"
        "MOJO_GATE_SERVER_HEADER=uvicorn\n"
        "MOJO_GATE_RATE_LIMIT=false\n"
        "MOJO_GATE_ENTRY_MAX_BYTES=4194304\n"
    )
    cfg = MojoGateConfig.from_env(env_file=env_file)
    assert cfg.port == 8095
    assert cfg.cache_ttl == 120
    assert cfg.rate_rules == [("/api/search", 90, 60), ("/source", 240, 60)]
    assert cfg.no_cache_prefixes == ["/_mojo_gate", "/question/", "/pdf"]
    assert cfg.server_header == "uvicorn"
    assert cfg.rate_limit is False
    assert cfg.entry_max_bytes == 4194304


def test_from_env_explicit_override_wins(monkeypatch, tmp_path):
    monkeypatch.delenv("MOJO_GATE_PORT", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("MOJO_GATE_PORT=8095\n")
    cfg = MojoGateConfig.from_env(env_file=env_file, port=7777)
    assert cfg.port == 7777


@pytest.mark.parametrize(
    ("value", "expected"),
    [("1", False), ("true", False), ("yes", False), ("0", True), ("false", True)],
)
def test_no_rate_limit_env(monkeypatch, value, expected):
    monkeypatch.setenv("MOJO_GATE_NO_RATE_LIMIT", value)
    monkeypatch.delenv("MOJO_GATE_RATE_LIMIT", raising=False)
    assert MojoGateConfig.from_env().rate_limit is expected


def test_serve_reads_dotenv(monkeypatch, tmp_path):
    import mojo_gate.front as front

    for key in ("MOJO_GATE_PORT", "MOJO_GATE_HOST", "MOJO_GATE_ENV_FILE"):
        monkeypatch.delenv(key, raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("MOJO_GATE_PORT=8095\nMOJO_GATE_HOST=0.0.0.0\n")

    captured: dict = {}
    monkeypatch.setattr(front, "ensure_binary", lambda: None)
    monkeypatch.setattr(front.uvicorn, "run", lambda _app, **kw: captured.update(kw))

    result = front.serve("app:app", env_file=str(env_file))
    assert result is False
    assert captured["port"] == 8095
    assert captured["host"] == "0.0.0.0"


def test_serve_explicit_arg_beats_dotenv(monkeypatch, tmp_path):
    import mojo_gate.front as front

    for key in ("MOJO_GATE_PORT",):
        monkeypatch.delenv(key, raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("MOJO_GATE_PORT=8095\n")

    captured: dict = {}
    monkeypatch.setattr(front, "ensure_binary", lambda: None)
    monkeypatch.setattr(front.uvicorn, "run", lambda _app, **kw: captured.update(kw))

    front.serve("app:app", port=1234, env_file=str(env_file))
    assert captured["port"] == 1234
