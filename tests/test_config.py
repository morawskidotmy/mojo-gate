from mojo_gate.config import MojoGateConfig


def test_default_config():
    cfg = MojoGateConfig(port=8080)
    assert cfg.port == 8080
    assert cfg.effective_upstream_port() == 8083
    assert cfg.cache_ttl == 60
    assert cfg.rate_limit is True


def test_custom_upstream_port():
    cfg = MojoGateConfig(port=8080, upstream_port=9000)
    assert cfg.effective_upstream_port() == 9000


def test_rate_rules_serialization():
    cfg = MojoGateConfig(rate_rules=[("/api", 100, 60), ("/search", 20, 10)])
    assert cfg.rate_rules_string() == "/api:100:60,/search:20:10"


def test_no_cache_prefixes_serialization():
    cfg = MojoGateConfig(no_cache_prefixes=["/_mojo_gate", "/mcp"])
    assert cfg.no_cache_prefixes_string() == "/_mojo_gate,/mcp"
