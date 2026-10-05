from mojo_gate.middleware import is_front_proxy_active


def test_is_front_proxy_active(monkeypatch):
    monkeypatch.delenv("MOJO_GATE_FRONT_PROXY", raising=False)
    assert not is_front_proxy_active()

    monkeypatch.setenv("MOJO_GATE_FRONT_PROXY", "1")
    assert is_front_proxy_active()
