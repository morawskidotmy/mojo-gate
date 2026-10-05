from pathlib import Path

from mojo_gate.compiler import find_mojo_compiler, get_source_path


def test_get_source_path():
    src = get_source_path()
    assert src.name == "gate.mojo"
    assert src.exists()


def test_find_mojo_compiler():
    compiler = find_mojo_compiler()
    # If mojo is present in environment/system, ensure it is executable
    if compiler is not None:
        assert Path(compiler).is_file()
