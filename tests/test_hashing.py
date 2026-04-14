from __future__ import annotations

from fferyman.core.hashing import hash_directory, hash_file


def test_hash_file_differs(tmp_path):
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.write_text("A")
    b.write_text("B")
    assert hash_file(a) != hash_file(b)


def test_hash_directory_notices_changes(tmp_path):
    d = tmp_path / "d"
    d.mkdir()
    (d / "f").write_text("1")
    h1 = hash_directory(d)
    (d / "f").write_text("2")
    h2 = hash_directory(d)
    assert h1 != h2
    (d / "f").write_text("1")
    h3 = hash_directory(d)
    assert h1 == h3


def test_hash_directory_notices_new_file(tmp_path):
    d = tmp_path / "d"
    d.mkdir()
    (d / "f").write_text("1")
    h1 = hash_directory(d)
    (d / "g").write_text("2")
    h2 = hash_directory(d)
    assert h1 != h2
