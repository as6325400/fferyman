from __future__ import annotations

from fferyman.core.fsops import copy_path, next_available_name


def test_next_available_name_file(tmp_path):
    dup_dir = tmp_path / "duplicate"
    assert next_available_name(dup_dir, "a", ".txt") == dup_dir / "a_2.txt"
    dup_dir.mkdir()
    (dup_dir / "a_2.txt").write_text("x")
    assert next_available_name(dup_dir, "a", ".txt") == dup_dir / "a_3.txt"


def test_next_available_name_dir(tmp_path):
    dup_dir = tmp_path / "duplicate"
    assert next_available_name(dup_dir, "bundle") == dup_dir / "bundle_2"


def test_copy_file(tmp_path):
    src = tmp_path / "s.txt"
    src.write_text("hi")
    dst = tmp_path / "out" / "s.txt"
    copy_path(src, dst)
    assert dst.read_text() == "hi"


def test_copy_directory(tmp_path):
    src = tmp_path / "bundle"
    (src / "inner").mkdir(parents=True)
    (src / "inner" / "f.txt").write_text("1")
    dst = tmp_path / "out" / "bundle"
    copy_path(src, dst)
    assert (dst / "inner" / "f.txt").read_text() == "1"
