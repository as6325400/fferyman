from __future__ import annotations

from pathlib import Path

from fferyman.core import fsops
from fferyman.core.fsops import atomic_copy_path, copy_path, next_available_name


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


def test_copy_path_prefers_rclone_when_available(tmp_path, monkeypatch):
    src = tmp_path / "s.txt"
    src.write_text("hi")
    dst = tmp_path / "out" / "s.txt"
    calls: list[list[str]] = []

    monkeypatch.setattr(fsops, "_rclone_binary", lambda: "/usr/bin/rclone")

    def _fake_run(cmd, **_kwargs):
        calls.append(cmd)
        fsops._python_copy_path(Path(cmd[2]), Path(cmd[3]))

    monkeypatch.setattr(fsops.subprocess, "run", _fake_run)
    copy_path(src, dst)

    assert calls == [["/usr/bin/rclone", "copyto", str(src), str(dst)]]
    assert dst.read_text() == "hi"


def test_copy_path_falls_back_without_rclone(tmp_path, monkeypatch):
    src = tmp_path / "s.txt"
    src.write_text("hi")
    dst = tmp_path / "out" / "s.txt"

    monkeypatch.setattr(fsops, "_rclone_binary", lambda: None)

    def _should_not_run(*_args, **_kwargs):
        raise AssertionError("subprocess.run should not be called without rclone")

    monkeypatch.setattr(fsops.subprocess, "run", _should_not_run)
    copy_path(src, dst)
    assert dst.read_text() == "hi"


def test_atomic_copy_path_prefers_rclone_when_available(tmp_path, monkeypatch):
    src = tmp_path / "s.txt"
    src.write_text("hi")
    dst = tmp_path / "out" / "s.txt"
    calls: list[list[str]] = []

    monkeypatch.setattr(fsops, "_rclone_binary", lambda: "/usr/bin/rclone")
    monkeypatch.setattr(fsops.os, "getpid", lambda: 1234)

    def _fake_run(cmd, **_kwargs):
        calls.append(cmd)
        fsops._python_copy_path(Path(cmd[2]), Path(cmd[3]))

    monkeypatch.setattr(fsops.subprocess, "run", _fake_run)
    atomic_copy_path(src, dst)

    assert calls == [[
        "/usr/bin/rclone",
        "copyto",
        str(src),
        str(dst.parent / ".s.txt.tmp.1234"),
    ]]
    assert dst.read_text() == "hi"
    assert not (dst.parent / ".s.txt.tmp.1234").exists()
