from __future__ import annotations

import hashlib
from pathlib import Path

_CHUNK = 1024 * 1024


def hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(_CHUNK)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def hash_directory(path: Path) -> str:
    """Merkle-style hash: hash each contained file, then hash the sorted list.

    The digest changes whenever any file under `path` is added, removed, or modified.
    Symlinks are hashed by their target string, not followed.
    """
    entries: list[tuple[str, str]] = []
    for p in sorted(path.rglob("*")):
        rel = p.relative_to(path).as_posix()
        if p.is_symlink():
            entries.append((rel, "L:" + str(p.readlink())))
        elif p.is_file():
            entries.append((rel, "F:" + hash_file(p)))
        elif p.is_dir():
            entries.append((rel, "D:"))
    h = hashlib.sha256()
    for rel, digest in entries:
        h.update(rel.encode("utf-8"))
        h.update(b"\0")
        h.update(digest.encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()


def hash_path(path: Path) -> str:
    if path.is_dir() and not path.is_symlink():
        return hash_directory(path)
    return hash_file(path)
