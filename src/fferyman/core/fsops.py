from __future__ import annotations

import os
import shutil
from pathlib import Path


def copy_path(src: Path, dst: Path) -> None:
    """Non-atomic copy. Kept for backwards compat / simple callers.

    Prefer `atomic_copy_path` in the engine — it copies to a temp name first
    and then renames, so a crash mid-copy never leaves a half-written target
    at `dst`.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.is_dir() and not src.is_symlink():
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst, symlinks=True)
    else:
        shutil.copy2(src, dst, follow_symlinks=False)


def atomic_copy_path(src: Path, dst: Path) -> None:
    """Copy `src` → `dst` via tmp-then-rename, so `dst` never appears in a
    half-written state.

    For files: copy to `<dst>.tmp.<pid>`, then `os.replace` (atomic on POSIX
    within the same filesystem).

    For directories: copytree to `<dst>.tmp.<pid>/`, then rmtree old `dst`
    and `os.rename` the tmp into place. The final swap is not strictly
    atomic (rmtree + rename is two steps), but at any point either the
    complete old tree or the complete new tree exists at `dst` — never a
    half-copied tree.

    If any step fails, the tmp is cleaned up and the exception is re-raised.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.parent / f".{dst.name}.tmp.{os.getpid()}"
    _cleanup_path(tmp)

    try:
        if src.is_dir() and not src.is_symlink():
            shutil.copytree(src, tmp, symlinks=True)
            if dst.exists() or dst.is_symlink():
                _cleanup_path(dst)
            os.rename(tmp, dst)
        else:
            shutil.copy2(src, tmp, follow_symlinks=False)
            os.replace(tmp, dst)
    except Exception:
        _cleanup_path(tmp)
        raise


def _cleanup_path(p: Path) -> None:
    try:
        if p.is_symlink() or p.is_file():
            p.unlink()
        elif p.is_dir():
            shutil.rmtree(p)
    except FileNotFoundError:
        pass
    except OSError:
        # Best-effort cleanup; do not mask the original error.
        pass


def next_available_name(parent: Path, stem: str, suffix: str = "") -> Path:
    """Return the first path `parent/{stem}_N{suffix}` that doesn't exist,
    starting at N=2. Pure disambiguation helper — no policy about where
    `parent` should be.
    """
    n = 2
    while True:
        candidate = parent / f"{stem}_{n}{suffix}"
        if not candidate.exists():
            return candidate
        n += 1
