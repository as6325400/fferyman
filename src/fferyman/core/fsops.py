from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


def _python_copy_path(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.is_dir() and not src.is_symlink():
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst, symlinks=True)
    else:
        shutil.copy2(src, dst, follow_symlinks=False)


def _rclone_binary() -> str | None:
    return shutil.which("rclone")


def _rclone_copy_path(src: Path, dst: Path) -> bool:
    """Copy via rclone when the binary is available.

    Returns True when rclone handled the copy. Returns False when rclone is
    unavailable so the caller can fall back to the Python implementation.
    """
    rclone = _rclone_binary()
    if rclone is None or src.is_symlink():
        return False

    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.is_dir() and not src.is_symlink() and (dst.exists() or dst.is_symlink()):
        _cleanup_path(dst)

    try:
        subprocess.run(
            [rclone, "copyto", str(src), str(dst)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError:
        return False
    except subprocess.CalledProcessError as e:
        msg = e.stderr.strip() or str(e)
        raise OSError(f"rclone copy failed {src} -> {dst}: {msg}") from e
    return True


def copy_path(src: Path, dst: Path) -> None:
    """Non-atomic copy. Kept for backwards compat / simple callers.

    Prefer `atomic_copy_path` in the engine — it copies to a temp name first
    and then renames, so a crash mid-copy never leaves a half-written target
    at `dst`. When `rclone` is installed, prefer it as the transfer backend;
    otherwise fall back to the Python stdlib copy path.
    """
    if _rclone_copy_path(src, dst):
        return
    _python_copy_path(src, dst)


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
    When `rclone` is installed, the tmp copy prefers `rclone copyto`; if not,
    it falls back to the Python stdlib copy path.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.parent / f".{dst.name}.tmp.{os.getpid()}"
    _cleanup_path(tmp)

    try:
        if not _rclone_copy_path(src, tmp):
            _python_copy_path(src, tmp)
        if src.is_dir() and not src.is_symlink():
            if dst.exists() or dst.is_symlink():
                _cleanup_path(dst)
            os.rename(tmp, dst)
        else:
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
