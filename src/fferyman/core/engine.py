from __future__ import annotations

import fnmatch
import logging
import re
import shutil
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from fferyman.core.db import MappingStore
from fferyman.core.fsops import atomic_copy_path, next_available_name
from fferyman.core.hashing import hash_path
from fferyman.core.mapper import MapperSpec
from fferyman.core.policy import OnChange, OnConflict, OnDelete, Policy

log = logging.getLogger("fferyman.engine")


@dataclass
class _ParsedMode:
    kind: str                         # "file" | "dir" | "glob" | "regex" | "custom"
    depth: int | None = None
    glob: str | None = None
    regex: "re.Pattern[str] | None" = None


def parse_watch_mode(mode: str) -> _ParsedMode:
    if mode == "file":
        return _ParsedMode(kind="file")
    if mode == "custom":
        return _ParsedMode(kind="custom")
    if mode.startswith("dir:"):
        return _ParsedMode(kind="dir", depth=int(mode.split(":", 1)[1]))
    if mode.startswith("glob:"):
        return _ParsedMode(kind="glob", glob=mode.split(":", 1)[1])
    if mode.startswith("regex:"):
        pat = mode.split(":", 1)[1]
        try:
            compiled = re.compile(pat)
        except re.error as e:
            raise ValueError(f"invalid regex in watch_mode {mode!r}: {e}") from None
        return _ParsedMode(kind="regex", regex=compiled)
    raise ValueError(f"unknown watch_mode {mode!r}")


def _unit_root_for(
    path: Path, source: Path, parsed: _ParsedMode, is_dir: bool
) -> Path | None:
    try:
        rel = path.relative_to(source)
    except ValueError:
        return None
    parts = rel.parts
    if parsed.kind == "file":
        # `file` mode guarantees the mapper only ever sees regular files.
        # Directory events (DirCreated/DirModified/DirDeleted) are dropped
        # here so mappers don't have to defend against it.
        if is_dir:
            return None
        return path
    if parsed.kind == "custom":
        return path
    if parsed.kind == "glob":
        assert parsed.glob is not None
        if fnmatch.fnmatch(rel.as_posix(), parsed.glob) or fnmatch.fnmatch(
            path.name, parsed.glob
        ):
            return path
        return None
    if parsed.kind == "dir":
        depth = parsed.depth or 1
        if len(parts) < depth:
            return None
        return source.joinpath(*parts[:depth])
    if parsed.kind == "regex":
        assert parsed.regex is not None
        # Walk up from `path` until we find a **directory** ancestor whose
        # basename matches, stopping at `source`. Child events inside a
        # matched dir therefore bubble up to the dir itself. A file whose
        # name happens to match is NOT treated as a unit (regex mode is
        # directory-only per the docs).
        current = path
        current_is_dir = is_dir
        while current != source:
            if current_is_dir and parsed.regex.fullmatch(current.name):
                return current
            parent = current.parent
            if parent == current:
                return None  # reached filesystem root without hitting source
            current = parent
            current_is_dir = True  # parents in a filesystem are always directories
        return None
    return None


def iter_units(source: Path, mode: str) -> Iterator[Path]:
    parsed = parse_watch_mode(mode)
    if parsed.kind == "file":
        for p in sorted(source.rglob("*")):
            if p.is_file():
                yield p
        return
    if parsed.kind == "dir":
        depth = parsed.depth or 1

        def _walk(cur: Path, d: int) -> Iterator[Path]:
            if d == depth:
                if cur.is_dir():
                    yield cur
                return
            if not cur.is_dir():
                return
            for child in sorted(cur.iterdir()):
                if child.is_dir():
                    yield from _walk(child, d + 1)

        yield from _walk(source, 0)
        return
    if parsed.kind == "glob":
        assert parsed.glob is not None
        for p in sorted(source.rglob("*")):
            rel = p.relative_to(source).as_posix()
            if fnmatch.fnmatch(rel, parsed.glob) or fnmatch.fnmatch(p.name, parsed.glob):
                yield p
        return
    if parsed.kind == "custom":
        for p in sorted(source.rglob("*")):
            yield p
        return
    if parsed.kind == "regex":
        assert parsed.regex is not None

        def _walk_regex(cur: Path) -> Iterator[Path]:
            if not cur.is_dir():
                return
            for child in sorted(cur.iterdir()):
                # Regex mode matches **directories only**. A file whose name
                # also matches is skipped (and has no children to descend
                # into either).
                if child.is_dir() and parsed.regex.fullmatch(child.name):
                    yield child
                    # Don't descend into a matched unit. Nested matches are
                    # treated as contents of the outer unit.
                elif child.is_dir():
                    yield from _walk_regex(child)

        yield from _walk_regex(source)
        return


class _Debouncer:
    def __init__(self, delay: float, dispatch):
        self._delay = delay
        self._dispatch = dispatch
        self._lock = threading.Lock()
        self._timers: dict[Path, threading.Timer] = {}
        self._pending: dict[Path, tuple[str, Path | None]] = {}
        self._stopped = False

    def submit(self, unit_root: Path, kind: str, src_unit_root: Path | None) -> None:
        with self._lock:
            if self._stopped:
                return
            prev = self._pending.get(unit_root)
            if prev and prev[0] == "created" and kind == "modified":
                kind = "created"
            self._pending[unit_root] = (kind, src_unit_root)
            t = self._timers.get(unit_root)
            if t:
                t.cancel()
            new = threading.Timer(self._delay, self._fire, args=(unit_root,))
            new.daemon = True
            self._timers[unit_root] = new
            new.start()

    def _fire(self, unit_root: Path) -> None:
        with self._lock:
            entry = self._pending.pop(unit_root, None)
            self._timers.pop(unit_root, None)
        if entry is not None:
            kind, src_unit = entry
            try:
                self._dispatch(kind, unit_root, src_unit)
            except Exception:
                log.exception("dispatch failed for %s", unit_root)

    def stop(self) -> None:
        with self._lock:
            self._stopped = True
            for t in self._timers.values():
                t.cancel()
            self._timers.clear()
            self._pending.clear()


@dataclass
class Watch:
    name: str
    spec: MapperSpec
    source: Path
    dest: Path
    params: dict[str, Any]
    store: MappingStore
    policy: Policy = field(default_factory=Policy)
    logger: logging.Logger = field(default=None)  # type: ignore[assignment]
    debounce_seconds: float = 0.5

    _parsed: _ParsedMode = field(init=False)
    _debouncer: _Debouncer | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        if self.logger is None:
            self.logger = logging.getLogger(f"fferyman.{self.name}")
        self._parsed = parse_watch_mode(self.spec.watch_mode)
        # Debouncer is needed whenever a unit can receive multiple raw events
        # during a single logical change (directory fill-in). "dir:N" and
        # "regex:" both fit that pattern; "file"/"custom"/"glob" dispatch per
        # raw event and don't need coalescing.
        if self._parsed.kind in ("dir", "regex"):
            self._debouncer = _Debouncer(self.debounce_seconds, self.dispatch)

    # ---- identity / fingerprint ----

    def fingerprint(self) -> str:
        return f"{self.spec.name}@{self.spec.revision};{self.policy.fingerprint()}"

    # ---- public API ----

    def scan_once(
        self,
        wrap: Callable[[list[Path]], Iterable[Path]] | None = None,
    ) -> None:
        """Ingest every unit under `source`.

        `wrap` is an optional function that receives the materialised list of
        units and returns an iterable to actually iterate. Use this for
        progress bars, e.g.:

            watch.scan_once(wrap=lambda us: tqdm(us, desc=watch.name))

        Passing `wrap=None` streams units without materialising the list
        (cheaper memory, no total known).
        """
        if wrap is None:
            for unit in iter_units(self.source, self.spec.watch_mode):
                self._ingest(unit)
            return
        units = list(iter_units(self.source, self.spec.watch_mode))
        for unit in wrap(units):
            self._ingest(unit)

    def reconcile(
        self,
        wrap: Callable[[list[Path]], Iterable[Path]] | None = None,
    ) -> None:
        """Full re-sync. Call after changing policy or algorithm revision.

        1. Re-ingests every unit under source (mapper decides canonical target,
           engine applies current policy; sources whose fingerprint already
           matches are skipped).
        2. Applies on_delete to any tracked source whose file has disappeared.
        """
        self.scan_once(wrap=wrap)
        for m in self.store.list_active():
            src = Path(m.source_path)
            if not src.exists():
                self._remove(src)

    def dispatch(self, kind: str, unit_root: Path, src_unit_root: Path | None) -> None:
        try:
            if kind == "deleted":
                # The raw event may have been for a child inside a dir-mode unit
                # (e.g. `source/X/f.txt` deleted under watch_mode="dir:1"). If
                # the unit itself still exists, this is a modification, not a
                # deletion of the whole unit.
                if unit_root.exists():
                    self._ingest(unit_root)
                else:
                    self._remove(unit_root)
            elif kind == "moved":
                # Move across a unit boundary inside source: old unit should be
                # treated as removed only if the unit itself is actually gone.
                if src_unit_root is not None and not src_unit_root.exists():
                    self._remove(src_unit_root)
                elif src_unit_root is not None and src_unit_root != unit_root:
                    self._ingest(src_unit_root)
                if unit_root.exists():
                    self._ingest(unit_root)
            else:  # created | modified
                if unit_root.exists():
                    self._ingest(unit_root)
        except Exception:
            log.exception("watch %s failed on %s", self.name, unit_root)

    def submit(self, kind: str, unit_root: Path, src_unit_root: Path | None) -> None:
        if self._debouncer is not None:
            self._debouncer.submit(unit_root, kind, src_unit_root)
        else:
            self.dispatch(kind, unit_root, src_unit_root)

    def stop(self) -> None:
        if self._debouncer is not None:
            self._debouncer.stop()

    # ---- core ingestion ----

    def _ingest(self, src: Path) -> None:
        if not src.exists():
            return
        try:
            content_hash = hash_path(src)
        except FileNotFoundError:
            return

        fp = self.fingerprint()

        # Fast path: same source + hash + fingerprint already done.
        if self.store.find_active_by_source_hash_fp(str(src), content_hash, fp) is not None:
            return

        # Ask mapper for the canonical target. Mapper is policy-free: it
        # must never look at `dest` filesystem state; just return where this
        # source *wants* to land.
        try:
            canonical = self.spec.fn(
                src, self.dest, hash_=content_hash, **self.params
            )
        except Exception:
            log.exception("mapper %s raised on %s", self.spec.name, src)
            return
        if canonical is None:
            return
        canonical = Path(canonical)

        if not self._validate_target(src, canonical):
            return

        prev = self.store.find_active_by_source(str(src))

        if prev is not None:
            content_same = prev.content_hash == content_hash
            target_same = prev.dest_path == str(canonical)
            if content_same and target_same:
                # Pure policy/fingerprint drift. Nothing to copy or move.
                self.store.update_fingerprint(prev.id, fp)
                return
            # Either content changed OR mapper now wants a different location
            # (revision bump / mapper edit). Both go through on_change so the
            # user's policy decides replace-in-place vs. keep-old-add-new.
            target = self._apply_on_change(prev, canonical, src)
        else:
            target = self._apply_on_conflict(canonical, src)
        if target is None:
            return

        # Copy atomically.
        try:
            atomic_copy_path(src, target)
        except OSError as e:
            log.error("copy failed %s -> %s: %s", src, target, e)
            return

        # REPLACE may leave an orphan at prev.dest_path if target moved.
        if (
            prev is not None
            and self.policy.on_change == OnChange.REPLACE
            and Path(prev.dest_path) != target
        ):
            self._cleanup_path(Path(prev.dest_path))

        if prev is not None:
            self.store.mark_deleted(prev.id)

        self.store.insert(
            source_path=str(src),
            content_hash=content_hash,
            dest_path=str(target),
            is_duplicate=(target != canonical),
            fingerprint=fp,
        )
        tag = " (duplicate)" if target != canonical else ""
        self.logger.info("mirrored %s -> %s%s", src, target, tag)

    # ---- policy application ----

    def _apply_on_change(
        self, prev, canonical: Path, src: Path
    ) -> Path | None:
        """prev exists for this source, new content has arrived."""
        if self.policy.on_change == OnChange.REPLACE:
            # Try canonical. If another source sits there, defer to on_conflict.
            clash = self.store.find_active_by_dest(str(canonical))
            if clash is not None and clash.id != prev.id:
                return self._apply_on_conflict(canonical, src)
            return canonical
        # VERSION: keep prev, new copy goes to duplicate_dir.
        return self._duplicate_target(canonical)

    def _apply_on_conflict(self, canonical: Path, src: Path) -> Path | None:
        """Target might collide with another existing mapping or an on-disk file."""
        taken_db = self.store.find_active_by_dest(str(canonical))
        taken_fs = canonical.exists()
        if taken_db is None and not taken_fs:
            return canonical

        mode = self.policy.on_conflict
        if mode == OnConflict.OVERWRITE:
            if taken_db is not None:
                self.store.mark_deleted(taken_db.id)
            return canonical
        if mode == OnConflict.DUPLICATE:
            return self._duplicate_target(canonical)
        if mode == OnConflict.ERROR:
            self.logger.error(
                "conflict at %s (on_conflict=error), skipping %s", canonical, src
            )
            return None
        return canonical

    def _duplicate_target(self, canonical: Path) -> Path:
        return next_available_name(
            self._ensure_under_dest(self.dest / self.policy.duplicate_dir),
            canonical.stem,
            canonical.suffix,
        )

    def _ensure_under_dest(self, p: Path) -> Path:
        """Resolve `p` and verify it stays under `self.dest`. Raises otherwise.
        Policy validation should catch bad names up-front; this is belt-and-
        suspenders for any construction path that bypasses that check.
        """
        try:
            resolved = p.resolve(strict=False)
            dest_resolved = self.dest.resolve(strict=False)
        except OSError:
            resolved, dest_resolved = p.absolute(), self.dest.absolute()
        try:
            resolved.relative_to(dest_resolved)
        except ValueError:
            raise ValueError(
                f"policy path {p} resolves outside dest {self.dest}"
            )
        return p

    # ---- delete / archive ----

    def _remove(self, src: Path) -> None:
        m = self.store.find_active_by_source(str(src))
        if m is None:
            return
        dest = Path(m.dest_path)

        if self.policy.on_delete == OnDelete.DELETE_DEST:
            self._cleanup_path(dest)
            self.logger.info("on_delete=delete_dest removed %s", dest)
        elif self.policy.on_delete == OnDelete.ARCHIVE:
            self._archive_path(dest)
        # KEEP_DEST: leave the file alone.

        self.store.mark_deleted(m.id)

    def _archive_path(self, dest: Path) -> None:
        try:
            rel = dest.relative_to(self.dest)
        except ValueError:
            self.logger.warning("cannot archive %s: not under dest %s", dest, self.dest)
            return
        try:
            arc_root = self._ensure_under_dest(self.dest / self.policy.archive_dir)
        except ValueError as e:
            self.logger.error("archive aborted: %s", e)
            return
        arc = arc_root / rel
        arc.parent.mkdir(parents=True, exist_ok=True)
        if arc.exists():
            arc = next_available_name(arc.parent, arc.stem, arc.suffix)
        try:
            dest.rename(arc)
            self.logger.info("on_delete=archive: moved %s -> %s", dest, arc)
        except OSError as e:
            self.logger.error("archive failed %s -> %s: %s", dest, arc, e)

    def _cleanup_path(self, p: Path) -> None:
        try:
            if p.is_symlink() or p.is_file():
                p.unlink()
            elif p.is_dir():
                shutil.rmtree(p)
        except FileNotFoundError:
            pass
        except OSError as e:
            self.logger.error("failed to remove %s: %s", p, e)

    # ---- target safety ----

    def _validate_target(self, src: Path, target: Path) -> bool:
        try:
            target_abs = target.resolve(strict=False)
        except OSError:
            target_abs = target.absolute()
        try:
            dest_abs = self.dest.resolve(strict=False)
        except OSError:
            dest_abs = self.dest.absolute()
        try:
            target_abs.relative_to(dest_abs)
        except ValueError:
            self.logger.error(
                "mapper returned target %s outside dest %s; skipping", target, self.dest
            )
            return False
        try:
            src_abs = src.resolve(strict=False)
        except OSError:
            src_abs = src.absolute()
        if target_abs == src_abs:
            self.logger.error(
                "mapper returned target equal to source: %s; skipping", src
            )
            return False
        return True


class _Handler(FileSystemEventHandler):
    def __init__(self, watch: Watch):
        self.w = watch

    def _forward(
        self,
        kind: str,
        path_str: str,
        is_dir: bool,
        src_path_str: str | None = None,
    ) -> None:
        unit = _unit_root_for(Path(path_str), self.w.source, self.w._parsed, is_dir)
        src_unit: Path | None = None
        if src_path_str:
            src_unit = _unit_root_for(
                Path(src_path_str), self.w.source, self.w._parsed, is_dir
            )

        if kind == "moved":
            # Cross-boundary moves: translate to a deletion / creation.
            if unit is None and src_unit is not None:
                # Moved out of source tree.
                self.w.submit("deleted", src_unit, None)
                return
            if unit is not None and src_unit is None:
                # Moved in from outside source tree.
                self.w.submit("created", unit, None)
                return

        if unit is None:
            return
        self.w.submit(kind, unit, src_unit)

    def on_created(self, event):
        self._forward("created", event.src_path, event.is_directory)

    def on_modified(self, event):
        self._forward("modified", event.src_path, event.is_directory)

    def on_deleted(self, event):
        self._forward("deleted", event.src_path, event.is_directory)

    def on_moved(self, event):
        self._forward(
            "moved", event.dest_path, event.is_directory, src_path_str=event.src_path
        )


class Engine:
    """Owns watchdog observers and dispatches events to watches."""

    def __init__(self) -> None:
        self._observer = Observer()
        self._watches: list[Watch] = []
        self._started = False

    def add(self, watch: Watch) -> None:
        self._watches.append(watch)
        self._observer.schedule(_Handler(watch), str(watch.source), recursive=True)

    def start(
        self,
        scan_wrap_factory: Callable[[str], Callable[[list[Path]], Iterable[Path]] | None] | None = None,
    ) -> None:
        """Run initial scan then start the watchdog observer.

        `scan_wrap_factory(watch_name)` returns a wrap for that watch's
        initial scan (or None to stream without progress).
        """
        for w in self._watches:
            wrap = scan_wrap_factory(w.name) if scan_wrap_factory else None
            w.scan_once(wrap=wrap)
        self._observer.start()
        self._started = True

    def run_forever(
        self,
        scan_wrap_factory: Callable[[str], Callable[[list[Path]], Iterable[Path]] | None] | None = None,
    ) -> None:
        self.start(scan_wrap_factory=scan_wrap_factory)
        try:
            self._observer.join()
        except KeyboardInterrupt:
            self.stop()

    def stop(self) -> None:
        for w in self._watches:
            w.stop()
        if self._started:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._started = False
