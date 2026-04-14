from __future__ import annotations

import shutil
import time
from pathlib import Path

from fferyman.core.db import Database
from fferyman.core.engine import Engine, Watch
from fferyman.core.policy import OnChange, OnConflict, OnDelete, Policy
from fferyman.core.registry import Registry

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def _make_registry() -> Registry:
    reg = Registry()
    reg.load_from_directory(EXAMPLES)
    return reg


def _make_watch(
    tmp_path: Path,
    algo_name: str,
    params=None,
    policy: Policy | None = None,
) -> tuple[Path, Path, Database, Watch]:
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    db = Database(tmp_path / "state.sqlite")
    spec = _make_registry().get(algo_name)
    watch = Watch(
        name="t",
        spec=spec,
        source=src,
        dest=dst,
        params=dict(params or {}),
        store=db.scope("t", spec.name),
        policy=policy or Policy(),
    )
    return src, dst, db, watch


def test_flatten_initial_scan_with_duplicates(tmp_path):
    src, dst, db, watch = _make_watch(tmp_path, "flatten")
    (src / "01").mkdir()
    (src / "02").mkdir()
    (src / "01" / "I").write_text("A")
    (src / "02" / "I").write_text("B")

    watch.scan_once()

    top = dst / "I"
    dup = dst / "duplicate" / "I_2"
    assert top.exists() and dup.exists()
    assert {top.read_text(), dup.read_text()} == {"A", "B"}
    db.close()


def test_flatten_delete_recreate_goes_to_duplicate(tmp_path):
    src, dst, db, watch = _make_watch(tmp_path, "flatten")
    f = src / "sub" / "I"
    f.parent.mkdir()
    f.write_text("A")
    watch.scan_once()
    assert (dst / "I").read_text() == "A"

    f.unlink()
    watch.dispatch("deleted", f, None)

    f.write_text("Z")
    watch.dispatch("created", f, None)

    assert (dst / "I").read_text() == "A"
    dup = dst / "duplicate" / "I_2"
    assert dup.exists() and dup.read_text() == "Z"
    db.close()


def test_flatten_same_content_is_noop(tmp_path):
    src, dst, db, watch = _make_watch(tmp_path, "flatten")
    f = src / "sub" / "I"
    f.parent.mkdir()
    f.write_text("A")
    watch.scan_once()

    watch.dispatch("modified", f, None)
    assert not (dst / "duplicate").exists()
    db.close()


def test_mirror_dirs_pattern(tmp_path):
    src, dst, db, watch = _make_watch(
        tmp_path,
        "mirror_matching_dirs",
        params={"include": "batch_*", "exclude": "tmp_*"},
    )
    (src / "batch_1").mkdir()
    (src / "batch_1" / "f").write_text("1")
    (src / "tmp_1").mkdir()
    (src / "tmp_1" / "f").write_text("2")
    (src / "other").mkdir()
    (src / "other" / "f").write_text("3")

    watch.scan_once()

    assert (dst / "batch_1" / "f").read_text() == "1"
    assert not (dst / "tmp_1").exists()
    assert not (dst / "other").exists()
    db.close()


def test_mirror_dirs_duplicate_on_content_change(tmp_path):
    src, dst, db, watch = _make_watch(
        tmp_path, "mirror_matching_dirs", params={"include": "*"}
    )
    d = src / "X"
    d.mkdir()
    (d / "f").write_text("v1")
    watch.scan_once()
    assert (dst / "X" / "f").read_text() == "v1"

    shutil.rmtree(d)
    d.mkdir()
    (d / "f").write_text("v2")

    watch.dispatch("modified", d, None)

    assert (dst / "X" / "f").read_text() == "v1"
    assert (dst / "duplicate" / "X_2" / "f").read_text() == "v2"
    db.close()


def test_engine_run_with_watchdog(tmp_path):
    """End-to-end using the real engine + watchdog observer."""
    src, dst, db, watch = _make_watch(tmp_path, "flatten")

    engine = Engine()
    engine.add(watch)
    engine.start()
    try:
        (src / "A").write_text("hello")
        deadline = time.time() + 5
        while time.time() < deadline and not (dst / "A").exists():
            time.sleep(0.05)
        assert (dst / "A").read_text() == "hello"
    finally:
        engine.stop()
        db.close()
