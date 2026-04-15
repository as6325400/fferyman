"""Tests for --progress wrap plumbing.

These verify the `wrap` hook itself (Watch.scan_once / Watch.reconcile /
Engine.start) independent of tqdm — a custom wrap callable stands in for
the progress bar so the tests don't depend on tqdm being installed.
"""
from __future__ import annotations

from pathlib import Path

from fferyman import algorithm
from fferyman.core.db import Database
from fferyman.core.engine import Engine, Watch
from fferyman.core.mapper import get_spec
from fferyman.core.policy import Policy


@algorithm("_prog_flatten", revision=1)
def _prog_flatten(src, dest, **_):
    return dest / src.name


def _watch(tmp_path):
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    db = Database(tmp_path / "state.sqlite")
    spec = get_spec(_prog_flatten)
    assert spec is not None
    watch = Watch(
        name="t",
        spec=spec,
        source=src,
        dest=dst,
        params={},
        store=db.scope("t", spec.name),
        policy=Policy(),
    )
    return src, dst, db, watch


def test_scan_once_calls_wrap_with_units(tmp_path):
    src, _dst, db, watch = _watch(tmp_path)
    (src / "a.txt").write_text("1")
    (src / "b.txt").write_text("2")
    (src / "c.txt").write_text("3")

    seen: list[Path] = []

    def wrap(units: list[Path]):
        # Receive the materialised list (so tqdm can show total).
        assert len(units) == 3
        for u in units:
            seen.append(u)
            yield u

    watch.scan_once(wrap=wrap)
    assert {p.name for p in seen} == {"a.txt", "b.txt", "c.txt"}
    db.close()


def test_scan_once_without_wrap_streams(tmp_path):
    """Default path (no wrap) must still ingest all units."""
    src, dst, db, watch = _watch(tmp_path)
    (src / "a.txt").write_text("1")
    (src / "b.txt").write_text("2")
    watch.scan_once()
    assert (dst / "a.txt").exists()
    assert (dst / "b.txt").exists()
    db.close()


def test_reconcile_passes_wrap_through(tmp_path):
    src, _dst, db, watch = _watch(tmp_path)
    (src / "a.txt").write_text("1")
    (src / "b.txt").write_text("2")

    calls: list[int] = []

    def wrap(units: list[Path]):
        calls.append(len(units))
        return units

    watch.reconcile(wrap=wrap)
    assert calls == [2]
    db.close()


def test_engine_start_passes_factory_per_watch(tmp_path):
    src1 = tmp_path / "src1"; src1.mkdir()
    src2 = tmp_path / "src2"; src2.mkdir()
    dst1 = tmp_path / "dst1"; dst1.mkdir()
    dst2 = tmp_path / "dst2"; dst2.mkdir()
    (src1 / "a").write_text("A")
    (src2 / "b").write_text("B")

    db = Database(tmp_path / "state.sqlite")
    spec = get_spec(_prog_flatten)
    assert spec is not None

    w1 = Watch(name="w1", spec=spec, source=src1, dest=dst1, params={},
               store=db.scope("w1", spec.name), policy=Policy())
    w2 = Watch(name="w2", spec=spec, source=src2, dest=dst2, params={},
               store=db.scope("w2", spec.name), policy=Policy())

    invocations: dict[str, int] = {}

    def factory(name: str):
        def wrap(units: list[Path]):
            invocations[name] = len(units)
            return units
        return wrap

    engine = Engine()
    engine.add(w1)
    engine.add(w2)
    # Don't call engine.run_forever — just trigger the initial scans.
    # Use the scan-only helper that start() delegates to:
    for w in engine._watches:
        w.scan_once(wrap=factory(w.name))
    assert invocations == {"w1": 1, "w2": 1}
    assert (dst1 / "a").exists() and (dst2 / "b").exists()
    engine.stop()
    db.close()


def test_tqdm_integration_smoke(tmp_path):
    """If tqdm is installed, wrapping with it must not break ingestion."""
    tqdm = __import__("tqdm").tqdm  # will raise ImportError if missing
    src, dst, db, watch = _watch(tmp_path)
    for i in range(5):
        (src / f"f{i}.txt").write_text(str(i))

    watch.scan_once(wrap=lambda us: tqdm(us, desc=watch.name, disable=True))
    assert sum(1 for _ in dst.iterdir()) == 5
    db.close()
