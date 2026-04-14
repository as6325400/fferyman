from __future__ import annotations

from pathlib import Path

import pytest

from fferyman import algorithm
from fferyman.core.db import Database
from fferyman.core.engine import Watch
from fferyman.core.mapper import get_spec
from fferyman.core.policy import OnChange, OnConflict, OnDelete, Policy


# ---- tiny inline mappers for testing --------------------------------------

@algorithm("_test_flatten_file", revision=1)
def _test_flatten_file(src, dest, **_):
    return dest / src.name


@algorithm("_test_escape", revision=1)
def _test_escape(src, dest, **_):
    # Deliberately points outside dest — target safety must reject.
    return dest.parent / "elsewhere" / src.name


@algorithm("_test_src_equals_target", revision=1)
def _test_src_equals_target(src, dest, **_):
    # Points back at the source file itself.
    return src


# ---- helpers --------------------------------------------------------------

def _setup(tmp_path: Path, policy: Policy, spec_fn=_test_flatten_file) -> tuple[Path, Path, Database, Watch]:
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    db = Database(tmp_path / "state.sqlite")
    spec = get_spec(spec_fn)
    assert spec is not None
    watch = Watch(
        name="t",
        spec=spec,
        source=src,
        dest=dst,
        params={},
        store=db.scope("t", spec.name),
        policy=policy,
    )
    return src, dst, db, watch


# ---- on_conflict ----------------------------------------------------------

def test_on_conflict_overwrite(tmp_path):
    src, dst, db, watch = _setup(tmp_path, Policy(on_conflict=OnConflict.OVERWRITE))
    (src / "01").mkdir()
    (src / "02").mkdir()
    (src / "01" / "I").write_text("A")
    (src / "02" / "I").write_text("B")

    watch.scan_once()

    # Only one file at dest; later ingest overwrote earlier.
    assert (dst / "I").exists()
    assert not (dst / "duplicate").exists()
    assert (dst / "I").read_text() in {"A", "B"}
    db.close()


def test_on_conflict_duplicate(tmp_path):
    src, dst, db, watch = _setup(tmp_path, Policy(on_conflict=OnConflict.DUPLICATE))
    (src / "01").mkdir()
    (src / "02").mkdir()
    (src / "01" / "I").write_text("A")
    (src / "02" / "I").write_text("B")

    watch.scan_once()

    assert (dst / "I").exists()
    assert (dst / "duplicate" / "I_2").exists()
    db.close()


def test_on_conflict_error(tmp_path, caplog):
    src, dst, db, watch = _setup(tmp_path, Policy(on_conflict=OnConflict.ERROR))
    (src / "01").mkdir()
    (src / "02").mkdir()
    (src / "01" / "I").write_text("A")
    (src / "02" / "I").write_text("B")

    watch.scan_once()

    # Only the first landed; the second was refused.
    assert (dst / "I").exists()
    assert not (dst / "duplicate").exists()
    db.close()


# ---- on_change ------------------------------------------------------------

def test_on_change_replace(tmp_path):
    src, dst, db, watch = _setup(tmp_path, Policy(on_change=OnChange.REPLACE))
    f = src / "sub" / "I"
    f.parent.mkdir()
    f.write_text("v1")
    watch.scan_once()
    assert (dst / "I").read_text() == "v1"

    f.write_text("v2")
    watch.dispatch("modified", f, None)

    assert (dst / "I").read_text() == "v2"      # replaced
    assert not (dst / "duplicate").exists()
    db.close()


def test_on_change_version(tmp_path):
    src, dst, db, watch = _setup(tmp_path, Policy(on_change=OnChange.VERSION))
    f = src / "sub" / "I"
    f.parent.mkdir()
    f.write_text("v1")
    watch.scan_once()

    f.write_text("v2")
    watch.dispatch("modified", f, None)

    assert (dst / "I").read_text() == "v1"      # untouched
    assert (dst / "duplicate" / "I_2").read_text() == "v2"
    db.close()


# ---- on_delete ------------------------------------------------------------

def test_on_delete_keep_dest(tmp_path):
    src, dst, db, watch = _setup(tmp_path, Policy(on_delete=OnDelete.KEEP_DEST))
    f = src / "I"
    f.write_text("A")
    watch.scan_once()
    assert (dst / "I").read_text() == "A"

    f.unlink()
    watch.dispatch("deleted", f, None)

    assert (dst / "I").read_text() == "A"       # still there
    db.close()


def test_on_delete_delete_dest(tmp_path):
    src, dst, db, watch = _setup(tmp_path, Policy(on_delete=OnDelete.DELETE_DEST))
    f = src / "I"
    f.write_text("A")
    watch.scan_once()
    assert (dst / "I").exists()

    f.unlink()
    watch.dispatch("deleted", f, None)

    assert not (dst / "I").exists()
    db.close()


def test_on_delete_archive(tmp_path):
    src, dst, db, watch = _setup(tmp_path, Policy(on_delete=OnDelete.ARCHIVE))
    f = src / "I"
    f.write_text("A")
    watch.scan_once()
    assert (dst / "I").exists()

    f.unlink()
    watch.dispatch("deleted", f, None)

    assert not (dst / "I").exists()
    assert (dst / "archive" / "I").read_text() == "A"
    db.close()


# ---- target safety --------------------------------------------------------

def test_target_outside_dest_is_rejected(tmp_path):
    src, dst, db, watch = _setup(
        tmp_path, Policy(), spec_fn=_test_escape
    )
    (src / "leak.txt").write_text("secret")
    watch.scan_once()

    # Nothing should appear under dest, nor outside.
    assert not any(dst.rglob("*"))
    assert not (dst.parent / "elsewhere").exists()
    db.close()


def test_target_equals_source_is_rejected(tmp_path):
    src, dst, db, watch = _setup(
        tmp_path, Policy(), spec_fn=_test_src_equals_target
    )
    f = src / "a.txt"
    f.write_text("hello")
    watch.scan_once()

    # Mapper returned src itself — engine must refuse and leave source intact.
    assert f.read_text() == "hello"
    assert not any(dst.rglob("*"))
    db.close()


# ---- fingerprint invalidation --------------------------------------------

def test_policy_change_refreshes_fingerprint_without_recopy(tmp_path):
    src, dst, db, watch = _setup(tmp_path, Policy(on_change=OnChange.VERSION))
    f = src / "I"
    f.write_text("A")
    watch.scan_once()
    before = list(db.scope("t", watch.spec.name).list_active())
    assert len(before) == 1
    assert before[0].fingerprint == watch.fingerprint()

    # Swap policy, re-scan. Content is unchanged → no new file, just fp refresh.
    watch.policy = Policy(on_change=OnChange.REPLACE)
    watch.scan_once()

    after = db.scope("t", watch.spec.name).list_active()
    assert len(after) == 1
    assert after[0].id == before[0].id
    assert after[0].fingerprint == watch.fingerprint()
    assert after[0].fingerprint != before[0].fingerprint
    # No extra copies on disk.
    assert list(dst.rglob("*")) == [dst / "I"]
    db.close()


# ---- reconcile ------------------------------------------------------------

def test_reconcile_applies_on_delete_to_vanished_sources(tmp_path):
    src, dst, db, watch = _setup(tmp_path, Policy(on_delete=OnDelete.DELETE_DEST))
    f = src / "I"
    f.write_text("A")
    watch.scan_once()
    assert (dst / "I").exists()

    # Source removed while daemon was offline (no event received).
    f.unlink()

    # reconcile notices and fires on_delete.
    watch.reconcile()

    assert not (dst / "I").exists()
    assert db.scope("t", watch.spec.name).find_active_by_source(str(f)) is None
    db.close()
