"""Regression tests for bugs found after the policy refactor."""
from __future__ import annotations

from pathlib import Path

import pytest

from fferyman import algorithm
from fferyman.core.db import Database
from fferyman.core.engine import Watch, _Handler, _unit_root_for, parse_watch_mode
from fferyman.core.mapper import MapperSpec, get_spec
from fferyman.core.policy import OnChange, OnConflict, OnDelete, Policy


# ---- mappers used by several tests --------------------------------------

@algorithm("_reg_flatten_file", revision=1)
def _reg_flatten_file(src, dest, **_):
    return dest / src.name


@algorithm("_reg_flatten_dir", watch_mode="dir:1", revision=1)
def _reg_flatten_dir(src, dest, **_):
    return dest / src.name


def _make_watch(
    tmp_path: Path,
    policy: Policy,
    spec_fn=_reg_flatten_file,
) -> tuple[Path, Path, Database, Watch]:
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


# ---- Bug 1: dir:N child delete was nuking the whole unit ---------------

def test_bug1_child_delete_in_dir_mode_is_not_unit_delete(tmp_path):
    src, dst, db, watch = _make_watch(
        tmp_path, Policy(on_delete=OnDelete.DELETE_DEST, on_change=OnChange.VERSION),
        spec_fn=_reg_flatten_dir,
    )
    d = src / "X"
    d.mkdir()
    (d / "a.txt").write_text("A")
    (d / "b.txt").write_text("B")
    watch.scan_once()
    assert (dst / "X" / "a.txt").read_text() == "A"

    # Delete a child file but keep the unit directory present.
    (d / "a.txt").unlink()
    watch.dispatch("deleted", d, None)

    # Unit dir still exists → must NOT be treated as unit deletion.
    assert (dst / "X").exists(), "on_delete wrongly fired for a child deletion"
    # And we re-ingested: the dir hash changed, so under version policy the
    # updated tree lands as a duplicate.
    assert (dst / "duplicate" / "X_2").exists()
    db.close()


def test_bug1_whole_unit_delete_still_fires_on_delete(tmp_path):
    """Sanity-check that the legitimate case still works."""
    src, dst, db, watch = _make_watch(
        tmp_path, Policy(on_delete=OnDelete.DELETE_DEST),
        spec_fn=_reg_flatten_dir,
    )
    d = src / "Y"
    d.mkdir()
    (d / "f").write_text("v")
    watch.scan_once()
    assert (dst / "Y").exists()

    import shutil as _sh
    _sh.rmtree(d)
    watch.dispatch("deleted", d, None)

    assert not (dst / "Y").exists()
    db.close()


# ---- Bug 2: move out of source tree dropped the delete ------------------

def test_bug2_move_out_of_source_triggers_on_delete(tmp_path):
    src, dst, db, watch = _make_watch(
        tmp_path, Policy(on_delete=OnDelete.DELETE_DEST),
    )
    f = src / "a.txt"
    f.write_text("hello")
    watch.scan_once()
    assert (dst / "a.txt").exists()

    outside = tmp_path / "outside"
    outside.mkdir()
    target_outside = outside / "a.txt"
    f.rename(target_outside)

    class _Evt:
        src_path = str(f)
        dest_path = str(target_outside)
        is_directory = False

    _Handler(watch).on_moved(_Evt())

    assert not (dst / "a.txt").exists(), "move-out did not trigger on_delete"
    db.close()


def test_bug2_move_in_from_outside_triggers_ingest(tmp_path):
    src, dst, db, watch = _make_watch(tmp_path, Policy())
    outside = tmp_path / "outside"
    outside.mkdir()
    ext = outside / "b.txt"
    ext.write_text("new")

    inside = src / "b.txt"
    ext.rename(inside)

    class _Evt:
        src_path = str(ext)
        dest_path = str(inside)
        is_directory = False

    _Handler(watch).on_moved(_Evt())

    assert (dst / "b.txt").read_text() == "new"
    db.close()


# ---- Bug 3: canonical target moved (revision bump / mapper edit) -------

def test_bug3_revision_bump_moves_file_under_replace(tmp_path):
    src, dst, db, watch = _make_watch(
        tmp_path, Policy(on_change=OnChange.REPLACE),
    )
    f = src / "a.txt"
    f.write_text("X")
    watch.scan_once()
    assert (dst / "a.txt").read_text() == "X"

    # Simulate a revision bump where the mapper now writes to a sub/ prefix.
    def _v2(src, dest, **_):
        return dest / "sub" / src.name

    watch.spec = MapperSpec(
        name=watch.spec.name, fn=_v2, watch_mode=watch.spec.watch_mode, revision=2
    )
    watch.scan_once()

    assert not (dst / "a.txt").exists(), "old canonical should be cleaned up under replace"
    assert (dst / "sub" / "a.txt").read_text() == "X"
    db.close()


def test_bug3_policy_only_change_does_not_copy(tmp_path):
    """Sanity-check: pure policy drift still takes the fingerprint-only path."""
    src, dst, db, watch = _make_watch(
        tmp_path, Policy(on_change=OnChange.VERSION),
    )
    f = src / "a.txt"
    f.write_text("X")
    watch.scan_once()

    before_rows = len(db.scope("t", watch.spec.name).list_active())
    # Flip a policy field that does NOT affect canonical target.
    watch.policy = Policy(on_change=OnChange.REPLACE)
    watch.scan_once()

    # Exactly one active row still; no duplicate created.
    after_rows = db.scope("t", watch.spec.name).list_active()
    assert len(after_rows) == before_rows
    assert not (dst / "duplicate").exists()
    db.close()


# ---- Bug 4: watch_mode="file" must not forward directory events --------

def test_bug4_file_mode_rejects_dir_unit_root():
    parsed = parse_watch_mode("file")
    assert _unit_root_for(Path("/src/X"), Path("/src"), parsed, is_dir=True) is None
    assert _unit_root_for(Path("/src/f.txt"), Path("/src"), parsed, is_dir=False) == Path("/src/f.txt")


def test_bug4_file_mode_ignores_dircreated_event(tmp_path):
    src, dst, db, watch = _make_watch(tmp_path, Policy())
    d = src / "a_dir"
    d.mkdir()

    class _Evt:
        src_path = str(d)
        is_directory = True

    _Handler(watch).on_created(_Evt())
    # Nothing in dest should have been created.
    assert list(dst.iterdir()) == []
    db.close()


# ---- Bug 5: duplicate_dir / archive_dir cannot escape dest -------------

@pytest.mark.parametrize(
    "bad",
    [
        "../leak",
        "..",
        "foo/bar",
        "foo\\bar",
        "/abs",
        "",
    ],
)
def test_bug5_policy_rejects_escaping_duplicate_dir(bad):
    with pytest.raises(ValueError):
        Policy(duplicate_dir=bad)


@pytest.mark.parametrize(
    "bad",
    [
        "../leak",
        "..",
        "nested/deep",
        "/abs",
        "",
    ],
)
def test_bug5_policy_rejects_escaping_archive_dir(bad):
    with pytest.raises(ValueError):
        Policy(archive_dir=bad)


def test_bug5_policy_from_dict_rejects_escaping(tmp_path):
    from fferyman.core.policy import policy_from_dict
    with pytest.raises(ValueError):
        policy_from_dict({"duplicate_dir": "../leak"})
    with pytest.raises(ValueError):
        policy_from_dict({"archive_dir": "/abs"})
