"""Tests for the `regex:<pattern>` watch_mode and configurable debounce."""
from __future__ import annotations

import time
from pathlib import Path

from fferyman import algorithm
from fferyman.core.db import Database
from fferyman.core.engine import (
    Engine,
    Watch,
    _Handler,
    _unit_root_for,
    iter_units,
    parse_watch_mode,
)
from fferyman.core.mapper import get_spec
from fferyman.core.policy import OnChange, OnConflict, OnDelete, Policy


_PATTERN = r"^[A-Z0-9]+-\d{8}-\d{6}$"


@algorithm("_reg_sendout", watch_mode=f"regex:{_PATTERN}", revision=1)
def _reg_sendout(src, dest, **_):
    return dest / src.name


def _setup(tmp_path: Path, policy: Policy | None = None, debounce: float = 0.5):
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    db = Database(tmp_path / "state.sqlite")
    spec = get_spec(_reg_sendout)
    assert spec is not None
    watch = Watch(
        name="t",
        spec=spec,
        source=src,
        dest=dst,
        params={},
        store=db.scope("t", spec.name),
        policy=policy or Policy(),
        debounce_seconds=debounce,
    )
    return src, dst, db, watch


# ---- parse ----

def test_parse_regex_mode_compiles_pattern():
    parsed = parse_watch_mode(f"regex:{_PATTERN}")
    assert parsed.kind == "regex"
    assert parsed.regex is not None
    assert parsed.regex.fullmatch("ABC123-20260324-153909")


def test_parse_regex_mode_rejects_bad_pattern():
    import pytest
    with pytest.raises(ValueError, match="invalid regex"):
        parse_watch_mode("regex:[unclosed")


# ---- _unit_root_for walk-up ----

def test_unit_root_walks_up_to_matching_ancestor(tmp_path):
    parsed = parse_watch_mode(f"regex:{_PATTERN}")
    source = tmp_path / "s"
    deep = source / "layer1" / "layer2" / "ABC123-20260324-153909" / "inner" / "file.txt"
    unit = _unit_root_for(deep, source, parsed, is_dir=False)
    assert unit == source / "layer1" / "layer2" / "ABC123-20260324-153909"


def test_unit_root_returns_none_when_no_ancestor_matches(tmp_path):
    parsed = parse_watch_mode(f"regex:{_PATTERN}")
    source = tmp_path / "s"
    other = source / "misc" / "file.txt"
    assert _unit_root_for(other, source, parsed, is_dir=False) is None


def test_unit_root_matches_self(tmp_path):
    parsed = parse_watch_mode(f"regex:{_PATTERN}")
    source = tmp_path / "s"
    matched = source / "deep" / "XYZ999-20260101-000000"
    assert _unit_root_for(matched, source, parsed, is_dir=True) == matched


def test_unit_root_rejects_matching_file_name(tmp_path):
    """A file whose own name matches the regex must NOT become a unit —
    regex mode is directory-only per the docs."""
    parsed = parse_watch_mode(f"regex:{_PATTERN}")
    source = tmp_path / "s"
    suspicious = source / "ABC999-20260101-010101"   # looks like a matched name
    # Pretend this is a file event (is_dir=False).
    assert _unit_root_for(suspicious, source, parsed, is_dir=False) is None


def test_unit_root_bubbles_up_past_matching_file_to_matching_dir(tmp_path):
    """If a file happens to match but sits inside a matching dir, the walk
    should skip the file and return the dir."""
    parsed = parse_watch_mode(f"regex:{_PATTERN}")
    source = tmp_path / "s"
    dir_match = source / "DIR0000-20260101-010101"
    file_match = dir_match / "XYZ999-20260202-020202"     # matches regex, but it's a file
    unit = _unit_root_for(file_match, source, parsed, is_dir=False)
    assert unit == dir_match


# ---- iter_units ----

def test_iter_units_finds_matches_at_varying_depth(tmp_path):
    src = tmp_path / "s"
    (src / "a" / "b" / "AAA-20260101-010101").mkdir(parents=True)
    (src / "c" / "BBB-20260202-020202").mkdir(parents=True)
    (src / "d" / "e" / "f" / "g" / "CCC-20260303-030303").mkdir(parents=True)
    (src / "misc" / "non_matching").mkdir(parents=True)

    units = set(iter_units(src, f"regex:{_PATTERN}"))
    names = {u.name for u in units}
    assert names == {"AAA-20260101-010101", "BBB-20260202-020202", "CCC-20260303-030303"}


def test_iter_units_does_not_descend_into_matched_dirs(tmp_path):
    src = tmp_path / "s"
    outer = src / "OUTER00-20260101-010101"
    nested = outer / "INNER00-20260202-020202"
    nested.mkdir(parents=True)

    units = list(iter_units(src, f"regex:{_PATTERN}"))
    assert outer in units
    assert nested not in units


def test_iter_units_skips_files_with_matching_names(tmp_path):
    """A regular file whose name matches must not be yielded as a unit."""
    src = tmp_path / "s"
    src.mkdir()
    real_dir = src / "DIR0000-20260101-010101"
    real_dir.mkdir()
    suspicious_file = src / "FILE000-20260202-020202"   # matches regex
    suspicious_file.write_text("not a dir")

    units = list(iter_units(src, f"regex:{_PATTERN}"))
    assert real_dir in units
    assert suspicious_file not in units


# ---- ingest end-to-end ----

def test_scan_flattens_matching_dirs_to_dest(tmp_path):
    src, dst, db, watch = _setup(tmp_path)
    deep = src / "layer1" / "ABC123-20260324-153909"
    deep.mkdir(parents=True)
    (deep / "data.txt").write_text("hello")

    watch.scan_once()

    assert (dst / "ABC123-20260324-153909" / "data.txt").read_text() == "hello"
    db.close()


def test_child_file_event_bubbles_up_to_matched_unit(tmp_path):
    """Given an event for a child file, the unit that gets re-ingested
    should be the matched ancestor dir, not the child file.
    """
    src, dst, db, watch = _setup(
        tmp_path, policy=Policy(on_change=OnChange.REPLACE), debounce=0.0
    )
    outer = src / "deep" / "XYZ000-20260101-010101"
    outer.mkdir(parents=True)
    (outer / "v1.txt").write_text("1")
    watch.scan_once()
    assert (dst / "XYZ000-20260101-010101" / "v1.txt").read_text() == "1"

    # Child file appears. Handler must translate this into an event on the
    # outer matched dir.
    new_child = outer / "v2.txt"
    new_child.write_text("2")

    class _Evt:
        src_path = str(new_child)
        is_directory = False

    _Handler(watch).on_created(_Evt())
    # debounce=0 means dispatch happens immediately (0s Timer still fires
    # asynchronously; wait briefly for the thread).
    time.sleep(0.1)

    assert (dst / "XYZ000-20260101-010101" / "v2.txt").read_text() == "2"
    db.close()


def test_collision_between_same_named_dirs_at_different_paths(tmp_path):
    """Two matching dirs with the same name under different parent paths
    end up with the second one in DUPLICATE/name_2 (default duplicate policy).
    """
    src, dst, db, watch = _setup(tmp_path, policy=Policy(duplicate_dir="DUPLICATE"))
    (src / "a" / "ABC123-20260324-153909").mkdir(parents=True)
    (src / "a" / "ABC123-20260324-153909" / "f").write_text("A")
    (src / "b" / "ABC123-20260324-153909").mkdir(parents=True)
    (src / "b" / "ABC123-20260324-153909" / "f").write_text("B")

    watch.scan_once()

    primary = dst / "ABC123-20260324-153909"
    dup = dst / "DUPLICATE" / "ABC123-20260324-153909_2"
    assert primary.exists() and dup.exists()
    assert {(primary / "f").read_text(), (dup / "f").read_text()} == {"A", "B"}
    db.close()


# ---- debounce behavior ----

def test_debounce_coalesces_rapid_events(tmp_path):
    """Multiple rapid events for the same unit should collapse into one ingest."""
    src, dst, db, watch = _setup(tmp_path, debounce=0.3)
    outer = src / "DEB0000-20260101-010101"
    outer.mkdir()
    (outer / "a").write_text("1")

    handler = _Handler(watch)

    def _fake_event(path, is_dir=False):
        class _Evt:
            src_path = str(path)
            is_directory = is_dir
        return _Evt()

    # Fire three modifications in quick succession.
    handler.on_modified(_fake_event(outer / "a"))
    handler.on_modified(_fake_event(outer / "a"))
    handler.on_modified(_fake_event(outer / "a"))

    # Before debounce fires, nothing should be in dest.
    assert not (dst / "DEB0000-20260101-010101").exists()

    # Wait past debounce window.
    time.sleep(0.5)

    assert (dst / "DEB0000-20260101-010101" / "a").read_text() == "1"
    # Only one active mapping (one ingest, not three).
    assert len(db.scope("t", watch.spec.name).list_active()) == 1
    watch.stop()
    db.close()


def test_debounce_seconds_configurable():
    spec = get_spec(_reg_sendout)
    assert spec is not None
    w = Watch(
        name="t",
        spec=spec,
        source=Path("/tmp"),
        dest=Path("/tmp"),
        params={},
        store=None,  # type: ignore[arg-type]
        debounce_seconds=2.5,
    )
    assert w.debounce_seconds == 2.5
    assert w._debouncer is not None
    assert w._debouncer._delay == 2.5


# ---- config parsing ----

def test_config_parses_debounce_seconds(tmp_path):
    import yaml
    from fferyman.config import load

    (tmp_path / "src").mkdir()
    (tmp_path / "dst").mkdir()
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(
        yaml.dump(
            {
                "database": str(tmp_path / "s.sqlite"),
                "debounce_seconds": 1.0,
                "watches": [
                    {
                        "name": "w1",
                        "algorithm": "x",
                        "source": str(tmp_path / "src"),
                        "dest": str(tmp_path / "dst"),
                    },
                    {
                        "name": "w2",
                        "algorithm": "x",
                        "source": str(tmp_path / "src"),
                        "dest": str(tmp_path / "dst"),
                        "debounce_seconds": 5.0,
                    },
                ],
            }
        )
    )
    cfg = load(cfg_path)
    assert cfg.watches[0].debounce_seconds == 1.0       # inherits top-level
    assert cfg.watches[1].debounce_seconds == 5.0       # overrides


def test_config_rejects_negative_debounce(tmp_path):
    import pytest
    import yaml
    from fferyman.config import load

    (tmp_path / "src").mkdir()
    (tmp_path / "dst").mkdir()
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(
        yaml.dump(
            {
                "database": str(tmp_path / "s.sqlite"),
                "watches": [
                    {
                        "name": "w",
                        "algorithm": "x",
                        "source": str(tmp_path / "src"),
                        "dest": str(tmp_path / "dst"),
                        "debounce_seconds": -1,
                    }
                ],
            }
        )
    )
    with pytest.raises(ValueError, match="debounce_seconds"):
        load(cfg_path)
