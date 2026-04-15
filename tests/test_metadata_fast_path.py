from __future__ import annotations

import hashlib
import os
from pathlib import Path

from fferyman import algorithm
from fferyman.core import engine as engine_module
from fferyman.core.db import Database
from fferyman.core.engine import Watch
from fferyman.core.mapper import MapperSpec, get_spec
from fferyman.core.policy import HashPolicy, OnChange, Policy


@algorithm("_meta_flatten", revision=1)
def _meta_flatten(src, dest, **_):
    return dest / src.name


@algorithm("_meta_hash_bucket", revision=1)
def _meta_hash_bucket(src, dest, *, hash_, **_):
    assert hash_ is not None
    return dest / hash_[:2] / src.name


def _make_watch(
    tmp_path: Path,
    *,
    policy: Policy | None = None,
    spec_fn=_meta_flatten,
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
        policy=policy or Policy(),
    )
    return src, dst, db, watch


def test_unchanged_file_scan_reuses_metadata_without_hash(tmp_path, monkeypatch):
    src, dst, db, watch = _make_watch(tmp_path)
    f = src / "a.txt"
    f.write_text("hello")
    watch.scan_once()
    assert (dst / "a.txt").read_text() == "hello"

    def _should_not_hash(_path):
        raise AssertionError("hash_path should not run for unchanged file metadata")

    monkeypatch.setattr(engine_module, "hash_path", _should_not_hash)
    watch.scan_once()

    active = db.scope("t", watch.spec.name).list_active()
    assert len(active) == 1
    db.close()


def test_metadata_drift_same_content_refreshes_stored_file_metadata(
    tmp_path, monkeypatch
):
    src, dst, db, watch = _make_watch(tmp_path)
    f = src / "a.txt"
    f.write_text("hello")
    watch.scan_once()

    before = db.scope("t", watch.spec.name).find_active_by_source(str(f))
    assert before is not None
    assert before.source_mtime_ns is not None

    st = f.stat()
    os.utime(f, ns=(st.st_atime_ns + 1_000_000_000, st.st_mtime_ns + 1_000_000_000))

    calls = 0
    real_hash_path = engine_module.hash_path

    def _count_hash(path):
        nonlocal calls
        calls += 1
        return real_hash_path(path)

    monkeypatch.setattr(engine_module, "hash_path", _count_hash)
    watch.scan_once()
    assert calls == 1

    refreshed = db.scope("t", watch.spec.name).find_active_by_source(str(f))
    assert refreshed is not None
    assert refreshed.id == before.id
    assert refreshed.source_mtime_ns == f.stat().st_mtime_ns
    assert refreshed.source_size == f.stat().st_size
    assert (dst / "a.txt").read_text() == "hello"

    def _should_not_hash_again(_path):
        raise AssertionError("metadata refresh should make the next scan skip hashing")

    monkeypatch.setattr(engine_module, "hash_path", _should_not_hash_again)
    watch.scan_once()
    db.close()


def test_revision_retarget_reuses_cached_hash_for_unchanged_file(
    tmp_path, monkeypatch
):
    src, dst, db, watch = _make_watch(
        tmp_path,
        policy=Policy(on_change=OnChange.REPLACE),
    )
    f = src / "a.txt"
    f.write_text("hello")
    watch.scan_once()

    old = db.scope("t", watch.spec.name).find_active_by_source(str(f))
    assert old is not None

    def _v2(src, dest, *, hash_, **_):
        assert hash_ == old.content_hash
        return dest / "moved" / src.name

    def _should_not_hash(_path):
        raise AssertionError("revision retarget should reuse cached file hash")

    monkeypatch.setattr(engine_module, "hash_path", _should_not_hash)
    watch.spec = MapperSpec(
        name=watch.spec.name,
        fn=_v2,
        watch_mode=watch.spec.watch_mode,
        revision=2,
    )

    watch.scan_once()

    assert not (dst / "a.txt").exists()
    assert (dst / "moved" / "a.txt").read_text() == "hello"
    db.close()


def test_always_hash_policy_rehashes_unchanged_file_on_scan(tmp_path, monkeypatch):
    src, dst, db, watch = _make_watch(
        tmp_path,
        policy=Policy(hash_policy=HashPolicy.ALWAYS),
    )
    f = src / "a.txt"
    f.write_text("hello")
    watch.scan_once()
    assert (dst / "a.txt").read_text() == "hello"

    calls = 0
    real_hash_path = engine_module.hash_path

    def _count_hash(path):
        nonlocal calls
        calls += 1
        return real_hash_path(path)

    monkeypatch.setattr(engine_module, "hash_path", _count_hash)
    watch.scan_once()

    assert calls == 1
    db.close()


def test_copy_then_hash_first_sync_hashes_staged_copy_and_supports_hash_mapper(
    tmp_path, monkeypatch
):
    src, dst, db, watch = _make_watch(
        tmp_path,
        policy=Policy(hash_policy=HashPolicy.COPY_THEN_HASH),
        spec_fn=_meta_hash_bucket,
    )
    f = src / "a.txt"
    f.write_text("hello")

    calls: list[Path] = []
    real_hash_path = engine_module.hash_path

    def _record_hash(path):
        path = Path(path)
        calls.append(path)
        assert path != f
        assert path.exists()
        assert dst in path.parents
        return real_hash_path(path)

    monkeypatch.setattr(engine_module, "hash_path", _record_hash)
    watch.scan_once()

    expected_hash = hashlib.sha256(b"hello").hexdigest()
    expected = dst / expected_hash[:2] / "a.txt"
    assert len(calls) == 1
    assert calls[0].parent == dst / ".fferyman-staging"
    assert expected.read_text() == "hello"

    active = db.scope("t", watch.spec.name).list_active()
    assert len(active) == 1
    assert active[0].content_hash == expected_hash
    assert active[0].dest_path == str(expected)
    db.close()


def test_config_parses_hash_policy(tmp_path):
    import yaml

    from fferyman.config import load

    (tmp_path / "src").mkdir()
    (tmp_path / "dst").mkdir()
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(
        yaml.dump(
            {
                "database": str(tmp_path / "s.sqlite"),
                "hash_policy": "always",
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
                        "hash_policy": "copy_then_hash",
                    },
                ],
            }
        )
    )

    cfg = load(cfg_path)
    assert cfg.watches[0].policy.hash_policy == HashPolicy.ALWAYS
    assert cfg.watches[1].policy.hash_policy == HashPolicy.COPY_THEN_HASH
