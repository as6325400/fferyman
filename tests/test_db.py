from __future__ import annotations

from fferyman.core.db import Database


def test_insert_and_lookup(tmp_path):
    db = Database(tmp_path / "state.sqlite")
    store = db.scope("w1", "algo")

    m = store.insert(
        source_path="/s/a",
        content_hash="h1",
        dest_path="/d/a",
        is_duplicate=False,
    )
    assert m.id > 0

    found = store.find_active_by_source("/s/a")
    assert found is not None and found.content_hash == "h1"

    assert store.find_active_by_source_hash_fp("/s/a", "h1", "") is not None
    assert store.find_active_by_source_hash_fp("/s/a", "other", "") is None

    assert store.find_active_by_dest("/d/a") is not None

    store.mark_deleted(m.id)
    assert store.find_active_by_source("/s/a") is None


def test_watch_isolation(tmp_path):
    db = Database(tmp_path / "state.sqlite")
    a = db.scope("w1", "algo")
    b = db.scope("w2", "algo")
    a.insert(source_path="/s", content_hash="h", dest_path="/d", is_duplicate=False)
    assert a.find_active_by_source("/s") is not None
    assert b.find_active_by_source("/s") is None
