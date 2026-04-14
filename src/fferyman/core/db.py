from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS mappings (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  watch_name    TEXT    NOT NULL,
  algorithm     TEXT    NOT NULL,
  source_path   TEXT    NOT NULL,
  source_inode  INTEGER,
  source_mtime  REAL,
  content_hash  TEXT    NOT NULL,
  dest_path     TEXT    NOT NULL,
  is_duplicate  INTEGER NOT NULL DEFAULT 0,
  fingerprint   TEXT    NOT NULL DEFAULT '',
  status        TEXT    NOT NULL,
  created_at    REAL    NOT NULL,
  updated_at    REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_map_src  ON mappings(watch_name, source_path, status);
CREATE INDEX IF NOT EXISTS ix_map_hash ON mappings(watch_name, content_hash);
CREATE INDEX IF NOT EXISTS ix_map_dst  ON mappings(watch_name, dest_path);
CREATE INDEX IF NOT EXISTS ix_map_fp   ON mappings(watch_name, source_path, content_hash, fingerprint, status);
"""

_MIGRATIONS = [
    "ALTER TABLE mappings ADD COLUMN fingerprint TEXT NOT NULL DEFAULT ''",
]


@dataclass
class Mapping:
    id: int
    watch_name: str
    algorithm: str
    source_path: str
    source_inode: int | None
    source_mtime: float | None
    content_hash: str
    dest_path: str
    is_duplicate: bool
    fingerprint: str
    status: str
    created_at: float
    updated_at: float

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Mapping":
        return cls(
            id=row["id"],
            watch_name=row["watch_name"],
            algorithm=row["algorithm"],
            source_path=row["source_path"],
            source_inode=row["source_inode"],
            source_mtime=row["source_mtime"],
            content_hash=row["content_hash"],
            dest_path=row["dest_path"],
            is_duplicate=bool(row["is_duplicate"]),
            fingerprint=row["fingerprint"] if "fingerprint" in row.keys() else "",
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


class Database:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(SCHEMA)
        self._apply_migrations()

    def _apply_migrations(self) -> None:
        for sql in _MIGRATIONS:
            try:
                self._conn.execute(sql)
            except sqlite3.OperationalError:
                # Column already exists — migration is a no-op.
                pass

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def scope(self, watch_name: str, algorithm: str) -> "MappingStore":
        return MappingStore(self, watch_name, algorithm)


class MappingStore:
    def __init__(self, db: Database, watch_name: str, algorithm: str):
        self._db = db
        self.watch_name = watch_name
        self.algorithm = algorithm

    def _exec(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._db._lock:
            return self._db._conn.execute(sql, params)

    def find_active_by_source(self, source_path: str) -> Mapping | None:
        cur = self._exec(
            "SELECT * FROM mappings WHERE watch_name=? AND source_path=? AND status='active' ORDER BY id DESC LIMIT 1",
            (self.watch_name, source_path),
        )
        row = cur.fetchone()
        return Mapping.from_row(row) if row else None

    def find_active_by_source_hash_fp(
        self, source_path: str, content_hash: str, fingerprint: str
    ) -> Mapping | None:
        cur = self._exec(
            "SELECT * FROM mappings WHERE watch_name=? AND source_path=? AND content_hash=? AND fingerprint=? AND status='active' LIMIT 1",
            (self.watch_name, source_path, content_hash, fingerprint),
        )
        row = cur.fetchone()
        return Mapping.from_row(row) if row else None

    def find_active_by_dest(self, dest_path: str) -> Mapping | None:
        cur = self._exec(
            "SELECT * FROM mappings WHERE watch_name=? AND dest_path=? AND status='active' LIMIT 1",
            (self.watch_name, dest_path),
        )
        row = cur.fetchone()
        return Mapping.from_row(row) if row else None

    def list_active(self) -> list[Mapping]:
        cur = self._exec(
            "SELECT * FROM mappings WHERE watch_name=? AND status='active'",
            (self.watch_name,),
        )
        return [Mapping.from_row(r) for r in cur.fetchall()]

    def insert(
        self,
        *,
        source_path: str,
        content_hash: str,
        dest_path: str,
        is_duplicate: bool,
        fingerprint: str = "",
        source_inode: int | None = None,
        source_mtime: float | None = None,
    ) -> Mapping:
        now = time.time()
        cur = self._exec(
            """
            INSERT INTO mappings(
                watch_name, algorithm, source_path, source_inode, source_mtime,
                content_hash, dest_path, is_duplicate, fingerprint,
                status, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                self.watch_name,
                self.algorithm,
                source_path,
                source_inode,
                source_mtime,
                content_hash,
                dest_path,
                1 if is_duplicate else 0,
                fingerprint,
                "active",
                now,
                now,
            ),
        )
        new_id = cur.lastrowid
        row = self._exec("SELECT * FROM mappings WHERE id=?", (new_id,)).fetchone()
        return Mapping.from_row(row)

    def mark_deleted(self, mapping_id: int) -> None:
        self._exec(
            "UPDATE mappings SET status='deleted', updated_at=? WHERE id=?",
            (time.time(), mapping_id),
        )

    def update_fingerprint(self, mapping_id: int, fingerprint: str) -> None:
        self._exec(
            "UPDATE mappings SET fingerprint=?, updated_at=? WHERE id=?",
            (fingerprint, time.time(), mapping_id),
        )
