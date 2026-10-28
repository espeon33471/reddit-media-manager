"""SQLite-laag (aiosqlite). Eén gedeelde, geserialiseerde verbinding.

Tabelaan:
  downloads  — de wachtrij én de history (id, sub, post_id, url, sha256,
               local_path, downloaded_at, status, attempts, error)
  sub_state  — per subreddit: last_after_cursor, last_post_id, last_synced_at
  subs       — per-sub configuratie (enabled, max_posts, date_from, sort)
  settings   — key/value app-instellingen
"""
from __future__ import annotations

from typing import Any, Optional

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS downloads (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  sub           TEXT    NOT NULL,
  post_id       TEXT    NOT NULL,
  url           TEXT    NOT NULL,
  sha256        TEXT,
  local_path    TEXT,
  downloaded_at TEXT,
  status        TEXT    NOT NULL DEFAULT 'pending',
  attempts      INTEGER NOT NULL DEFAULT 0,
  error         TEXT,
  post_date     TEXT,
  post_title    TEXT,
  post_author   TEXT,
  bytes_done    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_dl_url    ON downloads(url);
CREATE INDEX IF NOT EXISTS idx_dl_sha    ON downloads(sha256);
CREATE INDEX IF NOT EXISTS idx_dl_post   ON downloads(sub, post_id);
CREATE INDEX IF NOT EXISTS idx_dl_status ON downloads(status);

CREATE TABLE IF NOT EXISTS sub_state (
  sub               TEXT PRIMARY KEY,
  last_after_cursor TEXT,
  last_post_id      TEXT,
  last_synced_at    TEXT
);

CREATE TABLE IF NOT EXISTS subs (
  sub       TEXT PRIMARY KEY,
  enabled   INTEGER NOT NULL DEFAULT 1,
  max_posts INTEGER NOT NULL DEFAULT 0,
  date_from TEXT,
  sort      TEXT    NOT NULL DEFAULT 'new'
);

CREATE TABLE IF NOT EXISTS settings (
  key   TEXT PRIMARY KEY,
  value TEXT
);
"""


class DB:
    """Dunne async wrapper om aiosqlite met rijen als dict."""

    def __init__(self, path: str):
        self.path = path
        self.conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.executescript(SCHEMA)
        # Nieuwe kolommen voor bestaande databases (idempotent).
        for col, ddl in (
            ("post_title", "ALTER TABLE downloads ADD COLUMN post_title TEXT"),
            ("post_author", "ALTER TABLE downloads ADD COLUMN post_author TEXT"),
            ("bytes_done", "ALTER TABLE downloads ADD COLUMN bytes_done INTEGER NOT NULL DEFAULT 0"),
        ):
            try:
                await self.conn.execute(ddl)
            except aiosqlite.OperationalError:
                pass  # kolom bestaat al
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.commit()

    async def close(self) -> None:
        if self.conn is not None:
            await self.conn.close()
            self.conn = None

    async def execute(self, sql: str, params: tuple = ()) -> aiosqlite.Cursor:
        assert self.conn is not None, "DB niet geopend"
        cur = await self.conn.execute(sql, params)
        await self.conn.commit()
        return cur

    async def query(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        assert self.conn is not None, "DB niet geopend"
        cur = await self.conn.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]

    async def one(self, sql: str, params: tuple = ()) -> Optional[dict[str, Any]]:
        rows = await self.query(sql, params)
        return rows[0] if rows else None
