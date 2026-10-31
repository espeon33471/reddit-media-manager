"""Sync-logica: posts ophalen, media detecteren, in de queue zetten."""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

from .db import DB
from .media import classify_media, extract_media_items
from .external import resolve_external_video
from .reddit import RedditClient, RedditError
from .util import now_iso, sanitize_sub

log = logging.getLogger("rmm.sync")

SORTS = ("new", "top", "hot")


def parse_sub_names(text: str) -> list[str]:
    """Textarea-upload: één naam per regel óf komma/gestreept-gescheiden."""
    names: list[str] = []
    for line in (text or "").splitlines():
        for part in re.split(r"[,\s]+", line):
            name = sanitize_sub(part)
            if name and name not in names:
                names.append(name)
    return names


def _parse_date_from(value: str | None) -> float | None:
    """'YYYY-MM-DD' → UTC-middernacht als timestamp; None bij afwezig/ongeldig."""
    if not value:
        return None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", value.strip())
    if not m:
        return None
    try:
        return datetime(int(m[1]), int(m[2]), int(m[3]), tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None


class SyncService:
    def __init__(self, db: DB, settings: dict[str, Any], reddit: RedditClient, dataroot_fn):
        self.db = db
        self.settings = settings
        self.reddit = reddit
        self.dataroot_fn = dataroot_fn

    # -- subbeheer -------------------------------------------------------------
    async def import_subs(self, text: str) -> dict[str, Any]:
        names = parse_sub_names(text)
        added, existing = 0, 0
        for name in names:
            row = await self.db.one("SELECT sub FROM subs WHERE sub = ?", (name,))
            if row:
                existing += 1
                continue
            await self.db.execute(
                "INSERT INTO subs(sub, enabled, max_posts, date_from, sort) VALUES (?, 1, 0, '', 'new')",
                (name,),
            )
            added += 1
        return {"added": added, "existing": existing, "names": names}

    async def update_sub(self, sub: str, *, enabled: bool | None = None, max_posts: int | None = None,
                         date_from: str | None = None, sort: str | None = None) -> dict[str, Any]:
        sub = sanitize_sub(sub)
        row = await self.db.one("SELECT * FROM subs WHERE sub = ?", (sub,))
        if row is None:
            raise ValueError(f"Subreddit '{sub}' staat niet in de lijst.")
        enabled = row["enabled"] if enabled is None else (1 if enabled else 0)
        if max_posts is not None:
            max_posts = max(0, int(max_posts))
        date_from = row["date_from"] if date_from is None else (date_from or "").strip()
        sort = (sort or row["sort"]).lower()
        if sort not in SORTS:
            sort = "new"
        await self.db.execute(
            "UPDATE subs SET enabled = ?, max_posts = ?, date_from = ?, sort = ? WHERE sub = ?",
            (enabled, max_posts, date_from, sort, sub),
        )
        return await self.get_sub(sub)  # type: ignore[return-value]

    async def delete_sub(self, sub: str) -> None:
        sub = sanitize_sub(sub)
        await self.db.execute("DELETE FROM subs WHERE sub = ?", (sub,))
        await self.db.execute("DELETE FROM sub_state WHERE sub = ?", (sub,))

    async def list_subs(self) -> list[dict[str, Any]]:
        rows = await self.db.query(
            """
            SELECT s.*, st.last_after_cursor, st.last_post_id, st.last_synced_at,
                   (SELECT COUNT(*) FROM downloads d WHERE d.sub = s.sub AND d.url != '') AS file_count,
                   (SELECT COUNT(*) FROM downloads d WHERE d.sub = s.sub AND d.status = 'done') AS done_count
            FROM subs s LEFT JOIN sub_state st ON st.sub = s.sub
            ORDER BY s.sub
            """
        )
        return rows

    async def get_sub(self, sub: str) -> dict[str, Any] | None:
        return await self.db.one(
            """
            SELECT s.*, st.last_after_cursor, st.last_post_id, st.last_synced_at
            FROM subs s LEFT JOIN sub_state st ON st.sub = s.sub
            WHERE s.sub = ?
            """,
            (sub,),
        )

    # -- sync --------------------------------------------------------------------
    async def sync_sub(self, sub: str, mode: str = "new") -> dict[str, Any]:
        """Nieuwe posts ophalen en in de queue zetten.

        mode='new' → vanaf laatste cursor; mode='all' → vanaf het begin.
        """
        sub = sanitize_sub(sub)
        sub_row = await self.db.one("SELECT * FROM subs WHERE sub = ?", (sub,))
        if sub_row is None:
            raise ValueError(f"Subreddit '{sub}' staat niet in de lijst. Importeer hem eerst.")

        sort = sub_row["sort"] or "new"
        max_posts = sub_row["max_posts"] or 0
        date_from_ts = _parse_date_from(sub_row["date_from"])

        cursor: str | None = None
        if mode == "new":
            state = await self.db.one("SELECT * FROM sub_state WHERE sub = ?", (sub,))
            cursor = (state or {}).get("last_after_cursor") or None

        summary: dict[str, Any] = {
            "sub": sub,
            "pages": 0,
            "new_posts": 0,
            "files_enqueued": 0,
            "empty_posts": 0,
            "already_seen_posts": 0,
            "before_date_posts": 0,
        }

        try:
            while True:
                listing = await self.reddit.listing(sub, sort=sort, limit=100, after=cursor)
                children = listing["children"]
                if not children:
                    break
                summary["pages"] += 1

                new_in_page = 0
                stop = False
                for child in children:
                    data = child.get("data") or {}
                    post_id = data.get("id")
                    if not post_id:
                        continue

                    # Post-level dedup: skip alleen als er al een 'done' bestand is.
                    # Posts met uitsluitend failed/skipped/pending worden bij een
                    # re-sync opnieuw geprobeerd (bijv. na een media/NSFW-fix).
                    done_row = await self.db.one(
                        "SELECT id FROM downloads WHERE sub = ? AND post_id = ? AND status = 'done' LIMIT 1",
                        (sub, post_id),
                    )
                    if done_row:
                        summary["already_seen_posts"] += 1
                        continue
                    # Oude failed/skipped rijen opruimen zodat een retry geen duplicaten maakt.
                    await self.db.execute(
                        "DELETE FROM downloads WHERE sub = ? AND post_id = ? AND status IN ('failed','skipped')",
                        (sub, post_id),
                    )

                    created_utc = float(data.get("created_utc") or 0)
                    if date_from_ts is not None and created_utc and created_utc < date_from_ts:
                        summary["before_date_posts"] += 1
                        await self.db.execute(
                            "INSERT INTO downloads(sub, post_id, url, status, error) "
                            "VALUES (?, ?, '', 'skipped', 'ouder dan date_from')",
                            (sub, post_id),
                        )
                        continue

                    items = extract_media_items(child)  # list[list[str]]

                    # Externe video (RedGifs/Gifer): los de ANIMEERDE MP4 op en
                    # maak hem de eerste kandidaat (statische preview blijft fallback).
                    # NB: resolve_external_video leest data.get('url'/'media') — dus de
                    # DATA-dict meedoen, niet de {kind, data}-wrapper (anders leeg).
                    ext_cands = await resolve_external_video(child.get("data", child))
                    if ext_cands:
                        items = ([ext_cands + items[0]] + items[1:]) if items else [ext_cands]

                    if not items:
                        # Lege post (geen media) → skipped, geen fout.
                        summary["empty_posts"] += 1
                        await self.db.execute(
                            "INSERT INTO downloads(sub, post_id, url, status, error) "
                            "VALUES (?, ?, '', 'skipped', 'geen media in post')",
                            (sub, post_id),
                        )
                        continue

                    # File-type filtering op basis van settings (dict of legacy comma-string)
                    enabled_types = self._enabled_file_types()
                    kept_items: list[list[str]] = []
                    for cands in items:
                        mtype = classify_media(cands[0])
                        if mtype is None:
                            kept_items.append(cands)  # onbekend → toch toevoegen
                        elif mtype in enabled_types:
                            kept_items.append(cands)

                    if not kept_items:
                        await self.db.execute(
                            "INSERT INTO downloads(sub, post_id, url, status, error) "
                            "VALUES (?, ?, '', 'skipped', 'file type niet geselecteerd')",
                            (sub, post_id),
                        )
                        continue

                    post_date = (
                        datetime.fromtimestamp(created_utc, tz=timezone.utc).strftime("%Y%m%d")
                        if created_utc else datetime.now(timezone.utc).strftime("%Y%m%d")
                    )
                    post_title = (data.get("title") or "").strip()
                    post_author = data.get("author") or ""
                    for cands in kept_items:
                        await self.db.execute(
                            "INSERT INTO downloads(sub, post_id, url, candidates, status, post_date, post_title, post_author) "
                            "VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)",
                            (sub, post_id, cands[0], json.dumps(cands), post_date, post_title, post_author),
                        )
                    summary["files_enqueued"] += len(kept_items)
                    new_in_page += 1

                    self._write_meta(sub, post_id, data, [c[0] for c in kept_items], post_date)
                    log.info("Ingepland %s/%s: %d bestanden (%d overgeslagen)", sub, post_id, len(kept_items), len(items) - len(kept_items))

                    if max_posts and summary["new_posts"] + 1 >= max_posts:
                        summary["new_posts"] = max_posts
                        stop = True
                        break
                    summary["new_posts"] += 1

                if stop:
                    break

                next_cursor = listing["after"]
                if not next_cursor:
                    break
                # Geen enkele nieuwe post in deze paginade → verder bladeren is zinloos.
                if new_in_page == 0:
                    break
                cursor = next_cursor

        except RedditError:
            # Melding ook zichtbaar in de queue (spec).
            await self._record_sync_error(sub, "Sync mislukt (zie logs).")
            raise

        await self.db.execute(
            """
            INSERT INTO sub_state(sub, last_after_cursor, last_post_id, last_synced_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(sub) DO UPDATE SET
              last_after_cursor = COALESCE(excluded.last_after_cursor, sub_state.last_after_cursor),
              last_post_id = COALESCE(excluded.last_post_id, sub_state.last_post_id),
              last_synced_at = excluded.last_synced_at
            """,
            (sub, cursor, None, now_iso()),
        )
        # laatste post_id van deze run (eerste geënsyncde post van deze sub)
        last = await self.db.one(
            "SELECT post_id FROM downloads WHERE sub = ? AND url != '' ORDER BY id DESC LIMIT 1", (sub,)
        )
        if last:
            await self.db.execute(
                "UPDATE sub_state SET last_post_id = ? WHERE sub = ?", (last["post_id"], sub)
            )
        log.info(
            "Sync %s klaar: %d pagina's, %d nieuwe posts, %d bestanden in queue, %d lege posts",
            sub, summary["pages"], summary["new_posts"], summary["files_enqueued"], summary["empty_posts"],
        )
        return summary

    def _enabled_file_types(self) -> set[str]:
        """Bestandsformaten die moeten worden gedownloade (dict of legacy comma-string)."""
        raw = self.settings.get("file_types")
        if isinstance(raw, dict):
            return {str(k).strip().lower() for k, v in raw.items() if v}
        return {t.strip().lower() for t in str(raw or "").split(",") if t.strip()}

    async def _record_sync_error(self, sub: str, msg: str) -> None:
        try:
            await self.db.execute(
                "INSERT INTO downloads(sub, post_id, url, status, error) "
                "VALUES (?, 'sync-fout', '', 'failed', ?)",
                (sub, msg[:400]),
            )
        except Exception:  # noqa: BLE001 — nooit de echte fout overmaskeren
            log.exception("Kon sync-fout niet in queue vastleggen")

    def _write_meta(self, sub: str, post_id: str, data: dict, urls: list[str], post_date: str) -> None:
        """meta.json naast de bestanden (alleen in posts-naming mode)."""
        if str(self.settings.get("naming_mode") or "flat").lower() != "posts":
            return
        directory = os.path.join(self.dataroot_fn(), sub, f"{post_date}-{post_id}")
        try:
            os.makedirs(directory, exist_ok=True)
            meta = {
                "title": data.get("title") or "",
                "author": data.get("author") or "",
                "created_utc": data.get("created_utc"),
                "permalink": data.get("permalink") or "",
                "sub": sub,
                "urls": urls,
            }
            with open(os.path.join(directory, "meta.json"), "w", encoding="utf-8") as fh:
                json.dump(meta, fh, ensure_ascii=False, indent=2)
        except OSError:
            log.exception("Kon meta.json niet schrijven voor %s/%s", sub, post_id)
