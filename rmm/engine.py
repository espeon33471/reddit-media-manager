"""Download-engine: async workers rond de queue in de DB.

State-machine: stopped → running ⇄ paused → stopped.
  - Pauze/stop onderbreekt ook lopende downloads (chunk-check) en zet het
    item terug op `pending`, zodat resume exact verder gaat waar het stopte.
  - Na een crash/restart worden `running`-rijen weer `pending` en de engine
    start automatisch → queue hervat vanzelf.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from typing import Any, Optional

from .db import DB
from .convert import ConvertError, convert_file
from .downloader import DownloadError, MediaDownloader, PausedError
from .media import classify_media, guess_extension_from_content_type, url_extension
from .util import now_iso, sanitize_stem

log = logging.getLogger("rmm.engine")

SPEED_WINDOW = 10.0  # seconden voor de snelheidsberekening


def _parse_candidates(raw) -> list[str] | None:
    """Kandidaat-URL's uit de rij (JSON of lijst) → list[str]; onjuist/leeg → None."""
    if not raw:
        return None
    if isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        try:
            items = json.loads(raw)
        except (ValueError, TypeError):
            return None
        if not isinstance(items, list):
            return None
    out = [str(x).strip() for x in items if str(x).strip()]
    return out or None


class Engine:
    def __init__(self, db: DB, settings: dict[str, Any], downloader: MediaDownloader, dataroot_fn):
        self.db = db
        self.settings = settings
        self.downloader = downloader
        self.dataroot_fn = dataroot_fn
        self.state = "stopped"  # stopped | running | paused
        self._tasks: list[asyncio.Task] = []
        self._lock = asyncio.Lock()
        self._bytes_log: deque[tuple[float, int]] = deque()
        self._session_bytes = 0
        self.errors: deque[dict[str, str]] = deque(maxlen=50)

    # -- besturing -----------------------------------------------------------
    async def start(self) -> None:
        async with self._lock:
            if self.state != "stopped":
                return
            # Herstel: items die bij een crash 'running' waren → pending.
            await self.db.execute(
                "UPDATE downloads SET status = 'pending', error = NULL WHERE status = 'running'"
            )
            self.state = "running"
            n = max(1, int(self.settings.get("concurrency", 4)))
            self._tasks = [asyncio.create_task(self._worker(i)) for i in range(n)]
            log.info("Engine gestart met %d workers", n)

    async def pause(self) -> None:
        self.state = "paused"
        log.info("Engine gepauzeerd")

    async def resume(self) -> None:
        if self.state == "stopped":
            await self.start()
        else:
            self.state = "running"
            log.info("Engine hervat")

    async def stop(self) -> None:
        self.state = "stopped"
        current = asyncio.current_task()
        tasks, self._tasks = self._tasks, []
        # Nooit het huidige task cancellen/gatheren: bij auto-stop roept een
        # worker zelf stop() aan — zichzelf cancellen is een asyncio-antipattern
        # (deadlock/recursie in Task.cancel). Die worker loopt via zijn loop
        # gewoon uit op `state == "stopped"`.
        others = [t for t in tasks if t is not current and not t.done()]
        for t in others:
            t.cancel()
        if others:
            await asyncio.gather(*others, return_exceptions=True)
        log.info("Engine gestopt")

    # -- status ----------------------------------------------------------------
    def record_bytes(self, n: int) -> None:
        now = time.monotonic()
        self._bytes_log.append((now, n))
        self._session_bytes += n
        cutoff = now - SPEED_WINDOW
        while self._bytes_log and self._bytes_log[0][0] < cutoff:
            self._bytes_log.popleft()

    def speed_bps(self) -> float:
        now = time.monotonic()
        cutoff = now - SPEED_WINDOW
        total = sum(n for ts, n in self._bytes_log if ts >= cutoff)
        span = min(SPEED_WINDOW, max(1.0, now - (self._bytes_log[0][0] if self._bytes_log else now)))
        return total / span if total else 0.0

    async def status(self) -> dict[str, Any]:
        counters = {"pending": 0, "running": 0, "done": 0, "failed": 0, "skipped": 0}
        for row in await self.db.query("SELECT status, COUNT(*) AS c FROM downloads GROUP BY status"):
            if row["status"] in counters:
                counters[row["status"]] = row["c"]
        total = await self.db.one("SELECT COUNT(*) AS c FROM downloads WHERE url != ''")
        return {
            "state": self.state,
            "counters": counters,
            "total_items": total["c"] if total else 0,
            "speed_bps": round(self.speed_bps(), 1),
            "session_bytes": self._session_bytes,
            "recent_errors": list(self.errors)[-20:][::-1],
        }

    # -- worker ------------------------------------------------------------------
    async def _worker(self, idx: int) -> None:
        log_ = logging.getLogger(f"rmm.worker{idx}")
        while True:
            if self.state == "stopped":
                return
            if self.state != "running":
                await asyncio.sleep(0.25)
                continue
            row = await self._claim_next()
            if row is None:
                await self._maybe_auto_stop()
                await asyncio.sleep(0.5)
                continue
            try:
                await self._process(row)
            except PausedError:
                await self.db.execute(
                    "UPDATE downloads SET status = 'pending', error = NULL WHERE id = ?", (row["id"],)
                )
                log_.info("Item %d teruggezet naar pending (pauze/stop)", row["id"])
            except asyncio.CancelledError:
                await self._revert(row["id"])
                raise
            except Exception as exc:  # noqa: BLE001 — worker mag nooit dood
                msg = str(exc)[:400]
                log_.exception("Item %d (%s/%s) gefaald: %s", row["id"], row["sub"], row["post_id"], msg)
                await self.db.execute(
                    "UPDATE downloads SET status = 'failed', error = ? WHERE id = ?", (msg, row["id"])
                )
                self.errors.append({"at": now_iso(), "msg": f"{row['sub']}/{row['post_id']}: {msg}"})

    async def _revert(self, item_id: int) -> None:
        try:
            await self.db.execute(
                "UPDATE downloads SET status = 'pending', error = NULL WHERE id = ? AND status = 'running'",
                (item_id,),
            )
        except Exception:  # noqa: BLE001
            pass

    async def _claim_next(self) -> Optional[dict[str, Any]]:
        row = await self.db.one(
            "SELECT id FROM downloads WHERE status = 'pending' ORDER BY id LIMIT 1"
        )
        if row is None:
            return None
        cur = await self.db.execute(
            "UPDATE downloads SET status = 'running', attempts = attempts + 1 WHERE id = ? AND status = 'pending'",
            (row["id"],),
        )
        if cur.rowcount == 0:
            return None
        return await self.db.one("SELECT * FROM downloads WHERE id = ?", (row["id"],))

    async def _maybe_auto_stop(self) -> None:
        """Wachtrij leeg (geen pending, niets in verwerking) → netjes stoppen."""
        if self.state != "running":
            return
        row = await self.db.one(
            "SELECT COUNT(*) AS c FROM downloads WHERE status IN ('pending', 'running')"
        )
        if row and row["c"] == 0:
            log.info("Wachtrij leeg — engine stopt")
            await self.stop()

    # -- verwerking van één item ---------------------------------------------------
    def _should_pause(self) -> bool:
        return self.state != "running"

    async def _process(self, row: dict[str, Any]) -> None:
        sub, post_id, url = row["sub"], row["post_id"], row["url"]
        if not url:
            await self.db.execute(
                "UPDATE downloads SET status = 'skipped', error = COALESCE(error, 'geen media') WHERE id = ?",
                (row["id"],),
            )
            return

        # 1) Deduplicatie op URL: zelfde URL al voltooid → skip.
        dup = await self.db.one(
            "SELECT local_path FROM downloads WHERE url = ? AND status = 'done' AND id != ?",
            (url, row["id"]),
        )
        if dup:
            await self.db.execute(
                "UPDATE downloads SET status = 'skipped', local_path = ?, error = 'URL reeds gedownload (dedup)', downloaded_at = ? WHERE id = ?",
                (dup["local_path"], now_iso(), row["id"]),
            )
            log.info("Skip %s/%s: URL reeds gedownload", sub, post_id)
            return

        # Bestemming: vlak <dataroot>/<sub>[/<yyyy-mm>]/<post_title>[_idx].<ext>
        # (of posts-mode: <dataroot>/<sub>/<post_date>-<post_id>/NN.<ext>)
        post_date = row.get("post_date") or time.strftime("%Y%m%d", time.gmtime())
        naming_mode = str(self.settings.get("naming_mode") or "flat").lower()
        date_sub = str(self.settings.get("date_subfolder") or "none").lower()
        base_dir = os.path.join(self.dataroot_fn(), sub)
        if date_sub == "month":
            try:
                year, month = post_date[:4], post_date[4:6]
                base_dir = os.path.join(base_dir, f"{year}-{month}")
            except Exception:  # noqa: BLE001 — post_date ongeldig
                pass
        directory = base_dir

        ext = url_extension(url)
        if not ext:
            ct = await self.downloader.content_type_for(url)
            ext = guess_extension_from_content_type(ct or "") or "bin"
        ext = ext.replace("/", "_")

        siblings = await self.db.query(
            "SELECT id FROM downloads WHERE sub = ? AND post_id = ? AND url != '' ORDER BY id",
            (sub, post_id),
        )
        idx = next((i for i, s in enumerate(siblings) if s["id"] == row["id"]), 0)

        stem = "media"
        if naming_mode == "posts":
            directory = os.path.join(base_dir, f"{post_date}-{post_id}")
            dest = os.path.join(directory, f"{idx + 1:02d}.{ext}")
        else:
            # flat: naam = post-titel (uit de downloads-rij), met idx-suffix
            title = row.get("post_title") or ""
            stem = sanitize_stem(title, f"media_{post_id}")
            if idx > 0:
                stem = f"{stem[:70]}_{idx + 1}"
            dest = os.path.join(directory, f"{stem}.{ext}")
            # Collision-guard: ander bestand met die naam bestaat al → post_id erachter
            if os.path.exists(dest):
                dest = os.path.join(directory, f"{stem[:60]}_{post_id}.{ext}")

        # 2) Download (met pauze-check per chunk + snelheidsmeting + bytes_done in DB).
        # on_bytes(total) = cumulatieve bytes; record_bytes krijgt de delta (snelheid).
        _prog = {"last": 0}
        def on_bytes(total: int) -> None:
            delta = total - _prog["last"]
            _prog["last"] = total
            if delta > 0:
                self.record_bytes(delta)
            try:
                import asyncio as _a
                _a.get_running_loop().create_task(self._set_bytes_done(row["id"], total))
            except RuntimeError:
                pass

        # Kandidaat-URL's (full-res eerst): uit de rij (JSON) of fallback op [url].
        cands = _parse_candidates(row.get("candidates")) or [url]

        sha, size = await self.downloader.download(cands, dest, self._should_pause, on_bytes)
        log.info("Gedownload %s (%.1f KB) → %s", cands[0], size / 1024, dest)

        # 3) Deduplicatie op sha256: identiek bestand bestaat al → weghalen + skip.
        dup_sha = await self.db.one(
            "SELECT local_path FROM downloads WHERE sha256 = ? AND status = 'done' AND id != ?",
            (sha, row["id"]),
        )
        if dup_sha:
            try:
                os.remove(dest)
            except OSError:
                pass
            await self.db.execute(
                "UPDATE downloads SET status = 'skipped', sha256 = ?, local_path = ?, "
                "error = 'Identiek bestand bestaat al (sha256-dedup)', downloaded_at = ? WHERE id = ?",
                (sha, dup_sha["local_path"], now_iso(), row["id"]),
            )
            log.info("Skip %s/%s: sha256 bestaat al", sub, post_id)
            return

        # 4) Format-conversie op basis van settings
        photo_fmt = str(self.settings.get("photo_format") or "jpeg").lower().lstrip(".")
        video_fmt = str(self.settings.get("video_format") or "mp4").lower().lstrip(".")
        gif_mode = str(self.settings.get("gif_mode") or "keep").lower()
        photo_quality = int(self.settings.get("photo_quality", 95))
        video_quality = int(self.settings.get("video_quality", 18))
        mtype = classify_media(url)

        try:
            new_path = await convert_file(
                dest, mtype, photo_fmt, video_fmt, gif_mode,
                photo_quality=photo_quality, video_quality=video_quality,
            )
            if new_path != dest:
                # SHA256 opnieuw berekenen voor het geconverteerde bestand
                import hashlib
                h = hashlib.sha256()
                with open(new_path, "rb") as f:
                    for chunk in iter(lambda: f.read(8192 * 1024), b""):
                        h.update(chunk)
                sha = h.hexdigest()
                dest = new_path
                log.info("Geconverteerd → %s", dest)
        except ConvertError as e:
            log.warning("Conversie mislukte (%s) — behoud origineel bestand: %s", e, dest)
        except Exception as e:  # noqa: BLE001 — conversie mag de download niet verpesten
            log.warning("Onverwacht conversiefout (%s) — behoud origineel: %s", e, dest)

        # Autoritatieve bytes_done = echte bestandsgrootte (post-conversie) van disk.
        try:
            final_bytes = os.path.getsize(dest)
        except OSError:
            final_bytes = int(size or 0)
        await self.db.execute(
            "UPDATE downloads SET status = 'done', sha256 = ?, local_path = ?, downloaded_at = ?, "
            "error = NULL, bytes_done = ? WHERE id = ?",
            (sha, dest, now_iso(), final_bytes, row["id"]),
        )

    # -- handmatige acties ----------------------------------------------------------
    async def _set_bytes_done(self, item_id: int, n: int) -> None:
        """Update bytes_done (throttle: maximaal ~1x/sec per item)."""
        now = time.monotonic()
        last = getattr(self, "_bytes_done_ts", {}).get(item_id, 0.0)
        if now - last < 1.0:
            return
        setattr(self, "_bytes_done_ts", {**getattr(self, "_bytes_done_ts", {}), item_id: now})
        try:
            await self.db.execute(
                "UPDATE downloads SET bytes_done = ? WHERE id = ?", (n, item_id)
            )
        except Exception:  # noqa: BLE001 — status-update mag niet crashen
            pass

    async def redownload(self, sub: str | None = None) -> int:
        """Zet `done`-items terug op `pending` (her-download, hoogste resolutie)."""
        sql = "UPDATE downloads SET status = 'pending', error = NULL, local_path = NULL WHERE status = 'done'"
        params: tuple = ()
        if sub:
            sql += " AND sub = ?"
            params = (sub,)
        cur = await self.db.execute(sql, params)
        n = cur.rowcount if cur else 0
        log.info("Her-download: %d items teruggezet op pending (sub=%s)", n, sub or "alle")
        if n and self.state == "stopped":
            await self.start()
        return n

    async def retry_item(self, item_id: int) -> bool:
        row = await self.db.one(
            "SELECT id FROM downloads WHERE id = ? AND status IN ('failed', 'skipped')", (item_id,)
        )
        if row is None:
            return False
        await self.db.execute(
            "UPDATE downloads SET status = 'pending', error = NULL WHERE id = ?", (item_id,)
        )
        if self.state == "stopped":
            await self.start()
        return True
