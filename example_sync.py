"""Werkend headless-voorbeeld: sync één subreddit en download alle media.

Gebruik:
    python example_sync.py aww
    python example_sync.py MechanicalGulls --max-posts 5
    python example_sync.py pics --full

Draait dezelfde componenten als de web-app (zelfde state.db en dataroot),
zonder FastAPI.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from rmm.config import Settings
from rmm.db import DB
from rmm.downloader import MediaDownloader
from rmm.engine import Engine
from rmm.reddit import RedditClient, RedditError
from rmm.sync import SyncService
from rmm.util import data_dir, sanitize_sub, setup_logging

PROJECT_ROOT = Path(__file__).resolve().parent
# Zelfde datamap-logic als de web-app (RMM_DATA_DIR, bv. /data in Docker).
DATA_DIR = data_dir()
BASE_DIR = (DATA_DIR or PROJECT_ROOT)


def dataroot_fn(settings: Settings) -> str:
    def _fn() -> str:
        p = Path(settings.dataroot or "downloads").expanduser()
        return str(p if p.is_absolute() else BASE_DIR / p)

    return _fn


async def run(sub: str, max_posts: int | None, full: bool) -> int:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    setup_logging(str(BASE_DIR / "app.log"))
    log = logging.getLogger("example")

    db = DB(str(BASE_DIR / "state.db"))
    await db.connect()
    settings = Settings()
    await settings.load(db)
    reddit = RedditClient(settings)
    downloader = MediaDownloader(settings)
    engine = Engine(db, settings, downloader, dataroot_fn(settings))
    sync = SyncService(db, settings, reddit, dataroot_fn(settings))

    exit_code = 0
    try:
        name = sanitize_sub(sub)
        if not name:
            print("Geen geldige subredditnaam opgegeven.")
            return 2

        result = await sync.import_subs(name)
        if max_posts is not None:
            await db.execute("UPDATE subs SET max_posts = ? WHERE sub = ?", (max(0, max_posts), name))
        log.info("Sub r/%s in lijst (nieuw: %d)", name, result["added"])

        try:
            summary = await sync.sync_sub(name, mode="all" if full else "new")
        except RedditError as exc:
            print(f"\nFOUT: {exc}")
            print("De fout staat ook in de web-UI onder Queue (status: failed).")
            return 1

        print(f"\nSync r/{name} (mode={'all' if full else 'new'}):")
        for key in ("pages", "new_posts", "files_enqueued", "empty_posts",
                    "already_seen_posts", "before_date_posts"):
            print(f"  {key:>20}: {summary[key]}")

        if summary["files_enqueued"] == 0:
            print("\nNiets nieuws te downloaden.")
            return 0

        await engine.resume()
        print("\nDownloaden ...")
        while True:
            status = await engine.status()
            pending = status["counters"]["pending"] + status["counters"]["running"]
            done = status["counters"]["done"]
            failed = status["counters"]["failed"]
            print(f"  pending={pending} done={done} failed={failed} "
                  f"speed={status['speed_bps'] / 1024:.0f} KB/s")
            if pending == 0:
                break
            await asyncio.sleep(1)
            if engine.state == "stopped" and pending == 0:
                break

        failed_items = await db.query(
            "SELECT sub, post_id, url, error FROM downloads WHERE sub = ? AND status = 'failed'",
            (name,),
        )
        if failed_items:
            print("\nMislukte items:")
            for row in failed_items[:20]:
                print(f"  {row['post_id']} {row['url']} — {row['error']}")

        print(f"\nKlaar. Bestanden in: {dataroot_fn(settings)()}\\u_{name}\\")
        return 0

    finally:
        await engine.stop()
        await downloader.close()
        await reddit.close()
        await db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Headless Reddit-media-sync (voorbeeld)")
    parser.add_argument("subreddit", help="naam van de subreddit, bijv. aww")
    parser.add_argument("--max-posts", type=int, default=None, help="max. aantal posts (0 = alles)")
    parser.add_argument("--full", action="store_true", help="resync vanaf het begin (negeert cursor)")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args.subreddit, args.max_posts, args.full)))


if __name__ == "__main__":
    main()
