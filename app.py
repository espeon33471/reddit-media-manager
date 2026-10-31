"""Reddit Media Download Manager — FastAPI-app.

Start:
    pip install -r requirements.txt
    uvicorn app:app --host 127.0.0.1 --port 8000
"""
from __future__ import annotations

import asyncio
import logging
import mimetypes
import re
import shutil
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from rmm.config import DEFAULTS, CONFIG_DIR, DEFAULT_DATAROOT, SETTINGS_PATH, load_settings, save_settings
from rmm.db import DB
from rmm.downloader import MediaDownloader
from rmm.engine import Engine
from rmm.reddit import RedditClient, RedditError
from rmm.sync import SyncService
from rmm.util import sanitize_sub, setup_logging

PROJECT_ROOT = Path(__file__).resolve().parent

setup_logging(str(CONFIG_DIR / "app.log"))
log = logging.getLogger("rmm.app")

DB_PATH = CONFIG_DIR / "state.db"


# --------------------------------------------------------------------------- helpers
def resolve_dataroot(dataroot: str) -> str:
    """Relatieve dataroot-padten t.o.v. de configmap (RMM_CONFIG of projectmap)."""
    p = Path(dataroot or "").expanduser()
    if not str(p) or not p.is_absolute():
        p = Path(DEFAULT_DATAROOT) / p
    return str(p)


class AppState:
    def __init__(self) -> None:
        self.db = DB(str(DB_PATH))
        self.settings: dict[str, Any] = load_settings()
        self.reddit: Optional[RedditClient] = None
        self.downloader: Optional[MediaDownloader] = None
        self.engine: Optional[Engine] = None
        self.sync: Optional[SyncService] = None
        self.autosync_task: Optional[asyncio.Task] = None


state = AppState()


def dataroot_fn() -> str:
    return resolve_dataroot(state.settings.get("dataroot", DEFAULT_DATAROOT))


# --------------------------------------------------------------------------- background
async def autosync_loop() -> None:
    """Elke 30s checken: auto_sync_minutes > 0 en engine niet bezig → sync alle enabled subs."""
    while True:
        await asyncio.sleep(30)
        try:
            mins = int(state.settings.get("auto_sync_minutes", 0) or 0)
            if mins <= 0:
                continue
            if state.engine is None or state.engine.state == "running":
                continue
            subs = await state.db.query(
                "SELECT sub FROM subs WHERE enabled = 1 ORDER BY sub"
            )
            for row in subs:
                if state.engine.state == "running":
                    break
                try:
                    await state.sync.sync_sub(row["sub"], "new")
                except Exception as exc:  # noqa: BLE001 — autosync mag nooit crashen
                    log.warning("Auto-sync %s mislukt: %s", row["sub"], exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("Auto-sync loop fout: %s", exc)


async def migrate_db_settings_to_json() -> None:
    """Eénmalige migratie: oude key/value settings-tabel → settings.json."""
    if SETTINGS_PATH.exists():
        return
    try:
        rows = await state.db.query("SELECT key, value FROM settings")
    except Exception:  # noqa: BLE001 — tabel bestaat niet → niets te migreren
        return
    if not rows:
        return
    patch: dict[str, Any] = {}
    for row in rows:
        key, value = row["key"], row["value"]
        if key not in DEFAULTS:
            continue
        default = DEFAULTS[key]
        if isinstance(default, int) and not isinstance(default, bool):
            try:
                value = int(value)
            except (TypeError, ValueError):
                continue
        elif isinstance(default, float):
            try:
                value = float(value)
            except (TypeError, ValueError):
                continue
        patch[key] = value
    if patch:
        save_settings(patch)
        log.info("Settings gemigreerd van DB naar %s: %s", SETTINGS_PATH, sorted(patch))


# --------------------------------------------------------------------------- lifespan
@asynccontextmanager
async def lifespan(_: FastAPI):
    await state.db.connect()
    await migrate_db_settings_to_json()
    state.settings.update(load_settings())  # JSON heeft precedence na migratie
    state.reddit = RedditClient(state.settings)
    state.downloader = MediaDownloader(state.settings)
    state.engine = Engine(state.db, state.settings, state.downloader, dataroot_fn)
    state.sync = SyncService(state.db, state.settings, state.reddit, dataroot_fn)
    # Engine start direct: queue uit een eerdere run hervat automatisch.
    await state.engine.start()
    state.autosync_task = asyncio.create_task(autosync_loop())
    log.info("App klaar — dataroot: %s | concurrency: %s | cookie: %s",
             dataroot_fn(), state.settings.get("concurrency", 4),
             "ingegeven" if state.settings.get("reddit_session") else "niet ingesteld")
    try:
        yield
    finally:
        if state.autosync_task is not None:
            state.autosync_task.cancel()
            try:
                await state.autosync_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        if state.engine is not None:
            await state.engine.stop()
        if state.downloader is not None:
            await state.downloader.close()
        if state.reddit is not None:
            await state.reddit.close()
        await state.db.close()


app = FastAPI(title="Reddit Media Download Manager", version="1.1.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(PROJECT_ROOT / "static")), name="static")
templates = Jinja2Templates(directory=str(PROJECT_ROOT / "templates"))


def page(request: Request, template: str, active: str, **ctx: Any) -> HTMLResponse:
    ctx.update(active=active,
               engine_state=state.engine.state if state.engine else "stopped")
    return templates.TemplateResponse(request, template, ctx)


# --------------------------------------------------------------------------- pagina's
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    return page(request, "dashboard.html", "dash")


@app.get("/subs", response_class=HTMLResponse)
async def subs_page(request: Request) -> HTMLResponse:
    return page(request, "subs.html", "subs")


@app.get("/history", response_class=HTMLResponse)
async def history_page(request: Request) -> HTMLResponse:
    return page(request, "history.html", "history")


@app.get("/queue", response_class=HTMLResponse)
async def queue_page(request: Request) -> HTMLResponse:
    return page(request, "queue.html", "queue")


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request) -> HTMLResponse:
    return page(request, "settings.html", "settings")


@app.get("/gallery", response_class=HTMLResponse)
async def gallery_page(request: Request) -> HTMLResponse:
    return page(request, "gallery.html", "gallery")


# --------------------------------------------------------------------------- status
@app.get("/api/status")
async def api_status() -> dict[str, Any]:
    assert state.engine is not None
    return await state.engine.status()


@app.post("/api/engine/{action}")
async def api_engine_action(action: str) -> dict[str, Any]:
    assert state.engine is not None
    actions = {
        "start": state.engine.start,
        "pause": state.engine.pause,
        "resume": state.engine.resume,
        "stop": state.engine.stop,
    }
    handler = actions.get(action)
    if handler is None:
        raise HTTPException(status_code=404, detail=f"Onbekende actie: {action}")
    await handler()
    return {"state": state.engine.state}


# --------------------------------------------------------------------------- subreddits
@app.get("/api/subs")
async def api_list_subs() -> dict[str, Any]:
    assert state.sync is not None
    return {"subs": await state.sync.list_subs()}


@app.post("/api/subs/import")
async def api_import_subs(payload: dict) -> dict[str, Any]:
    assert state.sync is not None
    text = (payload or {}).get("text", "")
    result = await state.sync.import_subs(text)
    log.info("Subs geïmporteerd: %d nieuw, %d al aanwezig", result["added"], result["existing"])
    return result


@app.post("/api/subs/import/file")
async def api_import_subs_file(file: UploadFile = File(...)) -> dict[str, Any]:
    assert state.sync is not None
    raw = await file.read()
    text = raw.decode("utf-8", errors="replace")
    result = await state.sync.import_subs(text)
    log.info("Subs via bestand geïmporteerd: %d nieuw, %d al aanwezig", result["added"], result["existing"])
    return result


@app.post("/api/subs/{sub}/config")
async def api_sub_config(sub: str, payload: dict) -> dict[str, Any]:
    assert state.sync is not None
    try:
        return await state.sync.update_sub(
            sub,
            enabled=(payload or {}).get("enabled"),
            max_posts=(payload or {}).get("max_posts"),
            date_from=(payload or {}).get("date_from"),
            sort=(payload or {}).get("sort"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/subs/{sub}")
async def api_delete_sub(sub: str) -> dict[str, Any]:
    assert state.sync is not None
    await state.sync.delete_sub(sub)
    return {"ok": True}


@app.post("/api/subs/{sub}/sync")
async def api_sub_sync(sub: str, payload: dict) -> dict[str, Any]:
    assert state.sync is not None and state.engine is not None
    mode = (payload or {}).get("mode", "new")
    if mode not in ("new", "all"):
        mode = "new"
    force_start = bool((payload or {}).get("start"))
    try:
        summary = await state.sync.sync_sub(sub, mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RedditError as exc:
        # Fout is ook in de queue vastgelegd; hier duidelijke melding voor de UI.
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if force_start:
        await state.engine.resume()  # start of hervat, ook na pauze
    elif state.engine.state == "stopped":
        await state.engine.start()
    return summary


# --------------------------------------------------------------------------- queue
@app.get("/api/queue")
async def api_queue(limit: int = 500) -> dict[str, Any]:
    limit = max(1, min(limit, 2000))
    rows = await state.db.query(
        """
        SELECT id, sub, post_id, url, status, attempts, error, downloaded_at,
               post_title, bytes_done
        FROM downloads
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    return {"items": rows}


@app.post("/api/queue/{item_id}/retry")
async def api_queue_retry(item_id: int) -> dict[str, Any]:
    assert state.engine is not None
    if not await state.engine.retry_item(item_id):
        raise HTTPException(status_code=404, detail="Item niet gevonden of niet in status failed/skipped")
    return {"ok": True}


@app.post("/api/queue/redownload")
async def api_queue_redownload(payload: dict) -> dict[str, Any]:
    """Her-download: `done`-items (optioneel per subreddit) terug op pending."""
    assert state.engine is not None
    sub = sanitize_sub(str((payload or {}).get("sub", "") or "")) or None
    n = await state.engine.redownload(sub)
    log.info("Her-download gestart: %d items%s", n, f" ({sub})" if sub else "")
    return {"ok": True, "count": n}


# --------------------------------------------------------------------------- history
@app.get("/api/history")
async def api_history(sub: Optional[str] = None, limit: int = 300) -> dict[str, Any]:
    limit = max(1, min(limit, 1000))
    sql = """
        SELECT sub, post_id,
               COALESCE(MAX(post_date), '') AS post_date,
               COALESCE(MAX(post_title), '') AS post_title,
               COUNT(*) AS files,
               SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done,
               SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
               SUM(CASE WHEN status IN ('pending', 'running') THEN 1 ELSE 0 END) AS active,
               MAX(downloaded_at) AS last_activity
        FROM downloads
        WHERE url != ''
    """
    params: list[Any] = []
    if sub:
        sql += " AND sub = ?"
        params.append(sanitize_sub(sub))
    sql += " GROUP BY sub, post_id ORDER BY last_activity DESC, sub, post_id LIMIT ?"
    params.append(limit)
    rows = await state.db.query(sql, tuple(params))
    for row in rows:
        if row["active"]:
            row["status"] = "in_progress"
        elif row["done"] and not row["failed"]:
            row["status"] = "done"
        elif row["done"]:
            row["status"] = "partial"
        elif row["failed"]:
            row["status"] = "failed"
        else:
            row["status"] = "skipped"
    return {"posts": rows}


@app.post("/api/history/delete")
async def api_history_delete(payload: dict) -> dict[str, Any]:
    sub = sanitize_sub((payload or {}).get("sub", ""))
    post_id = str((payload or {}).get("post_id", "")).strip()
    if not sub or not post_id:
        raise HTTPException(status_code=400, detail="sub en post_id vereist")
    # Bestanden alleen verwijderen als géén andere rij (bv. in een andere sub)
    # dezelfde bestandsreferentie heeft.
    root_p = Path(resolve_dataroot(state.settings.get("dataroot", DEFAULT_DATAROOT))).resolve()
    post_paths = {
        r["local_path"]
        for r in await state.db.query(
            "SELECT DISTINCT local_path FROM downloads WHERE sub = ? AND post_id = ? AND local_path IS NOT NULL AND local_path != ''",
            (sub, post_id),
        )
    }
    other_paths = {
        r["local_path"]
        for r in await state.db.query(
            "SELECT DISTINCT local_path FROM downloads "
            "WHERE local_path IS NOT NULL AND local_path != '' AND NOT (sub = ? AND post_id = ?)",
            (sub, post_id),
        )
    }
    removed = 0
    for path in post_paths - other_paths:  # alleen bestanden die exclusief van deze post zijn
        try:
            Path(path).resolve().relative_to(root_p)  # moet ín de dataroot liggen
        except (ValueError, OSError):
            continue
        try:
            Path(path).unlink()
            removed += 1
        except OSError:
            pass
    await state.db.execute("DELETE FROM downloads WHERE sub = ? AND post_id = ?", (sub, post_id))
    log.info("History-verwijderd: %s/%s (%d bestanden)", sub, post_id, removed)
    return {"ok": True, "files_removed": removed}


# --------------------------------------------------------------------------- migratie (flat structuur)
def _unique_dest(dest: Path) -> Path:
    """Bestaand bestand → `_2`, `_3`, …-suffix."""
    if not dest.exists():
        return dest
    for i in range(2, 1000):
        cand = dest.with_name(f"{dest.stem}_{i}{dest.suffix}")
        if not cand.exists():
            return cand
    return dest.with_name(f"{dest.stem}_{int(time.time())}{dest.suffix}")


_MONTH_DIR = re.compile(r"^\d{4}-\d{2}$")


async def _flatten_sub(base: Path, sub: str, skip_month_dirs: bool = False) -> dict[str, Any]:
    """Bestanden in `<base>/<sub>/*/<...>` één niveau omhoog verplaatsen.

    Retourneert o.a. `relocated`: [(oud_pad, nieuw_pad), …] voor de DB-update.
    """
    sub_dir = base / sub
    moved, skipped = 0, 0
    relocated: list[tuple[str, str]] = []
    if not sub_dir.is_dir():
        return {"moved": 0, "skipped": 0, "removed_dirs": 0, "relocated": []}
    for child in sorted(sub_dir.iterdir()):
        if not child.is_dir():
            continue
        if skip_month_dirs and _MONTH_DIR.match(child.name):
            continue  # maand-map is intentioneel (date_subfolder=month)
        for f in sorted(p for p in child.rglob("*") if p.is_file()):
            dest = _unique_dest(sub_dir / f.name)
            try:
                shutil.move(str(f), str(dest))
                moved += 1
                relocated.append((str(f), str(dest)))
            except OSError as exc:
                log.warning("Migratie: kon %s niet verplaatsen: %s", f, exc)
                skipped += 1
    # Lege post-mapjes opruimen
    removed_dirs = 0
    for child in sorted(sub_dir.iterdir()):
        if child.is_dir():
            try:
                if not any(child.iterdir()):
                    child.rmdir()
                    removed_dirs += 1
            except OSError:
                pass
    return {"moved": moved, "skipped": skipped, "removed_dirs": removed_dirs,
            "relocated": relocated}


@app.post("/api/migrate/flatten")
async def api_migrate_flatten() -> dict[str, Any]:
    """Vlakke mapstructuur: `<sub>/post-mapje/foto.jpg` → `<sub>/foto.jpg`. Idempotent."""
    base = Path(resolve_dataroot(state.settings.get("dataroot", DEFAULT_DATAROOT))).resolve()
    skip_month_dirs = state.settings.get("date_subfolder") == "month"
    total_moved, total_skipped, total_dirs = 0, 0, 0
    relocated: list[tuple[str, str]] = []
    per_sub: dict[str, dict[str, int]] = {}
    if base.is_dir():
        for sub_dir in sorted(base.iterdir()):
            if not sub_dir.is_dir():
                continue
            result = await _flatten_sub(base, sub_dir.name, skip_month_dirs=skip_month_dirs)
            relocated.extend(result.pop("relocated", []))
            per_sub[sub_dir.name] = result
            total_moved += result["moved"]
            total_skipped += result["skipped"]
            total_dirs += result["removed_dirs"]
    # DB bijwerken: galerij/dedup/delete lezen `local_path` uit downloads.
    updated_rows = 0
    for old, new in relocated:
        cur = await state.db.execute(
            "UPDATE downloads SET local_path = ? WHERE local_path = ?", (new, old)
        )
        updated_rows += cur.rowcount if cur else 0
    log.info("Migratie naar vlakke structuur: %d bestanden verplaatst, %d overgeslagen, %d DB-rijen bijgewerkt",
             total_moved, total_skipped, updated_rows)
    return {"ok": True, "moved": total_moved, "skipped": total_skipped,
            "removed_dirs": total_dirs, "db_updated": updated_rows, "per_sub": per_sub}


# --------------------------------------------------------------------------- dedup
@app.get("/api/dedup/similar")
async def api_dedup_similar() -> dict[str, Any]:
    """Groepen van identieke bestanden (zelfde sha256, status done)."""
    dataroot = resolve_dataroot(state.settings.get("dataroot", DEFAULT_DATAROOT))
    groups = await state.db.query(
        """
        SELECT sha256, COUNT(*) AS c, GROUP_CONCAT(id) AS ids
        FROM downloads
        WHERE status = 'done' AND sha256 IS NOT NULL AND sha256 != ''
        GROUP BY sha256 HAVING c > 1
        ORDER BY c DESC
        """
    )
    out = []
    for g in groups:
        ids = [int(x) for x in g["ids"].split(",") if x.strip()]
        keep_id = min(ids)
        keep = await state.db.one("SELECT * FROM downloads WHERE id = ?", (keep_id,))
        if keep is None:
            continue  # rij verdween tussen de queries
        others = await state.db.query(
            "SELECT * FROM downloads WHERE id IN (%s)" % ",".join("?" * (len(ids) - 1)),
            tuple(i for i in ids if i != keep_id),
        )
        out.append({
            "sha256": g["sha256"][:16],
            "count": g["c"],
            "keep": {"id": keep_id, "sub": keep["sub"], "post_id": keep["post_id"],
                     "post_title": keep["post_title"] or "",
                     "local_path": _relpath(dataroot, keep["local_path"])},
            "delete": [
                {"id": r["id"], "sub": r["sub"], "post_id": r["post_id"],
                 "post_title": r["post_title"] or "",
                 "local_path": _relpath(dataroot, r["local_path"]),
                 "local_path_abs": r["local_path"] or ""}
                for r in others
            ],
        })
    return {"groups": out, "total_groups": len(out)}


def _relpath(dataroot: str, local_path: str | None) -> str:
    if not local_path:
        return ""
    try:
        return str(Path(local_path).resolve().relative_to(Path(dataroot).resolve()))
    except ValueError:
        return local_path


@app.post("/api/dedup/delete")
async def api_dedup_delete(payload: dict) -> dict[str, Any]:
    """Verwijder één duplicaat-rij + bestand (alleen als geen andere rij het deelt)."""
    item_id = int((payload or {}).get("id", 0))
    if not item_id:
        raise HTTPException(status_code=400, detail="id vereist")
    row = await state.db.one("SELECT * FROM downloads WHERE id = ?", (item_id,))
    if row is None:
        raise HTTPException(status_code=404, detail="Item niet gevonden")
    removed = 0
    local_path = row["local_path"]
    if local_path:
        others = await state.db.query(
            "SELECT id FROM downloads WHERE id != ? AND local_path = ?", (item_id, local_path)
        )
        if not others:
            try:
                Path(local_path).unlink()
                removed = 1
            except OSError as exc:
                log.warning("Dedup: bestand verwijderen mislukt (%s): %s", local_path, exc)
    await state.db.execute("DELETE FROM downloads WHERE id = ?", (item_id,))
    log.info("Dedup: rij %d verwijderd (bestand: %d)", item_id, removed)
    return {"ok": True, "file_removed": removed}


# --------------------------------------------------------------------------- reddit (cookie-sessie)
@app.post("/api/reddit/verify")
async def api_reddit_verify() -> dict[str, Any]:
    """Controleer of de opgeslagen sessie-cookie nog geldig is."""
    assert state.reddit is not None
    try:
        result = await state.reddit.verify_session()
    except RedditError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error") or "Sessie ongeldig.")
    return result


# --------------------------------------------------------------------------- gallery / media
@app.get("/api/media")
async def api_media(
    sub: Optional[str] = None,
    type: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    """Lijst gedownloade media-bestanden (status='done') voor de galerij."""
    limit = max(1, min(limit, 1000))
    offset = max(0, offset)

    base_where = "status = 'done' AND local_path IS NOT NULL AND local_path != ''"
    ext_map = {
        "photo": (".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff"),
        "video": (".mp4", ".webm", ".mkv", ".mov"),
        "gif": (".gif",),
    }
    where_extra = ""
    params: list[Any] = []
    if sub:
        where_extra += " AND sub = ?"
        params.append(sanitize_sub(sub))
    if type in ext_map:
        exts = ext_map[type]
        where_extra += " AND (" + " OR ".join(["LOWER(local_path) LIKE ?" for _ in exts]) + ")"
        params.extend([f"%{e}" for e in exts])

    sql = (
        "SELECT local_path, sub, post_id, post_date, url, downloaded_at, post_title, post_author, bytes_done "
        f"FROM downloads WHERE {base_where}{where_extra} "
        "ORDER BY downloaded_at DESC LIMIT ? OFFSET ?"
    )
    rows = await state.db.query(sql, tuple(params + [limit, offset]))

    # `media_type` afleiden uit de extensie, zodat de galerij video's/gif's
    # als <video> rendert in plaats van als (kapotte) <img>.
    ext_to_type = {ext: t for t, exts in ext_map.items() for ext in exts}
    # Echte bestandsgrootte van disk (authoritatief) — lost de oude (throttléde)
    # bytes_done-waarden retroactief op voor reeds gedownloade items.
    base = Path(resolve_dataroot(state.settings.get("dataroot", DEFAULT_DATAROOT))).resolve()
    items: list[dict[str, Any]] = []
    for r in rows:
        r = dict(r)
        lp = r.get("local_path") or ""
        ext = Path(lp).suffix.lower()
        r["media_type"] = ext_to_type.get(ext, "photo")
        size = int(r.get("bytes_done") or 0)
        if lp:
            target = (base / lp).resolve()
            if target.is_relative_to(base) and target.is_file():
                try:
                    size = target.stat().st_size
                except OSError:
                    pass
        r["size"] = size
        items.append(r)

    count_sql = f"SELECT COUNT(*) AS c FROM downloads WHERE {base_where}{where_extra}"
    total_row = await state.db.one(count_sql, tuple(params))
    total = total_row["c"] if total_row else 0

    sub_rows = await state.db.query(
        f"SELECT DISTINCT sub FROM downloads WHERE {base_where} ORDER BY sub"
    )
    return {"items": items, "total": total, "subs": [r["sub"] for r in sub_rows]}


@app.get("/media/{path:path}")
async def serve_media(path: str) -> FileResponse:
    """Dien gedownloade media bestanden op (met path-traversal bescherming)."""
    dataroot = resolve_dataroot(state.settings.get("dataroot", DEFAULT_DATAROOT))
    base = Path(dataroot).resolve()
    target = (base / path).resolve()
    # Veilig: `is_relative_to` voorkomt ook sibling-dirs (`/data/media-evil`).
    if not target.is_relative_to(base):
        raise HTTPException(status_code=403, detail="Toegang geweigerd")
    if not target.is_file():
        raise HTTPException(status_code=404, detail="Bestand niet gevonden")
    mime = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
    return FileResponse(str(target), media_type=mime, filename=target.name)


# --------------------------------------------------------------------------- settings
@app.get("/api/settings")
async def api_get_settings() -> dict[str, Any]:
    return {"settings": dict(state.settings)}


@app.post("/api/settings")
async def api_set_settings(payload: dict) -> dict[str, Any]:
    """Settings updaten. Waarden worden genormaliseerd en geverifieerd."""
    patch: dict[str, Any] = {}
    for key, value in (payload or {}).items():
        if key not in DEFAULTS:
            continue
        if key == "file_types":
            if isinstance(value, dict):
                patch[key] = {k: bool(v) for k, v in value.items() if k in ("photo", "video", "gif")}
            elif isinstance(value, (list, str)):
                allowed = {t.strip().lower() for t in (value if isinstance(value, list) else value.split(",")) if t.strip()}
                patch[key] = {"photo": "photo" in allowed,
                              "video": "video" in allowed,
                              "gif": "gif" in allowed}
            continue
        if key in ("concurrency", "auto_sync_minutes"):
            try:
                v = int(value)
            except (TypeError, ValueError) as exc:
                raise HTTPException(status_code=400, detail=f"{key} moet een getal zijn") from exc
            lo, hi = (1, 16) if key == "concurrency" else (0, 1440)
            patch[key] = max(lo, min(hi, v))
        elif key == "request_interval":
            try:
                patch[key] = max(0.5, min(60.0, float(value)))
            except (TypeError, ValueError) as exc:
                raise HTTPException(status_code=400, detail="request_interval moet een getal zijn (seconden)") from exc
        elif key == "photo_quality":
            try:
                patch[key] = max(40, min(100, int(value)))
            except (TypeError, ValueError) as exc:
                raise HTTPException(status_code=400, detail="photo_quality moet een getal zijn (40-100)") from exc
        elif key == "video_quality":
            try:
                patch[key] = max(0, min(51, int(value)))
            except (TypeError, ValueError) as exc:
                raise HTTPException(status_code=400, detail="video_quality moet een getal zijn (0-51)") from exc
        elif key == "naming_mode":
            patch[key] = value if value in ("flat", "posts") else "flat"
        elif key == "date_subfolder":
            patch[key] = value if value in ("none", "month") else "none"
        elif key in ("photo_format", "video_format", "gif_mode"):
            patch[key] = str(value or "").strip() or DEFAULTS[key]
        else:
            patch[key] = "" if value is None else str(value)
    # In-place mutatie: engine/sync/downloader delen dezelfde dict-referentie.
    try:
        state.settings.update(save_settings(patch))
    except RuntimeError as exc:
        # Duidelijke melding i.p.v. een kale 500 (bijv. /config niet schrijfbaar).
        log.error("Settings opslaan mislukt: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    log.info("Settings bijgewerkt: %s", sorted(patch))
    return {"settings": dict(state.settings)}


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"ok": "true"}
