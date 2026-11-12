"""Configuration: env-driven defaults merged with user JSON settings."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

APP_ROOT = Path(__file__).resolve().parent.parent

# /config → app-state: settings.json, state.db, app.log
# (fallback op legacy `RMM_DATA` zodat bestaande installs niet breken)
_raw_config = (os.environ.get("RMM_CONFIG") or os.environ.get("RMM_DATA") or "").strip()
CONFIG_DIR = Path(_raw_config).expanduser() if _raw_config else APP_ROOT / "config"
SETTINGS_PATH = CONFIG_DIR / "settings.json"
# Reddit-cookies opzichzelf in een apart 0600-bestand, zodat ze niet bij de
# overige instellingen (die vaker worden gelezen/geëxporteerd) liggen.
COOKIES_PATH = CONFIG_DIR / "cookies.json"

# /data → gedownloade media (subreddit-mappen); in Docker standaard /data
DEFAULT_DATAROOT = os.environ.get("RMM_DATAROOT", str(APP_ROOT / "data"))


DEFAULTS: dict[str, Any] = {
    "dataroot": DEFAULT_DATAROOT,
    "concurrency": int(os.environ.get("RMM_CONCURRENCY", "2")),
    "request_interval": float(os.environ.get("RMM_REQUEST_INTERVAL", "6.0")),
    # Minimale pauze (sec) tussen het *starten* van nieuwe downloads over alle
    # workers heen — dempt de initieele burst zodat de CDN minder snel 429 geeft.
    "download_spacing": float(os.environ.get("RMM_DOWNLOAD_SPACING", "2.0")),
    "user_agent": os.environ.get(
        "RMM_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ),
    # Cookie-based auth (primary method) — daadwerkelijke waarden staan in
    # cookies.json / env-vars; hier blijven ze als (lege) placeholder in de defaults.
    "reddit_username": "",
    "reddit_session": "",
    "reddit_loid": "",
    # Storage / file handling
    "file_types": {"photo": True, "video": True, "gif": True},
    "photo_format": "jpeg",
    "video_format": "mp4",
    "gif_mode": "keep",
    # Maximale download-snelheid in MB/s (0 = onbeperkt)
    "max_bandwidth_mb": 0,
    # Optionele proxy voor API + downloads (bijv. "http://host:port" of "socks5://host:port").
    # Leeg = directe verbinding. Handig als Reddit de lokale IP blokkeert (403 van de CDN).
    "proxy_url": "",
    # Naming & structure
    "naming_mode": "flat",          # "flat" or "posts"
    "date_subfolder": "none",       # "none" or "month"
    # Quality
    "photo_quality": 95,
    "video_quality": 18,
    # Auto-sync
    "auto_sync_minutes": 0,         # 0 = disabled
}


def _read_cookies_file() -> dict[str, str]:
    """Lees cookies.json (0600). Ontbreekt het bestand → lege dict."""
    if not COOKIES_PATH.exists():
        return {}
    try:
        data = json.loads(COOKIES_PATH.read_text("utf-8"))
        if isinstance(data, dict):
            return {
                "reddit_username": str(data.get("reddit_username", "")),
                "reddit_session": str(data.get("reddit_session", "")),
                "reddit_loid": str(data.get("reddit_loid", "")),
            }
    except Exception as exc:  # noqa: BLE001
        logging.getLogger("rmm.config").warning("Kon cookies.json niet lezen: %s", exc)
    return {}


def _write_cookies_file(c: dict[str, str]) -> None:
    """Schrijf cookies.json met 0600-permissies."""
    COOKIES_PATH.parent.mkdir(parents=True, exist_ok=True)
    COOKIES_PATH.write_text(json.dumps(c, indent=2, ensure_ascii=False), "utf-8")
    try:
        os.chmod(COOKIES_PATH, 0o600)
    except OSError:
        pass


def _migrate_cookies_from_settings() -> None:
    """Eénmalige migratie: cookies uit settings.json → cookies.json (0600)."""
    if COOKIES_PATH.exists():
        return
    if not SETTINGS_PATH.exists():
        return
    try:
        stored = json.loads(SETTINGS_PATH.read_text("utf-8"))
    except Exception:
        return
    cookies = {
        "reddit_username": stored.get("reddit_username", ""),
        "reddit_session": stored.get("reddit_session", ""),
        "reddit_loid": stored.get("reddit_loid", ""),
    }
    if any(cookies.values()):
        _write_cookies_file(cookies)
        # Verwijder cookies uit settings.json (wordt later opnieuw geschreven)
        for k in ("reddit_username", "reddit_session", "reddit_loid"):
            stored.pop(k, None)
        SETTINGS_PATH.write_text(json.dumps(stored, indent=2, ensure_ascii=False), "utf-8")
        logging.getLogger("rmm.config").info("Cookies gemigreerd naar cookies.json (0600)")


def _normalize_file_types(raw: Any) -> dict[str, bool]:
    """file_types als dict, of legacy comma-string ('photo,video,gif') → dict."""
    if isinstance(raw, dict):
        return {k: bool(v) for k, v in raw.items() if k in ("photo", "video", "gif")}
    if isinstance(raw, str):
        enabled = {t.strip().lower() for t in raw.split(",") if t.strip()}
        return {k: (k in enabled) for k in ("photo", "video", "gif")}
    return dict(DEFAULTS["file_types"])


def load_settings() -> dict[str, Any]:
    """Merge DEFAULTS with persisted JSON (JSON wins) + cookies (cookies.json / env)."""
    _migrate_cookies_from_settings()

    settings: dict[str, Any] = dict(DEFAULTS)
    if SETTINGS_PATH.exists():
        try:
            stored = json.loads(SETTINGS_PATH.read_text("utf-8"))
            if not isinstance(stored, dict):
                raise ValueError("verwacht een JSON-object")
            settings.update(stored)
            # file_types kan dict (nieuw) of legacy comma-string (oud bestand) zijn
            settings["file_types"] = _normalize_file_types(stored.get("file_types"))
        except Exception as exc:
            logging.getLogger("rmm.config").error(
                "Kon %s niet lezen (%s: %s); gebruik default instellingen.",
                SETTINGS_PATH, type(exc).__name__, exc,
            )

    # Cookies: cookies.json heeft voorrang, dan env-vars (RMM_REDDIT_*).
    file_cookies = _read_cookies_file()
    env_map = {
        "reddit_username": "RMM_REDDIT_USERNAME",
        "reddit_session": "RMM_REDDIT_SESSION",
        "reddit_loid": "RMM_REDDIT_LOID",
    }
    for key, env_name in env_map.items():
        env_val = (os.environ.get(env_name) or "").strip()
        file_val = (file_cookies.get(key) or "").strip()
        settings[key] = env_val or file_val

    return settings


_COOKIE_KEYS = ("reddit_username", "reddit_session", "reddit_loid")


def save_settings(patch: dict[str, Any]) -> dict[str, Any]:
    """Apply a partial update and persist. Returns the full new settings."""
    current = load_settings()
    for k, v in patch.items():
        if k in DEFAULTS:
            current[k] = v
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    # Cookies naar cookies.json (0600); overige instellingen naar settings.json.
    cookie_patch = {k: str(current.get(k, "")) for k in _COOKIE_KEYS}
    non_cookie = {k: v for k, v in current.items() if k not in _COOKIE_KEYS}

    _write_cookies_file(cookie_patch)

    try:
        SETTINGS_PATH.write_text(json.dumps(non_cookie, indent=2, ensure_ascii=False), "utf-8")
    except PermissionError as exc:
        raise RuntimeError(
            "Kan instellingen niet opslaan: de map /config is niet schrijfbaar door "
            "de app-gebruiker (rmm). Los op met: docker exec -u root <container> "
            "chown -R rmm:rmm /config  (of gebruik het image met de auto-chown "
            f"entrypoint). Oorsprong: {exc}"
        ) from exc
    except OSError as exc:
        raise RuntimeError(
            f"Kan instellingen niet opslaan naar {SETTINGS_PATH}: {exc}"
        ) from exc
    return current


def get(k: str, default: Any = None) -> Any:
    return load_settings().get(k, DEFAULTS.get(k, default))


@dataclass
class Config:
    """Typed view of the most-used settings (loaded once per engine run)."""
    dataroot: str
    concurrency: int
    request_interval: float
    user_agent: str
    reddit_username: str
    reddit_session: str
    reddit_loid: str
    file_types: dict[str, bool]
    photo_format: str
    video_format: str
    gif_mode: str
    naming_mode: str
    date_subfolder: str
    photo_quality: int
    video_quality: int
    auto_sync_minutes: int
    max_bandwidth_mb: int

    @classmethod
    def from_settings(cls) -> "Config":
        s = load_settings()
        return cls(
            dataroot=s["dataroot"],
            concurrency=int(s["concurrency"]),
            request_interval=float(s["request_interval"]),
            user_agent=s["user_agent"],
            reddit_username=s.get("reddit_username", ""),
            reddit_session=s.get("reddit_session", ""),
            reddit_loid=s.get("reddit_loid", ""),
            file_types=s.get("file_types", dict(DEFAULTS["file_types"])),
            photo_format=s.get("photo_format", "jpeg"),
            video_format=s.get("video_format", "mp4"),
            gif_mode=s.get("gif_mode", "keep"),
            naming_mode=s.get("naming_mode", "flat"),
            date_subfolder=s.get("date_subfolder", "none"),
            photo_quality=int(s.get("photo_quality", 95)),
            video_quality=int(s.get("video_quality", 18)),
            auto_sync_minutes=int(s.get("auto_sync_minutes", 0)),
            max_bandwidth_mb=int(s.get("max_bandwidth_mb", 0) or 0),
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
