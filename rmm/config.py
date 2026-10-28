"""Configuration: env-driven defaults merged with user JSON settings."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

APP_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("RMM_DATA", APP_ROOT / "data"))
SETTINGS_PATH = DATA_DIR / "settings.json"


DEFAULTS: dict[str, Any] = {
    "dataroot": os.environ.get("RMM_DATAROOT", "/data/media"),
    "concurrency": int(os.environ.get("RMM_CONCURRENCY", "4")),
    "request_interval": float(os.environ.get("RMM_REQUEST_INTERVAL", "6.0")),
    "user_agent": os.environ.get(
        "RMM_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ),
    # Cookie-based auth (primary method)
    "reddit_username": "",
    "reddit_session": "",
    "reddit_loid": "",
    # Storage / file handling
    "file_types": {"photo": True, "video": True, "gif": True},
    "photo_format": "jpeg",
    "video_format": "mp4",
    "gif_mode": "keep",
    # Naming & structure
    "naming_mode": "flat",          # "flat" or "posts"
    "date_subfolder": "none",       # "none" or "month"
    # Quality
    "photo_quality": 95,
    "video_quality": 18,
    # Auto-sync
    "auto_sync_minutes": 0,         # 0 = disabled
}


def _normalize_file_types(raw: Any) -> dict[str, bool]:
    """file_types als dict, of legacy comma-string ('photo,video,gif') → dict."""
    if isinstance(raw, dict):
        return {k: bool(v) for k, v in raw.items() if k in ("photo", "video", "gif")}
    if isinstance(raw, str):
        enabled = {t.strip().lower() for t in raw.split(",") if t.strip()}
        return {k: (k in enabled) for k in ("photo", "video", "gif")}
    return dict(DEFAULTS["file_types"])


def load_settings() -> dict[str, Any]:
    """Merge DEFAULTS with persisted JSON (JSON wins)."""
    if SETTINGS_PATH.exists():
        try:
            stored = json.loads(SETTINGS_PATH.read_text("utf-8"))
            if not isinstance(stored, dict):
                raise ValueError("verwacht een JSON-object")
            merged = {**DEFAULTS, **stored}
            # file_types kan dict (nieuw) of legacy comma-string (oud bestand) zijn
            merged["file_types"] = _normalize_file_types(stored.get("file_types"))
            return merged
        except Exception as exc:
            # Niet zwijgend terugvallen op defaultwaarden: de gebruiker moet
            # weten dat zijn opgeslagen instellingen (o.a. Reddit-cookie) niet
            # zijn gelezen. Zie: legacy file_types-string veroorzaakte een
            # TypeError die hier onzichtbaar werd opgeslokt.
            logging.getLogger("rmm.config").error(
                "Kon %s niet lezen (%s: %s); gebruik default instellingen.",
                SETTINGS_PATH, type(exc).__name__, exc,
            )
    return dict(DEFAULTS)


def save_settings(patch: dict[str, Any]) -> dict[str, Any]:
    """Apply a partial update and persist. Returns the full new settings."""
    current = load_settings()
    for k, v in patch.items():
        if k in DEFAULTS:
            current[k] = v
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SETTINGS_PATH.write_text(json.dumps(current, indent=2, ensure_ascii=False), "utf-8")
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
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
