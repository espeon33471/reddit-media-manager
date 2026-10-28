"""Kleine hulpmiddelen: logging, sanitiseren, tijdsstempels."""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

_CONFIGURED = False


def setup_logging(log_file: str = "app.log") -> None:
    """Console + roterend app.log. Veilig meerdere keren aanroepen."""
    global _CONFIGURED
    if _CONFIGURED:
        return
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(sh)

    try:
        fh = RotatingFileHandler(log_file, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
    except OSError:
        # Geen schrijftoegang voor het logbestand — alleen console, geen crash.
        pass

    # httpx/uvicorn wat zachter houden
    logging.getLogger("httpx").setLevel(logging.WARNING)
    _CONFIGURED = True


def data_dir() -> "Path | None":
    """Env-var `RMM_DATA`: één map voor state.db, log en media (bv. /data in Docker).

    Leeg/onopgegeven → None: alles blijft in de projectmap (standaardgedrag).
    """
    raw = (os.environ.get("RMM_DATA") or "").strip()
    return Path(raw).expanduser() if raw else None


def now_iso() -> str:
    """UTC ISO-tijdstempel (seconden-nauwkeurigheid)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sanitize_sub(name: str) -> str:
    """Reddit-naam naar veilige map-/DB-naam.

    Verwijdert optionele 'r/' of volledige URL, vervangt ongeldige
    karakters door underscore en voorkomt lege namen.
    """
    name = (name or "").strip()
    if not name:
        return ""
    name = re.sub(r"^https?://(www\.)?reddit\.com/r/", "", name, flags=re.I)
    name = re.sub(r"^r/", "", name, flags=re.I)
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_-]+", "_", name).strip("_")
    return name


def sanitize_stem(name: str, fallback: str = "file", max_len: int = 80) -> str:
    """Bestandsnaam-stem: geen spaties/special chars, lengte-beperkt."""
    stem = (name or "").strip()
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._")
    if not stem:
        return fallback
    if len(stem) > max_len:
        stem = stem[:max_len].strip("._") or fallback
    return stem
