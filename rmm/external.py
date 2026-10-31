"""Externe video-embeds (RedGifs / Gifer) → directe MP4-URL.

Reddit host deze GIFs zelf niet: de post bevat alleen een statische preview
(`.jpeg`) + een embed/iframe naar `redgifs.com/watch/{id}` (of `gifer.com/{id}`).
Eerder downloadde de engine daardoor een *statisch frame* in plaats van de
animerde GIF.

Hier fetchen we de watch-pagina en extraheren de `media.redgifs.com` MP4, zodat
de animerde GIF full-res gedownload wordt. De statische preview blijft als
fallback-kandidaat.

Alleen dit pad is getest en werkend (watch-pagina parsen). Alternatieven
(`/watch/{id}/download` → HTML, `api.redgifs.com/v2/gifs/{id}` → 401) werken NIET.
"""
from __future__ import annotations

import logging
import re
from typing import Any

import httpx

log = logging.getLogger("rmm.external")

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# redgifs.com/(watch|ifr)/{id}   of   gifer.com/(watch|ifr)/{id}
_EMBED_RE = re.compile(
    r"(?:redgifs\.com|gifer\.com)/(?:watch|ifr)/([0-9A-Za-z]+)",
    re.IGNORECASE,
)
# Directe media-URL's op de CDN.
_MP4_RE = re.compile(
    r"https?://media\.(?:redgifs|gifer)\.com/[^\"'\s<>\\]+?\.mp4",
    re.IGNORECASE,
)


def _iter_haystacks(data: dict[str, Any]) -> list[str]:
    """Alle velden verzamelen die een embed-URL kunnen bevatten."""
    out: list[str] = []
    for key in ("url_overridden_by_dest", "url"):
        v = data.get(key)
        if isinstance(v, str):
            out.append(v)
    media = data.get("media") or {}
    if not isinstance(media, dict):
        media = {}
    embed = media.get("media_embed") or {}
    if isinstance(embed, dict):
        v = embed.get("content")
        if isinstance(v, str):
            out.append(v)
    oembed = media.get("oembed") or {}
    if isinstance(oembed, dict):
        for k in ("html", "url"):
            v = oembed.get(k)
            if isinstance(v, str):
                out.append(v)
    return out


def detect_external_video(data: dict[str, Any]) -> tuple[str, str] | None:
    """Return `(host, id)` als de post een RedGifs/Gifer-embed bevat, anders `None`."""
    for h in _iter_haystacks(data):
        m = _EMBED_RE.search(h or "")
        if m:
            host = "redgifs" if "redgifs" in h.lower() else "gifer"
            return host, m.group(1)
    return None


async def resolve_external_video(
    data: dict[str, Any],
    client: httpx.AsyncClient | None = None,
) -> list[str] | None:
    """Los een externe video-embed op naar geordende MP4-kandidaten (beste eerst).

    Retourneert `None` als de post geen RedGifs/Gifer-embed bevat of het oplossen
    faalt (dan valt de engine terug op de statische preview).
    """
    hit = detect_external_video(data)
    if not hit:
        return None
    host, gid = hit
    cands: list[str] = []

    # Gifer: de directe media-URL werkt meestal zonder pagina-fetch.
    if host == "gifer":
        cands.append(f"https://media.gifer.com/{gid}.mp4")

    # RedGifs: watch-pagina parsen → media.redgifs.com MP4.
    if host == "redgifs":
        watch = f"https://www.redgifs.com/watch/{gid}"
        ref = "https://www.redgifs.com/"
    else:
        watch = f"https://www.gifer.com/{gid}"
        ref = "https://www.gifer.com/"

    own = client is None
    http = client or httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0))
    try:
        headers = {
            "User-Agent": _BROWSER_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": ref,
        }
        resp = await http.get(watch, headers=headers, follow_redirects=True)
        if resp.status_code == 200:
            m = _MP4_RE.search(resp.text)
            if m:
                cands.insert(0, m.group(0))
                log.info("Externe video opgelost: %s → %s", watch, m.group(0))
        else:
            log.warning("Watch-pagina %s → HTTP %s", watch, resp.status_code)
    except httpx.HTTPError as exc:
        log.warning("Externe video oplossen gefaald (%s): %s", watch, exc)
    finally:
        if own:
            await http.aclose()

    # Dedup, volgorde behouden.
    seen: set[str] = set()
    out: list[str] = []
    for c in cands:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out or None
