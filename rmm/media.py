"""Media-extractie per post, in de vereiste prioriteit.

  1. data.media.reddit_video.fallback_url      → video
  2. data.preview.images[].source.url          → afbeelding/gif
  3. post_url eindigt op een media-extensie   → directe link

Posts zonder media (tekst/externe link) leveren een lege lijst.
"""
from __future__ import annotations

import os
import re
from typing import Any

MEDIA_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4", ".mov", ".m4v"}


def url_extension(url: str) -> str:
    """Kleine-extensie (zonder punt) van een URL, query/fragment genegeerd."""
    path = (url or "").split("?", 1)[0].split("#", 1)[0]
    return os.path.splitext(path)[1].lstrip(".").lower()


# ---------------------------------------------------------------------------
# Media-type classificatie
# ---------------------------------------------------------------------------

PHOTO_EXTS = {"jpg", "jpeg", "png", "webp", "bmp", "tiff", "tif"}
VIDEO_EXTS = {"mp4", "mov", "m4v", "webm", "mkv", "avi", "flv"}
GIF_EXTS = {"gif"}


def classify_media(url: str) -> str | None:
    """Classificeer een media-URL als 'photo', 'video' of 'gif'. None als onbekend."""
    ext = url_extension(url)
    if ext in PHOTO_EXTS:
        return "photo"
    if ext in GIF_EXTS:
        return "gif"
    if ext in VIDEO_EXTS:
        return "video"
    # Fallback: Reddit's fallback_url is meestal mp4
    if "fallback_url" in url or ".mp4" in url:
        return "video"
    if ".gif" in url:
        return "gif"
    return None


def _is_media_url(url: str) -> bool:
    return url_extension(url) in MEDIA_EXTS


# Quality-suffixes die een lagere resolutie forceren (:s1024, :h96, :medium, ...)
_QUALITY_SUFFIX_RE = re.compile(r":(?:s\d+|h\d+|medium|large|placeholder|original)(?![A-Za-z0-9])")


def strip_quality_suffixes(url: str) -> str:
    """Verwijder resolutie-limiters zodat de hoogste resolutie wordt gedownload.

    - path-suffixes:  :s1024, :h96, :medium, :placeholder
    - query-params:   auto=webp (houdt native format), s=NNN (max dimensie)
    """
    url = _QUALITY_SUFFIX_RE.sub("", url or "")
    url = re.sub(r"([?&])auto=[^&]*", "", url)
    url = re.sub(r"([?&])s=\d+", "", url)
    url = url.replace("?&", "?")
    url = url.rstrip("?&")
    return url


def extract_media_urls(post: dict[str, Any]) -> list[str]:
    """Keer een geordende, dedupliceerde lijst media-URLs terug (kan leeg zijn)."""
    data = post.get("data") or {}

    # 1) Reddit video — heeft voorrang, dan stoppen we.
    rv = (data.get("media") or {}).get("reddit_video") or {}
    fallback = rv.get("fallback_url")
    if isinstance(fallback, str) and fallback:
        return [strip_quality_suffixes(fallback)]

    # 2) Preview-afbeeldingen (ook albums en GIFs).
    #    Voor GIFs staat de échte geanimeerde URL in variants.gif.source.url;
    #    source.url is daar vaak een stille frame (placeholder). variants.gif EERST.
    urls: list[str] = []
    for img in (data.get("preview") or {}).get("images") or []:
        if not isinstance(img, dict):
            continue
        variants = img.get("variants") or {}
        gif_url = (variants.get("gif") or {}).get("source", {}).get("url")
        source = gif_url or (img.get("source") or {}).get("url")
        if not source:
            continue
        source = strip_quality_suffixes(source)
        if source not in urls:
            urls.append(source)
    if urls:
        return urls

    # 3) Directe mediakop via post_url.
    post_url = data.get("url") or ""
    if _is_media_url(post_url):
        return [strip_quality_suffixes(post_url)]

    return []


def guess_extension_from_content_type(content_type: str) -> str | None:
    """Falls back extensie uit Content-Type bij ontbreken in de URL."""
    ct = (content_type or "").split(";", 1)[0].strip().lower()
    mapping = {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/webp": "webp",
        "image/gif": "gif",
        "video/mp4": "mp4",
        "video/quicktime": "mov",
        "video/webm": "webm",
    }
    return mapping.get(ct)
