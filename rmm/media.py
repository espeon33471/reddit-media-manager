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
    ext = url_extension(url)
    return ("." + ext) in MEDIA_EXTS if ext else False


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


_MIME_TO_EXT = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "image/gif": "gif",
    "image/bmp": "bmp",
    "image/tiff": "tiff",
    "image/tif": "tif",
}


def _ext_from_mime(mime: str) -> str | None:
    return _MIME_TO_EXT.get((mime or "").lower().split(";", 1)[0].strip())


def _gallery_candidates(data: dict[str, Any]) -> list[list[str]]:
    """Gallery-posts: `preview.images` is leeg maar `media_metadata` bevat image-IDs.

    Bouwt `https://i.redd.it/{id}.{ext}` per image. De extensie komt uit de
    preview-URL (natuurlijke extensie — i.redd.it normaliseert de extensie NIET);
    fallback op de mime-`m`, dan `jpg`.
    """
    meta = data.get("media_metadata") or {}
    if not isinstance(meta, dict):
        return []
    items: list[list[str]] = []
    for _key, entry in meta.items():
        if not isinstance(entry, dict):
            continue
        kind = str(entry.get("e") or "").lower()
        mime = str(entry.get("m") or "").lower()
        img_id = entry.get("id") or ""
        if not img_id or not (kind == "image" or mime.startswith("image/")):
            continue
        s_url = ((entry.get("s") or {}).get("u")) or ""
        ext = url_extension(s_url) or _ext_from_mime(mime) or "jpg"
        items.append([f"https://i.redd.it/{img_id}.{ext}"])
    return items


_SHORT_ID_RE = re.compile(r"^[a-z0-9]{8,24}$")


def _valid_redd_id(value: str) -> bool:
    """Is `value` een plausibel short i.redd.it/preview ID (base36, lowercase)?

    Langere hashes (mixed case, '-'/'_', >24 chars) zijn géén valide IDs en
    leveren 404 op bij i.redd.it/{id}; die mag je nooit als URL gebruiken.
    """
    return bool(_SHORT_ID_RE.match(value or ""))


def _media_candidates_for(img: dict[str, Any], extra: str | None = None) -> list[str]:
    """Geordende kandidaat-URL's (beste eerst) voor één preview-image.

    1. extra (originele post-URL) → betrouwbaarste full-res voor de primaire image
    2. i.redd.it/{id}.{ext} → full-res, alleen bij een plausibel short ID
    3. gestripte preview-URL → laatste redmiddel (vaak 403 voor NSFW)
    """
    if not isinstance(img, dict):
        return []
    variants = img.get("variants") or {}
    gif_src = (variants.get("gif") or {}).get("source") or {}
    src = gif_src or (img.get("source") or {}) or {}
    url = src.get("url") or ""
    img_id = src.get("id") or img.get("id") or ""
    ext = url_extension(url) if url else ""

    i_redd_url = f"https://i.redd.it/{img_id}.{ext}" if (_valid_redd_id(img_id) and ext) else ""
    stripped_extra = strip_quality_suffixes(extra) if (extra and _is_media_url(extra)) else ""
    stripped_preview = strip_quality_suffixes(url) if url else ""
    # Een low-res preview.redd.it-link nooit vóór een full-res i.redd.it plaatsen.
    extra_is_preview = bool(extra) and ("preview.redd.it" in extra)

    cands: list[str] = []
    if extra_is_preview and i_redd_url:
        cands += [i_redd_url, stripped_extra, stripped_preview]
    else:
        cands += [stripped_extra, i_redd_url, stripped_preview]

    seen: set[str] = set()
    out: list[str] = []
    for c in cands:
        c = (c or "").strip()
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def extract_media_items(post: dict[str, Any]) -> list[list[str]]:
    """Geordende lijst media-items; elk item = geordende kandidaat-URL's (beste eerst).

    De eerste kandidaat is de beste (full-res). De downloader probeert de lijst
    op volgorde tot een 200, zodat zowel Reddit-geüploade media (i.redd.it) als
    externe media (originele URL) full-res gedownload worden.
    """
    data = post.get("data") or {}

    # 1) Reddit video — voorrang.
    rv = (data.get("media") or {}).get("reddit_video") or {}
    fallback = rv.get("fallback_url")
    if isinstance(fallback, str) and fallback:
        return [[strip_quality_suffixes(fallback)]]

    # Originele post-URL (full-res external / i.redd.it) — alleen zinvol bij één image.
    raw_post_url = data.get("url_overridden_by_dest") or data.get("url") or ""
    post_media_url = strip_quality_suffixes(raw_post_url) if _is_media_url(raw_post_url) else None

    imgs = (data.get("preview") or {}).get("images") or []
    imgs = [i for i in imgs if isinstance(i, dict)]
    if imgs:
        single = len(imgs) == 1
        items: list[list[str]] = []
        for i, img in enumerate(imgs):
            extra = post_media_url if (single and i == 0) else None
            cands = _media_candidates_for(img, extra)
            if cands:
                items.append(cands)
        if items:
            return items

    # 2) GALLERY — preview.images leeg maar media_metadata gevuld (image-IDs).
    gallery = _gallery_candidates(data)
    if gallery:
        return gallery

    # 3) Directe mediakop via post_url (single-image post zonder preview).
    if post_media_url:
        return [[post_media_url]]
    return []


def extract_media_urls(post: dict[str, Any]) -> list[str]:
    """Primair-URL per media-item (backward-compat; voor classificatie/meta)."""
    return [item[0] for item in extract_media_items(post) if item]


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
