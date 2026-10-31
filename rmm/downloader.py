"""Media-bestanden downloaden: gestreamd, sha256, pauze-herkenning, retry, kandidaat-fallback.

De downloader accepteert één URL óf een geordende lijst kandidaat-URL's (beste eerst).
Hij probeert ze op volgorde tot een 200. Harde HTTP-fouten (401/403/404/410) slaan
direct de volgende kandidaat over; transiënte fouten (netwerk, 5xx, 429) worden
binnen één kandidaat herhaald met backoff.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from typing import Any, Callable

import httpx

from .config import load_settings  # noqa: F401

log = logging.getLogger("rmm.downloader")

CHUNK_SIZE = 64 * 1024
MAX_ATTEMPTS = 4

# Status-codes die niet op dezelfde URL oplossen met een retry.
# NB: 403 staat hier DELIBERAAT NIET in — bij Reddit/CDN is 403 vaak
# TRANSIËNTE rate-limiting, dus die herhalen we (met backoff) binnen
# dezelfde kandidaat vóór dat we naar de volgende kandidaat vallen.
HARD_STATUS = {401, 404, 410}


class DownloadError(Exception):
    pass


class HardHTTPError(DownloadError):
    """Harde HTTP-fout (401/404/410) → probeer de volgende kandidaat-URL."""

    def __init__(self, status: int, url: str):
        self.status = status
        self.url = url
        super().__init__(f"HTTP {status} bij download van {url}")


class PausedError(Exception):
    """Pauze/stop tijdens download — item moet terug naar pending."""


class MediaDownloader:
    def __init__(self, settings: dict[str, Any]):
        self.settings = settings
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(180.0, connect=20.0),
            follow_redirects=True,
        )

    _BROWSER_UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )

    def _headers_for(self, url: str) -> dict[str, str]:
        """Headers per doel-host. Externe video-hosts (RedGifs/Gifer) krijgen een
        browser-UA + matching referer (getest werkend); Reddit-CDN's de ingestelde
        UA + reddit-referer."""
        host = (url or "").split("://", 1)[-1].split("/", 1)[0].lower()
        if "redgifs.com" in host or "gifer.com" in host:
            ref = "https://www.redgifs.com/" if "redgifs" in host else "https://www.gifer.com/"
            return {"User-Agent": self._BROWSER_UA, "Referer": ref, "Accept": "*/*"}
        return {
            "User-Agent": self.settings.get("user_agent", "rmm/1.0"),
            "Referer": "https://www.reddit.com/",
            "Accept": "*/*",
        }

    async def close(self) -> None:
        await self._client.aclose()

    async def download(
        self,
        url: "str | list[str]",
        dest_path: str,
        should_pause: Callable[[], bool],
        on_bytes: Callable[[int], None] | None = None,
    ) -> tuple[str, int]:
        """Download één URL óf een geordende lijst kandidaat-URL's naar `dest_path`.

        Kandidaten worden op volgorde geprobeerd tot een 200. Retourneert
        (sha256, bytes). Geeft PausedError bij pauze/stop.
        """
        if isinstance(url, str):
            url = [url]
        candidates = [u.strip() for u in url if u and u.strip()]
        if not candidates:
            raise DownloadError("Geen geldige download-URL's")

        os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)

        last_err: Exception | None = None
        for cand in candidates:
            if should_pause():
                _remove_quiet(dest_path + ".part")
                raise PausedError()
            try:
                return await self._download_one(cand, dest_path, should_pause, on_bytes)
            except PausedError:
                raise
            except HardHTTPError as exc:
                last_err = exc
                log.info("Kandidaat %s → %s; probeer volgende", cand, exc)
                continue
            except DownloadError as exc:
                last_err = exc
                log.warning("Kandidaat %s mislukt (%s); probeer volgende", cand, exc)
                continue

        raise DownloadError(f"Alle kandidaat-URL's mislukt (laatste: {last_err})")

    async def _download_one(
        self,
        url: str,
        dest_path: str,
        should_pause: Callable[[], bool],
        on_bytes: Callable[[int], None] | None,
    ) -> tuple[str, int]:
        """Download één URL (met retry op transiënte fouten) via `.part` + hernoem."""
        tmp_path = dest_path + ".part"
        last_error: Exception | None = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            if should_pause():
                _remove_quiet(tmp_path)
                raise PausedError()

            sha = hashlib.sha256()
            total = 0
            resp = None
            try:
                req = self._client.build_request("GET", url, headers=self._headers_for(url))
                resp = await self._client.send(req, stream=True)
                if resp.status_code in HARD_STATUS:
                    raise HardHTTPError(resp.status_code, url)
                if resp.status_code >= 400:
                    # 403/429/5xx → transiënt, retry binnen deze kandidaat.
                    # 403/429 = vaak rate-limiting → langer wachten.
                    last_error = DownloadError(f"HTTP {resp.status_code} bij download van {url}")
                    _remove_quiet(tmp_path)
                    if attempt == MAX_ATTEMPTS:
                        break
                    base = 3 if resp.status_code in (403, 429) else 1
                    wait = min(2 ** attempt * base, 30)
                    log.warning("HTTP %d (poging %d/%d) %s — wacht %ds",
                                resp.status_code, attempt, MAX_ATTEMPTS, url, wait)
                    await asyncio.sleep(wait)
                    continue

                with open(tmp_path, "wb") as fh:
                    async for chunk in resp.aiter_bytes(CHUNK_SIZE):
                        if should_pause():
                            raise PausedError()
                        fh.write(chunk)
                        sha.update(chunk)
                        total += len(chunk)
                        if on_bytes:
                            on_bytes(total)

                # Lege response (0 bytes) → probeer opnieuw, anders mislukt.
                if total == 0:
                    _remove_quiet(tmp_path)
                    last_error = DownloadError("lege response (0 bytes)")
                    if attempt == MAX_ATTEMPTS:
                        break
                    await asyncio.sleep(min(2 ** attempt, 10))
                    continue

                os.replace(tmp_path, dest_path)
                return sha.hexdigest(), total

            except PausedError:
                _remove_quiet(tmp_path)
                raise
            except HardHTTPError:
                _remove_quiet(tmp_path)
                raise
            except (httpx.HTTPError, OSError, DownloadError) as exc:
                last_error = exc
                _remove_quiet(tmp_path)
                if attempt == MAX_ATTEMPTS:
                    break
                wait = min(2 ** attempt, 15)
                log.warning("Download fout (poging %d/%d) %s: %s — wacht %ds",
                            attempt, MAX_ATTEMPTS, url, exc, wait)
                await asyncio.sleep(wait)
            finally:
                if resp is not None:
                    try:
                        await resp.aclose()
                    except httpx.HTTPError:
                        pass

        raise DownloadError(f"Download mislukt na {MAX_ATTEMPTS} pogingen: {last_error}")

    async def content_type_for(self, url: str) -> str | None:
        """HEAD/GET met directe afsluit voor extensie-giswerk; mag falen."""
        try:
            async with self._client.stream("GET", url, headers=self._headers_for(url)) as resp:
                return resp.headers.get("content-type")
        except httpx.HTTPError:
            return None


def _remove_quiet(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass
