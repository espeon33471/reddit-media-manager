"""Media-bestanden downloaden: gestreamd, sha256, pauze-herkenning, retry."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from typing import Callable

import httpx

from typing import Any

from .config import load_settings  # noqa: F401  (settings is een dict)

log = logging.getLogger("rmm.downloader")

CHUNK_SIZE = 64 * 1024
MAX_ATTEMPTS = 5


class DownloadError(Exception):
    pass


class PausedError(Exception):
    """Pauze/stop tijdens download — item moet terug naar pending."""


class MediaDownloader:
    def __init__(self, settings: dict[str, Any]):
        self.settings = settings
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=20.0),
            follow_redirects=True,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def download(
        self,
        url: str,
        dest_path: str,
        should_pause: Callable[[], bool],
        on_bytes: Callable[[int], None] | None = None,
    ) -> tuple[str, int]:
        """Download `url` naar `dest_path` (via `.part` + atomair hernoemen).

        Retourneert (sha256, bytes). Geeft PausedError bij pauze/stop.
        """
        dest_dir = os.path.dirname(dest_path) or "."
        os.makedirs(dest_dir, exist_ok=True)
        tmp_path = dest_path + ".part"
        headers = {
            "User-Agent": self.settings.get("user_agent", "rmm/1.0"),
            "Referer": "https://www.reddit.com/",
            "Accept": "*/*",
        }

        last_error: Exception | None = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            if should_pause():
                _remove_quiet(tmp_path)
                raise PausedError()

            sha = hashlib.sha256()
            total = 0
            resp = None
            try:
                req = self._client.build_request("GET", url, headers=headers)
                resp = await self._client.send(req, stream=True)
                if resp.status_code >= 400:
                    raise DownloadError(f"HTTP {resp.status_code} bij download van {url}")

                with open(tmp_path, "wb") as fh:
                    async for chunk in resp.aiter_bytes(CHUNK_SIZE):
                        if should_pause():
                            raise PausedError()
                        fh.write(chunk)
                        sha.update(chunk)
                        total += len(chunk)
                        if on_bytes:
                            on_bytes(len(chunk))
                # Stream volledig gelezen → definitief hernoemen (atomair).
                os.replace(tmp_path, dest_path)
                return sha.hexdigest(), total

            except PausedError:
                _remove_quiet(tmp_path)
                raise
            except (httpx.HTTPError, OSError, DownloadError) as exc:
                last_error = exc
                _remove_quiet(tmp_path)
                if attempt == MAX_ATTEMPTS:
                    break
                wait = min(2 ** attempt, 20)
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
            async with self._client.stream("GET", url, headers={
                "User-Agent": self.settings.get("user_agent", "rmm/1.0"),
                "Referer": "https://www.reddit.com/",
            }) as resp:
                return resp.headers.get("content-type")
        except httpx.HTTPError:
            return None


def _remove_quiet(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass
