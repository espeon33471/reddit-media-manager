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
import random
from typing import Any, Callable

import httpx

from .config import load_settings  # noqa: F401
from .throttle import block_breaker

log = logging.getLogger("rmm.downloader")

CHUNK_SIZE = 64 * 1024
MAX_ATTEMPTS = 4

# Status-codes die niet op dezelfde URL oplossen met een retry.
# NB: 403 staat hier DELIBERAAT NIET in — bij Reddit/CDN is 403 vaak
# TRANSIËNTE rate-limiting, dus die herhalen we (met backoff) binnen
# dezelfde kandidaat vóór dat we naar de volgende kandidaat vallen.
HARD_STATUS = {401, 404, 410}


class DownloadError(Exception):
    def __init__(self, msg: str, transient: bool = False):
        self.transient = transient
        super().__init__(msg)


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
        # Globale start-spacing over alle workers heen (dempt de initieele burst).
        self._spacing = max(2.0, float(settings.get("download_spacing", 2.0)))
        self._space_lock = asyncio.Lock()
        self._last_start = 0.0
        # Optionele bandbreedte-limiet (MB/s → bytes/sec; 0 = onbeperkt).
        try:
            bw_mb = float(settings.get("max_bandwidth_mb", 0) or 0)
        except (TypeError, ValueError):
            bw_mb = 0.0
        self._bw_bytes_per_sec = max(0.0, bw_mb) * 1024 * 1024

    _BROWSER_UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )

    _REDDIT_HOSTS = ("redd.it", "reddit.com", "redditvideo.com", "redditmedia.com", "redditstatic.com")

    def _ua(self) -> str:
        """Gebruik de ingestelde UA alleen als die écht browser-achtig is; anders
        de ingebouwde browser-UA — zodat we nooit `rmm/1.0` sturen (die krijgt
        direct een 403 van de Reddit-CDN)."""
        ua = (self.settings.get("user_agent") or "").strip()
        return ua if "mozilla" in ua.lower() else self._BROWSER_UA

    @staticmethod
    def _host(url: str) -> str:
        return (url or "").split("://", 1)[-1].split("/", 1)[0].lower()

    def _is_reddit_host(self, url: str) -> bool:
        host = self._host(url)
        return any(host == h or host.endswith("." + h) for h in self._REDDIT_HOSTS)

    def _headers_for(self, url: str) -> dict[str, str]:
        """Headers per doel-host. Externe video-hosts (RedGifs/Gifer) krijgen een
        browser-UA + matching referer; Reddit-CDN's een browser-UA + reddit-referer
        (altijd browser-UA, onafhankelijk van de ingestelde setting)."""
        host = self._host(url)
        ua = self._ua()
        if "redgifs.com" in host or "gifer.com" in host:
            ref = "https://www.redgifs.com/" if "redgifs" in host else "https://www.gifer.com/"
            return {"User-Agent": ua, "Referer": ref, "Accept": "*/*"}
        if self._is_reddit_host(url):
            headers = {
                "User-Agent": ua,
                "Referer": "https://www.reddit.com/",
                "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
            # NSFW/age-gated media op redd.it/redditvideo.com eisen dat we
            # geïdentificeerd zijn — zonder cookies krijgt de CDN 403 terug.
            parts: list[str] = []
            loid = str(self.settings.get("reddit_loid") or "").strip()
            session = str(self.settings.get("reddit_session") or "").strip()
            if loid:
                parts.append(f"loid={loid}")
            if session:
                parts.append(f"reddit_session={session}")
            if parts:
                headers["Cookie"] = "; ".join(parts)
            return headers
        # Overige externe hosts: browser-UA, zonder misleidende referer.
        return {"User-Agent": ua, "Accept": "*/*"}

    async def close(self) -> None:
        await self._client.aclose()

    async def _throttle(self, nbytes: int) -> None:
        """Pas de download-snelheid aan op de ingestelde bandbreedte-limiet (MB/s).
        0 = onbeperkt (geen await)."""
        if self._bw_bytes_per_sec <= 0 or nbytes <= 0:
            return
        await asyncio.sleep(nbytes / self._bw_bytes_per_sec)

    async def _acquire_start_slot(self) -> None:
        """Zorgt dat over alle workers heen minstens `download_spacing` sec tussen
        het *starten* van twee downloads zit — dempt de initieele burst die de CDN
        anders snel in een 429-rate-limit duwt."""
        if self._spacing <= 0:
            return
        loop = asyncio.get_event_loop()
        async with self._space_lock:
            now = loop.time()
            wait = self._last_start + self._spacing - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_start = loop.time()

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
        last_transient = False
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
                last_transient = False
                log.info("Kandidaat %s → %s; probeer volgende", cand, exc)
                continue
            except DownloadError as exc:
                last_err = exc
                last_transient = bool(getattr(exc, "transient", False))
                log.warning("Kandidaat %s mislukt (%s); probeer volgende", cand, exc)
                continue

        raise DownloadError(
            f"Alle kandidaat-URL's mislukt (laatste: {last_err})", transient=last_transient
        )

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

            # Circuit-breaker: bij een 403-storm wacht iedereen tot de pauze
            # is afgerold i.p.v. de block te verergeren.
            breaker_wait = await block_breaker.wait_if_open()
            if breaker_wait > 0:
                log.info("Breaker-pauze %.0fs afgerold; hervat %s", breaker_wait, url)

            await self._acquire_start_slot()

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
                    # 403/429 = vaak rate-limiting → langer wachten, respecteer
                    # Retry-After als de server die stuurt, en voeg jitter toe
                    # zodat workers niet tegelijk terugkomen (thundering herd).
                    last_error = DownloadError(f"HTTP {resp.status_code} bij download van {url}")
                    if resp.status_code in (403, 429):
                        block_breaker.report_block(resp.status_code)
                    _remove_quiet(tmp_path)
                    if attempt == MAX_ATTEMPTS:
                        break
                    base = 3 if resp.status_code in (403, 429) else 1
                    wait = min(2 ** attempt * base, 60)
                    ra = resp.headers.get("Retry-After")
                    if ra:
                        try:
                            wait = max(wait, min(float(ra), 120))
                        except (TypeError, ValueError):
                            pass
                    wait = max(0.5, wait * (0.8 + random.random() * 0.4))  # jitter ±20%
                    log.warning("HTTP %d (poging %d/%d) %s — wacht %.1fs",
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
                        await self._throttle(len(chunk))

                # Lege response (0 bytes) → probeer opnieuw, anders mislukt.
                if total == 0:
                    _remove_quiet(tmp_path)
                    last_error = DownloadError("lege response (0 bytes)")
                    if attempt == MAX_ATTEMPTS:
                        break
                    await asyncio.sleep(min(2 ** attempt, 10) * (0.8 + random.random() * 0.4))
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
                wait = min(2 ** attempt, 15) * (0.8 + random.random() * 0.4)
                log.warning("Download fout (poging %d/%d) %s: %s — wacht %.1fs",
                            attempt, MAX_ATTEMPTS, url, exc, wait)
                await asyncio.sleep(wait)
            finally:
                if resp is not None:
                    try:
                        await resp.aclose()
                    except httpx.HTTPError:
                        pass

        # Alles hier is transiënt (403/429/5xx/netwerk/leeg) → geef de engine
        # door dat we later opnieuw mogen proberen i.p.v. permanent mislukt.
        raise DownloadError(
            f"Download mislukt na {MAX_ATTEMPTS} pogingen: {last_error}", transient=True
        )

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
