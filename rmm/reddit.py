"""Reddit-API client: cookie-auth, rate limiting, retry/backoff, paginatie.

Gebruikt urllib.request (via asyncio.to_thread) in plaats van httpx,
omdat Reddit's bot-detectie httpx/httpcore blokkeert op TLS-fingerprint.
Auth: loid + reddit_session cookie (username/password-login is verwijderd).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

log = logging.getLogger("rmm.reddit")

MAX_ATTEMPTS = 5
TIMEOUT = 30.0


class RedditError(Exception):
    """Duidelijke, gebruikersbestendige foutmelding."""


class RateLimiter:
    """Geeft max. 1 aanroep per `min_interval` seconden (geheel programma)."""

    def __init__(self, interval_fn):
        self._interval_fn = interval_fn
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def acquire(self) -> None:
        interval = max(0.0, float(self._interval_fn()))
        async with self._lock:
            now = time.monotonic()
            wait = self._last + interval - now
            if wait > 0:
                log.debug("rate-limit: wacht %.1fs", wait)
                await asyncio.sleep(wait)
            self._last = time.monotonic()


def _http(
    url: str,
    headers: dict[str, str] | None = None,
    method: str = "GET",
    body: bytes | None = None,
) -> tuple[int, dict[str, str], bytes]:
    """Synchron HTTP via urllib.request.
    Geeft (status, headers, body) terug — ook bij 4xx/5xx.
    """
    hdrs = dict(headers or {})
    req = urllib.request.Request(url, data=body, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()
    except urllib.error.URLError as e:
        raise RedditError(f"Netwerkfout: {e.reason}") from e


class RedditClient:
    BASE = "https://old.reddit.com"

    def __init__(self, settings: dict[str, Any]):
        self.settings = settings
        self._limiter = RateLimiter(
            lambda: float(settings.get("request_interval", 6.0))
        )

    async def close(self) -> None:
        """Niet nodig met urllib — geen persistente connecties."""
        pass

    # ── intern ────────────────────────────────────────────────────────

    async def _auth_headers(self) -> dict[str, str]:
        """Bouw auth-headers: Cookie (loid + reddit_session)."""
        loid = str(self.settings.get("reddit_loid") or "")
        session = str(self.settings.get("reddit_session") or "")
        headers: dict[str, str] = {}
        parts: list[str] = []
        if loid:
            parts.append(f"loid={loid}")
        if session:
            parts.append(f"reddit_session={session}")
        if parts:
            headers["Cookie"] = "; ".join(parts)
        return headers

    async def _get_json(
        self, path: str, method: str = "GET", data: str | None = None
    ) -> Any:
        """GET/POST naar Reddit met rate-limit + retry; retour JSON."""
        url = path if path.startswith("http") else f"{self.BASE}{path}"
        body = data.encode("utf-8") if data else None

        last_exc: Exception | None = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            await self._limiter.acquire()
            auth = await self._auth_headers()
            hdrs = {
                "User-Agent": str(self.settings.get("user_agent") or ""),
                "Accept": "application/json, text/html, */*",
                "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
                **auth,
            }
            try:
                status, resp_headers, raw = await asyncio.to_thread(
                    _http, url, hdrs, method, body
                )
            except RedditError:
                raise
            except Exception as exc:
                last_exc = exc
                delay = min(2.0 * attempt, 30.0)
                log.warning("Netwerkfout (poging %d/%d): %s — wacht %.1fs",
                            attempt, MAX_ATTEMPTS, exc, delay)
                await asyncio.sleep(delay)
                continue

            if status in (429, 503):
                ra = resp_headers.get("retry-after") or resp_headers.get("Retry-After") or "0"
                try:
                    retry_after = float(ra)
                except ValueError:
                    retry_after = 0.0
                delay = max(retry_after, 2.0 * attempt)
                log.warning("HTTP %s — probeer opnieuw over %.1fs (poging %d/%d)",
                            status, delay, attempt, MAX_ATTEMPTS)
                await asyncio.sleep(delay)
                continue

            if status >= 400:
                try:
                    err = json.loads(raw)
                except (json.JSONDecodeError, ValueError):
                    err = raw.decode("utf-8", errors="replace")[:300]
                raise RedditError(f"Reddit HTTP {status}: {err}")

            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                # Niet-JSON antwoord (HTML) — waarschijnlijk expired session of redirect naar login-pagina
                snippet = raw.decode("utf-8", errors="replace")[:150]
                if "<html" in snippet.lower() or "<!doctype" in snippet.lower() or "login" in snippet.lower():
                    raise RedditError(
                        "Sessie is verlopen of ongeldig (Reddit verwees naar login-pagina). "
                        "Plak een verse sessie-cookie via Settings."
                    )
                return raw.decode("utf-8", errors="replace")

        raise RedditError(f"Reddit niet bereikbaar na {MAX_ATTEMPTS} pogingen: {last_exc}")

    # ── publieke API ─────────────────────────────────────────────────

    async def whoami(self) -> dict[str, Any]:
        """Haal de gebruikersidentiteit op via /api/me.json."""
        data = await self._get_json("/api/me.json")
        # Reddit retourneert soms een lijst [{...}] in plaats van {...}
        if isinstance(data, list):
            if not data:
                raise RedditError("Leeg antwoord van /api/me.json — sessie mogelijk ongeldig")
            data = data[0]
        if not isinstance(data, dict):
            raise RedditError(
                f"Onverwacht antwoord van /api/me.json (type: {type(data).__name__}). "
                f"Sessie is waarschijnlijk verlopen — plak een verse cookie."
            )
        # /api/me.json retourneert {"loid": "...", "kind": "t2", "data": {...}}
        user = data.get("data", data)
        if not isinstance(user, dict):
            user = data
        return user

    async def verify_session(self) -> dict[str, Any]:
        """Check of de sessie geldig is en NSFW-toegang werkt."""
        info = await self.whoami()
        return {
            "ok": True,
            "username": info.get("name", "?"),
            "id": info.get("id", "?"),
            "nsfw": bool(info.get("over_18", False)),
            "karma": info.get("link_karma", 0),
        }

    async def listing(
        self, subreddit: str, sort: str = "new", limit: int = 25, after: str | None = None
    ) -> dict[str, Any]:
        """Haal een subreddit-listing op (new/top/hot) met NSFW-toegang."""
        params: dict[str, str | int] = {"limit": limit, "raw_json": 1, "over_18": 1}
        if after:
            params["after"] = after
        if sort == "top":
            params["t"] = "all"
        qs = urllib.parse.urlencode(params)
        result = await self._get_json(f"/r/{subreddit}/{sort}.json?{qs}")
        if isinstance(result, dict) and "data" in result:
            return result["data"]
        return result

    async def subreddit_posts(
        self, subreddit: str, limit: int = 25, after: str | None = None
    ) -> dict[str, Any]:
        return await self.listing(subreddit, sort="new", limit=limit, after=after)

    async def user_posts(
        self, username: str, limit: int = 25, after: str | None = None
    ) -> dict[str, Any]:
        params: dict[str, str | int] = {"limit": limit, "raw_json": 1, "over_18": 1}
        if after:
            params["after"] = after
        qs = urllib.parse.urlencode(params)
        result = await self._get_json(f"/user/{username}/submitted.json?{qs}")
        if isinstance(result, dict) and "data" in result:
            return result["data"]
        return result

    async def resolve_media(self, post: dict[str, Any]) -> list[str]:
        """Haal de media-URLs uit een post op."""
        urls: list[str] = []
        data = post.get("data", post)
        media = data.get("media", {}) or {}

        if media:
            for kind in ("image", "reddit_video", "gif"):
                entry = media.get(kind)
                if isinstance(entry, dict):
                    src = entry.get("fallback_url") or entry.get("poster")
                    if src:
                        urls.append(src)
            # gallery items
            gallery = data.get("gallery_data")
            if isinstance(gallery, dict):
                for item in gallery.get("items", []):
                    mid = item.get("media_id")
                    if mid:
                        m = media.get(f"id_{mid}")
                        if isinstance(m, dict):
                            src = m.get("fallback_url") or m.get("poster")
                            if src:
                                urls.append(src)

        url = data.get("url")
        if url and url not in urls and not url.startswith(
            ("https://www.reddit.com", "https://reddit.com")
        ):
            urls.append(url)
        return urls
