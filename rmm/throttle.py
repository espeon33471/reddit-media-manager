"""Globele circuit-breaker voor Reddit-403/429-stormen.

Zonder dit mechanisme pompen alle workers door met 403's (rate-limiting of
IP-block), verbruiken ze hun retry-quota en eindigen honderden items als
"failed" terwijl Reddit gewoon even pauze nodig had.

Werking:
  - `report_block()` wordt aangeroepen bij elke 403/429 (API én downloads).
  - Zodra er binnen `WINDOW` seconden ≥ `THRESHOLD` blocks zijn, gaat de
    breaker OPEN voor `COOLDOWN` seconden.
  - `wait_if_open()` wordt vóór nieuwe Reddit-aanroepen aangeroepen; terwijl
    de breaker open is wachten alle callers tot hij weer gesloten is.
  - `state()` levert de status voor de UI (dashboard: "Reddit blokkeert …").

De breaker is een module-level singleton zodat downloader en API-client
elkaars blocks delen.
"""
from __future__ import annotations

import asyncio
import logging
import time

log = logging.getLogger("rmm.throttle")

WINDOW = 120.0        # seconden: window waarin blocks worden geteld
THRESHOLD = 6         # aantal blocks in de window → breaker open
COOLDOWN = 180.0      # seconden pauze zodra de breaker open is
MAX_COOLDOWN = 600.0  # elke opeenvolgende trip verlengt de pauze (max 10 min)


class BlockBreaker:
    def __init__(self) -> None:
        self._blocks: list[float] = []
        self._open_until = 0.0
        self._trips = 0
        self._lock = asyncio.Lock()
        self._waiters: list[asyncio.Event] = []

    def report_block(self, status: int | None = None) -> None:
        """Registreer een 403/429. Belt de breaker open bij te veel blocks."""
        now = time.monotonic()
        self._blocks.append(now)
        cutoff = now - WINDOW
        while self._blocks and self._blocks[0] < cutoff:
            self._blocks.pop(0)

        if len(self._blocks) >= THRESHOLD and now >= self._open_until:
            self._open_until = now + COOLDOWN
            self._trips = min(self._trips + 1, 5)
            log.warning(
                "Reddit-blokker detecteerd (%d blocks/%ds) — pauze %.0fs (trip %d)",
                len(self._blocks), int(WINDOW), COOLDOWN, self._trips,
            )
            # Wekker zodat de pauze ook afloopt als niemand wacht.
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.call_later(COOLDOWN, self._close_waiters)
            except RuntimeError:
                pass

    async def wait_if_open(self) -> float:
        """Wacht tot de breaker gesloten is. Retourneert de gewachte seconden."""
        waited = 0.0
        while True:
            remaining = self._open_until - time.monotonic()
            if remaining <= 0:
                return waited
            event = asyncio.Event()
            self._waiters.append(event)
            try:
                await asyncio.wait_for(event.wait(), timeout=remaining + 0.1)
                return waited + remaining
            except asyncio.TimeoutError:
                waited += remaining
                # Opnieuw checken: mogelijk weer getript door een andere caller.
                continue
            finally:
                try:
                    self._waiters.remove(event)
                except ValueError:
                    pass

    def _close_waiters(self) -> None:
        for event in list(self._waiters):
            if not event.is_set():
                event.set()

    def state(self) -> dict:
        """Status voor de UI/logs."""
        remaining = max(0.0, self._open_until - time.monotonic())
        return {
            "open": remaining > 0,
            "cooldown_remaining": round(remaining, 1),
            "trips": self._trips,
        }


# Singleton: gedeeld over downloader en API-client.
block_breaker = BlockBreaker()
