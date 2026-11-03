"""Failure detection: heartbeats + per-tier circuit breaker + latency tracking.

Two independent signals, because they catch different failures:
  - Heartbeat (GET /health every health_interval_s): catches crashes and partitions
    even when no query is running. After `down_after_misses` misses -> DOWN.
  - Circuit breaker (fed by real requests): catches "gray" failures where /health
    still answers but real requests fail or time out (overloaded / throttled tier).
    After `breaker_failures` failures in a row -> OPEN (skip the tier) for
    `breaker_open_s`, then HALF_OPEN (let one request try), then CLOSED again on success.
"""
from __future__ import annotations

import asyncio
import time

import httpx

UP, DOWN = "UP", "DOWN"
CLOSED, OPEN, HALF_OPEN = "CLOSED", "OPEN", "HALF_OPEN"


class CircuitBreaker:
    def __init__(self, failures_to_open: int = 3, open_s: float = 5.0, clock=time.monotonic):
        self.failures_to_open = failures_to_open
        self.open_s = open_s
        self.clock = clock
        self.state = CLOSED
        self.failures = 0
        self.opened_at = 0.0

    def allow(self) -> bool:
        """True if requests may be sent. OPEN turns into HALF_OPEN after open_s; the next
        result then closes the circuit (success) or opens it again (failure)."""
        if self.state == OPEN and self.clock() - self.opened_at >= self.open_s:
            self.state = HALF_OPEN
        return self.state != OPEN

    def record_success(self) -> None:
        self.state, self.failures = CLOSED, 0

    def record_failure(self) -> None:
        self.failures += 1
        if self.state == HALF_OPEN or self.failures >= self.failures_to_open:
            self.state, self.opened_at = OPEN, self.clock()


class TierHealth:
    def __init__(self, name: str, cfg: dict):
        self.name = name
        self.status = UP  # optimistic until the first heartbeat says otherwise
        self.misses = 0
        self.ewma_latency_s: float | None = None
        self.breaker = CircuitBreaker(cfg["breaker_failures"], cfg["breaker_open_s"])
        self.last_change = time.monotonic()
        self.events: list[tuple[float, str]] = []

    def observe_latency(self, s: float, alpha: float = 0.3) -> None:
        self.ewma_latency_s = s if self.ewma_latency_s is None else alpha * s + (1 - alpha) * self.ewma_latency_s

    def set_status(self, status: str) -> None:
        if status != self.status:
            self.status = status
            self.last_change = time.monotonic()
            self.events.append((time.time(), status))

    @property
    def available(self) -> bool:
        return self.status == UP and self.breaker.allow()


class HealthMonitor:
    def __init__(self, urls: dict[str, str], cfg: dict):
        self.urls = urls
        self.cfg = cfg
        self.tiers = {t: TierHealth(t, cfg) for t in urls}
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=self.cfg["health_timeout_s"])
        await self.check_all()
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with _suppress():
                await self._task
        await self._client.aclose()

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg["health_interval_s"])
            await self.check_all()

    async def check_all(self) -> None:
        await asyncio.gather(*(self._check(t) for t in self.tiers))

    async def _check(self, tier: str) -> None:
        h = self.tiers[tier]
        t0 = time.perf_counter()
        try:
            r = await self._client.get(f"{self.urls[tier]}/health")
            r.raise_for_status()
            h.misses = 0
            h.set_status(UP)
            h.observe_latency(time.perf_counter() - t0)
        except (httpx.HTTPError, OSError):
            h.misses += 1
            if h.misses >= self.cfg["down_after_misses"]:
                h.set_status(DOWN)

    def snapshot(self) -> dict:
        return {t: {"status": h.status, "breaker": h.breaker.state,
                    "ewma_ms": None if h.ewma_latency_s is None else round(h.ewma_latency_s * 1000, 1)}
                for t, h in self.tiers.items()}


class _suppress:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return True
