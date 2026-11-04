"""SemanticRescue coordinator (runs on the smart-glasses side).

ask(query) = route -> run each part on its tier with failover/hedging -> fault-aware fusion.

Per part, in fault-tolerant mode:
  1. Build the candidate list from the policy chain, healthy tiers first.
  2. Send to the first candidate with a per-request timeout.
  3. Failover: if it fails (refused, error, timeout) try the next candidate.
  4. Hedging: if it is merely slow (no answer after the hedge delay), start the next
     candidate in parallel and keep whichever answers first.
  5. Give up on the part after part_deadline_s; fusion then reports it as missing.
Independent parts run in parallel; a part with depends_on waits for its inputs.

Baseline mode (fault_tolerant=False) is the original pipeline: primary tier only,
same timeout, no health checks, no hedging, no backups.
"""
from __future__ import annotations

import asyncio
import time

import httpx

from ..config import TIERS, tier_client_url
from .fusion import fuse
from .health import HealthMonitor
from .policy import chain_for, primary_tier, write_chain_for


class Coordinator:
    def __init__(self, cfg: dict, router, fault_tolerant: bool = True, urls: dict | None = None):
        self.cfg = cfg
        self.ccfg = cfg["coordinator"]
        self.router = router
        self.fault_tolerant = fault_tolerant
        self.urls = urls or {t: tier_client_url(cfg, t) for t in TIERS}
        self.health = HealthMonitor(self.urls, self.ccfg)
        self.answer_ewma: dict[str, float] = {}

    async def start(self) -> None:
        limits = httpx.Limits(max_connections=50, max_keepalive_connections=0)
        self.client = httpx.AsyncClient(timeout=self.ccfg["request_timeout_s"], limits=limits)
        if self.fault_tolerant:
            await self.health.start()
        else:
            self.health._client = None

    async def stop(self) -> None:
        await self.client.aclose()
        if self.fault_tolerant:
            await self.health.stop()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *exc):
        await self.stop()

    # ------------------------------------------------------------------ public
    async def ask(self, query: str, scene: dict | None = None, parts: list[dict] | None = None) -> dict:
        t0 = time.perf_counter()
        parts = parts if parts is not None else await self.router.route(query)
        results = await self._run_graph(parts, scene)
        ordered = [results[p["id"]] for p in parts]
        fused = fuse(ordered, self.ccfg["stale_warn_s"])
        return {"query": query, "mode": "semantic_rescue" if self.fault_tolerant else "baseline",
                "latency_s": time.perf_counter() - t0, "parts": ordered, **fused}

    async def write(self, ns: str, key: str, value: str) -> dict:
        for tier in self._order(write_chain_for(ns, self.fault_tolerant)):
            try:
                r = await self.client.post(f"{self.urls[tier]}/write", json={"ns": ns, "key": key, "value": value})
                r.raise_for_status()
                self._record(tier, True)
                return {"ok": True, "tier": tier}
            except (httpx.HTTPError, OSError):
                self._record(tier, False)
        return {"ok": False, "tier": None}

    def status(self) -> dict:
        return self.health.snapshot()

    # ------------------------------------------------------------------ graph
    async def _run_graph(self, parts: list[dict], scene: dict | None) -> dict[str, dict]:
        done: dict[str, dict] = {}
        tasks: dict[str, asyncio.Task] = {}

        async def run(p: dict) -> dict:
            deps = p.get("depends_on") or []
            if deps:
                dep_results = await asyncio.gather(*(tasks[d] for d in deps))
                if any(r["status"] != "ok" for r in dep_results):
                    return self._result(p, "skipped")
                p = dict(p)
                for d, r in zip(deps, dep_results):
                    if p["arg"] == f"${d}":
                        p["arg"] = r["value"]
            return await self._run_part(p, scene)

        for p in parts:  # parts are listed in dependency order
            tasks[p["id"]] = asyncio.create_task(run(p))
        for pid, t in tasks.items():
            done[pid] = await t
        return done

    # ------------------------------------------------------------------ one part
    async def _run_part(self, part: dict, scene: dict | None) -> dict:
        t0 = time.perf_counter()
        deadline = t0 + self.ccfg["part_deadline_s"]
        candidates = self._order(chain_for(part["op"], self.fault_tolerant))
        payload = {"op": part["op"], "arg": part["arg"], "text": part.get("text", ""), "scene": scene}
        attempts: list[dict] = []
        pending: dict[asyncio.Task, str] = {}
        idx, hedged = 0, False

        def launch():
            nonlocal idx
            tier = candidates[idx]
            idx += 1
            pending[asyncio.create_task(self._call(tier, payload))] = tier

        launch()
        try:
            while pending:
                now = time.perf_counter()
                if now >= deadline:
                    break
                can_hedge = self.fault_tolerant and self.ccfg["hedging"] and idx < len(candidates)
                wait_s = min(self._hedge_delay(candidates[idx - 1]), deadline - now) if can_hedge else deadline - now
                finished, _ = await asyncio.wait(pending, timeout=wait_s, return_when=asyncio.FIRST_COMPLETED)
                if not finished:  # slow tier: hedge to the next candidate
                    if can_hedge:
                        hedged = True
                        launch()
                    continue
                for task in finished:
                    tier = pending.pop(task)
                    out = task.result()
                    attempts.append({"tier": tier, "outcome": out["outcome"], "latency_s": round(out["latency_s"], 4)})
                    if out["outcome"] in ("ok", "not_found"):
                        losers = list(pending.items())
                        for other, otier in losers:  # the losers of a hedge were slower, not failed
                            other.cancel()
                            attempts.append({"tier": otier, "outcome": "cancelled_slow", "latency_s": None})
                        if losers:
                            await asyncio.gather(*(task for task, _ in losers), return_exceptions=True)
                        pending.clear()
                        res = self._result(part, out["outcome"], tier=tier, data=out.get("data"))
                        res.update(latency_s=time.perf_counter() - t0, attempts=attempts, hedged=hedged,
                                   fallback=tier != primary_tier(part["op"]))
                        return res
                if not pending and idx < len(candidates):  # failover
                    launch()
        finally:
            unfinished = list(pending)
            for t in unfinished:
                t.cancel()
            if unfinished:
                await asyncio.gather(*unfinished, return_exceptions=True)
        res = self._result(part, "failed")
        res.update(latency_s=time.perf_counter() - t0, attempts=attempts, hedged=hedged, fallback=False)
        return res

    async def _call(self, tier: str, payload: dict) -> dict:
        t0 = time.perf_counter()
        try:
            r = await self.client.post(f"{self.urls[tier]}/answer", json=payload)
            lat = time.perf_counter() - t0
            if r.status_code == 404:
                self._record(tier, True, lat)
                return {"outcome": "not_found", "latency_s": lat}
            if r.status_code in (403, 422):  # policy/capability refusal: not a tier failure
                return {"outcome": "refused", "latency_s": lat}
            r.raise_for_status()
            self._record(tier, True, lat)
            return {"outcome": "ok", "latency_s": lat, "data": r.json()}
        except httpx.TimeoutException:
            self._record(tier, False)
            return {"outcome": "timeout", "latency_s": time.perf_counter() - t0}
        except (httpx.HTTPError, OSError):
            self._record(tier, False)
            return {"outcome": "error", "latency_s": time.perf_counter() - t0}

    # ------------------------------------------------------------------ helpers
    def _order(self, chain: list[str]) -> list[str]:
        """Healthy tiers first (in policy order), suspected-dead ones last as a last resort."""
        if not self.fault_tolerant:
            return chain
        healthy = [t for t in chain if self.health.tiers[t].available]
        return healthy + [t for t in chain if t not in healthy]

    def _hedge_delay(self, tier: str) -> float:
        c = self.ccfg
        ewma = self.answer_ewma.get(tier)
        if ewma is None:
            return c["hedge_max_s"]
        return max(c["hedge_min_s"], min(c["hedge_max_s"], c["hedge_factor"] * ewma))

    def _record(self, tier: str, ok: bool, latency_s: float | None = None) -> None:
        if not self.fault_tolerant:
            return
        b = self.health.tiers[tier].breaker
        if ok:
            b.record_success()
            if latency_s is not None:
                prev = self.answer_ewma.get(tier)
                self.answer_ewma[tier] = latency_s if prev is None else 0.3 * latency_s + 0.7 * prev
        else:
            b.record_failure()

    @staticmethod
    def _result(part: dict, status: str, tier: str | None = None, data: dict | None = None) -> dict:
        data = data or {}
        return {"id": part["id"], "op": part["op"], "arg": part["arg"], "text": part.get("text", ""),
                "status": status, "tier": tier, "answer": data.get("answer"), "value": data.get("value"),
                "staleness_s": data.get("staleness_s"), "latency_s": 0.0, "attempts": [],
                "hedged": False, "fallback": False}
