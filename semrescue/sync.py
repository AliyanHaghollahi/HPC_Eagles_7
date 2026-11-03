"""Periodic delta sync between a local tier (edge or fog) and the cloud.

Every `interval_s` seconds (the sync window) the agent:
  1. PUSHes local changes since its last push (records not received from the cloud)
  2. PULLs cloud changes since its last pull
Conflicts are resolved with last-writer-wins on (updated_at, origin).

What may flow where (enforced again on the cloud side):
  edge : push personal | pull personal, pull map (map is only a cache on the edge)
  fog  : push map      | pull map             (personal data never reaches the fog)

Cursors are stored in the local database, so a tier that restarts with its disk
intact resumes where it left off, and a tier whose disk was wiped starts from 0
and re-downloads everything it is allowed to hold (recovery).
"""
from __future__ import annotations

import asyncio
import json
import time

import httpx

from .store import Store

SYNC_PLAN = {
    "edge": {"push": ["personal"], "pull": ["personal", "map"]},
    "fog": {"push": ["map"], "pull": ["map"]},
}


class SyncAgent:
    def __init__(self, role: str, store: Store, cloud_url: str, interval_s: float,
                 push_on_write: bool = False, timeout_s: float = 2.0):
        self.role = role
        self.store = store
        self.cloud_url = cloud_url.rstrip("/")
        self.interval_s = interval_s
        self.push_on_write = push_on_write
        self.timeout_s = timeout_s
        self.plan = SYNC_PLAN[role]
        self.paused = False
        self._wake = asyncio.Event()
        self._lock = asyncio.Lock()
        self.stats = {"rounds": 0, "ok": 0, "failed": 0, "requests": 0, "bytes": 0,
                      "pushed": 0, "pulled": 0, "last_ok": None}

    def notify_write(self) -> None:
        if self.push_on_write:
            self._wake.set()

    async def run(self) -> None:
        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            self._client = client
            while True:
                await self.sync_once()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self.interval_s)
                except asyncio.TimeoutError:
                    pass
                self._wake.clear()

    async def sync_once(self) -> bool:
        if self.paused:
            return False
        async with self._lock:
            self.stats["rounds"] += 1
            try:
                for ns in self.plan["push"]:
                    cursor = int(self.store.meta_get(f"push_cursor:{ns}", 0))
                    recs, max_seq = self.store.changes_since(ns, cursor, exclude_origin="cloud")
                    body = {"origin": self.role, "ns": ns, "records": recs}
                    r = await self._client.post(f"{self.cloud_url}/sync/push", json=body)
                    r.raise_for_status()
                    self._count(body, r)
                    self.store.meta_set(f"push_cursor:{ns}", max_seq)
                    self.stats["pushed"] += len(recs)
                for ns in self.plan["pull"]:
                    cursor = int(self.store.meta_get(f"pull_cursor:{ns}", 0))
                    r = await self._client.get(
                        f"{self.cloud_url}/sync/pull",
                        params={"ns": ns, "since": cursor, "requester": self.role},
                    )
                    r.raise_for_status()
                    data = r.json()
                    self._count(None, r)
                    for rec in data["records"]:
                        self.store.put(ns, rec["key"], rec["value"], origin=rec["origin"],
                                       updated_at=rec["updated_at"])
                    self.store.meta_set(f"pull_cursor:{ns}", data["cursor"])
                    self.stats["pulled"] += len(data["records"])
                self.stats["ok"] += 1
                self.stats["last_ok"] = time.time()
                self.store.meta_set("last_sync_ok", self.stats["last_ok"])
                return True
            except (httpx.HTTPError, OSError):
                self.stats["failed"] += 1
                return False

    def _count(self, body, resp) -> None:
        self.stats["requests"] += 1
        self.stats["bytes"] += len(resp.content) + (len(json.dumps(body)) if body else 0)
