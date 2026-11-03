"""Local cluster manager: runs edge/fog/cloud as separate processes on one machine,
puts a fault proxy in front of each, and injects the failure scenarios.

  crash(tier)            kill the process (tier NOT available)
  restart(tier, wipe)    start it again; wipe=True deletes its disk so it must recover from the cloud
  partition(tier)        tier keeps running but the glasses cannot reach it (black-hole link)
  heal(tier)             restore the link
  slow(tier, ms)         tier is up and passes health checks, but answers slowly (gray failure)
"""
from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx
import yaml

from .config import ROOT, TIERS, tier_direct_url
from .faults.proxy import FaultProxy


class LocalCluster:
    def __init__(self, cfg: dict, runtime_dir: str | Path | None = None, quiet: bool = True):
        self.cfg = cfg
        self.dir = Path(runtime_dir or tempfile.mkdtemp(prefix="semrescue_"))
        self.dir.mkdir(parents=True, exist_ok=True)
        self.quiet = quiet
        self.procs: dict[str, subprocess.Popen] = {}
        self._db_gen: dict[str, int] = {}
        self.proxies: dict[str, FaultProxy] = {}
        self.cfg_path = self.dir / "cluster_config.yaml"
        with open(self.cfg_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(cfg, f)

    # ---------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        self._spawn("cloud")
        await self.wait_healthy("cloud")
        for t in ("edge", "fog"):
            self._spawn(t)
        for t in ("edge", "fog"):
            await self.wait_healthy(t, need_sync=True)
        if self.cfg["network"].get("use_proxy"):
            for t in TIERS:
                p = FaultProxy(self.cfg["network"]["proxy_ports"][t], self.cfg["tiers"][t]["host"],
                               self.cfg["tiers"][t]["port"], self.cfg["network"]["link_delay_ms"][t])
                await p.start()
                self.proxies[t] = p

    async def stop(self) -> None:
        for p in self.proxies.values():
            await p.stop()
        for t in list(self.procs):
            self._kill(t)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *exc):
        await self.stop()

    def _db(self, tier: str) -> Path:
        gen = self._db_gen.get(tier, 0)
        return self.dir / (f"{tier}.db" if gen == 0 else f"{tier}_{gen}.db")

    def _spawn(self, tier: str, seed: bool = True) -> None:
        cmd = [sys.executable, "-m", "semrescue.tier_service", "--role", tier,
               "--config", str(self.cfg_path), "--db", str(self._db(tier))]
        if not seed:
            cmd.append("--no-seed")
        out = subprocess.DEVNULL if self.quiet else None
        env = dict(os.environ, PYTHONPATH=str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))
        self.procs[tier] = subprocess.Popen(cmd, cwd=str(ROOT), stdout=out, stderr=out, env=env)

    def _kill(self, tier: str) -> None:
        p = self.procs.pop(tier, None)
        if p and p.poll() is None:
            p.kill()
            p.wait(timeout=10)

    async def wait_healthy(self, tier: str, need_sync: bool = False, timeout_s: float = 30.0) -> None:
        url = tier_direct_url(self.cfg, tier)
        end = time.monotonic() + timeout_s
        async with httpx.AsyncClient(timeout=1.0) as c:
            while time.monotonic() < end:
                try:
                    r = await c.get(f"{url}/health")
                    if r.status_code == 200 and (not need_sync or r.json().get("last_sync_ok")):
                        return
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.1)
        raise RuntimeError(f"{tier} did not become healthy")

    # ---------------------------------------------------------------- faults
    async def crash(self, tier: str) -> None:
        self._kill(tier)

    async def restart(self, tier: str, wipe: bool = False) -> None:
        self._kill(tier)
        if wipe:
            # Simulate disk loss: start on a brand-new, empty database file. (On Windows the
            # killed process can keep the old file locked for a moment, so we do not rely on
            # deleting it; old files are removed best-effort.)
            old = self._db(tier)
            self._db_gen[tier] = self._db_gen.get(tier, 0) + 1
            for suffix in ("", "-wal", "-shm"):
                try:
                    Path(str(old) + suffix).unlink(missing_ok=True)
                except OSError:
                    pass
        self._spawn(tier, seed=not wipe)
        await self.wait_healthy(tier, need_sync=tier != "cloud")

    async def partition(self, tier: str) -> None:
        self.proxies[tier].set_mode("partition")

    async def heal(self, tier: str) -> None:
        self.proxies[tier].set_mode("pass")

    async def slow(self, tier: str, ms: int) -> None:
        await self.admin(tier, "/admin/slow", {"ms": ms})

    async def admin(self, tier: str, path: str, params: dict | None = None, method: str = "POST"):
        async with httpx.AsyncClient(timeout=5.0) as c:
            r = await c.request(method, f"{tier_direct_url(self.cfg, tier)}{path}", params=params)
            r.raise_for_status()
            return r.json()

    async def read(self, tier: str, ns: str, key: str):
        async with httpx.AsyncClient(timeout=2.0) as c:
            r = await c.get(f"{tier_direct_url(self.cfg, tier)}/read", params={"ns": ns, "key": key})
            return r.json() if r.status_code == 200 else None

    def cleanup(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)
