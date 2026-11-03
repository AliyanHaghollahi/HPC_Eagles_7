"""Live demo: start edge/fog/cloud locally and walk through the failure scenarios,
printing what the baseline and SemanticRescue answer in each one.

  python scripts/demo.py
  python scripts/demo.py --router keyword   # route free text with the rule-based router
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from semrescue.cluster import LocalCluster  # noqa: E402
from semrescue.config import load_config  # noqa: E402
from semrescue.coordinator.coordinator import Coordinator  # noqa: E402
from semrescue.coordinator.router import GoldRouter, KeywordRouter  # noqa: E402

QUERIES = [
    ("Where is my gate?", None),
    ("What does this sign say and when does my flight leave?",
     {"objects": ["departure board"], "text": ["AA 1432 Dallas - ON TIME"]}),
]


def show(r):
    route = ", ".join(
        f"{p['op']}->{p['tier'] or 'NONE'}" + (" (backup)" if p["fallback"] else "") + (" (hedged)" if p["hedged"] else "")
        for p in r["parts"])
    print(f"   {r['mode']:16s} {r['status']:8s} {r['latency_s'] * 1000:6.0f} ms  [{route}]")
    print(f"   {'':16s} \"{r['text']}\"")


async def step(cl, cfg, router, title, apply=None, revert=None, settle=1.2):
    print(f"\n=== {title} ===")
    if apply:
        await apply()
    async with Coordinator(cfg, router, fault_tolerant=False) as base, \
            Coordinator(cfg, router, fault_tolerant=True) as rescue:
        await asyncio.sleep(settle if apply else 0)  # heartbeats get a chance to notice
        for q, scene in QUERIES:
            print(f" Q: {q}")
            show(await base.ask(q, scene))
            show(await rescue.ask(q, scene))
    if revert:
        await revert()


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--router", choices=["gold", "keyword"], default="gold")
    args = ap.parse_args()
    cfg = load_config()
    router = GoldRouter() if args.router == "gold" else KeywordRouter()
    print("Starting cloud, edge and fog ...")
    async with LocalCluster(cfg) as cl:
        try:
            await step(cl, cfg, router, "Normal operation")
            await step(cl, cfg, router, "Edge NOT available (crashed)",
                       lambda: cl.crash("edge"), lambda: cl.restart("edge"))
            await step(cl, cfg, router, "Edge available but UNREACHABLE (network partition)",
                       lambda: cl.partition("edge"), lambda: cl.heal("edge"))
            await step(cl, cfg, router, "Fog NOT available (crashed)",
                       lambda: cl.crash("fog"), lambda: cl.restart("fog"))
            await step(cl, cfg, router, "Fog available but UNREACHABLE (network partition)",
                       lambda: cl.partition("fog"), lambda: cl.heal("fog"))
            await step(cl, cfg, router, "Edge and fog both down",
                       lambda: asyncio.gather(cl.crash("edge"), cl.crash("fog")),
                       lambda: asyncio.gather(cl.restart("edge"), cl.restart("fog")))
        finally:
            cl.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
