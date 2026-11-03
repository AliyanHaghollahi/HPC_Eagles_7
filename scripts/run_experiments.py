"""Run the SemanticRescue experiments on a local cluster.

E1  Failure scenarios x {baseline, semantic_rescue}: recovery rate, completeness,
    correctness, latency.
E2  Sync window sweep: how many personal updates are lost (stale answers from the cloud)
    when the edge crashes, for different sync intervals, and what the sync costs.

  python scripts/run_experiments.py                 # both experiments
  python scripts/run_experiments.py --only e1 --reps 1
Results go to results/ (raw JSONL + summary CSV); plot with scripts/plot_results.py.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from semrescue.cluster import LocalCluster  # noqa: E402
from semrescue.config import ROOT, load_config, load_json  # noqa: E402
from semrescue.coordinator.coordinator import Coordinator  # noqa: E402
from semrescue.coordinator.policy import primary_tier  # noqa: E402
from semrescue.coordinator.router import GoldRouter  # noqa: E402

RESULTS = ROOT / "results"


# --------------------------------------------------------------------------- scenarios
class Scenario:
    def __init__(self, name, desc, faulty, apply, revert, background=None):
        self.name, self.desc, self.faulty = name, desc, faulty
        self.apply, self.revert, self.background = apply, revert, background


async def _noop(cl):
    pass


async def _flap(cl, stop: asyncio.Event, period_s=1.5):
    down = False
    while not stop.is_set():
        down = not down
        await (cl.partition("edge") if down else cl.heal("edge"))
        try:
            await asyncio.wait_for(stop.wait(), timeout=period_s)
        except asyncio.TimeoutError:
            pass
    await cl.heal("edge")


def scenarios() -> list[Scenario]:
    S = Scenario
    return [
        S("S0_normal", "no failure", [], _noop, _noop),
        S("S1_edge_crash", "edge not available (process killed)", ["edge"],
          lambda c: c.crash("edge"), lambda c: c.restart("edge")),
        S("S2_edge_partition", "edge available but unreachable", ["edge"],
          lambda c: c.partition("edge"), lambda c: c.heal("edge")),
        S("S3_edge_slow", "edge up but slow (+3 s, e.g. throttled Pi)", ["edge"],
          lambda c: c.slow("edge", 3000), lambda c: c.slow("edge", 0)),
        S("S4_fog_crash", "fog not available (process killed)", ["fog"],
          lambda c: c.crash("fog"), lambda c: c.restart("fog")),
        S("S5_fog_partition", "fog available but unreachable", ["fog"],
          lambda c: c.partition("fog"), lambda c: c.heal("fog")),
        S("S6_fog_slow", "fog up but slow (+3 s)", ["fog"],
          lambda c: c.slow("fog", 3000), lambda c: c.slow("fog", 0)),
        S("S7_edge_fog_crash", "both local tiers down", ["edge", "fog"],
          lambda c: _both(c.crash("edge"), c.crash("fog")),
          lambda c: _both(c.restart("edge"), c.restart("fog"))),
        S("S8_cloud_crash", "cloud down (only the backup is lost)", ["cloud"],
          lambda c: c.crash("cloud"), lambda c: c.restart("cloud")),
        S("S9_edge_flapping", "edge link goes up/down every 1.5 s", ["edge"],
          _noop, _noop, background=_flap),
    ]


async def _both(a, b):
    await asyncio.gather(a, b)


# --------------------------------------------------------------------------- truth
def expected_values(q: dict, personal: dict, fmap: dict) -> dict[str, str]:
    exp = {}
    for p in q["parts"]:
        if p["op"] in ("personal_lookup", "position"):
            exp[p["id"]] = personal[p["arg"]]
        elif p["op"] == "map_lookup":
            place = exp[p["arg"][1:]] if p["arg"].startswith("$") else p["arg"]
            exp[p["id"]] = fmap.get(place)
        else:
            items = (q.get("scene") or {}).get("text" if p["op"] == "scene_read" else "objects") or []
            exp[p["id"]] = "; ".join(items) or "nothing readable"
    return exp


def affected(q: dict, faulty: list[str]) -> set[str]:
    hit = set()
    for p in q["parts"]:
        if primary_tier(p["op"]) in faulty or any(d in hit for d in p.get("depends_on") or []):
            hit.add(p["id"])
    return hit


# --------------------------------------------------------------------------- E1
async def run_e1(cfg, reps: int, out_raw: Path) -> list[dict]:
    queries = load_json("data/queries.json")
    personal = {p["key"]: p["value"] for p in load_json(cfg["data"]["personal"])}
    fmap = load_json(cfg["data"]["floor_map"])
    router = GoldRouter()
    rows = []
    async with LocalCluster(cfg) as cl:
        try:
            for sc in scenarios():
                for mode in ("baseline", "semantic_rescue"):
                    async with Coordinator(cfg, router, fault_tolerant=mode == "semantic_rescue") as co:
                        for q in queries[::4]:  # warm-up (all query types): health + latency averages
                            await co.ask(q["query"], q.get("scene"), parts=q["parts"])
                        await sc.apply(cl)
                        stop = asyncio.Event()
                        bg = asyncio.create_task(sc.background(cl, stop)) if sc.background else None
                        t_fault = time.time()
                        order = [q for _ in range(reps) for q in queries]
                        for i, q in enumerate(order):
                            r = await co.ask(q["query"], q.get("scene"), parts=q["parts"])
                            exp = expected_values(q, personal, fmap)
                            aff = affected(q, sc.faulty)
                            row = {
                                "scenario": sc.name, "mode": mode, "idx": i, "qid": q["id"], "qtype": q["type"],
                                "t_since_fault_s": round(time.time() - t_fault, 3),
                                "status": r["status"], "latency_s": round(r["latency_s"], 4),
                                "parts": len(r["parts"]),
                                "answered": sum(p["status"] in ("ok", "not_found") for p in r["parts"]),
                                "correct": sum(p["status"] == "ok" and p["value"] == exp[p["id"]] for p in r["parts"]),
                                "affected": len(aff),
                                "affected_answered": sum(p["id"] in aff and p["status"] == "ok" for p in r["parts"]),
                                "fallbacks": sum(p["fallback"] for p in r["parts"]),
                                "hedged": sum(p["hedged"] for p in r["parts"]),
                                "tiers": [p["tier"] for p in r["parts"]],
                            }
                            rows.append(row)
                            with open(out_raw, "a", encoding="utf-8") as f:
                                f.write(json.dumps(row) + "\n")
                        stop.set()
                        if bg:
                            await bg
                        await sc.revert(cl)
                    s = summarize([x for x in rows if x["scenario"] == sc.name and x["mode"] == mode])
                    print(f"{sc.name:20s} {mode:16s} complete={s['complete_pct']:5.1f}% "
                          f"recovery={s['recovery_pct'] if s['recovery_pct'] is not None else '  n/a'}% "
                          f"p50={s['p50_ms']:6.0f} ms p95={s['p95_ms']:6.0f} ms", flush=True)
        finally:
            cl.cleanup()
    return rows


def pct(a, b):
    return None if b == 0 else round(100.0 * a / b, 1)


def summarize(rows: list[dict]) -> dict:
    lat = sorted(r["latency_s"] * 1000 for r in rows)
    n = len(rows)
    return {
        "queries": n,
        "complete_pct": pct(sum(r["status"] == "complete" for r in rows), n),
        "partial_pct": pct(sum(r["status"] == "partial" for r in rows), n),
        "failed_pct": pct(sum(r["status"] == "failed" for r in rows), n),
        "parts_answered_pct": pct(sum(r["answered"] for r in rows), sum(r["parts"] for r in rows)),
        "parts_correct_pct": pct(sum(r["correct"] for r in rows), sum(r["parts"] for r in rows)),
        "recovery_pct": pct(sum(r["affected_answered"] for r in rows), sum(r["affected"] for r in rows)),
        "mean_ms": round(statistics.mean(lat), 1),
        "p50_ms": round(lat[n // 2], 1),
        "p95_ms": round(lat[min(n - 1, int(0.95 * n))], 1),
        "fallback_parts": sum(r["fallbacks"] for r in rows),
        "hedged_parts": sum(r["hedged"] for r in rows),
    }


# --------------------------------------------------------------------------- E2
async def run_e2(cfg, trials: int, out_raw: Path) -> list[dict]:
    """Write a new value on the edge, wait a random time d in [0, max_wait], crash the edge,
    ask for the value (answered by the cloud backup). Stale answer = update lost.
    Then restart the edge (disk intact) and check that the cloud catches up."""
    configs = [(0.5, False), (1.0, False), (2.0, False), (4.0, False), (4.0, True)]
    max_wait = 4.0
    rng = random.Random(7)
    router = GoldRouter()
    seat_q = "What is my seat number?"
    seat_parts = [{"id": "p1", "op": "personal_lookup", "arg": "boarding_pass.seat", "text": "seat"}]
    rows = []
    async with LocalCluster(cfg) as cl:
        try:
            for interval, pow_ in configs:
                await cl.admin("edge", "/admin/sync", {"interval_s": interval, "push_on_write": pow_})
                # sync cost over an idle 8 s window
                s0 = (await cl.admin("edge", "/admin/stats", method="GET"))["sync"]
                await asyncio.sleep(8.0)
                s1 = (await cl.admin("edge", "/admin/stats", method="GET"))["sync"]
                req_per_min = (s1["requests"] - s0["requests"]) * 60 / 8.0
                bytes_per_min = (s1["bytes"] - s0["bytes"]) * 60 / 8.0
                for k in range(trials):
                    new_val = f"{rng.randint(1, 40)}{rng.choice('ABCDEF')}-{k}"
                    async with Coordinator(cfg, router, fault_tolerant=True) as co:
                        # The restart forced a sync just now; wait a random part of the window
                        # so the write lands at a random moment relative to the next sync.
                        await asyncio.sleep(rng.uniform(0, interval))
                        w = await co.write("personal", "boarding_pass.seat", new_val)
                        assert w["tier"] == "edge", w
                        d = rng.uniform(0, max_wait)
                        await asyncio.sleep(d)
                        await cl.crash("edge")
                        r = await co.ask(seat_q, parts=seat_parts)
                        p = r["parts"][0]
                    await cl.restart("edge")  # disk intact: it pushes the missed update
                    await cl.admin("edge", "/admin/sync", {"interval_s": interval, "push_on_write": pow_})
                    cloud_after = await cl.read("cloud", "personal", "boarding_pass.seat")
                    row = {"interval_s": interval, "push_on_write": pow_, "trial": k, "wait_s": round(d, 3),
                           "answer_tier": p["tier"], "answer_value": p["value"], "truth": new_val,
                           "stale": p["value"] != new_val, "staleness_s": p["staleness_s"],
                           "cloud_caught_up": bool(cloud_after and cloud_after["value"] == new_val),
                           "sync_req_per_min": round(req_per_min, 1), "sync_bytes_per_min": round(bytes_per_min)}
                    rows.append(row)
                    with open(out_raw, "a", encoding="utf-8") as f:
                        f.write(json.dumps(row) + "\n")
                sub = [r for r in rows if r["interval_s"] == interval and r["push_on_write"] == pow_]
                print(f"sync {interval:>4}s push_on_write={pow_!s:5}: stale {sum(r['stale'] for r in sub)}/{len(sub)}, "
                      f"caught up {sum(r['cloud_caught_up'] for r in sub)}/{len(sub)}, "
                      f"{req_per_min:.0f} req/min, {bytes_per_min:.0f} B/min", flush=True)
        finally:
            cl.cleanup()
    return rows


# --------------------------------------------------------------------------- main
def write_csv(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["e1", "e2"], default=None)
    ap.add_argument("--reps", type=int, default=2, help="E1: passes over the 24 queries per scenario/mode")
    ap.add_argument("--trials", type=int, default=30, help="E2: trials per sync setting")
    ap.add_argument("--config", default=None)
    args = ap.parse_args()
    cfg = load_config(args.config)
    RESULTS.mkdir(exist_ok=True)

    if args.only in (None, "e1"):
        raw = RESULTS / "e1_raw.jsonl"
        raw.unlink(missing_ok=True)
        rows = await run_e1(cfg, args.reps, raw)
        summary = []
        for sc in scenarios():
            for mode in ("baseline", "semantic_rescue"):
                sub = [r for r in rows if r["scenario"] == sc.name and r["mode"] == mode]
                summary.append({"scenario": sc.name, "description": sc.desc, "mode": mode, **summarize(sub)})
        write_csv(RESULTS / "e1_summary.csv", summary)

    if args.only in (None, "e2"):
        raw = RESULTS / "e2_raw.jsonl"
        raw.unlink(missing_ok=True)
        rows = await run_e2(cfg, args.trials, raw)
        summary = []
        for key in sorted({(r["interval_s"], r["push_on_write"]) for r in rows}):
            sub = [r for r in rows if (r["interval_s"], r["push_on_write"]) == key]
            # Expected loss if the crash comes d ~ U(0, 4 s) after the write and the next sync
            # is U(0, T) away: P(stale) = T / (2 * 4) for T <= 4 (0 with push-on-write).
            expected = 0.0 if key[1] else round(100 * min(1.0, key[0] / 8.0), 1)
            summary.append({"interval_s": key[0], "push_on_write": key[1], "trials": len(sub),
                            "stale_pct": pct(sum(r["stale"] for r in sub), len(sub)),
                            "expected_stale_pct": expected,
                            "caught_up_pct": pct(sum(r["cloud_caught_up"] for r in sub), len(sub)),
                            "sync_req_per_min": sub[0]["sync_req_per_min"],
                            "sync_bytes_per_min": sub[0]["sync_bytes_per_min"]})
        write_csv(RESULTS / "e2_summary.csv", summary)
    print("results written to", RESULTS)


if __name__ == "__main__":
    asyncio.run(main())
