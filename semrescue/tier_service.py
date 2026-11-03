"""Edge / Fog / Cloud tier service.

One program, three roles:
  edge  (Raspberry Pi 5): personal facts + device position, cached floor map, no vision
  fog   (laptop):         floor map + camera scene (vision), NO personal data (privacy rule)
  cloud (backup tier):    replica of the personal store (from edge) and the floor map,
                          can do vision; slower to reach (WAN)

Run:  python -m semrescue.tier_service --role edge
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .backend import make_backend
from .config import load_config, load_json, resolve_path, tier_direct_url
from .store import Store
from .sync import SYNC_PLAN, SyncAgent

# Which operations each tier can answer.
CAPABILITIES = {
    "edge": {"personal_lookup", "position", "map_lookup"},
    "fog": {"map_lookup", "scene_read", "scene_describe"},
    "cloud": {"personal_lookup", "position", "map_lookup", "scene_read", "scene_describe"},
}
PERSONAL_OPS = {"personal_lookup", "position"}
SCENE_OPS = {"scene_read", "scene_describe"}
# Who may write which namespace directly.
WRITABLE = {"edge": {"personal"}, "fog": {"map"}, "cloud": {"personal", "map"}}


class AnswerRequest(BaseModel):
    op: str
    arg: str = ""
    text: str = ""
    scene: dict | None = None


class WriteRequest(BaseModel):
    ns: str
    key: str
    value: str


class PushRequest(BaseModel):
    origin: str
    ns: str
    records: list[dict]


def create_app(role: str, cfg: dict, seed: bool = True, db_path: str | None = None) -> FastAPI:
    assert role in CAPABILITIES, role
    runtime = resolve_path(cfg["data"]["runtime_dir"])
    store = Store(db_path or runtime / f"{role}.db")
    backend = make_backend(cfg, role)
    labels = {p["key"]: p["label"] for p in load_json(cfg["data"]["personal"])}
    state = {"slow_ms": 0, "started": time.time()}

    if seed:
        if role == "edge" and store.count("personal") == 0:
            for p in load_json(cfg["data"]["personal"]):
                store.put("personal", p["key"], p["value"], origin="seed", updated_at=0.0)
        if role in ("fog", "cloud") and store.count("map") == 0:
            for place, directions in load_json(cfg["data"]["floor_map"]).items():
                store.put("map", place, directions, origin="seed", updated_at=0.0)

    sync_agent = None
    if role in SYNC_PLAN:
        sync_agent = SyncAgent(role, store, tier_direct_url(cfg, "cloud"),
                               cfg["sync"]["interval_s"], cfg["sync"].get("push_on_write", False))

    @contextlib.asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(sync_agent.run()) if sync_agent else None
        yield
        if task:
            task.cancel()
        store.close()

    app = FastAPI(title=f"SemanticRescue {role} tier", lifespan=lifespan)
    app.state.store = store
    app.state.sync = sync_agent

    async def maybe_slow():
        if state["slow_ms"]:
            await asyncio.sleep(state["slow_ms"] / 1000.0)

    # ------------------------------------------------------------ health
    @app.get("/health")
    async def health():
        last = sync_agent.stats["last_ok"] if sync_agent else None  # this process, not the old disk
        return {"role": role, "status": "ok", "uptime_s": round(time.time() - state["started"], 2),
                "personal": store.count("personal"), "map": store.count("map"),
                "last_sync_ok": float(last) if last else None}

    # ------------------------------------------------------------ answer
    @app.post("/answer")
    async def answer(req: AnswerRequest):
        await maybe_slow()
        if role == "fog" and req.op in PERSONAL_OPS:
            raise HTTPException(403, "privacy rule: personal data is never processed on the fog")
        if req.op not in CAPABILITIES[role]:
            raise HTTPException(422, f"{role} cannot run {req.op}")

        staleness = 0.0
        if req.op in PERSONAL_OPS:
            rec = store.get("personal", req.arg)
            if rec is None:
                raise HTTPException(404, f"no personal record {req.arg}")
            value, label = rec["value"], labels.get(req.arg, req.arg)
            if role == "cloud":
                last = store.meta_get("last_push_from:edge")
                staleness = time.time() - float(last) if last else float("inf")
        elif req.op == "map_lookup":
            rec = store.get("map", req.arg)
            if rec is None:
                raise HTTPException(404, f"place not in floor map: {req.arg}")
            value, label = rec["value"], req.arg
            if role == "edge":  # cached copy
                last = store.meta_get("last_sync_ok")
                staleness = time.time() - float(last) if last else float("inf")
        else:  # scene ops: the image/scene travels with the request
            scene = req.scene or {}
            items = scene.get("text") if req.op == "scene_read" else scene.get("objects")
            value, label = "; ".join(items or []) or "nothing readable", "scene"

        text = await backend.generate(req.op, req.text, value, label)
        return {"tier": role, "op": req.op, "answer": text, "value": value,
                "staleness_s": None if staleness == float("inf") else round(staleness, 3)}

    # ------------------------------------------------------------ writes
    @app.post("/write")
    async def write(req: WriteRequest):
        await maybe_slow()
        if req.ns not in WRITABLE[role]:
            raise HTTPException(403, f"{role} may not write namespace {req.ns}")
        store.put(req.ns, req.key, req.value, origin=role)
        if sync_agent:
            sync_agent.notify_write()
        return {"tier": role, "ok": True}

    @app.get("/read")
    async def read(ns: str, key: str):
        if role == "fog" and ns == "personal":
            raise HTTPException(403, "privacy rule")
        rec = store.get(ns, key)
        if rec is None:
            raise HTTPException(404, "not found")
        return rec

    # ------------------------------------------------------------ sync (cloud side)
    if role == "cloud":
        @app.post("/sync/push")
        async def sync_push(req: PushRequest):
            allowed = {ns for ns in SYNC_PLAN.get(req.origin, {}).get("push", [])}
            if req.ns not in allowed:
                raise HTTPException(403, f"{req.origin} may not push {req.ns}")
            applied = 0
            for r in req.records:
                applied += store.put(req.ns, r["key"], r["value"], origin=r["origin"],
                                     updated_at=r["updated_at"])
            store.meta_set(f"last_push_from:{req.origin}", time.time())
            return {"applied": applied}

        @app.get("/sync/pull")
        async def sync_pull(ns: str, since: int, requester: str):
            if ns not in SYNC_PLAN.get(requester, {}).get("pull", []):
                raise HTTPException(403, f"{requester} may not pull {ns}")
            # No origin filter: a node that lost its disk must get back the records it wrote
            # itself. Echoes of a node's own writes are no-ops thanks to last-writer-wins.
            recs, max_seq = store.changes_since(ns, since)
            return {"records": recs, "cursor": max_seq}

    # ------------------------------------------------------------ admin / fault injection
    @app.post("/admin/slow")
    async def admin_slow(ms: int = 0):
        """Gray failure: the tier stays up and answers health checks, but /answer is slow
        (e.g. a thermally throttled Raspberry Pi)."""
        state["slow_ms"] = ms
        return {"slow_ms": ms}

    @app.post("/admin/sync")
    async def admin_sync(paused: bool | None = None, interval_s: float | None = None,
                         push_on_write: bool | None = None, now: bool = False):
        if not sync_agent:
            return {"sync": None}
        if paused is not None:
            sync_agent.paused = paused
        if interval_s is not None:
            sync_agent.interval_s = interval_s
            sync_agent._wake.set()  # end the current wait so the new window applies right away
        if push_on_write is not None:
            sync_agent.push_on_write = push_on_write
        if now:
            await sync_agent.sync_once()
        return {"paused": sync_agent.paused, "interval_s": sync_agent.interval_s,
                "push_on_write": sync_agent.push_on_write}

    @app.get("/admin/stats")
    async def admin_stats():
        return {"role": role, "slow_ms": state["slow_ms"],
                "sync": dict(sync_agent.stats) if sync_agent else None}

    return app


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--role", required=True, choices=list(CAPABILITIES))
    ap.add_argument("--config", default=None)
    ap.add_argument("--host", default=None, help="bind address (default: from config; use 0.0.0.0 on a real device)")
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--db", default=None, help="database file (default runtime/<role>.db)")
    ap.add_argument("--no-seed", action="store_true", help="start empty and recover from the cloud")
    ap.add_argument("--sync-interval", type=float, default=None)
    args = ap.parse_args()

    import uvicorn
    overrides = {}
    if args.sync_interval is not None:
        overrides = {"sync": {"interval_s": args.sync_interval}}
    cfg = load_config(args.config, overrides)
    app = create_app(args.role, cfg, seed=not args.no_seed, db_path=args.db)
    t = cfg["tiers"][args.role]
    uvicorn.run(app, host=args.host or t["host"], port=args.port or t["port"], log_level="warning")


if __name__ == "__main__":
    main()
