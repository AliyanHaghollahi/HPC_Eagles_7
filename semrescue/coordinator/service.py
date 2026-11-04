"""HTTP front-end for the coordinator, for a real multi-device testbed.
Runs on the device that plays the smart glasses (e.g. the laptop or a second Pi).

  python -m semrescue.coordinator.service --config config/testbed.yaml --router keyword
  curl -X POST localhost:8000/ask -H "content-type: application/json" -d "{\"query\": \"Where is my gate?\"}"
"""
from __future__ import annotations

import argparse
import contextlib

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from ..config import load_config
from .coordinator import Coordinator, InvalidPlanError
from .router import GoldRouter, KeywordRouter, SLMRouter


class Ask(BaseModel):
    query: str
    scene: dict | None = None
    baseline: bool = False


class Write(BaseModel):
    ns: str
    key: str
    value: str


def create_app(cfg: dict, router) -> FastAPI:
    coords = {True: Coordinator(cfg, router, fault_tolerant=True),
              False: Coordinator(cfg, router, fault_tolerant=False)}

    @contextlib.asynccontextmanager
    async def lifespan(app):
        for c in coords.values():
            await c.start()
        yield
        for c in coords.values():
            await c.stop()

    app = FastAPI(title="SemanticRescue coordinator", lifespan=lifespan)

    @app.post("/ask")
    async def ask(req: Ask):
        try:
            return await coords[not req.baseline].ask(req.query, req.scene)
        except InvalidPlanError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/write")
    async def write(req: Write):
        return await coords[True].write(req.ns, req.key, req.value)

    @app.get("/status")
    async def status():
        return coords[True].status()

    return app


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--router", choices=["gold", "keyword", "slm"], default="keyword")
    ap.add_argument("--slm-model", default="gemma3:4b")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()
    cfg = load_config(args.config)
    kw = KeywordRouter()
    router = {"gold": GoldRouter(), "keyword": kw,
              "slm": SLMRouter(cfg["backend"]["ollama_url"], args.slm_model, kw)}[args.router]
    import uvicorn
    uvicorn.run(create_app(cfg, router), host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
