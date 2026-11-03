"""Answer generation for a tier.

Every tier first RETRIEVES the fact it needs (personal store, floor map, or the
camera scene) and then GENERATES a sentence:
  - MockBackend: a fixed template (deterministic; used for the experiments)
  - OllamaBackend: asks a local model to phrase the answer from the retrieved fact
The retrieved `value` is returned separately so correctness can be scored exactly.
"""
from __future__ import annotations

import asyncio

import httpx


class MockBackend:
    def __init__(self, compute_ms: float = 0):
        self.compute_s = compute_ms / 1000.0

    async def generate(self, op: str, question: str, value: str, label: str = "") -> str:
        if self.compute_s:
            await asyncio.sleep(self.compute_s)  # emulate model inference time
        if op == "personal_lookup":
            return f"Your {label or 'record'} is {value}."
        if op == "position":
            return f"You are at {value}."
        if op == "map_lookup":
            return value
        if op == "scene_read":
            return f"The text says: {value}."
        if op == "scene_describe":
            return f"In front of you: {value}."
        return value


class OllamaBackend:
    def __init__(self, url: str, model: str, timeout_s: float = 60.0):
        self.url = url.rstrip("/")
        self.model = model
        self.timeout_s = timeout_s

    async def generate(self, op: str, question: str, value: str, label: str = "") -> str:
        prompt = (
            "You help a visually impaired user. Answer the question in one short sentence, "
            "using only the fact given. Do not add anything else.\n"
            f"Fact: {label + ': ' if label else ''}{value}\nQuestion: {question}\nAnswer:"
        )
        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            r = await client.post(
                f"{self.url}/api/generate",
                json={"model": self.model, "prompt": prompt, "stream": False,
                      "options": {"temperature": 0, "seed": 0}},
            )
            r.raise_for_status()
            return r.json()["response"].strip()


def make_backend(cfg: dict, role: str):
    b = cfg["backend"]
    if b["type"] == "ollama":
        return OllamaBackend(b["ollama_url"], b["models"][role])
    return MockBackend(cfg["tiers"][role].get("compute_ms", 0))
