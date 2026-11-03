"""Semantic router: turns a user query into sub-queries (parts), each with an operation.

Three interchangeable routers:
  GoldRouter     uses the hand-written plans in data/queries.json. The experiments use
                 this one so results measure fault tolerance, not routing mistakes.
  KeywordRouter  small rule-based router for free-text demos; runs anywhere.
  SLMRouter      hook for the trained SLM router from the semantic routing project
                 (served by Ollama). If its host is down, it falls back to KeywordRouter,
                 so routing itself survives an edge failure.

A part looks like:
  {"id": "p2", "op": "map_lookup", "arg": "$p1", "text": "where is that gate", "depends_on": ["p1"]}
"$p1" means "use the value returned by part p1" (sequential dependency).
"""
from __future__ import annotations

import json
import re

import httpx

from ..config import load_json


class GoldRouter:
    def __init__(self, queries_path: str = "data/queries.json"):
        self.by_text = {q["query"].lower(): q for q in load_json(queries_path)}

    async def route(self, query: str) -> list[dict]:
        q = self.by_text.get(query.lower().strip())
        if q is None:
            raise KeyError(f"query not in the gold set: {query}")
        return [dict(p) for p in q["parts"]]


class KeywordRouter:
    SCENE_READ = ("say", "read", "sign", "notice", "menu", "label", "written")
    SCENE_DESCRIBE = ("in front of me", "what do you see", "describe", "around me")
    # personal keys whose value is a place in the floor map (Where is my X? -> lookup, then map)
    PLACE_KEYS = {"gate": "boarding_pass.gate", "class": "schedule.next_class_room",
                  "appointment": "appointment.room"}

    def __init__(self, personal_path: str = "data/personal.json", map_path: str = "data/floor_map.json"):
        self.personal = load_json(personal_path)
        self.places = list(load_json(map_path).keys())

    async def route(self, query: str) -> list[dict]:
        chunks = [c.strip(" ?.!,") for c in re.split(r"\band\b|\?|;", query.lower()) if c.strip(" ?.!,")]
        parts: list[dict] = []
        for chunk in chunks:
            parts.extend(self._route_chunk(chunk, len(parts)))
        return parts or [{"id": "p1", "op": "scene_describe", "arg": "", "text": query}]

    def _route_chunk(self, c: str, offset: int) -> list[dict]:
        pid = lambda i: f"p{offset + i}"
        if re.search(r"\bwhere is my\b", c):
            for word, key in self.PLACE_KEYS.items():
                if word in c:
                    return [{"id": pid(1), "op": "personal_lookup", "arg": key, "text": f"which {word} is mine"},
                            {"id": pid(2), "op": "map_lookup", "arg": f"${pid(1)}", "text": f"where is my {word}",
                             "depends_on": [pid(1)]}]
        if "where am i" in c or "my position" in c:
            return [{"id": pid(1), "op": "position", "arg": "device.position", "text": c}]
        if any(k in c for k in self.SCENE_DESCRIBE):
            return [{"id": pid(1), "op": "scene_describe", "arg": "", "text": c}]
        if re.search(r"\b(my|i|me)\b", c):
            best = self._best_personal(c)
            if best:
                return [{"id": pid(1), "op": "personal_lookup", "arg": best, "text": c}]
        for place in self.places:
            if place.lower() in c:
                return [{"id": pid(1), "op": "map_lookup", "arg": place, "text": c}]
        if any(re.search(rf"\b{k}\b", c) for k in self.SCENE_READ):
            return [{"id": pid(1), "op": "scene_read", "arg": "", "text": c}]
        return [{"id": pid(1), "op": "scene_describe", "arg": "", "text": c}]

    def _best_personal(self, c: str) -> str | None:
        words = set(re.findall(r"[a-z]+", c)) - {"my", "is", "what", "the", "i", "am", "to", "does", "when", "me"}
        best, score = None, 0
        for p in self.personal:
            vocab = set(re.findall(r"[a-z]+", " ".join(p["aliases"] + [p["label"]]).lower()))
            s = len(words & vocab)
            if s > score:
                best, score = p["key"], s
        return best


class SLMRouter:
    """Calls an Ollama-served SLM that returns the parts as JSON. Replace PROMPT with the
    prompt (or fine-tuned model) from the semantic routing project."""

    PROMPT = (
        "Split the user's question into parts. For each part output an object with keys "
        "id, op, arg, text, depends_on. op is one of personal_lookup, position, map_lookup, "
        "scene_read, scene_describe. Personal keys: {keys}. Places: {places}. "
        "Use \"$p1\" as arg when a part needs the value of part p1. "
        "Reply with a JSON list only.\nQuestion: {query}\nJSON:"
    )

    def __init__(self, url: str, model: str, fallback: KeywordRouter, timeout_s: float = 10.0):
        self.url, self.model, self.fallback, self.timeout_s = url.rstrip("/"), model, fallback, timeout_s
        self.used_fallback = 0

    async def route(self, query: str) -> list[dict]:
        prompt = self.PROMPT.format(keys=[p["key"] for p in self.fallback.personal],
                                    places=self.fallback.places, query=query)
        try:
            async with httpx.AsyncClient(timeout=self.timeout_s) as c:
                r = await c.post(f"{self.url}/api/generate", json={
                    "model": self.model, "prompt": prompt, "stream": False, "format": "json",
                    "options": {"temperature": 0, "seed": 0}})
                r.raise_for_status()
                parts = json.loads(r.json()["response"])
                if isinstance(parts, dict):
                    parts = parts.get("parts", [parts])
                assert parts and all("op" in p for p in parts)
                return parts
        except Exception:
            self.used_fallback += 1
            return await self.fallback.route(query)
