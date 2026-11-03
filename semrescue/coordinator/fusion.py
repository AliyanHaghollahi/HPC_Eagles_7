"""Fault-aware response fusion.

Keeps every sub-answer that succeeded, explains what is missing instead of failing the
whole query, and warns when an answer came from a backup copy that may be out of date.
Template-based on purpose: it runs on the glasses side and must work when every
model tier is down.
"""
from __future__ import annotations

from .policy import PERSONAL_OPS


def fuse(parts: list[dict], stale_warn_s: float = 30.0) -> dict:
    sentences, answered = [], 0
    for p in parts:
        if p["status"] == "ok":
            answered += 1
            s = p["answer"].rstrip()
            if p.get("staleness_s") is not None and p["staleness_s"] > stale_warn_s:
                mins = p["staleness_s"] / 60
                age = f"{mins:.0f} min" if mins >= 1 else f"{p['staleness_s']:.0f} s"
                s += f" (backup copy, last updated {age} ago, so it may be out of date)"
            sentences.append(s)
        elif p["status"] == "not_found":
            answered += 1
            sentences.append(f"I don't have that information ({p['text']}).")
        elif p["status"] == "skipped":
            sentences.append(f"I couldn't answer \"{p['text']}\" because the step before it failed.")
        else:
            if p["op"] in PERSONAL_OPS:
                sentences.append("I can't reach your personal information right now, please try again in a moment.")
            else:
                sentences.append(f"I can't reach the service for \"{p['text']}\" right now.")
    total = len(parts)
    status = "complete" if answered == total else ("partial" if answered else "failed")
    return {"status": status, "answered_parts": answered, "total_parts": total,
            "completeness": answered / total if total else 0.0,
            "text": " ".join(_cap(s) for s in sentences)}


def _cap(s: str) -> str:
    s = s.strip()
    s = s[0].upper() + s[1:] if s else s
    return s if s.endswith((".", "!", "?", ")")) else s + "."
