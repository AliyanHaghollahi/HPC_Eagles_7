"""Fast unit tests (no network)."""
import asyncio

from semrescue.coordinator.fusion import fuse
from semrescue.coordinator.health import CLOSED, HALF_OPEN, OPEN, CircuitBreaker
from semrescue.coordinator.policy import FALLBACK_CHAINS, PERSONAL_OPS, WRITE_CHAINS, chain_for
from semrescue.coordinator.router import KeywordRouter
from semrescue.store import Store


def test_personal_data_never_routed_to_fog():
    for op in PERSONAL_OPS:
        assert "fog" not in FALLBACK_CHAINS[op]
    assert "fog" not in WRITE_CHAINS["personal"]


def test_baseline_uses_primary_only():
    assert chain_for("personal_lookup", False) == ["edge"]
    assert chain_for("map_lookup", True) == ["fog", "edge", "cloud"]
    assert chain_for("scene_read", True) == ["fog", "cloud"]  # edge has no vision


def test_store_last_writer_wins_and_changes():
    s = Store()
    assert s.put("personal", "k", "old", origin="edge", updated_at=1.0)
    assert not s.put("personal", "k", "older", origin="cloud", updated_at=0.5)
    assert s.put("personal", "k", "new", origin="cloud", updated_at=2.0)
    assert s.get("personal", "k")["value"] == "new"
    recs, seq = s.changes_since("personal", 0)
    assert len(recs) == 1 and seq == 2
    assert s.changes_since("personal", 0, exclude_origin="cloud")[0] == []


def test_circuit_breaker_cycle():
    now = [0.0]
    b = CircuitBreaker(failures_to_open=2, open_s=5.0, clock=lambda: now[0])
    b.record_failure()
    assert b.state == CLOSED and b.allow()
    b.record_failure()
    assert b.state == OPEN and not b.allow()
    now[0] = 6.0
    assert b.allow() and b.state == HALF_OPEN
    b.record_failure()
    assert b.state == OPEN
    now[0] = 12.0
    assert b.allow()
    b.record_success()
    assert b.state == CLOSED


def _part(pid, op, status, answer=None, staleness=None):
    return {"id": pid, "op": op, "text": "x", "status": status, "answer": answer, "staleness_s": staleness}


def test_fusion_partial_and_stale():
    r = fuse([_part("p1", "scene_read", "ok", "The text says: Exit."),
              _part("p2", "personal_lookup", "failed")])
    assert r["status"] == "partial" and r["completeness"] == 0.5
    assert "personal information" in r["text"]
    r = fuse([_part("p1", "personal_lookup", "ok", "Your seat is 14C.", staleness=600)])
    assert r["status"] == "complete" and "may be out of date" in r["text"]
    assert fuse([_part("p1", "map_lookup", "failed")])["status"] == "failed"


def test_keyword_router():
    r = KeywordRouter()
    parts = asyncio.run(r.route("Where is my gate?"))
    assert [p["op"] for p in parts] == ["personal_lookup", "map_lookup"]
    assert parts[1]["arg"] == "$p1" and parts[1]["depends_on"] == ["p1"]
    parts = asyncio.run(r.route("What does this sign say and what is my seat number?"))
    assert [p["op"] for p in parts] == ["scene_read", "personal_lookup"]
    assert parts[1]["arg"] == "boarding_pass.seat"
