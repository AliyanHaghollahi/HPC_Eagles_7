"""Fast unit tests (no network)."""
import asyncio

from semrescue.coordinator.fusion import fuse
from semrescue.coordinator.health import CLOSED, DOWN, HALF_OPEN, OPEN, CircuitBreaker
from semrescue.coordinator.coordinator import Coordinator, InvalidPlanError
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


def test_hedge_cancellation_does_not_count_as_tier_failure():
    cfg = {
        "coordinator": {
            "part_deadline_s": 1.0, "hedging": True, "hedge_factor": 1.0,
            "hedge_min_s": 0.01, "hedge_max_s": 0.01,
            "breaker_failures": 3, "breaker_open_s": 5.0,
        }
    }
    co = Coordinator(cfg, router=None, urls={"fog": "", "cloud": ""})

    async def fake_call(tier, payload):
        delay = 0.1 if tier == "fog" else 0.001
        await asyncio.sleep(delay)
        return {"outcome": "ok", "latency_s": delay,
                "data": {"answer": tier, "value": tier}}

    co._call = fake_call
    part = {"id": "p1", "op": "scene_read", "arg": "", "text": "read it"}
    result = asyncio.run(co._run_part(part, None))

    assert result["tier"] == "cloud" and result["hedged"]
    assert co.health.tiers["fog"].breaker.failures == 0
    assert any(a["outcome"] == "cancelled_slow" for a in result["attempts"])


def test_open_circuit_is_skipped_but_heartbeat_down_tier_is_last_resort():
    cfg = {"coordinator": {"breaker_failures": 2, "breaker_open_s": 30.0}}
    co = Coordinator(cfg, router=None, urls={"edge": "", "cloud": ""})
    edge = co.health.tiers["edge"]

    edge.breaker.record_failure()
    edge.breaker.record_failure()
    assert edge.breaker.state == OPEN
    assert co._order(["edge", "cloud"]) == ["cloud"]

    edge.breaker.record_success()
    edge.set_status(DOWN)
    assert co._order(["edge", "cloud"]) == ["cloud", "edge"]


def test_route_plan_validation_reports_model_errors_clearly():
    valid = [
        {"id": "p1", "op": "personal_lookup", "arg": "boarding_pass.gate"},
        {"id": "p2", "op": "map_lookup", "arg": "$p1", "depends_on": ["p1"]},
    ]
    Coordinator._validate_parts(valid)

    invalid_plans = [
        ([valid[0], dict(valid[0])], "duplicate part id"),
        ([{"id": "p1", "op": "invent_answer", "arg": ""}], "unsupported operation"),
        ([{"id": "p2", "op": "map_lookup", "arg": "$p1", "depends_on": ["p1"]}],
         "unknown or later"),
        ([{"id": "p1", "op": "map_lookup", "arg": "$missing"}],
         "without declaring it"),
    ]
    for parts, expected in invalid_plans:
        try:
            Coordinator._validate_parts(parts)
        except InvalidPlanError as exc:
            assert expected in str(exc)
        else:
            raise AssertionError(f"plan should have been rejected: {parts}")
