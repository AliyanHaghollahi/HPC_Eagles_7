"""End-to-end tests: real tier processes, real proxies, injected failures.
Uses its own ports so it can run next to a demo cluster."""
import asyncio

import pytest

from semrescue.cluster import LocalCluster
from semrescue.config import load_config
from semrescue.coordinator.coordinator import Coordinator
from semrescue.coordinator.router import GoldRouter

CFG = load_config(overrides={
    "tiers": {"edge": {"port": 18001}, "fog": {"port": 18002}, "cloud": {"port": 18003}},
    "network": {"proxy_ports": {"edge": 19001, "fog": 19002, "cloud": 19003}},
    "sync": {"interval_s": 0.5},
    "coordinator": {"request_timeout_s": 1.0, "part_deadline_s": 2.5},
})
GATE = "Where is my gate?"
SIGN = "What does this sign say and when does my flight leave?"
SCENE = {"objects": ["departure board"], "text": ["AA 1432 Dallas - ON TIME"]}


LOOP = asyncio.new_event_loop()  # one loop for the module: the fault proxies live in it


def run(coro):
    return LOOP.run_until_complete(coro)


async def _ask_both(cl, q, scene=None, settle_s=1.2):
    out = {}
    for ft in (False, True):
        async with Coordinator(CFG, GoldRouter(), fault_tolerant=ft) as co:
            await asyncio.sleep(settle_s)  # let heartbeats notice the failure
            out[ft] = await co.ask(q, scene)
    return out[False], out[True]


@pytest.fixture(scope="module")
def cluster():
    cl = LocalCluster(CFG)
    run(cl.start())
    yield cl
    run(cl.stop())
    cl.cleanup()


def test_normal_operation(cluster):
    base, ft = run(_ask_both(cluster, GATE, settle_s=0))
    for r in (base, ft):
        assert r["status"] == "complete"
        assert [p["tier"] for p in r["parts"]] == ["edge", "fog"]


def test_edge_crash_personal_part_goes_to_cloud(cluster):
    async def go():
        await cluster.crash("edge")
        try:
            return await _ask_both(cluster, GATE)
        finally:
            await cluster.restart("edge")
    base, ft = run(go())
    assert base["status"] == "failed"
    assert ft["status"] == "complete"
    assert ft["parts"][0]["tier"] == "cloud" and ft["parts"][0]["value"] == "gate D20"


def test_fog_partition_uses_edge_map_cache_and_cloud_vision(cluster):
    async def go():
        await cluster.partition("fog")
        try:
            return await _ask_both(cluster, SIGN, SCENE)
        finally:
            await cluster.heal("fog")
    base, ft = run(go())
    assert base["status"] == "partial"
    assert ft["status"] == "complete"
    assert ft["parts"][0]["tier"] == "cloud"  # scene needs vision: cloud, not edge


def test_fog_never_answers_personal(cluster):
    async def go():
        async with Coordinator(CFG, GoldRouter()) as co:
            r = await co.client.post(f"{co.urls['fog']}/answer",
                                     json={"op": "personal_lookup", "arg": "boarding_pass.seat"})
            return r.status_code
    assert run(go()) == 403


def test_edge_recovers_from_cloud_after_disk_loss(cluster):
    async def go():
        async with Coordinator(CFG, GoldRouter()) as co:
            await co.write("personal", "boarding_pass.seat", "31F")
        await asyncio.sleep(1.2)  # > sync interval: the cloud now has it
        await cluster.restart("edge", wipe=True)
        return await cluster.read("edge", "personal", "boarding_pass.seat")
    rec = run(go())
    assert rec is not None and rec["value"] == "31F"


def test_write_during_edge_outage_reaches_edge_after_recovery(cluster):
    async def go():
        await cluster.crash("edge")
        async with Coordinator(CFG, GoldRouter()) as co:
            await asyncio.sleep(1.2)
            w = await co.write("personal", "diet.allergy", "shellfish")
        await cluster.restart("edge")
        return w, await cluster.read("edge", "personal", "diet.allergy")
    w, rec = run(go())
    assert w["tier"] == "cloud" and rec["value"] == "shellfish"
