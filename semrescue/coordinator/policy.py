"""Backup-tier policy: which tier answers a sub-query when its primary tier fails.

The order is decided by WHO HOLDS THE DATA first and by distance second:

  operation        primary   backups (in order)   why
  ---------------  --------  -------------------  ------------------------------------------
  personal_lookup  edge      cloud                personal facts live on the edge; the cloud
  position         edge      cloud                keeps a synced backup. The fog is closer but
                                                  never holds personal data (privacy rule), so
                                                  it cannot answer, however fast it is.
  map_lookup       fog       edge, cloud          the floor map is not private, so the edge keeps
                                                  a cached copy: closest backup, no vision needed.
  scene_read       fog       cloud                needs a vision model; the Pi edge has none.
  scene_describe   fog       cloud

Writes follow the same idea: personal writes go to edge, else cloud (edge pulls them
back when it recovers); map updates go to fog, else cloud.
"""
from __future__ import annotations

FALLBACK_CHAINS: dict[str, list[str]] = {
    "personal_lookup": ["edge", "cloud"],
    "position": ["edge", "cloud"],
    "map_lookup": ["fog", "edge", "cloud"],
    "scene_read": ["fog", "cloud"],
    "scene_describe": ["fog", "cloud"],
}

WRITE_CHAINS: dict[str, list[str]] = {
    "personal": ["edge", "cloud"],
    "map": ["fog", "cloud"],
}

PERSONAL_OPS = {"personal_lookup", "position"}

# Hard rule, checked by tests: personal data is never sent to the fog.
for _op in PERSONAL_OPS:
    assert "fog" not in FALLBACK_CHAINS[_op]
assert "fog" not in WRITE_CHAINS["personal"]


def primary_tier(op: str) -> str:
    return FALLBACK_CHAINS[op][0]


def chain_for(op: str, fault_tolerant: bool) -> list[str]:
    """Baseline (no fault tolerance) only ever uses the primary tier."""
    chain = FALLBACK_CHAINS[op]
    return list(chain) if fault_tolerant else chain[:1]


def write_chain_for(ns: str, fault_tolerant: bool) -> list[str]:
    chain = WRITE_CHAINS[ns]
    return list(chain) if fault_tolerant else chain[:1]
