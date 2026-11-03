"""Configuration loading."""
from __future__ import annotations

import copy
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = ROOT / "config" / "default.yaml"
TIERS = ("edge", "fog", "cloud")


def load_config(path: str | Path | None = None, overrides: dict | None = None) -> dict:
    with open(path or DEFAULT_CONFIG, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if overrides:
        cfg = deep_merge(cfg, overrides)
    return cfg


def deep_merge(base: dict, extra: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in extra.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def tier_direct_url(cfg: dict, tier: str) -> str:
    t = cfg["tiers"][tier]
    return f"http://{t['host']}:{t['port']}"


def tier_client_url(cfg: dict, tier: str) -> str:
    """URL the coordinator (smart glasses) uses: through the fault proxy if enabled."""
    if cfg["network"].get("use_proxy"):
        return f"http://127.0.0.1:{cfg['network']['proxy_ports'][tier]}"
    return tier_direct_url(cfg, tier)


def resolve_path(p: str | Path) -> Path:
    p = Path(p)
    return p if p.is_absolute() else ROOT / p


def load_json(p: str | Path):
    with open(resolve_path(p), "r", encoding="utf-8") as f:
        return json.load(f)
