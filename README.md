# SemanticRescue

**Fault-tolerant semantic routing for edge-fog-cloud smart glasses.**
Cloud Computing course project, HPC Eagles (HPCC Lab, University of North Texas).

A semantic router sends each part of a user's question to the tier that holds the data:
personal parts to the **edge** (Raspberry Pi 5), environmental parts to the **fog** (laptop).
SemanticRescue keeps the system answering when the edge or the fog crashes, becomes
unreachable, or gets slow: it detects the failure, sends the failed part to the best backup
tier, keeps the cloud backup in sync, and fuses whatever answers are available.

The design (failure scenarios, which tier backs up which, sync window) is in
[docs/DESIGN.md](docs/DESIGN.md).

```
                 +-----------+   personal parts    +-------------------------------+
  smart glasses  |           | ------------------> | EDGE  (Pi 5)                  |
  (coordinator)  |  router   |                     | personal store + map cache    |
   query ------> |  health   |   environmental     +-------------------------------+
                 |  failover | ------------------> | FOG   (laptop)                |
   answer <----- |  fusion   |                     | floor map + camera scene      |
                 |           |   backup path       +-------------------------------+
                 +-----------+ - - - - - - - - - > | CLOUD (backup)                |
                                                   | replicas, synced every 2 s    |
                                                   +-------------------------------+
```

## Backup policy in one table

| Sub-query | Primary | Backups | Why |
|---|---|---|---|
| Personal fact / position | Edge | Cloud | Only edge and cloud hold personal data; the fog never does (privacy rule) |
| Map / directions | Fog | Edge (cached map), Cloud | Map is not private, so the closest tier with a copy goes first |
| Scene (read / describe) | Fog | Cloud | Needs vision; the Pi edge has none |

## Results

Local run: three tier processes on one machine, emulated link delays (edge 5 ms,
fog 10 ms, cloud 60 ms one way) and compute times, mock answer backend. Each scenario
covers 48 queries (24 queries, 8 personal, 8 environmental, 8 mixed, asked twice), and the
failure starts right before the first query. Reproduce with `python scripts/run_experiments.py`.

**E1: failure scenarios.** Request timeout 2 s for both systems.

| Scenario | Complete answers (baseline → SemanticRescue) | Correct sub-answers | p95 response time |
|---|---|---|---|
| no failure | 100% → **100%** | 100% → **100%** | 191 ms → 191 ms |
| edge not available (process killed) | 33% → **100%** | 41% → **100%** | 117 ms → 282 ms |
| edge available but unreachable | 33% → **100%** | 41% → **100%** | 2,007 ms → 466 ms |
| edge up but slow (+3 s, e.g. throttled Pi) | 33% → **100%** | 41% → **100%** | 2,008 ms → 467 ms |
| fog not available (process killed) | 33% → **100%** | 50% → **100%** | 78 ms → 167 ms |
| fog available but unreachable | 33% → **100%** | 50% → **100%** | 2,082 ms → 169 ms |
| fog up but slow (+3 s) | 33% → **100%** | 50% → **100%** | 2,080 ms → 541 ms |
| both local tiers down | 0% → **100%** | 0% → **100%** | 2 ms → 332 ms |
| cloud down (only the backup is lost) | 100% → **100%** | 100% → **100%** | 191 ms → 190 ms |
| edge link goes up/down every 1.5 s | 90% → **100%** | 89% → **100%** | 2,004 ms → 466 ms |

How to read it:
- Recovery rate (sub-queries hit by the failure that still got answered) is 100% in every
  failure scenario for SemanticRescue and 0% for the baseline (82% under flapping).
- When a tier **crashes**, the baseline is fast only because it fails at once (connection
  refused) and gives no answer. SemanticRescue pays the trip to the backup instead: +50 ms
  median for an edge crash (cloud is farther), about +0 ms for a fog crash (the edge's map
  cache is close).
- When a tier is **unreachable or slow**, the baseline waits out the 2 s timeout on every
  query. SemanticRescue pays that cost only for the first few queries, until the heartbeat or
  the circuit breaker removes the tier (see `results/fig_e1_timeline_edge_partition.png`).
  A slow tier still passes health checks; only hedging and the breaker catch it.
- Backup answers came from the cloud for personal parts and from the edge cache (map) or
  the cloud (scene) for environmental parts. The fog never received a personal sub-query.

**E2: sync window.** New personal value written on the edge, edge crashes 0 to 4 s later,
the value is then asked for (answered by the cloud). 30 trials per setting.

| Sync window | Stale answers (update lost) | Expected (window / 8) | Idle sync traffic |
|---|---|---|---|
| 0.5 s | 6.7% | 6.2% | 338 requests/min |
| 1 s | 13.3% | 12.5% | 158 requests/min |
| 2 s (default) | 26.7% | 25.0% | 68 requests/min |
| 4 s | 56.7% | 50.0% | 45 requests/min |
| 4 s + push on write | **0%** | 0% | 45 requests/min + 1 per write |

In all 150 trials the cloud caught up once the edge restarted with its disk. Since personal
facts change rarely, **push-on-write plus a slow periodic sync** gives the smallest loss for
the least traffic. Figures: `results/fig_*.png`.

Limits: one machine with emulated delays and template answers, so the latencies show the
mechanisms, not real Pi 5 numbers. The next step is the same experiments on the testbed with Ollama.

## Repository layout

```
semrescue/
  tier_service.py        edge / fog / cloud service (one program, --role picks the tier)
  store.py               versioned SQLite key-value store (last-writer-wins)
  sync.py                periodic delta sync with the cloud (the sync window)
  backend.py             answer generation: mock (repeatable) or Ollama (real model)
  cluster.py             starts/kills/partitions tiers locally for demos and experiments
  coordinator/
    router.py            Gold (annotated plans), Keyword (rules), SLM (Ollama hook) routers
    policy.py            backup chains: who replaces whom
    health.py            heartbeats + circuit breaker
    coordinator.py       failover, hedged requests, dependency-aware execution
    fusion.py            fault-aware fusion (complete / partial / failed + staleness warning)
    service.py           HTTP coordinator for the real testbed
  faults/proxy.py        TCP fault proxy: link delay, partition (black hole), reject
data/                    personal facts, floor map, 24 test queries with gold sub-query plans
scripts/
  demo.py                walk-through of every failure scenario, baseline vs SemanticRescue
  run_experiments.py     E1 (failure scenarios) and E2 (sync window sweep)
  plot_results.py        figures in results/
tests/                   unit tests + end-to-end tests with real processes and real faults
docs/DESIGN.md           design and evaluation plan
```

## Quick start (Windows PowerShell)

```powershell
git clone https://github.com/<your-account>/semantic-rescue.git
cd semantic-rescue
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

python -m pytest                     # 12 tests, about 15 s
python scripts/demo.py               # see every failure scenario live
python scripts/run_experiments.py    # about 15-20 min; writes results/*.csv
python scripts/plot_results.py       # writes results/*.png
```

On Linux/macOS use `source .venv/bin/activate`. Ports 8001-8003 and 9001-9003 must be free.

## Running on the real testbed (Pi 5 + laptop + cloud)

1. Copy `config/testbed.example.yaml` to `config/testbed.yaml` and put in the IP of each device.
2. On each device: `pip install -r requirements.txt`, pull the Ollama model, then start its tier:
   ```
   python -m semrescue.tier_service --role cloud --config config/testbed.yaml --host 0.0.0.0   # cloud VM
   python -m semrescue.tier_service --role edge  --config config/testbed.yaml --host 0.0.0.0   # Pi 5
   python -m semrescue.tier_service --role fog   --config config/testbed.yaml --host 0.0.0.0   # laptop
   ```
3. On the device that plays the glasses: `python -m semrescue.coordinator.service --config config/testbed.yaml`
   and ask: `curl -X POST localhost:8000/ask -H "content-type: application/json" -d '{"query": "Where is my gate?"}'`
4. Inject failures: stop a tier (crash), unplug its network or use `semrescue.faults.proxy`
   (partition), or `curl -X POST "<tier>/admin/slow?ms=3000"` (slow).

With the reverse-SSH tunnel to the Pi, point `tiers.edge.host/port` at the tunnel end.

## Using the trained SLM router

`coordinator/router.py` has an `SLMRouter` that asks an Ollama model for the sub-query plan
and falls back to the keyword router if the model is unreachable. Put the prompt or fine-tuned
model from the semantic routing project there and start the coordinator with `--router slm`.

## Team

Tapajit Chandra Paul, Aliyeh Haghollahi - HPC Eagles, Department of Computer Science and
Engineering, University of North Texas.

## Trello Board

https://trello.com/b/PzmFPhiB/hpceagles7-%F0%9F%9A%80
