# SemanticRescue design

Fault-tolerant semantic routing for the smart-glasses system (edge, fog, cloud).

## 1. System assumptions

| Component | Device | Holds | Can do |
|---|---|---|---|
| Smart glasses (coordinator) | glasses / phone | nothing permanent | router, failure detection, failover, template fusion |
| Edge | Raspberry Pi 5 | **personal store** (bookings, schedule, allergies, sizes, memberships, device position) + a **cached copy of the floor map** | personal lookups, map lookups. No vision model. |
| Fog | laptop | **floor map** (rooms, gates, exits) + camera scene | map lookups, scene reading/description (vision). **Never personal data.** |
| Cloud | cloud VM | **backup replicas** of the personal store and the floor map | everything, but it is the farthest tier (WAN delay) |

The glasses are connected to all three tiers. The coordinator runs on the glasses side so
that routing and fusion still work when any one tier is down. (In the research system the
router and fusion run on the edge; if the edge is down, they would go down with it.)

## 2. Failure scenarios

From the glasses' point of view a tier can be in one of four states:

| # | Scenario | What really happened | What the glasses see | How it is detected |
|---|---|---|---|---|
| F1 | **Tier not available** (crash) | process or device is down | connection refused, immediately | first request fails fast; heartbeat marks it DOWN after 2 misses (~1 s) |
| F2 | **Tier available but unreachable** (partition) | tier is running, the link is broken (Wi-Fi drop, reverse-SSH tunnel down) | requests hang until the timeout | heartbeat timeouts (DOWN after ~1 s); requests in flight hit the timeout |
| F3 | **Tier available but slow** (gray failure) | overloaded or thermally throttled Pi | `/health` still answers, real requests are slow | heartbeat does NOT catch it; hedging + circuit breaker do |
| F4 | **Flapping** | link goes up and down | mix of F2 and success | breaker opens after 3 failures, re-tries after 5 s |

Each of F1 to F4 can happen to the edge or to the fog, and also to both at once. A cloud failure
does not hurt answers (edge and fog keep working); it only stops the backups from being refreshed.

**Why F1 and F2 must be told apart:** in F1 the error comes instantly, so failing over costs
nothing. In F2 nothing comes back, so without detection *every* query waits the full timeout.
That is the main latency difference in the results. They also differ after recovery: after a
partition the edge still has all its data (and may have newer writes than the cloud); after a
crash with disk loss it must rebuild itself from the cloud.

## 3. Which tier replaces which (the backup policy)

The rule: **first, who holds the data; second, who is closest.**

| Sub-query | Primary | Backups, in order | Reason |
|---|---|---|---|
| Personal fact ("what is my seat") | Edge | **Cloud** | Only edge and cloud hold personal data. |
| Device position ("where am I") | Edge | Cloud | Cloud holds the last synced position, flagged with its age. |
| Map / directions ("where is the restroom") | Fog | **Edge**, then Cloud | The map is not private, so the edge keeps a cached copy: closest backup. |
| Scene ("what does this sign say") | Fog | **Cloud** | Needs a vision model; the Pi edge has none. |

### "If the edge fails, is the fog the better option because it is closer?"

**For personal sub-queries, no.** The fog is closer, but it does not have the personal data,
and by the design of the routing project it must never get it (personal answers are never sent
to the fog). A fast tier without the data cannot answer, so the backup for personal parts is the
cloud, which holds a synced backup of the edge store. The cost is extra delay (WAN round trip,
about 150 ms more in our setup), and the answer can be slightly out of date (see section 4).

It is different the other way round: **if the fog fails, the edge is the best backup for map
questions**, because map data is not private and the edge is the closest tier. For scene
questions the edge cannot help (no vision), so they go to the cloud.

The only way to make the fog a personal backup would be to copy personal data onto the fog,
which breaks the privacy rule of the routing project. We keep that rule and enforce it in
code: the fog returns HTTP 403 for any personal request, and `policy.py` asserts that the fog
is never in a personal backup chain.

### Mixed queries

Each sub-query fails over on its own. "Where is my gate?" is two steps: `personal_lookup(gate)`
(edge) and then `map_lookup(that gate)` (fog). With the edge down, step 1 goes to the cloud and
step 2 still goes to the fog. Independent parts run in parallel; a dependent part waits for its
input and is skipped (and explained) if its input failed.

### When nothing can answer a part

Fault-aware fusion keeps every part that worked and says plainly what is missing, e.g.
*"The text says: AA 1432 ON TIME. I can't reach your personal information right now."*
Answers are labelled `complete`, `partial` or `failed`.

## 4. Keeping the cloud up to date (sync)

Yes, a time window is needed. Every `sync.interval_s` seconds (the **sync window**, default 2 s)
the edge and the fog each run one delta-sync round with the cloud:

1. **Push** the records changed locally since the last push.
2. **Pull** the records changed in the cloud since the last pull.

| Data | Direction | Notes |
|---|---|---|
| Personal store | edge -> cloud, and cloud -> edge | cloud -> edge carries writes made while the edge was down |
| Floor map | cloud <-> fog, cloud -> edge (cache) | map updates (e.g. "elevator out of service") reach every tier |
| Personal store -> fog | **never** | blocked on the cloud side too (HTTP 403) |

Conflicts are resolved with *last-writer-wins* on `(updated_at, origin)`. Every record keeps its
timestamp, and each node keeps sync cursors in its own database.

**What the window costs and buys.** If the edge crashes, any personal update made in the last
window has not reached the cloud yet, so the cloud may answer with the old value. A shorter
window loses less but sends more sync traffic. `push_on_write: true` pushes right after every
write (almost no loss, one extra request per write). Experiment E2 measures this trade-off.
Cloud answers carry `staleness_s` (time since the edge last synced), and fusion warns the user
when it is above `stale_warn_s`.

**Recovery.**
- Edge back after a partition or a crash with its disk intact: the next sync round pushes the
  updates the cloud missed and pulls the writes made while it was away.
- Edge back after losing its disk: it starts empty (`--no-seed`) and pulls its full personal
  store and the map back from the cloud.
- Writes while the edge is down go to the cloud and reach the edge when it comes back.

## 5. Mechanisms (what the code does)

| Mechanism | Where | Default |
|---|---|---|
| Heartbeat to every tier | `coordinator/health.py` | every 0.5 s, DOWN after 2 misses |
| Per-request timeout | `coordinator/coordinator.py` | 2 s |
| Per-sub-query deadline (all backups) | same | 4 s |
| Failover to the next tier on error or timeout | same | from `policy.py` |
| Hedged request (slow tier: start the backup in parallel, take the first answer) | same | after 4x the tier's average latency, clamped to 0.25 to 1 s |
| Circuit breaker per tier | `coordinator/health.py` | opens after 3 failures, trial request after 5 s |
| Fault-aware fusion | `coordinator/fusion.py` | template, runs on the glasses |
| Delta sync with LWW | `sync.py`, `tier_service.py` | every 2 s |

The **baseline** is the same pipeline without fault tolerance: primary tier only, same
timeout, no heartbeat, no hedging, no backups.

## 6. Evaluation

Metrics from the proposal: **recovery rate** (sub-queries hit by the failure that still got
answered), **response latency** (p50/p95), **completeness and correctness** (complete / partial /
failed answers; sub-answers whose value matches the ground truth).

- **E1** - 10 scenarios (normal; edge crash / partition / slow; fog crash / partition / slow;
  edge+fog down; cloud down; edge flapping) x {baseline, SemanticRescue}, 24 queries
  (8 personal, 8 environmental, 8 mixed) x 2 passes each. Faults start right before the first
  query, so detection delay is included.
- **E2** - sync window sweep {0.5, 1, 2, 4 s, 4 s + push-on-write}: write a new value on the
  edge, wait a random 0 to 4 s, crash the edge, ask for the value. Stale answer = lost update.
  Also measures sync requests per minute, and checks that the cloud catches up when the edge
  restarts.

The experiments use the gold sub-query plans in `data/queries.json` so the results measure
fault tolerance, not routing errors, and the mock backend so the results are repeatable.
Latencies are emulated (link delay in the proxy, compute time in the tier). Real numbers
need the testbed run with Ollama (see the README).

## 7. Limitations and next steps

- Clocks: last-writer-wins assumes the device clocks are roughly in sync (NTP). Hybrid logical
  clocks would remove that assumption.
- The cloud backup holds personal data in plain text. Encrypting it at rest (key on the edge and
  glasses) is the obvious next step.
- Position from the cloud is only the last synced position; for navigation it can be wrong
  after a few seconds. It is labelled with its age.
- Measured on one machine with emulated delays; the Pi 5 / laptop / cloud testbed is next.
- The trained SLM router plugs in through `SLMRouter`; the experiments do not use it.
