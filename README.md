<div align="center">

<h1>🧠 Mnemosyne</h1>

<p><strong>Persistent operational memory for autonomous SRE agents, with context reconstruction in under 2 seconds and service-rename transparent matching.</strong></p>

[![Python](https://img.shields.io/badge/python-3.10%2B-blue?style=flat-square&logo=python)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green?style=flat-square)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-pytest-orange?style=flat-square&logo=pytest)](tests/)
[![Last Commit](https://img.shields.io/github/last-commit/AkashaPrasad/Mnemosyne?style=flat-square)](https://github.com/AkashaPrasad/Mnemosyne/commits)
[![No External DB](https://img.shields.io/badge/dependencies-zero%20external%20infra-purple?style=flat-square)](#)

<br/>

Gives every autonomous SRE agent three answers on every new incident: what caused it, whether it has happened before, and what resolved it last time. Behavioral fingerprints encode structural patterns rather than service names, so institutional memory survives topology drift and arbitrary service renames with no migrations and no reindexing.

</div>

---
██████████████████████████████████████████████████████████████████████
██████████████████████████████████████████████████████████████████████
★★★     A N V I L   ·   P - 0 2   ·   L 3   F I N A L   B E N C H     ★★★
★★★     Council Release · anvil-2026-p02-L3-final              ★★★
★★★     2026-05-16 11:21:51 +0530                                 ★★★
██████████████████████████████████████████████████████████████████████
██████████████████████████████████████████████████████████████████████
  ▸ L3 generator: 30 services · 21 days · 80 topology mutations · 60+25 incidents · 8 families
  ▸ Cascading renames: ON · decoy rate: 20%
  ▸ Seeds: [314159, 271828, 161803, 141421, 173205]
  ▸ Mode: fast

  ▸ Adapter source:    /Users/akashaaprasad/Documents/Anvil/Anvil-P-E/bench-p02-context/adapters/mnemosyne.py
  ▸ Adapter SHA-256:   ca3e38ee623f3070…

  ▸ Running L3 evaluation across all seeds …

██████████████████████████████████████████████████████████████████████
██████████████████████████████████████████████████████████████████████
★★★     A N V I L   ·   P - 0 2   ·   L 3   F I N A L   S C O R E     ★★★
★★★     0.5441  /  0.8000    ( 68.0 %)         ★★★
★★★     anvil-2026-p02-L3-final                                   ★★★
██████████████████████████████████████████████████████████████████████
██████████████████████████████████████████████████████████████████████

## The Problem

When a P0 fires at 3 AM, your on-call engineer needs to know three things **immediately**:

1. **What triggered this?** (the causal chain)
2. **Have we seen this before?** (similar past incidents)
3. **What fixed it last time?** (suggested remediations)

Current tools fail here. They store incidents by **service name**. Rename `payments-svc` to `billing-svc` and every historical match disappears. Topology drift silently erases institutional memory.

Mnemosyne solves this by identifying incidents by **what structurally happened**, not which services were involved. A deploy to a `backend`-role service followed by a `p95_ms` spike and an upstream cascade is the same incident pattern regardless of what that service was called last quarter.

---

## How It Works

```
Telemetry stream (deploys · logs · metrics · traces · topology · remediations)
        │
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│                         Mnemosyne Engine                              │
│                                                                       │
│  ┌──────────────────┐     ┌───────────────────────────────────────┐  │
│  │ TopologyTracker  │────▶│            Episodic Store             │  │
│  │                  │     │  append-only · 5-axis indexed         │  │
│  │  name ──────────▶│     │  (time · service · trace ·            │  │
│  │  canonical_id    │     │   incident · kind)                    │  │
│  │                  │     └──────────────────┬────────────────────┘  │
│  │  rename chains   │                        │                       │
│  │  resolve to the  │     ┌──────────────────▼────────────────────┐  │
│  │  same UUID       │────▶│       Temporal Causal Graph           │  │
│  └──────────────────┘     │  DEPENDS_ON · CAUSED_BY · RESOLVED_BY │  │
│                            │  confidence · valid_from · valid_to   │  │
│                            └────────────┬──────────────────────────┘  │
│                                         │                             │
│                        ┌────────────────┴──────────────┐             │
│                        ▼                               ▼             │
│             ┌──────────────────┐          ┌─────────────────────┐    │
│             │  CausalLinker    │          │   IncidentMemory    │    │
│             │  4-phase chain   │          │  BehavioralFingerpr.│    │
│             │  inference with  │          │  structural patterns│    │
│             │  calibrated conf │          │  no service names  │    │
│             └────────┬─────────┘          └──────────┬──────────┘    │
│                      │                               │               │
│                      └───────────────┬───────────────┘               │
│                                      ▼                               │
│                           ┌──────────────────┐                       │
│                           │ ContextCompiler  │                       │
│                           │  fast  ≤ 2s      │                       │
│                           │  deep  ≤ 6s+LLM  │                       │
│                           └──────────────────┘                       │
└───────────────────────────────────────────────────────────────────────┘
        │
        ▼
  Context {
    related_events          // causally-relevant telemetry
    causal_chain            // deploy → metric_spike → cascade
    similar_past_incidents  // rename-transparent matches
    suggested_remediations  // ranked by historical success
    confidence              // calibrated [0, 1]
    explain                 // human-readable narrative
  }
```

---

## Key Features

| Feature | What it means in practice |
|---|---|
| **Behavioral Fingerprinting** | Incidents matched by structural pattern (role sequence + metric signature), not service names. Rename-transparent by design. |
| **Temporal Causal Graph** | Dependency edges extracted from trace spans, causal edges inferred via 4-phase temporal correlation with calibrated confidence decay. |
| **Sub-2s Context Reconstruction** | Pre-computed fingerprints + O(1) index lookups. No full scans, no external round-trips. |
| **Rename Chain Resolution** | `payments-svc → billing-svc → checkout-svc` all resolve to the same `canonical_id`. Historical incidents are immediately queryable under any name. |
| **Calibrated Confidence** | Exponential time-decay weighting: a deploy 2 min before gets ~0.93, 14 min before gets ~0.15. Evidence IDs are verifiable. |
| **Zero External Infrastructure** | Single Python process. No Redis, no Postgres, no Kafka. Works offline after model download. |
| **Optional LLM Narration** | `mode="deep"` calls Claude Haiku for a 3–4 sentence incident summary. Falls back to template if unavailable. |

---

## Quick Start

> Get from zero to a working context reconstruction in under 2 minutes.

### 1. Clone & install

```bash
git clone https://github.com/AkashaPrasad/Mnemosyne
cd mnemosyne
pip install -e ".[dev]"
```

### 2. Run the test suite

```bash
pytest tests/ -v
```

### 3. Try it in Python

```python
from adapters.mnemosyne import Engine

engine = Engine()

# Ingest a stream of telemetry events
engine.ingest([
    {"kind": "trace", "id": "t1", "service": "frontend", "ts": "2024-01-01T11:00:00Z",
     "spans": [{"caller": "frontend", "callee": "payments-svc"},
               {"caller": "payments-svc", "callee": "postgres-db"}]},
    {"kind": "deploy", "id": "d1", "service": "payments-svc", "ts": "2024-01-01T11:50:00Z",
     "version": "v2.14.0", "status": "success"},
    {"kind": "metric", "service": "payments-svc", "ts": "2024-01-01T11:55:00Z",
     "metric": "latency_ms", "value": 920.0},
    {"kind": "incident_signal", "id": "inc-001", "service": "payments-svc",
     "ts": "2024-01-01T12:00:00Z", "alert": "P0: high latency", "severity": "P0"},
    {"kind": "remediation", "incident_id": "inc-001", "action": "rollback",
     "target": "payments-svc", "ts": "2024-01-01T12:15:00Z", "outcome": "resolved"},
    # Rename the service
    {"kind": "topology", "change": "rename", "old_name": "payments-svc",
     "new_name": "billing-svc", "ts": "2024-01-01T12:30:00Z"},
])

# New incident fires on the renamed service; history is fully preserved
ctx = engine.reconstruct_context({
    "id": "inc-002",
    "service": "billing-svc",
    "ts": "2024-01-01T13:10:00Z",
    "trigger": "high latency on billing-svc",
    "severity": "P0",
})

print(ctx["explain"])
# → "Incident inc-002 on billing-svc (formerly payments-svc). Root cause: deploy
#    v2.14.0 preceded latency spike. 1 similar past incident: inc-001 (similarity 87%).
#    Recommended: rollback billing-svc (confidence: 82%)."

print(ctx["similar_past_incidents"])
# → [{"incident_id": "inc-001", "similarity": 0.87, "rationale": "..."}]
```

---

## Installation

### Prerequisites

- Python 3.10 or higher
- `pip`

### Standard install

```bash
pip install -e .
```

### Development install (includes test runner + formatter)

```bash
pip install -e ".[dev]"
```

### Docker

```bash
# Run tests inside the container
docker compose run mnemosyne

# Run the benchmark harness
docker compose run bench
```

The Docker image pre-downloads the `all-MiniLM-L6-v2` sentence-transformer model so it works fully offline after build.

---

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | *(unset)* | Required only for `mode="deep"`. Falls back to template explain if unset. |
| `PYTHONPATH` | `.` | Should include the repo root so `engine.*` imports resolve. |

No other configuration is required. There are no database connection strings, no message queue URLs, no service discovery endpoints.

---

## API Reference

### `Engine`

```python
from adapters.mnemosyne import Engine
engine = Engine()
```

#### `engine.ingest(events: Iterable[Event]) -> None`

Processes a stream of telemetry events. Topology rename events are processed before all others to prevent canonical ID splits. All subsequent lookups are rename-transparent.

**Event kinds:** `deploy` · `log` · `metric` · `trace` · `topology` · `incident_signal` · `remediation`

#### `engine.reconstruct_context(signal: IncidentSignal, mode="fast") -> Context`

Reconstructs full investigation context for an incident signal.

| Mode | Latency (p95) | Explain source |
|---|---|---|
| `"fast"` | ≤ 2s | Template narrative |
| `"deep"` | ≤ 6s | Anthropic Claude Haiku |

**Returns** a `Context` TypedDict:

```python
{
    "related_events":          List[Event],         # causally-relevant telemetry
    "causal_chain":            List[CausalEdge],    # ordered cause → effect edges
    "similar_past_incidents":  List[IncidentMatch], # top-5 fingerprint matches
    "suggested_remediations":  List[Remediation],   # ranked by historical success
    "confidence":              float,               # overall confidence [0, 1]
    "explain":                 str,                 # human-readable summary
}
```

#### `engine.close() -> None`

Releases resources. No-op for the pure in-memory implementation.

---

## Architecture Deep Dive

### Layer 1: Episodic Store (`engine/ingestion.py`)

Append-only store indexed across five axes: **time bucket** (minute-level), **canonical service ID**, **trace ID**, **incident ID**, and **event kind**. Time-bucket indexing makes the common query (*"what happened in the 15 minutes before this incident?"*) resolve in O(1) index lookups rather than a full scan.

A `MetricSeries` per `(canonical_id, metric_name)` pair maintains a rolling window of 60 values and computes online mean/stdev for anomaly flagging at ingest time. Anomaly detection has zero query-time overhead.

### Layer 2: Temporal Causal Graph (`engine/memory_substrate.py`)

Typed adjacency dict with `DEPENDS_ON`, `CAUSED_BY`, `RESOLVED_BY`, and `CO_OCCURS_WITH` edges. Every edge carries `confidence`, an `evidence` list of verifiable event IDs, `valid_from`, and `valid_to` for point-in-time queries.

`topology_role()` classifies services by structural position (`upstream`, `gateway`, `backend`, `leaf`, `mid`, or `isolated`). This role annotation is the foundation of topology-independent fingerprinting.

### Layer 3: Incident Memory (`engine/incident_matcher.py`)

When a remediation closes an incident, a `BehavioralFingerprint` is extracted and stored. The fingerprint encodes **what structurally happened**, not which services were involved:

```
BehavioralFingerprint {
  had_pre_deploy, deploy_role, deploy_minutes_before   # deploy pattern
  error_role_sequence                                   # ["upstream", "gateway", "backend"]
  anomalous_metrics                                     # ["latency_ms", "error_rate"]
  resolution_role, resolution_action                    # structural fix description
}
```

Service names appear nowhere. All lookups use canonical IDs and topology roles. Two incidents match if they followed the same structural pattern, regardless of what those services were called.

### Causal Chain Inference: 4 Phases

```
Phase 1  Deploy → Incident         confidence = 0.95 × exp(−2.5 × Δt/900s)
Phase 2  Metric anomaly → Incident confidence = Phase1 × 0.80 + z-score boost
Phase 3  Upstream error → Incident confidence = Phase1 × 0.65
Phase 4  Deploy → Metric spike     Granger-precedence chaining (multi-hop)
```

Edges are deduplicated, keeping the highest-confidence pair per `(cause_id, effect_id)`.

---

## Project Structure

```
mnemosyne/
├── adapters/
│   └── mnemosyne.py          # Public Engine class, harness entry point
├── engine/
│   ├── schema.py             # TypedDicts: Event, Context, CausalEdge, ...
│   ├── topology_tracker.py   # Bidirectional alias registry, rename chains
│   ├── ingestion.py          # EpisodicStore + MetricSeries
│   ├── memory_substrate.py   # TemporalCausalGraph
│   ├── causal_linker.py      # 4-phase causal chain inference
│   ├── incident_matcher.py   # BehavioralFingerprint + IncidentMemory
│   ├── remediation_ranker.py # Historical success-rate ranking
│   ├── context_compiler.py   # Orchestrates reconstruct_context()
│   └── embeddings.py         # sentence-transformers wrapper + BOW fallback
├── tests/
│   ├── test_e2e.py           # End-to-end rename-transparent scenario
│   ├── test_causal_linker.py
│   ├── test_incident_matcher.py
│   ├── test_ingestion.py
│   └── test_topology_tracker.py
├── writeup/
│   └── architecture.md       # Detailed design defense
├── bench/
│   └── run.sh                # Benchmark runner script
├── scripts/
│   └── run_benchmark.sh
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── setup.py
```

---

## Running the Benchmark

Mnemosyne is built against the `bench-p02-context` harness from [Anvil-P-E](https://github.com/Sauhard74/Anvil-P-E).

```bash
# Clone the harness alongside this repo
git clone https://github.com/Sauhard74/Anvil-P-E ../Anvil-P-E
cd ../Anvil-P-E/bench-p02-context

# Quick self-check (~30 seconds)
python self_check.py --adapter adapters.mnemosyne:Engine --quick

# Full benchmark across 5 random seeds
python run.py --adapter adapters.mnemosyne:Engine --mode fast \
    --seeds 9999 31415 27182 16180 11235 --out report.json
```

Or use Docker Compose:

```bash
docker compose run bench
```

---

## Dependencies

| Package | Version | Role |
|---|---|---|
| `sentence-transformers` | 2.7.0 | Semantic embeddings via `all-MiniLM-L6-v2` |
| `scikit-learn` | 1.4.0 | Cosine similarity, normalization |
| `numpy` | 1.26.4 | Numerical operations |
| `networkx` | 3.3 | Graph traversal utilities |
| `python-dotenv` | 1.0.1 | Environment config |
| `anthropic` | ≥0.25.0 | LLM explain in `deep` mode (optional) |
| `pytest` | 8.1.0 | Test runner |

---

## Testing

```bash
# All tests with verbose output
pytest tests/ -v

# Single test file
pytest tests/test_e2e.py -v

# With coverage report
pytest tests/ --cov=engine --cov-report=term-missing
```

The end-to-end test in `tests/test_e2e.py` covers the canonical rename scenario: ingest normal telemetry → deploy → incident → remediation → rename → new incident → verify historical match survives the rename.

---

## Troubleshooting

**`ModuleNotFoundError: No module named 'engine'`**
→ Set `PYTHONPATH` to the repo root: `PYTHONPATH=. pytest tests/ -v`

**Sentence-transformer model download hangs**
→ Pre-download in the container: `docker compose build`. The Dockerfile fetches the model at build time.

**`reconstruct_context` returns empty `similar_past_incidents`**
→ Fingerprints are only stored after a `remediation` event closes an incident. Ensure your event stream includes a `remediation` event with a matching `incident_id` before querying.

**`mode="deep"` returns a template explain**
→ Set `ANTHROPIC_API_KEY` in your environment. Deep mode falls back gracefully to the template if the API is unreachable.

---

## Contributing

Contributions are welcome. To get started:

```bash
git clone https://github.com/AkashaPrasad/Mnemosyne
cd mnemosyne
pip install -e ".[dev]"
pytest tests/ -v          # make sure everything passes first
```

Before opening a pull request:
- Run `black engine/ adapters/ tests/` for formatting
- Add or update tests for any behavior changes
- Keep new engine modules under `engine/` and register them in `Engine.__init__`

Open an issue first for large changes to avoid duplicate effort.

---

## Roadmap

- [ ] Persistent storage backend (SQLite / DuckDB) for multi-session memory
- [ ] REST/gRPC adapter for integration with external SRE platforms
- [ ] Streaming ingestion via async generator protocol
- [ ] Multi-tenant isolation for shared SRE platforms
- [ ] Benchmark result dashboard

---

## License

MIT. See [LICENSE](LICENSE).

---

## Acknowledgements

- Benchmark harness: [Anvil-P-E](https://github.com/Sauhard74/Anvil-P-E) (`bench-p02-context`)
- Embeddings: [sentence-transformers](https://www.sbert.net/) / `all-MiniLM-L6-v2`
- LLM narration: [Anthropic Claude](https://www.anthropic.com/) Haiku

---

<div align="center">

If Mnemosyne saved your on-call rotation, consider giving it a ⭐

[Report a bug](https://github.com/AkashaPrasad/Mnemosyne/issues/new?template=bug_report.md) · [Request a feature](https://github.com/AkashaPrasad/Mnemosyne/issues/new?template=feature_request.md) · [Read the architecture writeup](writeup/architecture.md)

</div>
