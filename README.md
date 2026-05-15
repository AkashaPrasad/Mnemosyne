# Mnemosyne — Persistent Operational Memory for Autonomous SRE

> *Named after the Greek goddess of memory and the mother of the Muses.*

Mnemosyne is a **Persistent Context Engine** that gives autonomous SRE agents operational memory. It ingests a stream of telemetry events (deploys, logs, metrics, traces, topology changes, incidents, remediations), builds a temporal causal graph of service relationships, and reconstructs full investigation context for new incidents — including similar past incidents, causal chains, and suggested remediations — in under 2 seconds.

The key innovation: **behavioral fingerprinting**. Incidents are identified by *what structurally happened* (deploy to upstream → metric spike → cascade), not by which service names were involved. This means a rename from `payments-svc` to `billing-svc` is fully transparent — past incidents on `payments-svc` match new incidents on `billing-svc` without any special handling at query time.

---

## Quick Start

```bash
# 1. Clone this repo and the harness
git clone https://github.com/Sauhard74/Anvil-P-E ../Anvil-P-E
git clone <this-repo> anvil-p02-mnemosyne
cd anvil-p02-mnemosyne

# 2. Install dependencies
pip install -e ".[dev]"

# 3. Run unit tests
pytest tests/ -v

# 4. Run the harness self-check (requires harness repo)
cd ../Anvil-P-E/bench-p02-context
python self_check.py --adapter adapters.mnemosyne:Engine --quick

# 5. Run full benchmark and view report
python run.py --adapter adapters.mnemosyne:Engine --mode fast \
    --seeds 9999 31415 27182 16180 11235 --out report.json
```

---

## Architecture Overview

Mnemosyne uses a **three-layer memory architecture**:

1. **Episodic Store** (append-only) — every ingested event stored with full provenance, indexed by time window, canonical service ID, trace ID, and incident ID.
2. **Semantic Graph** — a temporal causal graph with service nodes (identified by stable canonical IDs), dependency edges extracted from trace spans, and causal edges inferred via temporal window correlation.
3. **Incident Memory** — closed incidents are stored as `BehavioralFingerprint` structs encoding *structural patterns* (deploy role, error cascade role sequence, anomalous metrics, resolution action) rather than service names.

The **TopologyTracker** maintains a bidirectional alias registry so that service renames are resolved at the `resolve()` call site. All internal operations use stable `canonical_id` values, never raw service names.

---

## Key Design Decisions

- **Behavioral fingerprinting beats string matching.** Similarity is computed on structural role sequences (upstream → gateway → backend) and metric patterns, not service name equality. This is the only approach that survives topology drift.

- **Temporal window correlation with calibrated confidence.** Causal edges are weighted by inverse time-distance: a deploy 2 minutes before an incident gets confidence ~0.93; one 14 minutes before gets ~0.15. Evidence lists contain actual event IDs that judges can verify.

- **Rename chains are processed synchronously.** When a `topology/rename` event arrives, the alias registry is updated immediately. All subsequent lookups via `resolve()` transparently return the same `canonical_id` for old and new names alike. There is no eventual consistency window.

---

## Benchmark Results

| Metric | Value |
|---|---|
| recall@5 | TBD |
| precision@5_mean | TBD |
| remediation_acc | TBD |
| latency_p95 (fast) | TBD |

*Fill in after running `scripts/run_benchmark.sh`.*

---

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| sentence-transformers | 2.7.0 | Semantic embeddings for log/remediation clustering |
| scikit-learn | 1.4.0 | Cosine similarity, normalization |
| numpy | 1.26.4 | Numerical operations |
| networkx | 3.3 | Graph traversal utilities |
| python-dotenv | 1.0.1 | Config management |
| anthropic | ≥0.25.0 | LLM explain in deep mode (optional, falls back gracefully) |
| pytest | 8.1.0 | Test runner |

All inference runs in a **single Python process** — no external databases, message queues, or network services required at evaluation time.

---

## License

MIT
