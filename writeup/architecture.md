# Mnemosyne — Architectural Defense

## Section 1: Memory Representation

### Three-Layer Architecture

Mnemosyne stores operational knowledge across three distinct layers, each optimized for a different access pattern.

**Layer 1: Episodic Store** (`engine/ingestion.py`)

The episodic store is append-only and stores every ingested event with full provenance. Events are indexed by five axes: time bucket (minute-level), canonical service ID, trace ID, incident ID, and event kind. Time-bucket indexing means the common query — "what happened in the 15 minutes before this incident?" — resolves in O(1) index lookups rather than a full scan. The canonical service ID index is the critical one: it is populated by routing every service name through `TopologyTracker.resolve()` at ingestion time, so all future queries are rename-transparent by construction.

A `MetricSeries` per `(canonical_id, metric_name)` pair maintains a rolling window of the last 60 values and computes online mean/stdev for anomaly flagging. This means anomaly detection has zero query-time overhead — the `_anomalous` flag and z-score are computed during `ingest()` and stored on the event.

**Layer 2: Semantic Graph** (`engine/memory_substrate.py`)

The semantic graph is a typed adjacency dict. Nodes represent `ServiceEntity`, `IncidentEntity`, and `DeployEntity`. Edges carry `rel` (DEPENDS_ON, CAUSED_BY, RESOLVED_BY, CO_OCCURS_WITH), `confidence`, an `evidence` list of event IDs, `valid_from`, and `valid_to`. The valid-time fields enable point-in-time queries ("what was the dependency topology before the rename?"). DEPENDS_ON edges are upserted (not duplicated) — repeated trace spans for the same caller→callee pair increment confidence rather than creating duplicate edges.

The graph provides a `topology_role()` method that classifies services by structural position: `upstream` (no inbound calls), `gateway` (many inbound, few outbound), `backend`, `leaf`, `mid`, or `isolated`. This role annotation is the foundation of topology-independent fingerprinting.

**Layer 3: Incident Memory** (`engine/incident_matcher.py`)

When a remediation event closes an incident, a `BehavioralFingerprint` is extracted and stored. The fingerprint encodes *what structurally happened*, not which services were involved:

- `had_pre_deploy`, `deploy_role`, `deploy_minutes_before` — deploy pattern
- `error_role_sequence` — ordered list of topology roles that emitted errors (e.g. `["upstream", "gateway", "backend"]`)
- `anomalous_metrics` — sorted list of metric names that were flagged as anomalous
- `resolution_role`, `resolution_action` — structural description of the fix

Service names appear nowhere in the fingerprint. All lookups during similarity computation go through canonical IDs and topology roles.

### Why Behavioral Fingerprints Beat String Matching

A naive implementation would store `{"service": "payments-svc", "prior_incidents": [...]}` and match future incidents by service name equality. This breaks completely when `payments-svc` is renamed to `billing-svc` — the matching index has no entry for the new name.

Behavioral fingerprinting sidesteps the problem entirely: two incidents are similar if they followed the same structural pattern (deploy to a `backend`-role service → latency spike in `p95_ms` → cascade to `leaf` databases), regardless of what those services were called. The rename changes names, not structure.

### How Canonical IDs Survive Topology Drift

Every service in the system is assigned a UUID `canonical_id` at first contact. The `TopologyTracker` maintains a bijection between names and canonical IDs. When a rename arrives, the old name entry is updated to point to the same canonical ID as the new name — both names resolve to the same UUID. Rename chains (`A → B → C`) preserve the same canonical ID throughout: when `B → C` arrives, `B` already resolves to the same UUID as `A`, so `C` is registered to that same UUID. All historical episodes and fingerprints stored under that UUID are immediately queryable under any of the three names.

---

## Section 2: Relationship Synthesis Algorithm

### Dependency Edge Extraction from Trace Spans

Trace events contain a `spans` list where each span has a `caller` and `callee` field. The `CausalLinker.extract_dependency_edges_from_traces()` method iterates over spans and calls `graph.upsert_depends_on(caller_cid, callee_cid, ...)`. The `upsert` semantics are important: if a DEPENDS_ON edge for this caller→callee pair already exists and is still active (`valid_to is None`), the existing edge's confidence is incremented by 0.05 and the trace ID is appended to the evidence list. This means highly-trafficked dependencies accumulate confidence quickly while infrequent or transient edges remain at lower confidence, creating a natural reliability signal.

### Causal Edge Inference (Temporal Window Correlation)

The causal chain for an incident is built in four phases, each operating on a 15-minute lookback window:

**Phase 1 (Deploy → Incident):** Find all deploys to services within 2 dependency hops of the triggering service in the time window. Each deploy event becomes a potential root cause. Confidence is computed via inverse exponential decay: `0.95 * exp(-2.5 * (delta_seconds / 900))`, giving ~0.93 for a 2-minute gap and ~0.11 for a 14-minute gap. If the deploy targeted the exact triggering service (not just a dependency), confidence is boosted by 0.15.

**Phase 2 (Metric Anomaly → Incident):** Find metric events on the triggering service flagged as anomalous (z-score > 2.0 at ingest time). Each becomes a potential proximate cause. Base confidence is scaled to 0.80× of the deploy formula, then boosted by up to 0.15 proportional to z-score magnitude.

**Phase 3 (Upstream Error Cascade → Incident):** Find error-level logs from services that depend on the triggering service (reverse 2-hop traversal). These represent upstream callers that degraded before the incident signal. Confidence is scaled to 0.65×.

**Phase 4 (Deploy → Metric Spike chaining):** For each deploy event and anomalous metric event where `deploy_ts < metric_ts < incident_ts`, a Granger-precedence edge is added between the deploy and the metric event. This creates multi-hop causal chains (deploy → metric_spike → incident) rather than flat star topologies.

Deduplication removes duplicate (cause_id, effect_id) pairs, keeping the highest-confidence edge. The result is sorted by confidence descending.

### Confidence Calibration

Confidence is never fixed at 0.99 for all edges. The calibration principles are:

1. **Time-distance weighting**: earlier causes have exponentially lower confidence. The formula uses `exp(-2.5 * ratio)` where `ratio = delta_seconds / window_seconds`, giving a natural half-life around 4 minutes.
2. **Evidence-count boosting**: repeated observations of the same causal relationship (via trace span accumulation) increment confidence incrementally.
3. **Structural role boosting**: a deploy to the exact triggering service gets a +0.15 boost vs. a deploy to a downstream dependency.
4. **Dimension scaling**: metric anomalies (proximate) and upstream errors (circumstantial) are scaled to 0.80× and 0.65× of the deploy confidence respectively, reflecting their lower causal priority.

---

## Section 3: Drift-Handling Strategy

### The Alias Registry Design

`TopologyTracker` maintains three data structures:

- `_name_to_canonical: Dict[str, str]` — fast current lookup: O(1) name → canonical_id
- `_canonical_to_names: Dict[str, Set[str]]` — all historical names for a canonical_id
- `_rename_history: Dict[str, List[tuple]]` — ordered `(name, valid_from, valid_to)` per canonical_id

The canonical_id is a UUID assigned at first registration and never changes. The `_rename_history` provides temporal validity windows so point-in-time name lookups are possible: `historical_names(canonical_id, before_ts)` returns all names that were active before a given timestamp.

### Forward and Backward Lookup

**Forward (name → canonical_id):** `topology.resolve(name)` returns the canonical_id in O(1). If the name is unknown, a new service entity is created on-the-fly — the engine never crashes on an unseen service name.

**Backward (canonical_id → name):** `topology.current_name(canonical_id)` returns the most recent active name by scanning `_rename_history` in reverse. `topology.aliases(canonical_id)` returns all names ever used. The `explain` field in `reconstruct_context` uses `current_name()` to display the up-to-date name, and `aliases()` to add a "(formerly payments-svc)" annotation when a rename has occurred.

### Why Rename Events Are Processed Synchronously

The `topology` kind event is the only event type that bypasses the episodic store and is processed immediately (before `store.add()` is called). This is intentional: if a rename arrived asynchronously (after subsequent events for the new service name), those events would have been indexed under a different canonical_id, causing a split in the incident history.

By processing rename events synchronously in `_handle_topology()`, we guarantee that from the moment the rename is registered, all subsequent calls to `topology.resolve(old_name)` and `topology.resolve(new_name)` return the same canonical_id. There is no eventual consistency window — the alias registry is immediately consistent.

This also correctly handles the case where the rename event arrives *after* an incident has already occurred on the old name. The incident fingerprint was stored under the canonical_id (not the name), so the fingerprint is immediately queryable under the new name without any migration step.
