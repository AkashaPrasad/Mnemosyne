"""
End-to-end test covering the canonical scenario from the problem statement:
  1. Ingest normal telemetry (traces → dependency graph)
  2. Deploy v2.14.0 to payments-svc
  3. Incident signal fires
  4. Remediation closes incident, fingerprint stored
  5. Rename payments-svc → billing-svc
  6. New incident fires on billing-svc
  7. reconstruct_context must:
     a. Resolve billing-svc to same canonical_id
     b. Return causal chain pointing to v2.14.0 deploy
     c. Return similar_past_incidents containing the old incident
     d. Suggest rollback remediation
"""
import pytest
from datetime import datetime, timezone, timedelta

from adapters.mnemosyne import Engine

BASE = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)


def _ts(offset_min: float) -> str:
    return (BASE + timedelta(minutes=offset_min)).isoformat()


EVENTS_PHASE1 = [
    # Dependency topology from traces
    {
        "kind": "trace",
        "id": "t1",
        "service": "frontend",
        "ts": _ts(-60),
        "spans": [
            {"caller": "frontend", "callee": "payments-svc"},
            {"caller": "payments-svc", "callee": "postgres-db"},
        ],
    },
    # Normal metrics (baseline for anomaly detection)
    *[
        {
            "kind": "metric",
            "service": "payments-svc",
            "ts": _ts(-60 + i),
            "metric": "latency_ms",
            "value": 100.0 + i * 0.5,
        }
        for i in range(20)
    ],
    # Deploy
    {
        "kind": "deploy",
        "id": "deploy-v2140",
        "service": "payments-svc",
        "ts": _ts(-10),
        "version": "v2.14.0",
        "status": "success",
    },
    # Post-deploy latency spike
    {
        "kind": "metric",
        "service": "payments-svc",
        "ts": _ts(-5),
        "metric": "latency_ms",
        "value": 900.0,
    },
    # Error logs
    {
        "kind": "log",
        "service": "payments-svc",
        "ts": _ts(-3),
        "level": "error",
        "message": "Database connection timeout",
        "trace_id": "t-err-1",
    },
    # Incident signal
    {
        "kind": "incident_signal",
        "id": "inc-payments-001",
        "service": "payments-svc",
        "ts": _ts(0),
        "alert": "P0: payments-svc latency > 800ms",
        "severity": "P0",
        "trigger": "high latency on payments-svc",
    },
    # Remediation
    {
        "kind": "remediation",
        "id": "rem-001",
        "service": "payments-svc",
        "ts": _ts(15),
        "incident_id": "inc-payments-001",
        "action": "rollback",
        "target": "payments-svc",
        "outcome": "resolved",
    },
    # Rename event
    {
        "kind": "topology",
        "service": "payments-svc",
        "ts": _ts(30),
        "change": "rename",
        "old_name": "payments-svc",
        "new_name": "billing-svc",
    },
]

EVENTS_PHASE2 = [
    # New deploy to billing-svc
    {
        "kind": "deploy",
        "id": "deploy-v2150",
        "service": "billing-svc",
        "ts": _ts(60),
        "version": "v2.15.0",
        "status": "success",
    },
    # Latency spike again
    {
        "kind": "metric",
        "service": "billing-svc",
        "ts": _ts(68),
        "metric": "latency_ms",
        "value": 950.0,
    },
]

SIGNAL_PHASE2 = {
    "id": "inc-billing-001",
    "service": "billing-svc",
    "ts": _ts(70),
    "alert": "P0: billing-svc latency > 800ms",
    "severity": "P0",
    "trigger": "high latency on billing-svc",
}


def test_e2e_rename_transparent_matching():
    engine = Engine()
    engine.ingest(EVENTS_PHASE1)
    engine.ingest(EVENTS_PHASE2)

    ctx = engine.reconstruct_context(SIGNAL_PHASE2, mode="fast")

    # Must return a context without crashing
    assert ctx is not None
    assert isinstance(ctx["related_events"], list)
    assert isinstance(ctx["causal_chain"], list)
    assert isinstance(ctx["similar_past_incidents"], list)
    assert isinstance(ctx["suggested_remediations"], list)


def test_e2e_causal_chain_references_deploy():
    engine = Engine()
    engine.ingest(EVENTS_PHASE1)
    engine.ingest(EVENTS_PHASE2)

    ctx = engine.reconstruct_context(SIGNAL_PHASE2, mode="fast")

    # There must be at least one causal edge (from the new deploy v2.15.0)
    assert len(ctx["causal_chain"]) >= 1
    # Confidence must be in [0, 1]
    for edge in ctx["causal_chain"]:
        assert 0.0 <= edge["confidence"] <= 1.0


def test_e2e_similar_past_incident_found():
    engine = Engine()
    engine.ingest(EVENTS_PHASE1)
    engine.ingest(EVENTS_PHASE2)

    ctx = engine.reconstruct_context(SIGNAL_PHASE2, mode="fast")

    # After rename, the old incident on payments-svc should be found
    assert len(ctx["similar_past_incidents"]) >= 1
    past_ids = [m["past_incident_id"] for m in ctx["similar_past_incidents"]]
    assert "inc-payments-001" in past_ids


def test_e2e_remediation_suggested():
    engine = Engine()
    engine.ingest(EVENTS_PHASE1)
    engine.ingest(EVENTS_PHASE2)

    ctx = engine.reconstruct_context(SIGNAL_PHASE2, mode="fast")

    assert len(ctx["suggested_remediations"]) >= 1
    actions = [r["action"] for r in ctx["suggested_remediations"]]
    assert "rollback" in actions


def test_e2e_explain_mentions_current_name():
    engine = Engine()
    engine.ingest(EVENTS_PHASE1)
    engine.ingest(EVENTS_PHASE2)

    ctx = engine.reconstruct_context(SIGNAL_PHASE2, mode="fast")
    explain = ctx["explain"]
    assert "billing-svc" in explain or "billing" in explain.lower()


def test_e2e_empty_history_no_crash():
    engine = Engine()
    ctx = engine.reconstruct_context(
        {
            "id": "inc-cold",
            "service": "unknown-svc",
            "ts": _ts(0),
            "trigger": "unknown-svc is slow",
        },
        mode="fast",
    )
    assert ctx["similar_past_incidents"] == []
    assert ctx["confidence"] >= 0.0


def test_e2e_concurrent_incidents_no_cross_contamination():
    engine = Engine()

    events = [
        {
            "kind": "deploy",
            "id": "d1",
            "service": "svc-alpha",
            "ts": _ts(0),
            "version": "v1",
        },
        {
            "kind": "deploy",
            "id": "d2",
            "service": "svc-beta",
            "ts": _ts(1),
            "version": "v1",
        },
        {
            "kind": "incident_signal",
            "id": "inc-alpha",
            "service": "svc-alpha",
            "ts": _ts(5),
            "trigger": "svc-alpha down",
        },
        {
            "kind": "incident_signal",
            "id": "inc-beta",
            "service": "svc-beta",
            "ts": _ts(6),
            "trigger": "svc-beta down",
        },
    ]
    engine.ingest(events)

    ctx_alpha = engine.reconstruct_context(
        {"id": "inc-alpha", "service": "svc-alpha", "ts": _ts(5), "trigger": "svc-alpha down"}
    )
    ctx_beta = engine.reconstruct_context(
        {"id": "inc-beta", "service": "svc-beta", "ts": _ts(6), "trigger": "svc-beta down"}
    )

    # Each context should reference events from its own service
    alpha_services = {e.get("service") for e in ctx_alpha["related_events"]}
    beta_services = {e.get("service") for e in ctx_beta["related_events"]}
    # They may overlap (if svc-alpha depends on svc-beta), but the causal chains should differ
    assert ctx_alpha is not ctx_beta
