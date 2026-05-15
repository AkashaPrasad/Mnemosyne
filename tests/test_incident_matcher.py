"""Unit tests for IncidentMemory and similarity matching."""
import pytest
from datetime import datetime, timezone, timedelta

from engine.topology_tracker import TopologyTracker
from engine.ingestion import EpisodicStore
from engine.memory_substrate import TemporalCausalGraph
from engine.incident_matcher import IncidentMemory, BehavioralFingerprint, _compute_similarity


BASE = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)


def _build_stack():
    topo = TopologyTracker()
    store = EpisodicStore(topo)
    graph = TemporalCausalGraph()
    memory = IncidentMemory(store, graph, topo)
    return topo, store, graph, memory


def test_fingerprint_stored_on_extract():
    topo, store, graph, memory = _build_stack()
    cid = topo.register_service("payments-svc")

    fp = memory.extract_and_store(
        incident_id="inc-001",
        trigger_canonical=cid,
        incident_ts=BASE,
        remediation_event={"action": "rollback", "target": "payments-svc", "outcome": "resolved"},
    )
    assert fp.incident_id == "inc-001"
    assert memory.get("inc-001") is not None


def test_match_returns_empty_with_no_history():
    topo, store, graph, memory = _build_stack()
    cid = topo.register_service("svc-a")
    results = memory.match(cid, BASE, {}, top_k=5)
    assert results == []


def test_rename_agnostic_matching():
    """
    Fingerprint stored under payments-svc canonical_id must match
    a new incident on billing-svc (after rename) because they share canonical_id.
    """
    topo, store, graph, memory = _build_stack()

    # Register rename
    cid = topo.register_service("payments-svc")
    topo.register_rename("payments-svc", "billing-svc", BASE + timedelta(hours=1))

    # Store a past incident fingerprint under old name
    memory.extract_and_store(
        incident_id="past-001",
        trigger_canonical=cid,
        incident_ts=BASE,
        remediation_event={"action": "rollback", "target": "payments-svc", "outcome": "resolved"},
    )

    # New incident query uses billing-svc (resolved to same cid)
    billing_cid = topo.resolve("billing-svc")
    assert billing_cid == cid

    results = memory.match(billing_cid, BASE + timedelta(hours=2), {}, top_k=5)
    assert any(m["past_incident_id"] == "past-001" for m in results)


def test_similarity_deploy_pattern():
    fp_a = BehavioralFingerprint(
        incident_id="a", ts=BASE,
        had_pre_deploy=True, deploy_role="backend", deploy_minutes_before=5.0,
        error_role_sequence=["backend"], anomalous_metrics=["latency_ms"],
        resolution_role="backend", resolution_action="rollback",
        trigger_role="backend",
    )
    fp_b = BehavioralFingerprint(
        incident_id="b", ts=BASE,
        had_pre_deploy=True, deploy_role="backend", deploy_minutes_before=6.0,
        error_role_sequence=["backend"], anomalous_metrics=["latency_ms"],
        resolution_role="backend", resolution_action="rollback",
        trigger_role="backend",
    )
    sim = _compute_similarity(fp_a, fp_b)
    assert sim > 0.7


def test_similarity_no_deploy_mismatch():
    fp_a = BehavioralFingerprint(
        incident_id="a", ts=BASE,
        had_pre_deploy=True, deploy_role="backend", deploy_minutes_before=5.0,
    )
    fp_b = BehavioralFingerprint(
        incident_id="b", ts=BASE,
        had_pre_deploy=False,
    )
    sim = _compute_similarity(fp_a, fp_b)
    assert sim < 0.5
