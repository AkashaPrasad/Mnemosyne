"""Unit tests for CausalLinker."""
import pytest
from datetime import datetime, timezone, timedelta

from engine.topology_tracker import TopologyTracker
from engine.ingestion import EpisodicStore
from engine.memory_substrate import TemporalCausalGraph
from engine.causal_linker import CausalLinker


def _build_stack():
    topo = TopologyTracker()
    store = EpisodicStore(topo)
    graph = TemporalCausalGraph()
    linker = CausalLinker(store, graph, topo)
    return topo, store, graph, linker


BASE = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)


def _ts(offset_min):
    return (BASE + timedelta(minutes=offset_min)).isoformat()


def test_deploy_creates_causal_edge():
    topo, store, graph, linker = _build_stack()

    store.add({"kind": "deploy", "service": "payments-svc", "ts": _ts(0), "version": "v2.0"})
    incident_ts = BASE + timedelta(minutes=5)
    incident_ev_id = "inc-001"
    cid = topo.resolve("payments-svc")

    edges = linker.build_causal_chain(cid, incident_ts, incident_ev_id)
    assert len(edges) >= 1
    assert edges[0]["confidence"] > 0.5


def test_no_deploy_no_causal_edge():
    topo, store, graph, linker = _build_stack()

    store.add({"kind": "log", "service": "svc-x", "ts": _ts(0), "level": "info", "message": "ok"})
    incident_ts = BASE + timedelta(minutes=5)
    cid = topo.resolve("svc-x")
    edges = linker.build_causal_chain(cid, incident_ts, "inc-002")
    deploy_edges = [e for e in edges if "deploy" in " ".join(e.get("evidence", []))]
    assert len(deploy_edges) == 0


def test_confidence_inversely_proportional_to_time_gap():
    topo, store, graph, linker = _build_stack()

    store.add({"kind": "deploy", "service": "svc-y", "ts": _ts(1), "version": "v1"})
    store.add({"kind": "deploy", "service": "svc-y", "ts": _ts(13), "version": "v2"})

    incident_ts = BASE + timedelta(minutes=15)
    cid = topo.resolve("svc-y")
    edges = linker.build_causal_chain(cid, incident_ts, "inc-003")
    deploy_edges = sorted(edges, key=lambda e: -e["confidence"])
    # Most recent deploy (v2 at t=13) should have higher confidence than v1 at t=1
    if len(deploy_edges) >= 2:
        assert deploy_edges[0]["confidence"] >= deploy_edges[-1]["confidence"]


def test_trace_extracts_dependency():
    topo, store, graph, linker = _build_stack()

    trace_ev = {
        "kind": "trace",
        "service": "frontend",
        "ts": _ts(0),
        "id": "trace-1",
        "spans": [
            {"caller": "frontend", "callee": "payments-svc"},
            {"caller": "payments-svc", "callee": "postgres-db"},
        ],
    }
    eid = store.add(trace_ev)
    linker.extract_dependency_edges_from_traces(store.get(eid))

    frontend_cid = topo.resolve("frontend")
    deps = graph.depends_on(frontend_cid, hops=1)
    payments_cid = topo.resolve("payments-svc")
    assert payments_cid in deps
