"""Unit tests for EpisodicStore."""
import pytest
from datetime import datetime, timezone, timedelta

from engine.topology_tracker import TopologyTracker
from engine.ingestion import EpisodicStore


def _make_store():
    topo = TopologyTracker()
    return EpisodicStore(topo), topo


def _ev(kind, service, ts_offset_min=0, **extra):
    base = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    ts = base + timedelta(minutes=ts_offset_min)
    return {"kind": kind, "service": service, "ts": ts.isoformat(), **extra}


def test_add_and_retrieve():
    store, _ = _make_store()
    eid = store.add(_ev("log", "svc-a", message="hello"))
    assert store.get(eid) is not None


def test_events_in_window():
    store, _ = _make_store()
    store.add(_ev("log", "svc-a", ts_offset_min=0))
    store.add(_ev("log", "svc-a", ts_offset_min=5))
    store.add(_ev("log", "svc-a", ts_offset_min=20))  # outside window

    base = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    results = store.events_in_window(base, base + timedelta(minutes=10))
    assert len(results) == 2


def test_metric_anomaly_flagged():
    store, _ = _make_store()
    # Add baseline values with slight variance so stdev > 0
    for i in range(10):
        store.add(_ev("metric", "svc-a", value=100.0 + (i % 3) * 2.0, metric="latency_ms", ts_offset_min=i))
    # Add anomalous value far from baseline
    store.add(_ev("metric", "svc-a", value=500.0, metric="latency_ms", ts_offset_min=11))

    base = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    evs = store.events_in_window(
        base, base + timedelta(minutes=15),
        kinds=["metric"]
    )
    anomalous = [e for e in evs if e.get("_anomalous")]
    assert len(anomalous) >= 1


def test_canonical_id_consistent_after_rename():
    store, topo = _make_store()
    topo.register_service("payments-svc")
    topo.register_rename(
        "payments-svc", "billing-svc",
        datetime(2024, 1, 1, 12, 5, tzinfo=timezone.utc)
    )
    eid1 = store.add(_ev("log", "payments-svc", ts_offset_min=0))
    eid2 = store.add(_ev("log", "billing-svc", ts_offset_min=10))

    ev1 = store.get(eid1)
    ev2 = store.get(eid2)
    assert ev1["_canonical_id"] == ev2["_canonical_id"]


def test_recent_deploys_before():
    store, _ = _make_store()
    store.add(_ev("deploy", "svc-a", ts_offset_min=0, version="v1.0"))
    store.add(_ev("deploy", "svc-a", ts_offset_min=10, version="v1.1"))
    store.add(_ev("deploy", "svc-a", ts_offset_min=20, version="v1.2"))  # outside window

    base = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    incident_ts = base + timedelta(minutes=15)
    cid = store.topology.resolve("svc-a")
    deploys = store.recent_deploys_before(cid, incident_ts, window_minutes=15)
    assert len(deploys) == 2
