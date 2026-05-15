"""Unit tests for TopologyTracker."""
import pytest
from datetime import datetime, timezone

from engine.topology_tracker import TopologyTracker


def _ts(minute: int) -> datetime:
    return datetime(2024, 1, 1, 0, minute, tzinfo=timezone.utc)


def test_register_and_resolve():
    t = TopologyTracker()
    cid = t.register_service("payments-svc")
    assert t.resolve("payments-svc") == cid


def test_rename_same_canonical_id():
    t = TopologyTracker()
    cid = t.register_service("payments-svc")
    t.register_rename("payments-svc", "billing-svc", _ts(5))
    assert t.resolve("billing-svc") == cid
    assert t.resolve("payments-svc") == cid


def test_rename_chain():
    """A → B → C must all resolve to the same canonical_id."""
    t = TopologyTracker()
    cid = t.register_service("svc-a")
    t.register_rename("svc-a", "svc-b", _ts(1))
    t.register_rename("svc-b", "svc-c", _ts(2))
    assert t.resolve("svc-a") == cid
    assert t.resolve("svc-b") == cid
    assert t.resolve("svc-c") == cid


def test_aliases_contains_all_names():
    t = TopologyTracker()
    t.register_service("payments-svc")
    t.register_rename("payments-svc", "billing-svc", _ts(5))
    cid = t.resolve("billing-svc")
    aliases = t.aliases(cid)
    assert "payments-svc" in aliases
    assert "billing-svc" in aliases


def test_historical_names_before_rename():
    t = TopologyTracker()
    t.register_service("payments-svc")
    rename_ts = _ts(10)
    t.register_rename("payments-svc", "billing-svc", rename_ts)
    before_names = t.historical_names(t.resolve("billing-svc"), _ts(9))
    assert "payments-svc" in before_names
    assert "billing-svc" not in before_names


def test_unknown_service_created_on_resolve():
    t = TopologyTracker()
    cid = t.resolve("brand-new-svc")
    assert cid is not None
    assert t.resolve("brand-new-svc") == cid


def test_current_name_after_renames():
    t = TopologyTracker()
    t.register_service("a")
    t.register_rename("a", "b", _ts(1))
    t.register_rename("b", "c", _ts(2))
    cid = t.resolve("c")
    assert t.current_name(cid) == "c"
