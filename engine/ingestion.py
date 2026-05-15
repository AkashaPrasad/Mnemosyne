"""
Episodic store and event ingestion pipeline.

Events are stored append-only, indexed by:
  - time bucket (minute-level)
  - canonical service ID
  - trace_id
  - incident_id
  - event kind
"""
from __future__ import annotations

import re
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean, stdev
from typing import Any, Dict, Iterable, List, Optional

from .schema import Event
from .topology_tracker import TopologyTracker


def _parse_ts(ts_str: str) -> datetime:
    if not ts_str:
        return datetime.now(timezone.utc)
    ts_str = ts_str.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(ts_str)
    except ValueError:
        return datetime.now(timezone.utc)


def _minute_bucket(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M")


def _extract_service(event: Event, topology: TopologyTracker) -> str:
    """Return canonical_id for the primary service in an event."""
    name = event.get("service") or event.get("target") or ""
    if not name:
        return topology.resolve("__unknown__")
    return topology.resolve(name)


class MetricSeries:
    """Rolling time series for a single (canonical_id, metric_name) pair."""

    WINDOW = 60  # data points kept

    def __init__(self) -> None:
        self.values: List[float] = []
        self.timestamps: List[datetime] = []

    def add(self, value: float, ts: datetime) -> None:
        self.values.append(value)
        self.timestamps.append(ts)
        if len(self.values) > self.WINDOW:
            self.values.pop(0)
            self.timestamps.pop(0)

    def is_anomalous(self, value: float, sigma: float = 2.0) -> bool:
        if len(self.values) < 5:
            return False
        mu = mean(self.values)
        try:
            sd = stdev(self.values)
        except Exception:
            return False
        if sd == 0:
            return False
        return abs(value - mu) > sigma * sd

    def zscore(self, value: float) -> float:
        if len(self.values) < 2:
            return 0.0
        mu = mean(self.values)
        try:
            sd = stdev(self.values)
        except Exception:
            return 0.0
        if sd == 0:
            return 0.0
        return (value - mu) / sd


class EpisodicStore:
    """
    Append-only store for raw telemetry events.
    All lookups go through canonical_id (never raw service name).
    """

    def __init__(self, topology: TopologyTracker) -> None:
        self.topology = topology

        # Primary store: event_id → Event
        self._events: Dict[str, Event] = {}

        # Indexes
        self._by_canonical: Dict[str, List[str]] = defaultdict(list)      # canonical_id → [event_id]
        self._by_kind: Dict[str, List[str]] = defaultdict(list)           # kind → [event_id]
        self._by_trace: Dict[str, List[str]] = defaultdict(list)          # trace_id → [event_id]
        self._by_incident: Dict[str, List[str]] = defaultdict(list)       # incident_id → [event_id]
        self._by_minute: Dict[str, List[str]] = defaultdict(list)         # "YYYY-MM-DDTHH:MM" → [event_id]

        # Metric time series: (canonical_id, metric_name) → MetricSeries
        self._metric_series: Dict[tuple, MetricSeries] = defaultdict(MetricSeries)

        # Error rate series per canonical_id
        self._error_windows: Dict[str, List[datetime]] = defaultdict(list)

        # Recent deploy per canonical_id (for causal anchor)
        self._recent_deploys: Dict[str, List[str]] = defaultdict(list)   # canonical_id → [event_id]

        # Open incidents: incident_id → event_id
        self._open_incidents: Dict[str, str] = {}

        # Sorted event ids by timestamp (maintained in insertion order, sorted lazily)
        self._sorted_dirty: bool = False
        self._sorted_ids: List[str] = []

    def add(self, event: Event) -> str:
        """Normalize, index, and store an event. Returns the event_id."""
        eid = event.get("id") or str(uuid.uuid4())
        event = dict(event)  # make mutable copy
        event["id"] = eid
        event.setdefault("_ingested_at", datetime.now(timezone.utc).isoformat())

        ts = _parse_ts(event.get("ts", ""))
        event["_ts_parsed"] = ts

        canonical_id = _extract_service(event, self.topology)
        event["_canonical_id"] = canonical_id

        self._events[eid] = event
        self._sorted_dirty = True

        # Build indexes
        self._by_canonical[canonical_id].append(eid)
        kind = event.get("kind", "unknown")
        self._by_kind[kind].append(eid)

        bucket = _minute_bucket(ts)
        self._by_minute[bucket].append(eid)

        if trace_id := event.get("trace_id"):
            self._by_trace[trace_id].append(eid)

        if incident_id := event.get("incident_id"):
            self._by_incident[incident_id].append(eid)

        # Kind-specific side effects
        if kind == "metric":
            # Generator uses "name"; internal events may use "metric"
            name = event.get("metric") or event.get("name", "unknown")
            val = event.get("value", 0.0)
            series = self._metric_series[(canonical_id, name)]
            event["_anomalous"] = series.is_anomalous(val)
            event["_zscore"] = series.zscore(val)
            series.add(val, ts)

        elif kind == "log" and event.get("level") in ("error", "critical"):
            self._error_windows[canonical_id].append(ts)
            # Prune older than 1 hour
            cutoff = ts.timestamp() - 3600
            self._error_windows[canonical_id] = [
                t for t in self._error_windows[canonical_id] if t.timestamp() > cutoff
            ]

        elif kind == "deploy":
            self._recent_deploys[canonical_id].append(eid)

        elif kind == "incident_signal":
            iid = event.get("id", eid)
            self._open_incidents[iid] = eid

        elif kind == "remediation":
            linked = event.get("incident_id")
            if linked and linked in self._open_incidents:
                del self._open_incidents[linked]

        return eid

    def get(self, event_id: str) -> Optional[Event]:
        return self._events.get(event_id)

    def get_event_ts(self, event_id: str) -> Optional[datetime]:
        ev = self._events.get(event_id)
        if ev is None:
            return None
        return ev.get("_ts_parsed")

    def events_for_canonical(self, canonical_id: str) -> List[Event]:
        return [self._events[eid] for eid in self._by_canonical.get(canonical_id, [])]

    def events_in_window(
        self,
        start: datetime,
        end: datetime,
        canonical_ids: Optional[List[str]] = None,
        kinds: Optional[List[str]] = None,
    ) -> List[Event]:
        """Return events between start and end, optionally filtered."""
        results = []
        for ev in self._events.values():
            ts: datetime = ev.get("_ts_parsed")
            if ts is None:
                continue
            if not (start <= ts <= end):
                continue
            if canonical_ids and ev.get("_canonical_id") not in canonical_ids:
                continue
            if kinds and ev.get("kind") not in kinds:
                continue
            results.append(ev)
        results.sort(key=lambda e: e.get("_ts_parsed"))
        return results

    def recent_deploys_before(
        self, canonical_id: str, before: datetime, window_minutes: int = 15
    ) -> List[Event]:
        cutoff = before.timestamp() - window_minutes * 60
        result = []
        for eid in self._recent_deploys.get(canonical_id, []):
            ev = self._events[eid]
            ts: datetime = ev.get("_ts_parsed")
            if ts and cutoff <= ts.timestamp() <= before.timestamp():
                result.append(ev)
        return result

    def error_rate(self, canonical_id: str, window_seconds: int = 300) -> float:
        """Errors per second over the last window_seconds."""
        times = self._error_windows.get(canonical_id, [])
        if not times:
            return 0.0
        now = datetime.now(timezone.utc).timestamp()
        recent = [t for t in times if t.timestamp() > now - window_seconds]
        return len(recent) / window_seconds

    def open_incidents(self) -> Dict[str, str]:
        return dict(self._open_incidents)

    def all_events(self) -> List[Event]:
        return list(self._events.values())

    def total_count(self) -> int:
        return len(self._events)
