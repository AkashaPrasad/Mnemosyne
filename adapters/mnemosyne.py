"""
Mnemosyne adapter — implements the harness BaseAdapter interface.

Import path expected by the harness:
    from adapters.mnemosyne import Engine

Three public methods:
    ingest(events: Iterable[Event]) -> None
    reconstruct_context(signal: IncidentSignal, mode="fast") -> Context
    close() -> None
"""
from __future__ import annotations

import sys
import os

# Allow running from the bench-p02-context directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from engine.causal_linker import CausalLinker
from engine.context_compiler import ContextCompiler, _extract_service_name
from engine.incident_matcher import IncidentMemory
from engine.ingestion import EpisodicStore, _parse_ts
from engine.memory_substrate import TemporalCausalGraph
from engine.remediation_ranker import RemediationRanker
from engine.schema import Context, Event, IncidentSignal
from engine.topology_tracker import TopologyTracker


class Engine:
    """
    Persistent Context Engine for Autonomous SRE.

    Thread-safety: single-threaded ingestion assumed (as per harness contract).
    """

    def __init__(self) -> None:
        self.topology = TopologyTracker()
        self.graph = TemporalCausalGraph()
        self.store = EpisodicStore(self.topology)
        self.causal_linker = CausalLinker(self.store, self.graph, self.topology)
        self.incident_memory = IncidentMemory(self.store, self.graph, self.topology)
        self.remediation_ranker = RemediationRanker(self.incident_memory, self.topology)
        self.context_compiler = ContextCompiler(
            self.store,
            self.graph,
            self.topology,
            self.causal_linker,
            self.incident_memory,
            self.remediation_ranker,
        )
        # track open incidents: incident_id → (canonical_id, ts, event_id)
        self._open_incidents: Dict[str, tuple] = {}

    # ── Public API ─────────────────────────────────────────────────────────

    def ingest(self, events: Iterable[Event]) -> None:
        """
        Process a stream of telemetry events.
        Updates episodic store, semantic graph, and incident memory.
        """
        for event in events:
            self._process_event(event)

    def reconstruct_context(
        self, signal: IncidentSignal, mode: str = "fast"
    ) -> Context:
        """
        Reconstruct full investigation context for a new incident signal.

        mode="fast"  → pre-computed fingerprints + template explain (p95 ≤ 2s)
        mode="deep"  → full traversal + LLM explain (p95 ≤ 6s)
        """
        return self.context_compiler.compile(signal, mode=mode)

    def close(self) -> None:
        """Release resources. No-op for pure in-memory implementation."""
        pass

    # ── Internal event processing ──────────────────────────────────────────

    def _process_event(self, event: Event) -> None:
        kind = event.get("kind", "unknown")
        ts_str = event.get("ts", "")
        ts = _parse_ts(ts_str) if ts_str else datetime.now(timezone.utc)

        # ── Topology rename must be handled BEFORE add() ──────────────────
        if kind == "topology":
            self._handle_topology(event, ts)
            return  # topology events don't go into episodic store

        # Store in episodic store (assigns _canonical_id, _ts_parsed, etc.)
        event_id = self.store.add(event)
        stored_ev = self.store.get(event_id)

        canonical_id = stored_ev.get("_canonical_id", "")
        service_name = event.get("service", "")

        if canonical_id and service_name:
            self.graph.upsert_service(canonical_id, service_name)

        # Kind-specific graph/memory updates
        if kind == "deploy":
            self._handle_deploy(stored_ev, canonical_id, ts)

        elif kind == "trace":
            self.causal_linker.extract_dependency_edges_from_traces(stored_ev)

        elif kind == "incident_signal":
            self._handle_incident_signal(stored_ev, canonical_id, ts, event_id)

        elif kind == "remediation":
            self._handle_remediation(stored_ev, canonical_id, ts)

    def _handle_topology(self, event: Event, ts: datetime) -> None:
        change = event.get("change", "")
        if change == "rename":
            old_name = event.get("old_name", "")
            new_name = event.get("new_name", "")
            if old_name and new_name:
                cid = self.topology.register_rename(old_name, new_name, ts)
                self.graph.upsert_service(cid, new_name)
        elif change == "add":
            name = event.get("service") or event.get("new_name", "")
            if name:
                cid = self.topology.register_service(name)
                self.graph.upsert_service(cid, name)

    def _handle_deploy(self, event: Dict[str, Any], canonical_id: str, ts: datetime) -> None:
        version = event.get("version", "unknown")
        status = event.get("status", "success")
        self.graph.add_deploy(canonical_id, version, ts, status)

    def _handle_incident_signal(
        self,
        event: Dict[str, Any],
        canonical_id: str,
        ts: datetime,
        event_id: str,
    ) -> None:
        # Prefer the semantic incident_id field over the store-assigned event UUID
        incident_id = event.get("incident_id") or event.get("id", event_id)
        severity = event.get("severity", "P2")
        self.graph.open_incident(incident_id, canonical_id, ts, severity)
        self._open_incidents[incident_id] = (canonical_id, ts, event_id)

    def _handle_remediation(
        self, event: Dict[str, Any], canonical_id: str, ts: datetime
    ) -> None:
        incident_id = event.get("incident_id", "")
        action = event.get("action", "")
        target = event.get("target", "")
        outcome = event.get("outcome", "unknown")

        self.graph.close_incident(incident_id, ts)

        # Record action stats for future ranking
        target_cid = self.topology.resolve(target) if target else canonical_id
        target_role = self.graph.topology_role(target_cid)
        self.remediation_ranker.record_outcome(action, target_role, outcome)

        # Extract and store behavioral fingerprint for the closed incident
        if incident_id in self._open_incidents:
            trigger_cid, inc_ts, _ = self._open_incidents.pop(incident_id)
        else:
            trigger_cid = canonical_id
            inc_ts = ts

        self.incident_memory.extract_and_store(
            incident_id=incident_id,
            trigger_canonical=trigger_cid,
            incident_ts=inc_ts,
            remediation_event=dict(event),
        )
