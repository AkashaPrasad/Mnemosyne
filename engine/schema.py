"""
TypedDicts matching the harness contract for bench-p02-context.
Mirrors schema.py from the evaluation harness.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict


class Event(TypedDict, total=False):
    id: str
    kind: str          # deploy | log | metric | trace | topology | incident_signal | remediation
    ts: str            # ISO-8601
    service: str
    # deploy fields
    version: str
    status: str
    # log fields
    level: str         # info | warn | error | critical
    message: str
    trace_id: str
    # metric fields
    metric: str
    value: float
    unit: str
    # trace fields
    spans: List[Dict[str, Any]]
    latency_ms: float
    # topology fields
    change: str        # rename | add | remove
    old_name: str
    new_name: str
    # incident_signal fields
    alert: str
    severity: str      # P0 | P1 | P2 | P3
    # remediation fields
    incident_id: str
    action: str
    target: str
    outcome: str       # resolved | mitigated | no_effect


class IncidentSignal(TypedDict, total=False):
    id: str
    ts: str
    trigger: str       # e.g. "high latency on payments-svc"
    service: str
    alert: str
    severity: str
    metrics: Dict[str, float]
    logs: List[str]


class CausalEdge(TypedDict):
    cause_id: str
    effect_id: str
    evidence: List[str]
    confidence: float


class IncidentMatch(TypedDict):
    incident_id: str
    similarity: float
    rationale: str


class Remediation(TypedDict):
    action: str
    target: str
    historical_outcome: str
    confidence: float


class Context(TypedDict):
    related_events: List[Event]
    causal_chain: List[CausalEdge]
    similar_past_incidents: List[IncidentMatch]
    suggested_remediations: List[Remediation]
    confidence: float
    explain: str
