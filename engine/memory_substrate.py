"""
TemporalCausalGraph — the semantic graph layer.

Nodes: ServiceEntity, IncidentEntity, DeployEntity
Edges: DEPENDS_ON, CAUSED_BY, RESOLVED_BY, CO_OCCURS_WITH

All edges carry valid_from/valid_to and confidence.
"""
from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class GraphEdge:
    edge_id: str
    src: str          # canonical_id or entity_id
    dst: str
    rel: str          # DEPENDS_ON | CAUSED_BY | RESOLVED_BY | CO_OCCURS_WITH
    confidence: float
    evidence: List[str] = field(default_factory=list)
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ServiceNode:
    canonical_id: str
    current_name: str
    role: Optional[str] = None      # frontend | gateway | backend | db | queue | cache
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IncidentNode:
    incident_id: str
    canonical_id: str               # triggering service
    ts: datetime
    severity: str = "P2"
    status: str = "open"            # open | closed
    closed_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeployNode:
    deploy_id: str
    canonical_id: str
    version: str
    ts: datetime
    status: str = "success"


class TemporalCausalGraph:
    """
    In-memory graph with adjacency dicts for fast traversal.
    All mutations are append-only (edges can be closed but not deleted).
    """

    def __init__(self) -> None:
        self._services: Dict[str, ServiceNode] = {}
        self._incidents: Dict[str, IncidentNode] = {}
        self._deploys: Dict[str, DeployNode] = {}

        # Adjacency: src canonical_id → list of GraphEdge (outgoing)
        self._out_edges: Dict[str, List[GraphEdge]] = defaultdict(list)
        # Reverse adjacency for fast inbound lookup
        self._in_edges: Dict[str, List[GraphEdge]] = defaultdict(list)

    # ── Service nodes ──────────────────────────────────────────────────────

    def upsert_service(self, canonical_id: str, name: str, **meta) -> ServiceNode:
        if canonical_id not in self._services:
            self._services[canonical_id] = ServiceNode(canonical_id, name, metadata=meta)
        else:
            node = self._services[canonical_id]
            node.current_name = name
            node.metadata.update(meta)
        return self._services[canonical_id]

    def get_service(self, canonical_id: str) -> Optional[ServiceNode]:
        return self._services.get(canonical_id)

    def all_services(self) -> List[ServiceNode]:
        return list(self._services.values())

    # ── Incident nodes ─────────────────────────────────────────────────────

    def open_incident(
        self, incident_id: str, canonical_id: str, ts: datetime, severity: str = "P2"
    ) -> IncidentNode:
        node = IncidentNode(incident_id, canonical_id, ts, severity)
        self._incidents[incident_id] = node
        return node

    def close_incident(self, incident_id: str, closed_at: datetime) -> Optional[IncidentNode]:
        node = self._incidents.get(incident_id)
        if node:
            node.status = "closed"
            node.closed_at = closed_at
        return node

    def get_incident(self, incident_id: str) -> Optional[IncidentNode]:
        return self._incidents.get(incident_id)

    def closed_incidents(self) -> List[IncidentNode]:
        return [n for n in self._incidents.values() if n.status == "closed"]

    def open_incidents(self) -> List[IncidentNode]:
        return [n for n in self._incidents.values() if n.status == "open"]

    # ── Deploy nodes ───────────────────────────────────────────────────────

    def add_deploy(
        self, canonical_id: str, version: str, ts: datetime, status: str = "success"
    ) -> DeployNode:
        did = f"deploy-{canonical_id}-{ts.isoformat()}"
        node = DeployNode(did, canonical_id, version, ts, status)
        self._deploys[did] = node
        return node

    def deploys_for(self, canonical_id: str) -> List[DeployNode]:
        return [d for d in self._deploys.values() if d.canonical_id == canonical_id]

    # ── Edges ──────────────────────────────────────────────────────────────

    def add_edge(
        self,
        src: str,
        dst: str,
        rel: str,
        confidence: float,
        evidence: Optional[List[str]] = None,
        valid_from: Optional[datetime] = None,
    ) -> GraphEdge:
        edge = GraphEdge(
            edge_id=str(uuid.uuid4()),
            src=src,
            dst=dst,
            rel=rel,
            confidence=confidence,
            evidence=evidence or [],
            valid_from=valid_from,
        )
        self._out_edges[src].append(edge)
        self._in_edges[dst].append(edge)
        return edge

    def upsert_depends_on(
        self, caller: str, callee: str, evidence: str, ts: datetime
    ) -> GraphEdge:
        # Deduplicate: bump confidence if edge already exists
        for e in self._out_edges.get(caller, []):
            if e.dst == callee and e.rel == "DEPENDS_ON" and e.valid_to is None:
                e.confidence = min(1.0, e.confidence + 0.05)
                if evidence not in e.evidence:
                    e.evidence.append(evidence)
                return e
        return self.add_edge(caller, callee, "DEPENDS_ON", 0.7, [evidence], ts)

    def depends_on(self, canonical_id: str, hops: int = 1) -> Set[str]:
        """Return all canonical_ids reachable via DEPENDS_ON within N hops."""
        visited: Set[str] = set()
        frontier = {canonical_id}
        for _ in range(hops):
            next_frontier: Set[str] = set()
            for node in frontier:
                for e in self._out_edges.get(node, []):
                    if e.rel == "DEPENDS_ON" and e.dst not in visited:
                        next_frontier.add(e.dst)
            visited |= next_frontier
            frontier = next_frontier
        return visited

    def depended_on_by(self, canonical_id: str, hops: int = 1) -> Set[str]:
        """Return canonical_ids that depend on this service (reverse traversal)."""
        visited: Set[str] = set()
        frontier = {canonical_id}
        for _ in range(hops):
            next_frontier: Set[str] = set()
            for node in frontier:
                for e in self._in_edges.get(node, []):
                    if e.rel == "DEPENDS_ON" and e.src not in visited:
                        next_frontier.add(e.src)
            visited |= next_frontier
            frontier = next_frontier
        return visited

    def topology_role(self, canonical_id: str) -> str:
        """
        Infer structural role from graph position.
        upstream  = has no inbound DEPENDS_ON edges (nothing calls it)
        gateway   = many inbound, few outbound
        backend   = few inbound, many outbound
        leaf      = no outbound (calls nothing)
        mid       = everything else
        """
        in_count = sum(1 for e in self._in_edges.get(canonical_id, []) if e.rel == "DEPENDS_ON")
        out_count = sum(1 for e in self._out_edges.get(canonical_id, []) if e.rel == "DEPENDS_ON")
        if in_count == 0 and out_count == 0:
            return "isolated"
        if in_count == 0:
            return "upstream"
        if out_count == 0:
            return "leaf"
        if in_count > 3 and out_count <= 2:
            return "gateway"
        if out_count > 3 and in_count <= 2:
            return "backend"
        return "mid"

    def edges_involving(self, canonical_id: str) -> List[GraphEdge]:
        out = self._out_edges.get(canonical_id, [])
        inn = self._in_edges.get(canonical_id, [])
        return list(out) + list(inn)

    def all_edges(self) -> List[GraphEdge]:
        seen: Set[str] = set()
        result = []
        for edges in self._out_edges.values():
            for e in edges:
                if e.edge_id not in seen:
                    seen.add(e.edge_id)
                    result.append(e)
        return result
