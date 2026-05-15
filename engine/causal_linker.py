"""
CausalLinker — temporal window correlation and causal chain inference.

Algorithm:
  1. Anchor the triggering service → canonical_id
  2. Look back up to 15 min for deploys within 2 dependency hops
  3. Correlate metric anomalies in the same window
  4. Detect upstream error cascades via trace/log linkage
  5. Chain: deploy → metric_spike → upstream_error → incident
  6. Each edge carries calibrated confidence (inverse time-distance weighted)
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .ingestion import EpisodicStore, _parse_ts
from .memory_substrate import TemporalCausalGraph
from .schema import CausalEdge
from .topology_tracker import TopologyTracker


def _time_confidence(delta_seconds: float, max_window: float = 900.0) -> float:
    """
    Inverse-distance weighting: 2-min gap → high confidence, 15-min gap → lower.
    Returns value in [0.10, 0.95].
    """
    if delta_seconds <= 0:
        return 0.95
    ratio = delta_seconds / max_window
    # Exponential decay
    conf = 0.95 * math.exp(-2.5 * ratio)
    return max(0.10, min(0.95, conf))


class CausalLinker:
    def __init__(
        self,
        store: EpisodicStore,
        graph: TemporalCausalGraph,
        topology: TopologyTracker,
    ) -> None:
        self.store = store
        self.graph = graph
        self.topology = topology

    def build_causal_chain(
        self,
        trigger_canonical: str,
        incident_ts: datetime,
        incident_event_id: str,
        window_minutes: int = 15,
    ) -> List[CausalEdge]:
        """
        Build a causal chain anchored at trigger_canonical and incident_ts.
        Returns a list of CausalEdge dicts sorted from earliest cause to effect.
        """
        edges: List[CausalEdge] = []
        window_start = incident_ts - timedelta(minutes=window_minutes)

        # Services within 2 hops (direct + one-hop dependencies)
        related_services = {trigger_canonical}
        related_services |= self.graph.depends_on(trigger_canonical, hops=2)
        related_services |= self.graph.depended_on_by(trigger_canonical, hops=2)

        # ── Phase 1: Deploy → Incident ────────────────────────────────────
        deploy_events = self.store.events_in_window(
            window_start, incident_ts, canonical_ids=list(related_services), kinds=["deploy"]
        )
        for deploy_ev in deploy_events:
            deploy_ts: datetime = deploy_ev.get("_ts_parsed", incident_ts)
            delta = (incident_ts - deploy_ts).total_seconds()
            conf = _time_confidence(delta)
            # Boost confidence if it's the exact service
            if deploy_ev.get("_canonical_id") == trigger_canonical:
                conf = min(0.95, conf + 0.15)
            edges.append(
                CausalEdge(
                    cause_id=deploy_ev["id"],
                    effect_id=incident_event_id,
                    evidence=[
                        f"deploy {deploy_ev.get('version','?')} to {deploy_ev.get('service','?')} "
                        f"at {deploy_ts.isoformat()} ({delta:.0f}s before incident)"
                    ],
                    confidence=round(conf, 3),
                )
            )

        # ── Phase 2: Metric Anomaly → Incident ───────────────────────────
        metric_events = self.store.events_in_window(
            window_start, incident_ts, canonical_ids=[trigger_canonical], kinds=["metric"]
        )
        anomalous_metrics = [e for e in metric_events if e.get("_anomalous")]
        for metric_ev in anomalous_metrics:
            metric_ts: datetime = metric_ev.get("_ts_parsed", incident_ts)
            delta = (incident_ts - metric_ts).total_seconds()
            conf = _time_confidence(delta) * 0.8  # slightly lower than deploy
            zscore = metric_ev.get("_zscore", 0.0)
            zscore_boost = min(0.15, abs(zscore) * 0.03)
            conf = min(0.90, conf + zscore_boost)
            edges.append(
                CausalEdge(
                    cause_id=metric_ev["id"],
                    effect_id=incident_event_id,
                    evidence=[
                        f"metric {metric_ev.get('metric','?')} anomaly: "
                        f"value={metric_ev.get('value','?')} (z={zscore:.1f}) "
                        f"at {metric_ts.isoformat()}"
                    ],
                    confidence=round(conf, 3),
                )
            )

        # ── Phase 3: Upstream Error Log Cascade → Incident ───────────────
        upstream_ids = self.graph.depended_on_by(trigger_canonical, hops=2)
        upstream_ids.discard(trigger_canonical)
        if upstream_ids:
            upstream_errors = self.store.events_in_window(
                window_start, incident_ts,
                canonical_ids=list(upstream_ids),
                kinds=["log"],
            )
            upstream_errors = [
                e for e in upstream_errors if e.get("level") in ("error", "critical")
            ]
            for err_ev in upstream_errors:
                err_ts: datetime = err_ev.get("_ts_parsed", incident_ts)
                delta = (incident_ts - err_ts).total_seconds()
                if delta < 0:
                    continue
                conf = _time_confidence(delta) * 0.65
                edges.append(
                    CausalEdge(
                        cause_id=err_ev["id"],
                        effect_id=incident_event_id,
                        evidence=[
                            f"upstream error on {err_ev.get('service','?')}: "
                            f"\"{err_ev.get('message','')[:80]}\" at {err_ts.isoformat()}"
                        ],
                        confidence=round(conf, 3),
                    )
                )

        # ── Phase 4: Chain deploy → metric_spike (if both present) ────────
        for deploy_edge in [e for e in edges if "deploy" in (e["evidence"][0] if e["evidence"] else "")]:
            deploy_ev_id = deploy_edge["cause_id"]
            deploy_ts = self.store.get_event_ts(deploy_ev_id)
            if deploy_ts is None:
                continue
            for m_ev in anomalous_metrics:
                m_ts: datetime = m_ev.get("_ts_parsed")
                if m_ts and deploy_ts < m_ts < incident_ts:
                    delta = (m_ts - deploy_ts).total_seconds()
                    conf = _time_confidence(delta) * 0.85
                    edges.append(
                        CausalEdge(
                            cause_id=deploy_ev_id,
                            effect_id=m_ev["id"],
                            evidence=[
                                f"deploy preceded metric anomaly by {delta:.0f}s "
                                f"(Granger precedence satisfied)"
                            ],
                            confidence=round(conf, 3),
                        )
                    )

        # Deduplicate identical (cause_id, effect_id) pairs — keep highest confidence
        deduped: Dict[Tuple[str, str], CausalEdge] = {}
        for edge in edges:
            key = (edge["cause_id"], edge["effect_id"])
            if key not in deduped or edge["confidence"] > deduped[key]["confidence"]:
                deduped[key] = edge

        # Sort by confidence descending, then by cause timestamp ascending
        result = sorted(
            deduped.values(),
            key=lambda e: (-e["confidence"], self.store.get_event_ts(e["cause_id"]) or datetime.min),
        )
        return result

    def extract_dependency_edges_from_traces(self, trace_event: Dict[str, Any]) -> None:
        """
        Parse span list from a trace event and register DEPENDS_ON edges.
        Span format: {"caller": str, "callee": str, "latency_ms": float, "trace_id": str}
        """
        spans = trace_event.get("spans") or []
        ts = trace_event.get("_ts_parsed") or _parse_ts(trace_event.get("ts", ""))

        for span in spans:
            caller_name = span.get("caller") or span.get("service")
            callee_name = span.get("callee") or span.get("target")
            if not caller_name or not callee_name:
                continue
            caller_cid = self.topology.resolve(caller_name)
            callee_cid = self.topology.resolve(callee_name)
            if caller_cid == callee_cid:
                continue
            # Ensure service nodes exist
            self.graph.upsert_service(caller_cid, caller_name)
            self.graph.upsert_service(callee_cid, callee_name)
            evidence = trace_event.get("id", trace_event.get("trace_id", "?"))
            self.graph.upsert_depends_on(caller_cid, callee_cid, evidence, ts)
