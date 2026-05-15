"""
BehavioralFingerprint + IncidentMemory + topology-independent incident matching.

A BehavioralFingerprint encodes what HAPPENED structurally, not which services
were involved. This is the key to surviving topology renames.

Similarity dimensions and weights:
  deploy_pattern      0.35 — was there a pre-incident deploy? how recent? same role?
  error_cascade       0.30 — same structural error flow (role sequence)?
  metric_signature    0.20 — same metrics degraded?
  resolution_pattern  0.15 — similar fix topology role?
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from .ingestion import EpisodicStore, _parse_ts
from .memory_substrate import TemporalCausalGraph
from .schema import IncidentMatch, Remediation
from .topology_tracker import TopologyTracker


@dataclass
class BehavioralFingerprint:
    incident_id: str
    ts: datetime

    # deploy_pattern: (had_pre_deploy, deploy_role, minutes_before)
    had_pre_deploy: bool = False
    deploy_role: str = "unknown"       # topology role of deployed service
    deploy_minutes_before: float = 0.0

    # error_cascade: ordered list of topology roles that errored
    error_role_sequence: List[str] = field(default_factory=list)

    # metric_signature: frozenset of metric names that were anomalous
    anomalous_metrics: List[str] = field(default_factory=list)

    # resolution_pattern
    resolution_role: str = "unknown"   # topology role of fixed service
    resolution_action: str = ""        # rollback | config_change | restart | scale | etc.

    # trigger role
    trigger_role: str = "unknown"

    # services involved (canonical_ids, NOT names)
    involved_canonical_ids: List[str] = field(default_factory=list)

    # raw remediation for ranking
    raw_remediation: Optional[Dict[str, Any]] = None


class IncidentMemory:
    """Stores closed-incident BehavioralFingerprints, queryable by similarity."""

    def __init__(
        self,
        store: EpisodicStore,
        graph: TemporalCausalGraph,
        topology: TopologyTracker,
    ) -> None:
        self.store = store
        self.graph = graph
        self.topology = topology
        # incident_id → BehavioralFingerprint
        self._fingerprints: Dict[str, BehavioralFingerprint] = {}

    # ── Fingerprint extraction ─────────────────────────────────────────────

    def extract_and_store(
        self,
        incident_id: str,
        trigger_canonical: str,
        incident_ts: datetime,
        remediation_event: Optional[Dict[str, Any]] = None,
        window_minutes: int = 15,
    ) -> BehavioralFingerprint:
        fp = self._build_fingerprint(
            incident_id, trigger_canonical, incident_ts, remediation_event, window_minutes
        )
        self._fingerprints[incident_id] = fp
        return fp

    def _build_fingerprint(
        self,
        incident_id: str,
        trigger_canonical: str,
        incident_ts: datetime,
        remediation_event: Optional[Dict[str, Any]],
        window_minutes: int,
    ) -> BehavioralFingerprint:
        window_start = incident_ts - timedelta(minutes=window_minutes)
        # Extend deploy lookback to 45 min — incidents consistently have a
        # pre-incident deploy ~30 min before which falls outside the 15-min window.
        deploy_window_start = incident_ts - timedelta(minutes=max(window_minutes, 45))

        # Collect involved services (trigger + 2-hop)
        involved = {trigger_canonical}
        involved |= self.graph.depends_on(trigger_canonical, hops=2)
        involved |= self.graph.depended_on_by(trigger_canonical, hops=2)

        fp = BehavioralFingerprint(incident_id=incident_id, ts=incident_ts)
        fp.trigger_role = self.graph.topology_role(trigger_canonical)
        fp.involved_canonical_ids = list(involved)

        # ── Deploy pattern ────────────────────────────────────────────────
        deploys = self.store.events_in_window(
            deploy_window_start, incident_ts, canonical_ids=list(involved), kinds=["deploy"]
        )
        if deploys:
            earliest = min(deploys, key=lambda e: e.get("_ts_parsed", incident_ts))
            deploy_cid = earliest.get("_canonical_id", trigger_canonical)
            fp.had_pre_deploy = True
            fp.deploy_role = self.graph.topology_role(deploy_cid)
            fp.deploy_minutes_before = (
                incident_ts - earliest.get("_ts_parsed", incident_ts)
            ).total_seconds() / 60.0

        # ── Error cascade ─────────────────────────────────────────────────
        error_logs = self.store.events_in_window(
            window_start, incident_ts, canonical_ids=list(involved), kinds=["log"]
        )
        error_logs = [e for e in error_logs if e.get("level") in ("error", "critical")]
        error_logs.sort(key=lambda e: e.get("_ts_parsed", incident_ts))
        seen_roles: List[str] = []
        for ev in error_logs:
            role = self.graph.topology_role(ev.get("_canonical_id", ""))
            if not seen_roles or seen_roles[-1] != role:
                seen_roles.append(role)
        fp.error_role_sequence = seen_roles

        # ── Metric signature ──────────────────────────────────────────────
        metric_events = self.store.events_in_window(
            window_start, incident_ts, canonical_ids=[trigger_canonical], kinds=["metric"]
        )
        fp.anomalous_metrics = sorted({
            e.get("metric") or e.get("name", "") for e in metric_events if e.get("_anomalous")
        })

        # ── Resolution pattern ────────────────────────────────────────────
        if remediation_event:
            fp.raw_remediation = remediation_event
            target_name = remediation_event.get("target", "")
            target_cid = self.topology.resolve(target_name) if target_name else trigger_canonical
            fp.resolution_role = self.graph.topology_role(target_cid)
            action = remediation_event.get("action", "")
            fp.resolution_action = _normalize_action(action)

        return fp

    # ── Similarity matching ────────────────────────────────────────────────

    def match(
        self,
        trigger_canonical: str,
        incident_ts: datetime,
        signal: Dict[str, Any],
        top_k: int = 5,
        mode: str = "fast",
        window_minutes: int = 15,
    ) -> List[IncidentMatch]:
        if not self._fingerprints:
            return []

        current_fp = self._build_fingerprint(
            incident_id="__current__",
            trigger_canonical=trigger_canonical,
            incident_ts=incident_ts,
            remediation_event=None,
            window_minutes=window_minutes,
        )

        scored: List[Tuple[float, BehavioralFingerprint]] = []
        for fp in self._fingerprints.values():
            sim = _compute_similarity(current_fp, fp)
            if sim > 0.0:
                scored.append((sim, fp))

        scored.sort(key=lambda x: -x[0])
        results = []
        for sim, fp in scored[:top_k]:
            current_name = self.topology.current_name(trigger_canonical) or trigger_canonical
            historical_name = self.topology.current_name(fp.involved_canonical_ids[0]) if fp.involved_canonical_ids else "unknown"
            rationale = _build_rationale(current_fp, fp, sim, current_name, self.topology)
            results.append(
                IncidentMatch(
                    incident_id=fp.incident_id,
                    similarity=round(sim, 3),
                    rationale=rationale,
                )
            )
        return results

    def all_fingerprints(self) -> List[BehavioralFingerprint]:
        return list(self._fingerprints.values())

    def get(self, incident_id: str) -> Optional[BehavioralFingerprint]:
        return self._fingerprints.get(incident_id)


# ── Similarity computation ─────────────────────────────────────────────────

WEIGHTS = {
    "deploy_pattern": 0.35,
    "error_cascade": 0.30,
    "metric_signature": 0.20,
    "resolution_pattern": 0.15,
}


def _compute_similarity(a: BehavioralFingerprint, b: BehavioralFingerprint) -> float:
    score = 0.0

    # Deploy pattern
    deploy_sim = 0.0
    if a.had_pre_deploy and b.had_pre_deploy:
        role_match = 1.0 if a.deploy_role == b.deploy_role else 0.4
        time_sim = _time_bucket_similarity(a.deploy_minutes_before, b.deploy_minutes_before)
        deploy_sim = (role_match * 0.6 + time_sim * 0.4)
    elif not a.had_pre_deploy and not b.had_pre_deploy:
        deploy_sim = 0.8
    else:
        deploy_sim = 0.1
    score += WEIGHTS["deploy_pattern"] * deploy_sim

    # Error cascade
    cascade_sim = _sequence_similarity(a.error_role_sequence, b.error_role_sequence)
    score += WEIGHTS["error_cascade"] * cascade_sim

    # Metric signature
    metric_sim = _set_jaccard(a.anomalous_metrics, b.anomalous_metrics)
    score += WEIGHTS["metric_signature"] * metric_sim

    # Resolution pattern (skip if no resolution data)
    res_sim = 0.0
    has_res_a = a.resolution_role and a.resolution_role != "unknown"
    has_res_b = b.resolution_role and b.resolution_role != "unknown"
    if has_res_a and has_res_b:
        role_match = 1.0 if a.resolution_role == b.resolution_role else 0.3
        action_match = 1.0 if a.resolution_action and a.resolution_action == b.resolution_action else 0.4
        res_sim = (role_match * 0.5 + action_match * 0.5)
    score += WEIGHTS["resolution_pattern"] * res_sim

    return max(0.0, min(1.0, score))


def _time_bucket_similarity(a_min: float, b_min: float) -> float:
    """Two deploys are 'similar' timing if within same 5-minute bucket."""
    if a_min == 0 and b_min == 0:
        return 1.0
    diff = abs(a_min - b_min)
    return max(0.0, 1.0 - diff / 15.0)


def _sequence_similarity(a: List[str], b: List[str]) -> float:
    """LCS-based similarity on role sequences."""
    if not a and not b:
        return 0.5   # no evidence in this dimension — neutral, not perfect match
    if not a or not b:
        return 0.3
    lcs = _lcs_length(a, b)
    return (2 * lcs) / (len(a) + len(b))


def _lcs_length(a: List[str], b: List[str]) -> int:
    m, n = len(a), len(b)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if a[i - 1] == b[j - 1]:
                dp[i][j] = dp[i - 1][j - 1] + 1
            else:
                dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])
    return dp[m][n]


def _set_jaccard(a: List[str], b: List[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 0.5   # no evidence in this dimension — neutral
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _normalize_action(action: str) -> str:
    action = action.lower()
    if "rollback" in action:
        return "rollback"
    if "restart" in action or "reboot" in action:
        return "restart"
    if "scale" in action:
        return "scale"
    if "config" in action or "flag" in action:
        return "config_change"
    if "patch" in action or "deploy" in action:
        return "deploy"
    return action.split()[0] if action.split() else "unknown"


def _build_rationale(
    current: BehavioralFingerprint,
    past: BehavioralFingerprint,
    similarity: float,
    current_service_name: str,
    topology: TopologyTracker,
) -> str:
    parts = []
    parts.append(f"Similarity {similarity:.0%}.")

    if current.had_pre_deploy and past.had_pre_deploy:
        parts.append(
            f"Both incidents followed a deploy to a {past.deploy_role} service "
            f"(~{past.deploy_minutes_before:.0f} min before)."
        )

    if current.error_role_sequence and past.error_role_sequence:
        parts.append(
            f"Error cascade followed same role pattern: {' → '.join(past.error_role_sequence)}."
        )

    if current.anomalous_metrics and past.anomalous_metrics:
        shared = set(current.anomalous_metrics) & set(past.anomalous_metrics)
        if shared:
            parts.append(f"Shared anomalous metrics: {', '.join(sorted(shared))}.")

    if past.resolution_action and past.resolution_action != "unknown":
        parts.append(f"Past incident resolved via {past.resolution_action}.")

    return " ".join(parts)
