"""
RemediationRanker — ranks historical remediations by:
  1. Similarity to current incident
  2. Historical success rate
  3. Topology role alignment
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional

from .incident_matcher import BehavioralFingerprint, IncidentMemory
from .schema import IncidentMatch, Remediation
from .topology_tracker import TopologyTracker


class RemediationRanker:
    def __init__(self, memory: IncidentMemory, topology: TopologyTracker) -> None:
        self.memory = memory
        self.topology = topology
        # action_key → {"successes": int, "attempts": int}
        self._action_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"successes": 0, "attempts": 0})

    def record_outcome(self, action: str, target_role: str, outcome: str) -> None:
        key = f"{action}::{target_role}"
        self._action_stats[key]["attempts"] += 1
        if outcome in ("resolved", "mitigated"):
            self._action_stats[key]["successes"] += 1

    def rank(
        self,
        similar_incidents: List[IncidentMatch],
        trigger_canonical: str,
        top_k: int = 3,
    ) -> List[Remediation]:
        """
        Given a list of similar past incidents, aggregate their remediations
        and return the top-K ranked by historical success confidence.
        """
        candidates: List[Dict[str, Any]] = []

        for match in similar_incidents:
            fp = self.memory.get(match["incident_id"])
            if fp is None or fp.raw_remediation is None:
                continue

            rem = fp.raw_remediation
            action = rem.get("action", "unknown")
            raw_target = rem.get("target", "")
            outcome = rem.get("outcome", "unknown")

            # Translate historical target to current topology
            # If the target was a renamed service, resolve to canonical → current name
            if raw_target:
                target_cid = self.topology.resolve(raw_target)
                current_target = self.topology.current_name(target_cid) or raw_target
            else:
                current_target = raw_target

            target_role = fp.resolution_role
            success_rate = self._success_rate(action, target_role)
            similarity_boost = match["similarity"] * 0.3

            confidence = min(0.97, success_rate + similarity_boost)

            candidates.append({
                "action": action,
                "target": current_target,
                "historical_outcome": outcome,
                "confidence": round(confidence, 3),
                "sort_key": confidence,
            })

        # Deduplicate by (action, target) — keep highest confidence
        seen: Dict[str, Dict[str, Any]] = {}
        for c in candidates:
            key = f"{c['action']}::{c['target']}"
            if key not in seen or c["confidence"] > seen[key]["confidence"]:
                seen[key] = c

        ranked = sorted(seen.values(), key=lambda x: -x["sort_key"])

        return [
            Remediation(
                action=r["action"],
                target=r["target"],
                historical_outcome=r["historical_outcome"],
                confidence=r["confidence"],
            )
            for r in ranked[:top_k]
        ]

    def _success_rate(self, action: str, target_role: str) -> float:
        key = f"{action}::{target_role}"
        stats = self._action_stats[key]
        attempts = stats["attempts"]
        if attempts == 0:
            # No data — return neutral prior
            return 0.45
        return stats["successes"] / attempts
