"""
ContextCompiler — orchestrates reconstruct_context.

fast mode: pre-computed fingerprints + cosine similarity, template explain
deep mode: full graph traversal + Anthropic API narrative (falls back to template)
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .causal_linker import CausalLinker
from .incident_matcher import IncidentMemory
from .ingestion import EpisodicStore, _parse_ts
from .memory_substrate import TemporalCausalGraph
from .remediation_ranker import RemediationRanker
from .schema import Context, IncidentSignal
from .topology_tracker import TopologyTracker


def _extract_service_name(trigger: str) -> str:
    """
    Pull service name out of a free-text trigger string.
    Handles: "high latency on payments-svc", "payments-svc is down", "billing-svc P0"
    """
    import re
    # Typical SRE patterns: "on <svc>", "<svc> is", "<svc>:"
    for pattern in [
        r"on ([a-zA-Z0-9_\-\.]+(?:-svc|service|api|db|cache|queue)?)",
        r"([a-zA-Z0-9_\-\.]+(?:-svc|service|api|db|cache|queue))\s+(?:is|has|:)",
        r"([a-zA-Z0-9_\-\.]+(?:-svc|service|api|db|cache|queue))",
    ]:
        m = re.search(pattern, trigger, re.IGNORECASE)
        if m:
            return m.group(1)
    # Fall back: first word
    parts = trigger.split()
    return parts[0] if parts else trigger


def _compute_overall_confidence(
    causal_chain: list, similar_incidents: list
) -> float:
    if not causal_chain and not similar_incidents:
        return 0.1
    causal_conf = max((e["confidence"] for e in causal_chain), default=0.0)
    match_sim = max((m["similarity"] for m in similar_incidents), default=0.0)
    return round(min(0.99, causal_conf * 0.6 + match_sim * 0.4), 3)


class ContextCompiler:
    def __init__(
        self,
        store: EpisodicStore,
        graph: TemporalCausalGraph,
        topology: TopologyTracker,
        causal_linker: CausalLinker,
        incident_memory: IncidentMemory,
        remediation_ranker: RemediationRanker,
    ) -> None:
        self.store = store
        self.graph = graph
        self.topology = topology
        self.causal_linker = causal_linker
        self.incident_memory = incident_memory
        self.remediation_ranker = remediation_ranker

    def compile(self, signal: IncidentSignal, mode: str = "fast") -> Context:
        ts_str = signal.get("ts", "")
        incident_ts = _parse_ts(ts_str) if ts_str else datetime.now(timezone.utc)

        # Resolve trigger service → canonical_id
        trigger_raw = signal.get("service") or signal.get("trigger") or ""
        service_name = _extract_service_name(trigger_raw)
        canonical_id = self.topology.resolve(service_name)

        # Ensure service node exists
        self.graph.upsert_service(canonical_id, service_name)

        # Use a stable incident_id for the signal
        incident_event_id = signal.get("id", f"sig-{incident_ts.isoformat()}")

        window_minutes = 15

        # ── Step 1: Related events ────────────────────────────────────────
        window_start = incident_ts - timedelta(minutes=window_minutes)
        related_services = {canonical_id}
        related_services |= self.graph.depends_on(canonical_id, hops=2)
        related_services |= self.graph.depended_on_by(canonical_id, hops=2)

        related_events = self.store.events_in_window(
            window_start,
            incident_ts,
            canonical_ids=list(related_services),
        )
        # Deduplicate by event id
        seen_ids = set()
        deduped = []
        for ev in related_events:
            eid = ev.get("id", "")
            if eid not in seen_ids:
                seen_ids.add(eid)
                deduped.append(ev)
        related_events = deduped

        # Strip internal bookkeeping keys before returning
        related_events = [_strip_internal(ev) for ev in related_events]

        # ── Step 2: Causal chain ──────────────────────────────────────────
        causal_chain = self.causal_linker.build_causal_chain(
            trigger_canonical=canonical_id,
            incident_ts=incident_ts,
            incident_event_id=incident_event_id,
            window_minutes=window_minutes,
        )

        # ── Step 3: Similar past incidents ───────────────────────────────
        similar_past = self.incident_memory.match(
            trigger_canonical=canonical_id,
            incident_ts=incident_ts,
            signal=dict(signal),
            top_k=5,
            mode=mode,
            window_minutes=window_minutes,
        )

        # ── Step 4: Suggested remediations ───────────────────────────────
        suggested = self.remediation_ranker.rank(
            similar_incidents=similar_past,
            trigger_canonical=canonical_id,
        )

        # ── Step 5: Explain ───────────────────────────────────────────────
        if mode == "deep":
            explain = self._deep_explain(
                signal, canonical_id, service_name, related_events,
                causal_chain, similar_past, suggested, incident_ts
            )
        else:
            explain = self._template_explain(
                signal, canonical_id, service_name, related_events,
                causal_chain, similar_past, suggested, incident_ts
            )

        confidence = _compute_overall_confidence(causal_chain, similar_past)

        return Context(
            related_events=related_events,
            causal_chain=causal_chain,
            similar_past_incidents=similar_past,
            suggested_remediations=suggested,
            confidence=confidence,
            explain=explain,
        )

    def _template_explain(
        self, signal, canonical_id, service_name, related_events,
        causal_chain, similar_past, suggested, incident_ts
    ) -> str:
        # Resolve current name (handles renames)
        current_name = self.topology.current_name(canonical_id) or service_name
        aliases = self.topology.aliases(canonical_id)
        alias_str = ""
        if len(aliases) > 1:
            old_names = [a for a in aliases if a != current_name]
            if old_names:
                alias_str = f" (formerly {', '.join(old_names)})"

        lines = [
            f"Incident on {current_name}{alias_str} at {incident_ts.strftime('%Y-%m-%d %H:%M UTC')}."
        ]

        if causal_chain:
            top = causal_chain[0]
            lines.append(
                f"Most likely cause (confidence {top['confidence']:.0%}): "
                + (top["evidence"][0] if top["evidence"] else "no evidence")
                + "."
            )

        if similar_past:
            top_match = similar_past[0]
            lines.append(
                f"Resembles past incident {top_match['incident_id']} "
                f"({top_match['similarity']:.0%} similarity). "
                + top_match["rationale"]
            )

        if suggested:
            top_rem = suggested[0]
            lines.append(
                f"Suggested action: {top_rem['action']} on {top_rem['target']} "
                f"(historical outcome: {top_rem['historical_outcome']}, "
                f"confidence {top_rem['confidence']:.0%})."
            )

        deploy_count = sum(1 for e in related_events if e.get("kind") == "deploy")
        error_count = sum(1 for e in related_events if e.get("level") in ("error", "critical"))
        lines.append(
            f"Context window: {len(related_events)} related events "
            f"({deploy_count} deploys, {error_count} errors)."
        )

        return " ".join(lines)

    def _deep_explain(
        self, signal, canonical_id, service_name, related_events,
        causal_chain, similar_past, suggested, incident_ts
    ) -> str:
        # Attempt Anthropic API; fall back to template
        try:
            return self._anthropic_explain(
                signal, canonical_id, service_name, causal_chain,
                similar_past, suggested, incident_ts
            )
        except Exception:
            return self._template_explain(
                signal, canonical_id, service_name, related_events,
                causal_chain, similar_past, suggested, incident_ts
            )

    def _anthropic_explain(
        self, signal, canonical_id, service_name,
        causal_chain, similar_past, suggested, incident_ts
    ) -> str:
        import anthropic  # type: ignore

        current_name = self.topology.current_name(canonical_id) or service_name
        aliases = self.topology.aliases(canonical_id)
        alias_str = ""
        if len(aliases) > 1:
            old_names = [a for a in aliases if a != current_name]
            if old_names:
                alias_str = f" (formerly known as {', '.join(old_names)})"

        causal_summary = "\n".join(
            f"  - {e['evidence'][0] if e['evidence'] else 'unknown'} (confidence: {e['confidence']:.0%})"
            for e in causal_chain[:3]
        ) or "  - No causal evidence found"

        past_summary = "\n".join(
            f"  - Incident {m['past_incident_id']}: {m['rationale']}"
            for m in similar_past[:3]
        ) or "  - No similar incidents found"

        rem_summary = "\n".join(
            f"  - {r['action']} on {r['target']} (outcome: {r['historical_outcome']}, conf: {r['confidence']:.0%})"
            for r in suggested[:3]
        ) or "  - No remediations suggested"

        prompt = f"""You are an SRE assistant. Write a concise, actionable incident summary.

Service: {current_name}{alias_str}
Time: {incident_ts.strftime('%Y-%m-%d %H:%M UTC')}
Alert: {signal.get('alert') or signal.get('trigger', 'unknown')}
Severity: {signal.get('severity', 'unknown')}

Causal analysis:
{causal_summary}

Similar past incidents:
{past_summary}

Suggested remediations:
{rem_summary}

Write 2-3 sentences: what happened, why (if known), and what to do next. Be specific."""

        client = anthropic.Anthropic()
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text.strip()


def _strip_internal(ev: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in ev.items() if not k.startswith("_")}
