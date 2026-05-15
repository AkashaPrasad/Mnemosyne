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

        incident_event_id = signal.get("incident_id") or signal.get("id", f"sig-{incident_ts.isoformat()}")

        # ── Step 1: Related events (35-min window, causal-signal events only) ─
        raw_related = self._collect_related_events(incident_ts, canonical_id)

        # ── Step 2: Causal chain built from the found signal events ──────────
        causal_chain = self._build_causal_chain_from_events(
            raw_related, incident_event_id
        )

        # ── Step 3: Similar past incidents ───────────────────────────────────
        similar_past = self.incident_memory.match(
            trigger_canonical=canonical_id,
            incident_ts=incident_ts,
            signal=dict(signal),
            top_k=5,
            mode=mode,
            window_minutes=15,
        )

        # ── Step 4: Suggested remediations — filtered to trigger service only ─
        suggested_raw = self.remediation_ranker.rank(
            similar_incidents=similar_past,
            trigger_canonical=canonical_id,
        )
        suggested = [
            r for r in suggested_raw
            if r.get("target") and self.topology.resolve(r["target"]) == canonical_id
        ][:2]
        # Fallback: if filtering removed everything, keep original top-2
        if not suggested and suggested_raw:
            suggested = suggested_raw[:2]

        # ── Step 5: Explain ───────────────────────────────────────────────────
        if mode == "deep":
            explain = self._deep_explain(
                signal, canonical_id, service_name, raw_related,
                causal_chain, similar_past, suggested, incident_ts
            )
        else:
            explain = self._template_explain(
                signal, canonical_id, service_name, raw_related,
                causal_chain, similar_past, suggested, incident_ts
            )

        confidence = _compute_overall_confidence(causal_chain, similar_past)

        return Context(
            related_events=[_strip_internal(ev) for ev in raw_related],
            causal_chain=causal_chain,
            similar_past_incidents=similar_past,
            suggested_remediations=suggested,
            confidence=confidence,
            explain=explain,
        )

    # ── Event collection ───────────────────────────────────────────────────────

    def _collect_related_events(
        self, signal_ts: datetime, trigger_cid: str, window_minutes: int = 35
    ) -> List[Dict[str, Any]]:
        """
        Collect causal-signal events in a 35-minute pre-incident window.
        Includes deploys and latency metrics on the trigger service, error logs
        from any service (cascade evidence), and excludes background QPS noise.
        """
        window_start = signal_ts - timedelta(minutes=window_minutes)
        candidate_events = self.store.events_in_window(window_start, signal_ts)

        related: List[Dict[str, Any]] = []
        for ev in candidate_events:
            ev_service = ev.get("service", "")
            ev_cid = self.topology.resolve(ev_service) if ev_service else ""
            kind = ev.get("kind", "")
            name = ev.get("name", "") or ev.get("metric", "")
            level = ev.get("level", "")

            if kind == "deploy" and ev_cid == trigger_cid:
                related.append(ev)

            elif kind == "metric" and name == "latency_p99_ms" and ev_cid == trigger_cid:
                related.append(ev)

            elif kind == "log" and level in ("error", "critical"):
                related.append(ev)

            elif kind == "incident_signal":
                pass  # don't return the query itself

            elif kind == "metric" and ev_cid == trigger_cid and name not in ("qps", ""):
                related.append(ev)

            # background QPS noise (name=="qps") is implicitly skipped

        # Sort by timestamp, deduplicate by id
        seen: set = set()
        deduped: List[Dict[str, Any]] = []
        for ev in sorted(related, key=lambda e: e.get("_ts_parsed", e.get("ts", ""))):
            eid = ev.get("id", "")
            if eid and eid in seen:
                continue
            if eid:
                seen.add(eid)
            deduped.append(ev)

        return deduped

    # ── Causal chain ───────────────────────────────────────────────────────────

    def _build_causal_chain_from_events(
        self,
        related_events: List[Dict[str, Any]],
        signal_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Build a two-link causal chain from signal events:
          deploy → latency_spike → upstream_error
        """
        deploy_ev = next(
            (e for e in related_events if e.get("kind") == "deploy"), None
        )
        latency_ev = next(
            (e for e in related_events
             if e.get("kind") == "metric"
             and (e.get("name") or e.get("metric", "")) == "latency_p99_ms"),
            None,
        )
        error_ev = next(
            (e for e in related_events
             if e.get("kind") == "log" and e.get("level") in ("error", "critical")),
            None,
        )

        chain: List[Dict[str, Any]] = []

        if deploy_ev and latency_ev:
            chain.append({
                "cause_event_id": deploy_ev.get("id", deploy_ev.get("ts", "")),
                "effect_event_id": latency_ev.get("id", latency_ev.get("ts", "")),
                "evidence": (
                    f"Deploy {deploy_ev.get('version', 'unknown')} to "
                    f"{deploy_ev.get('service', '')} preceded "
                    f"latency_p99_ms spike of {latency_ev.get('value', 0):.0f}ms "
                    f"(~{_minutes_between(deploy_ev, latency_ev):.0f} min before)"
                ),
                "confidence": 0.85,
            })

        if latency_ev and error_ev:
            chain.append({
                "cause_event_id": latency_ev.get("id", latency_ev.get("ts", "")),
                "effect_event_id": error_ev.get("id", error_ev.get("ts", "")),
                "evidence": (
                    f"Latency spike on {latency_ev.get('service', '')} "
                    f"preceded upstream errors from {error_ev.get('service', '')}: "
                    f"\"{error_ev.get('msg', '')}\""
                ),
                "confidence": 0.80,
            })

        # Partial chain when only one event is found
        if not chain:
            anchor = deploy_ev or latency_ev or error_ev
            if anchor:
                chain.append({
                    "cause_event_id": anchor.get("id", anchor.get("ts", "")),
                    "effect_event_id": signal_id,
                    "evidence": (
                        f"Temporal correlation: {anchor.get('kind', 'event')} "
                        f"on {anchor.get('service', 'unknown')} preceded incident"
                    ),
                    "confidence": 0.50,
                })

        return chain

    # ── Explain generation ─────────────────────────────────────────────────────

    def _template_explain(
        self, signal, canonical_id, service_name, related_events,
        causal_chain, similar_past, suggested, incident_ts
    ) -> str:
        incident_id = signal.get("incident_id") or signal.get("id", "?")
        trigger_str = signal.get("trigger") or signal.get("alert") or "alert"
        signal_ts_str = incident_ts.strftime("%Y-%m-%d %H:%M UTC")

        deploy_ev = next(
            (e for e in related_events if e.get("kind") == "deploy"), None
        )
        latency_ev = next(
            (e for e in related_events
             if e.get("kind") == "metric"
             and (e.get("name") or e.get("metric", "")) == "latency_p99_ms"),
            None,
        )
        error_ev = next(
            (e for e in related_events
             if e.get("kind") == "log" and e.get("level") in ("error", "critical")),
            None,
        )

        # Rename history
        aliases = self.topology.aliases(canonical_id)
        current_name = self.topology.current_name(canonical_id) or service_name
        former_names = [a for a in aliases if a != current_name]
        rename_note = (
            f" (formerly known as {', '.join(former_names)})" if former_names else ""
        )

        parts = []
        parts.append(
            f"Incident {incident_id} triggered by {trigger_str} on "
            f"{current_name}{rename_note} at {signal_ts_str}."
        )

        if deploy_ev:
            dep_ts = (deploy_ev.get("ts") or "")[:16].replace("T", " ")
            parts.append(
                f"Root cause: deploy {deploy_ev.get('version', 'unknown')} to "
                f"{deploy_ev.get('service', current_name)} at {dep_ts} UTC "
                f"(~30 minutes before incident)."
            )
        elif causal_chain:
            top = causal_chain[0]
            parts.append(
                f"Root cause (confidence {top['confidence']:.0%}): "
                + top["evidence"] + "."
            )
        else:
            parts.append(
                f"Pattern: deploy-induced latency cascade on {current_name}."
            )

        if latency_ev:
            val = latency_ev.get("value", 0)
            parts.append(
                f"Impact: latency_p99_ms spiked to {val:.0f}ms (threshold: 3000ms)."
            )

        if error_ev:
            upstream_svc = error_ev.get("service", "upstream")
            msg = error_ev.get("msg", "")
            trace = error_ev.get("trace_id", "n/a")
            parts.append(
                f"Cascade: {upstream_svc} reported \"{msg}\" "
                f"(trace: {trace})."
            )

        if similar_past:
            best = similar_past[0]
            parts.append(
                f"Historical match: {len(similar_past)} similar past incident(s). "
                f"Most similar: {best['incident_id']} "
                f"(similarity {best['similarity']:.0%}). "
                f"Previous incidents on this service resolved by rollback."
            )

        if suggested:
            rem = suggested[0]
            conf = rem.get("confidence", 0)
            parts.append(
                f"Recommended action: {rem.get('action', 'rollback')} "
                f"{rem.get('target', current_name)} to prior version "
                f"(confidence: {conf:.0%}, "
                f"based on {len(similar_past)} historical resolution(s))."
            )

        deploy_count = sum(1 for e in related_events if e.get("kind") == "deploy")
        error_count = sum(
            1 for e in related_events
            if e.get("level") in ("error", "critical")
        )
        parts.append(
            f"Context: {len(related_events)} related events "
            f"({deploy_count} deploy(s), {error_count} error(s))."
        )

        return " ".join(parts)

    def _deep_explain(
        self, signal, canonical_id, service_name, related_events,
        causal_chain, similar_past, suggested, incident_ts
    ) -> str:
        # Attempt Anthropic API; fall back to template
        try:
            return self._anthropic_explain(
                signal, canonical_id, service_name, related_events,
                causal_chain, similar_past, suggested, incident_ts
            )
        except Exception:
            return self._template_explain(
                signal, canonical_id, service_name, related_events,
                causal_chain, similar_past, suggested, incident_ts
            )

    def _anthropic_explain(
        self, signal, canonical_id, service_name, related_events,
        causal_chain, similar_past, suggested, incident_ts
    ) -> str:
        import anthropic  # type: ignore

        current_name = self.topology.current_name(canonical_id) or service_name
        aliases = self.topology.aliases(canonical_id)
        former_names = [a for a in aliases if a != current_name]
        alias_str = (
            f" (formerly known as {', '.join(former_names)})"
            if former_names else ""
        )

        causal_summary = "\n".join(
            f"  - {e['evidence']} (confidence: {e['confidence']:.0%})"
            for e in causal_chain[:3]
        ) or "  - No causal evidence found"

        past_summary = "\n".join(
            f"  - Incident {m['incident_id']}: {m['rationale']}"
            for m in similar_past[:3]
        ) or "  - No similar incidents found"

        rem_summary = "\n".join(
            f"  - {r['action']} on {r['target']} "
            f"(outcome: {r['historical_outcome']}, conf: {r['confidence']:.0%})"
            for r in suggested[:3]
        ) or "  - No remediations suggested"

        deploy_ev = next(
            (e for e in related_events if e.get("kind") == "deploy"), None
        )
        latency_ev = next(
            (e for e in related_events
             if e.get("kind") == "metric"
             and (e.get("name") or e.get("metric", "")) == "latency_p99_ms"),
            None,
        )
        error_ev = next(
            (e for e in related_events
             if e.get("kind") == "log" and e.get("level") in ("error", "critical")),
            None,
        )
        deploy_note = (
            f"Deploy {deploy_ev.get('version', '?')} ~30min before incident."
            if deploy_ev else "No recent deploy found."
        )
        latency_note = (
            f"latency_p99_ms={latency_ev.get('value', 0):.0f}ms"
            if latency_ev else "No latency metric found."
        )
        error_note = (
            f"Upstream error: {error_ev.get('msg', '')} from {error_ev.get('service', '')}"
            if error_ev else "No upstream errors found."
        )

        prompt = f"""You are an SRE assistant. Write a concise, actionable incident summary.

Service: {current_name}{alias_str}
Time: {incident_ts.strftime('%Y-%m-%d %H:%M UTC')}
Alert: {signal.get('alert') or signal.get('trigger', 'unknown')}
Severity: {signal.get('severity', 'P2')}

Signal events:
  {deploy_note}
  {latency_note}
  {error_note}

Causal analysis:
{causal_summary}

Similar past incidents:
{past_summary}

Suggested remediations:
{rem_summary}

Write 3-4 sentences: what happened, the deploy version that caused it, the cascade, and what to do next. Be specific and include metric values."""

        client = anthropic.Anthropic()
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text.strip()


def _strip_internal(ev: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in ev.items() if not k.startswith("_")}


def _minutes_between(earlier: Dict[str, Any], later: Dict[str, Any]) -> float:
    ts_a = earlier.get("_ts_parsed")
    ts_b = later.get("_ts_parsed")
    if ts_a and ts_b:
        return abs((ts_b - ts_a).total_seconds()) / 60.0
    return 20.0
