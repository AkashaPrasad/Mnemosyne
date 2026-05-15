"""
TopologyTracker — bidirectional alias registry for service rename handling.

Services are identified by a stable canonical_id regardless of name changes.
Rename chains (A→B→C) resolve to the same canonical_id throughout.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Dict, List, Optional, Set


class TopologyTracker:
    def __init__(self) -> None:
        # name → canonical_id (always current)
        self._name_to_canonical: Dict[str, str] = {}
        # canonical_id → set of all historical names
        self._canonical_to_names: Dict[str, Set[str]] = {}
        # canonical_id → ordered list of (name, valid_from, valid_to)
        self._rename_history: Dict[str, List[tuple]] = {}

    def register_service(self, name: str, canonical_id: Optional[str] = None) -> str:
        """Register a new service name. Returns the canonical_id."""
        if name in self._name_to_canonical:
            return self._name_to_canonical[name]
        cid = canonical_id or str(uuid.uuid4())
        self._name_to_canonical[name] = cid
        self._canonical_to_names.setdefault(cid, set()).add(name)
        self._rename_history.setdefault(cid, []).append((name, None, None))
        return cid

    def register_rename(self, old_name: str, new_name: str, ts: datetime) -> str:
        """
        Record a service rename. Both old and new names resolve to the same
        canonical_id. Returns the canonical_id.
        """
        # Ensure old_name is already tracked
        if old_name not in self._name_to_canonical:
            self.register_service(old_name)

        cid = self._name_to_canonical[old_name]

        # Close the old name's validity window
        history = self._rename_history[cid]
        for i, (n, vfrom, vto) in enumerate(history):
            if n == old_name and vto is None:
                history[i] = (n, vfrom, ts)
                break

        # Register the new name under the same canonical_id
        self._name_to_canonical[new_name] = cid
        self._canonical_to_names[cid].add(new_name)
        self._rename_history[cid].append((new_name, ts, None))

        return cid

    def resolve(self, name: str) -> str:
        """
        Resolve a service name to its canonical_id.
        Creates a new entity if the name has never been seen.
        """
        if name not in self._name_to_canonical:
            return self.register_service(name)
        return self._name_to_canonical[name]

    def aliases(self, canonical_id: str) -> List[str]:
        """Return all known names for a canonical_id."""
        return list(self._canonical_to_names.get(canonical_id, set()))

    def current_name(self, canonical_id: str) -> Optional[str]:
        """Return the most recent name for a canonical_id."""
        history = self._rename_history.get(canonical_id, [])
        for name, _, valid_to in reversed(history):
            if valid_to is None:
                return name
        return None

    def historical_names(self, canonical_id: str, before_ts: datetime) -> List[str]:
        """Return names that were active before a given timestamp."""
        names = []
        for name, valid_from, valid_to in self._rename_history.get(canonical_id, []):
            started = valid_from is None or valid_from <= before_ts
            still_valid = valid_to is None or valid_to > before_ts
            if started and still_valid:
                names.append(name)
        return names

    def all_canonical_ids(self) -> List[str]:
        return list(self._canonical_to_names.keys())

    def name_to_canonical_snapshot(self) -> Dict[str, str]:
        return dict(self._name_to_canonical)
