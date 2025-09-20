# normalize.py
from __future__ import annotations

import hashlib
import json
from typing import Any

# Support both "package" and "flat script" imports
try:  # when imported as part of a package (e.g., munin.normalize)
    from .parsers import REGISTRY  # type: ignore
except Exception:  # when imported top-level (scripts add repo root to sys.path)
    from parsers import REGISTRY  # type: ignore


def choose_parser(sample: str, filename: str):
    """Pick the highest-confidence parser from the registry."""
    if not REGISTRY:
        raise RuntimeError("No parsers registered")
    scored = sorted(
        ((p.sniff(sample, filename), p) for p in REGISTRY),
        key=lambda x: x[0],
        reverse=True,
    )
    return scored[0][1]


def content_hash(ev: dict[str, Any]) -> str:
    """Stable hash for idempotency."""
    key = {
        "source_path": ev.get("source_path"),
        "event_time": ev.get("event_time"),
        "level": ev.get("level"),
        "message": ev.get("message"),
        "attrs": ev.get("attrs"),
    }
    return hashlib.sha256(json.dumps(key, sort_keys=True, default=str).encode("utf-8")).hexdigest()
