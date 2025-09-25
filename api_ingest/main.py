from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import FastAPI, Query

from storage.provider import get_provider

app = FastAPI()
provider = get_provider()


@app.post("/ingest")
def ingest(data: Dict[str, Any]):
    """
    Ingest a batch of events.
    Expects payload like: {"log_type": "evtx", "events": [ {...}, {...} ]}
    """
    log_type = data.get("log_type", "default")
    events = data.get("events", [])
    if not events:
        return {"status": "no events"}

    # Ensure a table exists for this log type
    provider.ensure_table(log_type, schema={})
    count = provider.insert_events(log_type, events)
    return {"status": "ok", "count": count, "log_type": log_type}


@app.get("/events")
def get_events(
    log_type: Optional[str] = Query(None, description="Filter by log type"),
    limit: int = Query(100, ge=1, le=1000, description="Max rows to return"),
    since: Optional[str] = Query(None, description="ISO timestamp for lower bound"),
    level: Optional[str] = Query(None, description="Filter by level"),
    source: Optional[str] = Query(None, description="Filter by source"),
    order_desc: bool = Query(True, description="Sort newest first"),
):
    """
    Fetch recent events. Supports filtering by log_type, level, source, and timestamp.
    """
    since_dt: Optional[datetime] = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except Exception:
            return {"status": "error", "error": f"Invalid since format: {since}"}

    filters: Dict[str, Any] = {}
    if level:
        filters["level"] = level
    if source:
        filters["source"] = source

    rows = provider.query_events(
        log_type=log_type,
        limit=limit,
        since=since_dt,
        filters=filters,
        order_desc=order_desc,
    )
    return {"status": "ok", "count": len(rows), "events": rows}


@app.get("/healthz")
def healthcheck():
    """
    Lightweight health check for provider.
    """
    return provider.health()
