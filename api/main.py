# api/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime, timezone
from typing import Optional, List, Literal, Dict, Any
from contextlib import asynccontextmanager
import sqlite3, json

# watcher lifecycle
from scripts.file_watcher import start_watcher, stop_watcher

# DB helpers
from .utils.db import get_conn, ensure_initialized

# ----- logging -----
import logging
logger = logging.getLogger("uvicorn.error")
logger.info("[Munin] >>> MAIN.PY loaded <<<")

# ----- extra schema for normalized/batched ingestion -----
def ensure_normalized_schema() -> None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS normalized_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          inserted_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          source_path TEXT NOT NULL,
          source_type TEXT,
          line_number INTEGER,
          event_time TEXT,
          level TEXT,
          message TEXT,
          attrs TEXT,
          raw_excerpt TEXT,
          content_hash TEXT
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_norm_time ON normalized_events (event_time);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_norm_src  ON normalized_events (source_path);")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_norm_hash ON normalized_events (content_hash);")
        conn.commit()

# ----- lifespan (startup/shutdown) -----
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    ensure_initialized()          # your existing logs table, etc.
    ensure_normalized_schema()    # new normalized_events table
    logger.info("[Munin] DB initialized (logs + normalized_events)")

    start_watcher()
    logger.info("[Munin] File watcher started")
    yield
    # Shutdown
    try:
        stop_watcher()
        logger.info("[Munin] File watcher stopped")
    except Exception as e:
        logger.warning(f"[Munin] stop_watcher error: {e}")

app = FastAPI(
    title="Munin-Core API",
    version="0.1.0",
    lifespan=lifespan,
)

# ----- Schemas -----
class IngestItem(BaseModel):
    source: str = Field(..., min_length=1, max_length=128)
    level: Optional[Literal["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]] = "INFO"
    message: str = Field(..., min_length=1)

class LogItem(BaseModel):
    timestamp: str
    level: Optional[str]
    message: str
    source: str

class NormalizedEventModel(BaseModel):
    source_path: str
    source_type: Optional[str] = None
    line_number: Optional[int] = None
    event_time: Optional[str] = None
    level: Optional[str] = None
    message: str
    attrs: Optional[Dict[str, Any]] = None
    raw_excerpt: Optional[str] = None
    content_hash: str

class BatchIngest(BaseModel):
    events: List[NormalizedEventModel]

# ----- Routes -----
@app.get("/health")
def health():
    try:
        with get_conn() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        return {"status": "degraded", "error": str(e)}

# Legacy single-line ingest (kept for compatibility)
@app.post("/ingest", response_model=dict)
def ingest(item: IngestItem):
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    try:
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO logs (source, timestamp, level, message) VALUES (?, ?, ?, ?)",
                (item.source, ts, item.level, item.message),
            )
            conn.commit()
        return {"ok": True, "timestamp": ts}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

# NEW: batched, normalized ingest
@app.post("/ingest/batch")
def ingest_batch(payload: BatchIngest):
    inserted = 0
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            for ev in payload.events:
                try:
                    cur.execute("""
                    INSERT OR IGNORE INTO normalized_events
                    (source_path, source_type, line_number, event_time, level,
                     message, attrs, raw_excerpt, content_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        ev.source_path, ev.source_type, ev.line_number, ev.event_time,
                        ev.level, ev.message, json.dumps(ev.attrs or {}, ensure_ascii=False),
                        ev.raw_excerpt, ev.content_hash
                    ))
                    inserted += cur.rowcount
                except Exception:
                    # skip bad rows and continue
                    pass
            conn.commit()
        return {"ok": True, "inserted": inserted, "received": len(payload.events)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

@app.get("/logs", response_model=List[LogItem])
def get_logs(source: Optional[str] = None, limit: int = 10):
    try:
        with get_conn() as conn:
            if source:
                cur = conn.execute(
                    "SELECT timestamp, level, message, source FROM logs "
                    "WHERE source=? ORDER BY id DESC LIMIT ?",
                    (source, limit),
                )
            else:
                cur = conn.execute(
                    "SELECT timestamp, level, message, source FROM logs "
                    "ORDER BY id DESC LIMIT ?",
                    (limit,),
                )
            rows = [LogItem(**dict(r)) for r in cur.fetchall()]
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

# NEW: query structured/normalized events
@app.get("/events")
def list_events(
    source: Optional[str] = None,
    level: Optional[str] = None,
    limit: int = 200
):
    try:
        with get_conn() as conn:
            q = "SELECT * FROM normalized_events"
            clauses, args = [], []
            if source:
                clauses.append("source_path LIKE ?"); args.append(f"%{source}%")
            if level:
                clauses.append("COALESCE(level,'') = ?"); args.append(level.upper())
            if clauses:
                q += " WHERE " + " AND ".join(clauses)
            q += " ORDER BY COALESCE(event_time, inserted_at) DESC LIMIT ?"
            args.append(limit)
            cur = conn.execute(q, args)
            rows = [dict(r) for r in cur.fetchall()]
        return {"events": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
