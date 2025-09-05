from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime, timezone
from typing import Optional, List, Literal
from contextlib import asynccontextmanager
from scripts.file_watcher import start_watcher, stop_watcher
from .utils.db import get_conn
from .utils.db import get_conn, ensure_initialized

#Temp adding logging on startup for testing
import logging
logger = logging.getLogger("uvicorn.error")
logger.info("[Munin] >>> MAIN.PY loaded <<<")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    ensure_initialized()
    start_watcher()
    yield
    # Shutdown
    stop_watcher()
    app = FastAPI(title="Munin-Core API", version="0.1.0", lifespan=lifespan)


app = FastAPI(
    title="Munin-Core API",
    version="0.1.0",
    lifespan=lifespan
)
    
class IngestItem(BaseModel):
    source: str = Field(..., min_length=1, max_length=128)
    level: Optional[Literal["DEBUG","INFO","WARN","ERROR","CRITICAL"]] = "INFO"
    message: str = Field(..., min_length=1)

class LogItem(BaseModel):
    timestamp: str
    level: Optional[str]
    message: str
    source: str

@app.get("/health")
def health():
    # Quick sanity DB check
    try:
        with get_conn() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        return {"status": "degraded", "error": str(e)}

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

@app.get("/logs", response_model=List[LogItem])
def get_logs(source: Optional[str] = None, limit: int = 10):
    try:
        with get_conn() as conn:
            if source:
                cur = conn.execute(
                    "SELECT timestamp, level, message, source FROM logs WHERE source=? "
                    "ORDER BY id DESC LIMIT ?",
                    (source, limit),
                )
            else:
                cur = conn.execute(
                    "SELECT timestamp, level, message, source FROM logs ORDER BY id DESC LIMIT ?",
                    (limit,),
                )
            rows = [LogItem(**dict(r)) for r in cur.fetchall()]
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
