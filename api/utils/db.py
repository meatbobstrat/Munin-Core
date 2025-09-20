import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# <repo root>/memory/munin_memory.db
DB_PATH = Path(__file__).resolve().parents[2] / "memory" / "munin_memory.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    -- event_time_utc from the log line (may be wrong/unknown)
    timestamp TEXT NOT NULL,           -- ISO UTC
    level TEXT,
    message TEXT
);
"""

# Alerts for surfacing storage/quarantine/backpressure signals
ALERTS_SQL = """
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    level TEXT NOT NULL,               -- info|warning|error
    code TEXT NOT NULL,                -- STORAGE_HIGH | QUARANTINE_NEW | INGESTION_BACKPRESSURE | ...
    message TEXT NOT NULL,
    created_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    metadata_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_utc DESC);
"""

ET_SQL = """
CREATE TABLE IF NOT EXISTS echotime (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time TEXT NOT NULL  -- ISO UTC
);
"""


def _column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())


def _ensure_ingest_time_column(conn: sqlite3.Connection) -> None:
    """
    Make sure logs.ingest_time_utc exists and auto-defaults.
    We prefer a real column so we can prune by COALESCE(event_time, ingest_time).
    """
    if not _column_exists(conn, "logs", "ingest_time_utc"):
        # Add the column
        conn.execute("""
            ALTER TABLE logs
            ADD COLUMN ingest_time_utc TEXT NOT NULL
            DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        """)
        # Optional index to speed up time-based pruning
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_ingest_time ON logs(ingest_time_utc)")


def ensure_initialized():
    """Create DB file and required tables if missing; set ET 0.0.0 if not set."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        # Core tables
        cur.execute(SCHEMA_SQL)
        cur.execute(ET_SQL)
        cur.executescript(ALERTS_SQL)

        # Migrate: add logs.ingest_time_utc if missing
        _ensure_ingest_time_column(conn)

        # Set ET epoch if not present
        cur.execute("SELECT COUNT(*) FROM echotime")
        if cur.fetchone()[0] == 0:
            cur.execute(
                "INSERT INTO echotime (start_time) VALUES (?)",
                (datetime.now(timezone.utc).isoformat(timespec="seconds"),),
            )
        conn.commit()


def get_conn() -> sqlite3.Connection:
    """Return a connection; ensures DB/schema exist."""
    if not DB_PATH.exists():
        ensure_initialized()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ---------- helpers we’ll use next steps ----------


def emit_alert(level: str, code: str, message: str, metadata_json: str | None = None) -> None:
    """
    Insert a row into alerts table. metadata_json may be a JSON-serialized string or None.
    """
    ensure_initialized()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO alerts(level, code, message, metadata_json) VALUES (?,?,?,?)",
            (level, code, message, metadata_json),
        )
        conn.commit()


def get_db_size_bytes() -> int:
    """
    Total size of SQLite DB including WAL/SHM if present.
    """
    base = str(DB_PATH)
    size = 0
    for p in (base, f"{base}-wal", f"{base}-shm"):
        if os.path.exists(p):
            size += os.path.getsize(p)
    return size
