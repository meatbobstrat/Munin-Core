from pathlib import Path
import sqlite3
from datetime import datetime, timezone

# <repo root>/memory/munin_memory.db
DB_PATH = Path(__file__).resolve().parents[2] / "memory" / "munin_memory.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    timestamp TEXT NOT NULL,  -- ISO UTC
    level TEXT,
    message TEXT
);
"""

ET_SQL = """
CREATE TABLE IF NOT EXISTS echotime (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time TEXT NOT NULL  -- ISO UTC
);
"""

def ensure_initialized():
    """Create DB file and required tables if missing; set ET 0.0.0 if not set."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(SCHEMA_SQL)
        cur.execute(ET_SQL)
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
