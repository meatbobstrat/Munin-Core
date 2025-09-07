import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "memory" / "munin_memory.db"


def ingest(source, level, message, ts=None):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if ts is None:
        ts = datetime.utcnow().isoformat()
    cur.execute(
        "INSERT INTO logs (source, timestamp, level, message) VALUES (?, ?, ?, ?)",
        (source, ts, level, message),
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    if len(sys.argv) == 2:
        # Expecting JSON: {"source": "...", "level": "...", "message": "..."}
        data = json.loads(sys.argv[1])
        ingest(data["source"], data.get("level", "INFO"), data["message"])
        print("Log ingested.")
    else:
        print(
            'Usage: python ingest_log.py \'{"source": "syslog", "level": "INFO", "message": "Test event"}\''
        )
