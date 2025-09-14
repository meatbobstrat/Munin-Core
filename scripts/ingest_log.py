import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from api.utils.quota import enforce_quota_loop   # 👈 new

DB_PATH = Path(__file__).resolve().parents[1] / "memory" / "munin_memory.db"



def ingest(source, level, message, ts=None):
    # run quota check before writing
    ok = enforce_quota_loop()
    if not ok:
        print("⚠️ storage high, skipping ingest")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if ts is None:
        ts = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(timespec="seconds")

    ingest_time = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(timespec="seconds")

    cur.execute(
        """
        INSERT INTO logs (source, timestamp, level, message, ingest_time_utc)
        VALUES (?, ?, ?, ?, ?)
        """,
        (source, ts, level, message, ingest_time),
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
