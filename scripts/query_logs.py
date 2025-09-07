import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "memory" / "munin_memory.db"


def query(source=None, limit=10):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if source:
        cur.execute(
            "SELECT timestamp, level, message FROM logs WHERE source=? ORDER BY id DESC LIMIT ?",
            (source, limit),
        )
    else:
        cur.execute("SELECT timestamp, level, message FROM logs ORDER BY id DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows


if __name__ == "__main__":
    source = sys.argv[1] if len(sys.argv) > 1 else None
    rows = query(source)
    for ts, level, msg in rows:
        print(f"[{ts}] {level}: {msg}")
