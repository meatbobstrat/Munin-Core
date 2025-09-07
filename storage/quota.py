from pathlib import Path
import sqlite3, time

def db_size_bytes(db_path: str) -> int:
    p = Path(db_path)
    total = p.stat().st_size
    # include -wal/-shm if present
    for ext in (".wal", ".shm"):
        f = p.with_suffix(p.suffix + ext)
        if f.exists():
            total += f.stat().st_size
    return total

def enforce_quota(conn: sqlite3.Connection, cfg) -> tuple[bool, int]:
    """
    Returns (ok, freed_bytes). ok=False if still above high_watermark after prune.
    """
    size = db_size_bytes(cfg.DB_PATH)
    if size <= cfg.DB_HIGH_WATERMARK: 
        return True, 0

    # don’t delete events newer than retention_min_days
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM event_occurrence
        WHERE event_time_utc IS NOT NULL
          AND event_time_utc < datetime('now', ?)
          AND id IN (
            SELECT id FROM event_occurrence
            ORDER BY event_time_utc ASC, id ASC
            LIMIT 50000
          )
    """, (f"-{int(cfg.RETENTION_MIN_DAYS)} days",))
    conn.commit()

    new_size = db_size_bytes(cfg.DB_PATH)
    if new_size > cfg.DB_LOW_WATERMARK:
        return False, size - new_size
    return True, size - new_size
