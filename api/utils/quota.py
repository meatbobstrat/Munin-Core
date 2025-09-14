import os
import sqlite3
from .db import DB_PATH, get_db_size_bytes, emit_alert

# configurable thresholds
HIGH_WM_GIB = float(os.getenv("HIGH_WM_GIB", "9"))   # pause if bigger than this
LOW_WM_GIB  = float(os.getenv("LOW_WM_GIB",  "8"))   # prune until under this
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "7"))
DELETE_BATCH = int(os.getenv("PRUNE_DELETE_BATCH", "50000"))

def enforce_quota_loop() -> bool:
    """
    Try to prune old rows until DB <= LOW_WM_GIB.
    Returns True if under control, False if still too big.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        while get_db_size_bytes() > HIGH_WM_GIB * (1 << 30):
            cur = conn.cursor()
            cur.execute("BEGIN")
            # delete oldest rows based on either event time OR ingest time
            cur.execute(
                """
                WITH cutoff(ts) AS (
                  SELECT datetime('now', ?)
                )
                DELETE FROM logs
                 WHERE COALESCE(timestamp, ingest_time_utc)
                       < (SELECT ts FROM cutoff)
                 LIMIT ?;
                """,
                (f"-{RETENTION_DAYS} days", DELETE_BATCH),
            )
            deleted = cur.rowcount if cur.rowcount is not None else 0
            cur.execute("COMMIT")

            if deleted <= 0:
                # nothing left old enough to prune
                emit_alert(
                    "error", "STORAGE_HIGH",
                    "DB above high watermark and no old rows to delete",
                    f'{{"high_gib": {HIGH_WM_GIB}, "low_gib": {LOW_WM_GIB}}}'
                )
                return False
        return get_db_size_bytes() <= LOW_WM_GIB * (1 << 30)
    finally:
        conn.close()
