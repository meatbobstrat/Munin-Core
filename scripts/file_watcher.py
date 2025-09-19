import os
import shutil
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List

import requests
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from ingestor.handlers import raw  # add others later (csvlog, evtx)

# -----------------------
# Config
# -----------------------
INGEST_API_URL = os.getenv("INGEST_API_URL", "http://api_ingest:8000/ingest")
SOURCE_NAME    = os.getenv("SOURCE_NAME", "default_source")

INCOMING_DIR   = Path("./incoming")
PROCESSING_DIR = Path("./processing")
QUARANTINE_DIR = Path("./quarantine")
DATA_DIR       = Path("/app/data")
DB_PATH        = DATA_DIR / "ingestor.db"

# Retry worker tuning
RETRY_INTERVAL_SEC = int(os.getenv("RETRY_INTERVAL_SEC", "3"))
RETRY_BATCH_SIZE   = int(os.getenv("RETRY_BATCH_SIZE", "500"))

# -----------------------
# SQLite queue
# -----------------------
def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    # Events that parsed OK and are waiting to be sent to API
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pending_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            file_type TEXT NOT NULL,
            ingest_time TEXT NOT NULL,
            line_number INTEGER,
            message TEXT NOT NULL,
            tags TEXT
        )
    """)

    # Quarantined file notes are stored on disk (.note). We keep a table for ops if needed later.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS quarantine_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            reason TEXT NOT NULL,
            quarantined_at TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


def buffer_events(events: List[Dict[str, Any]]) -> None:
    """Write parsed-good events to the pending queue."""
    if not events:
        return
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.executemany(
        """
        INSERT INTO pending_events (source, file_type, ingest_time, line_number, message, tags)
        VALUES (:source, :file_type, :ingest_time, :line_number, :message, :tags)
        """,
        events,
    )
    conn.commit()
    conn.close()


def fetch_pending_batch(limit: int) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute(
        """
        SELECT id, source, file_type, ingest_time, line_number, message, tags
        FROM pending_events
        ORDER BY id ASC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    keys = ["id", "source", "file_type", "ingest_time", "line_number", "message", "tags"]
    return [dict(zip(keys, row)) for row in rows]


def delete_pending_ids(ids: List[int]) -> None:
    if not ids:
        return
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.executemany("DELETE FROM pending_events WHERE id = ?", [(i,) for i in ids])
    conn.commit()
    conn.close()


def add_quarantine_index(filename: str, reason: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute(
        "INSERT INTO quarantine_index (filename, reason, quarantined_at) VALUES (?, ?, datetime('now'))",
        (filename, reason),
    )
    conn.commit()
    conn.close()

# -----------------------
# API send
# -----------------------
def send_batch_to_api(batch_rows: List[Dict[str, Any]]) -> None:
    """Send a batch of events (already parsed) to API. Raises on failure."""
    if not batch_rows:
        return

    # Transform DB rows back into event dicts (drop the 'id')
    events = [
        {
            "source": r["source"],
            "file_type": r["file_type"],
            "ingest_time": r["ingest_time"],
            "line_number": r["line_number"],
            "message": r["message"],
            "tags": r["tags"],
        }
        for r in batch_rows
    ]

    payload = {
        "source": SOURCE_NAME,  # source of the ingestor itself
        "file_type": events[0].get("file_type", "unknown"),
        "ingest_time": events[0].get("ingest_time"),
        "events": events,
    }
    r = requests.post(INGEST_API_URL, json=payload, timeout=10)
    # Optional: honor 429 retry headers later
    r.raise_for_status()

# -----------------------
# Retry worker
# -----------------------
def retry_worker(stop_evt: threading.Event) -> None:
    """Continuously flush pending_events to the API."""
    while not stop_evt.is_set():
        try:
            batch = fetch_pending_batch(RETRY_BATCH_SIZE)
            if not batch:
                time.sleep(RETRY_INTERVAL_SEC)
                continue

            # Attempt send
            send_batch_to_api(batch)
            # On success, delete them
            delete_pending_ids([r["id"] for r in batch])
        except Exception:
            # Back off briefly; we'll retry on next tick
            time.sleep(RETRY_INTERVAL_SEC)

# -----------------------
# Helpers
# -----------------------
def is_file_stable(path: Path, wait: float = 0.5) -> bool:
    """Return True if file size stops changing during a short wait."""
    try:
        s1 = path.stat().st_size
        time.sleep(wait)
        s2 = path.stat().st_size
        return s1 == s2
    except FileNotFoundError:
        return False


def parse_file_to_events(file_path: Path) -> List[Dict[str, Any]]:
    """Dispatch by extension; only raw for now."""
    ext = file_path.suffix.lower()
    handler = {
        # ".csv": csvlog.CSVLogHandler(),
        # ".evtx": evtx.EVTXHandler(),
    }.get(ext, raw.RawHandler())
    return handler.parse(str(file_path))

# -----------------------
# Watcher
# -----------------------
class LogHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        self.process_file(Path(event.src_path))

    def process_file(self, src: Path) -> None:
        """Wait until stable, move to processing, parse, buffer, delete. Quarantine on parse failure."""
        # Wait until writer is done
        while not is_file_stable(src):
            time.sleep(0.5)

        # Move → processing
        PROCESSING_DIR.mkdir(parents=True, exist_ok=True)
        dest = PROCESSING_DIR / src.name
        try:
            shutil.move(str(src), str(dest))
        except Exception as e:
            print(f"[ERROR] Move failed {src} → {dest}: {e}")
            return

        # Parse
        try:
            events = parse_file_to_events(dest)
            if not events:
                raise ValueError("Parser returned no events")
        except Exception as e:
            # Quarantine (no buffering of untrusted data)
            QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
            qpath = QUARANTINE_DIR / dest.name
            try:
                shutil.move(str(dest), str(qpath))
                note = qpath.with_suffix(qpath.suffix + ".note")
                with open(note, "w", encoding="utf-8") as f:
                    f.write(f"Failed to parse {dest.name}\nReason: {e}\n")
                add_quarantine_index(qpath.name, str(e))
                print(f"[QUARANTINE] {dest.name} → {qpath.name}: {e}")
            except Exception as qe:
                print(f"[FATAL] Could not quarantine {dest}: {qe}")
            return

        # Buffer then delete file (we are not a storage system)
        try:
            buffer_events(events)
            dest.unlink(missing_ok=True)
            print(f"[OK] Buffered {len(events)} events from {dest.name}; file deleted")
        except Exception as e:
            # If buffering fails (rare), quarantine the file to avoid data loss
            QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
            qpath = QUARANTINE_DIR / dest.name
            try:
                shutil.move(str(dest), str(qpath))
                note = qpath.with_suffix(qpath.suffix + ".note")
                with open(note, "w", encoding="utf-8") as f:
                    f.write(f"Failed to buffer events from {dest.name}\nReason: {e}\n")
                add_quarantine_index(qpath.name, f"buffer failure: {e}")
                print(f"[QUARANTINE] {dest.name} → {qpath.name}: buffer failure")
            except Exception as qe:
                print(f"[FATAL] Could not quarantine after buffer failure {dest}: {qe}")

# -----------------------
# Main
# -----------------------
if __name__ == "__main__":
    # Ensure dirs
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSING_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    # DB + retry worker
    init_db()
    stop_evt = threading.Event()
    t = threading.Thread(target=retry_worker, args=(stop_evt,), daemon=True)
    t.start()

    # Kick the observer
    handler  = LogHandler()
    observer = Observer()
    observer.schedule(handler, str(INCOMING_DIR), recursive=False)
    observer.start()

    # Trigger watcher for any files already present
    for fname in os.listdir(INCOMING_DIR):
        fpath = INCOMING_DIR / fname
        if fpath.is_file():
            handler.process_file(fpath)

    print(f"Watching {INCOMING_DIR} (buffer→delete; quarantine on parse failure)")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_evt.set()
        observer.stop()
    observer.join()
