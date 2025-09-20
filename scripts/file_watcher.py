import logging
import os
import shutil
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

import requests
from watchdog.events import FileSystemEventHandler

from ingestor.handlers.raw import RawHandler
from ingestor.handlers.registry import get_handler_for
from ingestor.sniffer import sniff_file

# -----------------------
# Watchdog observer selection (polling is more reliable on Docker/Windows bind mounts)
# -----------------------
USE_POLLING = os.getenv("WATCH_USE_POLLING", "1").lower() in ("1", "true", "yes")
if USE_POLLING:
    from watchdog.observers.polling import PollingObserver as Observer

    OBSERVER_NAME = "PollingObserver"
else:
    from watchdog.observers import Observer

    OBSERVER_NAME = "Observer"

# -----------------------
# Logging setup
# -----------------------
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# -----------------------
# Config (directory paths from env, with sane defaults)
# -----------------------
INGEST_API_URL = os.getenv("INGEST_API_URL", "http://api_ingest:8000/ingest")
SOURCE_NAME = os.getenv("SOURCE_NAME", "default_source")

INCOMING_DIR = Path(os.getenv("WATCH_DIR", "/app/incoming"))
PROCESSING_DIR = Path(os.getenv("PROCESSING_DIR", "/app/processing"))
QUARANTINE_DIR = Path(os.getenv("QUARANTINE_DIR", "/app/quarantine"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DB_PATH = DATA_DIR / "ingestor.db"

# Retry worker tuning
RETRY_INTERVAL_SEC = int(os.getenv("RETRY_INTERVAL_SEC", "3"))
RETRY_BATCH_SIZE = int(os.getenv("RETRY_BATCH_SIZE", "500"))
FILE_STABLE_WAIT = 0.5


# -----------------------
# SQLite queue
# -----------------------
def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
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


def buffer_events(events: list[dict[str, Any]]) -> None:
    if not events:
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO pending_events (source, file_type, ingest_time, line_number, message, tags)
        VALUES (:source, :file_type, :ingest_time, :line_number, :message, :tags)
    """,
        events,
    )
    conn.commit()
    conn.close()


def fetch_pending_batch(limit: int) -> list[dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
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
    return [dict(zip(keys, row, strict=False)) for row in rows]


def delete_pending_ids(ids: list[int]) -> None:
    if not ids:
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executemany("DELETE FROM pending_events WHERE id = ?", [(i,) for i in ids])
    conn.commit()
    conn.close()


def add_quarantine_index(filename: str, reason: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO quarantine_index (filename, reason, quarantined_at) VALUES (?, ?, datetime('now'))",
        (filename, reason),
    )
    conn.commit()
    conn.close()


# -----------------------
# API send
# -----------------------
def send_batch_to_api(batch_rows: list[dict[str, Any]]) -> None:
    if not batch_rows:
        return
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
        "source": SOURCE_NAME,
        "file_type": events[0].get("file_type", "unknown"),
        "ingest_time": events[0].get("ingest_time"),
        "events": events,
    }
    response = requests.post(INGEST_API_URL, json=payload, timeout=10)
    response.raise_for_status()


# -----------------------
# Retry worker
# -----------------------
def retry_worker(stop_evt: threading.Event) -> None:
    while not stop_evt.is_set():
        try:
            batch = fetch_pending_batch(RETRY_BATCH_SIZE)
            if not batch:
                time.sleep(RETRY_INTERVAL_SEC)
                continue
            send_batch_to_api(batch)
            delete_pending_ids([r["id"] for r in batch])
            logger.info("Successfully flushed %d events", len(batch))
        except requests.RequestException as rexc:
            logger.warning("Network/API error during retry: %s", rexc)
            time.sleep(RETRY_INTERVAL_SEC)
        except Exception as exc:
            logger.error("Unexpected error in retry_worker: %s", exc, exc_info=True)
            time.sleep(RETRY_INTERVAL_SEC)


# -----------------------
# Helpers
# -----------------------
def is_file_stable(path: Path, wait: float = FILE_STABLE_WAIT) -> bool:
    try:
        s1 = path.stat().st_size
        time.sleep(wait)
        s2 = path.stat().st_size
        return s1 == s2
    except FileNotFoundError:
        return False


def parse_file_to_events(file_path: Path) -> list[dict[str, Any]]:
    parser = sniff_file(file_path)
    if parser:
        return parser.parse(str(file_path))
    try:
        handler = get_handler_for(file_path)
        return handler.parse(str(file_path))
    except Exception:
        return RawHandler().parse(str(file_path))


# -----------------------
# Watcher
# -----------------------
class LogHandler(FileSystemEventHandler):
    def on_created(self, event) -> None:
        if not event.is_directory:
            self.process_file(Path(event.src_path))

    def on_moved(self, event) -> None:
        if not event.is_directory:
            dest_path = getattr(event, "dest_path", event.src_path)
            target = Path(dest_path)
            if target.parent == INCOMING_DIR:
                self.process_file(target)

    def process_file(self, src: Path) -> None:
        while not is_file_stable(src):
            time.sleep(FILE_STABLE_WAIT)

        PROCESSING_DIR.mkdir(parents=True, exist_ok=True)
        dest = PROCESSING_DIR / src.name
        try:
            shutil.move(str(src), str(dest))
        except Exception as e:
            logger.error("Move failed %s → %s: %s", src, dest, e)
            return

        try:
            events = parse_file_to_events(dest)
            if not events:
                raise ValueError("Parser returned no events (after sniff fallback)")
            buffer_events(events)
            dest.unlink(missing_ok=True)
            logger.info("Buffered %d events from %s; file deleted", len(events), dest.name)
        except Exception as e:
            QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
            qpath = QUARANTINE_DIR / dest.name
            try:
                shutil.move(str(dest), str(qpath))
                note = qpath.with_suffix(qpath.suffix + ".note")
                with open(note, "w", encoding="utf-8") as f:
                    f.write(f"Failed to parse {dest.name}\n")
                    f.write(f"Reason: {type(e).__name__}: {e}\n")
                    if "binary" in str(e).lower():
                        f.write("Hint: file appears to be binary/unsupported format.\n")
                    elif "no events" in str(e).lower():
                        f.write("Hint: file was readable but produced no events.\n")
                    else:
                        f.write("Hint: unexpected parser error.\n")
                add_quarantine_index(qpath.name, str(e))
                logger.warning("Quarantined %s due to parse error: %s", dest.name, e)
            except Exception as qe:
                logger.critical("Could not quarantine %s: %s", dest, qe, exc_info=True)


# -----------------------
# Main
# -----------------------
if __name__ == "__main__":
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSING_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    init_db()
    stop_evt = threading.Event()
    t = threading.Thread(target=retry_worker, args=(stop_evt,), daemon=True)
    t.start()

    handler = LogHandler()
    logger.info(
        "Config: incoming=%s, processing=%s, quarantine=%s, db=%s",
        INCOMING_DIR,
        PROCESSING_DIR,
        QUARANTINE_DIR,
        DB_PATH,
    )
    logger.info("Watcher: using %s", OBSERVER_NAME)

    observer = Observer()
    observer.schedule(handler, str(INCOMING_DIR), recursive=False)
    observer.start()

    for fname in os.listdir(INCOMING_DIR):
        fpath = INCOMING_DIR / fname
        if fpath.is_file():
            handler.process_file(fpath)

    logger.info("Watching %s (buffer→delete; quarantine on parse failure)", INCOMING_DIR)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_evt.set()
        observer.stop()
    observer.join()
