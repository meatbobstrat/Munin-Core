# scripts/file_watcher.py
"""
Munin-Core file watcher (lossless ingest, delete-after-ingest)

- Watches only the configured "incoming" directory.
- On file create/move-in:
    1) Wait until file size is stable (Windows-friendly)
    2) Atomically move -> processing/
    3) Compute SHA256; skip if file_manifest already has it
    4) Insert file_manifest(status='processing')
    5) Stream lines into event_occurrence (no dedup; one row per line)
    6) Mark manifest committed; delete file
    7) Enforce storage quota
- On any error: move file to quarantine/ and append a .reason.txt explaining why
- Quarantine is auto-purged by age and total size cap.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from watchdog.events import (
    FileCreatedEvent,
    FileModifiedEvent,
    FileMovedEvent,
    FileSystemEventHandler,
)
from watchdog.observers import Observer

# --- Local modules ---
# Expect: storage/quota.py with enforce_quota(conn, cfg)
try:
    from storage.quota import enforce_quota  # type: ignore
except Exception as e:  # pragma: no cover
    print("ERROR: storage/quota.py not found or import failed:", e, file=sys.stderr)
    raise

LOG = logging.getLogger("munin.watcher")
logging.basicConfig(
    level=os.environ.get("MUNIN_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)

# Global DB lock (serialize SQLite operations across threads)
_DB_LOCK = threading.RLock()


# ---------------------------
# Config
# ---------------------------


@dataclass
class IngestConfig:
    watch_dir: Path
    processing_dir: Path
    quarantine_dir: Path
    db_path: Path
    batch_size: int = 1000
    source_host: str = "unknown-host"
    source_app: str = "unknown-app"
    # Quota knobs (mirrors your quota.py expectations)
    DB_PATH: str = ""
    DB_HIGH_WATERMARK: int = 9 * 1024 * 1024 * 1024  # 9 GiB
    DB_LOW_WATERMARK: int = 8 * 1024 * 1024 * 1024  # 8 GiB
    RETENTION_MIN_DAYS: int = 7
    # Quarantine policy
    QUARANTINE_MAX_BYTES: int = 1 * 1024 * 1024 * 1024  # 1 GiB
    QUARANTINE_RETENTION_DAYS: int = 7  # 7 days

    @staticmethod
    def load(path: Path) -> "IngestConfig":
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)

        # Base dir: try explicit base_dir, then watch_dir, else current folder
        base = Path(raw.get("base_dir") or raw.get("watch_dir") or ".").resolve()

        watch_dir = Path(raw.get("watch_dir", base / "incoming")).resolve()
        processing_dir = Path(raw.get("processing_dir", base / "processing")).resolve()
        quarantine_dir = Path(raw.get("quarantine_dir", base / "quarantine")).resolve()

        # DB path can be overridden by env
        db_path = Path(os.environ.get("MUNIN_DB_PATH", "db/munin.sqlite")).resolve()

        cfg = IngestConfig(
            watch_dir=watch_dir,
            processing_dir=processing_dir,
            quarantine_dir=quarantine_dir,
            db_path=db_path,
            batch_size=int(raw.get("batch_size", 1000)),
            source_host=str(raw.get("source_host", "unknown-host")),
            source_app=str(raw.get("source_app", "unknown-app")),
        )

        # Quota knobs (env > json > defaults)
        cfg.DB_PATH = str(cfg.db_path)
        cfg.DB_HIGH_WATERMARK = int(
            os.environ.get(
                "MUNIN_DB_HIGH_WATERMARK", raw.get("db_high_watermark", cfg.DB_HIGH_WATERMARK)
            )
        )
        cfg.DB_LOW_WATERMARK = int(
            os.environ.get(
                "MUNIN_DB_LOW_WATERMARK", raw.get("db_low_watermark", cfg.DB_LOW_WATERMARK)
            )
        )
        cfg.RETENTION_MIN_DAYS = int(
            os.environ.get(
                "MUNIN_RETENTION_MIN_DAYS", raw.get("retention_min_days", cfg.RETENTION_MIN_DAYS)
            )
        )

        # Quarantine policy (env > json > defaults)
        cfg.QUARANTINE_MAX_BYTES = int(
            os.environ.get(
                "MUNIN_QUARANTINE_MAX_BYTES",
                raw.get("quarantine_max_bytes", cfg.QUARANTINE_MAX_BYTES),
            )
        )
        cfg.QUARANTINE_RETENTION_DAYS = int(
            os.environ.get(
                "MUNIN_QUARANTINE_RETENTION_DAYS",
                raw.get("quarantine_retention_days", cfg.QUARANTINE_RETENTION_DAYS),
            )
        )

        return cfg


# ---------------------------
# DB helpers
# ---------------------------


def connect(db_path: Path) -> sqlite3.Connection:
    # Allow use across the watchdog thread; serialize with _DB_LOCK
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    with _DB_LOCK:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def ensure_ingest_schema(conn: sqlite3.Connection) -> None:
    with _DB_LOCK:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS file_manifest (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            rel_path          TEXT NOT NULL,
            sha256            TEXT NOT NULL,
            bytes             INTEGER NOT NULL,
            mtime_utc         TEXT NOT NULL,
            source_host       TEXT NOT NULL,
            source_app        TEXT NOT NULL,
            started_at_utc    TEXT NOT NULL,
            ingested_at_utc   TEXT,
            status            TEXT NOT NULL CHECK (status IN ('processing','committed','deleted','error'))
        );
        """)
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_file_manifest_sha256 ON file_manifest(sha256);"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS ix_file_manifest_status ON file_manifest(status);")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS ix_file_manifest_started ON file_manifest(started_at_utc);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS ix_file_manifest_ingested ON file_manifest(ingested_at_utc);"
        )

        conn.execute("""
        CREATE TABLE IF NOT EXISTS event_occurrence (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id          INTEGER NOT NULL REFERENCES file_manifest(id) ON DELETE CASCADE,
            line_no          INTEGER NOT NULL,
            byte_offset      INTEGER NOT NULL,
            event_time_utc   TEXT,
            level            TEXT,
            message          TEXT NOT NULL,
            attrs_json       TEXT,
            source_host      TEXT NOT NULL,
            source_app       TEXT NOT NULL,
            content_sha256   TEXT NOT NULL,
            maybe_duplicate  INTEGER NOT NULL DEFAULT 0
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS ix_event_file_id ON event_occurrence(file_id);")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS ix_event_content_sha ON event_occurrence(content_sha256);"
        )
        conn.commit()


def file_manifest_has_sha(conn: sqlite3.Connection, sha256_hex: str) -> bool:
    with _DB_LOCK:
        cur = conn.execute("SELECT 1 FROM file_manifest WHERE sha256 = ? LIMIT 1;", (sha256_hex,))
        return cur.fetchone() is not None


def file_manifest_insert(
    conn: sqlite3.Connection,
    *,
    rel_path: str,
    sha256_hex: str,
    bytes_: int,
    mtime_iso: str,
    source_host: str,
    source_app: str,
) -> int:
    with _DB_LOCK:
        cur = conn.execute(
            """
            INSERT INTO file_manifest
                (rel_path, sha256, bytes, mtime_utc, source_host, source_app, started_at_utc, status)
            VALUES (?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ','now'), 'processing')
            """,
            (rel_path, sha256_hex, bytes_, mtime_iso, source_host, source_app),
        )
        return int(cur.lastrowid)


def file_manifest_mark(conn: sqlite3.Connection, file_id: int, status: str) -> None:
    if status not in ("processing", "committed", "deleted", "error"):
        raise ValueError(f"invalid status {status}")
    with _DB_LOCK:
        if status == "committed":
            conn.execute(
                "UPDATE file_manifest SET status=?, ingested_at_utc=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?;",
                (status, file_id),
            )
        else:
            conn.execute("UPDATE file_manifest SET status=? WHERE id=?;", (status, file_id))
        conn.commit()


def insert_occurrence_batch(conn: sqlite3.Connection, rows: list[tuple]) -> None:
    if not rows:
        return
    with _DB_LOCK:
        conn.executemany(
            """
            INSERT INTO event_occurrence
                (file_id, line_no, byte_offset, event_time_utc, level, message, attrs_json,
                 source_host, source_app, content_sha256, maybe_duplicate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            rows,
        )


# ---------------------------
# File helpers / quarantine
# ---------------------------

IGNORE_SUFFIXES = {".tmp", ".partial", ".swp", ".crdownload"}


def sha256_file(path: Path, bufsize: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(bufsize)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def iso_from_mtime(p: Path) -> str:
    ts = p.stat().st_mtime
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def content_hash(level: Optional[str], message: str, attrs_json: Optional[str]) -> str:
    level_n = (level or "").strip().upper()
    attrs_n = (attrs_json or "").strip()
    h = hashlib.sha256()
    h.update(level_n.encode("utf-8"))
    h.update(b"|")
    h.update(message.encode("utf-8"))
    h.update(b"|")
    h.update(attrs_n.encode("utf-8"))
    return h.hexdigest()


def parse_stream_simple(
    stream: Iterable[bytes],
) -> Iterable[tuple[Optional[str], Optional[str], str, Optional[str]]]:
    """
    Minimal parser: one log line per row.
    Returns tuples: (event_time_utc, level, message, attrs_json)
    """
    for raw in stream:
        msg = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        yield None, None, msg, None


def stream_bytes(path: Path, bufsize: int = 1024 * 64) -> Iterable[bytes]:
    with path.open("rb") as f:
        while True:
            chunk = f.readline()  # line-oriented to compute byte offsets
            if not chunk:
                break
            yield chunk


def wait_until_stable(path: Path, checks: int = 3, interval: float = 0.2) -> bool:
    """Return True when file size remains unchanged for `checks` consecutive samples."""
    try:
        last = path.stat().st_size
    except FileNotFoundError:
        return False
    stable = 0
    for _ in range(checks * 4):  # soft upper bound
        time.sleep(interval)
        try:
            sz = path.stat().st_size
        except FileNotFoundError:
            return False
        if sz == last:
            stable += 1
            if stable >= checks:
                return True
        else:
            stable = 0
            last = sz
    return False


def move_atomic(src: Path, dst_dir: Path, retries: int = 30, delay: float = 0.1) -> Path:
    """
    Try to rename; on Windows this may fail while another process holds the file.
    Retry a few times; if it still won't move, fall back to copy+unlink.
    """
    dst_dir.mkdir(parents=True, exist_ok=True)
    target = dst_dir / src.name
    if target.exists():
        target = dst_dir / f"{src.stem}.{int(time.time() * 1000)}{src.suffix}"

    for _ in range(retries):
        try:
            return src.rename(target)
        except (PermissionError, OSError):
            time.sleep(delay)
        except FileNotFoundError:
            # Source vanished
            raise

    # Fallback: copy2 then unlink
    shutil.copy2(src, target)
    try:
        src.unlink()
    except Exception:
        pass
    return target


def quarantine_with_reason(src: Path, quarantine_dir: Path, reason: str) -> None:
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    dest = quarantine_dir / src.name
    try:
        src.rename(dest)
    except Exception:
        shutil.copy2(src, dest)
        try:
            src.unlink()
        except Exception:
            pass
    try:
        (quarantine_dir / f"{dest.name}.reason.txt").write_text(reason + "\n", encoding="utf-8")
    except Exception:
        LOG.exception("Failed writing reason for %s", dest)


# ---- Quarantine purge (age + size cap) ----


def _dir_size_bytes(path: Path) -> int:
    total = 0
    for p in path.glob("*"):
        if p.is_file():
            try:
                total += p.stat().st_size
            except Exception:
                pass
    return total


def _paired_delete(files: list[Path]) -> None:
    """Delete files and their matching '.reason.txt' if present."""
    for f in files:
        try:
            f.unlink(missing_ok=True)
        except Exception:
            pass
        reason = f.with_name(f.name + ".reason.txt")
        try:
            reason.unlink(missing_ok=True)
        except Exception:
            pass


def purge_quarantine(cfg: IngestConfig, log: logging.Logger = LOG) -> None:
    q = cfg.quarantine_dir
    if not q.exists():
        return

    # 1) Delete by age
    cutoff = time.time() - (cfg.QUARANTINE_RETENTION_DAYS * 86400)
    old = []
    for p in q.glob("*"):
        if p.is_file() and not p.name.endswith(".reason.txt"):
            try:
                if p.stat().st_mtime < cutoff:
                    old.append(p)
            except Exception:
                pass
    if old:
        log.info("Quarantine: deleting %d expired file(s)", len(old))
        _paired_delete(old)

    # 2) Enforce size cap
    size = _dir_size_bytes(q)
    if size <= cfg.QUARANTINE_MAX_BYTES:
        return

    files = [p for p in q.glob("*") if p.is_file() and not p.name.endswith(".reason.txt")]
    files.sort(key=lambda p: p.stat().st_mtime)  # oldest first
    to_delete: list[Path] = []
    for p in files:
        if size <= cfg.QUARANTINE_MAX_BYTES:
            break
        try:
            size -= p.stat().st_size
        except Exception:
            pass
        to_delete.append(p)

    if to_delete:
        log.warning("Quarantine over cap; deleting %d oldest file(s)", len(to_delete))
        _paired_delete(to_delete)


# ---------------------------
# File processing
# ---------------------------


def process_one_file(cfg: IngestConfig, conn: sqlite3.Connection, incoming_file: Path) -> None:
    if not incoming_file.is_file():
        return
    if incoming_file.suffix.lower() in IGNORE_SUFFIXES:
        return

    LOG.info("Discovered file: %s", incoming_file)

    # Wait briefly for writers to finish (Windows-friendly)
    wait_until_stable(incoming_file, checks=3, interval=0.15)

    # Step 1: move to processing/
    try:
        proc_path = move_atomic(incoming_file, cfg.processing_dir)
    except Exception as e:
        LOG.exception("Failed to move %s to processing/: %s", incoming_file, e)
        return

    try:
        # Step 2: compute SHA and skip duplicates
        file_sha = sha256_file(proc_path)
        if file_manifest_has_sha(conn, file_sha):
            LOG.warning("Duplicate file by SHA %s — deleting %s", file_sha, proc_path)
            try:
                proc_path.unlink()
            except Exception:
                LOG.exception("Failed to delete duplicate file %s", proc_path)
            return

        # Step 3: insert manifest row
        rel_path = str(proc_path.relative_to(cfg.processing_dir.parent))  # relative under logs/
        file_id = file_manifest_insert(
            conn,
            rel_path=rel_path,
            sha256_hex=file_sha,
            bytes_=proc_path.stat().st_size,
            mtime_iso=iso_from_mtime(proc_path),
            source_host=cfg.source_host,
            source_app=cfg.source_app,
        )
        LOG.info("file_manifest id=%s for %s", file_id, proc_path)

        # Step 4: stream lines -> batch insert
        rows: list[tuple] = []
        byte_offset = 0
        line_no = 0
        with proc_path.open("rb") as f:
            while True:
                chunk = f.readline()
                if not chunk:
                    break
                line_no += 1
                msg = chunk.decode("utf-8", errors="replace").rstrip("\r\n")
                event_time_utc, level, message, attrs_json = (None, None, msg, None)
                c_hash = content_hash(level, message, attrs_json)
                rows.append(
                    (
                        file_id,
                        line_no,
                        byte_offset,
                        event_time_utc,
                        level,
                        message,
                        attrs_json,
                        cfg.source_host,
                        cfg.source_app,
                        c_hash,
                    )
                )
                byte_offset += len(chunk)

                if len(rows) >= cfg.batch_size:
                    insert_occurrence_batch(conn, rows)
                    conn.commit()
                    rows.clear()

        if rows:
            insert_occurrence_batch(conn, rows)
            conn.commit()

        # Step 5: commit manifest, delete file, enforce quota
        file_manifest_mark(conn, file_id, "committed")
        try:
            proc_path.unlink()
            file_manifest_mark(conn, file_id, "deleted")
        except Exception as e:
            LOG.warning("Committed but could not delete %s now (%s).", proc_path, e)

        ok, freed = enforce_quota(conn, cfg)
        if not ok:
            LOG.error("Quota enforcement failed to drop below low watermark.")
        if freed:
            LOG.info("Quota enforcement freed %s bytes", freed)

    except Exception as e:
        LOG.exception("Error while processing %s: %s", proc_path, e)
        quarantine_with_reason(proc_path, cfg.quarantine_dir, f"Ingest error: {e}")
        try:
            if "file_id" in locals():
                file_manifest_mark(conn, file_id, "error")
        except Exception:
            LOG.exception("Failed to mark manifest error for file id=%s", locals().get("file_id"))
        # keep quarantine tidy
        try:
            purge_quarantine(cfg)
        except Exception:
            LOG.exception("Quarantine purge failed")


# ---------------------------
# Watcher wiring
# ---------------------------


class IncomingHandler(FileSystemEventHandler):
    def __init__(self, cfg: IngestConfig, conn: sqlite3.Connection):
        super().__init__()
        self.cfg = cfg
        self.conn = conn

    def on_created(self, event):
        if isinstance(event, FileCreatedEvent) and not event.is_directory:
            process_one_file(self.cfg, self.conn, Path(event.src_path))

    def on_moved(self, event):
        if isinstance(event, FileMovedEvent) and not event.is_directory:
            dest = Path(event.dest_path)
            try:
                if self.cfg.watch_dir in dest.parents:
                    process_one_file(self.cfg, self.conn, dest)
            except Exception:
                LOG.exception("Error handling moved event for %s", dest)

    def on_modified(self, event):
        if isinstance(event, FileModifiedEvent) and not event.is_directory:
            p = Path(event.src_path)
            try:
                if self.cfg.watch_dir in p.parents and p.stat().st_size < 1_000_000:
                    process_one_file(self.cfg, self.conn, p)
            except Exception:
                pass


def ensure_dirs(cfg: IngestConfig) -> None:
    cfg.watch_dir.mkdir(parents=True, exist_ok=True)
    cfg.processing_dir.mkdir(parents=True, exist_ok=True)
    cfg.quarantine_dir.mkdir(parents=True, exist_ok=True)


def main():
    config_path = Path(
        os.environ.get("MUNIN_INGEST_CONFIG", "configs/ingest.config.json")
    ).resolve()
    if not config_path.exists():
        LOG.error("Missing config file: %s", config_path)
        sys.exit(2)

    cfg = IngestConfig.load(config_path)
    ensure_dirs(cfg)
    purge_quarantine(cfg)

    conn = connect(cfg.db_path)
    ensure_ingest_schema(conn)

    LOG.info("Watcher starting.")
    LOG.info("Watching: %s", cfg.watch_dir)
    LOG.info("Processing: %s", cfg.processing_dir)
    LOG.info("Quarantine: %s", cfg.quarantine_dir)
    LOG.info("DB: %s", cfg.db_path)

    observer = Observer()
    handler = IncomingHandler(cfg, conn)
    observer.schedule(handler, str(cfg.watch_dir), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        LOG.info("Shutting down...")
        observer.stop()
    observer.join()
    conn.close()


# --- Compatibility shims for API startup/teardown ---

_observer: Optional[Observer] = None
_conn: Optional[sqlite3.Connection] = None


def start_watcher(config_path: str | None = None):
    """
    Initialize and start the watchdog observer without blocking the event loop.
    Returns True if started, False if already running.
    """
    global _observer, _conn
    if _observer is not None:
        return False

    cfg_path = Path(os.environ.get("MUNIN_INGEST_CONFIG", "configs/ingest.config.json")).resolve()
    if config_path:
        cfg_path = Path(config_path).resolve()

    if not cfg_path.exists():
        LOG.error("Missing config file: %s", cfg_path)
        raise FileNotFoundError(cfg_path)

    cfg = IngestConfig.load(cfg_path)
    ensure_dirs(cfg)
    purge_quarantine(cfg)

    _conn = connect(cfg.db_path)
    ensure_ingest_schema(_conn)

    LOG.info("Watcher starting (API mode).")
    LOG.info("Watching: %s", cfg.watch_dir)
    LOG.info("Processing: %s", cfg.processing_dir)
    LOG.info("Quarantine: %s", cfg.quarantine_dir)
    LOG.info("DB: %s", cfg.db_path)

    handler = IncomingHandler(cfg, _conn)
    _observer = Observer()
    _observer.schedule(handler, str(cfg.watch_dir), recursive=False)
    _observer.daemon = True
    _observer.start()
    return True


def stop_watcher():
    """Stop observer and close DB if started via start_watcher()."""
    global _observer, _conn
    if _observer:
        LOG.info("Shutting down file watcher (API mode)...")
        _observer.stop()
        _observer.join(timeout=5)
        _observer = None
    if _conn:
        _conn.close()
        _conn = None


if __name__ == "__main__":
    main()
