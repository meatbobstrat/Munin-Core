# scripts/file_watcher.py
import json
import time
import fnmatch
import os
import io
import gzip
from pathlib import Path
from typing import Dict, Optional, List
import requests
from watchdog.observers import Observer as WatchdogObserver   # concrete runtime class
from watchdog.observers.api import BaseObserver 
from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileCreatedEvent
import logging
import sys

logger = logging.getLogger("uvicorn.error")

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "configs" / "ingest.config.json"

# make root importable for normalize/parsers
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))
from normalize import choose_parser, content_hash  # uses parsers/* registry

_observer = WatchdogObserver()  # global to manage lifecycle

# ------------------------ Windows shared-open helpers ------------------------
IS_WINDOWS = os.name == "nt"
if IS_WINDOWS:
    try:
        import msvcrt  # type: ignore
        import win32file  # type: ignore
        import win32con  # type: ignore
        _WIN_OK = True
    except Exception:
        _WIN_OK = False
else:
    _WIN_OK = False

def _open_shared_read(path: Path):
    """Try normal open; on Windows PermissionError, open with shared read flags."""
    try:
        return open(path, "r", encoding="utf-8", errors="ignore")
    except PermissionError as e:
        if not (IS_WINDOWS and _WIN_OK):
            raise
        handle = win32file.CreateFile(
            str(path),
            win32con.GENERIC_READ,
            win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE | win32con.FILE_SHARE_DELETE,
            None,
            win32con.OPEN_EXISTING,
            win32con.FILE_ATTRIBUTE_NORMAL,
            None,
        )
        fd = msvcrt.open_osfhandle(handle.Detach(), os.O_RDONLY)
        return os.fdopen(fd, "r", encoding="utf-8", errors="ignore")

def _open_stream(path: Path):
    if path.suffix.lower() == ".gz":
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", errors="ignore")
    return _open_shared_read(path)

# ------------------------- API readiness wait --------------------------------
def wait_for_api(api_url: str, timeout_s: int = 20) -> bool:
    health = api_url.replace("/ingest/batch", "/health")
    if health == api_url:  # if config had /ingest
        health = api_url.replace("/ingest", "/health")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = requests.get(health, timeout=2)
            if r.ok and r.json().get("status") == "ok":
                return True
        except Exception:
            pass
        time.sleep(0.5)
    logger.warning(f"[Munin] API not ready after {timeout_s}s; continuing anyway")
    return False

# ------------------------ config & state -------------------------------------
def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

class OffsetTracker:
    def __init__(self, state_path: Path):
        self.state_path = state_path
        self.offsets: Dict[str, int] = {}
        if state_path.exists():
            try:
                self.offsets = json.loads(state_path.read_text(encoding="utf-8"))
            except Exception:
                self.offsets = {}

    def get(self, file_path: Path) -> int:
        return int(self.offsets.get(str(file_path), 0))

    def set(self, file_path: Path, offset: int) -> None:
        self.offsets[str(file_path)] = int(offset)
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.offsets, indent=2), encoding="utf-8")
        tmp.replace(self.state_path)

# ---------------------- normalized ingestor ----------------------------------
class NormalizedIngestor(FileSystemEventHandler):
    def __init__(self, cfg: dict):
        self.watch_dir = (REPO_ROOT / cfg["watch_dir"]).resolve()
        self.glob = cfg.get("glob", "*")
        self.api_url = cfg["api_url"]  # should be /ingest/batch
        self.batch_size = int(cfg.get("batch_size", 200))
        self.tracker = OffsetTracker(REPO_ROOT / cfg.get("state_file", ".ingest_state.json"))
        self.parsers: Dict[Path, object] = {}  # cache parser per file

        logger.info(f"[Munin] Priming existing files in {self.watch_dir} (pattern={self.glob})")
        for p in self._existing_files():
            self._process_file(p)

    def _existing_files(self) -> List[Path]:
        return [p for p in self.watch_dir.glob("**/*") if p.is_file() and self._interesting(p)]

    def _interesting(self, p: Path) -> bool:
        # quick extension allowlist (still sniff actual format)
        exts = {".log", ".txt", ".json", ".jsonl", ".csv", ".gz", "", ".log.1"}
        if not fnmatch.fnmatch(p.name, self.glob):
            return False
        return p.suffix.lower() in exts

    def _pick_parser(self, path: Path):
        if path in self.parsers:
            return self.parsers[path]
        try:
            with _open_stream(path) as f:
                sample = f.read(2048)
        except Exception:
            sample = ""
        parser = choose_parser(sample, str(path))
        self.parsers[path] = parser
        return parser

    def _post_batch(self, events: list) -> None:
        backoff = 0.5
        for attempt in range(4):
            try:
                r = requests.post(self.api_url, json={"events": events}, timeout=10)
                r.raise_for_status()
                return
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(backoff); backoff *= 2

    def _process_file(self, path: Path) -> None:
        try:
            path = path.resolve()
            if not self._interesting(path):
                return

            # gz are treated as read-only blobs (no offsets)
            gz_mode = path.suffix.lower() == ".gz"

            try:
                size = path.stat().st_size
            except FileNotFoundError:
                return

            last = 0 if gz_mode else self.tracker.get(path)
            if size < last:
                last = 0
            if size == last and not gz_mode:
                return

            events, count = [], 0
            parser = self._pick_parser(path)

            with _open_stream(path) as f:
                # seek if we can (not for gz)
                try:
                    if last:
                        f.seek(last)
                except Exception:
                    pass

                i = 0
                for line in f:
                    i += 1
                    ev = parser.parse_line(line, i, str(path))
                    if not ev:
                        continue
                    ev["content_hash"] = content_hash(ev)
                    events.append(ev)
                    count += 1
                    if len(events) >= self.batch_size:
                        self._post_batch(events); events.clear()

                # save offset if supported
                if not gz_mode:
                    try:
                        pos = f.tell()
                        self.tracker.set(path, pos)
                    except Exception:
                        pass

            if events:
                self._post_batch(events)

            if count:
                logger.info(f"[Munin] Ingested {count} new line(s) from {path.name}")

        except Exception as e:
            logger.error(f"[Munin] Failed processing {path}: {e}")

    # watchdog hooks
    def on_modified(self, event):
        if isinstance(event, FileModifiedEvent) and not event.is_directory:
            self._process_file(Path(event.src_path))

    def on_created(self, event):
        if isinstance(event, FileCreatedEvent) and not event.is_directory:
            p = Path(event.src_path)
            if self._interesting(p):
                if p.suffix.lower() != ".gz":
                    try:
                        self.tracker.set(p.resolve(), 0)
                    except Exception:
                        pass
                self._process_file(p)

# ---------------------- lifecycle --------------------------------------------
def start_watcher() -> None:
    """Start the file watcher in-process (called from FastAPI lifespan)."""
    global _observer
    if _observer is not None:
        return  # already running

    cfg = load_config()
    # Wait for API to be ready
    wait_for_api(cfg.get("api_url", "http://127.0.0.1:8000/ingest/batch"), timeout_s=cfg.get("api_wait", 20))

    watch_dir = (REPO_ROOT / cfg["watch_dir"]).resolve()
    watch_dir.mkdir(parents=True, exist_ok=True)

    handler = NormalizedIngestor(cfg)
    _observer = WatchdogObserver()
    _observer.schedule(handler, str(watch_dir), recursive=True)
    _observer.start()
    logger.info(f"[Munin] Watching: {watch_dir} (pattern={cfg.get('glob','*')}) → {cfg['api_url']}")

def stop_watcher() -> None:
    """Stop the file watcher (called from FastAPI lifespan)."""
    global _observer
    if _observer is not None:
        try:
            _observer.stop()
            _observer.join()
        finally:
            _observer = None
            logger.info("[Munin] Watcher stopped")
