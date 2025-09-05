# scripts/file_watcher.py
import json
import time
import fnmatch
from pathlib import Path
from typing import Dict, Optional
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileCreatedEvent
import logging

logger = logging.getLogger("uvicorn.error")

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "configs" / "ingest.config.json"

_observer: Optional[Observer] = None  # global to manage lifecycle


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
        self.state_path.write_text(json.dumps(self.offsets, indent=2), encoding="utf-8")


class LogIngestor(FileSystemEventHandler):
    def __init__(self, cfg: dict):
        self.watch_dir = (REPO_ROOT / cfg["watch_dir"]).resolve()
        self.glob = cfg.get("glob", "*.log")
        self.source = cfg.get("source", "syslog")
        self.level = cfg.get("level", "INFO")
        self.api_url = cfg["api_url"]
        self.tracker = OffsetTracker(REPO_ROOT / cfg.get("state_file", ".ingest_state.json"))

        logger.info(f"[Munin] Priming existing files in {self.watch_dir} (pattern={self.glob})")
        for p in self.watch_dir.glob(self.glob):
            self._ingest_new_lines(p)

    def _matches(self, path: Path) -> bool:
        return fnmatch.fnmatch(path.name, self.glob)  # filename-only

    def _post_line(self, line: str) -> None:
        payload = {"source": self.source, "level": self.level, "message": line}
        r = requests.post(self.api_url, json=payload, timeout=5)
        r.raise_for_status()

    def _ingest_new_lines(self, path: Path) -> None:
        try:
            path = path.resolve()
            if not self._matches(path):
                return

            last = self.tracker.get(path)
            size = path.stat().st_size
            if size < last:  # rotated/truncated
                last = 0
            if size == last:
                return

            count = 0
            with path.open("r", encoding="utf-8", errors="ignore") as f:
                f.seek(last)
                for line in f:
                    line = line.rstrip("\r\n")
                    if not line.strip():
                        continue
                    try:
                        self._post_line(line)
                        count += 1
                    except Exception as e:
                        logger.warning(f"[Munin] POST failed for {path.name}: {e}")
                self.tracker.set(path, f.tell())

            if count:
                logger.info(f"[Munin] Ingested {count} new line(s) from {path.name}")

        except FileNotFoundError:
            pass
        except Exception as e:
            logger.error(f"[Munin] Failed to ingest from {path}: {e}")

    # watchdog hooks
    def on_modified(self, event):
        if isinstance(event, FileModifiedEvent) and not event.is_directory:
            self._ingest_new_lines(Path(event.src_path))

    def on_created(self, event):
        if isinstance(event, FileCreatedEvent) and not event.is_directory:
            p = Path(event.src_path)
            if self._matches(p):
                self.tracker.set(p.resolve(), 0)
                self._ingest_new_lines(p)


def start_watcher() -> None:
    """Start the file watcher in-process (called from FastAPI lifespan)."""
    global _observer
    if _observer is not None:
        return  # already running

    cfg = load_config()
    watch_dir = (REPO_ROOT / cfg["watch_dir"]).resolve()
    watch_dir.mkdir(parents=True, exist_ok=True)

    handler = LogIngestor(cfg)
    _observer = Observer()
    _observer.schedule(handler, str(watch_dir), recursive=False)
    _observer.start()
    logger.info(f"[Munin] Watching: {watch_dir} (pattern={cfg.get('glob','*.log')}) → {cfg['api_url']}")


def stop_watcher() -> None:
    """Stop the file watcher (called from FastAPI lifespan)."""
    global _observer
    if _observer is not None:
        _observer.stop()
        _observer.join()
        _observer = None
        logger.info("[Munin] Watcher stopped")
