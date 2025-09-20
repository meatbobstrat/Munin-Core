import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .registry import register

logger = logging.getLogger(__name__)

LOG_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:,\d{3})?)"
    r"\s*(?P<level>[A-Z]+)?\s*(?P<msg>.*)$"
)


@register("log")
class LogFileHandler:
    """
    Handler for generic `.log` files.
    Tries to parse lines like: 2025-09-20 12:34:56,789 INFO message...
    """

    def sniff(self, sample: str, filename: str) -> float:
        """
        Look for timestamp + log-level pattern in first few lines.
        """
        lines = sample.splitlines()
        hits = 0
        checked = 0
        for line in lines[:10]:
            checked += 1
            if LOG_LINE_RE.match(line.strip()):
                hits += 1
        if checked == 0:
            return 0.0
        return hits / checked

    def parse(self, file_path: str) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        ingested_at = datetime.now(timezone.utc).isoformat()
        path = Path(file_path)

        try:
            with path.open("r", encoding="utf-8", errors="ignore") as f:
                for i, line in enumerate(f, start=1):
                    m = LOG_LINE_RE.match(line.strip())
                    if not m:
                        continue
                    events.append(
                        {
                            "source": path.name,
                            "file_type": "log",
                            "ingest_time": ingested_at,
                            "line_number": i,
                            "message": m.group("msg"),
                            "tags": m.group("level") or "",
                        }
                    )
            logger.info("Parsed %d log events from %s", len(events), path.name)
        except Exception as exc:
            logger.error("LogFileHandler failed on %s: %s", file_path, exc, exc_info=True)

        return events
