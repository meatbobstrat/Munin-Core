import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .registry import register

logger = logging.getLogger(__name__)

@register("raw")
class RawHandler:
    """
    Fallback handler for unknown log formats.

    Reads the file as plain text and ensures every line
    is normalized into schema-compatible fields.
    """

    def parse(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Parse a file line by line into normalized events.

        Args:
            file_path (str): Path to the file to parse.

        Returns:
            List[Dict[str, Any]]: A list of normalized event dictionaries.
        """
        events: List[Dict[str, Any]] = []
        ingested_at = datetime.now(timezone.utc).isoformat()

        path = Path(file_path)
        if not path.exists():
            logger.warning("File does not exist: %s", file_path)
            return events

        try:
            with path.open("r", encoding="utf-8", errors="ignore") as f:
                for i, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue

                    events.append({
                        "source": path.name,
                        "file_type": path.suffix.lower() or "raw",
                        "ingest_time": ingested_at,
                        "line_number": i,
                        "message": line,
                        "tags": "",
                    })
            logger.info("Parsed %d events from %s", len(events), path.name)
        except Exception as exc:  # TODO: narrow exception types
            logger.error("RawHandler failed on %s: %s", file_path, exc, exc_info=True)

        return events
