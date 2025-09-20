# ingestor/handlers/csvlog.py
import csv
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .registry import register

logger = logging.getLogger(__name__)


@register("csvlog")
class CsvLogHandler:
    """
    Handler for CSV-formatted log files.

    Each row is normalized into schema-compatible fields.
    The first row is assumed to be a header.
    """

    def parse(self, file_path: str) -> list[dict[str, Any]]:
        """
        Parse a CSV file into normalized events.

        Args:
            file_path (str): Path to the CSV file to parse.

        Returns:
            List[Dict[str, Any]]: A list of normalized event dictionaries.
        """
        events: list[dict[str, Any]] = []
        ingested_at = datetime.now(UTC).isoformat()

        path = Path(file_path)
        if not path.exists():
            logger.warning("File does not exist: %s", file_path)
            return events

        try:
            with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader, start=1):
                    events.append(
                        {
                            "source": path.name,
                            "file_type": path.suffix.lower() or "csv",
                            "ingest_time": ingested_at,
                            "line_number": i,
                            "message": row,
                            "tags": "",
                        }
                    )
            logger.info("Parsed %d events from %s", len(events), path.name)
        except Exception as exc:  # TODO: narrow exception types
            logger.error("CsvLogHandler failed on %s: %s", file_path, exc, exc_info=True)

        return events
