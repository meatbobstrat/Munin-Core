# ingestor/handlers/raw.py
import os
from datetime import datetime, timezone


class RawHandler:
    """Fallback handler for unknown log formats.
    Reads file as plain text and ensures every line has schema-compatible fields.
    """

    def parse(self, file_path: str) -> list[dict]:
        events = []
        ingested_at = datetime.now(timezone.utc).isoformat()

        if not os.path.exists(file_path):
            return events

        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                for i, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue

                    events.append({
                        "source": os.path.basename(file_path),                   # schema: source
                        "file_type": os.path.splitext(file_path)[1].lower() or "raw",
                        "ingest_time": ingested_at,                             # schema: ingest_time
                        "line_number": i,                                       # schema: line_number
                        "message": line,                                        # schema: message
                        "tags": ""                                              # schema: tags
                    })
        except Exception as e:
            print(f"[ERROR] RawHandler failed on {file_path}: {e}")

        return events
