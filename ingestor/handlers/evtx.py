# ingestor/handlers/evtx.py
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .registry import register

logger = logging.getLogger(__name__)

# Try to import python-evtx once, fail gracefully in parse()
try:
    from Evtx.Evtx import Evtx  # type: ignore

    _HAVE_EVTX = True
    _IMPORT_ERR: Exception | None = None
except Exception as e:  # pragma: no cover
    _HAVE_EVTX = False
    _IMPORT_ERR = e


@register("evtx")
class EVTXHandler:
    """
    Windows Event Log (.evtx) handler.

    - `can_handle` checks the EVTX magic header so we can recognize renamed files.
    - `parse` iterates records and emits XML as the `message` field.
      (Pretty safe default; we can later add XML-to-fields mapping.)
    """

    EVTX_MAGIC = b"ElfFile"  # EVTX header (7 bytes incl. trailing NUL usually)

    def can_handle(self, path: Path) -> bool:
        try:
            with path.open("rb") as f:
                sig = f.read(len(self.EVTX_MAGIC))
            return sig == self.EVTX_MAGIC
        except Exception:
            return False

    def parse(self, file_path: str) -> list[dict[str, Any]]:
        if not _HAVE_EVTX:
            raise RuntimeError(
                "python-evtx is not installed; cannot parse .evtx files"
                + (f" ({_IMPORT_ERR})" if _IMPORT_ERR else "")
            )

        events: list[dict[str, Any]] = []
        ingested_at = datetime.now(UTC).isoformat()
        path = Path(file_path)

        try:
            with Evtx(file_path) as evtx:
                for i, record in enumerate(evtx.records(), start=1):
                    try:
                        xml = record.xml()
                    except Exception as rec_err:
                        xml = (
                            f"<Event parse_error='{type(rec_err).__name__}: {rec_err}'>"
                        )

                    events.append(
                        {
                            "source": path.name,
                            "file_type": path.suffix.lower() or ".evtx",
                            "ingest_time": ingested_at,
                            "line_number": i,  # record index as a stable sequence number
                            "message": xml,
                            "tags": "",  # optional: pack event_id/provider into tags later
                        }
                    )

            logger.info("Parsed %d events from %s", len(events), path.name)
            return events

        except Exception as exc:
            logger.error("EVTXHandler failed on %s: %s", file_path, exc, exc_info=True)
            raise
