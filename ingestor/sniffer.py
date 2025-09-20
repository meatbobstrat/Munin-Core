import logging
from pathlib import Path

from ingestor.handlers.evtx import EVTXHandler
from ingestor.handlers.raw import RawHandler  # fallback
from ingestor.handlers.registry import REGISTRY

logger = logging.getLogger(__name__)


def sniff_file(path: Path, sample_size: int = 5):
    """
    Pick the best handler for the given file.
    - Reads the first `sample_size` lines as a sample.
    - Returns a handler instance.
    """
    try:
        with path.open("rb") as bf:
            magic = bf.read(7)
        if magic == EVTXHandler.EVTX_MAGIC:
            return EVTXHandler()
    except Exception:
        pass

    try:
        sample_lines = []
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for _ in range(sample_size):
                line = f.readline()
                if not line:
                    break
                sample_lines.append(line.strip())

        sample_text = "\n".join(sample_lines)

        best_handler_cls: type | None = None
        best_conf = 0.0

        # Let each registered handler sniff
        for name, handler_cls in REGISTRY.items():
            handler = handler_cls()
            if hasattr(handler, "sniff"):
                try:
                    conf = handler.sniff(sample_text, str(path))
                    if conf > best_conf:
                        best_conf = conf
                        best_handler_cls = handler_cls
                except Exception as e:
                    logger.debug(
                        "Handler %s sniff failed on %s: %s", name, path.name, e
                    )

        if best_handler_cls:
            logger.debug(
                "Sniffer selected %s for %s (conf=%.2f)",
                best_handler_cls.__name__,
                path.name,
                best_conf,
            )
            return best_handler_cls()

        # Fallback: file had content but no handler matched → RawHandler
        if sample_text:
            logger.debug("No handler matched %s; falling back to RawHandler", path.name)
            return RawHandler()

        # If the file was empty/unreadable, return None → quarantine
        logger.warning("Could not read %s; returning None (quarantine)", path.name)
        return None

    except Exception as e:
        logger.error("Sniffer failed on %s: %s", path.name, e, exc_info=True)
        return None
