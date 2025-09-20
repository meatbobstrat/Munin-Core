"""
Handler registry for Munin-Core.

This allows new log parsers ("handlers") to be plugged in without
modifying the core watcher code. To add a new handler:

1. Create a new file in ingestor/handlers/, e.g. `csvlog.py`.
2. Define a class with:
      - parse(self, file_path: str) -> list[dict]
      - sniff(self, sample: str, filename: str) -> float  # optional
3. Optionally define a `can_handle(self, path: Path) -> bool` method.
4. Decorate the class with @register("<ext>") where <ext> is the file extension.

Example:

    from .registry import register

    @register("csv")
    class CSVLogHandler:
        def sniff(self, sample: str, filename: str) -> float:
            return 0.8 if "," in sample else 0.0

        def parse(self, file_path: str) -> list[dict]:
            return []
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Global handler registry: maps extension name → handler class
REGISTRY: dict[str, type] = {}


def register(name: str):
    """
    Decorator to register a handler class under a given name.

    Args:
        name (str): File extension or identifier for the handler
                    (e.g. "raw", "csv", "evtx").
    """

    def decorator(cls):
        REGISTRY[name.lower()] = cls
        return cls

    return decorator


def get_handler_for(path: Path):
    """
    Look up a handler for the given file path by extension,
    and run `can_handle` if defined. Falls back to raw.
    """
    from ingestor.handlers.raw import RawHandler  # local import to avoid cycles

    ext = path.suffix.lower().lstrip(".") or "raw"
    handler_cls = REGISTRY.get(ext, REGISTRY.get("raw", RawHandler))
    handler = handler_cls()

    if hasattr(handler, "can_handle"):
        try:
            if not handler.can_handle(path):
                return RawHandler()
        except Exception as e:
            logger.warning("can_handle check failed for %s: %s", handler, e)
            return RawHandler()

    return handler


def sniff_best_handler(sample: str, filename: str) -> object | None:
    """
    Iterate all registered handlers, ask each for a confidence score,
    and return the handler instance with the highest score.
    """
    from ingestor.handlers.raw import RawHandler

    best_score = 0.0
    best_handler: object | None = None

    for name, handler_cls in REGISTRY.items():
        handler = handler_cls()
        sniff = getattr(handler, "sniff", None)
        if sniff:
            try:
                score = sniff(sample, filename)
                if score > best_score:
                    best_score, best_handler = score, handler
            except Exception as e:
                logger.debug("Sniff failed for %s: %s", name, e)

    # Always guarantee *something*
    return best_handler or RawHandler()
