"""
Handler registry for Munin-Core.

This allows new log parsers ("handlers") to be plugged in without
modifying the core watcher code. To add a new handler:

1. Create a new file in ingestor/handlers/, e.g. `csvlog.py`.
2. Define a class with a `parse(self, file_path: str) -> list[dict]` method.
3. Optionally define a `can_handle(self, path: Path) -> bool` method
   to sanity-check the file before parsing.
4. Decorate the class with @register("<ext>") where <ext> is the file extension.

Example:

    from .registry import register

    @register("csv")
    class CSVLogHandler:
        def can_handle(self, path: Path) -> bool:
            with path.open("r") as f:
                return "," in f.readline()

        def parse(self, file_path: str) -> list[dict]:
            # parse file into event dicts
            return []
"""

from pathlib import Path
from typing import Dict, Type

# Global handler registry: maps extension name → handler class
REGISTRY: Dict[str, Type] = {}

def register(name: str):
    """
    Decorator to register a handler class under a given name.

    Args:
        name (str): File extension or identifier for the handler
                    (e.g. "raw", "csv", "evtx").

    Returns:
        decorator: A class decorator that registers the class in REGISTRY.
    """
    def decorator(cls):
        REGISTRY[name] = cls
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
        except Exception:
            return RawHandler()

    return handler
