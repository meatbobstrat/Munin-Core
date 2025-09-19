# ingestor/handlers/registry.py
"""
Handler registry for Munin-Core.

This allows new log parsers ("handlers") to be plugged in without
modifying the core watcher code. To add a new handler:

1. Create a new file in ingestor/handlers/, e.g. `csvlog.py`.
2. Define a class with a `parse(self, file_path: str) -> list[dict]` method.
3. Decorate the class with @register("<ext>") where <ext> is the file extension.

Example:

    from .registry import register

    @register("csv")
    class CSVLogHandler:
        def parse(self, file_path: str) -> list[dict]:
            # parse file into event dicts
            return []

The file watcher will automatically dispatch to the right handler
based on file extension, falling back to "raw" if no handler is found.
"""

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

