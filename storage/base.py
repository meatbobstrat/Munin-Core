from abc import ABC, abstractmethod
from typing import Any, Dict, List


class StorageBackend(ABC):
    """Abstract storage backend for Munin-Core."""

    @abstractmethod
    def connect(self):
        """Initialize DB connection and schema if needed."""

    @abstractmethod
    def write_batch(self, events: List[Dict[str, Any]]) -> None:
        """Insert a batch of events."""

    @abstractmethod
    def query_events(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Query events (used by UI API)."""

    @abstractmethod
    def close(self):
        """Close DB connection cleanly."""
