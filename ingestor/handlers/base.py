# src/munin/parsers/base.py
from abc import ABC, abstractmethod
from typing import Any


class NormalizedEvent(dict[str, Any]):
    """
    Dict with normalized keys:
    - source_path: str
    - source_type: str (e.g., "json", "syslog", "raw")
    - line_number: int
    - event_time: datetime | str
    - level: str
    - message: str
    - attrs: dict
    - raw_excerpt: str
    """


class Parser(ABC):
    @abstractmethod
    def sniff(self, sample: str, filename: str) -> float:
        """
        Return confidence (0.0–1.0) that this parser can handle the file/line.
        Called with a sample chunk + filename.
        """

    @abstractmethod
    def parse_line(
        self, line: str, line_no: int, filename: str
    ) -> NormalizedEvent | None:
        """
        Parse a single line into a NormalizedEvent.
        Return None if the line should be skipped.
        """


REGISTRY: list[Parser] = []


def register(parser: Parser):
    """Register a parser instance into the global REGISTRY."""
    REGISTRY.append(parser)


def best_parser(sample: str, filename: str) -> Parser | None:
    """
    Run sniff() across all registered parsers and return the highest-confidence parser.
    If all return 0, return None (caller should fall back to RawParser or quarantine).
    """
    if not REGISTRY:
        return None
    scored = [(p.sniff(sample, filename), p) for p in REGISTRY]
    best_score, best = max(scored, key=lambda x: x[0])
    return best if best_score > 0 else None
