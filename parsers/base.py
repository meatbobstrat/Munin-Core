# src/munin/parsers/base.py
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

class NormalizedEvent(Dict[str, Any]):
    """Dict with keys: source_path, source_type, line_number, event_time, level, message, attrs, raw_excerpt"""

class Parser(ABC):
    @abstractmethod
    def sniff(self, sample: str, filename: str) -> float:
        """Return confidence 0.0..1.0 that this parser can handle the file/line."""
        ...

    @abstractmethod
    def parse_line(self, line: str, line_no: int, filename: str) -> Optional[NormalizedEvent]:
        """Return normalized event or None (skip/blank)."""
        ...

REGISTRY: list[Parser] = []

def register(parser: Parser):
    REGISTRY.append(parser)
