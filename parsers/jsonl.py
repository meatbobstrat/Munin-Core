# src/munin/parsers/jsonl.py
import json
from dateutil import parser as dtp
from .base import Parser, NormalizedEvent, register

class JSONLParser(Parser):
    def sniff(self, sample: str, filename: str) -> float:
        s = sample.lstrip()
        if not s.startswith(("{", "[")): return 0.0
        try:
            obj = json.loads(sample)
            return 0.9 if isinstance(obj, (dict, list)) else 0.0
        except Exception:
            return 0.0

    def parse_line(self, line: str, line_no: int, filename: str):
        line = line.strip()
        if not line: return None
        try:
            obj = json.loads(line)
        except Exception:
            return None
        # common fields
        ts = obj.get("ts") or obj.get("time") or obj.get("timestamp")
        level = (obj.get("level") or obj.get("lvl") or obj.get("severity") or "").upper()
        msg = obj.get("msg") or obj.get("message") or obj.get("event") or line[:500]
        try:
            event_time = dtp.parse(ts).isoformat() if ts else None
        except Exception:
            event_time = None
        attrs = {k: v for k, v in obj.items() if k not in {"ts","time","timestamp","level","lvl","severity","msg","message","event"}}
        return NormalizedEvent(
            source_path=filename, source_type="jsonl", line_number=line_no,
            event_time=event_time, level=level, message=str(msg),
            attrs=attrs, raw_excerpt=line
        )

register(JSONLParser())
