# src/munin/parsers/plaintext.py
import re

from dateutil import parser as dtp

from .base import NormalizedEvent, Parser, register

TS_CAND = re.compile(
    r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)|([A-Z][a-z]{2}\s+\d{1,2}\s[\d:]{8})"
)
LEVEL_RE = re.compile(r"\b(DEBUG|INFO|WARN|WARNING|ERROR|CRITICAL|FATAL)\b", re.I)
IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
USER_RE = re.compile(r"user[name]?=([^ \]]+)", re.I)


class PlainTextParser(Parser):
    def sniff(self, sample: str, filename: str) -> float:
        # default fallback; give low confidence
        return 0.3

    def parse_line(self, line: str, line_no: int, filename: str):
        s = line.strip()
        if not s:
            return None
        ts = None
        m = TS_CAND.search(s)
        if m:
            try:
                ts = dtp.parse(m.group(0)).isoformat()
            except Exception:
                ts = None
        level = ""
        ml = LEVEL_RE.search(s)
        if ml:
            level = ml.group(1).upper()
        attrs = {}
        ip = IP_RE.search(s)
        user = USER_RE.search(s)
        if ip:
            attrs["ip"] = ip.group(0)
        if user:
            attrs["user"] = user.group(1)
        return NormalizedEvent(
            source_path=filename,
            source_type="txt",
            line_number=line_no,
            event_time=ts,
            level=level,
            message=s[:500],
            attrs=attrs,
            raw_excerpt=s,
        )


register(PlainTextParser())
