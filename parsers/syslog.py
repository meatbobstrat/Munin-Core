# src/munin/parsers/syslog.py
import re

from dateutil import parser as dtp

from .base import NormalizedEvent, Parser, register

SYSLOG_RE = re.compile(
    r"^(?P<ts>\w{3}\s+\d{1,2}\s[\d:]{8}|\d{4}-\d{2}-\d{2}T[\d:+\-:.Z]+)\s+(?P<host>\S+)\s+(?P<tag>[\w\-/\[\].]+):\s*(?P<msg>.*)$"
)


class SyslogParser(Parser):
    def sniff(self, sample: str, filename: str) -> float:
        return 0.7 if SYSLOG_RE.match(sample) else 0.0

    def parse_line(self, line: str, line_no: int, filename: str):
        m = SYSLOG_RE.match(line)
        if not m:
            return None
        d = m.groupdict()
        try:
            event_time = dtp.parse(d["ts"]).isoformat()
        except Exception:
            event_time = None
        msg = d["msg"]
        # simple level heuristic
        level = "ERROR" if re.search(r"\b(fail|error|critical|denied)\b", msg, re.I) else "INFO"
        attrs = {"host": d["host"], "tag": d["tag"]}
        return NormalizedEvent(
            source_path=filename,
            source_type="syslog",
            line_number=line_no,
            event_time=event_time,
            level=level,
            message=msg,
            attrs=attrs,
            raw_excerpt=line,
        )


register(SyslogParser())
