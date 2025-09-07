# src/munin/parsers/csvlog.py
import csv
import io

from dateutil import parser as dtp

from .base import NormalizedEvent, Parser, register


class CSVParser(Parser):
    _dialect = csv.excel

    def sniff(self, sample: str, filename: str) -> float:
        try:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            self._dialect = dialect
            return 0.6
        except Exception:
            return 0.0

    def parse_line(self, line: str, line_no: int, filename: str):
        if line_no == 1:
            return None  # header
        reader = csv.reader(io.StringIO(line), dialect=self._dialect)
        row = next(reader, None)
        if not row:
            return None
        # naive: treat first col as time if parseable
        ts = None
        try:
            ts = row[0]
            event_time = dtp.parse(ts).isoformat()
        except Exception:
            event_time = None
        msg = ",".join(row[:6])[:500]
        return NormalizedEvent(
            source_path=filename,
            source_type="csv",
            line_number=line_no,
            event_time=event_time,
            level="",
            message=msg,
            attrs={"columns": row},
            raw_excerpt=line,
        )


register(CSVParser())
