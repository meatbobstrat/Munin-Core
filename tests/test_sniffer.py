from pathlib import Path

from ingestor.handlers.raw import RawHandler
from ingestor.sniffer import sniff_file


def test_sniffer_falls_back_to_raw(tmp_path: Path):
    f = tmp_path / "fallback.log"
    f.write_text("line1\nline2\n")

    parser = sniff_file(f)
    assert parser is not None
    assert isinstance(parser, RawHandler)

    events = parser.parse(str(f))
    assert len(events) == 2
