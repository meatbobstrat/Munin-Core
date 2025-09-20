from pathlib import Path

from ingestor.handlers.raw import RawHandler


def test_raw_handler_parses_lines(tmp_path: Path):
    # Create a fake log file
    log_file = tmp_path / "sample.log"
    log_file.write_text("first line\nsecond line\n\nthird line\n")

    handler = RawHandler()
    events = handler.parse(str(log_file))

    # Should parse 3 non-empty lines
    assert len(events) == 3
    assert events[0]["line_number"] == 1
    assert events[0]["message"] == "first line"
    assert events[-1]["message"] == "third line"
