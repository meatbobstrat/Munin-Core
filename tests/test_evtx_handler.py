from pathlib import Path
import pytest

from ingestor.handlers.evtx import EvtxHandler
from ingestor.handlers.raw import RawHandler


def test_evtx_parsing_counts():
    """EvtxHandler should parse events and return a non-empty list."""

    fixture_path = Path("tests/fixtures/Security-small.evtx")
    assert fixture_path.exists(), f"Fixture missing: {fixture_path}"

    handler = EvtxHandler()
    events = handler.parse(str(fixture_path))

    assert isinstance(events, list)
    assert len(events) > 0

    # sanity check schema on first event
    first = events[0]
    for key in ["source", "file_type", "ingest_time", "line_number", "message"]:
        assert key in first


def test_evtx_fallback_to_raw(tmp_path):
    """Non-EVTX input should fail under EvtxHandler, but RawHandler should still parse."""

    fake_file = tmp_path / "not_evtx.txt"
    fake_file.write_text("This is not an EVTX file\nLine two\n")

    handler = EvtxHandler()

    with pytest.raises(Exception):
        handler.parse(str(fake_file))

    # Fallback via RawHandler
    raw_handler = RawHandler()
    events = raw_handler.parse(str(fake_file))

    assert isinstance(events, list)
    assert len(events) > 0
    first = events[0]
    for key in ["source", "file_type", "ingest_time", "line_number", "message"]:
        assert key in first
