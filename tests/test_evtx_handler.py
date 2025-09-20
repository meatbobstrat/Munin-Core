from pathlib import Path

import pytest

from ingestor.handlers.evtx import EVTXHandler  # <-- fixed name


def test_evtx_parsing_counts():
    """EVTXHandler should parse events and return a non-empty list."""

    fixture_path = Path("tests/fixtures/Security-small.evtx")
    assert fixture_path.exists(), f"Fixture missing: {fixture_path}"

    handler = EVTXHandler()  # <-- fixed name
    events = handler.parse(str(fixture_path))

    assert isinstance(events, list)
    assert len(events) > 0

    # sanity check schema on first event
    first = events[0]
    for key in ["source", "file_type", "ingest_time", "line_number", "message"]:
        assert key in first


def test_evtx_parsing_missing_file():
    """EVTXHandler should raise FileNotFoundError on bad path."""
    handler = EVTXHandler()
    fake_file = Path("tests/fixtures/does_not_exist.evtx")

    with pytest.raises(FileNotFoundError):
        handler.parse(str(fake_file))


def test_evtx_parsing_fallback_to_faw(tmp_path):
    """EVTXHandler should gracefully fall back to FAW parsing on corrupt input."""
    bad_file = tmp_path / "corrupt.evtx"
    bad_file.write_text("NOT_A_REAL_EVTX_FILE")

    handler = EVTXHandler()
    events = handler.parse(str(bad_file))

    assert isinstance(events, list)
