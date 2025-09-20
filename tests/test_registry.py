
from ingestor.handlers.raw import RawHandler
from ingestor.handlers.registry import get_handler_for


def test_registry_falls_back_to_raw(tmp_path):
    # File with unknown extension
    f = tmp_path / "mystery.unknown"
    f.write_text("some text\n")

    handler = get_handler_for(f)
    assert isinstance(handler, RawHandler)
    events = handler.parse(str(f))
    assert events[0]["message"] == "some text"
