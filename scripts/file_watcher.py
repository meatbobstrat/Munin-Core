import os
import time

import requests
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from ingestor.handlers import csvlog, evtx, raw

INGEST_API_URL = "http://api_ingest:8000/ingest"  # container → API
SOURCE_NAME = os.getenv("SOURCE_NAME", "default_source")

def send_to_api(events):
    try:
        payload = {
            "source": SOURCE_NAME,
            "file_type": events[0].get("file_type", "unknown"),
            "ingest_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "events": events
        }
        r = requests.post(INGEST_API_URL, json=payload, timeout=5)
        r.raise_for_status()
    except Exception as e:
        print(f"[ERROR] Failed to send events to API: {e}")
        # TODO: write to buffer for retry later

class LogHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            events = self.parse_file(event.src_path)
            if events:
                send_to_api(events)

    def parse_file(self, file_path):
        ext = os.path.splitext(file_path)[1].lower()
        handler = {
            ".csv": csvlog.CSVLogHandler(),
            ".evtx": evtx.EVTXHandler(),
        }.get(ext, raw.RawHandler())
        return handler.parse(file_path)

if __name__ == "__main__":
    path = "./incoming"
    event_handler = LogHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()
    print(f"Watching {path} for new logs...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
