from fastapi import FastAPI
from storage.factory import get_storage_backend

app = FastAPI()
backend = get_storage_backend("sqlite", db_path="munin.db")
backend.connect()

@app.post("/ingest")
def ingest(data: dict):
    events = data.get("events", [])
    if not events:
        return {"status": "no events"}
    backend.write_batch(events)
    return {"status": "ok", "count": len(events)}
