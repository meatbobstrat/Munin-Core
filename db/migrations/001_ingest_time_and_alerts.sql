-- Add ingest_time_utc (default now) so we can prune even when event_time_utc is NULL
ALTER TABLE event_occurrence
  ADD COLUMN IF NOT EXISTS ingest_time_utc TEXT
    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'));

CREATE INDEX IF NOT EXISTS idx_event_ingest_time
  ON event_occurrence(ingest_time_utc);

-- Simple alerts table
CREATE TABLE IF NOT EXISTS alert (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  level TEXT NOT NULL,            -- info|warning|error
  code TEXT NOT NULL,             -- STORAGE_HIGH | QUARANTINE_NEW | INGESTION_BACKPRESSURE
  message TEXT NOT NULL,
  created_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  metadata_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_alert_created
  ON alert(created_utc DESC);
