-- scripts/sql/001_normalized_events.sql
CREATE TABLE IF NOT EXISTS normalized_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  inserted_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  source_path TEXT NOT NULL,
  source_type TEXT,                -- e.g., 'jsonl','syslog','csv','txt','evtx'
  line_number INTEGER,             -- for line-oriented formats
  event_time TEXT,                 -- RFC3339 if we can parse it
  level TEXT,                      -- INFO/WARN/ERROR/etc.
  message TEXT,                    -- human-readable summary
  attrs TEXT,                      -- JSON blob of key/values (ips, user, pid, ...)
  raw_excerpt TEXT,                -- original line or snippet
  content_hash TEXT                -- idempotency (sha256 of normalized fields)
);

CREATE INDEX IF NOT EXISTS idx_norm_time ON normalized_events (event_time);
CREATE INDEX IF NOT EXISTS idx_norm_src ON normalized_events (source_path);
CREATE UNIQUE INDEX IF NOT EXISTS uq_norm_hash ON normalized_events (content_hash);
