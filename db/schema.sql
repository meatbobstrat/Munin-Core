-- file_manifest: tracks every ingested file
CREATE TABLE IF NOT EXISTS file_manifest (
  id INTEGER PRIMARY KEY,
  rel_path TEXT NOT NULL,
  sha256 TEXT NOT NULL UNIQUE,
  bytes INTEGER NOT NULL,
  mtime_utc TEXT NOT NULL,
  source_host TEXT,
  source_app  TEXT,
  started_at_utc  TEXT NOT NULL,
  ingested_at_utc TEXT,
  status TEXT NOT NULL CHECK(status IN ('processing','committed','deleted','error')) DEFAULT 'processing'
);

-- event_occurrence: every log line = one row (no dedup)
CREATE TABLE IF NOT EXISTS event_occurrence (
  id INTEGER PRIMARY KEY,
  file_id INTEGER NOT NULL REFERENCES file_manifest(id) ON DELETE CASCADE,
  line_no INTEGER NOT NULL,
  byte_offset INTEGER NOT NULL,
  event_time_utc TEXT,
  level TEXT,
  message TEXT NOT NULL,
  attrs_json TEXT,
  source_host TEXT,
  source_app TEXT,
  content_sha256 TEXT,
  maybe_duplicate INTEGER NOT NULL DEFAULT 0,
  UNIQUE(file_id, line_no)
);
CREATE INDEX IF NOT EXISTS idx_occ_time ON event_occurrence(event_time_utc);
CREATE INDEX IF NOT EXISTS idx_occ_level ON event_occurrence(level);
CREATE INDEX IF NOT EXISTS idx_occ_host_app_time ON event_occurrence(source_host, source_app, event_time_utc);
CREATE INDEX IF NOT EXISTS idx_occ_content ON event_occurrence(content_sha256);

-- (stub for v1 detectors)
CREATE TABLE IF NOT EXISTS event_annotations (
  id INTEGER PRIMARY KEY,
  occurrence_id INTEGER NOT NULL REFERENCES event_occurrence(id) ON DELETE CASCADE,
  finding_type TEXT NOT NULL,
  display_label TEXT NOT NULL,
  raw_value TEXT,
  confidence REAL NOT NULL DEFAULT 0.9,
  expires_at_utc TEXT,
  source_hint TEXT,
  created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

-- (sandboxed LLM memory)
CREATE TABLE IF NOT EXISTS notes (
  id INTEGER PRIMARY KEY,
  created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
  updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
  echo_epoch TEXT NOT NULL,
  echo_offset TEXT NOT NULL,
  author TEXT NOT NULL,
  title TEXT,
  body TEXT NOT NULL,
  tags TEXT,
  visibility TEXT NOT NULL DEFAULT 'org',
  pinned INTEGER NOT NULL DEFAULT 0,
  revision INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS note_links (
  id INTEGER PRIMARY KEY,
  note_id INTEGER NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
  link_type TEXT NOT NULL,
  link_ref TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS note_audit (
  id INTEGER PRIMARY KEY,
  at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  note_id INTEGER,
  delta_bytes INTEGER,
  meta TEXT
);
