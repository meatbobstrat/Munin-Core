import sqlite3
from typing import Any, Dict, List

from .base import StorageBackend


class SQLiteBackend(StorageBackend):
    def __init__(self, db_path="munin.db"):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self._create_schema()

    def _create_schema(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            file_type TEXT,
            ingest_time TEXT,
            line_number INTEGER,
            message TEXT,
            tags TEXT
        )
        """)
        self.conn.commit()

    def write_batch(self, events: List[Dict[str, Any]]) -> None:
        cur = self.conn.cursor()
        cur.executemany("""
        INSERT INTO events (source, file_type, ingest_time, line_number, message, tags)
        VALUES (:source, :file_type, :ingest_time, :line_number, :message, :tags)
        """, events)
        self.conn.commit()

    def query_events(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()
        query = "SELECT * FROM events WHERE 1=1"
        params = []

        if "file_type" in filters:
            query += " AND file_type = ?"
            params.append(filters["file_type"])
        if "source" in filters:
            query += " AND source = ?"
            params.append(filters["source"])

        cur.execute(query, params)
        rows = cur.fetchall()
        return [dict(zip([c[0] for c in cur.description], row)) for row in rows]

    def close(self):
        if self.conn:
            self.conn.close()
