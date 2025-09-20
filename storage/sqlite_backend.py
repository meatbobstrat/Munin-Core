import os
import sqlite3
from typing import Any

from .base import StorageBackend


class SQLiteBackend(StorageBackend):
    def __init__(self, db_path="munin.db", max_db_size_mb=100):
        """
        SQLite backend for Munin.
        :param db_path: Path to sqlite db file.
        :param max_db_size_mb: Maximum DB size before pruning oldest rows.
        """
        self.db_path = db_path
        self.max_db_size_mb = max_db_size_mb
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
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

    def _db_size_mb(self) -> float:
        """Return current DB size in MB (0 if not created yet)."""
        if os.path.exists(self.db_path):
            return os.path.getsize(self.db_path) / (1024 * 1024)
        return 0.0

    def _prune_oldest_rows(self, target_size_mb: float = None):
        """
        Delete oldest rows until DB size is under target_size_mb.
        Runs in 1000-row chunks to avoid long locks.
        """
        if target_size_mb is None:
            target_size_mb = self.max_db_size_mb * 0.9  # leave a 10% buffer

        cur = self.conn.cursor()
        while self._db_size_mb() > target_size_mb:
            cur.execute(
                "DELETE FROM events WHERE id IN (SELECT id FROM events ORDER BY id ASC LIMIT 1000)"
            )
            self.conn.commit()

        # TODO: add alert hook here (event_type="prune", details={...})

    def write_batch(self, events: list[dict[str, Any]]) -> None:
        """Write events, pruning if DB exceeds max size."""
        cur = self.conn.cursor()
        cur.executemany(
            """
        INSERT INTO events (source, file_type, ingest_time, line_number, message, tags)
        VALUES (:source, :file_type, :ingest_time, :line_number, :message, :tags)
        """,
            events,
        )
        self.conn.commit()

        if self._db_size_mb() > self.max_db_size_mb:
            self._prune_oldest_rows()

    def query_events(self, filters: dict[str, Any]) -> list[dict[str, Any]]:
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
        return [dict(zip([c[0] for c in cur.description], row, strict=False)) for row in rows]

    def close(self):
        if self.conn:
            self.conn.close()
