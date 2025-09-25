# storage/provider.py
from __future__ import annotations

import os
import json
import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Tuple, Protocol
from contextlib import contextmanager
from datetime import datetime


# ---------- Public Interface ----------

class StorageProvider(Protocol):
    """
    Minimal interface the API and ingestor need for persistence.
    Backends: SQLite (today), ClickHouse/Postgres/MSSQL (soon).
    """

    def ensure_table(self, log_type: str, schema: Dict[str, str]) -> None:
        """Create a table for this log_type if it doesn't exist."""
        ...

    def insert_events(self, log_type: str, events: Iterable[Dict[str, Any]]) -> int:
        """Insert a batch of events. Returns number inserted."""
        ...

    def query_events(
        self,
        log_type: Optional[str] = None,
        limit: int = 100,
        since: Optional[datetime] = None,
        filters: Optional[Dict[str, Any]] = None,
        order_desc: bool = True,
    ) -> List[Dict[str, Any]]:
        """Fetch recent events (optionally for a single log_type)."""
        ...

    def health(self) -> Dict[str, Any]:
        """Lightweight health/ready check metadata."""
        ...


def get_provider() -> StorageProvider:
    """
    Choose a provider from environment.
    Defaults to SQLite so current deployments keep working.
    """
    db_type = os.getenv("DB_TYPE", "sqlite").strip().lower()

    if db_type == "sqlite":
        db_path = os.getenv("DB_PATH", "/data/munin.db")
        return SQLiteProvider(db_path=db_path)

    elif db_type == "clickhouse":
        # We'll wire this up in a later file change
        # (keeping import local avoids optional dependency errors).
        return ClickHouseProvider(
            url=os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123"),
            database=os.getenv("CLICKHOUSE_DB", "munin"),
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            timeout=float(os.getenv("CLICKHOUSE_TIMEOUT", "10")),
        )

    else:
        raise ValueError(f"Unsupported DB_TYPE={db_type!r}")


# ---------- SQLite Implementation (works today) ----------

class SQLiteProvider:
    """
    Simple, robust default. One DB file, many tables (one per log type).
    Table shape:
      id INTEGER PK, ts TEXT, source TEXT, level TEXT, message TEXT, attrs TEXT(JSON)
    We keep it minimal; handlers can extend schema when we add migrations.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def ensure_table(self, log_type: str, schema: Dict[str, str]) -> None:
        # Base columns; "attrs" holds handler-specific fields as JSON for now.
        # When we switch to ClickHouse/Postgres, we’ll materialize columns per handler.
        table = self._table_name(log_type)
        with self._conn() as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    source TEXT,
                    level TEXT,
                    message TEXT,
                    attrs TEXT
                )
                """
            )
            conn.commit()

    def insert_events(self, log_type: str, events: Iterable[Dict[str, Any]]) -> int:
        table = self._table_name(log_type)
        rows = []
        for ev in events:
            ts = _coerce_ts(ev.get("timestamp") or ev.get("ts"))
            source = ev.get("source")
            level = ev.get("level")
            message = ev.get("message")
            # Pack everything else into attrs for now
            attrs_obj = {k: v for k, v in ev.items() if k not in {"timestamp", "ts", "source", "level", "message"}}
            attrs = json.dumps(attrs_obj, ensure_ascii=False)
            rows.append((ts, source, level, message, attrs))

        if not rows:
            return 0

        with self._conn() as conn:
            conn.executemany(
                f"INSERT INTO {table} (ts, source, level, message, attrs) VALUES (?, ?, ?, ?, ?)",
                rows,
            )
            conn.commit()
        return len(rows)

    def query_events(
        self,
        log_type: Optional[str] = None,
        limit: int = 100,
        since: Optional[datetime] = None,
        filters: Optional[Dict[str, Any]] = None,
        order_desc: bool = True,
    ) -> List[Dict[str, Any]]:
        filters = filters or {}
        out: List[Dict[str, Any]] = []

        def _fetch_from(table: str) -> List[Dict[str, Any]]:
            clauses: List[str] = []
            params: List[Any] = []

            if since is not None:
                clauses.append("ts >= ?")
                params.append(since.replace(microsecond=0).isoformat())

            if "level" in filters:
                clauses.append("level = ?")
                params.append(filters["level"])

            if "source" in filters:
                clauses.append("source = ?")
                params.append(filters["source"])

            order = "DESC" if order_desc else "ASC"
            where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            sql = f"SELECT ts, source, level, message, attrs FROM {table} {where} ORDER BY ts {order} LIMIT ?"
            params.append(int(limit))

            with self._conn() as conn:
                cur = conn.execute(sql, params)
                rows = cur.fetchall()

            items: List[Dict[str, Any]] = []
            for ts, source, level, message, attrs in rows:
                item = {
                    "timestamp": ts,
                    "source": source,
                    "level": level,
                    "message": message,
                }
                try:
                    extra = json.loads(attrs) if attrs else {}
                    if isinstance(extra, dict):
                        item.update(extra)
                except Exception:
                    item["attrs_raw"] = attrs
                items.append(item)
            return items

        if log_type:
            return _fetch_from(self._table_name(log_type))

        # No log_type: union of recent rows across all known tables (best-effort)
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'events_%' ORDER BY name"
            )
            tables = [r[0] for r in cur.fetchall()]

        for t in tables:
            out.extend(_fetch_from(t))
        # Already sorted per-table; keep simple for now
        return out[:limit]

    def health(self) -> Dict[str, Any]:
        try:
            with self._conn() as conn:
                conn.execute("SELECT 1")
            return {"ok": True, "backend": "sqlite", "path": self.db_path}
        except Exception as e:
            return {"ok": False, "backend": "sqlite", "error": str(e)}

    @staticmethod
    def _table_name(log_type: str) -> str:
        # normalize, e.g. "evtx", "csv", "log"
        safe = "".join(c if c.isalnum() or c == "_" else "_" for c in log_type.strip().lower())
        return f"events_{safe}"


def _coerce_ts(v: Any) -> str:
    if v is None:
        return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    if isinstance(v, datetime):
        return v.replace(microsecond=0).isoformat() + "Z"
    # string → trust but normalize best-effort
    s = str(v).strip()
    # ensure seconds and a Z suffix if missing
    if "Z" not in s and "+" not in s:
        if len(s) == 19:  # YYYY-MM-DDTHH:MM:SS
            return s + "Z"
    return s


# ---------- ClickHouse Provider (skeleton; implemented next) ----------

class ClickHouseProvider:
    """
    Placeholder so we can wire env/config now.
    We'll implement real inserts/queries in the next step.
    """
    def __init__(self, url: str, database: str, user: str, password: str, timeout: float = 10.0):
        self.url = url
        self.database = database
        self.user = user
        self.password = password
        self.timeout = timeout

    def ensure_table(self, log_type: str, schema: Dict[str, str]) -> None:
        raise NotImplementedError("ClickHouseProvider.ensure_table pending")

    def insert_events(self, log_type: str, events: Iterable[Dict[str, Any]]) -> int:
        raise NotImplementedError("ClickHouseProvider.insert_events pending")

    def query_events(
        self,
        log_type: Optional[str] = None,
        limit: int = 100,
        since: Optional[datetime] = None,
        filters: Optional[Dict[str, Any]] = None,
        order_desc: bool = True,
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError("ClickHouseProvider.query_events pending")

    def health(self) -> Dict[str, Any]:
        return {"ok": False, "backend": "clickhouse", "ready": False}
