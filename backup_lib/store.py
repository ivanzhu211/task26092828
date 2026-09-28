"""SQLite persistence layer (schema + connection management).

All mutating service operations run inside a single ``BEGIN IMMEDIATE``
transaction. SQLite allows only one writer per database file, which gives
us process-wide serialization; combined with WAL readers this also means a
restore that holds an IMMEDIATE lock can never observe a file whose catalog
row is being deleted in another transaction.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    sector TEXT NOT NULL,
    retention_days INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS backups (
    id INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id),
    backup_time TEXT NOT NULL,
    backup_type TEXT NOT NULL,
    base_id INTEGER REFERENCES backups(id),
    file_path TEXT,
    sha256 TEXT,
    corrupted INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0,
    UNIQUE(source_id, backup_time)
);
CREATE INDEX IF NOT EXISTS idx_backups_source ON backups(source_id, backup_time);
CREATE INDEX IF NOT EXISTS idx_backups_base ON backups(base_id);

CREATE TABLE IF NOT EXISTS cleanup_requests (
    id INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id),
    submitted_at TEXT NOT NULL,
    submitted_by TEXT NOT NULL,
    approved_at TEXT,
    approved_by TEXT
);

CREATE TABLE IF NOT EXISTS cleanup_items (
    id INTEGER PRIMARY KEY,
    request_id INTEGER NOT NULL REFERENCES cleanup_requests(id),
    backup_id INTEGER NOT NULL REFERENCES backups(id),
    final_status TEXT,
    final_reason TEXT DEFAULT '',
    UNIQUE(request_id, backup_id)
);
CREATE INDEX IF NOT EXISTS idx_items_request ON cleanup_items(request_id);
"""


class Store:
    def __init__(self, db_path: str | Path, storage_dir: str | Path) -> None:
        self.db_path = Path(db_path)
        self.storage_dir = Path(storage_dir)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        conn = self.connect()
        try:
            conn.executescript(SCHEMA)
        finally:
            conn.close()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        # A long-chain restore can hold the write lock for a while; wait
        # instead of failing concurrent writers with SQLITE_BUSY.
        conn.execute("PRAGMA busy_timeout=300000")
        return conn
