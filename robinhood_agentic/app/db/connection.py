"""SQLite connection helpers."""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .schema import SCHEMA_SQL

_local = threading.local()


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_conn(db_path: Path) -> sqlite3.Connection:
    path_key = str(db_path)
    cached = getattr(_local, "conns", None)
    if cached is None:
        _local.conns = {}
        cached = _local.conns
    if path_key not in cached:
        cached[path_key] = connect(db_path)
    return cached[path_key]


def init_db(db_path: Path) -> sqlite3.Connection:
    conn = get_conn(db_path)
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
