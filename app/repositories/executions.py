"""SQLite-backed execution store (swap for Postgres by re-implementing this class).

Each execution is persisted BEFORE the first broker call and updated after every
order, so a crash mid-rebalance leaves an auditable record of what was sent.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import threading
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS executions (
  id              TEXT PRIMARY KEY,
  idempotency_key TEXT UNIQUE,
  request_hash    TEXT,
  broker          TEXT,
  status          TEXT,
  created_at      TEXT,
  payload         TEXT NOT NULL
);
"""


class DuplicateIdempotencyKey(Exception):
    pass


class ExecutionRepository:
    def __init__(self, path: str):
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.executescript(SCHEMA)
        self._lock = threading.Lock()

    def _run(self, fn):
        with self._lock:
            return fn()

    async def _async(self, fn):
        return await asyncio.to_thread(self._run, fn)

    async def create(self, record) -> None:
        def go():
            try:
                self._db.execute(
                    "INSERT INTO executions VALUES (?,?,?,?,?,?,?)",
                    (record.id, record.idempotency_key, record.request_hash, record.broker,
                     record.status.value, record.created_at, json.dumps(record.to_dict(), default=str)))
                self._db.commit()
            except sqlite3.IntegrityError as e:
                raise DuplicateIdempotencyKey(record.idempotency_key) from e
        await self._async(go)

    async def update(self, record) -> None:
        def go():
            self._db.execute("UPDATE executions SET status=?, payload=? WHERE id=?",
                             (record.status.value, json.dumps(record.to_dict(), default=str), record.id))
            self._db.commit()
        await self._async(go)

    async def get(self, exec_id: str) -> Optional[dict]:
        def go():
            r = self._db.execute("SELECT payload FROM executions WHERE id=?", (exec_id,)).fetchone()
            return json.loads(r[0]) if r else None
        return await self._async(go)

    async def get_by_key(self, key: str) -> Optional[tuple[dict, str]]:
        """Returns (payload, request_hash) for an existing idempotency key."""
        def go():
            r = self._db.execute("SELECT payload, request_hash FROM executions WHERE idempotency_key=?", (key,)).fetchone()
            return (json.loads(r[0]), r[1]) if r else None
        return await self._async(go)

    async def list(self, limit: int = 50) -> list[dict]:
        def go():
            rows = self._db.execute("SELECT payload FROM executions ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
            return [json.loads(r[0]) for r in rows]
        return await self._async(go)
