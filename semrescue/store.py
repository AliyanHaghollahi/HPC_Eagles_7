"""Versioned key-value store (SQLite) used by every tier.

Each record carries (updated_at, origin) so replicas can be merged with
last-writer-wins, and a local sequence number so a node can ask
"what changed since my last sync?".

Namespaces:
  personal  - facts about the user (source of truth: edge; backup: cloud; never on fog)
  map       - building floor map (source of truth: cloud/fog; cached on edge)
"""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path


class Store:
    def __init__(self, path: str | Path = ":memory:"):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._lock = threading.Lock()
        with self._lock:
            if self.path != ":memory:":
                self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS records ("
                " ns TEXT, key TEXT, value TEXT, updated_at REAL, origin TEXT, seq INTEGER,"
                " PRIMARY KEY (ns, key))"
            )
            self._conn.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")
            row = self._conn.execute("SELECT COALESCE(MAX(seq), 0) FROM records").fetchone()
            self._seq = row[0]

    # ---------------------------------------------------------------- records
    def put(self, ns: str, key: str, value: str, origin: str, updated_at: float | None = None) -> bool:
        """Write with last-writer-wins. Returns True if the write was applied."""
        updated_at = time.time() if updated_at is None else updated_at
        with self._lock:
            cur = self._conn.execute(
                "SELECT updated_at, origin FROM records WHERE ns=? AND key=?", (ns, key)
            ).fetchone()
            if cur is not None and (cur[0], cur[1]) >= (updated_at, origin):
                return False
            self._seq += 1
            self._conn.execute(
                "INSERT OR REPLACE INTO records VALUES (?,?,?,?,?,?)",
                (ns, key, value, updated_at, origin, self._seq),
            )
            return True

    def get(self, ns: str, key: str) -> dict | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT key, value, updated_at, origin, seq FROM records WHERE ns=? AND key=?", (ns, key)
            ).fetchone()
        return _row(r) if r else None

    def all(self, ns: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, value, updated_at, origin, seq FROM records WHERE ns=? ORDER BY key", (ns,)
            ).fetchall()
        return [_row(r) for r in rows]

    def changes_since(self, ns: str, seq: int, exclude_origin: str | None = None,
                      only_origin: str | None = None) -> tuple[list[dict], int]:
        """Records in `ns` changed after local sequence `seq`. Returns (records, max_seq_seen)."""
        q = "SELECT key, value, updated_at, origin, seq FROM records WHERE ns=? AND seq>?"
        args: list = [ns, seq]
        if exclude_origin:
            q += " AND origin != ?"
            args.append(exclude_origin)
        if only_origin:
            q += " AND origin = ?"
            args.append(only_origin)
        with self._lock:
            rows = self._conn.execute(q + " ORDER BY seq", args).fetchall()
            max_seq = self._seq
        return [_row(r) for r in rows], max_seq

    def count(self, ns: str) -> int:
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM records WHERE ns=?", (ns,)).fetchone()[0]

    # ------------------------------------------------------------------- meta
    def meta_get(self, k: str, default=None):
        with self._lock:
            r = self._conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return r[0] if r else default

    def meta_set(self, k: str, v) -> None:
        with self._lock:
            self._conn.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, str(v)))

    def close(self) -> None:
        with self._lock:
            self._conn.close()


def _row(r) -> dict:
    return {"key": r[0], "value": r[1], "updated_at": r[2], "origin": r[3], "seq": r[4]}
