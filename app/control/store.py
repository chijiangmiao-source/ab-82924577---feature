"""Durable state for the control service (SQLite, WAL, fully synchronous).

The release intent (release_id -> sha256 + bytes) is immutable once inserted;
only the derived state and the repo receipts are ever appended afterwards.

Queue semantics (single activation fence): releases are ranked by their first
persistence order (rowid, exposed as ``seq``). When a new release is persisted,
every older non-terminal candidate WITHOUT any persisted repo-side activation
receipt is superseded by it in the SAME transaction, while an older candidate
that HAS activation evidence keeps the fence and forces the new release to
wait. Because the supersede decision and the new intent commit atomically, a
restart always recovers the same queue conclusion from the durable intent and
the repo receipts.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading

from app.common.receipts import utcnow
from app.control import core

SCHEMA = """
CREATE TABLE IF NOT EXISTS releases (
  release_id TEXT PRIMARY KEY,
  sha256     TEXT NOT NULL,
  size       INTEGER NOT NULL,
  artifact   BLOB NOT NULL,
  state      TEXT NOT NULL,
  error      TEXT,
  superseded_by TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS receipts (
  release_id TEXT NOT NULL,
  repo       TEXT NOT NULL,
  op         TEXT NOT NULL,
  op_key     TEXT NOT NULL,
  digest     TEXT NOT NULL,
  receipt    TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (release_id, repo, op)
);
"""


class Store:
    def __init__(self, path: str):
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        with self._lock:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.executescript(SCHEMA)
            self._migrate()
            self._db.commit()

    def _migrate(self) -> None:
        cols = {r[1] for r in self._db.execute("PRAGMA table_info(releases)")}
        if "superseded_by" not in cols:
            self._db.execute("ALTER TABLE releases ADD COLUMN superseded_by TEXT")

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # ---- releases ----
    def insert_release_with_fence(self, release_id: str, sha256: str,
                                  artifact: bytes) -> tuple[str, list[str]]:
        """Persist the intent and apply the activation fence atomically.

        Older non-terminal candidates without any persisted repo-side
        activation receipt are superseded by this release; an older candidate
        WITH activation evidence keeps the fence and makes this release WAIT.
        Returns (initial_state, [superseded_release_ids]).
        """
        now = utcnow()
        with self._lock:
            try:
                older = self._db.execute(
                    "SELECT release_id FROM releases WHERE state NOT IN (%s)"
                    " ORDER BY rowid" % ",".join("?" * len(core.TERMINAL_STATES)),
                    core.TERMINAL_STATES,
                ).fetchall()
                superseded: list[str] = []
                blocked = False
                for (rid,) in older:
                    if self._has_activation_receipt(rid):
                        # Activation evidence exists: the holder must converge
                        # (COMPLETED/REJECTED) before any newer release runs.
                        blocked = True
                        continue
                    self._db.execute(
                        "UPDATE releases SET state=?, superseded_by=?, updated_at=?"
                        " WHERE release_id=?",
                        (core.STATE_SUPERSEDED, release_id, now, rid),
                    )
                    superseded.append(rid)
                state = core.STATE_WAITING if blocked else core.STATE_PENDING
                self._db.execute(
                    "INSERT INTO releases(release_id, sha256, size, artifact, state,"
                    " error, superseded_by, created_at, updated_at)"
                    " VALUES(?,?,?,?,?,NULL,NULL,?,?)",
                    (release_id, sha256, len(artifact), artifact, state, now, now),
                )
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise
        return state, superseded

    def _has_activation_receipt(self, release_id: str) -> bool:
        return self._db.execute(
            "SELECT 1 FROM receipts WHERE release_id=? AND op=? LIMIT 1",
            (release_id, core.OP_ACTIVATE),
        ).fetchone() is not None

    def get_release(self, release_id: str) -> dict | None:
        with self._lock:
            row = self._db.execute(
                "SELECT rowid AS seq, * FROM releases WHERE release_id=?",
                (release_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_releases(self) -> list[dict]:
        with self._lock:
            rows = self._db.execute(
                "SELECT rowid AS seq, release_id, sha256, size, state, error,"
                " superseded_by, created_at, updated_at FROM releases ORDER BY rowid"
            ).fetchall()
        return [dict(r) for r in rows]

    def head_release_id(self) -> str | None:
        """The fence head: the oldest non-terminal release, if any."""
        marks = ",".join("?" * len(core.TERMINAL_STATES))
        with self._lock:
            row = self._db.execute(
                f"SELECT release_id FROM releases WHERE state NOT IN ({marks})"
                " ORDER BY rowid LIMIT 1",
                core.TERMINAL_STATES,
            ).fetchone()
        return row[0] if row else None

    def blocking_release_id(self, release_id: str) -> str | None:
        """The oldest non-terminal release ahead of ``release_id`` in the queue."""
        marks = ",".join("?" * len(core.TERMINAL_STATES))
        with self._lock:
            row = self._db.execute(
                f"SELECT release_id FROM releases WHERE state NOT IN ({marks})"
                " AND release_id != ? ORDER BY rowid LIMIT 1",
                (*core.TERMINAL_STATES, release_id),
            ).fetchone()
        return row[0] if row else None

    def update_state(self, release_id: str, state: str, error: str | None = None) -> None:
        with self._lock:
            self._db.execute(
                "UPDATE releases SET state=?, error=?, updated_at=? WHERE release_id=?",
                (state, error, utcnow(), release_id),
            )
            self._db.commit()

    # ---- receipts (repo-side evidence) ----
    def put_receipt(self, release_id: str, repo: str, op: str, op_key: str,
                    digest: str, receipt: dict) -> None:
        with self._lock:
            self._db.execute(
                "INSERT OR IGNORE INTO receipts(release_id, repo, op, op_key, digest,"
                " receipt, created_at) VALUES(?,?,?,?,?,?,?)",
                (release_id, repo, op, op_key, digest,
                 json.dumps(receipt, ensure_ascii=False), utcnow()),
            )
            self._db.commit()

    def get_receipt(self, release_id: str, repo: str, op: str) -> dict | None:
        with self._lock:
            row = self._db.execute(
                "SELECT receipt FROM receipts WHERE release_id=? AND repo=? AND op=?",
                (release_id, repo, op),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def receipts_for(self, release_id: str) -> dict:
        with self._lock:
            rows = self._db.execute(
                "SELECT repo, op, receipt FROM receipts WHERE release_id=?",
                (release_id,),
            ).fetchall()
        out: dict = {}
        for repo, op, receipt in rows:
            out.setdefault(repo, {})[op] = json.loads(receipt)
        return out
