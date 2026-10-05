"""Durable state for the control service (SQLite, WAL, fully synchronous).

The release intent (release_id -> sha256 + bytes) is immutable once inserted;
only the derived state and the repo receipts are ever appended afterwards.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading

from app.common.receipts import utcnow

SCHEMA = """
CREATE TABLE IF NOT EXISTS releases (
  release_id     TEXT PRIMARY KEY,
  seq            INTEGER,
  sha256         TEXT NOT NULL,
  size           INTEGER NOT NULL,
  artifact       BLOB NOT NULL,
  state          TEXT NOT NULL,
  error          TEXT,
  superseded_by  TEXT,
  created_at     TEXT NOT NULL,
  updated_at     TEXT NOT NULL
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

# Releases created before the activation-fence feature have no seq; assign one
# in created_at order (rowid breaks ties) the first time a fenced store opens.
BACKFILL_SEQ = """
UPDATE releases SET seq = (
  SELECT COUNT(*) FROM releases AS r2
  WHERE r2.created_at < releases.created_at
     OR (r2.created_at = releases.created_at AND r2.rowid <= releases.rowid)
) - 1 WHERE seq IS NULL;
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
        """Add fence columns to pre-fence databases, then backfill sequence."""
        cols = {r[1] for r in self._db.execute("PRAGMA table_info(releases)")}
        if "seq" not in cols:
            self._db.execute("ALTER TABLE releases ADD COLUMN seq INTEGER")
        if "superseded_by" not in cols:
            self._db.execute("ALTER TABLE releases ADD COLUMN superseded_by TEXT")
        self._db.execute(BACKFILL_SEQ)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # ---- releases ----
    def insert_release(self, release_id: str, sha256: str, artifact: bytes, state: str) -> None:
        now = utcnow()
        with self._lock:
            row = self._db.execute("SELECT COALESCE(MAX(seq), -1) + 1 AS next FROM releases").fetchone()
            next_seq = int(row["next"])
            self._db.execute(
                "INSERT INTO releases(release_id, seq, sha256, size, artifact, state, error,"
                " superseded_by, created_at, updated_at)"
                " VALUES(?,?,?,?,?,?,NULL,NULL,?,?)",
                (release_id, next_seq, sha256, len(artifact), artifact, state, now, now),
            )
            self._db.commit()

    def get_release(self, release_id: str) -> dict | None:
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM releases WHERE release_id=?", (release_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_releases(self) -> list[dict]:
        with self._lock:
            rows = self._db.execute(
                "SELECT release_id, seq, sha256, size, state, error, superseded_by,"
                " created_at, updated_at FROM releases ORDER BY seq"
            ).fetchall()
        return [dict(r) for r in rows]

    def ordered_releases(self) -> list[dict]:
        """All releases in durable publication (first-persist) order."""
        with self._lock:
            rows = self._db.execute("SELECT * FROM releases ORDER BY seq").fetchall()
        return [dict(r) for r in rows]

    def pending_release_ids(self, terminal_states) -> list[str]:
        marks = ",".join("?" for _ in terminal_states)
        with self._lock:
            rows = self._db.execute(
                f"SELECT release_id FROM releases WHERE state NOT IN ({marks})"
                " ORDER BY seq",
                tuple(terminal_states),
            ).fetchall()
        return [r[0] for r in rows]

    def mark_superseded(self, release_id: str, superseding_id: str) -> bool:
        """Lock a still-live, evidence-free release as SUPERSEDED.

        Only a non-terminal release can be superseded; returns whether a row
        was actually changed, so a terminal/concurrent conclusion wins.
        """
        with self._lock:
            cur = self._db.execute(
                "UPDATE releases SET state=?, superseded_by=?, error=NULL, updated_at=?"
                " WHERE release_id=? AND state NOT IN ('COMPLETED','REJECTED','SUPERSEDED')",
                ("SUPERSEDED", superseding_id, utcnow(), release_id),
            )
            self._db.commit()
            return cur.rowcount > 0

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
