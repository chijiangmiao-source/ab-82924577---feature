"""Unit tests for the activation-fence queue decisions in the control store."""
import os
import sqlite3
import tempfile
import unittest

from app.control import core
from app.control.store import Store


class FenceStoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.dir.name, "control.db"))

    def tearDown(self):
        self.store.close()
        self.dir.cleanup()

    def _receipt(self, rid, op=core.OP_ACTIVATE, repo="repo-a"):
        self.store.put_receipt(rid, repo, op, core.op_key(rid, repo, op),
                               "ab" * 32, {"digest": "ab" * 32})

    def test_first_release_becomes_fence_head(self):
        state, superseded = self.store.insert_release_with_fence("r1", "aa", b"x")
        self.assertEqual(state, core.STATE_PENDING)
        self.assertEqual(superseded, [])
        self.assertEqual(self.store.head_release_id(), "r1")

    def test_evidence_free_older_candidate_is_superseded(self):
        self.store.insert_release_with_fence("r1", "aa", b"x")
        state, superseded = self.store.insert_release_with_fence("r2", "bb", b"y")
        self.assertEqual(superseded, ["r1"])
        self.assertEqual(state, core.STATE_PENDING)
        rel = self.store.get_release("r1")
        self.assertEqual(rel["state"], core.STATE_SUPERSEDED)
        self.assertEqual(rel["superseded_by"], "r2")
        self.assertEqual(self.store.head_release_id(), "r2")

    def test_prepare_receipt_alone_does_not_block_supersede(self):
        self.store.insert_release_with_fence("r1", "aa", b"x")
        self._receipt("r1", op=core.OP_PREPARE)
        state, superseded = self.store.insert_release_with_fence("r2", "bb", b"y")
        self.assertEqual(superseded, ["r1"])
        self.assertEqual(state, core.STATE_PENDING)

    def test_activation_evidence_blocks_newer_release(self):
        self.store.insert_release_with_fence("r1", "aa", b"x")
        self._receipt("r1")  # activation receipt persisted for repo-a
        state, superseded = self.store.insert_release_with_fence("r2", "bb", b"y")
        self.assertEqual(superseded, [])
        self.assertEqual(state, core.STATE_WAITING)
        self.assertEqual(self.store.get_release("r1")["state"], core.STATE_PENDING)
        self.assertEqual(self.store.head_release_id(), "r1")
        self.assertEqual(self.store.blocking_release_id("r2"), "r1")

    def test_waiting_candidate_is_superseded_by_even_newer_one(self):
        self.store.insert_release_with_fence("r1", "aa", b"x")
        self._receipt("r1")
        self.store.insert_release_with_fence("r2", "bb", b"y")  # waits behind r1
        state, superseded = self.store.insert_release_with_fence("r3", "cc", b"z")
        # r2 never left activation evidence -> superseded by r3;
        # r1 still holds evidence -> r3 keeps waiting.
        self.assertEqual(superseded, ["r2"])
        self.assertEqual(state, core.STATE_WAITING)
        self.assertEqual(self.store.get_release("r2")["superseded_by"], "r3")
        self.assertEqual(self.store.blocking_release_id("r3"), "r1")

    def test_terminal_older_release_neither_blocks_nor_is_superseded(self):
        for terminal in core.TERMINAL_STATES:
            self.store.insert_release_with_fence(f"head-{terminal}", "aa", b"x")
            self.store.update_state(f"head-{terminal}", terminal)
            state, superseded = self.store.insert_release_with_fence(
                f"next-{terminal}", "bb", b"y")
            self.assertEqual(state, core.STATE_PENDING)
            self.assertEqual(superseded, [])
            self.assertEqual(self.store.get_release(f"head-{terminal}")["state"],
                             terminal)

    def test_supersede_chain(self):
        self.store.insert_release_with_fence("c1", "aa", b"x")
        self.store.insert_release_with_fence("c2", "bb", b"y")
        state, superseded = self.store.insert_release_with_fence("c3", "cc", b"z")
        self.assertEqual(superseded, ["c2"])
        self.assertEqual(state, core.STATE_PENDING)
        self.assertEqual(self.store.get_release("c1")["superseded_by"], "c2")
        self.assertEqual(self.store.get_release("c2")["superseded_by"], "c3")
        self.assertEqual(self.store.head_release_id(), "c3")

    def test_head_follows_first_persistence_order(self):
        self.store.insert_release_with_fence("r1", "aa", b"x")
        self._receipt("r1")
        self.store.insert_release_with_fence("r2", "bb", b"y")
        self.assertEqual(self.store.head_release_id(), "r1")
        self.store.update_state("r1", core.STATE_COMPLETED)
        self.assertEqual(self.store.head_release_id(), "r2")
        self.store.update_state("r2", core.STATE_REJECTED)
        self.assertIsNone(self.store.head_release_id())

    def test_seq_reflects_persistence_order(self):
        self.store.insert_release_with_fence("r1", "aa", b"x")
        self.store.insert_release_with_fence("r2", "bb", b"y")
        self.assertLess(self.store.get_release("r1")["seq"],
                        self.store.get_release("r2")["seq"])
        listed = self.store.list_releases()
        self.assertEqual([r["release_id"] for r in listed], ["r1", "r2"])
        self.assertEqual(listed[0]["superseded_by"], "r2")
        self.assertIsNone(listed[1]["superseded_by"])

    def test_duplicate_insert_raises_and_preserves_existing_rows(self):
        self.store.insert_release_with_fence("r1", "aa", b"x")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.insert_release_with_fence("r1", "aa", b"x")
        # The failed transaction rolled back cleanly; the store stays usable.
        self.assertEqual(self.store.get_release("r1")["state"], core.STATE_PENDING)
        state, _ = self.store.insert_release_with_fence("r2", "bb", b"y")
        self.assertEqual(state, core.STATE_PENDING)
        self.assertEqual(self.store.get_release("r1")["state"], core.STATE_SUPERSEDED)

    def test_queue_conclusion_survives_reopen(self):
        self.store.insert_release_with_fence("r1", "aa", b"x")
        self._receipt("r1")
        self.store.insert_release_with_fence("r2", "bb", b"y")
        self.store.insert_release_with_fence("r3", "cc", b"z")  # supersedes r2
        self.store.close()
        self.store = Store(os.path.join(self.dir.name, "control.db"))
        self.assertEqual(self.store.head_release_id(), "r1")
        self.assertEqual(self.store.get_release("r2")["state"], core.STATE_SUPERSEDED)
        self.assertEqual(self.store.get_release("r2")["superseded_by"], "r3")
        self.assertEqual(self.store.get_release("r3")["state"], core.STATE_WAITING)
        self.assertEqual(self.store.blocking_release_id("r3"), "r1")


if __name__ == "__main__":
    unittest.main()
