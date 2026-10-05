"""In-process end-to-end tests: control service + two mirror repos."""
import base64
import hashlib
import os
import tempfile
import time
import unittest
import urllib.parse

from app.common.httpjson import http_json
from app.control.core import op_key
from app.control.server import ControlService
from app.repo.server import RepoService


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def b64(b: bytes) -> str:
    return base64.b64encode(b).decode()


def wait_for(pred, timeout=20.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            value = pred()
            if value:
                return value
        except Exception:
            pass
        time.sleep(interval)
    raise AssertionError("timed out waiting for condition")


class Cluster:
    """A full deployment wired together on ephemeral localhost ports."""

    def __init__(self):
        self.dir = tempfile.TemporaryDirectory()
        root = self.dir.name
        self.repo_a = RepoService(data_dir=os.path.join(root, "ra"), name="repo-a",
                                  secret="s-a", port=0, fault_hooks=True)
        self.repo_b = RepoService(data_dir=os.path.join(root, "rb"), name="repo-b",
                                  secret="s-b", port=0, fault_hooks=True)
        self.repo_a.start()
        self.repo_b.start()
        self.control_dir = os.path.join(root, "ctl")
        self.control = self._new_control()
        self.control.start()

    def _new_control(self) -> ControlService:
        return ControlService(
            data_dir=self.control_dir,
            repo_urls={"repo-a": self.repo_a.url, "repo-b": self.repo_b.url},
            repo_secrets={"repo-a": "s-a", "repo-b": "s-b"},
            port=0, worker_interval=0.05, repo_timeout=1.0, fault_hooks=True,
        )

    def restart_control(self):
        """Simulate a control-service restart over the same durable state."""
        self.control.stop()
        self.control = self._new_control()
        self.control.start()

    def close(self):
        self.control.stop()
        self.repo_a.stop()
        self.repo_b.stop()
        self.dir.cleanup()


class ControlApi:
    """Shared HTTP helpers for tests driving a Cluster."""

    def post_release(self, rid: str, artifact: bytes):
        return http_json("POST", f"{self.c.control.url}/api/releases",
                         {"release_id": rid, "artifact_b64": b64(artifact)}, timeout=5)

    def state_of(self, rid: str) -> dict:
        s, b = http_json("GET", f"{self.c.control.url}/api/releases/{rid}", timeout=5)
        return b if s == 200 else {}

    def wait_state(self, rid: str, state: str, timeout=20.0) -> dict:
        return wait_for(
            lambda: (lambda b: b if b.get("state") == state else None)(self.state_of(rid)),
            timeout,
        )

    def repo_state(self, repo) -> dict:
        return http_json("GET", f"{repo.url}/v1/state", timeout=5)[1]


class IntegrationTests(ControlApi, unittest.TestCase):
    def setUp(self):
        self.c = Cluster()

    def tearDown(self):
        self.c.close()

    # ---- tests ----
    def test_happy_path_and_idempotent_duplicate(self):
        s, b = self.post_release("rel-1", b"payload-1")
        self.assertEqual(s, 202)
        self.assertEqual(b["sha256"], sha(b"payload-1"))
        d = self.wait_state("rel-1", "COMPLETED")
        self.assertEqual(d["current_digest"], sha(b"payload-1"))
        for repo in ("repo-a", "repo-b"):
            self.assertEqual(d["repos"][repo]["prepare"]["digest"], sha(b"payload-1"))
            self.assertEqual(d["repos"][repo]["activate"]["digest"], sha(b"payload-1"))
        self.assertEqual(self.repo_state(self.c.repo_a)["active_digest"], sha(b"payload-1"))
        self.assertEqual(self.repo_state(self.c.repo_b)["active_digest"], sha(b"payload-1"))

        # Duplicate submission: same id + same bytes -> replay, no second activation.
        s, b = self.post_release("rel-1", b"payload-1")
        self.assertEqual(s, 200)
        self.assertEqual(b["state"], "COMPLETED")
        time.sleep(0.5)
        self.assertEqual(self.repo_state(self.c.repo_a)["activation_count"], 1)
        self.assertEqual(self.repo_state(self.c.repo_b)["activation_count"], 1)

    def test_used_id_with_different_artifact_preserves_state(self):
        self.post_release("rel-2", b"aaa")
        self.wait_state("rel-2", "COMPLETED")
        s, b = self.post_release("rel-2", b"bbb")
        self.assertEqual(s, 409)
        self.assertEqual(b["error"]["code"], "release_id_in_use")
        d = self.state_of("rel-2")
        self.assertEqual(d["state"], "COMPLETED")
        self.assertEqual(d["sha256"], sha(b"aaa"))

    def test_validation_feedback(self):
        s, b = http_json("POST", f"{self.c.control.url}/api/releases",
                         {"release_id": "rel-3", "artifact_b64": "%%%invalid%%%"}, timeout=5)
        self.assertEqual(s, 400)
        self.assertEqual(b["error"]["code"], "invalid_base64")

        big = base64.b64encode(bytes(64 * 1024 + 1)).decode()
        s, b = http_json("POST", f"{self.c.control.url}/api/releases",
                         {"release_id": "rel-3", "artifact_b64": big}, timeout=5)
        self.assertEqual(s, 413)
        self.assertEqual(b["error"]["code"], "artifact_too_large")

        s, b = http_json("POST", f"{self.c.control.url}/api/releases",
                         {"release_id": "bad id!", "artifact_b64": b64(b"x")}, timeout=5)
        self.assertEqual(s, 400)
        self.assertEqual(b["error"]["code"], "invalid_release_id")

        s, _ = http_json("GET", f"{self.c.control.url}/api/releases/rel-3", timeout=5)
        self.assertEqual(s, 404)

    def test_boundary_64kib_completes(self):
        s, _ = self.post_release("rel-4", bytes(64 * 1024))
        self.assertEqual(s, 202)
        d = self.wait_state("rel-4", "COMPLETED")
        self.assertEqual(d["sha256"], sha(bytes(64 * 1024)))

    def test_foreign_digest_locks_rejection_and_preserves_pointer(self):
        self.post_release("rel-5", b"first")
        self.wait_state("rel-5", "COMPLETED")
        before = self.repo_state(self.c.repo_b)

        http_json("POST", f"{self.c.repo_b.url}/fault/corrupt-next-activate", {}, timeout=5)
        self.post_release("rel-6", b"second")
        d = self.wait_state("rel-6", "REJECTED")
        self.assertIsNone(d["current_digest"])
        self.assertTrue(d["error"])

        after = self.repo_state(self.c.repo_b)
        self.assertEqual(after["active_digest"], before["active_digest"])
        self.assertEqual(after["activation_count"], before["activation_count"])

        time.sleep(0.5)
        self.assertEqual(self.state_of("rel-6")["state"], "REJECTED")  # locked
        s, b = self.post_release("rel-6", b"second")
        self.assertEqual(s, 200)
        self.assertEqual(b["state"], "REJECTED")

    def test_restart_converges_from_repo_receipts(self):
        http_json("POST", f"{self.c.repo_b.url}/fault/disconnect-after-activate", {}, timeout=5)
        s, _ = self.post_release("rel-7", b"third")
        self.assertEqual(s, 202)

        def stalled():
            d = self.state_of("rel-7")
            repos = d.get("repos") or {}
            a = (repos.get("repo-a") or {}).get("activate")
            bb = (repos.get("repo-b") or {}).get("activate")
            if not (a and not bb and d.get("state") != "COMPLETED"):
                return None
            s, _ = http_json("GET", f"{self.c.repo_b.url}/v1/state", timeout=2)
            return d if s == 503 else None

        wait_for(stalled)
        s, _ = http_json("GET", f"{self.c.repo_b.url}/v1/state", timeout=2)
        self.assertEqual(s, 503)  # repo-b is dark

        self.c.restart_control()
        time.sleep(0.5)
        self.assertNotEqual(self.state_of("rel-7").get("state"), "COMPLETED")

        http_json("POST", f"{self.c.repo_b.url}/fault/recover", {}, timeout=5)
        d = self.wait_state("rel-7", "COMPLETED")
        self.assertEqual(d["current_digest"], sha(b"third"))

        sb = self.repo_state(self.c.repo_b)
        self.assertEqual(sb["active_digest"], sha(b"third"))
        self.assertEqual(sb["activation_count"], 1)  # no second activation

        key = urllib.parse.quote(op_key("rel-7", "repo-b", "activate"), safe="")
        s, b = http_json("GET", f"{self.c.repo_b.url}/v1/ops/{key}", timeout=5)
        self.assertEqual(b["receipt"]["receipt_id"],
                         d["repos"]["repo-b"]["activate"]["receipt_id"])

    def test_health_and_console_page(self):
        s, b = http_json("GET", f"{self.c.control.url}/healthz", timeout=5)
        self.assertEqual(s, 200)
        self.assertEqual(b["status"], "ok")
        self.assertTrue(b["boot_id"])
        from app.common.httpjson import http_text
        s, text = http_text("GET", f"{self.c.control.url}/", timeout=5)
        self.assertEqual(s, 200)
        self.assertIn('id="feedback"', text)
        self.assertIn('id="artifact"', text)


class FenceTests(ControlApi, unittest.TestCase):
    """Single activation fence: supersede / wait / recovery semantics."""

    def setUp(self):
        self.c = Cluster()

    def tearDown(self):
        self.c.close()

    # ---- helpers ----
    def disconnect_repos(self):
        for repo in (self.c.repo_a, self.c.repo_b):
            s, _ = http_json("POST", f"{repo.url}/fault/disconnect", {}, timeout=5)
            self.assertEqual(s, 200)

    def recover_repos(self):
        for repo in (self.c.repo_a, self.c.repo_b):
            s, _ = http_json("POST", f"{repo.url}/fault/recover", {}, timeout=5)
            self.assertEqual(s, 200)

    def wait_activating_with_b_dark(self, rid: str) -> dict:
        """Wait until rid holds a repo-a activate receipt while repo-b is dark."""
        def stalled():
            d = self.state_of(rid)
            repos = d.get("repos") or {}
            a = (repos.get("repo-a") or {}).get("activate")
            bb = (repos.get("repo-b") or {}).get("activate")
            if not (a and not bb and d.get("state") == "ACTIVATING"):
                return None
            s, _ = http_json("GET", f"{self.c.repo_b.url}/v1/state", timeout=2)
            return d if s == 503 else None
        return wait_for(stalled)

    # ---- tests ----
    def test_supersede_candidate_without_activation_evidence(self):
        self.disconnect_repos()
        s, b = self.post_release("fence-old", b"old-bytes")
        self.assertEqual(s, 202)
        self.assertEqual(b["state"], "PENDING")
        s, b = self.post_release("fence-new", b"new-bytes")
        self.assertEqual(s, 202)
        # The older candidate had no activation evidence -> superseded;
        # the newer one immediately holds the fence.
        self.assertEqual(b["state"], "PENDING")

        d = self.state_of("fence-old")
        self.assertEqual(d["state"], "SUPERSEDED")
        self.assertEqual(d["superseded_by"], "fence-new")
        self.assertIsNone(d["current_digest"])

        self.recover_repos()
        d = self.wait_state("fence-new", "COMPLETED")
        self.assertEqual(d["current_digest"], sha(b"new-bytes"))

        # The superseded release never triggered any repo-side operation.
        d = self.state_of("fence-old")
        self.assertEqual(d["state"], "SUPERSEDED")
        self.assertEqual(d["superseded_by"], "fence-new")
        for repo in ("repo-a", "repo-b"):
            self.assertIsNone(d["repos"][repo]["prepare"])
            self.assertIsNone(d["repos"][repo]["activate"])
        for repo in (self.c.repo_a, self.c.repo_b):
            st = self.repo_state(repo)
            self.assertEqual(st["prepare_count"], 1)
            self.assertEqual(st["activation_count"], 1)
            self.assertEqual(st["active_digest"], sha(b"new-bytes"))
            key = urllib.parse.quote(op_key("fence-old", st["repo"], "prepare"), safe="")
            s, _ = http_json("GET", f"{repo.url}/v1/ops/{key}", timeout=5)
            self.assertEqual(s, 404)  # no repo-side op key for the old candidate

        # Idempotent replay of a superseded release keeps its terminal state.
        s, b = self.post_release("fence-old", b"old-bytes")
        self.assertEqual(s, 200)
        self.assertEqual(b["state"], "SUPERSEDED")
        self.assertEqual(b["superseded_by"], "fence-new")
        s, b = self.post_release("fence-old", b"other-bytes")
        self.assertEqual(s, 409)
        self.assertEqual(b["error"]["code"], "release_id_in_use")

    def test_supersede_chain_newest_submission_wins(self):
        self.disconnect_repos()
        self.post_release("chain-1", b"c1")
        self.post_release("chain-2", b"c2")
        s, b = self.post_release("chain-3", b"c3")
        self.assertEqual(s, 202)
        self.assertEqual(b["state"], "PENDING")

        self.assertEqual(self.state_of("chain-1")["state"], "SUPERSEDED")
        self.assertEqual(self.state_of("chain-1")["superseded_by"], "chain-2")
        self.assertEqual(self.state_of("chain-2")["state"], "SUPERSEDED")
        self.assertEqual(self.state_of("chain-2")["superseded_by"], "chain-3")

        self.recover_repos()
        d = self.wait_state("chain-3", "COMPLETED")
        self.assertEqual(d["current_digest"], sha(b"c3"))
        for repo in (self.c.repo_a, self.c.repo_b):
            st = self.repo_state(repo)
            self.assertEqual(st["activation_count"], 1)  # only the newest one
            self.assertEqual(st["active_digest"], sha(b"c3"))
        self.assertEqual(self.state_of("chain-1")["state"], "SUPERSEDED")
        self.assertEqual(self.state_of("chain-2")["state"], "SUPERSEDED")

    def test_waits_for_older_candidate_with_activation_evidence(self):
        http_json("POST", f"{self.c.repo_b.url}/fault/disconnect-after-activate",
                  {}, timeout=5)
        s, b = self.post_release("hold-1", b"hold-bytes")
        self.assertEqual(s, 202)
        self.wait_activating_with_b_dark("hold-1")

        s, b = self.post_release("hold-2", b"next-bytes")
        self.assertEqual(s, 202)
        self.assertEqual(b["state"], "WAITING")
        self.assertEqual(b["blocked_by"], "hold-1")

        # The evidence-bearing older candidate is NOT superseded.
        d = self.state_of("hold-1")
        self.assertEqual(d["state"], "ACTIVATING")
        self.assertIsNone(d["superseded_by"])

        # While hold-1 has not converged, hold-2 must not touch any repo:
        # nothing on the reachable repo, and no evidence in the control view.
        for op in ("prepare", "activate"):
            key = urllib.parse.quote(op_key("hold-2", "repo-a", op), safe="")
            s, _ = http_json("GET", f"{self.c.repo_a.url}/v1/ops/{key}", timeout=5)
            self.assertEqual(s, 404)
        d = self.state_of("hold-2")
        for repo in ("repo-a", "repo-b"):
            self.assertIsNone(d["repos"][repo]["prepare"])
            self.assertIsNone(d["repos"][repo]["activate"])

        http_json("POST", f"{self.c.repo_b.url}/fault/recover", {}, timeout=5)
        d1 = self.wait_state("hold-1", "COMPLETED")
        self.assertEqual(d1["current_digest"], sha(b"hold-bytes"))
        # hold-2 enters coordination only after hold-1 converged.
        d2 = self.wait_state("hold-2", "COMPLETED")
        self.assertEqual(d2["current_digest"], sha(b"next-bytes"))
        # Ordering proof: hold-2's first repo-side receipt is younger than the
        # moment hold-1 converged (same host clock, ISO-8601 strings).
        self.assertGreater(d2["repos"]["repo-a"]["prepare"]["ts"], d1["updated_at"])
        for repo in (self.c.repo_a, self.c.repo_b):
            st = self.repo_state(repo)
            self.assertEqual(st["activation_count"], 2)  # hold-1 once, hold-2 once
            self.assertEqual(st["active_digest"], sha(b"next-bytes"))

    def test_restart_recovers_same_queue_conclusion(self):
        http_json("POST", f"{self.c.repo_b.url}/fault/disconnect-after-activate",
                  {}, timeout=5)
        self.post_release("rec-1", b"rec-one")
        self.wait_activating_with_b_dark("rec-1")
        s, b = self.post_release("rec-2", b"rec-two")
        self.assertEqual(b["state"], "WAITING")

        self.c.restart_control()

        # Same queue conclusion after the restart: rec-1 keeps the fence,
        # rec-2 still waits behind it, neither was superseded.
        d1 = self.state_of("rec-1")
        self.assertEqual(d1["state"], "ACTIVATING")
        self.assertIsNone(d1["superseded_by"])
        d2 = self.state_of("rec-2")
        self.assertEqual(d2["state"], "WAITING")
        self.assertEqual(d2["blocked_by"], "rec-1")

        http_json("POST", f"{self.c.repo_b.url}/fault/recover", {}, timeout=5)
        self.wait_state("rec-1", "COMPLETED")
        self.wait_state("rec-2", "COMPLETED")
        sb = self.repo_state(self.c.repo_b)
        self.assertEqual(sb["activation_count"], 2)  # rec-1 adopted, rec-2 once
        self.assertEqual(sb["active_digest"], sha(b"rec-two"))

    def test_superseded_state_survives_restart(self):
        self.disconnect_repos()
        self.post_release("sup-1", b"s1")
        self.post_release("sup-2", b"s2")
        self.assertEqual(self.state_of("sup-1")["state"], "SUPERSEDED")

        self.c.restart_control()

        d = self.state_of("sup-1")
        self.assertEqual(d["state"], "SUPERSEDED")
        self.assertEqual(d["superseded_by"], "sup-2")

        self.recover_repos()
        self.wait_state("sup-2", "COMPLETED")
        d = self.state_of("sup-1")
        self.assertEqual(d["state"], "SUPERSEDED")  # locked, never drifts
        for repo in ("repo-a", "repo-b"):
            self.assertIsNone(d["repos"][repo]["prepare"])
            self.assertIsNone(d["repos"][repo]["activate"])

    def test_list_and_detail_expose_queue_fields(self):
        self.disconnect_repos()
        self.post_release("q-1", b"q1")
        self.post_release("q-2", b"q2")
        s, b = http_json("GET", f"{self.c.control.url}/api/releases", timeout=5)
        self.assertEqual(s, 200)
        rels = {r["release_id"]: r for r in b["releases"]}
        self.assertEqual(rels["q-1"]["state"], "SUPERSEDED")
        self.assertEqual(rels["q-1"]["superseded_by"], "q-2")
        self.assertEqual(rels["q-2"]["state"], "PENDING")
        self.assertLess(rels["q-1"]["seq"], rels["q-2"]["seq"])

        d = self.state_of("q-2")
        self.assertIsNone(d["blocked_by"])  # fence head waits for nobody
        self.recover_repos()
        self.wait_state("q-2", "COMPLETED")


if __name__ == "__main__":
    unittest.main()
