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


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.c = Cluster()

    def tearDown(self):
        self.c.close()

    # ---- helpers ----
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

    def set_stall(self, repo, flag: bool):
        path = "/fault/stall-activate" if flag else "/fault/unstall-activate"
        return http_json("POST", f"{repo.url}{path}", {}, timeout=5)

    def activate_op_status(self, repo, rid: str, op: str = "activate") -> int:
        key = urllib.parse.quote(op_key(rid, repo.name, op), safe="")
        return http_json("GET", f"{repo.url}/v1/ops/{key}", timeout=5)[0]

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

    # ---- activation fence ----
    def test_new_candidate_supersedes_head_without_activation_evidence(self):
        # Stall both repos' activations while staying reachable (GET op -> 404).
        self.set_stall(self.c.repo_a, True)
        self.set_stall(self.c.repo_b, True)
        self.post_release("rel-old", b"old")
        d = wait_for(lambda: self.state_of("rel-old") if self.state_of("rel-old").get(
            "state") == "ACTIVATING" else None)
        # Prepares landed for both repos, but no activation evidence exists yet.
        for repo in (self.c.repo_a, self.c.repo_b):
            self.assertEqual(self.activate_op_status(repo, "rel-old"), 404)

        # A newer candidate overtakes the evidence-free old one.
        self.post_release("rel-new", b"new")
        old = self.wait_state("rel-old", "SUPERSEDED")
        self.assertEqual(old["superseded_by"], "rel-new")
        self.assertIsNone(old["current_digest"])

        # Release the stall; the newer release must be the only activation.
        self.set_stall(self.c.repo_a, False)
        self.set_stall(self.c.repo_b, False)
        new = self.wait_state("rel-new", "COMPLETED")
        self.assertEqual(new["current_digest"], sha(b"new"))

        time.sleep(0.6)
        old = self.state_of("rel-old")
        self.assertEqual(old["state"], "SUPERSEDED")  # locked, never re-prepared
        for repo in (self.c.repo_a, self.c.repo_b):
            # The old candidate never activated: no activate op, pointer is new.
            self.assertEqual(self.activate_op_status(repo, "rel-old"), 404)
            st = self.repo_state(repo)
            self.assertEqual(st["active_digest"], sha(b"new"))
            self.assertEqual(st["activation_count"], 1)

    def test_head_with_one_activation_evidence_blocks_newer_candidates(self):
        # repo-a commits its activation first; repo-b is stalled (reachable,
        # GET activate op -> 404). One persisted activation must hold the fence.
        self.set_stall(self.c.repo_b, True)
        self.post_release("rel-head", b"head")

        def head_evidence_partial():
            d = self.state_of("rel-head")
            a = (d.get("repos", {}).get("repo-a") or {}).get("activate")
            bb = (d.get("repos", {}).get("repo-b") or {}).get("activate")
            return d if (a and not bb) else None

        wait_for(head_evidence_partial)
        self.assertEqual(self.activate_op_status(self.c.repo_a, "rel-head"), 200)
        self.assertEqual(self.activate_op_status(self.c.repo_b, "rel-head"), 404)

        # A newer submission must queue behind the fence and never touch a repo.
        self.post_release("rel-wait", b"wait")
        w = self.wait_state("rel-wait", "WAITING")
        self.assertTrue(any(x["release_id"] == "rel-head" for x in w["waiting_for"]))
        time.sleep(0.8)
        self.assertEqual(self.state_of("rel-wait")["state"], "WAITING")
        for repo in (self.c.repo_a, self.c.repo_b):
            self.assertEqual(self.activate_op_status(repo, "rel-wait"), 404)

        # Let the head converge; only then does the waiter enter coordination.
        self.set_stall(self.c.repo_b, False)
        self.wait_state("rel-head", "COMPLETED")
        waiter = self.wait_state("rel-wait", "COMPLETED")
        self.assertEqual(waiter["current_digest"], sha(b"wait"))

        time.sleep(0.4)
        for repo in (self.c.repo_a, self.c.repo_b):
            self.assertEqual(self.repo_state(repo)["active_digest"], sha(b"wait"))
        # Each release activated exactly once per repo (never mixed pointers).
        self.assertEqual(self.repo_state(self.c.repo_a)["activation_count"], 2)
        self.assertEqual(self.repo_state(self.c.repo_b)["activation_count"], 2)

    def test_rejected_head_releases_fence_to_waiter_in_order(self):
        self.post_release("rel-h1", b"h1")
        self.wait_state("rel-h1", "COMPLETED")
        # Next head will be rejected by repo-b's foreign-digest receipt.
        http_json("POST", f"{self.c.repo_b.url}/fault/corrupt-next-activate", {}, timeout=5)
        self.post_release("rel-h2", b"h2")
        self.wait_state("rel-h2", "REJECTED")
        self.post_release("rel-h3", b"h3")
        d = self.wait_state("rel-h3", "COMPLETED")
        self.assertEqual(d["current_digest"], sha(b"h3"))
        for repo in (self.c.repo_a, self.c.repo_b):
            self.assertEqual(self.repo_state(repo)["active_digest"], sha(b"h3"))

    def test_supersession_decision_is_recovered_after_restart(self):
        self.set_stall(self.c.repo_a, True)
        self.set_stall(self.c.repo_b, True)
        self.post_release("rel-r1", b"r1")
        wait_for(lambda: self.state_of("rel-r1")
                 if self.state_of("rel-r1").get("state") == "ACTIVATING" else None)
        self.post_release("rel-r2", b"r2")
        self.wait_state("rel-r1", "SUPERSEDED")
        # r2 is WAITING only because repos are stalled and it is now the head.
        self.wait_state("rel-r2", "ACTIVATING")

        # Restart before any activation exists anywhere; the same queue view
        # (r1 SUPERSEDED, r2 the sole head) must be reconstructed from durable
        # intents + authoritative repo 404s.
        self.c.restart_control()
        time.sleep(0.6)
        self.assertEqual(self.state_of("rel-r1")["state"], "SUPERSEDED")
        self.assertEqual(self.state_of("rel-r1")["superseded_by"], "rel-r2")
        self.assertNotEqual(self.state_of("rel-r2")["state"], "SUPERSEDED")

        self.set_stall(self.c.repo_a, False)
        self.set_stall(self.c.repo_b, False)
        d = self.wait_state("rel-r2", "COMPLETED")
        self.assertEqual(d["current_digest"], sha(b"r2"))
        self.assertEqual(self.state_of("rel-r1")["state"], "SUPERSEDED")

    def test_unreachable_repo_blocks_supersession_until_absence_is_provable(self):
        # repo-a is reachable but stalls the activation (authoritative 404);
        # repo-b goes dark so its absence cannot be proven.
        self.set_stall(self.c.repo_a, True)
        self.set_stall(self.c.repo_b, True)
        self.post_release("rel-u1", b"u1")
        self.wait_state("rel-u1", "ACTIVATING")
        http_json("POST", f"{self.c.repo_b.url}/fault/disconnect", {}, timeout=5)
        self.post_release("rel-u2", b"u2")
        time.sleep(1.0)
        # repo-a 404 but repo-b unknown: absence is NOT provable, head retained.
        self.assertNotEqual(self.state_of("rel-u1")["state"], "SUPERSEDED")
        self.assertEqual(self.state_of("rel-u2")["state"], "WAITING")

        # repo-b recovers: BOTH repos authoritatively answer 404, so the old
        # candidate is proven evidence-free and is now superseded by the newer.
        http_json("POST", f"{self.c.repo_b.url}/fault/recover", {}, timeout=5)
        old = self.wait_state("rel-u1", "SUPERSEDED")
        self.assertEqual(old["superseded_by"], "rel-u2")
        self.set_stall(self.c.repo_a, False)
        self.set_stall(self.c.repo_b, False)
        new = self.wait_state("rel-u2", "COMPLETED")
        self.assertEqual(new["current_digest"], sha(b"u2"))
        time.sleep(0.4)
        for repo in (self.c.repo_a, self.c.repo_b):
            st = self.repo_state(repo)
            self.assertEqual(st["active_digest"], sha(b"u2"))
            self.assertEqual(st["activation_count"], 1)  # old candidate never fired


if __name__ == "__main__":
    unittest.main()
