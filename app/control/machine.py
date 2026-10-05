"""Release state machine: a single activation fence across both mirror repos.

Publication order is the order in which release intents were first persisted
(an immutable monotonic ``seq``). At any moment only the oldest still-live
candidate (the fence head) may drive repository operations, and the same
candidate is activated on BOTH repos, so the two repo active pointers can never
belong to releases at different positions in the queue.

Supersession rule:
  - A fence head that has left NO persisted activation evidence in either repo
    may be overtaken by a later submission: it is locked as SUPERSEDED (recording
    which release replaced it) and must never trigger prepare/activate again.
  - As soon as the head has persisted activation evidence in ANY repo, later
    candidates WAIT until it converges to COMPLETED or REJECTED from the
    existing receipts; only then does the next candidate enter coordination.

Crash safety: intents are persisted before any repo call and every observed
repo receipt is persisted immediately. Evidence absence is only acted upon when
BOTH repos authoritatively answer 404 for the derived activate op key; an
unreachable repo means the head is retained, so a response loss followed by a
restart can never let a newer release jump an activation that already happened.
"""
from __future__ import annotations

import base64
import logging
import threading

from app.common.httpjson import TransportError
from app.common.receipts import verify_receipt
from app.control import core

log = logging.getLogger("control.machine")

EVIDENCE_PRESENT = "present"
EVIDENCE_ABSENT = "absent"
EVIDENCE_UNKNOWN = "unknown"


class Rejected(Exception):
    """Internal signal: the release has just been locked as REJECTED."""


class ReleaseMachine:
    def __init__(self, store, clients: dict, secrets: dict):
        self.store = store
        self.clients = clients  # repo name -> RepoClient
        self.secrets = secrets  # repo name -> HMAC secret

    def reconcile(self) -> None:
        """One fence pass: supersede stale heads, then advance only the head."""
        ordered = self.store.pending_release_ids(core.TERMINAL_STATES)
        # Resolve any chain of evidence-free stale heads in publication order.
        while len(ordered) >= 2:
            head_id = ordered[0]
            head = self.store.get_release(head_id)
            if head is None or head["state"] in core.TERMINAL_STATES:
                ordered.pop(0)
                continue
            try:
                evidence = self._probe_activation_evidence(head)
            except Rejected:
                # Probing adopted a bad receipt which locked the head REJECTED;
                # re-read on the next tick rather than touching the fence now.
                return
            if any(v == EVIDENCE_UNKNOWN for v in evidence.values()):
                break  # cannot prove absence: retain the head this tick
            if all(v == EVIDENCE_ABSENT for v in evidence.values()):
                overtaken_by = ordered[1]
                if self.store.mark_superseded(head_id, overtaken_by):
                    log.info("release %s superseded by %s before any activation",
                             head_id, overtaken_by)
                ordered.pop(0)
                continue
            break  # head already holds activation evidence: it owns the fence
        if not ordered:
            return
        # Everyone behind the head queues behind the fence.
        for follower_id in ordered[1:]:
            follower = self.store.get_release(follower_id)
            if follower and follower["state"] not in core.TERMINAL_STATES \
                    and follower["state"] != core.STATE_WAITING:
                self.store.update_state(follower_id, core.STATE_WAITING)
        self.advance(ordered[0])

    def advance(self, release_id: str) -> None:
        rel = self.store.get_release(release_id)
        if rel is None or rel["state"] in core.TERMINAL_STATES:
            return
        try:
            self._run(rel)
        except Rejected:
            pass
        except Exception:  # noqa: BLE001 - keep the worker alive
            log.exception("advance failed for release %s", release_id)

    # ---- internals ----
    def _run(self, rel: dict) -> None:
        rid, sha = rel["release_id"], rel["sha256"]
        artifact_b64 = base64.b64encode(rel["artifact"]).decode()

        # Phase 1: prepare both repos with the identical candidate bytes.
        for repo in self.clients:
            receipt = self._ensure_op(rid, repo, core.OP_PREPARE, sha, artifact_b64)
            if receipt is None:
                self._set_state(rid, core.STATE_PREPARING)
                return
            self._check_digest(rid, repo, core.OP_PREPARE, sha, receipt)

        self._set_state(rid, core.STATE_ACTIVATING)

        # Phase 2: activate both repos; only identical digests may complete.
        for repo in self.clients:
            receipt = self._ensure_op(rid, repo, core.OP_ACTIVATE, sha, artifact_b64)
            if receipt is None:
                return
            self._check_digest(rid, repo, core.OP_ACTIVATE, sha, receipt)

        self._set_state(rid, core.STATE_COMPLETED)
        log.info("release %s completed (sha256=%s)", rid, sha)

    def _probe_activation_evidence(self, rel: dict) -> dict:
        """Authoritative per-repo view of persisted ACTIVATE evidence.

        Read-only with respect to the repos: it adopts an existing receipt via
        GET but never issues an activation. EVIDENCE_ABSENT is returned only
        when the repo explicitly answers 404; an unreachable repo is UNKNOWN.
        """
        rid = rel["release_id"]
        out: dict = {}
        for repo in self.clients:
            if self.store.get_receipt(rid, repo, core.OP_ACTIVATE) is not None:
                out[repo] = EVIDENCE_PRESENT
                continue
            client = self.clients[repo]
            key = core.op_key(rid, repo, core.OP_ACTIVATE)
            try:
                status, body = client.get_op(key)
            except TransportError as e:
                log.warning("activation evidence for %s in %s unknown: %s", rid, repo, e)
                out[repo] = EVIDENCE_UNKNOWN
                continue
            if status == 200:
                self._adopt(rid, repo, core.OP_ACTIVATE, key, body.get("receipt"))
                out[repo] = EVIDENCE_PRESENT
            elif status == 404:
                out[repo] = EVIDENCE_ABSENT
            else:
                log.warning("activation evidence probe %s %s -> HTTP %s", rid, repo, status)
                out[repo] = EVIDENCE_UNKNOWN
        return out

    def _ensure_op(self, rid: str, repo: str, op: str, sha: str,
                   artifact_b64: str) -> dict | None:
        """Return the repo receipt for this op, persisting it locally.

        Returns None when the repo is unreachable (the worker retries later).
        Raises Rejected when repo-side evidence conflicts with the release.
        """
        existing = self.store.get_receipt(rid, repo, op)
        if existing is not None:
            return existing
        client = self.clients[repo]
        key = core.op_key(rid, repo, op)
        try:
            # Post-crash adoption: the repo may already hold the first receipt.
            status, body = client.get_op(key)
            if status == 200:
                return self._adopt(rid, repo, op, key, body.get("receipt"))
            if status == 404:
                if op == core.OP_PREPARE:
                    status, body = client.prepare(key, sha, artifact_b64)
                else:
                    status, body = client.activate(key, sha)
                if status in (200, 201):
                    return self._adopt(rid, repo, op, key, body.get("receipt"))
                if status == 409:
                    self._reject_conflict(rid, repo, op, key, body)
                if status == 400 and (body.get("error") or {}).get("code") == "not_prepared":
                    # Repo lost its staging area; re-prepare, retry next tick.
                    client.prepare(core.op_key(rid, repo, core.OP_PREPARE), sha, artifact_b64)
                return None
            # 5xx or anything unexpected: treat as temporarily unreachable.
            log.warning("repo %s %s for %s -> HTTP %s", repo, op, rid, status)
            return None
        except TransportError as e:
            log.warning("repo %s unreachable for %s %s: %s", repo, op, rid, e)
            return None

    def _adopt(self, rid: str, repo: str, op: str, key: str, receipt) -> dict | None:
        if not isinstance(receipt, dict) or not receipt:
            log.warning("repo %s returned an empty receipt for %s", repo, key)
            return None
        if not verify_receipt(self.secrets.get(repo, ""), receipt):
            self._reject(rid, f"镜像仓 {repo} 的 {op} 证据签名校验失败，发布已锁定为拒绝")
            raise Rejected()
        self.store.put_receipt(rid, repo, op, key, str(receipt.get("digest", "")), receipt)
        return receipt

    def _check_digest(self, rid: str, repo: str, op: str, sha: str, receipt: dict) -> None:
        if receipt.get("digest") != sha:
            self._reject(
                rid,
                f"镜像仓 {repo} 的 {op} 回执摘要不属于本发布"
                f"（收到 {receipt.get('digest')}，期望 {sha}），发布已锁定为拒绝",
            )
            raise Rejected()

    def _reject_conflict(self, rid: str, repo: str, op: str, key: str, body: dict) -> None:
        err = body.get("error") or {}
        existing = err.get("existing_digest", "<unknown>")
        # Best effort: keep the conflicting repo-side receipt as evidence.
        try:
            status, body2 = self.clients[repo].get_op(key)
            if status == 200:
                receipt = body2.get("receipt")
                if isinstance(receipt, dict) and receipt:
                    self.store.put_receipt(
                        rid, repo, op, key, str(receipt.get("digest", "")), receipt
                    )
        except TransportError:
            pass
        self._reject(rid, f"镜像仓 {repo} 拒绝了 {op}：操作键已绑定不同摘要（{existing}）")
        raise Rejected()

    def _reject(self, rid: str, message: str) -> None:
        log.error("release %s rejected: %s", rid, message)
        self._set_state(rid, core.STATE_REJECTED, error=message)

    def _set_state(self, rid: str, state: str, error: str | None = None) -> None:
        rel = self.store.get_release(rid)
        if rel is None or rel["state"] in core.TERMINAL_STATES:
            return  # terminal states are locked and never rewritten
        if rel["state"] != state or error:
            self.store.update_state(rid, state, error)


class Worker(threading.Thread):
    """Background reconciler: advances the single activation fence.

    On process start it picks up all unfinished releases from the durable
    store in publication order, which is what makes a control-service restart
    recover the exact same queue conclusion (supersession included) from
    persisted intents and repository receipts.
    """

    def __init__(self, store, machine: ReleaseMachine, interval: float = 0.5):
        super().__init__(name="control-worker", daemon=True)
        self.store = store
        self.machine = machine
        self.interval = interval
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.machine.reconcile()
            except Exception:  # noqa: BLE001 - never kill the worker
                log.exception("worker tick failed")
            self._stop_event.wait(self.interval)
