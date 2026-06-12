"""
Abstract base class for all State Sidecar storage backends.

Every backend (SQLite, Redis, …) must implement this interface.
The MCP server only calls methods on this ABC, so swapping the backend
requires zero changes to the tool layer.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

from ..models import (
    LeaseResult,
    WorkflowMeta,
    ResumeContext,
    SessionSaveResult,
    SessionRestoreResult,
    HistoryEntry,
    HistoryLogResult,
)


_MISSING = object()  # canonical sentinel for "key not found"; import from here in all backends and server


class StateBackend(ABC):
    """Contract that every storage backend must satisfy."""

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    @abstractmethod
    async def initialise(self) -> None:
        """Create tables / check connectivity. Called once at server startup."""

    @abstractmethod
    async def close(self) -> None:
        """Release connections. Called at server shutdown."""

    # ── Key-Value ─────────────────────────────────────────────────────────────

    @abstractmethod
    async def kv_get(self, key: str) -> Optional[Any]:
        """Return the value for *key*, or None if missing / expired."""

    @abstractmethod
    async def kv_set(self, key: str, value: Any, ttl: Optional[int] = None, agent_id: Optional[str] = None) -> None:
        """Upsert *key* → *value*.  If *ttl* is given (seconds), expire after that.  agent_id is recorded in history (optional)."""

    @abstractmethod
    async def kv_delete(self, key: str) -> bool:
        """Delete *key*.  Returns True if the key existed."""

    @abstractmethod
    async def kv_list(self, prefix: str = "") -> list[str]:
        """Return all live (non-expired) keys that start with *prefix*."""

    # ── Leases ────────────────────────────────────────────────────────────────

    @abstractmethod
    async def lease_acquire(
        self, resource_id: str, holder_id: str, ttl: int
    ) -> LeaseResult:
        """
        Try to acquire an exclusive TTL-based lease on *resource_id*.

        Returns LeaseResult(acquired=True) on success or
        LeaseResult(acquired=False, holder=<current_holder>) on failure.
        Expired leases are silently reclaimed before attempting acquisition.
        """

    @abstractmethod
    async def lease_release(self, resource_id: str, holder_id: str) -> bool:
        """
        Release a held lease.  Returns True if the caller actually held it.
        A no-op (returns False) if the lease has already expired or belongs
        to a different holder.
        """

    @abstractmethod
    async def lease_renew(
        self, resource_id: str, holder_id: str, ttl: int
    ) -> tuple[bool, Optional[float]]:
        """
        Extend an existing lease TTL.
        Returns (True, new_expires_at) on success, (False, None) if the caller
        no longer holds the lease.
        """

    # ── Workflow Registry ─────────────────────────────────────────────────────

    @abstractmethod
    async def workflow_create(
        self, name: str, tags: dict[str, str]
    ) -> WorkflowMeta:
        """Mint a new run_id and persist the workflow record."""

    @abstractmethod
    async def workflow_get(self, run_id: str) -> Optional[WorkflowMeta]:
        """Fetch a single workflow by run_id. Returns None if not found."""

    @abstractmethod
    async def workflow_list(
        self,
        tags: Optional[dict[str, str]] = None,
        status: Optional[str] = None,
    ) -> list[WorkflowMeta]:
        """
        List workflows, optionally filtered by tag key/value pairs and/or
        status.  All provided tags must match (AND semantics).
        """

    @abstractmethod
    async def workflow_update(
        self,
        run_id: str,
        *,
        status: Optional[str] = None,
        agent_id: Optional[str] = None,
        last_step: Optional[int] = None,
        clear_agent: bool = False,
    ) -> Optional[WorkflowMeta]:
        """
        Partial update on a workflow record.  Returns the updated record
        or None if run_id does not exist.
        """

    @abstractmethod
    async def workflow_claim(
        self, run_id: str, agent_id: str
    ) -> tuple[bool, Optional[str]]:
        """
        Atomically transition a workflow from "created" to "claimed".

        Returns (True, None) on success.
        Returns (False, reason) if the workflow is already claimed, does not
        exist, or is in a terminal state.
        """

    # ── Step Checkpointing ────────────────────────────────────────────────────

    @abstractmethod
    async def checkpoint(
        self, run_id: str, step: int, output: Any
    ) -> None:
        """
        Persist a step's output and advance last_step atomically.
        The output is stored under the key
        ``workflow:{run_id}:step:{step}``.
        """

    @abstractmethod
    async def get_resume_context(self, run_id: str) -> Optional[ResumeContext]:
        """
        Return everything a replacement agent needs to resume a workflow:
        the WorkflowMeta plus all step outputs collected so far.
        Returns None if run_id does not exist.
        """

    # ── Diagnostics ───────────────────────────────────────────────────────────

    @abstractmethod
    async def reset(self) -> int:
        """
        Delete all rows from all six tables (kv_store, leases, workflows,
        step_outputs, history, sessions). Returns the total number of rows deleted.
        Use between demo runs to guarantee a clean starting state.
        WARNING: This also permanently erases all session snapshots and audit history.
        """

    @abstractmethod
    async def key_count(self) -> int:
        """Return the total number of live KV entries (excluding leases/workflows)."""

    @abstractmethod
    async def workflow_count(self) -> int:
        """Return the total number of registered workflows."""

    # ── Session & History ─────────────────────────────────────────────────────

    @abstractmethod
    async def session_save(self, session_id: str, context: dict) -> None:
        """
        Upsert a JSON context blob under session_id.
        saved_at is set on first insert; updated_at is refreshed on every upsert.
        """

    @abstractmethod
    async def session_restore(self, session_id: str) -> Any:
        """
        Return the context dict for session_id, or _MISSING sentinel if not found.
        Callers must check `result is not _MISSING` before using the value.
        """

    @abstractmethod
    async def history_get(self, key: Optional[str] = None, n: int = 10) -> list[HistoryEntry]:
        """
        Return last N history rows in reverse chronological order.
        If key is provided, filter to that key only.
        Returns list[HistoryEntry] — field names match DB columns.
        """
