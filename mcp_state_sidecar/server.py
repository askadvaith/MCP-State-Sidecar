"""
MCP State Sidecar — FastMCP Server
====================================
Exposes 16 MCP tools in 5 groups:

  Group 1 — Core Key-Value Store    (state_set, state_get, state_delete, state_list)
  Group 2 — Workflow Lifecycle       (workflow_create, workflow_discover, workflow_claim,
                                      workflow_checkpoint, workflow_resume,
                                      workflow_status, workflow_list)
  Group 3 — Lease / Concurrency     (lease_acquire, lease_release, lease_renew)
  Group 4 — Observability           (sidecar_health, sidecar_reset)
  Group 5 — Session & History       (session_save, session_restore, history_log)
"""

from __future__ import annotations

import asyncio
import sys
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Optional, AsyncIterator

import fastmcp

from mcp_state_sidecar.config import settings
from mcp_state_sidecar.backends import get_backend
from mcp_state_sidecar.backends.base import StateBackend
from mcp_state_sidecar.backends.base import _MISSING
from mcp_state_sidecar.models import (
    GetResult,
    SetResult,
    DeleteResult,
    ListResult,
    CreateWorkflowResult,
    DiscoverResult,
    ClaimResult,
    CheckpointResult,
    ResumeContext,
    WorkflowStatusResult,
    WorkflowListResult,
    LeaseResult,
    ReleaseResult,
    RenewResult,
    HealthResult,
    ResetResult,
    SessionSaveResult,
    SessionRestoreResult,
    HistoryEntry,
    HistoryLogResult,
)

_backend: StateBackend | None = None
_start_time: float = 0.0


def _get_backend() -> StateBackend:
    """Return the initialised backend or raise RuntimeError if lifespan has not completed yet."""
    if _backend is None:
        raise RuntimeError("Sidecar not yet initialised — retry in a moment")
    return _backend


@asynccontextmanager
async def _lifespan(server: fastmcp.FastMCP) -> AsyncIterator[None]:
    """Start the backend before serving tools; clean up on shutdown."""
    global _backend, _start_time
    _backend = get_backend(settings.backend, **settings.backend_kwargs())
    _start_time = time.time()
    print(
        f"[Sidecar] Starting with backend='{settings.backend}' "
        f"({'db: ' + settings.db_path if settings.backend == 'sqlite' else 'url: ' + settings.redis_url})"
    )
    await _backend.initialise()
    tools = await server.list_tools()
    print(f"[Sidecar] Backend ready. Serving {len(tools)} MCP tools.")
    try:
        yield
    finally:
        await _backend.close()
        print("[Sidecar] Backend closed.")


mcp = fastmcp.FastMCP(
    name="MCP State Sidecar",
    instructions=(
        "A stateful sidecar for distributed MCP workflows. "
        "Provides durable key-value storage, structured workflow lifecycle management "
        "(create → discover → claim → checkpoint → resume), and TTL-based lease locks "
        "for concurrency control across multi-agent, multi-process deployments. "
        "Use workflow_create + workflow_discover + workflow_claim to coordinate distributed "
        "workers without out-of-band coordination."
    ),
    lifespan=_lifespan,
)


# =============================================================================
# GROUP 1 — Core Key-Value Store
# =============================================================================

@mcp.tool()
async def state_set(
    key: str,
    value: Any,
    ttl_seconds: Optional[int] = None,
    agent_id: Optional[str] = None,
) -> SetResult:
    """
    Set a key-value pair in the shared state store.

    The value can be any JSON-serialisable type (dict, list, str, int, …).
    If ttl_seconds is provided, the entry will expire automatically after
    that many seconds. Existing entries are overwritten silently.
    If agent_id is provided, it is recorded in the audit history so the
    caller can be identified in history_log results.

    Use a structured key prefix to avoid collisions in multi-workflow
    deployments, e.g. 'shared:my_namespace:my_key' or simply 'fact_1'.
    """
    await _get_backend().kv_set(key, value, ttl=ttl_seconds, agent_id=agent_id)
    return SetResult(ok=True)


@mcp.tool()
async def state_get(key: str) -> GetResult:
    """
    Get the value stored under *key*.

    Returns found=True and the value if the key exists and has not expired.
    Returns found=False and value=None if the key is missing or expired.
    """
    val = await _get_backend().kv_get(key)
    found = val is not _MISSING
    return GetResult(key=key, value=None if not found else val, found=found)


@mcp.tool()
async def state_delete(key: str) -> DeleteResult:
    """
    Delete a key from the state store.

    Returns ok=True if the key existed and was removed, ok=False if
    the key was not found (idempotent — safe to call multiple times).
    """
    deleted = await _get_backend().kv_delete(key)
    return DeleteResult(ok=deleted)


@mcp.tool()
async def state_list(prefix: Optional[str] = None) -> ListResult:
    """
    List all live (non-expired) keys, optionally filtered by prefix.

    Example: state_list(prefix='fact_') returns all keys starting with 'fact_'.
    Returns an empty list if no keys match.
    """
    keys = await _get_backend().kv_list(prefix or "")
    return ListResult(keys=keys)


# =============================================================================
# GROUP 2 — Workflow Lifecycle & Discovery
# =============================================================================

@mcp.tool()
async def workflow_create(
    name: str,
    tags: Optional[dict[str, str]] = None,
) -> CreateWorkflowResult:
    """
    Register a new workflow and get back a unique run_id.

    Call this from the orchestrator before spawning workers. The returned
    run_id is stored in the sidecar — workers do NOT need to receive it
    out-of-band; they use workflow_discover() to find it.

    Tags are arbitrary key-value metadata used for discovery filtering:
    e.g. tags={'pipeline': 'data-etl', 'customer': 'acme', 'priority': 'high'}

    Status starts as 'created'. Lifecycle: created → claimed → running → done/failed.
    """
    meta = await _get_backend().workflow_create(name=name, tags=tags or {})
    return CreateWorkflowResult(run_id=meta.run_id, name=meta.name, status=meta.status)


@mcp.tool()
async def workflow_discover(
    tags: Optional[dict[str, str]] = None,
    status: Optional[str] = None,
) -> DiscoverResult:
    """
    Find workflows available for a worker to pick up.

    Filters by tag key/value pairs (AND semantics) and/or status.
    Common usage: workflow_discover(status='created') to find unclaimed work.

    Workers should follow this with workflow_claim() to atomically take
    ownership of one of the returned workflows.

    Returns an empty list if no matching workflows are found.
    """
    runs = await _get_backend().workflow_list(tags=tags, status=status)
    return DiscoverResult(runs=runs)


@mcp.tool()
async def workflow_claim(run_id: str, agent_id: str) -> ClaimResult:
    """
    Atomically claim a workflow for this agent.

    Only succeeds if the workflow is in 'created' status. If two agents
    call this concurrently for the same run_id, exactly one will succeed.

    After claiming, call workflow_checkpoint() as each step completes.
    If this agent crashes, use workflow_discover() + workflow_claim() from
    a replacement agent to resume — the sidecar preserves all checkpoint state.

    Returns claimed=False with a reason if the workflow is already taken
    or does not exist.
    """
    ok, reason = await _get_backend().workflow_claim(run_id=run_id, agent_id=agent_id)
    return ClaimResult(
        claimed=ok,
        run_id=run_id,
        agent_id=agent_id if ok else None,
        reason=reason,
    )


@mcp.tool()
async def workflow_checkpoint(
    run_id: str,
    step: int,
    output: Any,
) -> CheckpointResult:
    """
    Atomically persist a completed step's output and advance the step counter.

    Call this immediately after each pipeline step completes, before starting
    the next one. A replacement agent can resume from the last checkpoint
    using workflow_resume().

    The output can be any JSON-serialisable value (dict, list, str, …).
    If step N was already checkpointed, calling again with the same step
    overwrites the stored output (idempotent replay support).
    """
    await _get_backend().checkpoint(run_id=run_id, step=step, output=output)
    return CheckpointResult(ok=True, run_id=run_id, step=step)


@mcp.tool()
async def workflow_resume(run_id: str) -> ResumeContext:
    """
    Get everything a replacement agent needs to resume a crashed workflow.

    Returns:
    - last_step: the highest step that was successfully checkpointed
    - step_outputs: dict mapping step number (as string) to its output
    - meta: full workflow metadata (name, tags, status, agent_id, …)
    """
    ctx = await _get_backend().get_resume_context(run_id)
    if ctx is None:
        raise ValueError(f"Workflow '{run_id}' not found")
    return ctx


@mcp.tool()
async def workflow_status(run_id: str) -> WorkflowStatusResult:
    """
    Get the current status of a workflow — lightweight alternative to workflow_resume.

    Returns the status, last completed step, assigned agent, and timestamps.
    Does NOT return step outputs (use workflow_resume for that).
    """
    meta = await _get_backend().workflow_get(run_id)
    if meta is None:
        raise ValueError(f"Workflow '{run_id}' not found")
    return WorkflowStatusResult(
        run_id=meta.run_id,
        status=meta.status,
        last_step=meta.last_step,
        agent_id=meta.agent_id,
        created_at=meta.created_at,
        updated_at=meta.updated_at,
    )


@mcp.tool()
async def workflow_list() -> WorkflowListResult:
    """
    List all registered workflows (all statuses).

    For filtered listing, use workflow_discover(status=..., tags={...}) instead.
    """
    runs = await _get_backend().workflow_list()
    return WorkflowListResult(runs=runs)


# =============================================================================
# GROUP 3 — Lease / Concurrency Control
# =============================================================================

@mcp.tool()
async def lease_acquire(
    resource_id: str,
    holder_id: str,
    ttl_seconds: int,
) -> LeaseResult:
    """
    Try to acquire an exclusive, TTL-based lease on a named resource.

    Use this to prevent split-brain when multiple agents could simultaneously
    work on the same resource. Only one holder can hold the lease at a time.

    If the current holder crashes, the lease expires automatically after
    ttl_seconds and another agent can acquire it.

    Returns acquired=True on success with the expiry timestamp.
    Returns acquired=False with the current holder's ID on failure.
    """
    return await _get_backend().lease_acquire(
        resource_id=resource_id,
        holder_id=holder_id,
        ttl=ttl_seconds,
    )


@mcp.tool()
async def lease_release(resource_id: str, holder_id: str) -> ReleaseResult:
    """
    Voluntarily release a lease this agent holds.

    Only the current holder can release their own lease (holder_id must match).
    No-op if the lease has already expired or belongs to someone else.
    """
    released = await _get_backend().lease_release(
        resource_id=resource_id, holder_id=holder_id
    )
    return ReleaseResult(released=released, resource_id=resource_id)


@mcp.tool()
async def lease_renew(
    resource_id: str,
    holder_id: str,
    ttl_seconds: int,
) -> RenewResult:
    """
    Extend an existing lease's TTL while still holding it.

    Call this periodically from long-running agents to prevent their lease
    from expiring mid-execution. The holder_id must match the current holder.
    """
    ok, new_expires = await _get_backend().lease_renew(
        resource_id=resource_id,
        holder_id=holder_id,
        ttl=ttl_seconds,
    )
    return RenewResult(renewed=ok, resource_id=resource_id, expires_at=new_expires)


# =============================================================================
# GROUP 4 — Observability
# =============================================================================

@mcp.tool()
async def sidecar_health() -> HealthResult:
    """
    Check sidecar liveness and get backend diagnostics.
    """
    try:
        keys = await _get_backend().key_count()
        wfs = await _get_backend().workflow_count()
        status = "ok"
    except Exception as exc:
        print(f"[Sidecar] health check error: {exc}", file=sys.stderr)
        return HealthResult(
            status=f"error: {exc}",
            backend=settings.backend,
            uptime_s=round(time.time() - _start_time, 2),
            key_count=0,
            workflow_count=0,
        )

    return HealthResult(
        status=status,
        backend=settings.backend,
        uptime_s=round(time.time() - _start_time, 2),
        key_count=keys,
        workflow_count=wfs,
    )


@mcp.tool()
async def sidecar_reset() -> ResetResult:
    """
    Wipe all sidecar state: kv_store, leases, workflows, step_outputs, history, and sessions.
    """
    cleared = await _get_backend().reset()
    return ResetResult(ok=True, cleared=cleared)


# =============================================================================
# GROUP 5 — Session & History
# =============================================================================

@mcp.tool()
async def session_save(
    session_id: str,
    context: dict,
) -> SessionSaveResult:
    """
    Persist the full workflow context for a session_id.
    """
    await _get_backend().session_save(session_id=session_id, context=context)
    return SessionSaveResult(ok=True, session_id=session_id)


@mcp.tool()
async def session_restore(session_id: str) -> SessionRestoreResult:
    """
    Retrieve the workflow context saved for a session_id.
    """
    result = await _get_backend().session_restore(session_id=session_id)
    if result is _MISSING:
        return SessionRestoreResult(found=False, session_id=session_id, context=None)
    return SessionRestoreResult(found=True, session_id=session_id, context=result)


@mcp.tool()
async def history_log(
    key: Optional[str] = None,
    n: int = 10,
) -> HistoryLogResult:
    """
    Return the last N state-transition records in reverse chronological order.
    """
    entries = await _get_backend().history_get(key=key, n=n)
    return HistoryLogResult(entries=entries, total=len(entries))


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoints
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """HTTP server entry point."""
    print(f"[Sidecar] Starting MCP State Sidecar on {settings.host}:{settings.port}")
    print(f"[Sidecar] Backend: {settings.backend}")
    print(f"[Sidecar] MCP endpoint: http://{settings.host}:{settings.port}/mcp")

    mcp.run(
        transport="streamable-http",
        host=settings.host,
        port=settings.port,
    )


def stdio_main():
    """stdio entry point — IDEs spawn this as subprocess, communicate via stdin/stdout."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
