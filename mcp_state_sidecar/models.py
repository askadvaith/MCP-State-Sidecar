"""
Shared Pydantic models for the MCP State Sidecar.

All tool inputs and outputs are typed with these models so that FastMCP
can auto-generate correct JSON Schemas and the MCP-native client can
deserialise responses without any manual parsing.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional
from pydantic import BaseModel, Field


# ─── Utility ──────────────────────────────────────────────────────────────────

def _now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def _new_run_id() -> str:
    return f"wf-{uuid.uuid4().hex[:12]}"


# ─── Key-Value ────────────────────────────────────────────────────────────────

class GetResult(BaseModel):
    key: str
    value: Optional[Any] = None
    found: bool


class SetResult(BaseModel):
    ok: bool


class DeleteResult(BaseModel):
    ok: bool


class ListResult(BaseModel):
    keys: list[str]


# ─── Workflow ─────────────────────────────────────────────────────────────────

class WorkflowMeta(BaseModel):
    run_id: str = Field(default_factory=_new_run_id)
    name: str
    status: str = "created"          # created | claimed | running | done | failed
    tags: dict[str, str] = Field(default_factory=dict)
    agent_id: Optional[str] = None
    last_step: int = 0
    created_at: float = Field(default_factory=_now_ts)
    updated_at: float = Field(default_factory=_now_ts)


class CreateWorkflowResult(BaseModel):
    run_id: str
    name: str
    status: str


class DiscoverResult(BaseModel):
    runs: list[WorkflowMeta]


class ClaimResult(BaseModel):
    claimed: bool
    run_id: str
    agent_id: Optional[str] = None
    reason: Optional[str] = None     # populated when claimed=False


class CheckpointResult(BaseModel):
    ok: bool
    run_id: str
    step: int


class ResumeContext(BaseModel):
    run_id: str
    last_step: int
    step_outputs: dict[str, Any]     # {"1": <output>, "2": <output>, ...}
    meta: WorkflowMeta


class WorkflowStatusResult(BaseModel):
    run_id: str
    status: str
    last_step: int
    agent_id: Optional[str]
    created_at: float
    updated_at: float


class WorkflowListResult(BaseModel):
    runs: list[WorkflowMeta]


# ─── Lease ────────────────────────────────────────────────────────────────────

class LeaseResult(BaseModel):
    acquired: bool
    resource_id: str
    holder: Optional[str] = None
    expires_at: Optional[float] = None
    reason: Optional[str] = None


class ReleaseResult(BaseModel):
    released: bool
    resource_id: str


class RenewResult(BaseModel):
    renewed: bool
    resource_id: str
    expires_at: Optional[float] = None


# ─── Health ───────────────────────────────────────────────────────────────────

class HealthResult(BaseModel):
    status: str                      # ok | degraded | error
    backend: str
    uptime_s: float
    key_count: int
    workflow_count: int
    version: str = "1.0.0"


# ─── Reset ────────────────────────────────────────────────────────────────────

class ResetResult(BaseModel):
    ok: bool
    cleared: int   # total rows deleted across all six tables: kv_store, leases, workflows, step_outputs, history, sessions


# ─── Session & History ────────────────────────────────────────────────────────

class SessionSaveResult(BaseModel):
    ok: bool
    session_id: str


class SessionRestoreResult(BaseModel):
    found: bool
    session_id: str
    context: Optional[Any] = None      # None when found=False


class HistoryEntry(BaseModel):
    id: int
    key: str
    new_value: Optional[Any] = None
    previous_value: Optional[Any] = None
    agent_id: Optional[str] = None     # NULL stored as None
    written_at: float


class HistoryLogResult(BaseModel):
    entries: list[HistoryEntry]
    total: int
