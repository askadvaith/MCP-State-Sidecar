"""
SQLite backend for the MCP State Sidecar.

Uses aiosqlite for fully async, non-blocking access.
All writes use WAL mode for better concurrency.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any, Optional

import aiosqlite

from .base import StateBackend, _MISSING
from ..models import LeaseResult, WorkflowMeta, ResumeContext, HistoryEntry


_CREATE_KV = """
CREATE TABLE IF NOT EXISTS kv_store (
    key        TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    expires_at REAL          -- NULL means no expiry
)
"""

_CREATE_LEASES = """
CREATE TABLE IF NOT EXISTS leases (
    resource_id TEXT PRIMARY KEY,
    holder_id   TEXT NOT NULL,
    expires_at  REAL NOT NULL
)
"""

_CREATE_WORKFLOWS = """
CREATE TABLE IF NOT EXISTS workflows (
    run_id     TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'created',
    tags_json  TEXT NOT NULL DEFAULT '{}',
    agent_id   TEXT,
    last_step  INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
)
"""

_CREATE_STEPS = """
CREATE TABLE IF NOT EXISTS step_outputs (
    run_id      TEXT NOT NULL,
    step        INTEGER NOT NULL,
    output_json TEXT NOT NULL,
    PRIMARY KEY (run_id, step)
)
"""

_CREATE_HISTORY = """
CREATE TABLE IF NOT EXISTS history (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    key                  TEXT NOT NULL,
    new_value_json       TEXT NOT NULL,
    previous_value_json  TEXT,          -- NULL on first write for this key
    agent_id             TEXT,          -- NULL when caller omits agent_id
    written_at           REAL NOT NULL  -- unixepoch() set in SQL
)
"""

_CREATE_SESSIONS = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id   TEXT PRIMARY KEY,
    context_json TEXT NOT NULL,
    saved_at     REAL NOT NULL,         -- unixepoch() set in SQL on first insert
    updated_at   REAL NOT NULL          -- unixepoch() set in SQL; refreshed on every upsert
)
"""


def _row_to_workflow(row: aiosqlite.Row) -> WorkflowMeta:
    return WorkflowMeta(
        run_id=row["run_id"],
        name=row["name"],
        status=row["status"],
        tags=json.loads(row["tags_json"]),
        agent_id=row["agent_id"],
        last_step=row["last_step"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class SQLiteBackend(StateBackend):
    def __init__(self, db_path: str = "state_sidecar.db"):
        self._db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    async def initialise(self) -> None:
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._db.execute("PRAGMA busy_timeout=3000")
        for _stmt in (_CREATE_KV, _CREATE_LEASES, _CREATE_WORKFLOWS, _CREATE_STEPS,
                      _CREATE_HISTORY, _CREATE_SESSIONS):
            await self._db.execute(_stmt)
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _db_conn(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("SQLiteBackend not initialised — call initialise() first")
        return self._db

    @staticmethod
    def _now() -> float:
        return time.time()

    async def _workflow_get_unlocked(self, run_id: str) -> Optional[WorkflowMeta]:
        db = self._db_conn()
        async with db.execute(
            "SELECT * FROM workflows WHERE run_id = ?", (run_id,)
        ) as cur:
            row = await cur.fetchone()
        return _row_to_workflow(row) if row else None

    # ── Key-Value ─────────────────────────────────────────────────────────────

    async def kv_get(self, key: str) -> Any:
        async with self._lock:
            db = self._db_conn()
            async with db.execute(
                """
                SELECT value_json FROM kv_store
                WHERE key = ?
                  AND (expires_at IS NULL OR expires_at > unixepoch())
                """,
                (key,),
            ) as cur:
                row = await cur.fetchone()
            if row is None:
                return _MISSING
            return json.loads(row["value_json"])

    async def kv_set(self, key: str, value: Any, ttl: Optional[int] = None, agent_id: Optional[str] = None) -> None:
        async with self._lock:
            db = self._db_conn()
            await db.execute("BEGIN")
            try:
                # Read previous value within the transaction boundary
                async with db.execute(
                    """
                    SELECT value_json FROM kv_store
                    WHERE key = ?
                      AND (expires_at IS NULL OR expires_at > unixepoch())
                    """,
                    (key,),
                ) as cur:
                    prev_row = await cur.fetchone()
                previous_json = prev_row["value_json"] if prev_row is not None else None

                if ttl is not None:
                    await db.execute(
                        """
                        INSERT INTO kv_store (key, value_json, expires_at)
                        VALUES (?, ?, unixepoch() + ?)
                        ON CONFLICT(key) DO UPDATE SET
                            value_json = excluded.value_json,
                            expires_at = excluded.expires_at
                        """,
                        (key, json.dumps(value), ttl),
                    )
                else:
                    await db.execute(
                        """
                        INSERT INTO kv_store (key, value_json, expires_at)
                        VALUES (?, ?, NULL)
                        ON CONFLICT(key) DO UPDATE SET
                            value_json = excluded.value_json,
                            expires_at = excluded.expires_at
                        """,
                        (key, json.dumps(value)),
                    )
                await db.execute(
                    """
                    INSERT INTO history (key, new_value_json, previous_value_json, agent_id, written_at)
                    VALUES (?, ?, ?, ?, unixepoch())
                    """,
                    (key, json.dumps(value), previous_json, agent_id),
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def kv_delete(self, key: str) -> bool:
        async with self._lock:
            db = self._db_conn()
            cur = await db.execute("DELETE FROM kv_store WHERE key = ?", (key,))
            await db.commit()
            return cur.rowcount > 0

    async def kv_list(self, prefix: str = "") -> list[str]:
        async with self._lock:
            db = self._db_conn()
            async with db.execute(
                """
                SELECT key FROM kv_store
                WHERE key LIKE ? ESCAPE '\\'
                  AND (expires_at IS NULL OR expires_at > unixepoch())
                ORDER BY key
                """,
                (prefix.replace("%", r"\%").replace("_", r"\_") + "%",),
            ) as cur:
                rows = await cur.fetchall()
            return [r["key"] for r in rows]

    async def history_get(self, key: Optional[str] = None, n: int = 10) -> list[HistoryEntry]:
        if n < 1:
            raise ValueError(f"n must be >= 1, got {n}")
        async with self._lock:
            db = self._db_conn()
            if key is not None:
                async with db.execute(
                    """
                    SELECT id, key, new_value_json, previous_value_json, agent_id, written_at
                    FROM history
                    WHERE key = ?
                    ORDER BY written_at DESC, id DESC
                    LIMIT ?
                    """,
                    (key, n),
                ) as cur:
                    rows = await cur.fetchall()
            else:
                async with db.execute(
                    """
                    SELECT id, key, new_value_json, previous_value_json, agent_id, written_at
                    FROM history
                    ORDER BY written_at DESC, id DESC
                    LIMIT ?
                    """,
                    (n,),
                ) as cur:
                    rows = await cur.fetchall()
            return [
                HistoryEntry(
                    id=r["id"],
                    key=r["key"],
                    new_value=json.loads(r["new_value_json"]),
                    previous_value=json.loads(r["previous_value_json"]) if r["previous_value_json"] else None,
                    agent_id=r["agent_id"],
                    written_at=r["written_at"],
                )
                for r in rows
            ]

    # ── Leases ────────────────────────────────────────────────────────────────

    async def lease_acquire(
        self, resource_id: str, holder_id: str, ttl: int
    ) -> LeaseResult:
        async with self._lock:
            db = self._db_conn()
            await db.execute("BEGIN IMMEDIATE")
            try:
                # Purge expired leases for this resource
                await db.execute(
                    "DELETE FROM leases WHERE resource_id = ? AND expires_at < unixepoch()",
                    (resource_id,),
                )
                try:
                    expires_at = self._now() + ttl
                    await db.execute(
                        "INSERT INTO leases (resource_id, holder_id, expires_at) "
                        "VALUES (?, ?, ?)",
                        (resource_id, holder_id, expires_at),
                    )
                    await db.commit()
                    return LeaseResult(
                        acquired=True,
                        resource_id=resource_id,
                        holder=holder_id,
                        expires_at=expires_at,
                    )
                except aiosqlite.IntegrityError:
                    await db.rollback()
                    async with db.execute(
                        "SELECT holder_id, expires_at FROM leases WHERE resource_id = ?",
                        (resource_id,),
                    ) as cur:
                        row = await cur.fetchone()
                    current = row["holder_id"] if row else None
                    return LeaseResult(
                        acquired=False,
                        resource_id=resource_id,
                        holder=current,
                        reason=f"Held by '{current}'",
                    )
            except Exception:
                await db.rollback()
                raise

    async def lease_release(self, resource_id: str, holder_id: str) -> bool:
        async with self._lock:
            db = self._db_conn()
            cur = await db.execute(
                "DELETE FROM leases WHERE resource_id = ? AND holder_id = ?",
                (resource_id, holder_id),
            )
            await db.commit()
            return cur.rowcount > 0

    async def lease_renew(
        self, resource_id: str, holder_id: str, ttl: int
    ) -> tuple[bool, Optional[float]]:
        async with self._lock:
            db = self._db_conn()
            cur = await db.execute(
                """
                UPDATE leases SET expires_at = unixepoch() + ?
                WHERE resource_id = ? AND holder_id = ? AND expires_at > unixepoch()
                """,
                (ttl, resource_id, holder_id),
            )
            await db.commit()
            if cur.rowcount > 0:
                async with db.execute(
                    "SELECT expires_at FROM leases WHERE resource_id = ?", (resource_id,)
                ) as cur2:
                    row = await cur2.fetchone()
                return True, row["expires_at"] if row else None
            return False, None

    # ── Sessions ──────────────────────────────────────────────────────────────

    async def session_save(self, session_id: str, context: dict) -> None:
        async with self._lock:
            db = self._db_conn()
            await db.execute(
                """
                INSERT INTO sessions (session_id, context_json, saved_at, updated_at)
                VALUES (?, ?, unixepoch(), unixepoch())
                ON CONFLICT(session_id) DO UPDATE SET
                    context_json = excluded.context_json,
                    updated_at   = unixepoch()
                """,
                (session_id, json.dumps(context)),
            )
            await db.commit()

    async def session_restore(self, session_id: str) -> Any:
        async with self._lock:
            db = self._db_conn()
            async with db.execute(
                "SELECT context_json FROM sessions WHERE session_id = ?", (session_id,)
            ) as cur:
                row = await cur.fetchone()
            if row is None:
                return _MISSING
            return json.loads(row["context_json"])

    # ── Workflow Registry ─────────────────────────────────────────────────────

    async def workflow_create(
        self, name: str, tags: dict[str, str]
    ) -> WorkflowMeta:
        async with self._lock:
            db = self._db_conn()
            now = self._now()
            run_id = f"wf-{uuid.uuid4().hex[:12]}"
            meta = WorkflowMeta(
                run_id=run_id,
                name=name,
                status="created",
                tags=tags,
                created_at=now,
                updated_at=now,
            )
            await db.execute(
                """
                INSERT INTO workflows
                    (run_id, name, status, tags_json, agent_id, last_step, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    meta.run_id,
                    meta.name,
                    meta.status,
                    json.dumps(meta.tags),
                    meta.agent_id,
                    meta.last_step,
                    meta.created_at,
                    meta.updated_at,
                ),
            )
            await db.commit()
            return meta

    async def workflow_get(self, run_id: str) -> Optional[WorkflowMeta]:
        async with self._lock:
            return await self._workflow_get_unlocked(run_id)

    async def workflow_list(
        self,
        tags: Optional[dict[str, str]] = None,
        status: Optional[str] = None,
    ) -> list[WorkflowMeta]:
        async with self._lock:
            db = self._db_conn()
            query = "SELECT * FROM workflows"
            params: list = []
            conditions: list[str] = []

            if status:
                conditions.append("status = ?")
                params.append(status)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += " ORDER BY created_at DESC"

            async with db.execute(query, params) as cur:
                rows = await cur.fetchall()

            results = [_row_to_workflow(r) for r in rows]

            # Post-filter by tags (AND semantics)
            if tags:
                results = [
                    wf for wf in results
                    if all(wf.tags.get(k) == v for k, v in tags.items())
                ]

            return results

    async def workflow_update(
        self,
        run_id: str,
        *,
        status: Optional[str] = None,
        agent_id: Optional[str] = None,
        last_step: Optional[int] = None,
        clear_agent: bool = False,
    ) -> Optional[WorkflowMeta]:
        async with self._lock:
            db = self._db_conn()
            now = self._now()
            sets: list[str] = ["updated_at = ?"]
            params: list = [now]

            if status is not None:
                sets.append("status = ?")
                params.append(status)
            if clear_agent:
                sets.append("agent_id = ?")
                params.append(None)
            elif agent_id is not None:
                sets.append("agent_id = ?")
                params.append(agent_id)
            if last_step is not None:
                sets.append("last_step = ?")
                params.append(last_step)

            params.append(run_id)
            cur = await db.execute(
                f"UPDATE workflows SET {', '.join(sets)} WHERE run_id = ?",
                params,
            )
            await db.commit()
            if cur.rowcount == 0:
                return None
            return await self._workflow_get_unlocked(run_id)

    async def workflow_claim(
        self, run_id: str, agent_id: str
    ) -> tuple[bool, Optional[str]]:
        async with self._lock:
            db = self._db_conn()
            now = self._now()
            cur = await db.execute(
                """
                UPDATE workflows
                   SET status = 'claimed', agent_id = ?, updated_at = ?
                 WHERE run_id = ? AND status = 'created'
                """,
                (agent_id, now, run_id),
            )
            await db.commit()
            if cur.rowcount > 0:
                return True, None

            wf = await self._workflow_get_unlocked(run_id)
            if wf is None:
                return False, f"Workflow '{run_id}' not found"
            return False, f"Workflow is in status '{wf.status}' (agent: {wf.agent_id})"

    # ── Step Checkpointing ────────────────────────────────────────────────────

    async def checkpoint(self, run_id: str, step: int, output: Any) -> None:
        async with self._lock:
            db = self._db_conn()
            now = self._now()
            await db.execute("BEGIN")
            try:
                await db.execute(
                    """
                    INSERT INTO step_outputs (run_id, step, output_json)
                    VALUES (?, ?, ?)
                    ON CONFLICT(run_id, step) DO UPDATE SET output_json = excluded.output_json
                    """,
                    (run_id, step, json.dumps(output)),
                )
                await db.execute(
                    """
                    UPDATE workflows
                       SET last_step = MAX(last_step, ?),
                           status = CASE WHEN status IN ('claimed', 'created') THEN 'running' ELSE status END,
                           updated_at = ?
                     WHERE run_id = ?
                    """,
                    (step, now, run_id),
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def get_resume_context(self, run_id: str) -> Optional[ResumeContext]:
        async with self._lock:
            db = self._db_conn()
            meta = await self._workflow_get_unlocked(run_id)
            if meta is None:
                return None

            async with db.execute(
                "SELECT step, output_json FROM step_outputs WHERE run_id = ? ORDER BY step",
                (run_id,),
            ) as cur:
                rows = await cur.fetchall()

            step_outputs = {str(r["step"]): json.loads(r["output_json"]) for r in rows}
            return ResumeContext(
                run_id=run_id,
                last_step=meta.last_step,
                step_outputs=step_outputs,
                meta=meta,
            )

    # ── Diagnostics ───────────────────────────────────────────────────────────

    async def reset(self) -> int:
        async with self._lock:
            db = self._db_conn()
            total = 0
            for table in ("kv_store", "leases", "workflows", "step_outputs", "history", "sessions"):
                cur = await db.execute(f"DELETE FROM {table}")
                total += cur.rowcount
            await db.commit()
            return total

    async def key_count(self) -> int:
        async with self._lock:
            db = self._db_conn()
            async with db.execute(
                "SELECT COUNT(*) FROM kv_store WHERE expires_at IS NULL OR expires_at > unixepoch()"
            ) as cur:
                row = await cur.fetchone()
            return row[0] if row else 0

    async def workflow_count(self) -> int:
        async with self._lock:
            db = self._db_conn()
            async with db.execute("SELECT COUNT(*) FROM workflows") as cur:
                row = await cur.fetchone()
            return row[0] if row else 0
