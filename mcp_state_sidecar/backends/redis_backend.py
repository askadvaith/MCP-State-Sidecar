"""
Redis backend for the MCP State Sidecar.

Uses redis.asyncio for non-blocking I/O.
Lease acquisition uses a Lua script for true atomicity.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
import uuid
from typing import Any, Optional

try:
    import redis.asyncio as aioredis
    from redis.asyncio import Redis as AsyncRedis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

from .base import StateBackend, _MISSING
from ..models import LeaseResult, WorkflowMeta, ResumeContext


# Lua script: acquire lease atomically.
_ACQUIRE_LEASE_LUA = """
local existing = redis.call('GET', KEYS[1])
if existing then
    local data = cjson.decode(existing)
    local now = tonumber(ARGV[3])
    if data['expires_at'] > now then
        return cjson.encode({acquired=false, holder=data['holder_id']})
    end
end
local payload = cjson.encode({holder_id=ARGV[1], expires_at=tonumber(ARGV[2])})
local ttl_ms = math.ceil((tonumber(ARGV[2]) - tonumber(ARGV[3])) * 1000)
redis.call('SET', KEYS[1], payload, 'PX', ttl_ms)
return cjson.encode({acquired=true, holder=ARGV[1], expires_at=tonumber(ARGV[2])})
"""

# Lua script: claim workflow atomically (CAS on status == 'created')
_CLAIM_WF_LUA = """
local raw = redis.call('GET', KEYS[1])
if not raw then return 'NOT_FOUND' end
local wf = cjson.decode(raw)
if wf['status'] ~= 'created' then return 'WRONG_STATUS:' .. wf['status'] end
wf['status'] = 'claimed'
wf['agent_id'] = ARGV[1]
wf['updated_at'] = tonumber(ARGV[2])
redis.call('SET', KEYS[1], cjson.encode(wf))
return 'OK'
"""

# Lua script for atomic lease release
_RELEASE_LEASE_LUA = """
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local data = cjson.decode(raw)
if data['holder_id'] ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
return 1
"""

# Lua script for atomic lease renew
_RENEW_LEASE_LUA = """
local raw = redis.call('GET', KEYS[1])
if not raw then return cjson.encode({ok=false}) end
local data = cjson.decode(raw)
if data['holder_id'] ~= ARGV[1] then return cjson.encode({ok=false}) end
if data['expires_at'] <= tonumber(ARGV[4]) then return cjson.encode({ok=false}) end
data['expires_at'] = tonumber(ARGV[2])
redis.call('SET', KEYS[1], cjson.encode(data), 'PX', tonumber(ARGV[3]))
return cjson.encode({ok=true, expires_at=tonumber(ARGV[2])})
"""


class RedisBackend(StateBackend):
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        if not _REDIS_AVAILABLE:
            raise RuntimeError(
                "redis package is not installed. Run: pip install redis"
            )
        self._redis_url = redis_url
        self._client: Optional[AsyncRedis] = None
        self._acquire_script = None
        self._claim_script = None
        self._release_script = None
        self._renew_script = None

    async def initialise(self) -> None:
        self._client = aioredis.from_url(
            self._redis_url, encoding="utf-8", decode_responses=True
        )
        await self._client.ping()
        self._acquire_script = self._client.register_script(_ACQUIRE_LEASE_LUA)
        self._claim_script = self._client.register_script(_CLAIM_WF_LUA)
        self._release_script = self._client.register_script(_RELEASE_LEASE_LUA)
        self._renew_script = self._client.register_script(_RENEW_LEASE_LUA)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _r(self) -> AsyncRedis:
        if self._client is None:
            raise RuntimeError("RedisBackend not initialised — call initialise() first")
        return self._client

    @staticmethod
    def _now() -> float:
        return time.time()

    # ── Key-Value ─────────────────────────────────────────────────────────────

    async def kv_get(self, key: str) -> Any:
        raw = await self._r().get(f"kv:{key}")
        return json.loads(raw) if raw is not None else _MISSING

    async def kv_set(
        self, key: str, value: Any, ttl: Optional[int] = None, agent_id: Optional[str] = None
    ) -> None:
        r = self._r()
        encoded = json.dumps(value)
        if ttl is not None:
            await r.set(f"kv:{key}", encoded, ex=ttl)
        else:
            await r.set(f"kv:{key}", encoded)

    async def kv_delete(self, key: str) -> bool:
        deleted = await self._r().delete(f"kv:{key}")
        return deleted > 0

    async def kv_list(self, prefix: str = "") -> list[str]:
        pattern = f"kv:{prefix}*"
        keys = []
        async for k in self._r().scan_iter(pattern):
            keys.append(k[3:])
        return sorted(keys)

    # ── Sessions & History ───────────────────────────────────────────────────────

    async def session_save(self, session_id: str, context: dict) -> None:
        await self._r().set(f"session:{session_id}", json.dumps(context))

    async def session_restore(self, session_id: str) -> Any:
        raw = await self._r().get(f"session:{session_id}")
        return json.loads(raw) if raw is not None else _MISSING

    async def history_get(self, key: Optional[str] = None, n: int = 10) -> list[dict]:
        raise NotImplementedError(
            "history_get() is not supported by RedisBackend; use SQLiteBackend for audit history"
        )

    # ── Leases ────────────────────────────────────────────────────────────────

    async def lease_acquire(
        self, resource_id: str, holder_id: str, ttl: int
    ) -> LeaseResult:
        now = self._now()
        expires_at = now + ttl
        result_json = await self._acquire_script(
            keys=[f"lease:{resource_id}"],
            args=[holder_id, str(expires_at), str(now)],
        )
        result = json.loads(result_json)
        return LeaseResult(
            acquired=result.get("acquired", False),
            resource_id=resource_id,
            holder=result.get("holder"),
            expires_at=result.get("expires_at"),
            reason=None if result.get("acquired") else f"Held by '{result.get('holder')}'",
        )

    async def lease_release(self, resource_id: str, holder_id: str) -> bool:
        result = await self._release_script(
            keys=[f"lease:{resource_id}"],
            args=[holder_id],
        )
        return bool(result)

    async def lease_renew(
        self, resource_id: str, holder_id: str, ttl: int
    ) -> tuple[bool, Optional[float]]:
        now = self._now()
        new_expires = now + ttl
        ttl_ms = int(ttl * 1000)
        result_json = await self._renew_script(
            keys=[f"lease:{resource_id}"],
            args=[holder_id, str(new_expires), str(ttl_ms), str(now)],
        )
        result = json.loads(result_json)
        if result.get("ok"):
            return True, result.get("expires_at")
        return False, None

    # ── Workflow Registry ─────────────────────────────────────────────────────

    async def workflow_create(
        self, name: str, tags: dict[str, str]
    ) -> WorkflowMeta:
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
        await self._r().set(f"wf:meta:{run_id}", meta.model_dump_json())
        await self._r().sadd("wf:index", run_id)
        return meta

    async def workflow_get(self, run_id: str) -> Optional[WorkflowMeta]:
        raw = await self._r().get(f"wf:meta:{run_id}")
        return WorkflowMeta.model_validate_json(raw) if raw else None

    async def workflow_list(
        self,
        tags: Optional[dict[str, str]] = None,
        status: Optional[str] = None,
    ) -> list[WorkflowMeta]:
        run_ids = await self._r().smembers("wf:index")
        results: list[WorkflowMeta] = []
        for rid in run_ids:
            wf = await self.workflow_get(rid)
            if wf is None:
                continue
            if status and wf.status != status:
                continue
            if tags and not all(wf.tags.get(k) == v for k, v in tags.items()):
                continue
            results.append(wf)
        return sorted(results, key=lambda w: w.created_at, reverse=True)

    async def workflow_update(
        self,
        run_id: str,
        *,
        status: Optional[str] = None,
        agent_id: Optional[str] = None,
        last_step: Optional[int] = None,
        clear_agent: bool = False,
    ) -> Optional[WorkflowMeta]:
        key = f"wf:meta:{run_id}"
        for attempt in range(5):
            async with self._r().pipeline() as pipe:
                try:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    if not raw:
                        await pipe.reset()
                        return None
                    wf = WorkflowMeta.model_validate_json(raw)
                    if status is not None:
                        wf.status = status
                    if clear_agent:
                        wf.agent_id = None
                    elif agent_id is not None:
                        wf.agent_id = agent_id
                    if last_step is not None:
                        wf.last_step = last_step
                    wf.updated_at = self._now()
                    pipe.multi()
                    pipe.set(key, wf.model_dump_json())
                    await pipe.execute()
                    return wf
                except aioredis.WatchError:
                    if attempt < 4:
                        # Exponential backoff with jitter (10ms * 2^attempt + jitter)
                        sleep_time = (0.01 * (2 ** attempt)) + (random.random() * 0.02)
                        await asyncio.sleep(sleep_time)
                        continue
                    raise RuntimeError(
                        f"Failed to update workflow '{run_id}' after 5 attempts due to concurrent write contention"
                    )
        return None

    async def workflow_claim(
        self, run_id: str, agent_id: str
    ) -> tuple[bool, Optional[str]]:
        now = self._now()
        result = await self._claim_script(
            keys=[f"wf:meta:{run_id}"],
            args=[agent_id, str(now)],
        )
        if result == "OK":
            return True, None
        if result == "NOT_FOUND":
            return False, f"Workflow '{run_id}' not found"
        return False, f"Workflow cannot be claimed: {result}"

    # ── Step Checkpointing ────────────────────────────────────────────────────

    async def checkpoint(self, run_id: str, step: int, output: Any) -> None:
        meta = await self.workflow_get(run_id)
        if meta is None:
            return
        meta.status = "running"
        meta.last_step = max(meta.last_step, step)
        meta.updated_at = self._now()

        async with self._r().pipeline(transaction=True) as pipe:
            pipe.set(f"wf:step:{run_id}:{step}", json.dumps(output))
            pipe.set(f"wf:meta:{run_id}", meta.model_dump_json())
            await pipe.execute()

    async def get_resume_context(self, run_id: str) -> Optional[ResumeContext]:
        meta = await self.workflow_get(run_id)
        if meta is None:
            return None

        step_outputs: dict[str, Any] = {}
        pattern = f"wf:step:{run_id}:*"
        async for key in self._r().scan_iter(pattern):
            step_num = key.rsplit(":", 1)[-1]
            raw = await self._r().get(key)
            if raw:
                step_outputs[step_num] = json.loads(raw)

        return ResumeContext(
            run_id=run_id,
            last_step=meta.last_step,
            step_outputs=step_outputs,
            meta=meta,
        )

    # ── Diagnostics ───────────────────────────────────────────────────────────

    async def reset(self) -> int:
        r = self._r()
        total = 0
        patterns = ["kv:*", "lease:*", "wf:meta:*", "wf:step:*", "wf:index", "session:*"]
        for pattern in patterns:
            keys = []
            async for key in r.scan_iter(pattern):
                keys.append(key)
                if len(keys) >= 100:
                    total += await r.delete(*keys)
                    keys = []
            if keys:
                total += await r.delete(*keys)
        return total

    async def key_count(self) -> int:
        count = 0
        async for _ in self._r().scan_iter("kv:*"):
            count += 1
        return count

    async def workflow_count(self) -> int:
        return await self._r().scard("wf:index")
