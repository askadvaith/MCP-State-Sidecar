"""
Configuration for the MCP State Sidecar.

All settings are read from environment variables with sensible defaults.
No config file is required — just set env vars before starting the server.

Environment variables
---------------------
STATE_BACKEND   : "sqlite" (default) | "redis"
DB_PATH         : path to the SQLite database file (default: state_sidecar.db)
REDIS_URL       : Redis connection URL (default: redis://localhost:6379)
SIDECAR_HOST    : host to bind the SSE server (default: 0.0.0.0)
SIDECAR_PORT    : port for the SSE server (default: 8000)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    backend: str = field(default_factory=lambda: os.getenv("STATE_BACKEND", "sqlite").lower())
    db_path: str = field(default_factory=lambda: os.getenv("DB_PATH", "state_sidecar.db"))
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379"))
    host: str = field(default_factory=lambda: os.getenv("SIDECAR_HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(os.getenv("SIDECAR_PORT", "8000")))

    def backend_kwargs(self) -> dict:
        """Return the kwargs to pass to get_backend()."""
        if self.backend == "sqlite":
            return {"db_path": self.db_path}
        if self.backend == "redis":
            return {"redis_url": self.redis_url}
        return {}


settings = Settings()
