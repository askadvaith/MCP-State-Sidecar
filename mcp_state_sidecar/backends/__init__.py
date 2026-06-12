"""
Backend factory and package re-exports.
"""

from __future__ import annotations

from .base import StateBackend
from .sqlite_backend import SQLiteBackend
from .redis_backend import RedisBackend


def get_backend(backend_type: str, **kwargs) -> StateBackend:
    """
    Instantiate the requested backend.

    Parameters
    ----------
    backend_type : str
        One of "sqlite" or "redis" (case-insensitive).
    **kwargs
        Passed directly to the backend constructor.
        - SQLite: db_path (str, default "state_sidecar.db")
        - Redis:  redis_url (str, default "redis://localhost:6379")
    """
    match backend_type.lower():
        case "sqlite":
            return SQLiteBackend(**kwargs)
        case "redis":
            return RedisBackend(**kwargs)
        case _:
            raise ValueError(
                f"Unknown backend '{backend_type}'. "
                "Valid options: 'sqlite', 'redis'."
            )


__all__ = ["StateBackend", "SQLiteBackend", "RedisBackend", "get_backend"]
