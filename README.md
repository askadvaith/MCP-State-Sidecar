<!-- mcp-name: io.github.askadvaith/mcp-state-sidecar -->
# MCP State Sidecar Server

[![PyPI version](https://img.shields.io/pypi/v/mcp-state-sidecar.svg)](https://pypi.org/project/mcp-state-sidecar/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/mcp-state-sidecar.svg)](https://pypi.org/project/mcp-state-sidecar/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![MCP Registry](https://img.shields.io/badge/MCP-Registry-orange.svg)](https://registry.modelcontextprotocol.io)

An MCP-native state sidecar that externalises workflow state for distributed agent deployments. 

Quite a simple idea really; instead of storing state inside agents (which breaks when processes crash, scale horizontally, or span multiple frameworks), agents write to and read from this sidecar over the **Model Context Protocol (MCP)**. The sidecar is itself an MCP server; agents call its tools exactly the same way they call any other tool!

The server itself is built with distributed environments in mind, and natively handles concurrency, crash resilience and atomic claims in addition to being a common interface for state management between agents.

## Features

- **Durable Key-Value Store**: CRUD operations with optional TTL (Time-To-Live).
- **Workflow Lifecycle Registry**: Centralised coordination (create, claim, checkpoint, and resume) for distributed multi-agent workers without out-of-band communication.
- **TTL Leases & Locks**: Concurrency control to prevent race conditions and split-brain scenarios.
- **Audit Logging & Session Snapshotting**: Audit state transitions and persist session contexts.
- **Multiple Backends**: SQLite (with WAL mode & serialisation) and high-concurrency Redis currently supported.

---

## Installation

Install the package via `pip` or your favorite Python package manager:

```bash
pip install mcp-state-sidecar
```

If you want to use the Redis backend:

```bash
pip install mcp-state-sidecar[redis]
```

### Building from Source

To build and install the package from source:

1. Clone the repository:
   ```bash
   git clone https://github.com/askadvaith/MCP-State-Sidecar.git
   cd MCP-State-Sidecar
   ```
2. Install build dependencies:
   ```bash
   pip install --upgrade build
   ```
3. Build the wheel and source distribution:
   ```bash
   python -m build
   ```
4. Install the package locally:
   ```bash
   pip install dist/mcp_state_sidecar-*.whl
   ```
   Or install the package in editable mode for active development:
   ```bash
   pip install -e .
   ```

---

## Quick Start

### Running the Server

In a multi-agent distributed environment, you would typically run the state sidecar as an HTTP SSE service so multiple remote agents and clients can connect to it concurrently.

#### HTTP SSE Mode (Primary for Distributed Environments)
Start the SSE server to listen on a network port:

```bash
mcp-state-sidecar-http
```
By default, the server binds to `0.0.0.0` and listens on port `8000`. The MCP endpoint is available at `http://localhost:8000/mcp`.

#### Stdio Mode (For Subprocess / Local Agent Execution)
Launch the server via standard input/output:

```bash
mcp-state-sidecar
```

---

## Configuration

The server is configured entirely using environment variables:

| Environment Variable | Default | Description |
|---|---|---|
| `STATE_BACKEND` | `sqlite` | Storage backend: `sqlite` or `redis` |
| `DB_PATH` | `state_sidecar.db` | Path to the SQLite database file |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `SIDECAR_HOST` | `0.0.0.0` | IP host to bind the HTTP SSE server |
| `SIDECAR_PORT` | `8000` | Port for the HTTP SSE server |

---

## Tool Reference

### Group 1 — Key-Value Store
- `state_set(key, value, ttl_seconds?, agent_id?)`: Upsert a JSON-serialisable value with optional TTL.
- `state_get(key)`: Retrieve a value (returns `found=False` if missing or expired).
- `state_delete(key)`: Delete a key.
- `state_list(prefix?)`: List all live keys, optionally filtered by prefix.

### Group 2 — Workflow Lifecycle
- `workflow_create(name, tags?)`: Register a workflow; returns a unique `run_id`.
- `workflow_discover(tags?, status?)`: Find workflows filtered by tags or status.
- `workflow_claim(run_id, agent_id)`: Atomically claim a `created` workflow.
- `workflow_checkpoint(run_id, step, output)`: Persist step output and advance the step counter.
- `workflow_resume(run_id)`: Get full resume context including last step and all step outputs.
- `workflow_status(run_id)`: Get lightweight status (status, last step, and timestamps).
- `workflow_list()`: List all registered workflows.

### Group 3 — Lease & Concurrency Control
- `lease_acquire(resource_id, holder_id, ttl_seconds)`: Attempt to acquire an exclusive lock.
- `lease_release(resource_id, holder_id)`: Voluntarily release a held lease.
- `lease_renew(resource_id, holder_id, ttl_seconds)`: Extend lease duration without releasing.

### Group 4 — Sessions & History
- `session_save(session_id, context)`: Save a snapshot of workflow context.
- `session_restore(session_id)`: Retrieve saved context after crash or handoff.
- `history_log(key?, n?)`: Retrieve the last N state-transition records with timestamps and writer IDs.

### Group 5 — Observability
- `sidecar_health()`: Liveness, backend type, uptime, and database metrics.
- `sidecar_reset()`: Irreversibly wipe all data.

---

## License

This project is licensed under the MIT License. See `LICENSE` for details.
