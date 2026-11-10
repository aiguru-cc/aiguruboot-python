# Aiguru Workspace Collaboration Protocol v1

This directory is an open, client-neutral coordination bus. Codex, Claude Code,
Cursor, terminal agents and custom agents may participate without an Aiguru SDK.

## Quick start

```bash
python3 .aiguru/collaboration/agent_bridge.py heartbeat --id codex-1 --name Codex --type codex --task "review API" --status working --task-id '<task-id>'
python3 .aiguru/collaboration/agent_bridge.py message --id codex-1 --name Codex --type codex --text "API contract verified"
python3 .aiguru/collaboration/agent_bridge.py claim --id codex-1 --name Codex --type codex --reason "editing API" Sources/API.swift
python3 .aiguru/collaboration/agent_bridge.py release --id codex-1 --name Codex --type codex Sources/API.swift
python3 .aiguru/collaboration/agent_bridge.py command --id codex-1 --name Codex --type codex --target '*' --action request_status
python3 .aiguru/collaboration/agent_bridge.py command --id codex-1 --name Codex --type codex --target '<agent-id>' --action delegate_task --message "review the API"
python3 .aiguru/collaboration/agent_bridge.py task-create --id codex-1 --name Codex --type codex --target aiguru --title "Review API" --instruction "Review and fix the API"
python3 .aiguru/collaboration/agent_bridge.py task-update --id codex-1 --name Codex --type codex '<task-id>' --status completed --result "review complete"
python3 .aiguru/collaboration/agent_bridge.py status
```

Heartbeats older than 20 seconds are offline, but heartbeat presence never gates commands,
messages, or durable tasks. Claims expire after one hour.
Agents should heartbeat every 5–10 seconds, claim before editing shared files,
broadcast important decisions, release on completion, and read COORDINATION.md.
Commands use protocolVersion 1, expire after five minutes, are processed idempotently,
and write results to responses/<command-id>-<agent-id>.json. Only collaboration actions
and allowlisted read-only tools auto-run; mutating tools never bypass user approval.
JSON fields are additive; unknown fields must be ignored for compatibility.

## Task lifecycle and concurrency

New tasks start at `pending` with `revision: 0`, `attempt: 0`, and `maxAttempts: 3`.
Workers normally move `pending -> running -> completed`; use `blocked` for a recoverable
dependency, `failed` after attempts are exhausted, and `canceled` for an explicit stop.
Every state-changing client must pass `--expected-revision <current revision>` to task-update.
A revision conflict means another agent won the decision; reload the task before acting.
Terminal states (`completed`, `failed`, `canceled`, `rejected`) cannot be reopened.
A running worker must heartbeat with `--task-id` every 5–10 seconds. Task execution leases live
independently in `task-leases/`, expire after 60 seconds, and do not change task revisions.

## Local REST API

When TUI is active, read `api-endpoint.json` for the loopback base URL and read the bearer token
from the file named by `tokenFile`. The token file is mode 0600 and must never be logged or committed.
Mutating requests also require `X-Aiguru-Agent-ID`; send `X-Aiguru-Agent-Name` and
`X-Aiguru-Agent-Type` for auditable heterogeneous-agent identity.

- `GET /v1/status`
- `GET /v1/tasks?status=pending&target=*`
- `POST /v1/tasks`
- `POST /v1/tasks/{id}/transitions`
- `POST /v1/tasks/{id}/lease`
- `GET /v1/events?limit=100`

The REST API listens only on `127.0.0.1`. If it is unavailable, use `agent_bridge.py` and the
durable filesystem queue; both transports share the same task state machine and SQLite history.