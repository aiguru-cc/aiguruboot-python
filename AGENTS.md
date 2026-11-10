<!-- aiguru-collaboration-managed -->
## Aiguru multi-agent collaboration

Codex and compatible agents must read `.agents/skills/aiguru-collaboration/SKILL.md` at task startup.
Inspect `.aiguru/collaboration/tasks/` even when no peer heartbeat exists. Pending tasks are
durable work; claim files, execute the acceptance criteria, publish results, and release claims.
Heartbeats are presence leases only and must never gate messaging or task execution.
<!-- /aiguru-collaboration-managed -->
