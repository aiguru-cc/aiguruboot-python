---
name: aiguru-collaboration
description: Event-driven multi-agent coordination for this workspace. Use whenever another agent is active, a collaboration task exists, or shared files may be edited.
---
# Workspace multi-agent collaboration

This workspace uses `.aiguru/collaboration/PROTOCOL.md`. At the beginning of every task:

1. Read `PROTOCOL.md` and `COORDINATION.md`.
2. Register a heartbeat with `agent_bridge.py heartbeat`; identify your real client type. A missing
   heartbeat never means there is no work: tasks and inbox commands remain authoritative.
3. Run `agent_bridge.py status`, then inspect `tasks/` and commands addressed to you.
4. Claim shared files before editing. If a claim conflicts, notify the owner and do not overwrite.
5. Broadcast decisions, API contracts, blockers and completion. Release claims when done.

Events drive cooperation: a new task or inbox command must be acknowledged, executed once,
and answered through `responses/`. Use unique stable agent IDs for the current session. Address
Aiguru generically with target `aiguru`, or broadcast with `*`; do not cache its transient UUID.

Create a governed task:

```bash
python3 .aiguru/collaboration/agent_bridge.py task-create --id <your-id> --name <name> --type <client-type> --target aiguru --title "Task title" --instruction "Exact work and acceptance criteria" --files path/to/file
```

Heartbeat every 5–10 seconds while actively working and once on every major event. A peer is
offline after 20 seconds. Never send shell, write-file, browser or computer-control operations as
auto-run commands; those require the receiving agent's normal permission flow.

On startup and after every tool boundary, inspect `tasks/` for `pending` or `blocked` work addressed
to your stable agent ID, client type, or `*`. Do not require the creator to remain online. Update the
task lifecycle to `running`, then `completed` or `blocked`, and leave a response/event with the result.
Pass the task's current `revision` as `--expected-revision` for every update. If it conflicts, reload
the task instead of overwriting another agent's decision.