# OPC Demo

Multi-agent collaborative development project.

## Branch Strategy

| Branch | Role | Description |
|--------|------|-------------|
| `main` | Manager | Production-ready code, merge authority |
| `dev` | Developer | Active feature development |
| `review` | Reviewer | Code review and feedback |

## Worktree Layout

```
OPC-demo/              ← Manager workspace (main branch)
├── .worktrees/
│   ├── dev/           ← Developer workspace (dev branch)
│   └── review/        ← Reviewer workspace (review branch)
└── ...
```
