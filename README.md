# OPC-demo · 多智能体任务看板

基于本地 CLI 工具（Claude Code / Codex）的全自动多智能体协作系统。

## 快速开始

```bash
cp .env.example .env   # 按需修改服务轮询/超时配置
bash start.sh          # 自动安装依赖、启动服务
# 浏览器打开 http://localhost:8080
```

## 架构

```
OPC-demo/              ← 管理系统（本项目，不会被 agent 修改）
├── server/            ← FastAPI + SQLite 后端
├── agents/            ← Developer / Reviewer / Manager Agent
├── frontend/          ← 看板 UI（WebSocket 实时更新）
└── start.sh

~/projects/my-app/     ← 用户创建的项目（独立 git 仓库）
├── .git/
├── .worktrees/{key}/  ← 各 Agent 的独立工作目录（例如 developer-a）
├── branch: main
├── branch: agent/{key}
└── （agent 交付的文件）
```

## 任务流转

```
待开发 → 开发中 → 待审查 → 审查中 → 需修改 ↗
                                    ↘ 审查通过 → 合并中 → 待验收 → 已完成
```

## 配置（.env）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SERVER_URL`    | `http://localhost:8080` | Agent 请求后端的地址 |
| `POLL_INTERVAL` | `5`      | 轮询间隔（秒） |
| `CLI_TIMEOUT`   | `300`    | CLI 超时（秒） |

Agent CLI 类型（包括内置 `leader/developer/reviewer/manager`）在 Web 页面 `Agent 管理` 中配置。
