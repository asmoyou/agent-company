# OPC-demo · 多智能体任务看板

基于本地 CLI 工具（Claude Code / Codex）的全自动多智能体协作系统。

## 快速开始

```bash
cp .env.example .env   # 按需修改 CLI 工具配置
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
├── .worktrees/dev/    ← Developer Agent 工作目录
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
| `DEVELOPER_CLI` | `claude` | 开发 Agent 使用的 CLI |
| `REVIEWER_CLI`  | `claude` | 审查 Agent 使用的 CLI |
| `POLL_INTERVAL` | `5`      | 轮询间隔（秒） |
| `CLI_TIMEOUT`   | `300`    | CLI 超时（秒） |
