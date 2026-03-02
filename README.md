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
待审查/审查中 → 已阻塞（环境或系统错误，修复后可恢复到 in_review）
任意状态 → 已取消（自动归档，不再执行）
```

## 交接材料（Handoff）

- 每个关键流转节点都会写入结构化交接记录（`from_agent/to_agent/stage/status/commit_hash/conclusion/payload`）。
- 关键代码流转阶段（如 `dev_to_review/review_to_manager/merge_to_acceptance`）会强制携带 `commit_hash`。
- 建议在 payload 中携带 `source_branch`，便于跨 worktree 审阅定位。
- 后端接口：
  - `GET /tasks/{task_id}/handoffs`
  - `POST /tasks/{task_id}/handoffs`
- 前端任务详情面板中的「交接记录」展示该任务的完整交接链路，便于多 Agent 共同审阅。
- Leader 的 triage/decompose 结果同样采用结构化文件（`.opc/decisions/*.leader-*.json`）回传，不再以终端文本解析作为主流程。

## 结构化告警

- 告警由 Agent 显式上报，不再依赖日志文本正则猜测。
- 后端接口：
  - `POST /alerts`

## 跨 Agent 同步（Worktree）

- 默认启用 `HANDOFF_SYNC_STRATEGY=cherry-pick`：
  - Generic Agent 领取任务后，会基于最近 handoff 的 `commit_hash/source_branch` 自动同步代码到本分支。
- 可选策略：
  - `HANDOFF_SYNC_STRATEGY=merge`
  - `HANDOFF_SYNC_STRATEGY=none`（关闭自动同步）
- 同步失败会写入结构化告警并将任务置为 `blocked`，避免静默继续执行。

## 配置（.env）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SERVER_URL`    | `http://localhost:8080` | Agent 请求后端的地址 |
| `POLL_INTERVAL` | `5`      | 轮询间隔（秒） |
| `CLI_TIMEOUT`   | `300`    | CLI 超时（秒） |
| `CODEX_ENABLE_OUTPUT_SCHEMA` | `0` | 是否给 codex 传 `--output-schema`（默认关闭，避免 schema 兼容报错） |

Agent CLI 类型（包括内置 `leader/developer/reviewer/manager`）在 Web 页面 `Agent 管理` 中配置。
