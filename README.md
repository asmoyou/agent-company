# OPC-demo

Multi-Agent Terminal Orchestrator (MVP)

一个用于本地研发协作的多 Agent 看板系统。  
目标是让你在一个地方统一管理 `Codex`、`Claude` 等终端 Agent，让任务自动流转、自动交接、自动推进。

## 这是什么

- 这是一个随手做的 MVP（最小可用原型），重点是验证流程能跑通，不追求大而全。
- 核心链路：`任务分发 -> 开发 -> 审查 -> 合并 -> 验收`。
- 适合拿来试验多 Agent 协作、提示词策略、结构化交接、自动化流程控制。

## 解决的问题

- 多个终端 Agent 并行干活时，任务交接靠聊天记录，容易丢信息。
- 开发、审查、合并之间缺少统一状态机，容易“卡住”或重复执行。
- 跨 worktree 协作时，谁改了什么、基于哪个 commit，很难追踪。

OPC-demo 用结构化任务流和结构化交接材料来解决这些问题。

## 核心能力

- 一个看板统一管理多种 CLI Agent。
- 内置 `leader`、`developer`、`reviewer`、`manager`，支持扩展自定义 Agent。
- 支持配置 Codex / Claude 等终端 CLI。
- 任务状态机 + 阻塞态恢复，减少“无限重试”和假进度。
- 结构化 handoff（可审计、可回放、可追责）。
- 基于 Git `worktree` 并行开发，每个 Agent 独立工作区。
- 跨 Agent 自动同步提交（默认 `cherry-pick`）。
- Leader 自动决策是否分解任务：简单任务不分解直接开发，复杂任务才分解并强制输出 `todo_steps / deliverables / acceptance_criteria`。
- 支持按项目隔离的 worker 运行：同一 agent 类型可在多个项目并发处理，认领时强制携带 `project_id`（可配置）。

## 快速开始

### 前置依赖

- Python 3.10+
- Git
- 至少一个可用 Agent CLI（如 `codex` 或 `claude`）

### 启动

```bash
cp .env.example .env
bash start.sh
```

打开 `http://localhost:8080`。

`start.sh` 会自动执行这些动作：

- 若不存在 `.venv`，自动创建 Python 虚拟环境。
- 每次启动执行 `pip install -r requirements.txt`，自动补齐缺失依赖。
- 启动后端服务和 Agent 轮询进程，并写入 `.pids/` 便于下次清理旧进程。

### 数据库初始化说明

- 使用本地 SQLite，数据库文件是项目根目录的 `tasks.db`。
- 不需要手动建表：服务启动时会自动执行 `db.init_db()`，首次启动会自动创建所有表。
- `tasks.db` 已在 `.gitignore` 中，新用户克隆仓库后不会拿到旧数据。

如果你想手动初始化数据库，也可以执行：

```bash
python3 -c "from server import db; db.init_db(); print(f'initialized: {db.DB_PATH}')"
```

## 典型流程

1. 创建项目并绑定本地 Git 仓库路径。
2. 创建任务（默认进入 `triage`）。
3. `leader` 评估复杂度：简单任务 -> `todo`；复杂任务 -> `decomposed` 并自动生成子任务清单。
4. `developer` 提交代码 + handoff（包含 commit hash）。
5. `reviewer` 基于 commit 审查并给出结构化结论。
6. `manager` 合并到 `main`，进入验收。
7. 用户在 `pending_acceptance` 验收，最终 `completed`。

## 项目结构

```text
OPC-demo/
├── server/       # FastAPI + SQLite
├── agents/       # leader / developer / reviewer / manager / generic
├── frontend/     # 看板 UI（WebSocket 实时更新）
└── start.sh

<your-project>/
├── .git/
└── .worktrees/<agent-key>/   # 每个 Agent 的独立工作目录
```

## 任务状态流

```text
triage -> todo -> in_progress -> in_review -> approved -> pending_acceptance -> completed
             ^          |            |
             |          |            v
         needs_changes  |          blocked
                        v
                    cancelled

decompose -> decomposed -> (subtasks in todo...)
```

分解子任务默认按 `subtask_order` 串行推进：前序子任务未进入 `approved/pending_acceptance/completed/cancelled` 前，后续子任务不会被认领执行，避免并发导致依赖文件缺失。

状态流转由后端判定并执行，前端仅发送动作意图并展示结果：

- 用户动作接口：`POST /tasks/{task_id}/actions`
  - `accept` / `reject` / `retry_blocked` / `decompose` / `archive`
- 系统流转接口：`POST /tasks/{task_id}/transition`（供 Agent 使用）
- `PATCH /tasks/{task_id}` 不允许直接修改 `status`

## 交接材料（Handoff）

任务交接使用结构化记录，不依赖前端“猜文本”。

- 查询：`GET /tasks/{task_id}/handoffs`
- 写入：`POST /tasks/{task_id}/handoffs`
- 关键字段：`from_agent`、`to_agent`、`stage`、`status_from`/`status_to`、`commit_hash`、`conclusion`、`payload`（结构化详情）

建议每次交接都记录：

- 对应 commit hash
- 本轮修改范围
- 审查结论（pass / fail + 风险等级）
- 下一步动作和责任 Agent

## 跨 Worktree 协作说明

你关心的问题是对的：如果没合并到 `main`，其他 Agent 能不能看到 commit？

- 在同一个 Git 仓库下的多个 `worktree`，共享同一套对象库（`.git/objects`）。
- 当前默认采用“任务级隔离”：
  - 分支：`agent/<agent>/<task_id>`
  - 工作树：`.worktrees/<agent>/<task_id>`
- 所以只要开发 Agent 已经本地 commit，Reviewer 通常可以直接按 `commit_hash` 读取和审查，不必等合并到 `main`。
- Manager 再根据策略把该提交同步到目标分支（如 `cherry-pick` 到 `main`）。
- 只有在“不同 clone / 不同仓库”时，才需要通过 `push/fetch` 才能看到彼此提交。

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `SERVER_URL` | `http://localhost:8080` | Agent 请求后端地址 |
| `POLL_INTERVAL` | `5` | Agent 轮询间隔（秒） |
| `CLI_TIMEOUT` | `300` | 单次 CLI 超时（秒） |
| `FEATURE_STRICT_CLAIM_SCOPE` | `1` | 是否强制 `/tasks/claim` 与任务创建必须带 `project_id` |
| `PER_PROJECT_MAX_WORKERS` | `0` | 每个项目同时租约中的任务上限（0=不限制） |
| `PER_AGENT_TYPE_MAX_WORKERS` | `0` | 每种 agent 类型并发上限（0=不限制） |
| `PROJECT_WORKERS_PER_AGENT` | `1` | `run_all` 每个项目、每个 agent 类型启动的 worker 数 |
| `INCLUDE_IDLE_RUNTIME_PROJECTS` | `0` | `run_all` 是否为无 open task 的项目也启动 worker |
| `AGENT_PROJECT_IDS` | `` | 可选项目白名单（逗号分隔 project id），为空则自动发现 |
| `HANDOFF_SYNC_STRATEGY` | `cherry-pick` | 跨 Agent 提交同步策略：`cherry-pick` / `merge` / `none` |
| `BRANCH_SYNC_STRATEGY` | `merge` | 任务开始前同步 `main` 的策略：`merge` / `rebase` / `none` |
| `AUTO_CLEANUP_TASK_WORKSPACES` | `1` | 任务进入 `completed`/`cancelled` 后自动清理对应 task worktree/branch |
| `TASK_WORKSPACE_FORCE_DELETE_UNMERGED` | `0` | 对未合并分支是否允许强制删除（仅在自动清理中生效） |
| `TASK_WORKSPACE_SWEEP_SECS` | `180` | 周期性扫描终态任务并触发清理的间隔（秒） |
| `TASK_WORKSPACE_SWEEP_BATCH_SIZE` | `200` | 每次周期扫描最多处理的终态任务数量 |
| `TASK_WORKSPACE_CLEANUP_HISTORY_LIMIT` | `300` | 内存中保留的清理事件历史条数（用于运维观测） |
| `CODEX_ENABLE_OUTPUT_SCHEMA` | `0` | 是否给 Codex 传 `--output-schema`（默认关闭，降低兼容风险） |

### 清理运维接口

- `GET /runtime/workspace-cleanup`：查看清理配置、计数指标、最近清理事件、当前 in-flight 任务
- `POST /runtime/workspace-cleanup/sweep?max_tasks=100`：管理员手动触发一次终态任务清理扫描

## 当前边界（MVP）

- 仍在快速迭代，接口和行为可能变化。
- 建议先在沙箱仓库验证流程，再接入正式项目。
- 目标是“把协同链路跑顺”，不是一套完整企业级平台。

## 开源说明

这个仓库是公开的 MVP 版本，用于验证并分享多 Agent 协同方法。  
我有一个真实业务里的更大版本，当前暂未开源。

如果你对这个方向感兴趣，欢迎通过 GitHub `Issue` 或 `Discussion` 交流。
