# Patchset Delivery Backlog

## 目标

- 把任务交付单元从单个 `commit_hash` 升级为 `patchset(base..head)`。
- 让 developer / reviewer / manager 处理同一份冻结交付版本，减少重复回退和 token 浪费。
- 支持灰度切换、merge queue 观测和 stale 诊断。

## 当前阶段

- [x] Phase 0 止血：developer dirty worktree 不再送审；reviewer/manager 已支持 patchset 主链路。
- [x] Phase 1 数据模型：`task_patchsets`、`current_patchset_*`、`merged_patchset_id` 已落地。
- [x] Phase 2 运行时主链路：developer 提交 patchset、reviewer 审 patchset diff、manager squash merge patchset。
- [x] Phase 3 交付可视化：任务详情与 handoff 已展示 patchset。
- [x] Phase 4 灰度开关：新增 `TASK_DELIVERY_MODEL`、`MANAGER_MERGE_MODE`。
- [x] Phase 5 merge queue 基础：patchset 增加 queue 元数据与运行时指标。

## 剩余任务

### Queue / Refresh

- [x] 增加 manager 对 `queue_status=processing` 的实时落库，而不是只在最终 handoff 中写回。
- [x] 在 reviewer 通过时记录 `changed_files`/artifact manifest，供 merge queue 与 acceptance 复用。
- [x] 增加 patchset stale 自动 refresh 入口：当 `main` 前进且 merge 冲突时，能给 developer 明确 refresh 指令与差异摘要。

### 观测

- [x] 把 `/runtime/patchset-metrics` 接入前端运维面板。
- [x] 为 metrics 增加时间窗口维度，区分“累计历史”与“最近 24h”。
- [x] 增加 queue depth / merge latency / stale reason 的面板展示。

### 兼容与清理

- [x] 把 legacy 单 commit 文案和提示词继续收口到 patchset-first 表述。
- [x] 增加 `TASK_DELIVERY_MODEL=commit` 的回归测试，确保灰度回退路径稳定。
- [x] 在完成灰度后，评估是否移除 commit-only manager 主路径。
  当前决策：先保留 `TASK_DELIVERY_MODEL=commit` / `MANAGER_MERGE_MODE=single_commit` 作为灾备回退开关，待一轮稳定灰度后再删除。

## 验收标准

- [ ] reviewer 和 manager 始终围绕同一个 `patchset_id` 工作。
- [ ] manager 因“父提交不在 main”退回的次数稳定为 0。
- [ ] dirty worktree 不再进入 reviewer。
- [ ] merge queue 与 patchset metrics 能清楚反映 queued / stale / merged 状态。
