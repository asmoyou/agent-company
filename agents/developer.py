import asyncio

from base import BaseAgent, is_review_enabled, parse_status_list

DEVELOPER_PROMPT_DEFAULT = (
    "你是一名专业软件工程师，负责实现以下任务。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. **所有成果必须写入文件**，不要只在终端打印输出\n"
    "   - 代码任务 → 创建对应语言的源文件（.py / .ts / .go 等）\n"
    "   - 文档/方案任务 → 创建 `.md` 文件，把完整内容写入\n"
    "   - 目标是形成可审查的交付物；若本轮无需新增文件，需在交接中写明依据\n\n"
    "2. **质量标准**\n"
    "   - 代码需有适当注释，边界情况需处理\n"
    "   - 文档需完整、结构清晰\n\n"
    "3. **分支与交接约束**\n"
    "   - 在当前工作分支完成实现并提交，不要自行合并 main\n"
    "   - 提交后由 reviewer/manager 继续流程，不要跳过审查与合并环节\n"
    "   - 不要伪造“已合并/已发布”结论\n\n"
    "4. 直接开始实现，不需要解释计划"
)


class DeveloperAgent(BaseAgent):
    name = "developer"
    poll_statuses = ["todo", "needs_changes"]
    cli_name = "codex"
    working_status = "in_progress"

    def __init__(self, shutdown_event=None, config: dict | None = None):
        super().__init__(shutdown_event)
        cfg = config or {}
        self.poll_statuses = parse_status_list(cfg.get("poll_statuses"), ["todo", "needs_changes"])
        self.cli_name = str(cfg.get("cli") or "codex")
        self.prompt_template = str(cfg.get("prompt") or DEVELOPER_PROMPT_DEFAULT)
        self.working_status = str(cfg.get("working_status") or "in_progress")

    async def process_task(self, task: dict):
        task_id = task["id"]
        if await self.stop_if_task_cancelled(task_id, "开始处理前"):
            return
        _, worktree_dev, branch = await self.ensure_agent_workspace(task, agent_key=self.name)

        await self.add_log(task_id, f"Developer 接手，分支: {branch}，工作目录: {worktree_dev}")

        prev_status = task.get("_claimed_from_status", task.get("status"))
        is_rework = prev_status == "needs_changes" and task.get("review_feedback")
        if is_rework:
            await self.add_log(task_id, "根据审查意见返工")

        # ── Build prompt from template ────────────────────────────────────────
        rework_section = ""
        if is_rework:
            rework_section = f"## 审查反馈（必须全部修复）\n\n{task['review_feedback']}"

        template = (self.prompt_template or "").strip()
        if template:
            try:
                prompt = template.format(
                    task_title=task["title"],
                    task_description=task["description"] or "(无额外描述)",
                    rework_section=rework_section,
                )
            except Exception:
                prompt = template
        else:
            # Built-in fallback (should rarely be used)
            prompt = (
                f"实现任务：{task['title']}\n\n{task['description']}\n\n"
                f"{rework_section}\n\n"
                "要求：优先把可交付内容写入文件；若无需新增文件，请在交接中说明依据。"
            )
        handoff_context = await self.build_handoff_context(task_id)
        if handoff_context:
            prompt += f"\n\n{handoff_context}\n"
        head_before = ""
        try:
            head_before = (await self.git("rev-parse", "HEAD", cwd=worktree_dev)).strip()
        except Exception:
            head_before = ""

        # ── Run CLI ───────────────────────────────────────────────────────────
        returncode, output = await self.run_cli(
            prompt,
            cwd=worktree_dev,
            task_id=task_id,
            expected_status=str(task.get("status") or "").strip().lower(),
            expected_assignee=self.name,
        )
        if returncode != 0:
            if await self.stop_if_task_cancelled(task_id, "CLI 失败后"):
                return
            await self.add_log(task_id, f"❌ CLI 执行失败（exit={returncode}），任务退回 {prev_status}")
            if output.strip():
                await self.add_log(task_id, f"错误输出:\n{output[:800]}")
            await self.add_alert(
                summary=f"开发执行失败（exit={returncode}）",
                task_id=task_id,
                message=output[-1200:].strip(),
                kind="error",
                code="developer_cli_failed",
                stage="developer_failed",
                metadata={"exit_code": returncode, "rollback_to": prev_status},
            )
            await self.transition_task(
                task_id,
                fields={"status": prev_status, "assignee": None},
                handoff={
                    "stage": "developer_failed",
                    "to_agent": self.name,
                    "status_from": prev_status,
                    "status_to": prev_status,
                    "title": "开发执行失败",
                    "summary": f"CLI 执行失败（exit={returncode}），已退回 {prev_status}",
                    "conclusion": f"开发执行失败，回退到 {prev_status}",
                    "payload": {"exit_code": returncode},
                },
            )
            return
        if output.strip():
            await self.add_log(task_id, f"CLI 输出摘要:\n{output[:400]}")
        if await self.stop_if_task_cancelled(task_id, "CLI 执行后"):
            return

        # ── Stage & check diff ────────────────────────────────────────────────
        await self.git("add", "-A", cwd=worktree_dev)
        diff = await self.git("diff", "--cached", "--stat", cwd=worktree_dev)
        head_after = ""
        try:
            head_after = (await self.git("rev-parse", "HEAD", cwd=worktree_dev)).strip()
        except Exception:
            head_after = ""
        cli_created_commit = bool(
            head_before and head_after and head_before != head_after
        )

        if not diff.strip() and output.strip() and len(output) > 50:
            await self.add_log(task_id, "CLI 未生成新的可提交文件变更。")

        if cli_created_commit and head_after:
            commit_hash = head_after
            commit_short = head_after[:7]
            diff_stat = diff.strip()
            await self.add_log(task_id, f"检测到 CLI 已直接创建提交：{commit_short}")
            if diff_stat:
                await self.add_log(
                    task_id,
                    "检测到额外未提交改动；按最新 commit 推进审查，剩余改动保留在工作区。",
                )
            if await self.stop_if_task_cancelled(task_id, "CLI 已提交后状态更新前"):
                return

            handoff_payload = {
                "commit_hash": commit_hash,
                "source_branch": branch,
                "committed_by_cli": True,
            }
            if diff_stat:
                handoff_payload["has_uncommitted_changes"] = True
                handoff_payload["uncommitted_diff_stat"] = diff_stat[:1200]
            review_enabled = is_review_enabled(task)
            target_status = "in_review" if review_enabled else "approved"
            target_stage = "dev_to_review" if review_enabled else "dev_to_approved"
            target_agent = "reviewer" if review_enabled else "manager"
            target_title = "开发交接审查" if review_enabled else "开发直达合并"
            target_summary = (
                f"CLI 已提交 commit {commit_short}，等待审查"
                if review_enabled
                else f"CLI 已提交 commit {commit_short}，跳过审查，交由 Manager 合并"
            )
            target_conclusion = (
                "开发完成，等待审查结论"
                if review_enabled
                else "开发完成，已跳过审查并转交 Manager 合并"
            )
            handoff_payload["review_enabled"] = review_enabled

            # Commit is done by CLI. If state update fails transiently, retry
            # transition+handoff sync atomically.
            update_error = None
            for i in range(1, 7):
                try:
                    result = await self.transition_task(
                        task_id,
                        fields={
                            "status": target_status,
                            "assignee": None,
                            "assigned_agent": self.name if review_enabled else "manager",
                            "dev_agent": self.name,
                            "commit_hash": commit_hash,
                        },
                        handoff={
                            "stage": target_stage,
                            "to_agent": target_agent,
                            "status_from": prev_status,
                            "status_to": target_status,
                            "title": target_title,
                            "summary": target_summary,
                            "commit_hash": commit_hash,
                            "conclusion": target_conclusion,
                            "payload": handoff_payload,
                            "artifact_path": str(worktree_dev),
                        },
                    )
                    if result is None:
                        # Task was deleted/cancelled while syncing post-commit state.
                        await self.stop_if_task_cancelled(task_id, "提交后同步状态")
                        return
                    update_error = None
                    break
                except Exception as e:
                    update_error = e
                    self._post_output_bg(
                        f"⚠ CLI 已提交 {commit_short}，同步状态失败（{i}/6）：{str(e)[:120]}"
                    )
                    await asyncio.sleep(min(2 * i, 10))

            if update_error is not None:
                await self.add_log(
                    task_id,
                    (
                        f"⚠ CLI 已提交 {commit_short}，但无法把任务推进到 {target_status}：{update_error}。"
                        "保持当前状态，等待后续重试/人工处理。"
                    ),
                )
                await self.add_alert(
                    summary="提交已完成但状态同步失败",
                    task_id=task_id,
                    message=f"commit={commit_hash}; error={update_error}",
                    kind="warning",
                    code="developer_commit_sync_failed",
                    stage="developer_post_commit_sync",
                    metadata={"commit_hash": commit_hash},
                )
            return

        if not diff.strip():
            await self.add_log(task_id, "未检测到 CLI 内提交或文件改动，保持当前状态。")
            if await self.stop_if_task_cancelled(task_id, "无提交无变更回退前"):
                return
            fields = {"status": prev_status, "assignee": None}
            if str(prev_status or "").strip().lower() in {"todo", "needs_changes"}:
                fields["assigned_agent"] = self.name
                fields["dev_agent"] = self.name
            await self.transition_task(
                task_id,
                fields=fields,
                handoff={
                    "stage": "dev_no_progress",
                    "to_agent": self.name,
                    "status_from": prev_status,
                    "status_to": prev_status,
                    "title": "开发未产生新提交",
                    "summary": f"未检测到 CLI 内提交或文件改动，保持在 {prev_status}",
                    "conclusion": "本轮无可审查交付，等待下一轮开发",
                    "payload": {"has_commit": False, "no_progress": True, "source_branch": branch},
                    "artifact_path": str(worktree_dev),
                },
                log_message=f"无新提交，保持 {prev_status}",
            )
            return

        await self.add_log(
            task_id,
            "检测到未提交文件变更；外围不会自动提交，请在 CLI 内完成 commit 后再交接审查。",
        )
        if await self.stop_if_task_cancelled(task_id, "检测到未提交变更回退前"):
            return
        fields = {"status": prev_status, "assignee": None}
        if str(prev_status or "").strip().lower() in {"todo", "needs_changes"}:
            fields["assigned_agent"] = self.name
            fields["dev_agent"] = self.name
        await self.transition_task(
            task_id,
            fields=fields,
            handoff={
                "stage": "dev_commit_required",
                "to_agent": self.name,
                "status_from": prev_status,
                "status_to": prev_status,
                "title": "开发需在 CLI 内提交",
                "summary": "检测到未提交改动，未自动提交，保持当前状态",
                "conclusion": "请在 CLI 内完成提交后再交接",
                "payload": {
                    "has_commit": False,
                    "requires_cli_commit": True,
                    "source_branch": branch,
                    "diff_stat": diff.strip()[:1200],
                    "head_changed": cli_created_commit,
                },
                "artifact_path": str(worktree_dev),
            },
            log_message="检测到未提交改动，保持当前状态等待 CLI 提交",
        )


if __name__ == "__main__":
    asyncio.run(DeveloperAgent().run())
