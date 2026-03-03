import asyncio

from base import BaseAgent, parse_status_list

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
    "   - 至少创建一个文件，否则任务无法通过审查\n\n"
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
    cli_name = "claude"
    working_status = "in_progress"

    def __init__(self, shutdown_event=None, config: dict | None = None):
        super().__init__(shutdown_event)
        cfg = config or {}
        self.poll_statuses = parse_status_list(cfg.get("poll_statuses"), ["todo", "needs_changes"])
        self.cli_name = str(cfg.get("cli") or "claude")
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
                "要求：把所有内容写入文件，不要只输出文字。"
            )
        handoff_context = await self.build_handoff_context(task_id)
        if handoff_context:
            prompt += f"\n\n{handoff_context}\n"

        # ── Run CLI ───────────────────────────────────────────────────────────
        returncode, output = await self.run_cli(prompt, cwd=worktree_dev, task_id=task_id)
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
            await self.add_handoff(
                task_id,
                stage="developer_failed",
                to_agent=self.name,
                status_from=prev_status,
                status_to=prev_status,
                title="开发执行失败",
                summary=f"CLI 执行失败（exit={returncode}），已退回 {prev_status}",
                conclusion=f"开发执行失败，回退到 {prev_status}",
                payload={"exit_code": returncode},
            )
            await self.update_task(task_id, status=prev_status, assignee=None)
            return
        if output.strip():
            await self.add_log(task_id, f"CLI 输出摘要:\n{output[:400]}")
        if await self.stop_if_task_cancelled(task_id, "CLI 执行后"):
            return

        # ── Stage & check diff ────────────────────────────────────────────────
        await self.git("add", "-A", cwd=worktree_dev)
        diff = await self.git("diff", "--cached", "--stat", cwd=worktree_dev)

        # Fallback: save stdout as .md if nothing was written to disk
        if not diff.strip() and output.strip() and len(output) > 50:
            safe = "".join(
                c if c.isalnum() or c in "-_ " else "_"
                for c in task["title"][:40]
            ).strip().replace(" ", "_")
            fallback = worktree_dev / f"{safe or 'deliverable'}.md"
            fallback.write_text(f"# {task['title']}\n\n{output}\n", encoding="utf-8")
            await self.add_log(task_id, f"CLI 未创建文件，输出已保存为 {fallback.name}")
            await self.git("add", "-A", cwd=worktree_dev)
            diff = await self.git("diff", "--cached", "--stat", cwd=worktree_dev)

        if not diff.strip():
            await self.add_log(task_id, "无文件变更，提交审查")
            await self.add_handoff(
                task_id,
                stage="dev_to_review",
                to_agent="reviewer",
                status_from=prev_status,
                status_to="in_review",
                title="开发交接审查（无提交）",
                summary="本轮无文件变更，推进到审查",
                conclusion="无代码变更，直接交接审查",
                payload={"has_commit": False, "source_branch": branch},
                artifact_path=str(worktree_dev),
            )
            if await self.stop_if_task_cancelled(task_id, "推进审查前"):
                return
            await self.update_task(
                task_id,
                status="in_review",
                assignee=None,
                assigned_agent=self.name,
                dev_agent=self.name,
            )
            return

        try:
            await self.git(
                "-c", "user.email=agent@opc-demo.local",
                "-c", "user.name=OPC Agent",
                "commit", "-m", f"feat: {task['title'][:72]}\n\nTask ID: {task_id}",
                cwd=worktree_dev,
            )
            commit_hash = await self.git("rev-parse", "--short", "HEAD", cwd=worktree_dev)
        except Exception as e:
            await self.add_log(task_id, f"提交失败: {e}")
            await self.add_alert(
                summary="开发提交失败",
                task_id=task_id,
                message=str(e),
                kind="error",
                code="developer_commit_failed",
                stage="developer_failed",
            )
            await self.add_handoff(
                task_id,
                stage="developer_failed",
                to_agent=self.name,
                status_from=prev_status,
                status_to="todo",
                title="开发提交失败",
                summary=f"提交失败，任务退回 todo：{e}",
                conclusion="提交失败，任务回退到 todo",
                payload={"error": str(e)},
            )
            await self.update_task(task_id, status="todo", assignee=None)
            return

        await self.add_log(task_id, f"已提交: {commit_hash}\n{diff.strip()}")
        if await self.stop_if_task_cancelled(task_id, "提交后状态更新前"):
            return

        # Commit is already done locally. If task update fails transiently,
        # retry status sync instead of incorrectly rolling the task back to todo.
        update_error = None
        for i in range(1, 7):
            try:
                await self.update_task(
                    task_id,
                    status="in_review",
                    assignee=None,
                    assigned_agent=self.name,
                    dev_agent=self.name,
                    commit_hash=commit_hash,
                )
                update_error = None
                break
            except Exception as e:
                update_error = e
                self._post_output_bg(
                    f"⚠ 已提交 {commit_hash}，同步状态失败（{i}/6）：{str(e)[:120]}"
                )
                await asyncio.sleep(min(2 * i, 10))

        if update_error is not None:
            await self.add_log(
                task_id,
                (
                    f"⚠ 已本地提交 {commit_hash}，但无法把任务推进到 in_review：{update_error}。"
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

        await self.add_handoff(
            task_id,
            stage="dev_to_review",
            to_agent="reviewer",
            status_from=prev_status,
            status_to="in_review",
            title="开发交接审查",
            summary=f"已提交 commit {commit_hash}，等待审查",
            commit_hash=commit_hash,
            conclusion="开发完成，等待审查结论",
            payload={
                "commit_hash": commit_hash,
                "diff_stat": diff.strip(),
                "source_branch": branch,
            },
            artifact_path=str(worktree_dev),
        )


if __name__ == "__main__":
    asyncio.run(DeveloperAgent().run())
