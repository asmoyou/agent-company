import asyncio
import json

from base import BaseAgent, is_review_enabled


class GenericAgent(BaseAgent):
    """
    A configurable agent driven by an agent_type record from the database.
    Behavior: polls configured statuses → runs CLI prompt → commits any file
    changes → advances task to next_status.
    """

    def __init__(self, config: dict, shutdown_event=None):
        super().__init__(shutdown_event)
        self.name           = config["key"]
        self._display_name  = config["name"]
        self.poll_statuses  = json.loads(config.get("poll_statuses") or '["todo"]')
        self.next_status    = config.get("next_status") or "in_review"
        self._working_status = config.get("working_status") or "in_progress"
        self._prompt_tpl    = config.get("prompt") or ""
        self.cli_name       = config.get("cli") or "claude"

    async def process_task(self, task: dict):
        task_id = task["id"]
        if await self.stop_if_task_cancelled(task_id, "开始处理前"):
            return
        proj_root, worktree_dev, branch = await self.ensure_agent_workspace(task, agent_key=self.name)

        await self.add_log(
            task_id, f"{self._display_name} 接手，分支: {branch}，工作目录: {worktree_dev}"
        )
        sync_result = await self.sync_from_latest_handoff(task, worktree_dev, current_branch=branch)
        if sync_result.get("status") == "failed":
            return

        prev_status = task.get("_claimed_from_status", task.get("status"))
        is_rework = task.get("review_feedback") and prev_status == "needs_changes"
        rework_section = (
            f"## 审查反馈（必须全部修复）\n\n{task['review_feedback']}"
            if is_rework else ""
        )

        if self._prompt_tpl:
            try:
                prompt = self._prompt_tpl.format(
                    task_title=task["title"],
                    task_description=task["description"] or "(无额外描述)",
                    rework_section=rework_section,
                )
            except KeyError:
                prompt = self._prompt_tpl
        else:
            prompt = (
                f"完成任务：{task['title']}\n\n"
                f"{task['description'] or ''}\n\n"
                f"{rework_section}\n\n"
                "把所有输出写入文件，不要只输出文字。"
            )
        handoff_context = await self.build_handoff_context(task_id)
        if handoff_context:
            prompt += f"\n\n{handoff_context}\n"
        head_before = ""
        try:
            head_before = (await self.git("rev-parse", "HEAD", cwd=worktree_dev)).strip()
        except Exception:
            head_before = ""

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
            prev_status = task.get("_claimed_from_status", task.get("status"))
            await self.add_log(task_id, f"❌ CLI 执行失败（exit={returncode}），任务退回 {prev_status}")
            if output.strip():
                await self.add_log(task_id, f"错误输出:\n{output[:800]}")
            await self.add_alert(
                summary=f"{self._display_name} 执行失败（exit={returncode}）",
                task_id=task_id,
                message=output[-1200:].strip(),
                kind="error",
                code=f"{self.name}_cli_failed",
                stage=f"{self.name}_failed",
                metadata={"exit_code": returncode},
            )
            await self.transition_task(
                task_id,
                fields={"status": prev_status, "assignee": None},
                handoff={
                    "stage": f"{self.name}_failed",
                    "to_agent": self.name,
                    "status_from": prev_status,
                    "status_to": prev_status,
                    "title": f"{self._display_name} 执行失败",
                    "summary": f"CLI 执行失败（exit={returncode}）",
                    "conclusion": f"{self._display_name} 执行失败，回退到 {prev_status}",
                    "payload": {"exit_code": returncode},
                },
                log_message=f"❌ CLI 执行失败（exit={returncode}），任务退回 {prev_status}",
            )
            return
        if output.strip():
            await self.add_log(task_id, f"输出摘要:\n{output[:400]}")
        if await self.stop_if_task_cancelled(task_id, "CLI 执行后"):
            return

        # Stage & check diff
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
            diff_stat = diff.strip()
            commit_hash = (await self.git("rev-parse", "--short", "HEAD", cwd=worktree_dev)).strip()
            await self.add_log(task_id, f"检测到 CLI 已直接创建提交: {commit_hash}")
            if diff_stat:
                await self.add_log(
                    task_id,
                    "检测到额外未提交改动；按最新 commit 推进流转，剩余改动保留在工作区。",
                )
            if await self.stop_if_task_cancelled(task_id, "CLI 已提交后状态更新前"):
                return
            effective_next_status = self.next_status
            if effective_next_status == "in_review" and not is_review_enabled(task):
                effective_next_status = "approved"
            update_fields = {
                "status": effective_next_status,
                "assignee": None,
                "commit_hash": commit_hash,
            }
            if effective_next_status == "in_review":
                update_fields["assigned_agent"] = self.name
                update_fields["dev_agent"] = self.name
            elif effective_next_status == "approved":
                update_fields["assigned_agent"] = "manager"
                update_fields["dev_agent"] = self.name
            await self.transition_task(
                task_id,
                fields=update_fields,
                handoff={
                    "stage": f"{self.name}_handoff",
                    "to_agent": (update_fields.get("assigned_agent") or effective_next_status),
                    "status_from": prev_status,
                    "status_to": effective_next_status,
                    "title": f"{self._display_name} 交接",
                    "summary": f"检测到 CLI 已生成 commit {commit_hash}，推进到 {effective_next_status}",
                    "commit_hash": commit_hash,
                    "conclusion": f"{self._display_name} 完成，推进到 {effective_next_status}",
                    "payload": {
                        "commit_hash": commit_hash,
                        "source_branch": branch,
                        "committed_by_cli": True,
                        "review_enabled": is_review_enabled(task),
                        "has_uncommitted_changes": bool(diff_stat),
                        "uncommitted_diff_stat": diff_stat[:1200] if diff_stat else "",
                    },
                    "artifact_path": str(worktree_dev),
                },
                log_message=f"检测到 CLI 已生成提交，推进至 {effective_next_status}",
            )
            return

        if diff.strip():
            await self.add_log(
                task_id,
                "检测到未提交文件变更；外围不会自动提交，请在 CLI 内完成 commit 后再交接。",
            )
            if await self.stop_if_task_cancelled(task_id, "检测到未提交变更回退前"):
                return
            update_fields = {"status": prev_status, "assignee": None}
            if str(prev_status or "").strip().lower() in {"todo", "needs_changes"}:
                update_fields["assigned_agent"] = self.name
                update_fields["dev_agent"] = self.name
            await self.transition_task(
                task_id,
                fields=update_fields,
                handoff={
                    "stage": f"{self.name}_commit_required",
                    "to_agent": self.name,
                    "status_from": prev_status,
                    "status_to": prev_status,
                    "title": f"{self._display_name} 需在 CLI 内提交",
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
                log_message=f"检测到未提交改动，保持 {prev_status}",
            )
            return

        await self.add_log(task_id, "未检测到新提交或文件改动，不推进状态。")
        if await self.stop_if_task_cancelled(task_id, "无变更回退前"):
            return
        update_fields = {"status": prev_status, "assignee": None}
        if str(prev_status or "").strip().lower() in {"todo", "needs_changes"}:
            update_fields["assigned_agent"] = self.name
            update_fields["dev_agent"] = self.name
        await self.transition_task(
            task_id,
            fields=update_fields,
            handoff={
                "stage": f"{self.name}_no_progress",
                "to_agent": self.name,
                "status_from": prev_status,
                "status_to": prev_status,
                "title": f"{self._display_name} 未产生新变更",
                "summary": f"未检测到新提交或文件改动，保持在 {prev_status}",
                "conclusion": "本轮未产出可交付变更，等待下一轮执行",
                "payload": {"has_commit": False, "no_progress": True, "source_branch": branch},
                "artifact_path": str(worktree_dev),
            },
            log_message=f"无文件变更，保持 {prev_status}",
        )

    def working_status_for(self, status: str) -> str:
        return self._working_status or status


if __name__ == "__main__":
    import sys
    print("GenericAgent requires config dict; use run_all.py to launch.")
    sys.exit(1)
