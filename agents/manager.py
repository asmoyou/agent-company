import asyncio
import json
import re
from pathlib import Path

from base import BaseAgent, get_task_dev_agent, parse_status_list

MANAGER_PROMPT_DEFAULT = (
    "你是发布合并管理者。请在当前仓库中将已审查 commit 合并到 main。\n\n"
    "任务标题：{task_title}\n"
    "目标 commit：{commit_hash}\n"
    "来源分支：{dev_branch}\n"
    "仓库路径：{project_path}\n\n"
    "请执行：\n"
    "1. 切换到 main（不存在则创建）。\n"
    "2. 验证目标 commit 在来源分支上（git merge-base --is-ancestor）。\n"
    "3. 仅合并目标 commit（不要合并整个分支 HEAD）。\n"
    "4. 提交信息使用：{merge_message}\n\n"
    "若冲突，停止并保留冲突现场，不要强行解决。\n"
    "完成后把结果写入 JSON 文件：{decision_file}\n"
    "JSON 格式：\n"
    '{"decision":"merged|already_up_to_date|conflict|failed","message":"..."}\n'
    "并在回复最后一行输出同一个 JSON 对象。"
)


class ManagerAgent(BaseAgent):
    name = "manager"
    poll_statuses = ["approved"]
    cli_name = "claude"
    working_status = "merging"

    def __init__(self, shutdown_event=None, config: dict | None = None):
        super().__init__(shutdown_event)
        cfg = config or {}
        self.poll_statuses = parse_status_list(cfg.get("poll_statuses"), ["approved"])
        self.cli_name = str(cfg.get("cli") or "claude")
        self.prompt_template = str(cfg.get("prompt") or MANAGER_PROMPT_DEFAULT)
        self.working_status = str(cfg.get("working_status") or "merging")

    def respect_assignment_for(self, status: str) -> bool:
        return False

    async def _is_ancestor(self, repo_root: Path, ancestor: str, ref: str) -> bool:
        try:
            await self.git("merge-base", "--is-ancestor", ancestor, ref, cwd=repo_root)
            return True
        except Exception:
            return False

    async def _ensure_on_main(self, repo_root: Path):
        try:
            current = (await self.git("branch", "--show-current", cwd=repo_root)).strip()
        except Exception:
            current = ""
        if current == "main":
            return
        try:
            await self.git("checkout", "main", cwd=repo_root)
        except Exception:
            await self.git("checkout", "-b", "main", cwd=repo_root)

    async def _cleanup_merge_state(self, repo_root: Path):
        for cmd in (("merge", "--abort"), ("cherry-pick", "--abort"), ("rebase", "--abort")):
            try:
                await self.git(*cmd, cwd=repo_root)
            except Exception:
                continue

    def _load_decision_file(self, path: Path) -> dict | None:
        if not path.exists():
            return None
        try:
            raw = path.read_text(encoding="utf-8", errors="replace").strip()
            if not raw:
                return None
            data = json.loads(raw)
        except Exception:
            return None
        if not isinstance(data, dict):
            return None
        decision = str(data.get("decision") or "").strip().lower()
        if decision not in {"merged", "already_up_to_date", "conflict", "failed"}:
            return None
        msg = str(data.get("message") or "").strip()
        return {"decision": decision, "message": msg}

    def _parse_decision_from_output(self, text: str) -> dict | None:
        tail = text[-5000:] if len(text) > 5000 else text
        for m in reversed(re.findall(r"\{[^{}]*\}", tail)):
            try:
                data = json.loads(m)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            decision = str(data.get("decision") or "").strip().lower()
            if decision not in {"merged", "already_up_to_date", "conflict", "failed"}:
                continue
            return {"decision": decision, "message": str(data.get("message") or "").strip()}
        return None

    async def _return_to_dev_for_merge_issue(
        self,
        *,
        task_id: str,
        dev_agent: str,
        target_commit: str,
        related_history_commits: list[dict],
        feedback: str,
        dev_branch: str = "",
    ):
        await self.add_log(task_id, feedback[:500])
        await self.add_alert(
            summary="合并退回开发",
            task_id=task_id,
            message=feedback[:1000],
            kind="warning",
            code="manager_merge_to_dev",
            stage="merge_to_dev",
            metadata={"commit_hash": target_commit, "dev_branch": dev_branch},
        )
        await self.transition_task(
            task_id,
            fields={
                "status": "needs_changes",
                "assignee": None,
                "assigned_agent": dev_agent,
                "dev_agent": dev_agent,
                "review_feedback": feedback[:1000],
                "feedback_source": self.name,
                "feedback_stage": "merge_to_dev",
                "feedback_actor": self.name,
            },
            handoff={
                "stage": "merge_to_dev",
                "to_agent": dev_agent,
                "status_from": "approved",
                "status_to": "needs_changes",
                "title": "合并退回开发",
                "summary": feedback[:300],
                "commit_hash": target_commit,
                "conclusion": "合并前置检查失败或发生冲突，退回开发处理",
                "payload": {
                    "reason": "merge_to_dev",
                    "commit_hash": target_commit,
                    "related_history_commits": related_history_commits,
                },
            },
            log_message=feedback[:300],
        )

    async def process_task(self, task: dict):
        task_id = task["id"]
        if await self.stop_if_task_cancelled(task_id, "开始合并前"):
            return

        dev_agent = get_task_dev_agent(task)
        commit_hash = (task.get("commit_hash") or "").strip()
        task_commit_hash = commit_hash
        related_history_commits: list[dict] = []
        proj_root, _, dev_branch = await self.ensure_agent_workspace(
            task, agent_key=dev_agent, sync_with_main=False
        )

        if not commit_hash:
            commit_hash, related_history_commits = await self.resolve_handoff_commit_candidate(
                task_id,
                proj_root,
            )
        target_commit = commit_hash

        if not task_commit_hash and commit_hash:
            head = related_history_commits[0] if related_history_commits else {}
            await self.add_log(
                task_id,
                (
                    "任务未显式提供 commit_hash，已改用历史提交证据继续合并："
                    f"{head.get('short') or commit_hash[:12]}"
                ),
            )

        if not target_commit:
            feedback = (
                "[系统错误] 缺少可合并 commit_hash（未找到可用历史提交证据），"
                "无法精确合并。退回开发重提。"
            )
            await self._return_to_dev_for_merge_issue(
                task_id=task_id,
                dev_agent=dev_agent,
                target_commit="",
                related_history_commits=related_history_commits,
                feedback=feedback,
                dev_branch=dev_branch,
            )
            return

        await self.add_log(
            task_id,
            f"Manager 开始合并 commit {target_commit}（源分支: {dev_branch}，项目: {proj_root.name}）",
        )

        try:
            await self.git("cat-file", "-e", f"{target_commit}^{{commit}}", cwd=proj_root)
            if not await self._is_ancestor(proj_root, target_commit, dev_branch):
                raise RuntimeError(f"commit {target_commit} 不在分支 {dev_branch} 上")
        except Exception as e:
            feedback = f"[合并前置检查失败] {e}。请 {dev_agent} 重新提交可合并 commit。"
            await self._return_to_dev_for_merge_issue(
                task_id=task_id,
                dev_agent=dev_agent,
                target_commit=target_commit,
                related_history_commits=related_history_commits,
                feedback=feedback,
                dev_branch=dev_branch,
            )
            return

        await self._ensure_on_main(proj_root)
        head_before = (await self.git("rev-parse", "--short", "HEAD", cwd=proj_root)).strip()
        before_contains_target = await self._is_ancestor(proj_root, target_commit, "main")

        decision_dir = proj_root / ".opc" / "decisions"
        decision_dir.mkdir(parents=True, exist_ok=True)
        decision_file = decision_dir / f"{task_id}.manager-merge.json"
        try:
            decision_file.unlink(missing_ok=True)
        except Exception:
            pass

        merge_message = f"merge: {task['title'][:72]} | Task ID: {task_id}"
        template = (self.prompt_template or "").strip()
        if template:
            try:
                prompt = template.format(
                    task_title=task["title"],
                    task_description=task.get("description") or "(无额外描述)",
                    commit_hash=target_commit,
                    dev_branch=dev_branch,
                    project_path=str(proj_root),
                    merge_message=merge_message,
                    decision_file=str(decision_file),
                )
            except Exception:
                prompt = template
        else:
            prompt = MANAGER_PROMPT_DEFAULT.format(
                task_title=task["title"],
                commit_hash=target_commit,
                dev_branch=dev_branch,
                project_path=str(proj_root),
                merge_message=merge_message,
                decision_file=str(decision_file),
            )
        handoff_context = await self.build_handoff_context(task_id)
        if handoff_context:
            prompt += f"\n\n{handoff_context}\n"

        returncode, output = await self.run_cli(
            prompt,
            cwd=proj_root,
            task_id=task_id,
            expected_status=str(task.get("status") or "").strip().lower(),
            expected_assignee=self.name,
        )
        if await self.stop_if_task_cancelled(task_id, "合并 CLI 执行后"):
            return
        if output.strip():
            await self.add_log(task_id, f"合并输出摘要:\n{output[:500]}")

        decision = self._load_decision_file(decision_file)
        if decision is None:
            decision = self._parse_decision_from_output(output)

        # Best-effort cleanup if CLI left repository in partial merge state.
        await self._cleanup_merge_state(proj_root)
        await self._ensure_on_main(proj_root)

        head_after = (await self.git("rev-parse", "--short", "HEAD", cwd=proj_root)).strip()
        after_contains_target = await self._is_ancestor(proj_root, target_commit, "main")
        low_output = output.lower()
        is_conflict = (
            (decision or {}).get("decision") == "conflict"
            or "conflict" in low_output
            or "automatic merge failed" in low_output
            or "冲突" in output
        )

        if after_contains_target:
            already_up_to_date = (
                before_contains_target
                or (decision or {}).get("decision") == "already_up_to_date"
                or head_before == head_after
            )
            if already_up_to_date:
                await self.transition_task(
                    task_id,
                    fields={"status": "pending_acceptance", "assignee": None, "commit_hash": head_after},
                    handoff={
                        "stage": "merge_to_acceptance",
                        "to_agent": "user",
                        "status_from": "approved",
                        "status_to": "pending_acceptance",
                        "title": "无需合并，进入验收",
                        "summary": "目标提交已在 main，直接进入待验收",
                        "commit_hash": head_after or target_commit,
                        "conclusion": "目标提交已存在于 main，进入验收",
                        "payload": {
                            "commit_hash": head_after or target_commit,
                            "already_up_to_date": True,
                            "reviewed_commit": target_commit,
                            "related_history_commits": related_history_commits,
                            "cli_exit_code": returncode,
                        },
                        "artifact_path": str(proj_root),
                    },
                    log_message="目标提交已在 main，进入待验收",
                )
                return

            await self.transition_task(
                task_id,
                fields={"status": "pending_acceptance", "assignee": None, "commit_hash": head_after},
                handoff={
                    "stage": "merge_to_acceptance",
                    "to_agent": "user",
                    "status_from": "approved",
                    "status_to": "pending_acceptance",
                    "title": "合并完成，交接验收",
                    "summary": f"CLI 已将审查通过提交合并到 main：{head_after}",
                    "commit_hash": head_after,
                    "conclusion": "合并完成，进入待验收",
                    "payload": {
                        "commit_hash": head_after,
                        "source_branch": dev_branch,
                        "reviewed_commit": target_commit,
                        "related_history_commits": related_history_commits,
                        "cli_exit_code": returncode,
                    },
                    "artifact_path": str(proj_root),
                },
                log_message=f"✅ 合并成功: {head_after}",
            )
            return

        if is_conflict:
            feedback = (
                f"[合并冲突] main 与 {dev_branch} 在合并 commit {target_commit} 时发生冲突，"
                f"请 {dev_agent} 在其分支解决冲突后重新提交。"
            )
            await self._return_to_dev_for_merge_issue(
                task_id=task_id,
                dev_agent=dev_agent,
                target_commit=target_commit,
                related_history_commits=related_history_commits,
                feedback=feedback,
                dev_branch=dev_branch,
            )
            return

        err_hint = str((decision or {}).get("message") or "").strip()
        if not err_hint:
            err_hint = (output[-500:] or f"exit={returncode}").strip()
        await self.add_alert(
            summary="合并失败（系统错误）",
            task_id=task_id,
            message=err_hint[:1000],
            kind="error",
            code="manager_merge_failed",
            stage="merge_failed",
            metadata={"commit_hash": target_commit, "exit_code": returncode},
        )
        await self.transition_task(
            task_id,
            fields={"status": "approved", "assignee": None},
            handoff={
                "stage": "merge_failed",
                "to_agent": self.name,
                "status_from": "approved",
                "status_to": "approved",
                "title": "合并失败（系统）",
                "summary": f"CLI 合并未生效：{err_hint[:200]}",
                "commit_hash": target_commit,
                "conclusion": "合并系统错误，等待重试",
                "payload": {"error": err_hint[:500], "commit_hash": target_commit, "exit_code": returncode},
            },
        )


if __name__ == "__main__":
    asyncio.run(ManagerAgent().run())
