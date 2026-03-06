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
        self.cli_name       = config.get("cli") or "codex"
        self._runtime_profile = str(config.get("runtime_profile") or ("developer" if self.name == "developer" else "generic")).strip().lower() or "generic"
        self._sync_from_latest_handoff = self._coerce_bool(
            config.get("sync_from_latest_handoff"),
            default=self._runtime_profile != "developer",
        )
        self._post_commit_retry_max = self._coerce_int(
            config.get("post_commit_retry_max"),
            default=6 if self._runtime_profile == "developer" else 1,
            minimum=1,
        )
        self._commit_hash_mode = str(
            config.get("commit_hash_mode") or ("full" if self._runtime_profile == "developer" else "short")
        ).strip().lower() or "short"

    @staticmethod
    def _coerce_bool(value, *, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return int(value) != 0
        text = str(value).strip().lower()
        if not text:
            return default
        return text in {"1", "true", "yes", "on"}

    @staticmethod
    def _coerce_int(value, *, default: int, minimum: int = 1) -> int:
        try:
            parsed = int(value)
        except Exception:
            parsed = default
        return max(minimum, parsed)

    def _uses_developer_profile(self) -> bool:
        return self._runtime_profile == "developer"

    def _failure_stage(self) -> str:
        return "developer_failed" if self._uses_developer_profile() else f"{self.name}_failed"

    def _commit_required_stage(self) -> str:
        return "dev_commit_required" if self._uses_developer_profile() else f"{self.name}_commit_required"

    def _no_progress_stage(self) -> str:
        return "dev_no_progress" if self._uses_developer_profile() else f"{self.name}_no_progress"

    def _post_commit_sync_stage(self) -> str:
        return "developer_post_commit_sync" if self._uses_developer_profile() else f"{self.name}_post_commit_sync"

    def _dirty_patchset_transition_spec(
        self,
        *,
        commit_display: str,
        dirty_status: str,
    ) -> dict[str, str]:
        if self._uses_developer_profile():
            return {
                "stage": "dev_dirty_patchset",
                "to_agent": self.name,
                "title": "开发交付未冻结",
                "summary": f"CLI 已提交 commit {commit_display}，但工作区仍有未提交改动，未进入审查",
                "conclusion": "检测到提交后工作区不干净；请在 CLI 内整理并提交剩余改动后再送审",
            }
        return {
            "stage": f"{self.name}_dirty_patchset",
            "to_agent": self.name,
            "title": f"{self._display_name} 交付未冻结",
            "summary": f"检测到 CLI 已提交 commit {commit_display}，但工作区仍有未提交改动，保持在 {dirty_status}",
            "conclusion": f"{self._display_name} 需先清理工作区并完成提交，再继续推进",
        }

    def _post_commit_transition_spec(
        self,
        *,
        review_enabled: bool,
        commit_display: str,
        effective_next_status: str,
    ) -> dict[str, str]:
        if self._uses_developer_profile():
            return {
                "stage": "dev_to_review" if review_enabled else "dev_to_approved",
                "to_agent": "reviewer" if review_enabled else "manager",
                "title": "开发交接审查" if review_enabled else "开发直达合并",
                "summary": (
                    f"CLI 已提交 commit {commit_display}，等待审查"
                    if review_enabled
                    else f"CLI 已提交 commit {commit_display}，跳过审查，交由 Manager 合并"
                ),
                "conclusion": (
                    "开发完成，等待审查结论"
                    if review_enabled
                    else "开发完成，已跳过审查并转交 Manager 合并"
                ),
            }
        return {
            "stage": f"{self.name}_handoff",
            "to_agent": "manager" if effective_next_status == "approved" else self.name,
            "title": f"{self._display_name} 交接",
            "summary": f"检测到 CLI 已生成 commit {commit_display}，推进到 {effective_next_status}",
            "conclusion": f"{self._display_name} 完成，推进到 {effective_next_status}",
        }

    async def _resolve_commit_hash(self, worktree_dev, head_after: str) -> tuple[str, str]:
        display = head_after[:7] if head_after else ""
        if self._commit_hash_mode == "full":
            return head_after, display
        commit_hash = (await self.git("rev-parse", "--short", "HEAD", cwd=worktree_dev)).strip()
        return commit_hash, (commit_hash[:7] if commit_hash else display)

    async def process_task(self, task: dict):
        task_id = task["id"]
        if await self.stop_if_task_cancelled(task_id, "开始处理前"):
            return
        proj_root, worktree_dev, branch = await self.ensure_agent_workspace(task, agent_key=self.name)

        await self.add_log(
            task_id, f"{self._display_name} 接手，分支: {branch}，工作目录: {worktree_dev}"
        )
        if self._sync_from_latest_handoff:
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
        execution_contract = self.build_execution_contract_block(task)
        if execution_contract:
            prompt += f"\n\n{execution_contract}\n"
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
            commit_hash, commit_display = await self._resolve_commit_hash(worktree_dev, head_after)
            patchset = await self.build_patchset_snapshot(
                worktree_dev,
                source_branch=branch,
                head_sha=head_after,
            )
            await self.add_log(task_id, f"检测到 CLI 已直接创建提交: {commit_display}")
            if diff_stat:
                await self.add_log(
                    task_id,
                    "检测到提交后工作区仍有未提交改动；本轮不会进入审查/合并，需先清理工作区。",
                )
                if await self.stop_if_task_cancelled(task_id, "检测到 dirty patchset 回退前"):
                    return
                dirty_status = "needs_changes" if self._uses_developer_profile() else prev_status
                dirty_spec = self._dirty_patchset_transition_spec(
                    commit_display=commit_display,
                    dirty_status=dirty_status,
                )
                update_fields = {
                    "status": dirty_status,
                    "assignee": None,
                    "commit_hash": commit_hash,
                    "current_patchset_id": str(patchset.get("id") or "").strip() or None,
                    "current_patchset_status": "draft",
                }
                if self._uses_developer_profile():
                    update_fields["review_feedback"] = (
                        "检测到提交后工作区仍有未提交改动；本轮未送审，请在 CLI 内整理并提交剩余改动后重试。"
                    )
                if str(dirty_status or "").strip().lower() in {"todo", "needs_changes"}:
                    update_fields["assigned_agent"] = self.name
                    update_fields["dev_agent"] = self.name
                await self.transition_task(
                    task_id,
                    fields=update_fields,
                    handoff={
                        "stage": dirty_spec["stage"],
                        "to_agent": dirty_spec["to_agent"],
                        "status_from": prev_status,
                        "status_to": dirty_status,
                        "title": dirty_spec["title"],
                        "summary": dirty_spec["summary"],
                        "commit_hash": commit_hash,
                        "conclusion": dirty_spec["conclusion"],
                        "payload": {
                            "commit_hash": commit_hash,
                            "source_branch": branch,
                            "committed_by_cli": True,
                            "review_enabled": is_review_enabled(task),
                            "has_commit": True,
                            "has_uncommitted_changes": True,
                            "requires_clean_worktree": True,
                            "uncommitted_diff_stat": diff_stat[:1200],
                            "patchset": {
                                **patchset,
                                "status": "draft",
                                "summary": dirty_spec["conclusion"],
                            },
                        },
                        "artifact_path": str(worktree_dev),
                    },
                    log_message=f"检测到 dirty patchset，回退到 {dirty_status}",
                )
                return
            if await self.stop_if_task_cancelled(task_id, "CLI 已提交后状态更新前"):
                return
            effective_next_status = self.next_status
            if effective_next_status == "in_review" and not is_review_enabled(task):
                effective_next_status = "approved"
            review_enabled = is_review_enabled(task)
            update_fields = {
                "status": effective_next_status,
                "assignee": None,
                "commit_hash": commit_hash,
                "current_patchset_id": str(patchset.get("id") or "").strip() or None,
                "current_patchset_status": (
                    "approved" if effective_next_status == "approved" else "submitted"
                ),
            }
            if effective_next_status == "in_review":
                update_fields["assigned_agent"] = self.name
                update_fields["dev_agent"] = self.name
            elif effective_next_status == "approved":
                update_fields["assigned_agent"] = "manager"
                update_fields["dev_agent"] = self.name
            transition_spec = self._post_commit_transition_spec(
                review_enabled=review_enabled,
                commit_display=commit_display,
                effective_next_status=effective_next_status,
            )
            update_error = None
            for i in range(1, self._post_commit_retry_max + 1):
                try:
                    result = await self.transition_task(
                        task_id,
                        fields=update_fields,
                        handoff={
                            "stage": transition_spec["stage"],
                            "to_agent": transition_spec["to_agent"],
                            "status_from": prev_status,
                            "status_to": effective_next_status,
                            "title": transition_spec["title"],
                            "summary": transition_spec["summary"],
                            "commit_hash": commit_hash,
                            "conclusion": transition_spec["conclusion"],
                            "payload": {
                                "commit_hash": commit_hash,
                                "source_branch": branch,
                                "committed_by_cli": True,
                                "review_enabled": review_enabled,
                                "has_uncommitted_changes": bool(diff_stat),
                                "uncommitted_diff_stat": diff_stat[:1200] if diff_stat else "",
                                "patchset": {
                                    **patchset,
                                    "status": "approved" if effective_next_status == "approved" else "submitted",
                                    "summary": transition_spec["conclusion"],
                                },
                            },
                            "artifact_path": str(worktree_dev),
                        },
                        log_message=f"检测到 CLI 已生成提交，推进至 {effective_next_status}",
                    )
                    if result is None:
                        await self.stop_if_task_cancelled(task_id, "提交后同步状态")
                        return
                    update_error = None
                    break
                except Exception as e:
                    update_error = e
                    if i < self._post_commit_retry_max:
                        self._post_output_bg(
                            f"⚠ CLI 已提交 {commit_display}，同步状态失败（{i}/{self._post_commit_retry_max}）：{str(e)[:120]}"
                        )
                        await asyncio.sleep(min(2 * i, 10))
            if update_error is not None:
                await self.add_log(
                    task_id,
                    (
                        f"⚠ CLI 已提交 {commit_display}，但无法把任务推进到 {effective_next_status}：{update_error}。"
                        "保持当前状态，等待后续重试/人工处理。"
                    ),
                )
                await self.add_alert(
                    summary="提交已完成但状态同步失败",
                    task_id=task_id,
                    message=f"commit={commit_hash}; error={update_error}",
                    kind="warning",
                    code=f"{self.name}_commit_sync_failed",
                    stage=self._post_commit_sync_stage(),
                    metadata={"commit_hash": commit_hash},
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
                    "stage": self._commit_required_stage(),
                    "to_agent": self.name,
                    "status_from": prev_status,
                    "status_to": prev_status,
                    "title": "开发需在 CLI 内提交" if self._uses_developer_profile() else f"{self._display_name} 需在 CLI 内提交",
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
                "stage": self._no_progress_stage(),
                "to_agent": self.name,
                "status_from": prev_status,
                "status_to": prev_status,
                "title": "开发未产生新提交" if self._uses_developer_profile() else f"{self._display_name} 未产生新变更",
                "summary": (
                    f"未检测到 CLI 内提交或文件改动，保持在 {prev_status}"
                    if self._uses_developer_profile()
                    else f"未检测到新提交或文件改动，保持在 {prev_status}"
                ),
                "conclusion": (
                    "本轮无可审查交付，等待下一轮开发"
                    if self._uses_developer_profile()
                    else "本轮未产出可交付变更，等待下一轮执行"
                ),
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
