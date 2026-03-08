import asyncio
import json

from base import BaseAgent, is_review_enabled
from task_intelligence import (
    compute_failure_fingerprint,
    detect_surface_from_changed_files,
    evaluate_contract_evidence,
    find_surface_violations,
    next_retry_strategy,
    normalize_allowed_surface,
)


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

    def _delivery_retry_limit(self) -> int:
        return 0 if self._uses_developer_profile() else 3

    def _delivery_retry_stages(self) -> set[str]:
        return {self._commit_required_stage(), self._no_progress_stage()}

    def _delivery_blocked_stage(self) -> str:
        return f"{self.name}_delivery_blocked"

    async def _current_delivery_retry_count(self, task_id: str) -> int:
        retry_limit = self._delivery_retry_limit()
        if retry_limit <= 0:
            return 0
        handoffs = await self.get_handoffs(task_id)
        if not handoffs:
            return 0
        tracked_stages = self._delivery_retry_stages()
        count = 0
        for item in reversed(handoffs):
            stage = str(item.get("stage") or "").strip()
            if stage in tracked_stages:
                count += 1
                continue
            break
        return count

    async def _transition_delivery_failure(
        self,
        *,
        task: dict,
        task_id: str,
        prev_status: str,
        failure_stage: str,
        failure_title: str,
        failure_summary: str,
        failure_conclusion: str,
        failure_payload: dict,
        artifact_path: str,
        retry_reason: str,
        retry_log_message: str,
    ) -> None:
        current_status = str(task.get("status") or prev_status or "").strip() or prev_status
        retry_limit = self._delivery_retry_limit()
        retry_count = (
            await self._current_delivery_retry_count(task_id) + 1
            if retry_limit > 0
            else 1
        )
        failure_fingerprint = compute_failure_fingerprint(
            stage=failure_stage,
            summary=failure_summary,
            extra=json.dumps(failure_payload, ensure_ascii=False)[:800],
        )
        same_failure_streak = self._same_failure_streak(task, failure_fingerprint)
        retry_strategy = next_retry_strategy(
            current_strategy=self._current_retry_strategy(task),
            failure_stage=failure_stage,
            same_fingerprint_streak=same_failure_streak,
            open_issue_count=int(task.get("open_issue_count") or 0),
        )
        payload = {
            **failure_payload,
            "delivery_retry_count": retry_count,
            "delivery_retry_limit": retry_limit,
            "attempt": {
                "stage": failure_stage,
                "outcome": "delivery_failed",
                "execution_phase": self._current_execution_phase(task),
                "retry_strategy": retry_strategy,
                "failure_fingerprint": failure_fingerprint,
                "same_fingerprint_streak": same_failure_streak,
                "summary": failure_summary,
                "metadata": {
                    "retry_reason": retry_reason,
                    "delivery_retry_count": retry_count,
                },
            },
        }
        if retry_limit > 0 and retry_count >= retry_limit:
            await self.transition_task(
                task_id,
                fields={
                    "status": "blocked",
                    "assignee": None,
                    "assigned_agent": self.name,
                },
                handoff={
                    "stage": self._delivery_blocked_stage(),
                    "to_agent": self.name,
                    "status_from": current_status,
                    "status_to": "blocked",
                    "title": f"{self._display_name} 连续未完成交付",
                    "summary": f"连续 {retry_count} 次{retry_reason}，任务转为 blocked",
                    "conclusion": (
                        f"{self._display_name} 连续 {retry_count} 次未完成交付，"
                        "请人工检查提示词或执行链路后再重试"
                    ),
                    "payload": {
                        **payload,
                        "delivery_blocked": True,
                        "resume_status": prev_status,
                        "resume_assigned_agent": self.name,
                        "latest_failure_stage": failure_stage,
                    },
                    "artifact_path": artifact_path,
                },
                log_message=(
                    f"连续 {retry_count} 次{retry_reason}，任务转为 blocked，等待人工处理"
                ),
            )
            return

        update_fields = {"status": prev_status, "assignee": None}
        if str(prev_status or "").strip().lower() in {"todo", "needs_changes"}:
            update_fields["assigned_agent"] = self.name
            update_fields["dev_agent"] = self.name
        await self.transition_task(
            task_id,
            fields=update_fields,
            handoff={
                "stage": failure_stage,
                "to_agent": self.name,
                "status_from": current_status,
                "status_to": prev_status,
                "title": failure_title,
                "summary": failure_summary,
                "conclusion": failure_conclusion,
                "payload": payload,
                "artifact_path": artifact_path,
            },
            log_message=retry_log_message,
        )

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

    def _current_retry_strategy(self, task: dict) -> str:
        return str(task.get("retry_strategy") or "default_implement").strip() or "default_implement"

    def _current_execution_phase(self, task: dict, *, default: str = "explore") -> str:
        open_issue_count = int(task.get("open_issue_count") or 0)
        if open_issue_count > 0 or str(task.get("_claimed_from_status") or task.get("status") or "").strip().lower() == "needs_changes":
            return "converge"
        phase = str(task.get("execution_phase") or "").strip()
        return phase or default

    def _same_failure_streak(self, task: dict, fingerprint: str) -> int:
        previous = str(task.get("failure_fingerprint") or "").strip()
        streak = int(task.get("same_fingerprint_streak") or 0)
        if previous and previous == fingerprint:
            return streak + 1
        return 1

    def _build_pre_review_evidence_bundle(self, task: dict, patchset: dict) -> dict:
        contract = self._extract_task_contract(task)
        changed_files = patchset.get("changed_files") or []
        artifact_manifest = patchset.get("artifact_manifest") or {}
        current_surface = detect_surface_from_changed_files(changed_files)
        allowed_surface = normalize_allowed_surface(
            task.get("allowed_surface") or task.get("allowed_surface_json")
        )
        if not any(allowed_surface.values()):
            current_contract = task.get("current_contract")
            if isinstance(current_contract, dict):
                allowed_surface = normalize_allowed_surface(current_contract.get("allowed_surface"))
        if not any(allowed_surface.values()):
            allowed_surface = current_surface

        changed_paths = [str(item.get("path") or "").strip() for item in changed_files if isinstance(item, dict)]
        changed_paths = [path for path in changed_paths if path]
        changed_paths_lower = [path.lower() for path in changed_paths]
        issues: list[dict] = []
        acceptance_checks: list[dict] = []

        deliverable_paths = normalize_allowed_surface({"files": (allowed_surface.get("files") or [])}).get("files") or []
        for path in deliverable_paths[:24]:
            if changed_paths and any(item == path or item.endswith(path) for item in changed_paths):
                continue
            issues.append(
                {
                    "issue_id": f"deliverable-{path}",
                    "acceptance_item": "交付物",
                    "severity": "medium",
                    "category": "scope",
                    "summary": f"预检未发现约定交付物变更：{path}",
                    "reproducer": "检查本次 patchset changed_files",
                    "evidence_gap": f"缺少与交付物 {path} 对应的变更证据",
                    "scope": path,
                    "fix_hint": "确认交付物是否已实现；若已实现，请补充对应文件变更或调整合同描述",
                    "status": "new",
                }
            )

        for item in contract.get("acceptance", []) or []:
            lower = str(item or "").lower()
            status = "inferred"
            evidence = "patchset changed_files"
            if any(token in lower for token in ("测试", "test", "pytest", "unit", "e2e", "integration")):
                proved = any(
                    "/test" in path
                    or path.startswith("test")
                    or path.startswith("tests/")
                    for path in changed_paths_lower
                )
                status = "proved" if proved else "missing"
                evidence = "test paths in patchset"
                if not proved:
                    issues.append(
                        {
                            "issue_id": f"acceptance-test-{len(issues)+1}",
                            "acceptance_item": item,
                            "severity": "high",
                            "category": "coverage",
                            "summary": "预检未发现与测试验收项对应的测试文件变更",
                            "reproducer": "检查 patchset changed_files 是否包含 tests/ 或 test* 文件",
                            "evidence_gap": "缺少测试文件或测试证据",
                            "scope": "tests",
                            "fix_hint": "优先补齐真实验收路径的测试，再重新送审",
                            "status": "new",
                        }
                    )
            elif any(token in lower for token in ("文档", "readme", "说明", ".md")):
                proved = any(path.endswith(".md") for path in changed_paths_lower)
                status = "proved" if proved else "missing"
                evidence = "markdown/doc files in patchset"
                if not proved:
                    issues.append(
                        {
                            "issue_id": f"acceptance-doc-{len(issues)+1}",
                            "acceptance_item": item,
                            "severity": "medium",
                            "category": "docs",
                            "summary": "预检未发现与文档验收项对应的文档文件变更",
                            "reproducer": "检查 patchset changed_files 是否包含 .md 文档",
                            "evidence_gap": "缺少文档文件或文档证据",
                            "scope": "docs",
                            "fix_hint": "补齐 README/说明文档或提供更直接的行为证据",
                            "status": "new",
                        }
                    )
            acceptance_checks.append(
                {
                    "item": item,
                    "status": status,
                    "evidence": evidence,
                }
            )

        surface_violations = find_surface_violations(allowed_surface, current_surface)
        if str(task.get("_claimed_from_status") or task.get("status") or "").strip().lower() == "needs_changes":
            for violation in surface_violations:
                issues.append(
                    {
                        "issue_id": f"surface-{len(issues)+1}",
                        "acceptance_item": "allowed_surface",
                        "severity": "high",
                        "category": "scope",
                        "summary": violation,
                        "reproducer": "对比当前 patchset 交付面与冻结的 allowed_surface",
                        "evidence_gap": violation,
                        "scope": violation,
                        "fix_hint": "移除越界交付面，或在需求合同中显式放行后再继续",
                        "status": "persisting",
                    }
                )

        evidence_result = evaluate_contract_evidence(
            contract,
            changed_files=changed_files,
            current_surface=current_surface,
            allowed_surface=allowed_surface,
            artifact_manifest=artifact_manifest,
        )
        issues.extend(evidence_result["issues"])
        missing_checks = [item for item in acceptance_checks if item["status"] == "missing"]
        summary = (
            f"预检完成：changed_files={len(changed_paths)}，"
            f"missing_acceptance={len(missing_checks)}，"
            f"missing_evidence={len(evidence_result['missing_evidence_required'])}，"
            f"surface_violations={len(surface_violations)}"
        )
        return {
            "summary": summary,
            "bundle": {
                "contract_version": int(((task.get("current_contract") or {}).get("version") or 0)),
                "changed_files": changed_files,
                "acceptance_checks": acceptance_checks,
                "evidence_checks": evidence_result["evidence_checks"],
                "current_surface": current_surface,
                "allowed_surface": allowed_surface,
                "surface_violations": surface_violations,
                "missing_acceptance_checks": missing_checks,
                "missing_evidence_required": evidence_result["missing_evidence_required"],
                "assumption_conflicts": evidence_result["assumption_conflicts"],
                "artifact_manifest": artifact_manifest if isinstance(artifact_manifest, dict) else {},
            },
            "issues": issues,
            "allowed_surface": current_surface if any(current_surface.values()) else allowed_surface,
            "has_blockers": bool(issues),
        }

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
        current_status = str(task.get("status") or prev_status or "").strip() or prev_status
        is_rework = task.get("review_feedback") and prev_status == "needs_changes"
        rework_section = (
            f"## 审查反馈（必须全部修复）\n\n{task['review_feedback']}"
            if is_rework else ""
        )
        strategy_section = "\n\n".join(
            section
            for section in (
                self.build_retry_strategy_block(task),
                self.build_issue_ledger_block(task),
            )
            if str(section or "").strip()
        )

        if self._prompt_tpl:
            try:
                prompt = self._prompt_tpl.format(
                    task_title=task["title"],
                    task_description=task["description"] or "(无额外描述)",
                    rework_section=rework_section,
                    strategy_section=strategy_section,
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
        if strategy_section and "{strategy_section}" not in self._prompt_tpl:
            prompt += f"\n\n{strategy_section}\n"
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
            failure_stage = self._failure_stage()
            failure_summary = f"CLI 执行失败（exit={returncode}）"
            failure_fingerprint = compute_failure_fingerprint(
                stage=failure_stage,
                summary=failure_summary,
                output=output,
            )
            same_failure_streak = self._same_failure_streak(task, failure_fingerprint)
            retry_strategy = next_retry_strategy(
                current_strategy=self._current_retry_strategy(task),
                failure_stage=failure_stage,
                same_fingerprint_streak=same_failure_streak,
                open_issue_count=int(task.get("open_issue_count") or 0),
            )
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
            update_fields = {"status": prev_status, "assignee": None}
            if str(prev_status or "").strip().lower() in {"todo", "needs_changes"}:
                update_fields["assigned_agent"] = self.name
                update_fields["dev_agent"] = self.name
            await self.transition_task(
                task_id,
                fields=update_fields,
                handoff={
                    "stage": failure_stage,
                    "to_agent": self.name,
                    "status_from": current_status,
                    "status_to": prev_status,
                    "title": f"{self._display_name} 执行失败",
                    "summary": failure_summary,
                    "conclusion": f"{self._display_name} 执行失败，回退到 {prev_status}",
                    "payload": {
                        "exit_code": returncode,
                        "attempt": {
                            "stage": failure_stage,
                            "outcome": "cli_failed",
                            "execution_phase": self._current_execution_phase(task),
                            "retry_strategy": retry_strategy,
                            "failure_fingerprint": failure_fingerprint,
                            "same_fingerprint_streak": same_failure_streak,
                            "summary": failure_summary,
                            "metadata": {
                                "exit_code": returncode,
                                "output_excerpt": output[-1200:].strip(),
                            },
                        },
                    },
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
                dirty_fingerprint = compute_failure_fingerprint(
                    stage=dirty_spec["stage"],
                    summary=dirty_spec["summary"],
                    extra=diff_stat,
                )
                dirty_streak = self._same_failure_streak(task, dirty_fingerprint)
                dirty_strategy = next_retry_strategy(
                    current_strategy=self._current_retry_strategy(task),
                    failure_stage=dirty_spec["stage"],
                    same_fingerprint_streak=dirty_streak,
                    open_issue_count=int(task.get("open_issue_count") or 0),
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
                        "status_from": current_status,
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
                            "attempt": {
                                "stage": dirty_spec["stage"],
                                "outcome": "dirty_patchset",
                                "execution_phase": self._current_execution_phase(task),
                                "retry_strategy": dirty_strategy,
                                "failure_fingerprint": dirty_fingerprint,
                                "same_fingerprint_streak": dirty_streak,
                                "summary": dirty_spec["summary"],
                                "metadata": {
                                    "uncommitted_diff_stat": diff_stat[:1200],
                                },
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
            verifier_result = self._build_pre_review_evidence_bundle(task, patchset)
            if verifier_result["has_blockers"]:
                verifier_stage = (
                    "developer_pre_review_failed"
                    if self._uses_developer_profile()
                    else f"{self.name}_pre_review_failed"
                )
                verifier_status = "needs_changes" if self._uses_developer_profile() else prev_status
                verifier_fingerprint = compute_failure_fingerprint(
                    stage=verifier_stage,
                    summary=verifier_result["summary"],
                    extra="\n".join(
                        str(item.get("summary") or "").strip()
                        for item in verifier_result["issues"]
                    ),
                )
                verifier_streak = self._same_failure_streak(task, verifier_fingerprint)
                retry_strategy = next_retry_strategy(
                    current_strategy=self._current_retry_strategy(task),
                    failure_stage=verifier_stage,
                    same_fingerprint_streak=verifier_streak,
                    open_issue_count=int(task.get("open_issue_count") or 0),
                    has_surface_violation=bool(verifier_result["bundle"].get("surface_violations")),
                    has_evidence_gap=bool(verifier_result["bundle"].get("missing_acceptance_checks")),
                )
                update_fields = {
                    "status": verifier_status,
                    "assignee": None,
                    "commit_hash": commit_hash,
                    "current_patchset_id": str(patchset.get("id") or "").strip() or None,
                    "current_patchset_status": "draft",
                    "allowed_surface_json": verifier_result["allowed_surface"],
                }
                if self._uses_developer_profile():
                    update_fields["review_feedback"] = (
                        "预检未通过，本轮未送审。请先补齐缺失证据或收敛越界交付面后再重试。\n\n"
                        + self.build_issue_ledger_block({"open_issues": verifier_result["issues"]})
                    )[:4000]
                if str(verifier_status or "").strip().lower() in {"todo", "needs_changes"}:
                    update_fields["assigned_agent"] = self.name
                    update_fields["dev_agent"] = self.name
                await self.transition_task(
                    task_id,
                    fields=update_fields,
                    handoff={
                        "stage": verifier_stage,
                        "to_agent": self.name,
                        "status_from": current_status,
                        "status_to": verifier_status,
                        "title": "送审前预检未通过",
                        "summary": verifier_result["summary"],
                        "commit_hash": commit_hash,
                        "conclusion": "预检发现未闭环问题，回到开发收敛阶段",
                        "payload": {
                            "commit_hash": commit_hash,
                            "source_branch": branch,
                            "review_enabled": review_enabled,
                            "issues": verifier_result["issues"],
                            "resolve_missing_issues": True,
                            "evidence_bundle": verifier_result["bundle"],
                            "evidence_summary": verifier_result["summary"],
                            "patchset": {
                                **patchset,
                                "status": "draft",
                                "summary": verifier_result["summary"],
                            },
                            "attempt": {
                                "stage": verifier_stage,
                                "outcome": "pre_review_failed",
                                "execution_phase": "verify",
                                "retry_strategy": retry_strategy,
                                "failure_fingerprint": verifier_fingerprint,
                                "same_fingerprint_streak": verifier_streak,
                                "summary": verifier_result["summary"],
                                "metadata": {
                                    "issue_count": len(verifier_result["issues"]),
                                    "surface_violations": verifier_result["bundle"].get("surface_violations") or [],
                                },
                            },
                        },
                        "artifact_path": str(worktree_dev),
                    },
                    log_message=f"送审前预检未通过，回退到 {verifier_status}",
                )
                return
            update_fields = {
                "status": effective_next_status,
                "assignee": None,
                "commit_hash": commit_hash,
                "current_patchset_id": str(patchset.get("id") or "").strip() or None,
                "current_patchset_status": (
                    "approved" if effective_next_status == "approved" else "submitted"
                ),
                "allowed_surface_json": verifier_result["allowed_surface"],
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
                            "status_from": current_status,
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
                                "resolve_open_issues": True,
                                "resolve_issue_sources": ["verifier"],
                                "issue_resolution_reason": "submitted_for_review",
                                "evidence_bundle": verifier_result["bundle"],
                                "evidence_summary": verifier_result["summary"],
                                "patchset": {
                                    **patchset,
                                    "status": "approved" if effective_next_status == "approved" else "submitted",
                                    "summary": transition_spec["conclusion"],
                                },
                                "attempt": {
                                    "stage": transition_spec["stage"],
                                    "outcome": "submitted" if effective_next_status == "in_review" else "approved",
                                    "execution_phase": "verify",
                                    "retry_strategy": self._current_retry_strategy(task),
                                    "failure_fingerprint": "",
                                    "same_fingerprint_streak": 0,
                                    "summary": verifier_result["summary"],
                                    "metadata": {
                                        "review_enabled": review_enabled,
                                        "issue_count": len(verifier_result["issues"]),
                                    },
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
            await self._transition_delivery_failure(
                task=task,
                task_id=task_id,
                prev_status=prev_status,
                failure_stage=self._commit_required_stage(),
                failure_title=(
                    "开发需在 CLI 内提交"
                    if self._uses_developer_profile()
                    else f"{self._display_name} 需在 CLI 内提交"
                ),
                failure_summary="检测到未提交改动，未自动提交，保持当前状态",
                failure_conclusion="请在 CLI 内完成提交后再交接",
                failure_payload={
                    "has_commit": False,
                    "requires_cli_commit": True,
                    "source_branch": branch,
                    "diff_stat": diff.strip()[:1200],
                    "head_changed": cli_created_commit,
                },
                artifact_path=str(worktree_dev),
                retry_reason="未在 CLI 内提交交付改动",
                retry_log_message=f"检测到未提交改动，保持 {prev_status}",
            )
            return

        await self.add_log(task_id, "未检测到新提交或文件改动，不推进状态。")
        if await self.stop_if_task_cancelled(task_id, "无变更回退前"):
            return
        await self._transition_delivery_failure(
            task=task,
            task_id=task_id,
            prev_status=prev_status,
            failure_stage=self._no_progress_stage(),
            failure_title=(
                "开发未产生新提交"
                if self._uses_developer_profile()
                else f"{self._display_name} 未产生新变更"
            ),
            failure_summary=(
                f"未检测到 CLI 内提交或文件改动，保持在 {prev_status}"
                if self._uses_developer_profile()
                else f"未检测到新提交或文件改动，保持在 {prev_status}"
            ),
            failure_conclusion=(
                "本轮无可审查交付，等待下一轮开发"
                if self._uses_developer_profile()
                else "本轮未产出可交付变更，等待下一轮执行"
            ),
            failure_payload={"has_commit": False, "no_progress": True, "source_branch": branch},
            artifact_path=str(worktree_dev),
            retry_reason="未产出可交付文件",
            retry_log_message=f"无文件变更，保持 {prev_status}",
        )

    def working_status_for(self, status: str) -> str:
        return self._working_status or status


if __name__ == "__main__":
    import sys
    print("GenericAgent requires config dict; use run_all.py to launch.")
    sys.exit(1)
