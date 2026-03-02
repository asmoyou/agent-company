import asyncio
import json
import re
from pathlib import Path

from base import BaseAgent, get_project_dirs, parse_status_list

TRIAGE_PROMPT_DEFAULT = (
    "你是一个专业的项目评估与分解专家。请评估以下任务是否需要分解：\n\n"
    "## 任务标题\n{task_title}\n\n"
    "## 任务描述\n{task_description}\n\n"
    "## 可用 Agent 类型\n{agent_list}\n\n"
    "## 评估标准\n"
    "- **简单任务**：可以由单个 agent 独立完成，工作量在 1-2 小时内\n"
    "- **复杂任务**：涉及多个独立功能模块，或需要不同专业技能协作\n\n"
    "## 输出格式（严格 JSON，不要任何其他文字）\n\n"
    "如果是简单任务：\n"
    '{"action": "simple", "reason": "一句话说明为何不需要分解"}\n\n'
    "如果是复杂任务：\n"
    '{"action": "decompose", "subtasks": [\n'
    '  {"title": "子任务标题", "description": "详细描述和验收标准", "agent": "developer"}\n'
    "]}"
)

FORCE_DECOMPOSE_PROMPT = (
    "你是一个专业的项目分解专家。请将以下任务分解为 2-5 个可执行的子任务：\n\n"
    "## 任务标题\n{task_title}\n\n"
    "## 任务描述\n{task_description}\n\n"
    "## 可用 Agent 类型\n{agent_list}\n\n"
    "只输出 JSON 数组，不要任何其他文字：\n"
    "[\n"
    '  {"title": "子任务标题", "description": "详细描述和验收标准", "agent": "developer"}\n'
    "]"
)


class LeaderAgent(BaseAgent):
    name = "leader"
    poll_statuses = ["triage", "decompose"]
    cli_name = "claude"
    working_status = "triaging"

    def __init__(self, shutdown_event=None, config: dict | None = None):
        super().__init__(shutdown_event)
        cfg = config or {}
        self.poll_statuses = parse_status_list(cfg.get("poll_statuses"), ["triage", "decompose"])
        self.cli_name = str(cfg.get("cli") or "claude")
        self.prompt_template = str(cfg.get("prompt") or TRIAGE_PROMPT_DEFAULT)
        self.working_status = str(cfg.get("working_status") or "triaging")

    def respect_assignment_for(self, status: str) -> bool:
        return False

    async def _get_agent_list(self) -> str:
        try:
            r = await self.http.get("/agent-types")
            r.raise_for_status()
            types = r.json()
            lines = [
                f"- {t['key']}: {t['name']} — {t.get('description', '')}"
                for t in types if t["key"] not in ("leader", "manager")
            ]
            return "\n".join(lines) if lines else "- developer: 开发者"
        except Exception:
            return "- developer: 开发者"

    def _parse_triage(self, output: str) -> dict | None:
        """Parse {"action": "simple"|"decompose", ...} from CLI output."""
        for m in re.findall(r"```(?:json)?\s*(\{[\s\S]+?\})\s*```", output):
            try:
                d = json.loads(m)
                if "action" in d:
                    return d
            except json.JSONDecodeError:
                pass
        for m in re.findall(r"\{[^{}]*\"action\"[^{}]*\}", output, re.DOTALL):
            try:
                d = json.loads(m)
                if "action" in d:
                    return d
            except json.JSONDecodeError:
                pass
        # Bigger nested object with subtasks
        try:
            start = output.rfind('{"action"')
            if start == -1:
                start = output.rfind("{'action'")
            if start != -1:
                chunk = output[start:]
                # find balanced brace
                depth, end = 0, -1
                for i, c in enumerate(chunk):
                    if c == '{':
                        depth += 1
                    elif c == '}':
                        depth -= 1
                        if depth == 0:
                            end = i + 1
                            break
                if end > 0:
                    d = json.loads(chunk[:end])
                    if "action" in d:
                        return d
        except Exception:
            pass
        return None

    def _parse_subtasks_array(self, output: str) -> list[dict]:
        for m in re.findall(r"```(?:json)?\s*(\[[\s\S]+?\])\s*```", output):
            try:
                d = json.loads(m)
                if isinstance(d, list) and d:
                    return d
            except json.JSONDecodeError:
                pass
        for m in re.findall(r"\[\s*\{[\s\S]*?\}\s*\]", output):
            try:
                d = json.loads(m)
                if isinstance(d, list) and d:
                    return d
            except json.JSONDecodeError:
                pass
        return []

    async def _create_subtasks(self, task: dict, subtasks: list[dict]) -> int:
        created = 0
        for i, st in enumerate(subtasks, 1):
            title = str(st.get("title") or f"子任务 {i}")[:200]
            desc  = str(st.get("description") or "")
            agent = str(st.get("agent") or "developer")
            try:
                r = await self.http.post("/tasks", json={
                    "title": title,
                    "description": desc,
                    "project_id": task.get("project_id"),
                    "parent_task_id": task["id"],
                    "assigned_agent": agent,
                    "status": "todo",   # subtasks skip triage
                })
                r.raise_for_status()
                sub = r.json()
                created += 1
                await self.add_log(task["id"], f"  ✓ [{i}] {title} (→ {agent})")
                self._post_output_bg(f"  子任务 {i}: {title}")
                await self.add_handoff(
                    task["id"],
                    stage="leader_assign_subtask",
                    to_agent=agent,
                    status_from=task.get("status"),
                    status_to="todo",
                    title=f"分配子任务 {i}",
                    summary=f"已创建子任务「{title}」并分配给 {agent}",
                    conclusion=f"子任务已分配给 {agent}",
                    payload={
                        "subtask_id": sub.get("id"),
                        "subtask_title": title,
                        "subtask_agent": agent,
                    },
                )
            except Exception as e:
                await self.add_log(task["id"], f"  ✗ [{i}] 创建失败: {e}")
        return created

    async def process_task(self, task: dict):
        original_status = task.get("_claimed_from_status", task.get("status"))  # "triage" or "decompose"
        task_id = task["id"]
        if await self.stop_if_task_cancelled(task_id, "开始评估前"):
            return

        if original_status == "decompose":
            # User explicitly requested decomposition — skip evaluation
            await self.add_log(task_id, "Leader 执行强制分解（用户指定）")
            await self._force_decompose(task)
        else:
            # Auto triage — evaluate complexity first
            await self.add_log(task_id, "Leader 评估任务复杂度...")
            await self._auto_triage(task)

    async def _auto_triage(self, task: dict):
        task_id = task["id"]

        # Idempotency: if subtasks already exist (crash recovery), just mark decomposed
        r = await self.http.get(f"/tasks/{task_id}/subtasks")
        if r.status_code == 200 and r.json():
            await self.add_log(task_id, "已有子任务，标记为 decomposed（崩溃恢复）")
            await self.update_task(task_id, status="decomposed", assignee=None)
            return

        agent_list  = await self._get_agent_list()
        prompt_tpl  = self.prompt_template or TRIAGE_PROMPT_DEFAULT
        try:
            prompt = prompt_tpl.format(
                task_title=task["title"],
                task_description=task["description"] or "(无额外描述)",
                agent_list=agent_list,
            )
        except Exception:
            prompt = prompt_tpl
        handoff_context = await self.build_handoff_context(task_id)
        if handoff_context:
            prompt += f"\n\n{handoff_context}\n"

        proj_root, _ = get_project_dirs(task)
        run_dir = proj_root if proj_root.exists() else Path.cwd()

        returncode, output = await self.run_cli(prompt, cwd=run_dir, task_id=task_id)
        if returncode != 0:
            if await self.stop_if_task_cancelled(task_id, "评估 CLI 失败后"):
                return
            prev_status = task.get("_claimed_from_status", task.get("status", "triage"))
            await self.add_log(task_id, f"❌ Leader 评估失败（exit={returncode}），退回 {prev_status}")
            if output.strip():
                await self.add_log(task_id, f"错误输出:\n{output[:800]}")
            await self.add_alert(
                summary=f"Leader 评估失败（exit={returncode}）",
                task_id=task_id,
                message=output[-1200:].strip(),
                kind="error",
                code="leader_triage_failed",
                stage="leader_failed",
                metadata={"exit_code": returncode},
            )
            await self.add_handoff(
                task_id,
                stage="leader_failed",
                to_agent=self.name,
                status_from=prev_status,
                status_to=prev_status,
                title="任务评估失败",
                summary=f"Leader 评估失败（exit={returncode}），保持状态 {prev_status}",
                conclusion=f"评估失败，保持状态 {prev_status}",
                payload={"exit_code": returncode},
            )
            await self.update_task(task_id, status=prev_status, assignee=None)
            return
        await self.add_log(task_id, f"评估输出:\n{output[:600]}")
        if await self.stop_if_task_cancelled(task_id, "评估输出后"):
            return

        decision = self._parse_triage(output)

        if decision and decision.get("action") == "decompose":
            subtasks = decision.get("subtasks") or []
            if subtasks:
                await self.add_log(task_id, f"判断为复杂任务，分解为 {len(subtasks)} 个子任务")
                n = await self._create_subtasks(task, subtasks)
                await self.add_handoff(
                    task_id,
                    stage="leader_to_decomposed",
                    to_agent="multi-agent",
                    status_from=task.get("_claimed_from_status", task.get("status")),
                    status_to="decomposed",
                    title="任务分解完成",
                    summary=f"已分解为 {n} 个子任务并分配",
                    conclusion=f"复杂任务，已拆分为 {n} 个子任务",
                    payload={"subtask_count": n},
                )
                await self.update_task(task_id, status="decomposed", assignee=None)
                await self.add_log(task_id, f"✅ 分解完成，共创建 {n} 个子任务")
                self._post_output_bg(f"✓ 已分解为 {n} 个子任务")
                return

        # Simple task (or parse failed) — push to todo
        reason = decision.get("reason", "判定为简单任务") if decision else "无法解析评估结果，按简单任务处理"
        await self.add_log(task_id, f"判断为简单任务：{reason}")
        await self.add_handoff(
            task_id,
            stage="leader_to_todo",
            to_agent=task.get("assigned_agent") or "developer",
            status_from=task.get("_claimed_from_status", task.get("status")),
            status_to="todo",
            title="任务转入开发",
            summary=reason[:300],
            conclusion=reason[:300] or "判定为简单任务，转入开发",
            payload={"action": "simple"},
        )
        await self.update_task(task_id, status="todo", assignee=None)
        self._post_output_bg(f"✓ 简单任务，推进至 todo")

    async def _force_decompose(self, task: dict):
        task_id = task["id"]
        agent_list = await self._get_agent_list()
        try:
            prompt = FORCE_DECOMPOSE_PROMPT.format(
                task_title=task["title"],
                task_description=task["description"] or "(无额外描述)",
                agent_list=agent_list,
            )
        except Exception:
            prompt = FORCE_DECOMPOSE_PROMPT
        handoff_context = await self.build_handoff_context(task_id)
        if handoff_context:
            prompt += f"\n\n{handoff_context}\n"

        proj_root, _ = get_project_dirs(task)
        run_dir = proj_root if proj_root.exists() else Path.cwd()

        returncode, output = await self.run_cli(prompt, cwd=run_dir, task_id=task_id)
        if returncode != 0:
            if await self.stop_if_task_cancelled(task_id, "分解 CLI 失败后"):
                return
            prev_status = task.get("_claimed_from_status", task.get("status", "decompose"))
            await self.add_log(task_id, f"❌ Leader 分解失败（exit={returncode}），退回 {prev_status}")
            if output.strip():
                await self.add_log(task_id, f"错误输出:\n{output[:800]}")
            await self.add_alert(
                summary=f"Leader 分解失败（exit={returncode}）",
                task_id=task_id,
                message=output[-1200:].strip(),
                kind="error",
                code="leader_decompose_failed",
                stage="leader_failed",
                metadata={"exit_code": returncode},
            )
            await self.add_handoff(
                task_id,
                stage="leader_failed",
                to_agent=self.name,
                status_from=prev_status,
                status_to=prev_status,
                title="任务分解失败",
                summary=f"Leader 分解失败（exit={returncode}），保持状态 {prev_status}",
                conclusion=f"分解失败，保持状态 {prev_status}",
                payload={"exit_code": returncode},
            )
            await self.update_task(task_id, status=prev_status, assignee=None)
            return
        await self.add_log(task_id, f"分解输出:\n{output[:600]}")
        if await self.stop_if_task_cancelled(task_id, "分解输出后"):
            return

        subtasks = self._parse_subtasks_array(output)
        if not subtasks:
            await self.add_log(task_id, "⚠ 无法解析子任务，回退为 todo")
            await self.add_handoff(
                task_id,
                stage="leader_to_todo",
                to_agent=task.get("assigned_agent") or "developer",
                status_from=task.get("_claimed_from_status", task.get("status")),
                status_to="todo",
                title="分解失败回退开发",
                summary="无法解析子任务，回退为 todo",
                conclusion="分解结果不可解析，回退到开发",
                payload={"action": "fallback_todo"},
            )
            await self.update_task(task_id, status="todo", assignee=None)
            return

        n = await self._create_subtasks(task, subtasks)
        await self.add_handoff(
            task_id,
            stage="leader_to_decomposed",
            to_agent="multi-agent",
            status_from=task.get("_claimed_from_status", task.get("status")),
            status_to="decomposed",
            title="强制分解完成",
            summary=f"已强制分解为 {n} 个子任务",
            conclusion=f"强制分解完成，共 {n} 个子任务",
            payload={"subtask_count": n, "forced": True},
        )
        await self.update_task(task_id, status="decomposed", assignee=None)
        await self.add_log(task_id, f"✅ 强制分解完成，共创建 {n} 个子任务")
        self._post_output_bg(f"✓ 强制分解完成: {n} 个子任务")


if __name__ == "__main__":
    asyncio.run(LeaderAgent().run())
